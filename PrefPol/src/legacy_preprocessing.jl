"""
    get_most_known_candidates(dont_know_her, how_many)

Legacy compatibility helper.

Return the names of the `how_many` candidates with the lowest "don't know"
percentages from a precomputed list. Publication candidate-set selection should
use `resolve_active_candidate_set`, not this unweighted compatibility utility.
"""
function get_most_known_candidates(dont_know_her::Vector{Tuple{String, Float64}}, how_many)
    return first.(Iterators.take(dont_know_her, how_many))
end


"""
    select_top_candidates(countmaps, nrespondents; m, force_include=String[])

Legacy compatibility helper.

Select up to `m` candidates by ascending unweighted "don't know" rates. Forced
candidates are pinned first in the returned order and the remaining slots are
filled from the missingness ordering. Publication specs and raw-profile helpers
should use `resolve_active_candidate_set`, which applies configured candidate
universes, scenario forcing, and survey weights.
"""
function select_top_candidates(countmaps::Dict{String,<:AbstractDict},
                               nrespondents::Int;
                               m::Int,
                               force_include::Vector{String}=String[])
    poplist = [name for (name, _) in compute_dont_know_her(countmaps, nrespondents)]
    return _select_top_candidates_from_poplist(poplist; m = m, force_include = force_include)
end


"""
    compute_candidate_set(scores_df; candidate_cols, m, force_include=String[])

Legacy compatibility helper.

Determine an unweighted candidate set from raw candidate score distributions.
This helper counts sentinel-code missingness in the supplied score table and is
kept for older PrefPol workflows only. For paper pipeline specs and raw-profile
helpers, use `resolve_active_candidate_set`.
"""
function compute_candidate_set(scores_df::DataFrame;
                               candidate_cols,
                               m::Int,
                               force_include::Vector{String} = String[])

    countmaps     = build_candidate_score_distributions(scores_df, candidate_cols)
    countmaps2    = sanitize_countmaps(countmaps)
    nrespondents  = nrow(scores_df)

    return select_top_candidates(countmaps2, nrespondents;
                                 m = m, force_include = force_include)
end


"""
    dont_know_her

Legacy mutable candidate-missingness cache used by older in-memory helpers.
Publication candidate-set selection does not read this global.
"""
dont_know_her = Tuple{String,Float64}[]


"""
    get_df_just_top_candidates(df, how_top; demographics=String[])
    get_df_just_top_candidates(df, which_ones; demographics=String[])

Legacy compatibility helper.

Return `df` restricted to selected candidate columns plus optional
`demographics`. `how_top` chooses the top `how_top` candidates from the legacy
global `dont_know_her` list, while `which_ones` specifies candidate names
directly.
"""
function get_df_just_top_candidates(df::DataFrame, how_top::Int; demographics = String[] )
    most_known_candidates = get_most_known_candidates(dont_know_her, how_top)
    return df[!, vcat(most_known_candidates, demographics)]
end

function get_df_just_top_candidates(df::DataFrame, which_ones; demographics = String[])
    return df[!, vcat(which_ones, demographics)]
end
