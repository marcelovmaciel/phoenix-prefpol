# Raw profile loading, candidate resolution, and profile construction live in
# survey_config.jl so survey config and nested-pipeline semantics have one
# source of truth. This file keeps summary/printing compatibility wrappers.

# Generic pattern summarization/formatting now lives in Preferences.
"""
    profile_pattern_proportions(profile; weighted=true, candidate_set=nothing)

Applied wrapper around `Preferences.profile_pattern_proportions` for raw ESEB
profiles. Use this to summarize observed weak-rank response patterns after
`build_profile`; formal pattern definitions and weighting conventions are
documented in `Preferences`.
"""
function profile_pattern_proportions(profile; weighted::Bool = true,
                                     candidate_set = nothing)
    return _prefs().profile_pattern_proportions(
        profile;
        weighted = weighted,
        candidate_set = candidate_set,
    )
end

"""
    ranked_count(pattern_or_blocks)

Delegate to `Preferences.ranked_count`, which defines the formal rank-size
encoding used by pattern summaries.
"""
ranked_count(pattern::AbstractString) = _prefs().ranked_count(pattern)
ranked_count(blocks::AbstractVector{<:Integer}) = _prefs().ranked_count(blocks)

"""
    has_ties(pattern_or_blocks)

Delegate to `Preferences.has_ties` for the weak-order pattern convention used in
raw-profile summaries.
"""
has_ties(pattern::AbstractString) = _prefs().has_ties(pattern)
has_ties(blocks::AbstractVector{<:Integer}) = _prefs().has_ties(blocks)

"""
    ranking_type_support(r)

Delegate to `Preferences.ranking_type_support`, which enumerates the formal
weak-rank type support for `r` ranked candidates.
"""
ranking_type_support(r::Int) = _prefs().ranking_type_support(r)
"""
    ranking_type_template(blocks)

Delegate to `Preferences.ranking_type_template` for display labels of weak-rank
block patterns.
"""
ranking_type_template(blocks::AbstractVector{<:Integer}) = _prefs().ranking_type_template(blocks)

"""
    profile_ranksize_summary(profile; k, weighted=true, include_zero_rank=true)

Summarize how many candidates respondents ranked in a raw-profile build. This
is an applied wrapper; the table schema and rank-size convention are defined in
`Preferences.profile_ranksize_summary`.
"""
function profile_ranksize_summary(profile; k::Int, weighted::Bool = true,
                                  include_zero_rank::Bool = true)
    return _prefs().profile_ranksize_summary(
        profile;
        k = k,
        weighted = weighted,
        include_zero_rank = include_zero_rank,
    )
end

"""
    profile_ranking_type_proportions(profile; k, weighted=true, include_zero_rank=true)

Return weak-rank type proportions for a raw ESEB profile. PrefPol delegates the
formal ranking-type support and output schema to `Preferences`.
"""
function profile_ranking_type_proportions(profile; k::Int, weighted::Bool = true,
                                          include_zero_rank::Bool = true)
    return _prefs().profile_ranking_type_proportions(
        profile;
        k = k,
        weighted = weighted,
        include_zero_rank = include_zero_rank,
    )
end

"""
    pretty_print_ranksize_summary(summary; digits=4, io=stdout)

Print the rank-size summary table produced by `profile_ranksize_summary`. This
is a display wrapper around `Preferences`; it does not compute new profile
statistics.
"""
function pretty_print_ranksize_summary(summary; digits::Int = 4, io::IO = stdout)
    return _prefs().pretty_print_ranksize_summary(summary; digits = digits, io = io)
end

"""
    pretty_print_ranking_type_proportions(type_tbl; digits=4, io=stdout)

Print ranking-type proportions using the display convention from `Preferences`.
"""
function pretty_print_ranking_type_proportions(type_tbl; digits::Int = 4, io::IO = stdout)
    return _prefs().pretty_print_ranking_type_proportions(type_tbl; digits = digits, io = io)
end

"""
    pretty_print_profile_patterns(tbl; digits=4, io=stdout, others_threshold=nothing)

Print raw-profile pattern summaries. Pattern semantics are defined in
`Preferences`; PrefPol exposes this wrapper so ESEB profile diagnostics can be
called from the applied package namespace.
"""
function pretty_print_profile_patterns(tbl;
                                       digits::Int = 4,
                                       io::IO = stdout,
                                       others_threshold = nothing,
                                       others_label::AbstractString = "Others")
    return _prefs().pretty_print_profile_patterns(
        tbl;
        digits = digits,
        io = io,
        others_threshold = others_threshold,
        others_label = others_label,
    )
end
