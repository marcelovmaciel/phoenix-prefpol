function find_reversal_pairs(unique_rankings::AbstractVector{<:Tuple})
    return Preferences.reversal_pairs(unique_rankings)
end

function local_reversal_values(paired_accum::AbstractVector{<:Tuple}, proportion_rankings::Dict)
    return (
        2 * min(proportion_rankings[pair[1]], proportion_rankings[pair[3]])
        for pair in paired_accum
    )
end

function calc_total_reversal_component(paired_accum, proportion_rankings::Dict)
    return sum(local_reversal_values(paired_accum, proportion_rankings))
end

function calc_reversal_HHI(paired_accum, proportion_rankings::Dict)
    values = collect(local_reversal_values(paired_accum, proportion_rankings))
    total = sum(values)
    total == 0.0 && return 0.0
    return sum((value / total)^2 for value in values)
end

function fast_reversal_geometric(paired_accum, proportion_rankings::Dict)
    values = collect(local_reversal_values(paired_accum, proportion_rankings))
    total = sum(values)
    total == 0.0 && return 0.0
    return sqrt(sum(value^2 for value in values) / total)
end

function _strict_profile_tuple_strings(profile)
    strict = strict_profile(profile)
    tuples = Tuple[]
    sizehint!(tuples, Preferences.nballots(strict))

    for ballot in strict.ballots
        push!(tuples, Tuple(string.(Preferences.ranking_signature(ballot, strict.pool))))
    end

    return strict, tuples
end

function nab(candidate1, candidate2, profile::Vector{<:Dict})
    return count(ranking -> ranking[candidate1] < ranking[candidate2], profile)
end

function dab(candidate1, candidate2, profile::Vector{<:Dict})
    return abs(nab(candidate1, candidate2, profile) - nab(candidate2, candidate1, profile))
end

function Ψ(profile)
    return Preferences.can_polarization(strict_profile(profile))
end

function get_paired_rankings_and_proportions(profile)
    _, rankings = _strict_profile_tuple_strings(profile)
    unique_rankings = unique(rankings)
    paired, _ = find_reversal_pairs(unique_rankings)
    proportion_rankings = Dict(k => v for (k, v) in proportionmap(rankings))
    return paired, proportion_rankings
end

function calc_total_reversal_component(profile)
    return Preferences.total_reversal_component(strict_profile(profile))
end

function calc_reversal_HHI(profile)
    return Preferences.reversal_hhi(strict_profile(profile))
end

function fast_reversal_geometric(profile)
    return Preferences.reversal_geometric(strict_profile(profile))
end

kendall_tau_dict(args...; kwargs...) = Preferences.kendall_tau_dict(args...; kwargs...)
average_normalized_distance(args...; kwargs...) = Preferences.average_normalized_distance(args...; kwargs...)
consensus_for_group(args...; kwargs...) = Preferences.consensus_for_group(args...; kwargs...)
group_avg_distance(args...; kwargs...) = Preferences.group_avg_distance(args...; kwargs...)
weighted_coherence(args...; kwargs...) = Preferences.weighted_coherence(args...; kwargs...)
pairwise_group_divergence(args...; kwargs...) = Preferences.pairwise_group_divergence(args...; kwargs...)
pairwise_group_overlap(args...; kwargs...) = Preferences.pairwise_group_overlap(args...; kwargs...)
smoothed_overlap(args...; kwargs...) = Preferences.smoothed_overlap(args...; kwargs...)
pairwise_group_median_distance(args...; kwargs...) = Preferences.pairwise_group_median_distance(args...; kwargs...)
pairwise_group_separation(args...; kwargs...) = Preferences.pairwise_group_separation(args...; kwargs...)
overall_divergence(args...; kwargs...) = Preferences.overall_divergence(args...; kwargs...)
overall_divergences(args...; kwargs...) = Preferences.overall_divergences(args...; kwargs...)
overall_overlap(args...; kwargs...) = Preferences.overall_overlap(args...; kwargs...)
overall_overlap_smoothed(args...; kwargs...) = Preferences.overall_overlap_smoothed(args...; kwargs...)
overall_overlaps(args...; kwargs...) = Preferences.overall_overlaps(args...; kwargs...)
overall_overlaps_smoothed(args...; kwargs...) = Preferences.overall_overlaps_smoothed(args...; kwargs...)
overall_divergence_median(args...; kwargs...) = Preferences.overall_divergence_median(args...; kwargs...)
overall_divergences_median(args...; kwargs...) = Preferences.overall_divergences_median(args...; kwargs...)
overall_separation(args...; kwargs...) = Preferences.overall_separation(args...; kwargs...)
overall_separations(args...; kwargs...) = Preferences.overall_separations(args...; kwargs...)
grouped_gsep(args...; kwargs...) = Preferences.grouped_gsep(args...; kwargs...)
S(args...; kwargs...) = Preferences.S(args...; kwargs...)
S_old(args...; kwargs...) = Preferences.S_old(args...; kwargs...)

function _measure_input(x)
    if x isa DataFrame
        return x
    elseif hasproperty(x, :profile)
        return getproperty(x, :profile)
    end
    return x
end

function apply_measure_to_bts(bts, measure)
    return Dict(variant => map(rep -> measure(_measure_input(rep)), bts[variant]) for variant in keys(bts))
end

function apply_all_measures_to_bts(
    bts;
    measures = [Ψ, calc_total_reversal_component, calc_reversal_HHI, fast_reversal_geometric],
)
    return Dict(nameof(measure) => apply_measure_to_bts(bts, measure) for measure in measures)
end
compute_group_metrics(args...; kwargs...) = Preferences.compute_group_metrics(args...; kwargs...)
bootstrap_group_metrics(args...; kwargs...) = Preferences.bootstrap_group_metrics(args...; kwargs...)
