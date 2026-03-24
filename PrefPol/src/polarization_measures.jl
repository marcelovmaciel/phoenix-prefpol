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

function _strict_ballot_from_dict(ranking::AbstractDict; candidate_syms = nothing)
    strict = strict_profile([ranking]; candidate_syms = candidate_syms)
    return strict.pool, strict.ballots[1]
end

function _strict_consensus_ballot(consensus, pool)
    if consensus isa Preferences.StrictRank
        return consensus
    elseif consensus isa AbstractDict
        return _strict_ballot_from_dict(consensus; candidate_syms = Preferences.candidates(pool))[2]
    end
    throw(ArgumentError("Unsupported consensus type $(typeof(consensus)); expected Dict or Preferences.StrictRank."))
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

function consensus_for_group(subdf)
    strict = strict_profile(subdf)
    _, consensus_dict = get_consensus_ranking(strict)
    return (consensus_ranking = consensus_dict,)
end

function kendall_tau_dict(r1::Dict{T,Int}, r2::Dict{T,Int}) where {T}
    pool, b1 = _strict_ballot_from_dict(r1)
    _, b2 = _strict_ballot_from_dict(r2; candidate_syms = Preferences.candidates(pool))
    return Preferences.kendall_tau_distance(b1, b2)
end

function average_normalized_distance(profile, consensus)
    strict = strict_profile(profile)
    consensus_ballot = _strict_consensus_ballot(consensus, strict.pool)
    return Preferences.average_normalized_distance(strict, consensus_ballot)
end

function group_avg_distance(subdf)
    strict = strict_profile(subdf)
    _, consensus_ballot = get_consensus_ranking(strict)
    avg_dist = average_normalized_distance(strict, consensus_ballot)
    return (avg_distance = avg_dist, group_coherence = 1.0 - avg_dist)
end

function weighted_coherence(results_distance::DataFrame, proportion_map::Dict, key)
    return sum(row.group_coherence * proportion_map[row[key]] for row in eachrow(results_distance))
end

function pairwise_group_divergence(profile_i, consensus_j, m::Int)
    distance = average_normalized_distance(profile_i, consensus_j)
    m ≥ 2 || throw(ArgumentError("At least two candidates are required"))
    return distance
end

_profile_mass(profile::AbstractVector) = length(profile)
_profile_mass(profile::Preferences.Profile) = Preferences.nballots(profile)
_profile_mass(profile::Preferences.WeightedProfile) = Float64(Preferences.total_weight(profile))
_profile_mass(profile::AnnotatedProfile) = _profile_mass(profile.profile)

_consensus_length(consensus::AbstractDict) = length(consensus)
_consensus_length(consensus::Preferences.StrictRank) = length(Preferences.to_perm(consensus))

function overall_divergence(group_profiles, consensus_map)
    groups = keys(group_profiles)
    k = length(groups)
    n = sum(_profile_mass(profile) for profile in values(group_profiles))
    n > 0 || throw(ArgumentError("Grouped profiles must contain at least one ranking"))

    m = _consensus_length(first(values(consensus_map)))
    total = 0.0

    for i in groups
        n_i = _profile_mass(group_profiles[i])
        for j in groups
            i == j && continue
            d_ij = pairwise_group_divergence(group_profiles[i], consensus_map[j], m)
            total += (n_i / n) * d_ij
        end
    end

    return total / (k - 1)
end

function overall_divergences(grouped_consensus, whole_df, key)
    cols = Symbol.(names(grouped_consensus))
    consensus_col = if :consensus_ranking in cols
        :consensus_ranking
    elseif :x1 in cols
        :x1
    else
        throw(ArgumentError(
            "Grouped consensus table must contain either :consensus_ranking or :x1.",
        ))
    end

    k = nrow(grouped_consensus)
    groups_profiles = Dict(
        grouped_consensus[i, key] => map(
            row -> row.profile,
            Base.filter(row -> row[key] == grouped_consensus[i, key], eachrow(whole_df)),
        )
        for i in 1:k
    )
    consensus_map = Dict(row[key] => row[consensus_col] for row in eachrow(grouped_consensus))
    return overall_divergence(groups_profiles, consensus_map)
end

function _group_row_indices(bundle::AnnotatedProfile, demo)
    vals = bundle.metadata[!, demo]
    grouped = OrderedDict{Any,Vector{Int}}()

    for (idx, val) in pairs(vals)
        push!(get!(grouped, val, Int[]), idx)
    end

    return grouped
end

function overall_divergences(grouped_consensus::DataFrame, whole_bundle::AnnotatedProfile, key)
    cols = Symbol.(names(grouped_consensus))
    consensus_col = if :consensus_ranking in cols
        :consensus_ranking
    elseif :x1 in cols
        :x1
    else
        throw(ArgumentError(
            "Grouped consensus table must contain either :consensus_ranking or :x1.",
        ))
    end

    grouped_indices = _group_row_indices(whole_bundle, key)
    group_profiles = Dict(
        row[key] => _subset_profile(whole_bundle.profile, grouped_indices[row[key]])
        for row in eachrow(grouped_consensus)
    )
    consensus_map = Dict(row[key] => row[consensus_col] for row in eachrow(grouped_consensus))
    return overall_divergence(group_profiles, consensus_map)
end

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

function compute_group_metrics(df::DataFrame, demo)
    g = groupby(df, demo)
    results_distance = combine(g; threads = false) do subdf
        group_avg_distance(subdf)
    end

    prop = proportionmap(df[!, demo])
    C = weighted_coherence(results_distance, prop, demo)

    consensus = combine(g; threads = false) do subdf
        consensus_for_group(subdf)
    end

    D = overall_divergences(consensus, df, demo)
    return C, D
end

function compute_group_metrics(bundle::AnnotatedProfile, demo)
    grouped_indices = _group_row_indices(bundle, demo)
    group_vals = collect(keys(grouped_indices))

    avg_distance = Float64[]
    group_coherence = Float64[]
    consensus_rankings = Any[]

    for group in group_vals
        subprofile = _subset_profile(bundle.profile, grouped_indices[group])
        distance_row = group_avg_distance(subprofile)
        push!(avg_distance, distance_row.avg_distance)
        push!(group_coherence, distance_row.group_coherence)
        push!(consensus_rankings, consensus_for_group(subprofile).consensus_ranking)
    end

    results_distance = DataFrame(
        demo => group_vals,
        :avg_distance => avg_distance,
        :group_coherence => group_coherence,
    )
    prop = proportionmap(bundle.metadata[!, demo])
    C = weighted_coherence(results_distance, prop, demo)

    consensus = DataFrame(demo => group_vals, :consensus_ranking => consensus_rankings)
    D = overall_divergences(consensus, bundle, demo)
    return C, D
end

function bootstrap_group_metrics(bt_profiles, demo)
    result = Dict{Symbol, Dict{Symbol, Vector{Float64}}}()

    for (variant, reps) in bt_profiles
        Cvals = Float64[]
        Dvals = Float64[]

        for df in reps
            C, D = compute_group_metrics(df, demo)
            push!(Cvals, C)
            push!(Dvals, D)
        end

        result[variant] = Dict(:C => Cvals, :D => Dvals)
    end

    return result
end
