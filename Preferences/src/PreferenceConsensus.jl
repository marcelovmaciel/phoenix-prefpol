const _CONSENSUS_SCORE_TOL = 1.0e-9

struct LinearOrderCatalog{K}
    candidates::NTuple{K,Symbol}
    orders::Vector{SVector{K,UInt8}}
    max_kendall::Int
end

struct ConsensusResult{K,B<:StrictRank}
    candidates::NTuple{K,Symbol}
    consensus_perm::SVector{K,UInt8}
    consensus_ranking::Dict{Symbol,Int}
    consensus_ballot::B
    min_total_distance::Float64
    avg_normalized_distance::Float64
    total_mass::Float64
    is_tied_minimizer::Bool
    n_minimizers::Int
    tie_rule::Symbol
    all_minimizers::Vector{SVector{K,UInt8}}
end

const GLOBAL_LINEAR_ORDER_CACHE = Dict{Tuple{Vararg{Symbol}},Any}()

function _candidate_tuple(active_candidates)
    cands = tuple(Symbol.(collect(active_candidates))...)
    length(cands) >= 2 || throw(ArgumentError("At least two candidates are required."))
    length(unique(cands)) == length(cands) || throw(ArgumentError(
        "Active candidate tuple contains duplicates.",
    ))
    return cands
end

_profile_candidate_tuple(profile::Union{Profile,WeightedProfile}) =
    _candidate_tuple(candidates(profile.pool))

function _candidate_syms_from_rankings(rankings; fallback = nothing)
    if fallback !== nothing
        return Symbol.(collect(fallback))
    end

    isempty(rankings) && throw(ArgumentError(
        "Cannot infer candidate labels from an empty ranking collection without metadata.",
    ))

    first_ranking = rankings[firstindex(rankings)]
    first_ranking isa AbstractDict || throw(ArgumentError(
        "Expected ranking dictionaries when building a strict profile from a vector.",
    ))
    return sort!(collect(Symbol.(keys(first_ranking))); by = string)
end

function _strict_rank_from_dict(ranking::AbstractDict, pool::CandidatePool)
    ordered = sort!(collect(Symbol.(keys(ranking))); by = cand -> Int(ranking[cand]))
    return StrictRank(pool, ordered)
end

function _empty_strict_profile(candidate_syms)
    pool = CandidatePool(Symbol.(collect(candidate_syms)))
    exemplar = StrictRank(pool, collect(1:length(pool)))
    return Profile(pool, Vector{typeof(exemplar)}(undef, 0))
end

strict_profile(x::Profile{<:StrictRank}) = x
strict_profile(x::WeightedProfile{<:StrictRank}) = x

function strict_profile(x::Profile)
    is_strict(x) || throw(ArgumentError("Expected a strict Preferences.Profile input."))
    return x
end

function strict_profile(x::WeightedProfile)
    is_strict(x) || throw(ArgumentError("Expected a strict Preferences.WeightedProfile input."))
    return x
end

function strict_profile(df::AbstractDataFrame; col::Symbol = :profile, candidate_syms = nothing)
    hasproperty(df, col) || throw(ArgumentError("DataFrame is missing profile column `$col`."))
    return strict_profile(df[!, col]; candidate_syms = candidate_syms)
end

function strict_profile(profile::AbstractVector; candidate_syms = nothing)
    if isempty(profile)
        candidate_syms === nothing && throw(ArgumentError(
            "Cannot infer candidate labels from an empty ranking collection without metadata.",
        ))
        return _empty_strict_profile(candidate_syms)
    end

    all(ranking -> ranking isa AbstractDict, profile) || throw(ArgumentError(
        "Expected a vector of ranking dictionaries or a strict Preferences profile.",
    ))

    cand_syms = _candidate_syms_from_rankings(profile; fallback = candidate_syms)
    pool = CandidatePool(cand_syms)
    ballots = [_strict_rank_from_dict(ranking, pool) for ranking in profile]
    return Profile(pool, ballots)
end

function _build_linear_order_catalog(candidates::NTuple{K,Symbol}) where {K}
    ids = UInt8.(collect(1:K))
    orders = Vector{SVector{K,UInt8}}()
    sizehint!(orders, factorial(K))

    for perm in permutations(ids)
        push!(orders, SVector{K,UInt8}(perm))
    end

    sort!(orders; by = Tuple)
    return LinearOrderCatalog{K}(candidates, orders, binomial(K, 2))
end

function get_linear_order_catalog(active_candidates; cache = GLOBAL_LINEAR_ORDER_CACHE)
    candidates = _candidate_tuple(active_candidates)
    return get!(cache, candidates) do
        _build_linear_order_catalog(candidates)
    end
end

@inline function _perm_tuple(ballot::StrictRank, ::Val{K}) where {K}
    return ntuple(i -> UInt8(Int(ballot.perm[i])), K)
end

function _compressed_strict_ballots(profile::Profile{<:StrictRank}, ::Val{K}) where {K}
    masses = Dict{NTuple{K,UInt8},Float64}()
    order = NTuple{K,UInt8}[]

    @inbounds for ballot in profile.ballots
        key = _perm_tuple(ballot, Val(K))
        mass = get(masses, key, -1.0)
        if mass < 0
            push!(order, key)
            masses[key] = 1.0
        else
            masses[key] = mass + 1.0
        end
    end

    perms = [SVector{K,UInt8}(key) for key in order]
    weights = [masses[key] for key in order]
    return perms, weights, Float64(nballots(profile))
end

function _compressed_strict_ballots(profile::WeightedProfile{<:StrictRank}, ::Val{K}) where {K}
    masses = Dict{NTuple{K,UInt8},Float64}()
    order = NTuple{K,UInt8}[]

    @inbounds for (ballot, weight) in zip(profile.ballots, profile.weights)
        key = _perm_tuple(ballot, Val(K))
        if haskey(masses, key)
            masses[key] += Float64(weight)
        else
            push!(order, key)
            masses[key] = Float64(weight)
        end
    end

    perms = [SVector{K,UInt8}(key) for key in order]
    weights = [masses[key] for key in order]
    return perms, weights, Float64(total_weight(profile))
end

@inline _stable_number_string(x::Integer) = string(x)
@inline _stable_number_string(x::AbstractFloat) = @sprintf("%.17g", Float64(x))

function _escape_stable_string(s::AbstractString)
    escaped = replace(String(s), "\\" => "\\\\")
    escaped = replace(escaped, "(" => "\\(")
    escaped = replace(escaped, ")" => "\\)")
    escaped = replace(escaped, "," => "\\,")
    escaped = replace(escaped, "=" => "\\=")
    return escaped
end

_stable_serialize(x::Nothing) = "nothing"
_stable_serialize(x::Bool) = x ? "true" : "false"
_stable_serialize(x::Symbol) = "sym(" * _escape_stable_string(String(x)) * ")"
_stable_serialize(x::AbstractString) = "str(" * _escape_stable_string(x) * ")"
_stable_serialize(x::Integer) = "int(" * _stable_number_string(x) * ")"
_stable_serialize(x::AbstractFloat) = "float(" * _stable_number_string(x) * ")"

function _stable_serialize(xs::Tuple)
    return "tuple(" * join((_stable_serialize(x) for x in xs), ",") * ")"
end

function _stable_serialize(xs::NamedTuple)
    parts = String[]
    for name in propertynames(xs)
        push!(parts, String(name) * "=" * _stable_serialize(getfield(xs, name)))
    end
    return "named(" * join(parts, ",") * ")"
end

function _stable_serialize(xs::AbstractVector)
    return "vec(" * join((_stable_serialize(x) for x in xs), ",") * ")"
end

_stable_serialize(x) = "repr(" * repr(x) * ")"

function _default_tie_break_key(active_candidates::NTuple{K,Symbol},
                                ballot_perms::Vector{SVector{K,UInt8}},
                                ballot_weights::Vector{Float64}) where {K}
    zipped = collect(zip(ballot_perms, ballot_weights))
    sort!(zipped; by = pair -> Tuple(pair[1]))

    profile_sig = String[]
    for (perm, weight) in zipped
        push!(
            profile_sig,
            join((string(Int(idx)) for idx in perm), "-") * "@" * _stable_number_string(weight),
        )
    end

    return (
        candidates = active_candidates,
        compressed_profile = Tuple(profile_sig),
    )
end

function _digest_index(bytes::AbstractVector{UInt8}, n::Int)
    n >= 1 || throw(ArgumentError("cannot choose from an empty minimizer set"))
    idx = 0
    for byte in bytes
        idx = mod(idx * 256 + Int(byte), n)
    end
    return idx + 1
end

function _select_minimizer(minimizers::Vector{SVector{K,UInt8}},
                           tie_break_key;
                           rng = nothing) where {K}
    ordered = sort(copy(minimizers); by = Tuple)
    length(ordered) == 1 && return ordered[1], :unique

    if rng !== nothing
        return rand(rng, ordered), :random_minimizer
    end

    encoded_key = _stable_serialize(tie_break_key)
    digest = SHA.sha256(codeunits(encoded_key))
    selected_idx = _digest_index(digest, length(ordered))
    return ordered[selected_idx], :deterministic_pseudorandom_minimizer
end

@inline function _fill_positions!(pos::Vector{UInt8}, perm::SVector{K,UInt8}) where {K}
    @inbounds for rank in 1:K
        pos[Int(perm[rank])] = UInt8(rank)
    end
    return pos
end

@inline function _kendall_tau_distance_perm(ballot_perm::SVector{K,UInt8},
                                            order_pos::Vector{UInt8}) where {K}
    d = 0
    @inbounds for i in 1:(K - 1)
        pi = order_pos[Int(ballot_perm[i])]
        for j in (i + 1):K
            pj = order_pos[Int(ballot_perm[j])]
            pi > pj && (d += 1)
        end
    end
    return d
end

function _consensus_ranking_from_perm(candidates::NTuple{K,Symbol},
                                      perm::SVector{K,UInt8}) where {K}
    ranking = Dict{Symbol,Int}()
    @inbounds for rank in 1:K
        ranking[candidates[Int(perm[rank])]] = rank
    end
    return ranking
end

function _ballot_from_perm(pool::CandidatePool, perm::SVector{K,UInt8}) where {K}
    return StrictRank(pool, collect(Int.(perm)))
end

_candidate_set_matches(profile, consensus::ConsensusResult) =
    _profile_candidate_tuple(profile) == consensus.candidates

function _candidate_set_matches(profile, consensus::StrictRank)
    return length(profile.pool) == length(to_perm(consensus))
end

function _candidate_set_matches(profile, consensus::AbstractDict)
    return Set(Symbol.(keys(consensus))) == Set(_profile_candidate_tuple(profile))
end

function _find_consensus_impl(profile, active_candidates::NTuple{K,Symbol};
                              cache = GLOBAL_LINEAR_ORDER_CACHE,
                              rng = nothing,
                              tie_break_key = nothing,
                              debug_all_minimizers::Bool = false) where {K}
    strict = strict_profile(profile)
    profile_candidates = _profile_candidate_tuple(strict)
    active_candidates == profile_candidates || throw(ArgumentError(
        "Active candidate tuple does not match the profile pool order.",
    ))

    catalog = get_linear_order_catalog(active_candidates; cache = cache)
    ballot_perms, ballot_weights, total_mass = _compressed_strict_ballots(strict, Val(K))
    total_mass > 0 || throw(ArgumentError("Profile must contain positive mass."))

    best_total = Inf
    minimizers = SVector{K,UInt8}[]
    order_pos = zeros(UInt8, K)

    for perm in catalog.orders
        _fill_positions!(order_pos, perm)
        total = 0.0

        @inbounds for idx in eachindex(ballot_perms)
            total += ballot_weights[idx] * _kendall_tau_distance_perm(ballot_perms[idx], order_pos)
            total > best_total + _CONSENSUS_SCORE_TOL && break
        end

        if total + _CONSENSUS_SCORE_TOL < best_total
            best_total = total
            empty!(minimizers)
            push!(minimizers, perm)
        elseif abs(total - best_total) <= _CONSENSUS_SCORE_TOL
            push!(minimizers, perm)
        end
    end

    isempty(minimizers) && throw(ArgumentError("Failed to find a consensus ranking."))

    chosen_key = tie_break_key === nothing ?
        _default_tie_break_key(active_candidates, ballot_perms, ballot_weights) :
        tie_break_key
    best_perm, tie_rule = _select_minimizer(minimizers, chosen_key; rng = rng)

    pool = strict.pool
    consensus_ballot = _ballot_from_perm(pool, best_perm)
    consensus_ranking = _consensus_ranking_from_perm(active_candidates, best_perm)
    avg_normalized_distance = best_total / (total_mass * catalog.max_kendall)

    return ConsensusResult{K,typeof(consensus_ballot)}(
        active_candidates,
        best_perm,
        consensus_ranking,
        consensus_ballot,
        best_total,
        avg_normalized_distance,
        total_mass,
        length(minimizers) > 1,
        length(minimizers),
        tie_rule,
        copy(minimizers),
    )
end

function consensus_kendall(profile::Profile{<:StrictRank},
                           active_candidates;
                           cache = GLOBAL_LINEAR_ORDER_CACHE,
                           rng = nothing,
                           tie_break_key = nothing,
                           debug_all_minimizers::Bool = false)
    return _find_consensus_impl(profile, _candidate_tuple(active_candidates);
                                cache = cache,
                                rng = rng,
                                tie_break_key = tie_break_key,
                                debug_all_minimizers = debug_all_minimizers)
end

function consensus_kendall(profile::WeightedProfile{<:StrictRank},
                           active_candidates;
                           cache = GLOBAL_LINEAR_ORDER_CACHE,
                           rng = nothing,
                           tie_break_key = nothing,
                           debug_all_minimizers::Bool = false)
    return _find_consensus_impl(profile, _candidate_tuple(active_candidates);
                                cache = cache,
                                rng = rng,
                                tie_break_key = tie_break_key,
                                debug_all_minimizers = debug_all_minimizers)
end

function consensus_kendall(profile::Union{Profile{<:StrictRank},WeightedProfile{<:StrictRank}};
                           cache = GLOBAL_LINEAR_ORDER_CACHE,
                           rng = nothing,
                           tie_break_key = nothing,
                           debug_all_minimizers::Bool = false)
    return consensus_kendall(profile, _profile_candidate_tuple(profile);
                             cache = cache,
                             rng = rng,
                             tie_break_key = tie_break_key,
                             debug_all_minimizers = debug_all_minimizers)
end

function get_consensus_ranking(profile::Union{Profile{<:StrictRank},WeightedProfile{<:StrictRank}};
                               cache = GLOBAL_LINEAR_ORDER_CACHE,
                               rng = nothing,
                               tie_break_key = nothing)
    result = consensus_kendall(profile; cache = cache, rng = rng, tie_break_key = tie_break_key)
    ordered = [result.candidates[Int(id)] for id in result.consensus_perm]
    return ordered, result.consensus_ranking
end

function get_consensus_ranking(profile::AbstractVector;
                               cache = GLOBAL_LINEAR_ORDER_CACHE,
                               rng = nothing,
                               tie_break_key = nothing)
    return get_consensus_ranking(strict_profile(profile);
                                 cache = cache,
                                 rng = rng,
                                 tie_break_key = tie_break_key)
end

function _strict_ballot_from_dict(ranking::AbstractDict; candidate_syms = nothing)
    strict = strict_profile([ranking]; candidate_syms = candidate_syms)
    return strict.pool, strict.ballots[1]
end

_strict_consensus_ballot(consensus::ConsensusResult, pool::CandidatePool) = consensus.consensus_ballot
_strict_consensus_ballot(consensus::StrictRank, pool::CandidatePool) = consensus

function _strict_consensus_ballot(consensus::AbstractDict, pool::CandidatePool)
    return _strict_ballot_from_dict(consensus; candidate_syms = candidates(pool))[2]
end

function kendall_tau_dict(r1::AbstractDict, r2::AbstractDict)
    pool, b1 = _strict_ballot_from_dict(r1)
    _, b2 = _strict_ballot_from_dict(r2; candidate_syms = candidates(pool))
    return kendall_tau_distance(b1, b2)
end

function average_normalized_distance(profile::Union{Profile{<:StrictRank},WeightedProfile{<:StrictRank}},
                                     consensus::Union{ConsensusResult,AbstractDict})
    consensus_ballot = _strict_consensus_ballot(consensus, profile.pool)
    return average_normalized_distance(profile, consensus_ballot)
end

function average_normalized_distance(profile::AbstractVector, consensus)
    strict = strict_profile(profile)
    return average_normalized_distance(strict, consensus)
end

function _group_consensus_result(subdf;
                                 cache = GLOBAL_LINEAR_ORDER_CACHE,
                                 rng = nothing,
                                 tie_break_key = nothing,
                                 debug_all_minimizers::Bool = false)
    strict = strict_profile(subdf)
    return consensus_kendall(strict;
                             cache = cache,
                             rng = rng,
                             tie_break_key = tie_break_key,
                             debug_all_minimizers = debug_all_minimizers)
end

function consensus_for_group(subdf;
                             cache = GLOBAL_LINEAR_ORDER_CACHE,
                             rng = nothing,
                             tie_break_key = nothing,
                             debug_all_minimizers::Bool = false)
    result = _group_consensus_result(subdf;
                                     cache = cache,
                                     rng = rng,
                                     tie_break_key = tie_break_key,
                                     debug_all_minimizers = debug_all_minimizers)
    return (
        consensus_ranking = result.consensus_ranking,
        consensus_perm = result.consensus_perm,
        consensus_result = result,
        consensus_set = result.all_minimizers,
        is_tied_minimizer = result.is_tied_minimizer,
        n_minimizers = result.n_minimizers,
    )
end

function group_avg_distance(subdf;
                            cache = GLOBAL_LINEAR_ORDER_CACHE,
                            rng = nothing,
                            tie_break_key = nothing,
                            debug_all_minimizers::Bool = false)
    result = _group_consensus_result(subdf;
                                     cache = cache,
                                     rng = rng,
                                     tie_break_key = tie_break_key,
                                     debug_all_minimizers = debug_all_minimizers)
    return (
        avg_distance = result.avg_normalized_distance,
        group_coherence = 1.0 - result.avg_normalized_distance,
        min_total_distance = result.min_total_distance,
        consensus_result = result,
        consensus_set = result.all_minimizers,
        is_tied_minimizer = result.is_tied_minimizer,
        n_minimizers = result.n_minimizers,
    )
end

function weighted_coherence(results_distance::AbstractDataFrame, proportion_map::Dict, key)
    return sum(row.group_coherence * proportion_map[row[key]] for row in eachrow(results_distance))
end

@inline function _unit_interval(value::Real, name::AbstractString)
    x = Float64(value)
    if x < -1.0e-9 || x > 1.0 + 1.0e-9
        throw(ArgumentError("$name must lie in [0, 1], got $x."))
    end
    return clamp(x, 0.0, 1.0)
end

function pairwise_group_divergence(profile_i, consensus_j, m::Int)
    strict = strict_profile(profile_i)
    _candidate_set_matches(strict, consensus_j) || throw(ArgumentError(
        "Cannot compare groups with different candidate sets.",
    ))
    distance = average_normalized_distance(strict, consensus_j)
    m >= 2 || throw(ArgumentError("At least two candidates are required"))
    return distance
end

_profile_mass(profile::AbstractVector) = length(profile)
_profile_mass(profile::Profile) = nballots(profile)
_profile_mass(profile::WeightedProfile) = Float64(total_weight(profile))

_profile_candidate_tuple(profile::AbstractVector) = _profile_candidate_tuple(strict_profile(profile))
_profile_pool(profile::AbstractVector) = strict_profile(profile).pool
_profile_pool(profile::Profile) = profile.pool
_profile_pool(profile::WeightedProfile) = profile.pool

_consensus_length(consensus::AbstractDict) = length(consensus)
_consensus_length(consensus::ConsensusResult) = length(consensus.candidates)
_consensus_length(consensus::StrictRank) = length(to_perm(consensus))

function _consensus_ballots(consensus::ConsensusResult, pool::CandidatePool)
    return [_ballot_from_perm(pool, perm) for perm in consensus.all_minimizers]
end

function _consensus_ballots(consensus::StrictRank, pool::CandidatePool)
    return [_strict_consensus_ballot(consensus, pool)]
end

function _consensus_ballots(consensus::AbstractDict, pool::CandidatePool)
    return [_strict_consensus_ballot(consensus, pool)]
end

function _normalized_consensus_distance(consensus_i, consensus_j, pool::CandidatePool)
    ballot_i = _strict_consensus_ballot(consensus_i, pool)
    ballot_j = _strict_consensus_ballot(consensus_j, pool)
    m = length(pool)
    m >= 2 || throw(ArgumentError("At least two candidates are required"))
    return kendall_tau_distance(ballot_i, ballot_j) / binomial(m, 2)
end

function _assert_group_profile_candidate_tuple_consistency(group_profiles)
    expected = nothing

    for profile in values(group_profiles)
        tuple_i = _profile_candidate_tuple(profile)
        if expected === nothing
            expected = tuple_i
        elseif tuple_i != expected
            throw(ArgumentError("All groups must share the same active ordered candidate tuple."))
        end
    end

    expected === nothing && throw(ArgumentError("Grouped profiles must contain at least one group."))
    return expected
end

function _assert_group_candidate_tuple_consistency(group_profiles, consensus_map)
    expected = _assert_group_profile_candidate_tuple_consistency(group_profiles)

    for consensus in values(consensus_map)
        if consensus isa ConsensusResult
            consensus.candidates == expected || throw(ArgumentError(
                "Consensus result candidate tuple does not match grouped profiles.",
            ))
        elseif consensus isa AbstractDict
            Set(Symbol.(keys(consensus))) == Set(expected) || throw(ArgumentError(
                "Consensus ranking candidate set does not match grouped profiles.",
            ))
        elseif consensus isa StrictRank
            length(to_perm(consensus)) == length(expected) || throw(ArgumentError(
                "Consensus ballot length does not match grouped profiles.",
            ))
        else
            throw(ArgumentError("Unsupported consensus type $(typeof(consensus))."))
        end
    end

    return expected
end

function _group_pair_weights(group_profiles)
    groups = Any[]
    masses = Float64[]
    total_mass = 0.0

    for (group, profile) in pairs(group_profiles)
        mass = Float64(_profile_mass(profile))
        mass < 0.0 && throw(ArgumentError("Group profile masses must be nonnegative."))
        mass == 0.0 && continue
        push!(groups, group)
        push!(masses, mass)
        total_mass += mass
    end

    total_mass > 0 || throw(ArgumentError("Grouped profiles must contain positive mass."))
    length(groups) <= 1 && return OrderedDict{Tuple{Any,Any},Float64}()

    props = masses ./ total_mass
    denom = 1.0 - sum(pi^2 for pi in props)
    denom > 0 || return OrderedDict{Tuple{Any,Any},Float64}()

    weights = OrderedDict{Tuple{Any,Any},Float64}()
    for a in 1:(length(groups) - 1)
        for b in (a + 1):length(groups)
            weights[(groups[a], groups[b])] = (2.0 * props[a] * props[b]) / denom
        end
    end

    return weights
end

function _aggregate_group_pairs(group_profiles, pairwise_value)
    weights = _group_pair_weights(group_profiles)
    isempty(weights) && return 0.0

    total = 0.0
    for ((group_a, group_b), weight) in weights
        total += weight * pairwise_value(group_a, group_b)
    end

    return _unit_interval(total, "Grouped aggregate")
end

"""
    pairwise_group_overlap(profile_g, profile_h)

Return the overlap `sum_r min(p_g(r), p_h(r))` between two empirical ranking
distributions normalized within each group.
"""
function pairwise_group_overlap(profile_g, profile_h)
    strict_g = strict_profile(profile_g)
    strict_h = strict_profile(profile_h)

    _profile_candidate_tuple(strict_g) == _profile_candidate_tuple(strict_h) || throw(ArgumentError(
        "Cannot compare groups with different candidate sets.",
    ))
    _profile_mass(strict_g) > 0 || throw(ArgumentError("Grouped profiles must contain positive mass."))
    _profile_mass(strict_h) > 0 || throw(ArgumentError("Grouped profiles must contain positive mass."))

    props_g = ranking_proportions(strict_g)
    props_h = ranking_proportions(strict_h)

    if length(props_g) > length(props_h)
        props_g, props_h = props_h, props_g
    end

    overlap = sum(min(prop_g, get(props_h, ranking, 0.0)) for (ranking, prop_g) in props_g)
    return _unit_interval(overlap, "Pairwise group overlap")
end

"""
    pairwise_group_median_distance(consensus_g, consensus_h, pool)
    pairwise_group_median_distance(consensus_g::ConsensusResult, consensus_h::ConsensusResult)

Return the normalized Kendall distance between two group-median sets, averaged
exactly over all median pairs.
"""
function pairwise_group_median_distance(consensus_g, consensus_h, pool::CandidatePool)
    ballots_g = _consensus_ballots(consensus_g, pool)
    ballots_h = _consensus_ballots(consensus_h, pool)
    norm_factor = binomial(length(pool), 2)
    norm_factor > 0 || throw(ArgumentError("At least two candidates are required."))

    total = 0.0
    for ballot_g in ballots_g, ballot_h in ballots_h
        total += kendall_tau_distance(ballot_g, ballot_h) / norm_factor
    end

    distance = total / (length(ballots_g) * length(ballots_h))
    return _unit_interval(distance, "Pairwise group median distance")
end

function pairwise_group_median_distance(consensus_g::ConsensusResult,
                                        consensus_h::ConsensusResult)
    consensus_g.candidates == consensus_h.candidates || throw(ArgumentError(
        "Cannot compare consensus results with different candidate tuples.",
    ))
    return pairwise_group_median_distance(
        consensus_g,
        consensus_h,
        CandidatePool(collect(consensus_g.candidates)),
    )
end

function pairwise_group_median_distance(profile_g, profile_h;
                                        cache = GLOBAL_LINEAR_ORDER_CACHE,
                                        rng = nothing,
                                        tie_break_key = nothing,
                                        debug_all_minimizers::Bool = false)
    strict_g = strict_profile(profile_g)
    strict_h = strict_profile(profile_h)

    _profile_candidate_tuple(strict_g) == _profile_candidate_tuple(strict_h) || throw(ArgumentError(
        "Cannot compare groups with different candidate sets.",
    ))

    result_g = consensus_kendall(
        strict_g;
        cache = cache,
        rng = rng,
        tie_break_key = tie_break_key,
        debug_all_minimizers = debug_all_minimizers,
    )
    result_h = consensus_kendall(
        strict_h;
        cache = cache,
        rng = rng,
        tie_break_key = tie_break_key,
        debug_all_minimizers = debug_all_minimizers,
    )
    return pairwise_group_median_distance(result_g, result_h, strict_g.pool)
end

"""
    pairwise_group_separation(profile_g, consensus_g, profile_h, consensus_h)

Return the overlap-adjusted pairwise separation

`D_median(g, h) * (1 - O(g, h))`.
"""
function pairwise_group_separation(profile_g, consensus_g, profile_h, consensus_h)
    strict_g = strict_profile(profile_g)
    strict_h = strict_profile(profile_h)

    _profile_candidate_tuple(strict_g) == _profile_candidate_tuple(strict_h) || throw(ArgumentError(
        "Cannot compare groups with different candidate sets.",
    ))

    distance = pairwise_group_median_distance(consensus_g, consensus_h, strict_g.pool)
    overlap = pairwise_group_overlap(strict_g, strict_h)
    return _unit_interval(distance * (1.0 - overlap), "Pairwise group separation")
end

function pairwise_group_separation(profile_g, profile_h;
                                   cache = GLOBAL_LINEAR_ORDER_CACHE,
                                   rng = nothing,
                                   tie_break_key = nothing,
                                   debug_all_minimizers::Bool = false)
    strict_g = strict_profile(profile_g)
    strict_h = strict_profile(profile_h)
    distance = pairwise_group_median_distance(
        strict_g,
        strict_h;
        cache = cache,
        rng = rng,
        tie_break_key = tie_break_key,
        debug_all_minimizers = debug_all_minimizers,
    )
    overlap = pairwise_group_overlap(strict_g, strict_h)
    return _unit_interval(distance * (1.0 - overlap), "Pairwise group separation")
end

function overall_divergence(group_profiles, consensus_map)
    groups = keys(group_profiles)
    k = length(groups)
    n = sum(_profile_mass(profile) for profile in values(group_profiles))
    n > 0 || throw(ArgumentError("Grouped profiles must contain at least one ranking"))

    _assert_group_candidate_tuple_consistency(group_profiles, consensus_map)
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

"""
    overall_overlap(group_profiles)

Aggregate pairwise overlaps over unordered group pairs using the normalized
population weights `2π_gπ_h / (1 - sum_r π_r^2)`.
"""
function overall_overlap(group_profiles)
    _assert_group_profile_candidate_tuple_consistency(group_profiles)
    return _aggregate_group_pairs(group_profiles, (group_g, group_h) ->
        pairwise_group_overlap(group_profiles[group_g], group_profiles[group_h])
    )
end

"""
    overall_divergence_median(group_profiles, consensus_map)

Aggregate exact pairwise median distances over unordered group pairs using the
normalized population pair weights induced by `group_profiles`.
"""
function overall_divergence_median(group_profiles, consensus_map)
    _assert_group_candidate_tuple_consistency(group_profiles, consensus_map)
    return _aggregate_group_pairs(group_profiles, (group_g, group_h) ->
        pairwise_group_median_distance(
            consensus_map[group_g],
            consensus_map[group_h],
            _profile_pool(group_profiles[group_g]),
        )
    )
end

"""
    overall_separation(group_profiles, consensus_map)

Aggregate exact pairwise separations `D_median * (1 - O)` over unordered group
pairs using the normalized population pair weights induced by `group_profiles`.
"""
function overall_separation(group_profiles, consensus_map)
    _assert_group_candidate_tuple_consistency(group_profiles, consensus_map)
    return _aggregate_group_pairs(group_profiles, (group_g, group_h) ->
        pairwise_group_separation(
            group_profiles[group_g],
            consensus_map[group_g],
            group_profiles[group_h],
            consensus_map[group_h],
        )
    )
end

"""
    grouped_gsep(C, Sep)

Return `sqrt(C * Sep)` after validating that both arguments lie in `[0, 1]`.
"""
function grouped_gsep(C::Real, Sep::Real)
    return sqrt(_unit_interval(C, "C") * _unit_interval(Sep, "Sep"))
end

function _group_profiles_from_dataframe(grouped_consensus::AbstractDataFrame,
                                        whole_df::AbstractDataFrame,
                                        key)
    k = nrow(grouped_consensus)
    return Dict(
        grouped_consensus[i, key] => map(
            row -> row.profile,
            Base.filter(row -> row[key] == grouped_consensus[i, key], eachrow(whole_df)),
        )
        for i in 1:k
    )
end

function _consensus_column(grouped_consensus)
    cols = Symbol.(names(grouped_consensus))
    if :consensus_result in cols
        return :consensus_result
    elseif :consensus_ranking in cols
        return :consensus_ranking
    elseif :x1 in cols
        return :x1
    end

    throw(ArgumentError(
        "Grouped consensus table must contain either :consensus_result, :consensus_ranking or :x1.",
    ))
end

function overall_divergences(grouped_consensus::AbstractDataFrame,
                             whole_df::AbstractDataFrame,
                             key)
    consensus_col = _consensus_column(grouped_consensus)
    group_profiles = _group_profiles_from_dataframe(grouped_consensus, whole_df, key)
    consensus_map = Dict(row[key] => row[consensus_col] for row in eachrow(grouped_consensus))
    return overall_divergence(group_profiles, consensus_map)
end

function overall_overlaps(grouped_consensus::AbstractDataFrame,
                          whole_df::AbstractDataFrame,
                          key)
    group_profiles = _group_profiles_from_dataframe(grouped_consensus, whole_df, key)
    return overall_overlap(group_profiles)
end

function overall_divergences_median(grouped_consensus::AbstractDataFrame,
                                    whole_df::AbstractDataFrame,
                                    key)
    consensus_col = _consensus_column(grouped_consensus)
    group_profiles = _group_profiles_from_dataframe(grouped_consensus, whole_df, key)
    consensus_map = Dict(row[key] => row[consensus_col] for row in eachrow(grouped_consensus))
    return overall_divergence_median(group_profiles, consensus_map)
end

function overall_separations(grouped_consensus::AbstractDataFrame,
                             whole_df::AbstractDataFrame,
                             key)
    consensus_col = _consensus_column(grouped_consensus)
    group_profiles = _group_profiles_from_dataframe(grouped_consensus, whole_df, key)
    consensus_map = Dict(row[key] => row[consensus_col] for row in eachrow(grouped_consensus))
    return overall_separation(group_profiles, consensus_map)
end

function _merge_tie_break_key(base_key, extension::NamedTuple)
    if base_key === nothing
        return extension
    elseif base_key isa NamedTuple
        return merge(base_key, extension)
    end

    return (; context = base_key, extension...)
end

function _compute_group_metric_details(df::AbstractDataFrame, demo;
                                       cache = GLOBAL_LINEAR_ORDER_CACHE,
                                       rng = nothing,
                                       tie_break_context = nothing)
    g = groupby(df, demo)
    group_vals = Any[]
    avg_distance = Float64[]
    group_coherence = Float64[]
    consensus_results = Any[]
    consensus_rankings = Any[]

    for subdf in g
        group_val = subdf[1, demo]
        tie_key = _merge_tie_break_key(
            tie_break_context,
            (demographic = String(demo), group = string(group_val)),
        )
        result = _group_consensus_result(subdf; cache = cache, rng = rng, tie_break_key = tie_key)
        push!(group_vals, group_val)
        push!(avg_distance, result.avg_normalized_distance)
        push!(group_coherence, 1.0 - result.avg_normalized_distance)
        push!(consensus_results, result)
        push!(consensus_rankings, result.consensus_ranking)
    end

    results_distance = DataFrame(
        demo => group_vals,
        :avg_distance => avg_distance,
        :group_coherence => group_coherence,
    )

    prop = proportionmap(df[!, demo])
    C = weighted_coherence(results_distance, prop, demo)

    consensus = DataFrame(
        demo => group_vals,
        :consensus_result => consensus_results,
        :consensus_ranking => consensus_rankings,
    )

    D = overall_divergences(consensus, df, demo)
    D_median = overall_divergences_median(consensus, df, demo)
    O = overall_overlaps(consensus, df, demo)
    Sep = overall_separations(consensus, df, demo)
    Gsep = grouped_gsep(C, Sep)
    return (
        C = C,
        D = D,
        D_median = D_median,
        O = O,
        Sep = Sep,
        Gsep = Gsep,
    )
end

function compute_group_metrics(df::AbstractDataFrame, demo;
                               cache = GLOBAL_LINEAR_ORDER_CACHE,
                               rng = nothing,
                               tie_break_context = nothing)
    details = _compute_group_metric_details(
        df,
        demo;
        cache = cache,
        rng = rng,
        tie_break_context = tie_break_context,
    )
    return details.C, details.D
end

function bootstrap_group_metrics(bt_profiles, demo;
                                 rng = nothing,
                                 tie_break_context = nothing)
    result = Dict{Symbol, Dict{Symbol, Vector{Float64}}}()

    for (variant, reps) in bt_profiles
        Cvals = Float64[]
        Dvals = Float64[]
        Dmedianvals = Float64[]
        Ovals = Float64[]
        Sepvals = Float64[]
        Gsepvals = Float64[]

        for (rep_idx, rep) in enumerate(reps)
            rep_tie_key = _merge_tie_break_key(
                tie_break_context,
                (variant = String(variant), replicate = rep_idx),
            )
            details = _compute_group_metric_details(
                rep,
                demo;
                rng = rng,
                tie_break_context = rep_tie_key,
            )
            push!(Cvals, details.C)
            push!(Dvals, details.D)
            push!(Dmedianvals, details.D_median)
            push!(Ovals, details.O)
            push!(Sepvals, details.Sep)
            push!(Gsepvals, details.Gsep)
        end

        result[variant] = Dict(
            :C => Cvals,
            :D => Dvals,
            :D_median => Dmedianvals,
            :O => Ovals,
            :Sep => Sepvals,
            :Gsep => Gsepvals,
        )
    end

    return result
end
