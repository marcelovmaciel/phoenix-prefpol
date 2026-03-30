const _CONSENSUS_SCORE_TOL = 1.0e-9

struct LinearOrderCatalog{K}
    candidates::NTuple{K,Symbol}
    orders::Vector{SVector{K,UInt8}}
    max_kendall::Int
end

struct ConsensusResult{K,B<:Preferences.StrictRank}
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

_profile_candidate_tuple(profile) = _candidate_tuple(Preferences.candidates(profile.pool))

function ranking_to_perm(ranking)
    strict = strict_profile([ranking])
    return Preferences.ranking_signature(strict.ballots[1], strict.pool)
end

function perms_out_of_rankings(profile)
    strict = strict_profile(profile)
    return [Preferences.ranking_signature(ballot, strict.pool) for ballot in strict.ballots]
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

@inline function _perm_tuple(ballot::Preferences.StrictRank, ::Val{K}) where {K}
    perm = ballot.perm
    return ntuple(i -> UInt8(Int(perm[i])), K)
end

function _compressed_strict_ballots(profile::Preferences.Profile{<:Preferences.StrictRank}, ::Val{K}) where {K}
    masses = Dict{NTuple{K,UInt8},Float64}()
    order = NTuple{K,UInt8}[]

    @inbounds for ballot in profile.ballots
        key = _perm_tuple(ballot, Val(K))
        idx = get(masses, key, -1.0)
        if idx < 0
            push!(order, key)
            masses[key] = 1.0
        else
            masses[key] = idx + 1.0
        end
    end

    perms = [SVector{K,UInt8}(key) for key in order]
    weights = [masses[key] for key in order]
    return perms, weights, Float64(Preferences.nballots(profile))
end

function _compressed_strict_ballots(profile::Preferences.WeightedProfile{<:Preferences.StrictRank}, ::Val{K}) where {K}
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
    return perms, weights, Float64(Preferences.total_weight(profile))
end

@inline function _fill_positions!(pos::Vector{UInt8}, perm::SVector{K,UInt8}) where {K}
    @inbounds for rank in 1:K
        pos[Int(perm[rank])] = UInt8(rank)
    end
    return pos
end

@inline function _kendall_tau_distance_perm(ballot_perm::SVector{K,UInt8}, order_pos::Vector{UInt8}) where {K}
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

function _consensus_ranking_from_perm(candidates::NTuple{K,Symbol}, perm::SVector{K,UInt8}) where {K}
    ranking = Dict{Symbol,Int}()
    @inbounds for rank in 1:K
        ranking[candidates[Int(perm[rank])]] = rank
    end
    return ranking
end

function _ballot_from_perm(pool, perm::SVector{K,UInt8}) where {K}
    return Preferences.StrictRank(pool, collect(Int.(perm)))
end

function _candidate_set_matches(profile, consensus::ConsensusResult)
    return _profile_candidate_tuple(profile) == consensus.candidates
end

function _candidate_set_matches(profile, consensus::Preferences.StrictRank)
    return length(profile.pool) == length(Preferences.to_perm(consensus))
end

function _candidate_set_matches(profile, consensus::AbstractDict)
    return Set(Symbol.(keys(consensus))) == Set(_profile_candidate_tuple(profile))
end

function _find_consensus_impl(profile, active_candidates::NTuple{K,Symbol};
                              cache = GLOBAL_LINEAR_ORDER_CACHE,
                              rng = Random.default_rng(),
                              debug_all_minimizers::Bool = false) where {K}
    strict_profile(profile)
    profile_candidates = _profile_candidate_tuple(profile)
    active_candidates == profile_candidates || throw(ArgumentError(
        "Active candidate tuple does not match the profile pool order.",
    ))

    catalog = get_linear_order_catalog(active_candidates; cache = cache)
    ballot_perms, ballot_weights, total_mass = _compressed_strict_ballots(profile, Val(K))
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

    best_perm = rand(rng, minimizers)
    tie_rule = length(minimizers) == 1 ? :unique : :random_minimizer

    pool = profile.pool
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

function consensus_kendall(profile::Preferences.Profile{<:Preferences.StrictRank},
                           active_candidates;
                           cache = GLOBAL_LINEAR_ORDER_CACHE,
                           rng = Random.default_rng(),
                           debug_all_minimizers::Bool = false)
    return _find_consensus_impl(profile, _candidate_tuple(active_candidates);
                                cache = cache,
                                rng = rng,
                                debug_all_minimizers = debug_all_minimizers)
end

function consensus_kendall(profile::Preferences.WeightedProfile{<:Preferences.StrictRank},
                           active_candidates;
                           cache = GLOBAL_LINEAR_ORDER_CACHE,
                           rng = Random.default_rng(),
                           debug_all_minimizers::Bool = false)
    return _find_consensus_impl(profile, _candidate_tuple(active_candidates);
                                cache = cache,
                                rng = rng,
                                debug_all_minimizers = debug_all_minimizers)
end

function consensus_kendall(profile::Union{Preferences.Profile{<:Preferences.StrictRank},
                                          Preferences.WeightedProfile{<:Preferences.StrictRank}};
                           cache = GLOBAL_LINEAR_ORDER_CACHE,
                           rng = Random.default_rng(),
                           debug_all_minimizers::Bool = false)
    return consensus_kendall(profile, _profile_candidate_tuple(profile);
                             cache = cache,
                             rng = rng,
                             debug_all_minimizers = debug_all_minimizers)
end

function get_consensus_ranking(profile::Union{Preferences.Profile{<:Preferences.StrictRank},
                                              Preferences.WeightedProfile{<:Preferences.StrictRank}};
                               cache = GLOBAL_LINEAR_ORDER_CACHE,
                               rng = Random.default_rng())
    result = consensus_kendall(profile; cache = cache, rng = rng)
    ordered = [result.candidates[Int(id)] for id in result.consensus_perm]
    return ordered, result.consensus_ranking
end

function get_consensus_ranking(profile::AbstractVector{<:AbstractDict};
                               cache = GLOBAL_LINEAR_ORDER_CACHE,
                               rng = Random.default_rng())
    return get_consensus_ranking(strict_profile(profile); cache = cache, rng = rng)
end
