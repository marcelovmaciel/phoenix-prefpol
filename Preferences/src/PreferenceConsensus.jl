const _CONSENSUS_SCORE_TOL = 1.0e-9

struct LinearOrderCatalog{K}
    candidates::NTuple{K,Symbol}
    orders::Vector{SVector{K,UInt8}}
    max_kendall::Int
end

@doc raw"""
    ConsensusResult

Result of an exhaustive Kendall/Kemeny consensus search. For profile rankings
`r_i` with masses `w_i`, the selected consensus `c` minimizes

```math
\sum_i w_i d_K(r_i,c)
```

over all strict linear orders of the active candidate set. `min_total_distance`
stores this objective, `avg_normalized_distance` divides it by
`total_mass * binomial(m, 2)`, and `consensus_ballot`, `consensus_perm`, and
`consensus_ranking` give equivalent strict-order representations.

The result is produced from strict `Profile` or `WeightedProfile` inputs with
positive total mass and at least two candidates. The normalized average distance
lies in `[0, 1]`. When multiple minimizers exist, `all_minimizers` records them,
`is_tied_minimizer` and `n_minimizers` describe the tie, and `tie_rule` records
whether the chosen minimizer was unique, random, or deterministic
pseudorandom.

Interpretation: the consensus is the ranking minimizing total Kendall
disagreement with the profile; the normalized distance is within-group
incoherence, so `1 - avg_normalized_distance` is group coherence.
"""
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

@doc raw"""
    strict_profile(x; candidate_syms=nothing, col=:profile)

Coerce supported profile-like inputs to a strict `Profile` or `WeightedProfile`
without linearizing weak orders. Existing strict `Profile{<:StrictRank}` and
`WeightedProfile{<:StrictRank}` inputs are returned unchanged; non-strict
formal profiles throw `ArgumentError`. A `DataFrame` input reads the vector in
`col` and delegates to the vector method. A vector input must contain ranking
dictionaries mapping candidate symbols to rank positions and is converted to an
unweighted strict `Profile`.

Empty vectors require `candidate_syms` metadata so the candidate universe is
known; otherwise they throw. Empty strict formal profiles can pass through here,
but downstream measures that need positive mass reject them.

Interpretation: this adapter fixes the active candidate set and strict ranking
representation used by Kendall consensus, group divergence, overlap, and
separation quantities. It does not impute, tie-break, or otherwise change
preference content.
"""
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

@doc raw"""
    consensus_kendall(profile[, active_candidates]; cache=GLOBAL_LINEAR_ORDER_CACHE, rng=nothing, tie_break_key=nothing)

Compute the Kendall/Kemeny consensus ranking. For strict rankings `r_i` and
masses `w_i`, the objective is

```math
\min_c \sum_i w_i d_K(r_i,c),
```

where the minimum ranges over every strict linear order of `active_candidates`.
Distances are Kendall discordant-pair counts and the reported average is
normalized by `binomial(m, 2)`.

Inputs are strict `Profile` or `WeightedProfile` objects; `active_candidates`,
when supplied, must match the profile pool order. Empty or zero-mass profiles
throw `ArgumentError`, and at least two candidates are required. The result is a
`ConsensusResult`. Tied minimizers are all retained; with `rng` the selected
minimizer is random, otherwise a stable SHA-based pseudorandom rule chooses one
from the sorted minimizer set.

Interpretation: this returns the central strict ranking minimizing total
Kendall disagreement with the profile, together with the profile's normalized
incoherence around that consensus.
"""
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

@doc raw"""
    get_consensus_ranking(profile; cache=GLOBAL_LINEAR_ORDER_CACHE, rng=nothing, tie_break_key=nothing)
    get_consensus_ranking(rankings; kwargs...)

Return the selected Kendall consensus as `(ordered_symbols, ranking_dict)`. This
is a projection of `consensus_kendall`: `ordered_symbols` is the best-to-worst
candidate-symbol vector and `ranking_dict` maps each symbol to its rank
position.

Inputs are strict formal profiles or vectors accepted by `strict_profile`. Empty
or zero-mass profiles follow `consensus_kendall` and throw unless the error
occurs earlier during candidate inference. The ranking is a strict linear order
over the active candidate set; ties among Kemeny minimizers use the same random
or deterministic pseudorandom rule as `consensus_kendall`.

Interpretation: this is a convenience representation of the group or profile
consensus, not a different consensus rule.
"""
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

@doc raw"""
    kendall_tau_dict(r1, r2)

Return the Kendall tau distance between two ranking dictionaries. Each
dictionary maps candidate symbols to strict rank positions; both rankings must
cover the same candidate set. The value is the discordant-pair count
`d_K(r1,r2)` in `0:binomial(m, 2)`.

Empty dictionaries fail because a strict profile cannot infer a valid candidate
universe with at least two candidates.

Interpretation: this is the dictionary-facing form of `kendall_tau_distance`
used by consensus adapters.
"""
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

@doc raw"""
    consensus_for_group(subdf; kwargs...)

Compute the Kendall/Kemeny consensus for one grouped subset and return a named
tuple with `consensus_ranking`, `consensus_perm`, `consensus_result`,
`consensus_set`, and tie diagnostics. `subdf` is accepted by `strict_profile`,
typically a `DataFrame` whose `:profile` column contains ranking dictionaries.

The mathematical definition, input restrictions, normalization, and empty or
zero-mass behavior are those of `consensus_kendall`.

Interpretation: this is the group-level consensus object used by downstream
coherence, divergence, overlap, and separation summaries.
"""
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

@doc raw"""
    group_avg_distance(subdf; kwargs...)

Compute a group's Kendall consensus and return its average normalized distance
and coherence. If `c_g` is the group consensus and `p_g` the group's empirical
ranking distribution, then

```math
W_g = \sum_r p_g(r) \frac{d_K(r,c_g)}{\binom{m}{2}},
\qquad C_g = 1 - W_g.
```

Inputs and zero-mass behavior follow `consensus_kendall`. `avg_distance` is
`W_g` in `[0, 1]`; `group_coherence` is `C_g` in `[0, 1]`.

Interpretation: lower average distance means rankings in the group are more
coherent around their Kendall consensus.
"""
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

@doc raw"""
    weighted_coherence(results_distance, proportion_map, key)

Aggregate group coherences into aggregate quantity `C`:

```math
C = \sum_g \pi_g C_g,
```

where `C_g` is read from `results_distance.group_coherence` and `π_g` from
`proportion_map[row[key]]`.

Inputs are a DataFrame with one row per group, a group-proportion dictionary,
and the grouping column name. This helper does not renormalize or validate the
map; missing keys or malformed columns raise the ordinary Julia/DataFrames
errors. Empty inputs return the empty sum `0.0`.

Interpretation: `C` is average within-group consensus coherence, weighted by
group population shares.
"""
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

@inline _normalized_kendall_distance(ballot_i, ballot_j, norm_factor::Real) =
    kendall_tau_distance(ballot_i, ballot_j) / norm_factor

@doc raw"""
    pairwise_group_divergence(profile_i, consensus_j, m)

Return the directed normalized Kendall distance from group `i`'s profile to
group `j`'s consensus:

```math
D_{i \to j} = \sum_r p_i(r) \frac{d_K(r,c_j)}{\binom{m}{2}}.
```

Inputs are a strict profile-like object accepted by `strict_profile`, a
consensus represented as `ConsensusResult`, `StrictRank`, or ranking dictionary,
and `m >= 2`. Candidate sets must match. Empty or zero-mass profile inputs
throw through `average_normalized_distance`. The value lies in `[0, 1]`.

Interpretation: this asks how far one group's members are from another group's
consensus, so it is directed and need not equal `D_{j -> i}`.
"""
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

@doc raw"""
    pairwise_group_overlap(profile_g, profile_h)

Return the exact ranking-distribution overlap between two groups:

```math
O(g,h) = \sum_r \min(p_g(r), p_h(r)).
```

Inputs are strict profile-like objects accepted by `strict_profile`; both groups
must have the same ordered candidate set and positive mass. The value lies in
`[0, 1]`, with `1` for identical empirical ranking distributions and `0` for
disjoint observed ranking support. Empty or zero-mass groups throw
`ArgumentError`.

Interpretation: overlap measures shared preference-profile support, independent
of where the groups' consensus rankings lie in Kendall space.
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

@inline function _adjacent_swap_neighbor(ranking::NTuple{N,T}, i::Int) where {N,T}
    1 <= i < N || throw(ArgumentError(
        "Adjacent swap index $i is out of bounds for ranking length $N.",
    ))
    # Kendall distance 1 between complete linear orders is exactly one adjacent transposition.
    return ntuple(j -> j == i ? ranking[i + 1] : j == i + 1 ? ranking[i] : ranking[j], N)
end

@doc raw"""
    _radius1_smoothed_ranking_proportions(profile)

Return the empirical ranking distribution after radius-1 Kendall smoothing.

Each observed complete linear order spreads its normalized mass uniformly over
its Kendall radius-1 ball: the ranking itself plus the `m - 1` rankings reached
by one adjacent swap. Because that ball has exactly `1 + (m - 1) = m` elements,
the uniform kernel weight on each member is exactly `1 / m`.
"""
function _radius1_smoothed_ranking_proportions(profile)
    strict = strict_profile(profile)
    props = ranking_proportions(strict)
    m = length(strict.pool)
    m >= 2 || throw(ArgumentError("At least two candidates are required."))

    smoothed = Dict{Tuple,Float64}()
    kernel_mass = 1.0 / m

    for (ranking, mass) in props
        contribution = mass * kernel_mass
        smoothed[ranking] = get(smoothed, ranking, 0.0) + contribution

        for swap_idx in 1:(m - 1)
            neighbor = _adjacent_swap_neighbor(ranking, swap_idx)
            smoothed[neighbor] = get(smoothed, neighbor, 0.0) + contribution
        end
    end

    total = sum(values(smoothed))
    isapprox(total, 1.0; atol = 1.0e-9, rtol = 1.0e-9) || throw(ArgumentError(
        "Smoothed ranking proportions must sum to 1, got $total.",
    ))

    return smoothed
end

@doc raw"""
    smoothed_overlap(profile_g, profile_h)

Return the radius-1 Kendall-smoothed overlap between two empirical ranking
distributions. Each ranking of mass `p_r` spreads mass uniformly over its
Kendall ball containing itself and the `m - 1` adjacent-swap neighbors, so each
neighbor receives `p_r / m`; overlap is then `sum_r min(tilde p_g(r), tilde p_h(r))`.

Inputs, candidate-set checks, positive-mass requirements, and `[0, 1]` range are
the same as `pairwise_group_overlap`. Empty or zero-mass groups throw.

Interpretation: smoothed overlap treats nearly identical rankings as partially
shared support while leaving grouped aggregation weights unchanged.
"""
function smoothed_overlap(profile_g, profile_h)
    strict_g = strict_profile(profile_g)
    strict_h = strict_profile(profile_h)

    _profile_candidate_tuple(strict_g) == _profile_candidate_tuple(strict_h) || throw(ArgumentError(
        "Cannot compare groups with different candidate sets.",
    ))
    _profile_mass(strict_g) > 0 || throw(ArgumentError("Grouped profiles must contain positive mass."))
    _profile_mass(strict_h) > 0 || throw(ArgumentError("Grouped profiles must contain positive mass."))

    props_g = _radius1_smoothed_ranking_proportions(strict_g)
    props_h = _radius1_smoothed_ranking_proportions(strict_h)

    if length(props_g) > length(props_h)
        props_g, props_h = props_h, props_g
    end

    overlap = sum(min(prop_g, get(props_h, ranking, 0.0)) for (ranking, prop_g) in props_g)
    return _unit_interval(overlap, "Smoothed pairwise group overlap")
end

@doc raw"""
    pairwise_group_median_distance(consensus_g, consensus_h, pool)
    pairwise_group_median_distance(consensus_g::ConsensusResult, consensus_h::ConsensusResult)
    pairwise_group_median_distance(profile_g, profile_h; kwargs...)

Return the normalized Kendall distance between group consensus sets. For two
sets of tied Kendall minimizers `M_g` and `M_h`, the distance is

```math
D_{median}(g,h) = \frac{1}{|M_g||M_h|} \sum_{c \in M_g} \sum_{d \in M_h}
\frac{d_K(c,d)}{\binom{m}{2}}.
```

Inputs may be consensus objects with an explicit `CandidatePool`, two
`ConsensusResult`s sharing candidates, or two strict profile-like objects from
which consensuses are computed. At least two candidates are required; mismatched
candidate sets throw. The result lies in `[0, 1]`.

Interpretation: this is undirected separation between group medians, averaging
over all tied consensus rankings rather than only the selected representative.
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

@doc raw"""
    pairwise_group_separation(profile_g, consensus_g, profile_h, consensus_h)
    pairwise_group_separation(profile_g, profile_h; kwargs...)

Return overlap-adjusted pairwise group separation:

```math
Sep(g,h) = D_{median}(g,h)\,(1 - O(g,h)).
```

Inputs are strict profile-like objects and either supplied consensus objects or
profiles from which consensuses are computed. Candidate sets must match, groups
must have positive mass, and at least two candidates are required. The result is
clamped/validated to `[0, 1]`.

Interpretation: two groups are separated only when their consensus sets are far
apart and their exact ranking supports do not strongly overlap.
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

@doc raw"""
    overall_divergence(group_profiles, consensus_map)

Aggregate directed cross-group divergence as quantity `D`. With group
shares `π_i`, group consensuses `c_j`, and `G` groups,

```math
D = \frac{1}{G-1} \sum_i \pi_i \sum_{j \ne i} D_{i \to j}.
```

`group_profiles` maps group labels to strict profile-like objects and
`consensus_map` maps the same labels to `ConsensusResult`, `StrictRank`, or
ranking-dictionary consensuses. Candidate sets must be consistent and total
group mass must be positive. Empty total mass throws; with only one group the
current arithmetic returns `NaN`.

Interpretation: `D` is average distance from members of each group to outgroup
consensuses, weighted by source-group population shares.
"""
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

@doc raw"""
    overall_overlap(group_profiles)

Aggregate exact pairwise overlaps over unordered group pairs. Pair weights are

```math
\omega_{gh} = \frac{2\pi_g\pi_h}{1 - \sum_r \pi_r^2},
```

computed from positive group masses, and the returned quantity is
`sum_{g<h} omega_gh O(g,h)`.

Inputs map group labels to strict profile-like objects with a common candidate
set. Zero total group mass throws. If fewer than two positive-mass groups remain,
the aggregate returns `0.0`; otherwise the range is `[0, 1]`.

Interpretation: this is the between-group shared ranking-support component used
to temper separation quantities.
"""
function overall_overlap(group_profiles)
    _assert_group_profile_candidate_tuple_consistency(group_profiles)
    return _aggregate_group_pairs(group_profiles, (group_g, group_h) ->
        pairwise_group_overlap(group_profiles[group_g], group_profiles[group_h])
    )
end

@doc raw"""
    overall_overlap_smoothed(group_profiles)

Aggregate radius-1 Kendall-smoothed pairwise overlaps over unordered group pairs
using the same normalized weights `omega_gh` as `overall_overlap`; only the
underlying pairwise overlap object is replaced by `smoothed_overlap`.

Inputs, zero-mass behavior, and range are the same as `overall_overlap`.
Interpretation: this treats adjacent Kendall rankings as local shared support
before aggregating between groups.
"""
function overall_overlap_smoothed(group_profiles)
    _assert_group_profile_candidate_tuple_consistency(group_profiles)
    return _aggregate_group_pairs(group_profiles, (group_g, group_h) ->
        smoothed_overlap(group_profiles[group_g], group_profiles[group_h])
    )
end

@doc raw"""
    overall_divergence_median(group_profiles, consensus_map)

Aggregate exact pairwise median distances over unordered group pairs:

```math
D_{median} = \sum_{g<h} \omega_{gh} D_{median}(g,h),
```

where `omega_gh` are the population pair weights from `overall_overlap`.

Inputs are group profiles and consensus maps with common candidate sets. Zero
total group mass throws; fewer than two positive-mass groups return `0.0`. The
range is `[0, 1]`.

Interpretation: unlike directed `D`, this compares group consensus sets to each
other directly and symmetrically.
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

@doc raw"""
    overall_separation(group_profiles, consensus_map)

Aggregate overlap-adjusted pairwise separations over unordered group pairs:

```math
Sep = \sum_{g<h} \omega_{gh} Sep(g,h), \qquad
Sep(g,h)=D_{median}(g,h)(1-O(g,h)).
```

Inputs are group profiles and consensus maps with common candidate sets. Zero
total group mass throws; fewer than two positive-mass groups return `0.0`. The
range is `[0, 1]`.

Interpretation: this is high only when groups have distinct consensus rankings
and little shared exact ranking support.
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

@doc raw"""
    grouped_gsep(C, Sep)

Return `sqrt(C * Sep)` after validating that both arguments lie in `[0, 1]`.
"""
function grouped_gsep(C::Real, Sep::Real)
    return sqrt(_unit_interval(C, "C") * _unit_interval(Sep, "Sep"))
end

@doc raw"""
    group_coherence_from_within_dispersion(W)

Return the normalized grouped coherence

```math
C = 1 - 2W
```

from within-group consensus dispersion `W`. This is the scalar conversion used
in the grouped `C`-`D` decomposition. The helper intentionally performs no range
validation so callers can preserve existing numerical behavior while deciding
where profile-derived admissibility checks belong.
"""
group_coherence_from_within_dispersion(W::Real) = 1.0 - (2.0 * Float64(W))

@doc raw"""
    within_dispersion_from_group_coherence(C)

Return the within-group dispersion baseline

```math
W = \frac{1-C}{2}
```

from normalized grouped coherence `C`.
"""
within_dispersion_from_group_coherence(C::Real) = (1.0 - Float64(C)) / 2.0

@doc raw"""
    grouped_geometric_index(C, D)

Return the geometric grouped index `sqrt(max(C * D, 0))`.

This preserves the historical grouped-`G` convention: negative products are
floored at zero before the square root instead of being treated as domain
errors. The helper is pure scalar algebra and does not validate whether `C` and
`D` came from an admissible profile.
"""
grouped_geometric_index(C::Real, D::Real) = sqrt(max(Float64(C) * Float64(D), 0.0))

@doc raw"""
    separation_ratio(D, W)

Return the scalar grouped separation ratio `D / W`.

The function deliberately uses ordinary floating-point division so `W == 0`
keeps Julia's existing `Inf` or `NaN` behavior.
"""
separation_ratio(D::Real, W::Real) = Float64(D) / Float64(W)

@doc raw"""
    overall_sstar_from_CD(C, D)

Return the excess-divergence component `D - (1 - C) / 2` from the
consensus-relative `C`-`D` decomposition.

For admissible profile-derived `C` and `D`, Appendix-style reasoning gives
`D >= (1 - C) / 2`, so this diagnostic is nonnegative. `C` and `D` are
validated on their native unit-interval scales, but the helper does not enforce
the theoretical cross-constraint; a negative raw result means the numeric inputs
are invalid for this diagnostic.
"""
function overall_sstar_from_CD(C::Real, D::Real)
    coherence = _unit_interval(C, "C")
    divergence = _unit_interval(D, "D")
    return divergence - within_dispersion_from_group_coherence(coherence)
end

@doc raw"""
    S(C, D)

Return the excess-divergence component from coherence `C` and directed
divergence `D`:

```math
S = D - \frac{1-C}{2}.
```

Under the consensus-relative definitions of `C` and `D`, admissible
profile-derived inputs satisfy `D >= (1 - C) / 2`, so `S >= 0`. `S` is derived
from `C` and `D`; it is not a legacy measure and not an arbitrary composite
polarization score.

The function validates only the unit-interval scale of `C` and `D`. If arbitrary
numeric inputs violate the theoretical bound, the raw result can be negative;
such inputs are invalid for this diagnostic rather than examples of admissible
negative `S`. Use `S_old` for the legacy support-separation statistic.
"""
S(C::Real, D::Real) = overall_sstar_from_CD(C, D)

@doc raw"""
    normalized_consensus_separation(W, D; atol = 1e-10)

Return the normalized excess-divergence ratio

    E = 1 - W / D

where `W = (1 - C) / 2` is the within-group dispersion baseline from the
`C`-`D` decomposition and `D` is directed divergence to outgroup consensuses.
When `D > 0`, `E = S / D`; when `D == 0`, the package convention returns
`0.0`. Tiny numerical excursions outside `[0, 1]` are clamped; larger violations
throw an error.
"""
function normalized_consensus_separation(W::Real, D::Real; atol::Real = 1.0e-10)
    w = Float64(W)
    d = Float64(D)
    tol = Float64(atol)

    w < -tol && throw(ArgumentError("W must be nonnegative, got $w."))
    d < -tol && throw(ArgumentError("D must be nonnegative, got $d."))

    d <= tol && return 0.0

    E = 1.0 - (w / d)
    if E < -tol || E > 1.0 + tol
        throw(ArgumentError("E outside [0, 1]: E=$E, W=$w, D=$d."))
    end

    return clamp(E, 0.0, 1.0)
end

@doc raw"""
    consensus_excess_separation(W, D; kwargs...)

Alias in the derived `C`-`D` decomposition family for
`normalized_consensus_separation(W, D; kwargs...)`. Returns `E = 1 - W/D`, with
the same `D == 0 => 0.0` convention and unit-range validation.
"""
consensus_excess_separation(W::Real, D::Real; kwargs...) =
    normalized_consensus_separation(W, D; kwargs...)

@doc raw"""
    group_E(W, D; kwargs...)

Alias in the derived `C`-`D` decomposition family for
`normalized_consensus_separation(W, D; kwargs...)`, used when `W` and `D`
describe a group-level within-versus-outgroup distance contrast.
"""
group_E(W::Real, D::Real; kwargs...) = normalized_consensus_separation(W, D; kwargs...)

@doc raw"""
    aggregate_E(W, D; kwargs...)

Alias in the derived `C`-`D` decomposition family for
`normalized_consensus_separation(W, D; kwargs...)`, used when `W` and `D` are
aggregate coherence/divergence quantities.
"""
aggregate_E(W::Real, D::Real; kwargs...) = normalized_consensus_separation(W, D; kwargs...)

@doc raw"""
    E(W, D; kwargs...)

Shorthand for the normalized excess-divergence ratio in the derived `C`-`D`
decomposition family:

```math
E = 1 - \frac{W}{D}.
```

For `D > 0`, `E = S/D`. The implementation returns `0.0` when `D == 0`,
validates nonnegative inputs, and clamps only tiny numerical excursions outside
`[0, 1]`.
"""
E(W::Real, D::Real; kwargs...) = normalized_consensus_separation(W, D; kwargs...)

function _within_group_average_normalized_kendall(profile)
    strict = strict_profile(profile)
    ballots = strict.ballots
    n = length(ballots)
    n > 1 || return NaN

    norm_factor = binomial(length(strict.pool), 2)
    norm_factor > 0 || throw(ArgumentError("At least two candidates are required."))

    total = 0.0

    for a in 1:(n - 1)
        ballot_a = ballots[a]
        for b in (a + 1):n
            total += 2.0 * _normalized_kendall_distance(ballot_a, ballots[b], norm_factor)
        end
    end

    return total / (n * (n - 1))
end

function _cross_group_average_normalized_kendall(profile_i, profile_j)
    strict_i = strict_profile(profile_i)
    strict_j = strict_profile(profile_j)
    ballots_i = strict_i.ballots
    ballots_j = strict_j.ballots
    n_i = length(ballots_i)
    n_j = length(ballots_j)
    (n_i > 0 && n_j > 0) || throw(ArgumentError("Grouped profiles must be nonempty."))
    Tuple(strict_i.pool.names) == Tuple(strict_j.pool.names) || throw(ArgumentError(
        "All groups must share the same active candidate set.",
    ))

    norm_factor = binomial(length(strict_i.pool), 2)
    norm_factor > 0 || throw(ArgumentError("At least two candidates are required."))

    total = 0.0
    for ballot_i in ballots_i, ballot_j in ballots_j
        total += _normalized_kendall_distance(ballot_i, ballot_j, norm_factor)
    end

    return total / (n_i * n_j)
end

@doc raw"""
    overall_support_separation_old(group_profiles, group_sizes)

Return the legacy support-separation contrast that used to back grouped `S`.

This logic is preserved unchanged under the explicit public alias `S_old` so
the new cleaned `S` can remain unambiguous.
"""
function overall_support_separation_old(group_profiles, group_sizes)
    groups = [group for group in keys(group_profiles) if group_sizes[group] > 0]
    length(groups) >= 2 || return NaN

    within = Dict{Any,Float64}()
    for group in groups
        within[group] = _within_group_average_normalized_kendall(group_profiles[group])
    end

    total = 0.0
    weight_sum = 0.0

    # Legacy S_old is aggregated over unordered pairs only. Pairs touching
    # singleton groups are skipped because W_i is undefined at n_i = 1.
    for a in 1:(length(groups) - 1)
        group_a = groups[a]
        W_a = within[group_a]
        n_a = group_sizes[group_a]

        for b in (a + 1):length(groups)
            group_b = groups[b]
            W_b = within[group_b]
            (isnan(W_a) || isnan(W_b)) && continue

            weight = n_a * group_sizes[group_b]
            total += weight * (
                _cross_group_average_normalized_kendall(
                    group_profiles[group_a],
                    group_profiles[group_b],
                ) - ((W_a + W_b) / 2.0)
            )
            weight_sum += weight
        end
    end

    return weight_sum > 0 ? total / weight_sum : NaN
end

@doc raw"""
    S_old(group_profiles, group_sizes)

Legacy alias for `overall_support_separation_old`. This is the historical
support-separation statistic based on average cross-group normalized Kendall
distance minus the mean within-group dispersion for unordered group pairs. It is
not the current derived excess-divergence diagnostic `S(C,D)`.

Groups with nonpositive size are skipped; fewer than two remaining groups return
`NaN`, and singleton groups are omitted from pair contributions because their
within-group dispersion is undefined.
"""
S_old(group_profiles, group_sizes) = overall_support_separation_old(group_profiles, group_sizes)

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

@doc raw"""
    overall_divergences(grouped_consensus, whole_df, key)

DataFrame adapter for `overall_divergence`. `grouped_consensus` must contain
`key` and one consensus column named `:consensus_result`, `:consensus_ranking`,
or `:x1`; `whole_df` must contain `key` and a `:profile` column accepted by
`strict_profile`. Groups are rebuilt from `whole_df`, and the consensus map is
read from `grouped_consensus`.

The returned value is directed aggregate divergence `D`, with the same
candidate-set, positive-mass, range, and one-group behavior as
`overall_divergence`.
"""
function overall_divergences(grouped_consensus::AbstractDataFrame,
                             whole_df::AbstractDataFrame,
                             key)
    consensus_col = _consensus_column(grouped_consensus)
    group_profiles = _group_profiles_from_dataframe(grouped_consensus, whole_df, key)
    consensus_map = Dict(row[key] => row[consensus_col] for row in eachrow(grouped_consensus))
    return overall_divergence(group_profiles, consensus_map)
end

@doc raw"""
    overall_overlaps(grouped_consensus, whole_df, key)

DataFrame adapter for `overall_overlap`. Groups are reconstructed from
`whole_df[key]` and `whole_df.profile`; consensus columns in `grouped_consensus`
are ignored except for group labels. The result is aggregate exact ranking
overlap `O` in `[0, 1]`, with the same zero-mass and candidate-set requirements
as `overall_overlap`.
"""
function overall_overlaps(grouped_consensus::AbstractDataFrame,
                          whole_df::AbstractDataFrame,
                          key)
    group_profiles = _group_profiles_from_dataframe(grouped_consensus, whole_df, key)
    return overall_overlap(group_profiles)
end

@doc raw"""
    overall_overlaps_smoothed(grouped_consensus, whole_df, key)

DataFrame adapter for `overall_overlap_smoothed`. It rebuilds group profiles
from `whole_df` and returns radius-1 Kendall-smoothed aggregate overlap
`O_smoothed`, using the same group weights and zero-mass conventions as
`overall_overlap_smoothed`.
"""
function overall_overlaps_smoothed(grouped_consensus::AbstractDataFrame,
                                   whole_df::AbstractDataFrame,
                                   key)
    group_profiles = _group_profiles_from_dataframe(grouped_consensus, whole_df, key)
    return overall_overlap_smoothed(group_profiles)
end

@doc raw"""
    overall_divergences_median(grouped_consensus, whole_df, key)

DataFrame adapter for `overall_divergence_median`. Consensus values are read
from `:consensus_result`, `:consensus_ranking`, or `:x1`; group profiles are
rebuilt from `whole_df`. The result is symmetric median-set divergence
`D_median` in `[0, 1]`, with the same zero-mass and candidate-set conventions as
`overall_divergence_median`.
"""
function overall_divergences_median(grouped_consensus::AbstractDataFrame,
                                    whole_df::AbstractDataFrame,
                                    key)
    consensus_col = _consensus_column(grouped_consensus)
    group_profiles = _group_profiles_from_dataframe(grouped_consensus, whole_df, key)
    consensus_map = Dict(row[key] => row[consensus_col] for row in eachrow(grouped_consensus))
    return overall_divergence_median(group_profiles, consensus_map)
end

@doc raw"""
    overall_separations(grouped_consensus, whole_df, key)

DataFrame adapter for `overall_separation`. Consensus values are read from
`grouped_consensus`, group profiles from `whole_df`, and the returned aggregate
`Sep` combines median-set distance with exact ranking non-overlap. Range,
zero-mass behavior, and candidate-set checks match `overall_separation`.
"""
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
    O_smoothed = overall_overlaps_smoothed(consensus, df, demo)
    Sep = overall_separations(consensus, df, demo)
    Gsep = grouped_gsep(C, Sep)
    group_profiles = _group_profiles_from_dataframe(consensus, df, demo)
    group_sizes = OrderedDict{Any,Float64}()

    for (group, profile) in pairs(group_profiles)
        group_sizes[group] = Float64(_profile_mass(profile))
    end

    W = within_dispersion_from_group_coherence(C)
    cleaned_S = overall_sstar_from_CD(C, D)
    normalized_S = normalized_consensus_separation(W, D)
    support_separation_S_old = overall_support_separation_old(group_profiles, group_sizes)
    return (
        C = C,
        W = W,
        D = D,
        D_median = D_median,
        O = O,
        O_smoothed = O_smoothed,
        Sep = Sep,
        Gsep = Gsep,
        S = cleaned_S,
        E = normalized_S,
        S_old = support_separation_S_old,
    )
end

@doc raw"""
    compute_group_metrics(df, demo; cache=GLOBAL_LINEAR_ORDER_CACHE, rng=nothing, tie_break_context=nothing)

Compute the pair `(C, D)` for groups defined by column `demo` in
`df`. Each row's `:profile` entry is interpreted as a strict ranking dictionary
through `strict_profile`; a Kendall consensus is computed for each group.

`C = sum_g π_g C_g` is weighted group coherence, where `C_g = 1 - W_g`; `D` is
`overall_divergence` from each group to outgroup consensuses. Both are on
`[0, 1]` when the grouped inputs have at least two candidates and positive
mass. Empty groups are absent from the DataFrames produced by `groupby`;
zero-mass or malformed profile inputs throw downstream errors.

Interpretation: this function returns only the compact `(C, D)` pair even though
the internal detail computation also derives `S` and `E` from the same `C`-`D`
decomposition, plus median divergence, overlap, experimental separation
diagnostics, and legacy `S_old`.
"""
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

@doc raw"""
    bootstrap_group_metrics(bt_profiles, demo; rng=nothing, tie_break_context=nothing)

Compute grouped consensus metrics for bootstrap replicates. `bt_profiles` maps
variant symbols to iterable replicate DataFrames, each accepted by
`compute_group_metrics`'s internal detail path. The result maps each variant to
a dictionary of vectors for `:C`, `:D`, `:D_median`, `:O`, `:O_smoothed`,
`:Sep`, `:Gsep`, `:W`, `:S`, `:E`, and `:S_old`.

Each replicate follows the same mathematical definitions, input restrictions,
normalizations, and error behavior as the corresponding non-bootstrap group
metrics.

Interpretation: this preserves the replicate distribution of coherence,
divergence, derived `C`-`D` excess diagnostics, overlap, experimental separation
quantities, and legacy `S_old` for uncertainty summaries.
"""
function bootstrap_group_metrics(bt_profiles, demo;
                                 rng = nothing,
                                 tie_break_context = nothing)
    result = Dict{Symbol, Dict{Symbol, Vector{Float64}}}()

    for (variant, reps) in bt_profiles
        Cvals = Float64[]
        Dvals = Float64[]
        Dmedianvals = Float64[]
        Ovals = Float64[]
        Osmoothedvals = Float64[]
        Sepvals = Float64[]
        Gsepvals = Float64[]
        Wvals = Float64[]
        Svals = Float64[]
        Evals = Float64[]
        Soldvals = Float64[]

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
            push!(Osmoothedvals, details.O_smoothed)
            push!(Sepvals, details.Sep)
            push!(Gsepvals, details.Gsep)
            push!(Wvals, details.W)
            push!(Svals, details.S)
            push!(Evals, details.E)
            push!(Soldvals, details.S_old)
        end

        result[variant] = Dict(
            :C => Cvals,
            :D => Dvals,
            :D_median => Dmedianvals,
            :O => Ovals,
            :O_smoothed => Osmoothedvals,
            :Sep => Sepvals,
            :Gsep => Gsepvals,
            :W => Wvals,
            :S => Svals,
            :E => Evals,
            :S_old => Soldvals,
        )
    end

    return result
end
