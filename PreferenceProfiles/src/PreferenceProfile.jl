# PreferenceProfile.jl

##############################
# Profile type
##############################

"""
    Profile(pool::CandidatePool, ballots::AbstractVector) -> Profile

Finite profile of ballots over a common candidate pool. The domain is a
`CandidatePool` and a vector of ballots; the return value is a `Profile{B}` with
uniform concrete ballot type `B`.

The representation invariant is that every ballot has the same candidate count
as `pool`, and candidate IDs inside ballots are implementation-level positions
in that common pool. `Profile` does not attach weights. Missing and tie behavior
is inherited from the ballot representation. Construction rejects mixed concrete
ballot types and pool-size mismatches with `ArgumentError`.

Example:

```julia
using PreferenceProfiles

pool = CandidatePool([:a, :b, :c])
p = Profile(pool, [
    StrictRank(pool, [:a, :b, :c]),
    StrictRank(pool, [:b, :a, :c]),
])

nballots(p)
pretty_profile_table(p)
```
"""
struct Profile{B}
    pool::CandidatePool
    ballots::Vector{B}
    function Profile(pool::CandidatePool, ballots::AbstractVector)
        bvec, btype = _normalize_ballots(ballots)

        n_pool = length(pool)
        for b in bvec
            _ballot_n(b) == n_pool || throw(ArgumentError("Ballot size mismatch with pool"))
        end

        return new{btype}(pool, bvec)
    end
end

"""
    WeightedProfile(pool::CandidatePool, ballots::AbstractVector, weights::AbstractVector) -> WeightedProfile
    WeightedProfile(profile::Profile, weights::AbstractVector) -> WeightedProfile

Finite weighted profile over a common candidate pool. The domain is a profile or
pool plus ballots and a weight vector; the return value is a `WeightedProfile`
with survey or population weights attached to ballots.

The representation invariant is that ballots are stored once and weights are
parallel numeric entries, not replicated rows. Candidate IDs remain positions in
the common pool. Construction checks weight element type is `<: Real` and length
matches ballots; `validate(...; strict=true)` checks finite nonnegative weights.
Missing and tie behavior is inherited from the ballots.

Example:

```julia
using PreferenceProfiles

pool = CandidatePool([:a, :b, :c])
ballots = [
    StrictRank(pool, [:a, :b, :c]),
    StrictRank(pool, [:c, :b, :a]),
]
wp = WeightedProfile(pool, ballots, [0.75, 0.25])

weights(wp)
total_weight(wp)
```
"""
struct WeightedProfile{B,W<:Real}
    pool::CandidatePool
    ballots::Vector{B}
    weights::Vector{W}
    function WeightedProfile(pool::CandidatePool, ballots::AbstractVector, weights::AbstractVector)
        profile = Profile(pool, ballots)
        Wt = eltype(weights)
        Wt <: Real || throw(ArgumentError("weights must have eltype <: Real"))
        wvec = Vector{Wt}(weights)
        length(wvec) == nballots(profile) || throw(ArgumentError("weights length must match ballots length"))
        return new{eltype(profile),Wt}(profile.pool, profile.ballots, wvec)
    end
end

##############################
# Constructors
##############################

function Profile{B}(pool::CandidatePool, ballots::AbstractVector) where {B}
    return Profile(pool, Vector{B}(ballots))
end

function WeightedProfile(profile::Profile{B}, weights::AbstractVector) where {B}
    return WeightedProfile(profile.pool, profile.ballots, weights)
end

##############################
# Core API
##############################

"""
    nballots(p::Union{Profile,WeightedProfile}) -> Int

Return the number of stored ballot rows. The domain is a profile; the return
type is `Int`.

The invariant is that this counts rows, not total survey/population weight.
Missing, ties, and weights do not affect the count.
"""
@inline nballots(p::Profile) = length(p.ballots)
@inline Base.length(p::Profile) = nballots(p)
@inline nballots(p::WeightedProfile) = length(p.ballots)
@inline Base.length(p::WeightedProfile) = nballots(p)

Base.eltype(::Type{Profile{B}}) where {B} = B
Base.eltype(p::Profile{B}) where {B} = B
Base.eltype(::Type{WeightedProfile{B,W}}) where {B,W} = B
Base.eltype(p::WeightedProfile{B,W}) where {B,W} = B

"""
    weights(p::WeightedProfile) -> AbstractVector{<:Real}

Return the stored weight vector for a weighted profile. The domain is a
`WeightedProfile`; the return value is the profile's weight storage.

The invariant is `weights(p)[i]` is the survey or population weight for
`p.ballots[i]`, not a replication count. The accessor returns storage, not a
copy. Missing and tie behavior belongs to the ballots. Strict validation of
finite nonnegative weights is performed by `validate`, not this accessor.
"""
@inline weights(p::WeightedProfile) = p.weights

"""
    total_weight(p::WeightedProfile) -> Real

Return the sum of stored profile weights. The domain is a `WeightedProfile`; the
return type is the result of `sum(p.weights)`.

The invariant is that total weight is aggregate survey/population mass, not the
number of replicated rows. Missing and ties do not affect the sum. Invalid
weights are not rejected here; use `validate(p; strict=true)` for finite
nonnegative checks.
"""
@inline total_weight(p::WeightedProfile) = sum(p.weights)

"""
    validate(p::Union{Profile,WeightedProfile}; strict=true) -> Bool

Validate profile representation invariants and return `true`. The domain is a
profile; the return type is `Bool`.

For `Profile`, strict validation checks every ballot size matches the common
candidate pool. For `WeightedProfile`, validation always checks weight length;
strict validation also checks finite nonnegative weights and ballot sizes.
Missing ranks and ties are valid when the ballot type allows them. Violations
throw `ArgumentError`.
"""
function validate(p::Profile; strict::Bool=true)
    if strict
        n_pool = length(p.pool)
        @inbounds for b in p.ballots
            _ballot_n(b) == n_pool || throw(ArgumentError("Ballot size mismatch with pool"))
        end
    end
    return true
end

function validate(p::WeightedProfile; strict::Bool=true)
    n = nballots(p)
    length(p.weights) == n || throw(ArgumentError("weights length must match ballots length"))
    if strict
        @inbounds for w in p.weights
            (isfinite(w) && w ≥ 0) || throw(ArgumentError("weights must be finite and ≥ 0"))
        end
        n_pool = length(p.pool)
        @inbounds for b in p.ballots
            _ballot_n(b) == n_pool || throw(ArgumentError("Ballot size mismatch with pool"))
        end
    end
    return true
end

##############################
# Bootstrap utilities
##############################

"""
    resample_indices(p::Union{Profile,WeightedProfile}; rng=Random.GLOBAL_RNG,
                     n=nballots(p)) -> Vector{Int}

Sample ballot row indices with replacement. The domain is a profile and sample
size; the return type is `Vector{Int}`.

For unweighted profiles, rows are sampled uniformly. For weighted profiles, rows
are sampled with probability proportional to stored survey/population weights;
weights are not treated as replicated rows. Missing and tie behavior does not
affect sampling. Negative `n`, negative weights, mismatched weight length, or
zero total weight throw `ArgumentError`; empty profiles return `Int[]`.
"""
function resample_indices(p::Profile; rng::AbstractRNG=Random.GLOBAL_RNG,
                          n::Integer=nballots(p))
    n ≥ 0 || throw(ArgumentError("n must be ≥ 0"))
    N = nballots(p)
    N == 0 && return Int[]

    return rand(rng, 1:N, Int(n))
end

function resample_indices(p::WeightedProfile; rng::AbstractRNG=Random.GLOBAL_RNG,
                          n::Integer=nballots(p))
    n ≥ 0 || throw(ArgumentError("n must be ≥ 0"))
    N = nballots(p)
    N == 0 && return Int[]

    weights = p.weights
    length(weights) == N || throw(ArgumentError("weights length must match ballots length"))
    @inbounds for w in weights
        w ≥ 0 || throw(ArgumentError("weights must be ≥ 0"))
    end
    total = sum(weights)
    total > 0 || throw(ArgumentError("weights must sum to > 0"))

    cdf = cumsum(float.(weights))
    idx = Vector{Int}(undef, Int(n))
    @inbounds for k in eachindex(idx)
        u = rand(rng) * cdf[end]
        idx[k] = searchsortedfirst(cdf, u)
    end
    return idx
end

"""
    bootstrap(p::Union{Profile,WeightedProfile}; rng=Random.GLOBAL_RNG,
              n=nballots(p)) -> Profile

Return a bootstrap resample of ballot rows with replacement. The domain is a
profile and sample size; the return type is an unweighted `Profile`.

The invariant is that the returned profile uses the same candidate pool and
contains sampled ballot objects. For a `WeightedProfile`, sampling probabilities
are proportional to weights, but the returned object is not weighted; weights
guide sampling rather than becoming replicated rows. Missing and ties are
preserved in sampled ballots. Sampling errors are those of `resample_indices`.
"""
function bootstrap(p::Profile; rng::AbstractRNG=Random.GLOBAL_RNG,
                   n::Integer=nballots(p))
    idx = resample_indices(p; rng=rng, n=n)
    new_ballots = p.ballots[idx]
    return Profile(p.pool, new_ballots)
end

function bootstrap(p::WeightedProfile; rng::AbstractRNG=Random.GLOBAL_RNG,
                   n::Integer=nballots(p))
    idx = resample_indices(p; rng=rng, n=n)
    new_ballots = p.ballots[idx]
    return Profile(p.pool, new_ballots)
end

"""
    bootstrap_counts(p::Union{Profile,WeightedProfile}; rng=Random.GLOBAL_RNG,
                     n=nballots(p)) -> Vector{Int}

Return row-selection counts for a bootstrap draw. The domain is a profile and
sample size; the return type is a `Vector{Int}` with one count per stored ballot
row.

The invariant is that counts are produced by sampling row indices with
replacement. Weighted profiles use weights as sampling probabilities, not as
pre-expanded rows. Missing and tie behavior does not affect counts. Sampling
errors are those of `resample_indices`.
"""
function bootstrap_counts(p::Profile; rng::AbstractRNG=Random.GLOBAL_RNG,
                          n::Integer=nballots(p))
    idx = resample_indices(p; rng=rng, n=n)
    counts = zeros(Int, nballots(p))
    @inbounds for i in idx
        counts[i] += 1
    end
    return counts
end

function bootstrap_counts(p::WeightedProfile; rng::AbstractRNG=Random.GLOBAL_RNG,
                          n::Integer=nballots(p))
    idx = resample_indices(p; rng=rng, n=n)
    counts = zeros(Int, nballots(p))
    @inbounds for i in idx
        counts[i] += 1
    end
    return counts
end

##############################
# Restriction
##############################

"""
    restrict(p::Union{Profile,WeightedProfile}, subset_syms::AbstractVector{Symbol}) -> (new_profile, new_pool, backmap)

Restrict every ballot in a profile to a candidate subset. The domain is a
profile and subset symbols in the desired new order; the return value is the
restricted profile, a new `CandidatePool`, and `backmap::Vector{Int}`.

The invariant is `backmap[new_id] == old_id`, relating candidate positions in
the returned pool to positions in the original pool. Ballot-level restriction
semantics apply: strict orders preserve relative order, weak ranks renormalize
present rank levels and preserve missingness/ties, and pairwise ballots preserve
the selected submatrix. Weighted profiles copy their weights; weights are
survey/population mass and are not replicated rows. Unknown or duplicate subset
symbols throw through pool construction/lookup.
"""
function restrict(p::Profile, subset_syms::AbstractVector{Symbol})
    n = nballots(p)
    if n == 0
        new_pool = CandidatePool(subset_syms)
        backmap = Int[ p.pool[s] for s in subset_syms ]
        new_profile = Profile(new_pool, Vector{eltype(p)}())
        return (new_profile, new_pool, backmap)
    end

    first_ballot, new_pool, backmap = restrict(p.ballots[1], p.pool, subset_syms)
    new_ballots = Vector{typeof(first_ballot)}(undef, n)
    new_ballots[1] = first_ballot

    @inbounds for i in 2:n
        new_ballots[i], _, _ = restrict(p.ballots[i], p.pool, subset_syms)
    end

    new_profile = Profile(new_pool, new_ballots)
    return (new_profile, new_pool, backmap)
end

function restrict(p::WeightedProfile, subset_syms::AbstractVector{Symbol})
    n = nballots(p)
    if n == 0
        new_pool = CandidatePool(subset_syms)
        backmap = Int[ p.pool[s] for s in subset_syms ]
        new_profile = Profile(new_pool, Vector{eltype(p)}())
        new_weights = copy(p.weights)
        return (WeightedProfile(new_profile, new_weights), new_pool, backmap)
    end

    first_ballot, new_pool, backmap = restrict(p.ballots[1], p.pool, subset_syms)
    new_ballots = Vector{typeof(first_ballot)}(undef, n)
    new_ballots[1] = first_ballot

    @inbounds for i in 2:n
        new_ballots[i], _, _ = restrict(p.ballots[i], p.pool, subset_syms)
    end

    new_profile = Profile(new_pool, new_ballots)
    new_weights = copy(p.weights)
    return (WeightedProfile(new_profile, new_weights), new_pool, backmap)
end

##############################
# Trait helpers
##############################

is_complete(p::Profile) = all(is_complete, p.ballots)
is_strict(p::Profile) = all(is_strict, p.ballots)
is_weak_order(p::Profile) = all(is_weak_order, p.ballots)
is_transitive(p::Profile) = all(is_transitive, p.ballots)
is_complete(p::WeightedProfile) = all(is_complete, p.ballots)
is_strict(p::WeightedProfile) = all(is_strict, p.ballots)
is_weak_order(p::WeightedProfile) = all(is_weak_order, p.ballots)
is_transitive(p::WeightedProfile) = all(is_transitive, p.ballots)

##############################
# Internal helpers
##############################

_ballot_n(x::WeakRank) = length(ranks(x))
_ballot_n(x::StrictRank) = length(ranks(x))
_ballot_n(x::StrictRankMutable) = length(to_perm(x))
_ballot_n(x) = length(to_perm(x))

function _normalize_ballots(ballots::AbstractVector{B}) where {B}
    if isempty(ballots)
        bvec = ballots isa Vector{B} ? ballots : Vector{B}(ballots)
        return bvec, B
    end
    first_ballot = ballots[firstindex(ballots)]
    btype = typeof(first_ballot)
    for b in ballots
        typeof(b) == btype || throw(ArgumentError("Ballots must have a uniform concrete type"))
    end
    bvec = ballots isa Vector{btype} ? ballots : Vector{btype}(ballots)
    return bvec, btype
end
