# PreferenceProfile.jl

##############################
# Profile type
##############################

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

@inline nballots(p::Profile) = length(p.ballots)
@inline Base.length(p::Profile) = nballots(p)
@inline nballots(p::WeightedProfile) = length(p.ballots)
@inline Base.length(p::WeightedProfile) = nballots(p)

Base.eltype(::Type{Profile{B}}) where {B} = B
Base.eltype(p::Profile{B}) where {B} = B
Base.eltype(::Type{WeightedProfile{B,W}}) where {B,W} = B
Base.eltype(p::WeightedProfile{B,W}) where {B,W} = B

@inline weights(p::WeightedProfile) = p.weights
@inline total_weight(p::WeightedProfile) = sum(p.weights)

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
