# PreferenceLinearization.jl

@inline linearize(x::StrictRank; kwargs...) = x

function _normalize_linearize_tie_break(tie_break, rng::AbstractRNG, pool)
    if tie_break isa Symbol && tie_break ∉ (:error, :random)
        return make_rank_bucket_linearizer(tie_break; rng = rng, pool = pool)
    end
    return tie_break
end

function linearize(x::WeakRank;
                   tie_break = :random,
                   rng::AbstractRNG = Random.GLOBAL_RNG,
                   pool::Union{Nothing,CandidatePool} = nothing)
    normalized = _normalize_linearize_tie_break(tie_break, rng, pool)
    return to_strict(x; tie_break = normalized, rng = rng, pool = pool)
end

@inline linearize(p::Profile{<:StrictRank}; kwargs...) = p
@inline linearize(p::WeightedProfile{<:StrictRank}; kwargs...) = p

function linearize(p::Profile{<:WeakRank};
                   tie_break = :random,
                   rng::AbstractRNG = Random.GLOBAL_RNG)
    n = nballots(p)

    if n == 0
        return Profile(p.pool, StrictRank[])
    end

    first_ballot = linearize(p.ballots[1]; tie_break = tie_break, rng = rng, pool = p.pool)
    strict_ballots = Vector{typeof(first_ballot)}(undef, n)
    strict_ballots[1] = first_ballot

    @inbounds for i in 2:n
        strict_ballots[i] = linearize(
            p.ballots[i];
            tie_break = tie_break,
            rng = rng,
            pool = p.pool,
        )
    end

    return Profile(p.pool, strict_ballots)
end

function linearize(p::WeightedProfile{<:WeakRank};
                   tie_break = :random,
                   rng::AbstractRNG = Random.GLOBAL_RNG)
    strict_profile = linearize(
        Profile(p.pool, p.ballots);
        tie_break = tie_break,
        rng = rng,
    )
    return WeightedProfile(strict_profile, copy(weights(p)))
end
