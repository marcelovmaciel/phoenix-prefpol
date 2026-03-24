# PreferenceLinearization.jl

@inline linearize(x::StrictRank; kwargs...) = x

function _normalize_incomplete_policy(incomplete_policy)
    policy = Symbol(incomplete_policy)
    policy in (:error, :preserve, :complete) || throw(ArgumentError(
        "Unsupported incomplete_policy `$incomplete_policy`. Use :error, :preserve, or :complete.",
    ))
    return policy
end

function _normalize_linearize_tie_break(tie_break, rng::AbstractRNG, pool)
    if tie_break isa Symbol && tie_break ∉ (:error, :random)
        return make_rank_bucket_linearizer(tie_break; rng = rng, pool = pool)
    end
    return tie_break
end

function _linearize_weakrank_preserve(x::WeakRank,
                                      tie_break,
                                      rng::AbstractRNG,
                                      pool::Union{Nothing,CandidatePool})
    rx = ranks(x)
    buckets = Dict{Int,Vector{Int}}()
    unranked = Int[]

    @inbounds for id in eachindex(rx)
        rk = rx[id]
        if ismissing(rk)
            push!(unranked, id)
        else
            push!(get!(buckets, rk::Int, Int[]), id)
        end
    end

    linearize_bucket = _resolve_bucket_linearizer(tie_break, rng, pool, rx)
    out = Vector{Union{Missing,Int}}(undef, length(rx))
    fill!(out, missing)
    next_rank = 1

    for rk in sort!(collect(keys(buckets)))
        grp = buckets[rk]
        ordered = length(grp) == 1 ? grp : linearize_bucket(grp)
        @inbounds for id in ordered
            out[id] = next_rank
            next_rank += 1
        end
    end

    return WeakRank(out)
end

function linearize(x::WeakRank;
                   tie_break = :random,
                   rng::AbstractRNG = Random.GLOBAL_RNG,
                   pool::Union{Nothing,CandidatePool} = nothing,
                   incomplete_policy::Symbol = :error)
    normalized = _normalize_linearize_tie_break(tie_break, rng, pool)
    policy = _normalize_incomplete_policy(incomplete_policy)

    if policy === :preserve
        return _linearize_weakrank_preserve(x, normalized, rng, pool)
    end

    return to_strict(
        x;
        tie_break = normalized,
        rng = rng,
        pool = pool,
        incomplete_policy = policy,
    )
end

@inline linearize(p::Profile{<:StrictRank}; kwargs...) = p
@inline linearize(p::WeightedProfile{<:StrictRank}; kwargs...) = p

function linearize(p::Profile{<:WeakRank};
                   tie_break = :random,
                   rng::AbstractRNG = Random.GLOBAL_RNG,
                   incomplete_policy::Symbol = :error)
    n = nballots(p)
    policy = _normalize_incomplete_policy(incomplete_policy)

    if n == 0
        empty_ballots = policy === :preserve ? WeakRank[] : StrictRank[]
        return Profile(p.pool, empty_ballots)
    end

    first_ballot = linearize(
        p.ballots[1];
        tie_break = tie_break,
        rng = rng,
        pool = p.pool,
        incomplete_policy = policy,
    )
    strict_ballots = Vector{typeof(first_ballot)}(undef, n)
    strict_ballots[1] = first_ballot

    @inbounds for i in 2:n
        strict_ballots[i] = linearize(
            p.ballots[i];
            tie_break = tie_break,
            rng = rng,
            pool = p.pool,
            incomplete_policy = policy,
        )
    end

    return Profile(p.pool, strict_ballots)
end

function linearize(p::WeightedProfile{<:WeakRank};
                   tie_break = :random,
                   rng::AbstractRNG = Random.GLOBAL_RNG,
                   incomplete_policy::Symbol = :error)
    strict_profile = linearize(
        Profile(p.pool, p.ballots);
        tie_break = tie_break,
        rng = rng,
        incomplete_policy = incomplete_policy,
    )
    return WeightedProfile(strict_profile, copy(weights(p)))
end
