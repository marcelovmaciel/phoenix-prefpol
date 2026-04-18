# PreferenceLinearization.jl

abstract type AbstractWeakOrderLinearizer end

mutable struct PatternConditionalLinearizer{P} <: AbstractWeakOrderLinearizer
    reference::P
    alpha::Float64
    fallback::Symbol
    cache::Dict{Any,Tuple{Vector{Vector{Int}},Vector{Float64}}}
end

function PatternConditionalLinearizer(reference; alpha::Real = 0.5, fallback::Symbol = :uniform)
    (isfinite(alpha) && alpha > 0) || throw(ArgumentError(
        "PatternConditionalLinearizer requires alpha > 0.",
    ))
    fallback in (:uniform, :error) || throw(ArgumentError(
        "PatternConditionalLinearizer fallback must be :uniform or :error.",
    ))

    strict_reference = _strict_reference_profile(reference)
    validate(strict_reference)

    return PatternConditionalLinearizer{typeof(strict_reference)}(
        strict_reference,
        Float64(alpha),
        fallback,
        Dict{Any,Tuple{Vector{Vector{Int}},Vector{Float64}}}(),
    )
end

function _strict_reference_profile(reference)
    throw(ArgumentError(
        "PatternConditionalLinearizer reference must be a Profile or WeightedProfile of StrictRank or WeakRank ballots.",
    ))
end

@inline _strict_reference_profile(reference::Profile{<:StrictRank}) = reference
@inline _strict_reference_profile(reference::WeightedProfile{<:StrictRank}) = reference

@inline function _strict_from_complete_weakrank(x::WeakRank)
    return to_strict(x; tie_break = :error, incomplete_policy = :error)
end

function _strict_reference_profile(reference::Profile{<:WeakRank})
    strict_ballots = StrictRank[]

    for ballot in reference.ballots
        _is_complete_strict_weakrank(ballot) || continue
        push!(strict_ballots, _strict_from_complete_weakrank(ballot))
    end

    isempty(strict_ballots) && throw(ArgumentError(
        "PatternConditionalLinearizer requires at least one complete strict ballot when coercing a weak reference profile.",
    ))

    return Profile(reference.pool, strict_ballots)
end

function _strict_reference_profile(reference::WeightedProfile{<:WeakRank})
    strict_ballots = StrictRank[]
    kept_weights = eltype(reference.weights)[]

    for (ballot, weight) in zip(reference.ballots, reference.weights)
        _is_complete_strict_weakrank(ballot) || continue
        push!(strict_ballots, _strict_from_complete_weakrank(ballot))
        push!(kept_weights, weight)
    end

    isempty(strict_ballots) && throw(ArgumentError(
        "PatternConditionalLinearizer requires at least one complete strict ballot when coercing a weak reference profile.",
    ))

    return WeightedProfile(Profile(reference.pool, strict_ballots), kept_weights)
end

function _is_complete_strict_weakrank(x::WeakRank)
    rx = ranks(x)
    any(ismissing, rx) && return false
    seen = Set{Int}()
    for rk in rx
        value = rk::Int
        value in seen && return false
        push!(seen, value)
    end
    return true
end

@inline _weak_bucket_key(x::WeakRank) = Tuple(Tuple(level) for level in to_weakorder(x))

function _has_ties(x::WeakRank)
    return any(length(level) > 1 for level in to_weakorder(x))
end

function _enumerate_extensions(levels::Vector{Vector{Int}})
    exts = Vector{Vector{Int}}([Int[]])

    for level in levels
        bucket_perms = length(level) <= 1 ? [copy(level)] : [collect(p) for p in permutations(level)]
        next_exts = Vector{Vector{Int}}()
        sizehint!(next_exts, length(exts) * length(bucket_perms))

        for prefix in exts
            for bucket_perm in bucket_perms
                push!(next_exts, vcat(prefix, bucket_perm))
            end
        end

        exts = next_exts
    end

    return exts
end

function _perm_refines_levels(perm::AbstractVector{Int}, levels::Vector{Vector{Int}})
    total = sum(length, levels)
    length(perm) == total || return false

    pos = zeros(Int, total)
    @inbounds for (idx, id) in enumerate(perm)
        (1 <= id <= total) || return false
        pos[id] == 0 || return false
        pos[id] = idx
    end

    prev_max = 0
    for level in levels
        level_max = prev_max
        @inbounds for id in level
            p = pos[id]
            p > prev_max || return false
            level_max = max(level_max, p)
        end
        prev_max = level_max
    end

    return true
end

function _compatible_extension_distribution(reference::Profile{<:StrictRank},
                                            levels::Vector{Vector{Int}},
                                            alpha::Real,
                                            fallback::Symbol)
    exts = _enumerate_extensions(levels)
    masses = fill(Float64(alpha), length(exts))
    ext_index = Dict{Tuple{Vararg{Int}},Int}(Tuple(ext) => idx for (idx, ext) in enumerate(exts))
    compatible_mass = 0.0

    for ballot in reference.ballots
        ballot_perm = perm(ballot)
        _perm_refines_levels(ballot_perm, levels) || continue
        masses[ext_index[Tuple(ballot_perm)]] += 1.0
        compatible_mass += 1.0
    end

    if compatible_mass == 0.0
        fallback === :uniform || throw(ArgumentError(
            "PatternConditionalLinearizer found no compatible strict reference ballots for the requested weak-order pattern.",
        ))
        fill!(masses, 1.0)
    end

    return exts, masses
end

function _compatible_extension_distribution(reference::WeightedProfile{<:StrictRank},
                                            levels::Vector{Vector{Int}},
                                            alpha::Real,
                                            fallback::Symbol)
    exts = _enumerate_extensions(levels)
    masses = fill(Float64(alpha), length(exts))
    ext_index = Dict{Tuple{Vararg{Int}},Int}(Tuple(ext) => idx for (idx, ext) in enumerate(exts))
    compatible_mass = 0.0

    for (ballot, weight) in zip(reference.ballots, reference.weights)
        w = Float64(weight)
        (isfinite(w) && w >= 0) || throw(ArgumentError(
            "PatternConditionalLinearizer requires nonnegative finite reference weights.",
        ))
        ballot_perm = perm(ballot)
        _perm_refines_levels(ballot_perm, levels) || continue
        masses[ext_index[Tuple(ballot_perm)]] += w
        compatible_mass += w
    end

    if compatible_mass == 0.0
        fallback === :uniform || throw(ArgumentError(
            "PatternConditionalLinearizer found no compatible strict reference weight for the requested weak-order pattern.",
        ))
        fill!(masses, 1.0)
    end

    return exts, masses
end

function _sample_index(weights, rng::AbstractRNG)
    total = 0.0
    for weight in weights
        w = Float64(weight)
        w >= 0 || throw(ArgumentError("Sampling weights must be nonnegative."))
        total += w
    end
    total > 0 || throw(ArgumentError("Sampling weights must sum to a positive value."))

    threshold = rand(rng) * total
    partial = 0.0
    @inbounds for idx in eachindex(weights)
        partial += Float64(weights[idx])
        threshold <= partial && return idx
    end

    return lastindex(weights)
end

function _linearize_with_pattern_conditioning(x::WeakRank,
                                              model::PatternConditionalLinearizer,
                                              rng::AbstractRNG)
    is_complete(x) || throw(ArgumentError(
        "PatternConditionalLinearizer only supports complete weak orders; missing ranks are not allowed.",
    ))

    ballot_size = length(ranks(x))
    reference_size = length(model.reference.pool)
    ballot_size == reference_size || throw(ArgumentError(
        "PatternConditionalLinearizer reference profile has $reference_size candidates, but the weak rank has $ballot_size.",
    ))

    !_has_ties(x) && return _strict_from_complete_weakrank(x)

    key = _weak_bucket_key(x)
    exts, masses = get!(model.cache, key) do
        levels = to_weakorder(x)
        _compatible_extension_distribution(model.reference, levels, model.alpha, model.fallback)
    end

    return StrictRank(exts[_sample_index(masses, rng)])
end

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
    policy = _normalize_incomplete_policy(incomplete_policy)

    if tie_break isa AbstractWeakOrderLinearizer
        policy === :error || throw(ArgumentError(
            "Weak-order linearizers require incomplete_policy = :error.",
        ))
        return _linearize_with_pattern_conditioning(x, tie_break, rng)
    end

    normalized = _normalize_linearize_tie_break(tie_break, rng, pool)

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
