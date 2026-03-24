struct AnnotatedProfile{P}
    profile::P
    metadata::DataFrame
end

Base.length(bundle::AnnotatedProfile) = Preferences.nballots(bundle.profile)

const _PROFILE_ENCODING_RANK_VECTOR_V1 = "rank_vector_v1"

function _metadata_or(df::AbstractDataFrame, key::AbstractString, default)
    try
        return metadata(df, key)
    catch
        return default
    end
end

function _normalize_ballot_kind(ballot_kind)
    ballot_kind === nothing && return nothing

    kind = Symbol(ballot_kind)
    if kind in (:strict, :linear, :linearized)
        return :strict
    elseif kind === :weak
        return :weak
    end

    throw(ArgumentError(
        "Unsupported ballot_kind `$ballot_kind`. Use :strict or :weak.",
    ))
end

function _metadata_ballot_kind(df::AbstractDataFrame)
    raw = _metadata_or(df, "profile_kind", nothing)
    raw === nothing && return nothing
    return _normalize_ballot_kind(raw)
end

function _metadata_profile_encoding(df::AbstractDataFrame)
    raw = _metadata_or(df, "profile_encoding", nothing)
    raw === nothing && return nothing
    return String(raw)
end

function _candidate_syms_from_rankings(rankings; fallback = nothing)
    if fallback !== nothing
        return Symbol.(collect(fallback))
    end

    isempty(rankings) && throw(ArgumentError(
        "Cannot infer candidate labels from an empty ranking collection without metadata.",
    ))

    first_ranking = rankings[firstindex(rankings)]
    return sort!(collect(Symbol.(keys(first_ranking))); by = string)
end

function _ranking_is_strict(ranking::AbstractDict, m::Int)
    vals = Int[value for value in values(ranking)]
    length(vals) == m || return false
    length(unique(vals)) == m || return false
    return sort(vals) == collect(1:m)
end

function _strict_rank_from_dict(ranking::AbstractDict, pool)
    ordered = sort(collect(Symbol.(keys(ranking))); by = cand -> Int(ranking[cand]))
    return Preferences.StrictRank(pool, ordered)
end

function _weak_rank_from_dict(ranking::AbstractDict, pool)
    return Preferences.WeakRank(pool, Dict(Symbol(k) => Int(v) for (k, v) in ranking))
end

function _compact_rank_code(ballot)
    ranks_vec = Preferences.ranks(ballot)
    return ntuple(length(ranks_vec)) do i
        rk = ranks_vec[i]
        return ismissing(rk) ? UInt16(0) : UInt16(Int(rk))
    end
end

function _decode_compact_rank_code(code, kind::Symbol, pool)
    if kind === :strict
        ranks_vec = Int.(collect(code))
        sort(copy(ranks_vec)) == collect(1:length(ranks_vec)) || throw(ArgumentError(
            "Compact strict-profile artifact contains a non-strict ballot.",
        ))
        return Preferences.StrictRank(pool, sortperm(ranks_vec))
    end

    rank_values = Vector{Union{Missing,Int}}(undef, length(code))
    @inbounds for i in eachindex(code)
        rk = Int(code[i])
        rank_values[i] = rk == 0 ? missing : rk
    end
    return Preferences.WeakRank(pool, rank_values)
end

function compact_profile_artifact_dataframe(bundle::AnnotatedProfile;
                                            col::Symbol = :profile)
    bundle.profile isa Preferences.WeightedProfile && throw(ArgumentError(
        "Compact artifact encoding currently supports unweighted pseudo-profiles only.",
    ))

    pool_syms = collect(Preferences.candidates(bundle.profile.pool))
    n = Preferences.nballots(bundle.profile)
    m = length(pool_syms)
    T = NTuple{m,UInt16}
    encoded = Vector{T}(undef, n)

    @inbounds for i in 1:n
        encoded[i] = _compact_rank_code(bundle.profile.ballots[i])
    end

    df = hcat(DataFrame(col => PooledArray(encoded; compress = true)), copy(bundle.metadata))
    metadata!(df, "candidates", pool_syms)
    metadata!(df, "profile_kind", Preferences.is_strict(bundle.profile) ? "linearized" : "weak")
    metadata!(df, "profile_encoding", _PROFILE_ENCODING_RANK_VECTOR_V1)
    return df
end

function _compact_rank_profile_to_preferences(rankings;
                                              candidate_syms,
                                              ballot_kind)
    candidate_syms === nothing && throw(ArgumentError(
        "Compact profile artifacts require candidate metadata.",
    ))

    kind = _normalize_ballot_kind(ballot_kind)
    pool = Preferences.CandidatePool(Symbol.(collect(candidate_syms)))
    ballots = if kind === :strict
        isempty(rankings) ? Preferences.StrictRank[] :
            [_decode_compact_rank_code(rankings[i], kind, pool) for i in eachindex(rankings)]
    else
        isempty(rankings) ? Preferences.WeakRank[] :
            [_decode_compact_rank_code(rankings[i], kind, pool) for i in eachindex(rankings)]
    end

    return Preferences.Profile(pool, ballots)
end

function dict_profile_to_preferences(profile::AbstractVector{<:AbstractDict};
                                     candidate_syms = nothing,
                                     ballot_kind = nothing)
    cand_syms = _candidate_syms_from_rankings(profile; fallback = candidate_syms)
    pool = Preferences.CandidatePool(cand_syms)
    kind = _normalize_ballot_kind(ballot_kind)
    kind === nothing && throw(ArgumentError(
        "Pass `ballot_kind = :strict` or `:weak`, or attach DataFrame metadata `profile_kind` before conversion.",
    ))

    ballots = if kind === :strict
        isempty(profile) ? Preferences.StrictRank[] :
            [_strict_rank_from_dict(ranking, pool) for ranking in profile]
    elseif kind === :weak
        isempty(profile) ? Preferences.WeakRank[] :
            [_weak_rank_from_dict(ranking, pool) for ranking in profile]
    end

    return Preferences.Profile(pool, ballots)
end

function dataframe_to_annotated_profile(df::AbstractDataFrame;
                                        col::Symbol = :profile,
                                        ballot_kind = nothing)
    hasproperty(df, col) || throw(ArgumentError("DataFrame is missing profile column `$col`."))
    df_local = df isa DataFrame ? df : DataFrame(df)
    rankings = df_local[!, col]
    cand_meta = _metadata_or(df, "candidates", nothing)
    kind = something(_normalize_ballot_kind(ballot_kind), _metadata_ballot_kind(df))
    kind === nothing && throw(ArgumentError(
        "Ambiguous profile conversion: pass `ballot_kind = :strict` or `:weak`, " *
        "or attach DataFrame metadata `profile_kind`.",
    ))

    profile = if _metadata_profile_encoding(df) == _PROFILE_ENCODING_RANK_VECTOR_V1
        _compact_rank_profile_to_preferences(
            rankings;
            candidate_syms = cand_meta,
            ballot_kind = kind,
        )
    else
        dict_profile_to_preferences(
            rankings;
            candidate_syms = cand_meta,
            ballot_kind = kind,
        )
    end

    meta_df = select(df_local, Not(col))
    return AnnotatedProfile(profile, meta_df)
end

function profile_to_ranking_dicts(profile::Union{Preferences.Profile,Preferences.WeightedProfile})
    return [Preferences.asdict(ballot, profile.pool) for ballot in profile.ballots]
end

function annotated_profile_to_dataframe(bundle::AnnotatedProfile;
                                        col::Symbol = :profile)
    rankings = profile_to_ranking_dicts(bundle.profile)
    df = hcat(DataFrame(col => rankings), copy(bundle.metadata))
    metadata!(df, "candidates", collect(Preferences.candidates(bundle.profile.pool)))
    metadata!(df, "profile_kind", Preferences.is_strict(bundle.profile) ? "linearized" : "weak")
    return df
end

function linearize_annotated_profile(bundle::AnnotatedProfile;
                                     tie_break = :random,
                                     rng = Random.GLOBAL_RNG)
    return AnnotatedProfile(
        Preferences.linearize(
            bundle.profile;
            tie_break = tie_break,
            rng = rng,
            incomplete_policy = :error,
        ),
        copy(bundle.metadata),
    )
end

@inline annotated_profile(bundle::AnnotatedProfile; kwargs...) = bundle

function annotated_profile(df::AbstractDataFrame; col::Symbol = :profile, ballot_kind = nothing)
    return dataframe_to_annotated_profile(df; col = col, ballot_kind = ballot_kind)
end

function _subset_profile(profile::Preferences.Profile, idxs)
    return Preferences.Profile(profile.pool, profile.ballots[idxs])
end

function _subset_profile(profile::Preferences.WeightedProfile, idxs)
    inner = Preferences.Profile(profile.pool, profile.ballots[idxs])
    return Preferences.WeightedProfile(inner, profile.weights[idxs])
end

function subset_annotated_profile(bundle::AnnotatedProfile, idxs)
    idxv = collect(idxs)
    return AnnotatedProfile(
        _subset_profile(bundle.profile, idxv),
        bundle.metadata[idxv, :],
    )
end

function strict_profile(x::Preferences.Profile)
    Preferences.is_strict(x) || throw(ArgumentError("Expected a strict Preferences.Profile input."))
    return x
end

function strict_profile(x::Preferences.WeightedProfile)
    Preferences.is_strict(x) || throw(ArgumentError("Expected a strict Preferences.WeightedProfile input."))
    return x
end

strict_profile(x::AnnotatedProfile) = strict_profile(x.profile)

function strict_profile(df::AbstractDataFrame)
    return strict_profile(dataframe_to_annotated_profile(df; ballot_kind = :strict))
end

function strict_profile(profile::AbstractVector{<:AbstractDict}; candidate_syms = nothing)
    return strict_profile(
        dict_profile_to_preferences(
            profile;
            candidate_syms = candidate_syms,
            ballot_kind = :strict,
        ),
    )
end
