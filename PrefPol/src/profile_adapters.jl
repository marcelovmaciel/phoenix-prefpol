struct AnnotatedProfile{P}
    profile::P
    metadata::DataFrame
end

Base.length(bundle::AnnotatedProfile) = Preferences.nballots(bundle.profile)

function _metadata_or(df::AbstractDataFrame, key::AbstractString, default)
    try
        return metadata(df, key)
    catch
        return default
    end
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

function dict_profile_to_preferences(profile::AbstractVector{<:AbstractDict};
                                     candidate_syms = nothing,
                                     ballot_kind = nothing)
    cand_syms = _candidate_syms_from_rankings(profile; fallback = candidate_syms)
    pool = Preferences.CandidatePool(cand_syms)
    kind = if ballot_kind === nothing
        isempty(profile) ? :strict : (_ranking_is_strict(profile[1], length(pool)) ? :strict : :weak)
    else
        Symbol(ballot_kind)
    end

    ballots = if kind === :strict
        isempty(profile) ? Preferences.StrictRank[] :
            [_strict_rank_from_dict(ranking, pool) for ranking in profile]
    elseif kind === :weak
        isempty(profile) ? Preferences.WeakRank[] :
            [_weak_rank_from_dict(ranking, pool) for ranking in profile]
    else
        throw(ArgumentError("Unsupported ballot_kind `$ballot_kind`. Use :strict or :weak."))
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
    profile = dict_profile_to_preferences(
        rankings;
        candidate_syms = cand_meta,
        ballot_kind = ballot_kind,
    )
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
        Preferences.linearize(bundle.profile; tie_break = tie_break, rng = rng),
        copy(bundle.metadata),
    )
end

@inline annotated_profile(bundle::AnnotatedProfile; kwargs...) = bundle

function annotated_profile(df::AbstractDataFrame; col::Symbol = :profile, ballot_kind = nothing)
    return dataframe_to_annotated_profile(df; col = col, ballot_kind = ballot_kind)
end

function _subset_profile(profile::Preferences.Profile, idxs)
    return Preferences.Profile(profile.pool, profile.ballots[collect(idxs)])
end

function _subset_profile(profile::Preferences.WeightedProfile, idxs)
    idxv = collect(idxs)
    inner = Preferences.Profile(profile.pool, profile.ballots[idxv])
    return Preferences.WeightedProfile(inner, profile.weights[idxv])
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
    return strict_profile(dataframe_to_annotated_profile(df))
end

function strict_profile(profile::AbstractVector{<:AbstractDict}; candidate_syms = nothing)
    return strict_profile(dict_profile_to_preferences(profile; candidate_syms = candidate_syms))
end
