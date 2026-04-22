struct AnnotatedProfile{P,M}
    profile::P
    metadata::M

    function AnnotatedProfile{P,M}(profile::P, metadata::M) where {P,M}
        metadata isa NamedTuple && _validate_metadata_lengths(metadata, nballots(profile))
        metadata isa AbstractDataFrame && _validate_metadata_lengths(metadata, nballots(profile))
        return new{P,M}(profile, metadata)
    end
end

Base.length(bundle::AnnotatedProfile) = nballots(bundle.profile)

const _PROFILE_ENCODING_RANK_VECTOR_V1 = "rank_vector_v1"

function _validate_metadata_lengths(metadata::NamedTuple, expected_rows::Integer)
    for name in propertynames(metadata)
        col = getproperty(metadata, name)
        col isa AbstractVector || throw(ArgumentError(
            "AnnotatedProfile metadata column `$name` must be an AbstractVector.",
        ))
        length(col) == expected_rows || throw(ArgumentError(
            "AnnotatedProfile metadata column `$name` has length $(length(col)); expected $expected_rows.",
        ))
    end

    return metadata
end

function _validate_metadata_lengths(metadata::AbstractDataFrame, expected_rows::Integer)
    nrow(metadata) == expected_rows || throw(ArgumentError(
        "AnnotatedProfile metadata has $(nrow(metadata)) rows; expected $expected_rows.",
    ))
    return metadata
end

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

function _copy_metadata_namedtuple(metadata::NamedTuple)
    return (; (
        name => copy(getproperty(metadata, name))
        for name in propertynames(metadata)
    )...)
end

function _metadata_namedtuple(df::AbstractDataFrame; exclude::Union{Nothing,Symbol} = nothing)
    colnames = Symbol.(names(df))
    exclude === nothing || (colnames = [name for name in colnames if name != exclude])
    return (; (name => copy(df[!, name]) for name in colnames)...)
end

_metadata_namedtuple(metadata::NamedTuple; copycols::Bool = false) =
    copycols ? _copy_metadata_namedtuple(metadata) : metadata

function _metadata_dataframe(metadata::NamedTuple; copycols::Bool = true)
    cols = copycols ? _copy_metadata_namedtuple(metadata) : metadata
    return DataFrame(cols; copycols = false)
end

_metadata_dataframe(metadata::AbstractDataFrame; copycols::Bool = true) =
    copycols ? copy(metadata) : DataFrame(metadata; copycols = false)

function _subset_metadata(metadata::NamedTuple, idxs)
    return (; (
        name => getproperty(metadata, name)[idxs]
        for name in propertynames(metadata)
    )...)
end

_subset_metadata(metadata::AbstractDataFrame, idxs) = _metadata_namedtuple(metadata[idxs, :])

function _metadata_column(metadata::NamedTuple, key)
    name = Symbol(key)
    hasproperty(metadata, name) || throw(ArgumentError(
        "AnnotatedProfile metadata is missing column `$name`.",
    ))
    return getproperty(metadata, name)
end

function _metadata_column(metadata::AbstractDataFrame, key)
    name = Symbol(key)
    hasproperty(metadata, name) || throw(ArgumentError(
        "AnnotatedProfile metadata is missing column `$name`.",
    ))
    return metadata[!, name]
end

function AnnotatedProfile(profile, metadata::NamedTuple)
    _validate_metadata_lengths(metadata, nballots(profile))
    return AnnotatedProfile{typeof(profile),typeof(metadata)}(profile, metadata)
end

function AnnotatedProfile(profile, metadata::AbstractDataFrame)
    _validate_metadata_lengths(metadata, nballots(profile))
    return AnnotatedProfile(profile, _metadata_namedtuple(metadata))
end

AnnotatedProfile(df::AbstractDataFrame; col::Symbol = :profile, ballot_kind = nothing) =
    dataframe_to_annotated_profile(df; col = col, ballot_kind = ballot_kind)

function _annotated_candidate_syms_from_rankings(rankings; fallback = nothing)
    if fallback !== nothing
        return Symbol.(collect(fallback))
    end

    isempty(rankings) && throw(ArgumentError(
        "Cannot infer candidate labels from an empty ranking collection without metadata.",
    ))

    first_ranking = rankings[firstindex(rankings)]
    return sort!(collect(Symbol.(keys(first_ranking))); by = string)
end

function _annotated_strict_rank_from_dict(ranking::AbstractDict, pool::CandidatePool)
    ordered = sort!(collect(Symbol.(keys(ranking))); by = cand -> Int(ranking[cand]))
    return StrictRank(pool, ordered)
end

function _annotated_weak_rank_from_dict(ranking::AbstractDict, pool::CandidatePool)
    return WeakRank(pool, Dict(Symbol(k) => Int(v) for (k, v) in ranking))
end

@inline function _empty_preferences_profile(candidate_syms, ::Val{:strict})
    pool = CandidatePool(Symbol.(collect(candidate_syms)))
    exemplar = StrictRank(pool, collect(1:length(pool)))
    return Profile(pool, Vector{typeof(exemplar)}(undef, 0))
end

@inline function _empty_preferences_profile(candidate_syms, ::Val{:weak})
    pool = CandidatePool(Symbol.(collect(candidate_syms)))
    exemplar = WeakRank(pool, fill(missing, length(pool)))
    return Profile(pool, Vector{typeof(exemplar)}(undef, 0))
end

function _compact_rank_code(ballot)
    ranks_vec = ranks(ballot)
    return ntuple(length(ranks_vec)) do i
        rk = ranks_vec[i]
        return ismissing(rk) ? UInt16(0) : UInt16(Int(rk))
    end
end

function _decode_compact_rank_code(code, kind::Symbol, pool::CandidatePool)
    if kind === :strict
        ranks_vec = Int.(collect(code))
        sort(copy(ranks_vec)) == collect(1:length(ranks_vec)) || throw(ArgumentError(
            "Compact strict-profile artifact contains a non-strict ballot.",
        ))
        return StrictRank(pool, sortperm(ranks_vec))
    end

    rank_values = Vector{Union{Missing,Int}}(undef, length(code))
    @inbounds for i in eachindex(code)
        rk = Int(code[i])
        rank_values[i] = rk == 0 ? missing : rk
    end
    return WeakRank(pool, rank_values)
end

function _compact_rank_profile_to_preferences(rankings;
                                              candidate_syms,
                                              ballot_kind)
    candidate_syms === nothing && throw(ArgumentError(
        "Compact profile artifacts require candidate metadata.",
    ))

    kind = _normalize_ballot_kind(ballot_kind)
    pool = CandidatePool(Symbol.(collect(candidate_syms)))
    ballots = if isempty(rankings)
        _empty_preferences_profile(candidate_syms, Val(kind)).ballots
    elseif kind === :strict
        [_decode_compact_rank_code(rankings[i], kind, pool) for i in eachindex(rankings)]
    else
        [_decode_compact_rank_code(rankings[i], kind, pool) for i in eachindex(rankings)]
    end

    return Profile(pool, ballots)
end

function dict_profile_to_preferences(profile::AbstractVector{<:AbstractDict};
                                     candidate_syms = nothing,
                                     ballot_kind = nothing)
    cand_syms = _annotated_candidate_syms_from_rankings(profile; fallback = candidate_syms)
    kind = _normalize_ballot_kind(ballot_kind)
    kind === nothing && throw(ArgumentError(
        "Pass `ballot_kind = :strict` or `:weak`, or attach DataFrame metadata `profile_kind` before conversion.",
    ))

    pool = CandidatePool(cand_syms)
    ballots = if isempty(profile)
        _empty_preferences_profile(cand_syms, Val(kind)).ballots
    elseif kind === :strict
        [_annotated_strict_rank_from_dict(ranking, pool) for ranking in profile]
    else
        [_annotated_weak_rank_from_dict(ranking, pool) for ranking in profile]
    end

    return Profile(pool, ballots)
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

    return AnnotatedProfile(profile, _metadata_namedtuple(df_local; exclude = col))
end

function profile_to_ranking_dicts(profile::Union{Profile,WeightedProfile})
    return [asdict(ballot, profile.pool) for ballot in profile.ballots]
end

profile_to_ranking_dicts(bundle::AnnotatedProfile) = profile_to_ranking_dicts(bundle.profile)

function compact_profile_artifact_dataframe(bundle::AnnotatedProfile;
                                            col::Symbol = :profile)
    bundle = annotated_profile(bundle)
    bundle.profile isa WeightedProfile && throw(ArgumentError(
        "Compact artifact encoding currently supports unweighted pseudo-profiles only.",
    ))

    pool_syms = collect(candidates(bundle.profile.pool))
    n = nballots(bundle.profile)
    m = length(pool_syms)
    T = NTuple{m,UInt16}
    encoded = Vector{T}(undef, n)

    @inbounds for i in 1:n
        encoded[i] = _compact_rank_code(bundle.profile.ballots[i])
    end

    df = hcat(
        DataFrame(col => PooledArray(encoded; compress = true)),
        _metadata_dataframe(bundle.metadata),
    )
    metadata!(df, "candidates", pool_syms)
    metadata!(df, "profile_kind", is_strict(bundle.profile) ? "linearized" : "weak")
    metadata!(df, "profile_encoding", _PROFILE_ENCODING_RANK_VECTOR_V1)
    return df
end

function annotated_profile_to_dataframe(bundle::AnnotatedProfile;
                                        col::Symbol = :profile)
    bundle = annotated_profile(bundle)
    rankings = profile_to_ranking_dicts(bundle.profile)
    df = hcat(DataFrame(col => rankings), _metadata_dataframe(bundle.metadata))
    metadata!(df, "candidates", collect(candidates(bundle.profile.pool)))
    metadata!(df, "profile_kind", is_strict(bundle.profile) ? "linearized" : "weak")
    return df
end

function linearize_annotated_profile(bundle::AnnotatedProfile;
                                     tie_break = :random,
                                     rng = Random.GLOBAL_RNG)
    bundle = annotated_profile(bundle)
    return AnnotatedProfile(
        linearize(
            bundle.profile;
            tie_break = tie_break,
            rng = rng,
            incomplete_policy = :error,
        ),
        _copy_metadata_namedtuple(bundle.metadata),
    )
end

function annotated_profile(bundle::AnnotatedProfile; kwargs...)
    return bundle.metadata isa NamedTuple ? bundle : AnnotatedProfile(bundle.profile, bundle.metadata)
end

annotated_profile(profile, metadata; kwargs...) = AnnotatedProfile(profile, metadata)

function annotated_profile(df::AbstractDataFrame; col::Symbol = :profile, ballot_kind = nothing)
    return dataframe_to_annotated_profile(df; col = col, ballot_kind = ballot_kind)
end

function _subset_profile(profile::Profile, idxs)
    return Profile(profile.pool, profile.ballots[idxs])
end

function _subset_profile(profile::WeightedProfile, idxs)
    inner = Profile(profile.pool, profile.ballots[idxs])
    return WeightedProfile(inner, profile.weights[idxs])
end

function subset_annotated_profile(bundle::AnnotatedProfile, idxs)
    bundle = annotated_profile(bundle)
    idxv = collect(idxs)
    return AnnotatedProfile(
        _subset_profile(bundle.profile, idxv),
        _subset_metadata(bundle.metadata, idxv),
    )
end

strict_profile(x::AnnotatedProfile) = strict_profile(annotated_profile(x).profile)

function _group_row_indices(bundle::AnnotatedProfile, demo)
    bundle = annotated_profile(bundle)
    vals = _metadata_column(bundle.metadata, demo)
    grouped = OrderedDict{Any,Vector{Int}}()

    for (idx, val) in pairs(vals)
        push!(get!(grouped, val, Int[]), idx)
    end

    return grouped
end

function _group_proportion_map(bundle::AnnotatedProfile, grouped_indices)
    if bundle.profile isa WeightedProfile
        total = Float64(total_weight(bundle.profile))
        total > 0 || throw(ArgumentError("Weighted profile total weight must be positive."))
        return Dict(
            group => sum(Float64(bundle.profile.weights[idx]) for idx in idxs) / total
            for (group, idxs) in grouped_indices
        )
    end

    total = length(bundle)
    total > 0 || throw(ArgumentError("Annotated profile metadata must contain at least one row."))
    return Dict(group => length(idxs) / total for (group, idxs) in grouped_indices)
end

function overall_divergences(grouped_consensus::AbstractDataFrame,
                             whole_bundle::AnnotatedProfile,
                             key)
    whole_bundle = annotated_profile(whole_bundle)
    consensus_col = _consensus_column(grouped_consensus)
    grouped_indices = _group_row_indices(whole_bundle, key)
    group_profiles = Dict(
        row[key] => _subset_profile(whole_bundle.profile, grouped_indices[row[key]])
        for row in eachrow(grouped_consensus)
    )
    consensus_map = Dict(row[key] => row[consensus_col] for row in eachrow(grouped_consensus))
    return overall_divergence(group_profiles, consensus_map)
end

function overall_overlaps(grouped_consensus::AbstractDataFrame,
                          whole_bundle::AnnotatedProfile,
                          key)
    whole_bundle = annotated_profile(whole_bundle)
    grouped_indices = _group_row_indices(whole_bundle, key)
    group_profiles = Dict(
        row[key] => _subset_profile(whole_bundle.profile, grouped_indices[row[key]])
        for row in eachrow(grouped_consensus)
    )
    return overall_overlap(group_profiles)
end

function overall_overlaps_smoothed(grouped_consensus::AbstractDataFrame,
                                   whole_bundle::AnnotatedProfile,
                                   key)
    whole_bundle = annotated_profile(whole_bundle)
    grouped_indices = _group_row_indices(whole_bundle, key)
    group_profiles = Dict(
        row[key] => _subset_profile(whole_bundle.profile, grouped_indices[row[key]])
        for row in eachrow(grouped_consensus)
    )
    return overall_overlap_smoothed(group_profiles)
end

function overall_divergences_median(grouped_consensus::AbstractDataFrame,
                                    whole_bundle::AnnotatedProfile,
                                    key)
    whole_bundle = annotated_profile(whole_bundle)
    consensus_col = _consensus_column(grouped_consensus)
    grouped_indices = _group_row_indices(whole_bundle, key)
    group_profiles = Dict(
        row[key] => _subset_profile(whole_bundle.profile, grouped_indices[row[key]])
        for row in eachrow(grouped_consensus)
    )
    consensus_map = Dict(row[key] => row[consensus_col] for row in eachrow(grouped_consensus))
    return overall_divergence_median(group_profiles, consensus_map)
end

function overall_separations(grouped_consensus::AbstractDataFrame,
                             whole_bundle::AnnotatedProfile,
                             key)
    whole_bundle = annotated_profile(whole_bundle)
    consensus_col = _consensus_column(grouped_consensus)
    grouped_indices = _group_row_indices(whole_bundle, key)
    group_profiles = Dict(
        row[key] => _subset_profile(whole_bundle.profile, grouped_indices[row[key]])
        for row in eachrow(grouped_consensus)
    )
    consensus_map = Dict(row[key] => row[consensus_col] for row in eachrow(grouped_consensus))
    return overall_separation(group_profiles, consensus_map)
end

function _compute_group_metric_details(bundle::AnnotatedProfile, demo;
                                       cache = GLOBAL_LINEAR_ORDER_CACHE,
                                       rng = nothing,
                                       tie_break_context = nothing)
    bundle = annotated_profile(bundle)
    grouped_indices = _group_row_indices(bundle, demo)
    group_vals = collect(keys(grouped_indices))

    avg_distance = Float64[]
    group_coherence = Float64[]
    consensus_results = Any[]
    consensus_rankings = Any[]
    group_profiles = OrderedDict{Any,Any}()
    group_sizes = OrderedDict{Any,Float64}()

    for group in group_vals
        subprofile = _subset_profile(bundle.profile, grouped_indices[group])
        tie_key = _merge_tie_break_key(
            tie_break_context,
            (demographic = String(demo), group = string(group)),
        )
        result = consensus_kendall(subprofile;
                                   cache = cache,
                                   rng = rng,
                                   tie_break_key = tie_key)
        push!(avg_distance, result.avg_normalized_distance)
        push!(group_coherence, 1.0 - result.avg_normalized_distance)
        push!(consensus_results, result)
        push!(consensus_rankings, result.consensus_ranking)
        group_profiles[group] = subprofile
        group_sizes[group] = Float64(length(grouped_indices[group]))
    end

    results_distance = DataFrame(
        demo => group_vals,
        :avg_distance => avg_distance,
        :group_coherence => group_coherence,
    )

    prop = _group_proportion_map(bundle, grouped_indices)
    C = weighted_coherence(results_distance, prop, demo)

    consensus = DataFrame(
        demo => group_vals,
        :consensus_result => consensus_results,
        :consensus_ranking => consensus_rankings,
    )

    D = overall_divergences(consensus, bundle, demo)
    D_median = overall_divergences_median(consensus, bundle, demo)
    O = overall_overlaps(consensus, bundle, demo)
    O_smoothed = overall_overlaps_smoothed(consensus, bundle, demo)
    Sep = overall_separations(consensus, bundle, demo)
    Gsep = grouped_gsep(C, Sep)
    cleaned_S = overall_sstar_from_CD(C, D)
    support_separation_S_old = overall_support_separation_old(group_profiles, group_sizes)
    return (
        C = C,
        D = D,
        D_median = D_median,
        O = O,
        O_smoothed = O_smoothed,
        Sep = Sep,
        Gsep = Gsep,
        S = cleaned_S,
        S_old = support_separation_S_old,
    )
end

function compute_group_metrics(bundle::AnnotatedProfile, demo;
                               cache = GLOBAL_LINEAR_ORDER_CACHE,
                               rng = nothing,
                               tie_break_context = nothing)
    details = _compute_group_metric_details(
        bundle,
        demo;
        cache = cache,
        rng = rng,
        tie_break_context = tie_break_context,
    )
    return details.C, details.D
end
