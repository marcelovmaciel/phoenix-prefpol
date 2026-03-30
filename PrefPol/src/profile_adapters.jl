const AnnotatedProfile = Preferences.AnnotatedProfile

const _PROFILE_ENCODING_RANK_VECTOR_V1 = Preferences._PROFILE_ENCODING_RANK_VECTOR_V1

_metadata_or(args...; kwargs...) = Preferences._metadata_or(args...; kwargs...)
_normalize_ballot_kind(args...; kwargs...) = Preferences._normalize_ballot_kind(args...; kwargs...)
_metadata_ballot_kind(args...; kwargs...) = Preferences._metadata_ballot_kind(args...; kwargs...)
_metadata_profile_encoding(args...; kwargs...) = Preferences._metadata_profile_encoding(args...; kwargs...)

dict_profile_to_preferences(args...; kwargs...) =
    Preferences.dict_profile_to_preferences(args...; kwargs...)

compact_profile_artifact_dataframe(args...; kwargs...) =
    Preferences.compact_profile_artifact_dataframe(args...; kwargs...)

dataframe_to_annotated_profile(args...; kwargs...) =
    Preferences.dataframe_to_annotated_profile(args...; kwargs...)

profile_to_ranking_dicts(args...; kwargs...) =
    Preferences.profile_to_ranking_dicts(args...; kwargs...)

annotated_profile_to_dataframe(args...; kwargs...) =
    Preferences.annotated_profile_to_dataframe(args...; kwargs...)

linearize_annotated_profile(args...; kwargs...) =
    Preferences.linearize_annotated_profile(args...; kwargs...)

annotated_profile(args...; kwargs...) = Preferences.annotated_profile(args...; kwargs...)

subset_annotated_profile(args...; kwargs...) =
    Preferences.subset_annotated_profile(args...; kwargs...)

strict_profile(x::AnnotatedProfile) = Preferences.strict_profile(x)

function strict_profile(df::AbstractDataFrame)
    return Preferences.strict_profile(dataframe_to_annotated_profile(df; ballot_kind = :strict))
end
