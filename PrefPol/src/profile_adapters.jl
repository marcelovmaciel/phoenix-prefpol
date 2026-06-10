const AnnotatedProfile = PreferenceProfiles.AnnotatedProfile

const _PROFILE_ENCODING_RANK_VECTOR_V1 = PreferenceProfiles._PROFILE_ENCODING_RANK_VECTOR_V1

_metadata_or(args...; kwargs...) = PreferenceProfiles._metadata_or(args...; kwargs...)
_normalize_ballot_kind(args...; kwargs...) = PreferenceProfiles._normalize_ballot_kind(args...; kwargs...)
_metadata_ballot_kind(args...; kwargs...) = PreferenceProfiles._metadata_ballot_kind(args...; kwargs...)
_metadata_profile_encoding(args...; kwargs...) = PreferenceProfiles._metadata_profile_encoding(args...; kwargs...)


"""
    strict_profile(df::AbstractDataFrame)

Convert an applied strict-profile artifact table to `PreferenceProfiles.AnnotatedProfile`
and return its formal strict profile. This is a cache/reporting adapter; formal
profile semantics are defined in `PreferenceProfiles`.
"""
function strict_profile(df::AbstractDataFrame)
    return PreferenceProfiles.strict_profile(
        PreferenceProfiles.dataframe_to_annotated_profile(df; ballot_kind = :strict),
    )
end
