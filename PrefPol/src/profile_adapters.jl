const AnnotatedProfile = Preferences.AnnotatedProfile

const _PROFILE_ENCODING_RANK_VECTOR_V1 = Preferences._PROFILE_ENCODING_RANK_VECTOR_V1

_metadata_or(args...; kwargs...) = Preferences._metadata_or(args...; kwargs...)
_normalize_ballot_kind(args...; kwargs...) = Preferences._normalize_ballot_kind(args...; kwargs...)
_metadata_ballot_kind(args...; kwargs...) = Preferences._metadata_ballot_kind(args...; kwargs...)
_metadata_profile_encoding(args...; kwargs...) = Preferences._metadata_profile_encoding(args...; kwargs...)


"""
    strict_profile(df::AbstractDataFrame)

Convert an applied strict-profile artifact table to `Preferences.AnnotatedProfile`
and return its formal strict profile. This is a cache/reporting adapter; formal
profile semantics are defined in `Preferences`.
"""
function strict_profile(df::AbstractDataFrame)
    return Preferences.strict_profile(
        Preferences.dataframe_to_annotated_profile(df; ballot_kind = :strict),
    )
end
