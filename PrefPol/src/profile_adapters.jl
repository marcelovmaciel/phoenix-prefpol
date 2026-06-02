const AnnotatedProfile = Preferences.AnnotatedProfile

const _PROFILE_ENCODING_RANK_VECTOR_V1 = Preferences._PROFILE_ENCODING_RANK_VECTOR_V1

_metadata_or(args...; kwargs...) = Preferences._metadata_or(args...; kwargs...)
_normalize_ballot_kind(args...; kwargs...) = Preferences._normalize_ballot_kind(args...; kwargs...)
_metadata_ballot_kind(args...; kwargs...) = Preferences._metadata_ballot_kind(args...; kwargs...)
_metadata_profile_encoding(args...; kwargs...) = Preferences._metadata_profile_encoding(args...; kwargs...)

"""
    dict_profile_to_preferences(args...; kwargs...)

Delegate conversion of dictionary-encoded applied rankings to formal
`Preferences` profiles. PrefPol keeps this wrapper so legacy ESEB preprocessing
code can call the adapter from the applied package namespace.
"""
dict_profile_to_preferences(args...; kwargs...) =
    Preferences.dict_profile_to_preferences(args...; kwargs...)

"""
    compact_profile_artifact_dataframe(args...; kwargs...)

Delegate compact profile artifact serialization to `Preferences`. The nested
pipeline uses this before writing linearized profile cache artifacts.
"""
compact_profile_artifact_dataframe(args...; kwargs...) =
    Preferences.compact_profile_artifact_dataframe(args...; kwargs...)

"""
    dataframe_to_annotated_profile(args...; kwargs...)

Delegate conversion from applied profile DataFrames to
`Preferences.AnnotatedProfile`. Formal metadata and ballot-encoding conventions
are documented in `Preferences`; PrefPol supplies ESEB-generated profile tables.
"""
dataframe_to_annotated_profile(args...; kwargs...) =
    Preferences.dataframe_to_annotated_profile(args...; kwargs...)

"""
    profile_to_ranking_dicts(args...; kwargs...)

Delegate formal profile-to-dictionary conversion to `Preferences` for reporting
and legacy applied code.
"""
profile_to_ranking_dicts(args...; kwargs...) =
    Preferences.profile_to_ranking_dicts(args...; kwargs...)

"""
    annotated_profile_to_dataframe(args...; kwargs...)

Delegate conversion of `Preferences.AnnotatedProfile` objects back to tabular
applied artifacts. Used by pipeline cache/reporting code; no formal semantics
are redefined in PrefPol.
"""
annotated_profile_to_dataframe(args...; kwargs...) =
    Preferences.annotated_profile_to_dataframe(args...; kwargs...)

"""
    linearize_annotated_profile(args...; kwargs...)

Delegate weak-order linearization to `Preferences`. PrefPol chooses the
application policy (`:random_ties` or `:pattern_conditional`) and seed; the
formal linearizer behavior lives in `Preferences`.
"""
linearize_annotated_profile(args...; kwargs...) =
    Preferences.linearize_annotated_profile(args...; kwargs...)

"""
    annotated_profile(args...; kwargs...)

Delegate construction of annotated formal profiles to `Preferences`.
"""
annotated_profile(args...; kwargs...) = Preferences.annotated_profile(args...; kwargs...)

"""
    subset_annotated_profile(args...; kwargs...)

Delegate row subsetting of annotated formal profiles to `Preferences`; PrefPol
uses it when splitting strict profiles by demographic group.
"""
subset_annotated_profile(args...; kwargs...) =
    Preferences.subset_annotated_profile(args...; kwargs...)

"""
    strict_profile(x::AnnotatedProfile)

Delegate extraction of the formal strict profile from an annotated profile to
`Preferences`. PrefPol uses this before applying formal global and grouped
measures.
"""
strict_profile(x::AnnotatedProfile) = Preferences.strict_profile(x)

"""
    strict_profile(df::AbstractDataFrame)

Convert an applied strict-profile artifact table to `Preferences.AnnotatedProfile`
and return its formal strict profile. This is a cache/reporting adapter; formal
profile semantics are defined in `Preferences`.
"""
function strict_profile(df::AbstractDataFrame)
    return Preferences.strict_profile(dataframe_to_annotated_profile(df; ballot_kind = :strict))
end
