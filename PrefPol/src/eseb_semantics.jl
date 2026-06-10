# ESEB-specific survey semantics.
#
# This file contains candidate thermometer score normalization and derived
# ESEB grouping variables. General preference/profile semantics belong in
# PreferenceProfiles.

const ESEB_SCORE_MISSING_CODES = (96, 97, 98, 99)
const ESEB_VALID_SCORE_MIN = 0.0
const ESEB_VALID_SCORE_MAX = 10.0

@inline _unwrap_eseb_score(x) = x isa CategoricalValue ? unwrap(x) : x

function normalize_eseb_score(x)::Union{Missing,Float64}
    raw = _unwrap_eseb_score(x)
    ismissing(raw) && return missing

    value = if raw isa Real
        Float64(raw)
    elseif raw isa AbstractString
        parsed = tryparse(Float64, strip(raw))
        parsed === nothing && return missing
        parsed
    else
        return missing
    end

    isfinite(value) || return missing
    rounded = round(Int, value)
    if isapprox(value, rounded; atol = 1e-8) && rounded in ESEB_SCORE_MISSING_CODES
        return missing
    end
    (ESEB_VALID_SCORE_MIN <= value <= ESEB_VALID_SCORE_MAX) || return missing
    return value
end

@inline is_eseb_missing_score(x)::Bool = normalize_eseb_score(x) === missing

function normalize_eseb_score_column!(df::DataFrame, col)
    df[!, col] = Union{Missing,Float64}[normalize_eseb_score(x) for x in df[!, col]]
    return df
end

function normalize_eseb_score_columns!(df::DataFrame, cols)
    for col in cols
        normalize_eseb_score_column!(df, col)
    end
    return df
end

const LULA_SCORE_GROUP_LEVELS = ["low_lula", "medium_lula", "high_lula"]

"""
    lula_score_group_value(x) -> Union{Missing,String}

Return the derived `LulaScoreGroup` label for one Lula thermometer score.

This is a grouping variable derived from the Lula score column, not an active
candidate score. Missing values, ESEB thermometer sentinels, nonnumeric values,
nonfinite values, and values outside the valid 0-10 thermometer range map to
`missing`.
"""
function lula_score_group_value(x)
    normalized = normalize_eseb_score(x)
    normalized === missing && return missing

    if 0 <= normalized <= 3
        return "low_lula"
    elseif 4 <= normalized <= 6
        return "medium_lula"
    elseif 7 <= normalized <= 10
        return "high_lula"
    end

    return missing
end

"""
    lula_score_group_column(scores)

Construct the ordered categorical `LulaScoreGroup` column for a score vector.

Levels are exactly `LULA_SCORE_GROUP_LEVELS`, preserving missing values from
`lula_score_group_value`.
"""
function lula_score_group_column(scores)
    groups = Union{Missing,String}[lula_score_group_value(x) for x in scores]
    return categorical(
        groups;
        ordered = true,
        levels = LULA_SCORE_GROUP_LEVELS,
    )
end

"""
    add_lula_score_group!(df; source=:Lula, target=:LulaScoreGroup)

Add or replace the derived ordered categorical `LulaScoreGroup` column in `df`.

The source is a Lula thermometer score column. The target is a respondent
partition used by grouped analyses, not an active candidate column.
"""
function add_lula_score_group!(df::DataFrame; source = :Lula, target = :LulaScoreGroup)
    df[!, target] = lula_score_group_column(df[!, source])
    return df
end
