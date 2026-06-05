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
