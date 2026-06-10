const _STRICT_PROFILE_MESSAGE = "VotingGeometry requires strict complete profiles. Use PreferenceProfiles.linearize first."
const _PROFILE_ZERO_SUM_ATOL = 1e-9

function _validate_profile_shape_and_finiteness(p, n::Integer)::Vector{Float64}
    n > 0 || throw(ArgumentError("profile vector length must be positive"))
    length(p) == n || throw(ArgumentError("expected profile vector of length $n, got $(length(p))"))
    v = [Float64(x) for x in p]
    all(isfinite, v) || throw(ArgumentError("profile vector entries must be finite"))
    return v
end

"""
    validate_profile_differential(p, n; require_zero_sum=false)

Validate a length-`n` profile differential. Differentials may contain signed
entries, but all entries must be finite. With `require_zero_sum=true`, require
`sum(v)` to be within absolute tolerance `$(_PROFILE_ZERO_SUM_ATOL)` of zero.
"""
function validate_profile_differential(p, n::Integer; require_zero_sum::Bool = false)::Vector{Float64}
    v = _validate_profile_shape_and_finiteness(p, n)
    if require_zero_sum && !isapprox(sum(v), 0.0; atol = _PROFILE_ZERO_SUM_ATOL, rtol = 0.0)
        throw(ArgumentError("profile differential must sum to zero within absolute tolerance $(_PROFILE_ZERO_SUM_ATOL)"))
    end
    return v
end

"""
    validate_profile_counts(p, n; require_positive_mass=true)

Validate a length-`n` actual electorate/profile count or share vector. Entries
must be finite and nonnegative. By default, the total mass must be strictly
positive; set `require_positive_mass=false` only for callers that explicitly
allow the zero profile.
"""
function validate_profile_counts(p, n::Integer; require_positive_mass::Bool = true)::Vector{Float64}
    v = _validate_profile_shape_and_finiteness(p, n)
    all(x -> x >= 0.0, v) || throw(ArgumentError("profile counts must be nonnegative"))
    if require_positive_mass && !(sum(v) > 0.0)
        throw(ArgumentError("profile counts must have positive total mass"))
    end
    return v
end

"""
    validate_profile_vector(p, n)

Compatibility/internal alias for `validate_profile_differential(p, n)`. This
validator allows signed entries; use `validate_profile_counts` for actual
electorates or profile-frequency displays.
"""
validate_profile_vector(p, n::Integer)::Vector{Float64} = validate_profile_differential(p, n)

function _check_profile_basis_pool(profile, basis::SaariBasis)
    PreferenceProfiles.candidates(profile.pool) == PreferenceProfiles.candidates(basis.pool) ||
        throw(ArgumentError("profile and basis must use the same candidate symbols in the same order"))
    return nothing
end

function _canonical_index_for_ballot(ballot, basis::SaariBasis{N}) where {N}
    key = SVector{N,Int}(PreferenceProfiles.perm(ballot))
    idx = get(basis.index, key, nothing)
    idx === nothing && throw(ArgumentError("ballot permutation is not in the canonical Saari basis"))
    return idx
end

function profile_counts(profile::PreferenceProfiles.Profile{<:PreferenceProfiles.StrictRank}, basis::SaariBasis{N}) where {N}
    _check_profile_basis_pool(profile, basis)
    counts = zeros(Float64, length(basis.permutations))
    @inbounds for ballot in profile.ballots
        counts[_canonical_index_for_ballot(ballot, basis)] += 1.0
    end
    return counts
end

function profile_counts(profile::PreferenceProfiles.WeightedProfile{<:PreferenceProfiles.StrictRank}, basis::SaariBasis{N}) where {N}
    _check_profile_basis_pool(profile, basis)
    counts = zeros(Float64, length(basis.permutations))
    profile_weights = PreferenceProfiles.weights(profile)
    @inbounds for i in eachindex(profile.ballots)
        idx = _canonical_index_for_ballot(profile.ballots[i], basis)
        counts[idx] += Float64(profile_weights[i])
    end
    return counts
end

function profile_counts(profile::Union{PreferenceProfiles.Profile,PreferenceProfiles.WeightedProfile}, basis::SaariBasis)
    throw(ArgumentError(_STRICT_PROFILE_MESSAGE))
end

function profile_vector(profile, basis::SaariBasis; normalize::Bool = true)
    counts = profile_counts(profile, basis)
    normalize || return counts

    mass = sum(counts)
    mass > 0 || throw(ArgumentError("cannot normalize a zero-mass profile"))
    return counts ./ mass
end
