const _STRICT_PROFILE_MESSAGE = "VotingGeometry requires strict complete profiles. Use Preferences.linearize first."

function validate_profile_vector(p, n::Integer)::Vector{Float64}
    n > 0 || throw(ArgumentError("profile vector length must be positive"))
    length(p) == n || throw(ArgumentError("expected profile vector of length $n, got $(length(p))"))
    v = [Float64(x) for x in p]
    all(isfinite, v) || throw(ArgumentError("profile vector entries must be finite"))
    return v
end

function _check_profile_basis_pool(profile, basis::SaariBasis)
    Preferences.candidates(profile.pool) == Preferences.candidates(basis.pool) ||
        throw(ArgumentError("profile and basis must use the same candidate symbols in the same order"))
    return nothing
end

function _canonical_index_for_ballot(ballot, basis::SaariBasis{N}) where {N}
    key = SVector{N,Int}(Preferences.perm(ballot))
    idx = get(basis.index, key, nothing)
    idx === nothing && throw(ArgumentError("ballot permutation is not in the canonical Saari basis"))
    return idx
end

function profile_counts(profile::Preferences.Profile{<:Preferences.StrictRank}, basis::SaariBasis{N}) where {N}
    _check_profile_basis_pool(profile, basis)
    counts = zeros(Float64, length(basis.permutations))
    @inbounds for ballot in profile.ballots
        counts[_canonical_index_for_ballot(ballot, basis)] += 1.0
    end
    return counts
end

function profile_counts(profile::Preferences.WeightedProfile{<:Preferences.StrictRank}, basis::SaariBasis{N}) where {N}
    _check_profile_basis_pool(profile, basis)
    counts = zeros(Float64, length(basis.permutations))
    profile_weights = Preferences.weights(profile)
    @inbounds for i in eachindex(profile.ballots)
        idx = _canonical_index_for_ballot(profile.ballots[i], basis)
        counts[idx] += Float64(profile_weights[i])
    end
    return counts
end

function profile_counts(profile::Union{Preferences.Profile,Preferences.WeightedProfile}, basis::SaariBasis)
    throw(ArgumentError(_STRICT_PROFILE_MESSAGE))
end

function profile_vector(profile, basis::SaariBasis; normalize::Bool = true)
    counts = profile_counts(profile, basis)
    normalize || return counts

    mass = sum(counts)
    mass > 0 || throw(ArgumentError("cannot normalize a zero-mass profile"))
    return counts ./ mass
end
