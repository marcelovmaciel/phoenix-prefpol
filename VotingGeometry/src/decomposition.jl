const COMPONENT_LABELS = (
    :Da,
    :Db,
    :Dc,
    :departure_subset_1,
    :departure_subset_2,
    :departure_subset_3,
    :departure_subset_4,
    :departure_subset_5,
    :departure_subset_6,
    :departure_subset_7,
    :departure_subset_8,
    :Ba,
    :Bb,
    :Bc,
    :Cabcd,
    :Cabdc,
    :Cacbd,
    :K,
    :double_reversal_ac,
    :double_reversal_cb,
    :double_reversal_ab,
    :double_reversal_cd,
    :double_reversal_bd,
    :double_reversal_ad,
)

const COMPONENT_GROUPS = Dict(
    :departure_differentials => (:Da, :Db, :Dc),
    :subset_departures => COMPONENT_LABELS[4:11],
    :basic_profile_differentials => (:Ba, :Bb, :Bc),
    :condorcet => (:Cabcd, :Cabdc, :Cacbd),
    :kernel => (:K,),
    :double_reversals => COMPONENT_LABELS[19:24],
)

struct Decomposition
    profile::Vector{Float64}
    coefficients::Vector{Float64}
    components::Matrix{Float64}
end

const CONDORCET_GENERATORS_4C = (
    (@SVector [1, 2, 3, 4]),
    (@SVector [1, 2, 4, 3]),
    (@SVector [1, 3, 2, 4]),
)

const DOUBLE_REVERSAL_EDGE_ORDER_4C = (
    (1, 3),
    (3, 2),
    (1, 2),
    (3, 4),
    (2, 4),
    (1, 4),
)

const _DOUBLE_REVERSAL_SIGNS_4C = Dict(
    (1, 3) => -1,
    (3, 2) => -1,
    (1, 2) => -1,
    (3, 4) => 1,
    (2, 4) => -1,
    (1, 4) => 1,
)

function _ranking_index_4c(order)
    key = SVector{4,Int}(order)
    idx = findfirst(==(key), CANONICAL_4C_IDS)
    idx === nothing && throw(ArgumentError("ranking is not in CANONICAL_4C_IDS: $order"))
    return idx
end

function _ranking_rotations_4c(order)
    r = collect(order)
    return [SVector{4,Int}(r[[mod1(i + k, 4) for i in 1:4]]) for k in 0:3]
end

"""
    kernel_profile_4c()

Return Saari's `K4` all-ranking neutral profile direction.
"""
kernel_profile_4c() = ones(Float64, 24)

"""
    basic_profile_differential_4c(candidate_id)

Return `B^4_c`: `+1` on rankings where `candidate_id` is top-ranked, `-1` on
rankings where it is bottom-ranked, and `0` otherwise. Saari 2000,
"Mathematical Structure of Voting Paradoxes", Definition 2.
"""
function basic_profile_differential_4c(candidate_id::Integer)
    1 <= candidate_id <= 4 || throw(ArgumentError("candidate_id must be in 1:4"))
    v = zeros(Float64, 24)
    @inbounds for (i, order) in pairs(CANONICAL_4C_IDS)
        if order[1] == candidate_id
            v[i] = 1.0
        elseif order[4] == candidate_id
            v[i] = -1.0
        end
    end
    return v
end

"""
    borda_profile_differential_4c(candidate_id)

Return Saari's four-candidate Borda profile differential `Bor^4_c`, assigning
`5 - 2k` to rankings where the candidate is in position `k`.
"""
function borda_profile_differential_4c(candidate_id::Integer)
    1 <= candidate_id <= 4 || throw(ArgumentError("candidate_id must be in 1:4"))
    v = zeros(Float64, 24)
    @inbounds for (i, order) in pairs(CANONICAL_4C_IDS)
        rank = findfirst(==(candidate_id), order)
        v[i] = 5.0 - 2.0 * rank
    end
    return v
end

"""
    condorcet_profile_differential_4c(order)

Return `C^4_r = R^4_r - R^4_{rho(r)}` using Saari's Condorcet four-tuple
construction from Definition 8.
"""
function condorcet_profile_differential_4c(order)
    r = SVector{4,Int}(order)
    sort(collect(r)) == [1, 2, 3, 4] || throw(ArgumentError("order must be a permutation of 1:4"))
    v = zeros(Float64, 24)
    for ranking in _ranking_rotations_4c(r)
        v[_ranking_index_4c(ranking)] += 1.0
    end
    for ranking in _ranking_rotations_4c(reverse(r))
        v[_ranking_index_4c(ranking)] -= 1.0
    end
    return v
end

"""
    double_reversal_differential_4c(edge)

Return the Saari Figure 8 double-reversal differential for one of the six source
edge labels in `DOUBLE_REVERSAL_EDGE_ORDER_4C`. The signs match the Theorem
14 matrix columns.
"""
function double_reversal_differential_4c(edge)
    edge_tuple = Tuple(Int(x) for x in edge)
    haskey(_DOUBLE_REVERSAL_SIGNS_4C, edge_tuple) ||
        throw(ArgumentError("edge must be one of DOUBLE_REVERSAL_EDGE_ORDER_4C"))
    u, vtx = edge_tuple
    complement = [x for x in 1:4 if x != u && x != vtx]
    x, y = complement
    sign = _DOUBLE_REVERSAL_SIGNS_4C[edge_tuple]
    out = zeros(Float64, 24)
    plus = (SVector{4,Int}(u, vtx, y, x), SVector{4,Int}(vtx, u, x, y))
    minus = (SVector{4,Int}(u, vtx, x, y), SVector{4,Int}(vtx, u, y, x))
    for ranking in plus
        out[_ranking_index_4c(ranking)] += sign
    end
    for ranking in minus
        out[_ranking_index_4c(ranking)] -= sign
    end
    return out
end

function source_component_basis_matrix_4c()
    mat = zeros(Float64, 24, 13)
    mat[:, 1] .= basic_profile_differential_4c(1)
    mat[:, 2] .= basic_profile_differential_4c(2)
    mat[:, 3] .= basic_profile_differential_4c(3)
    for (j, r) in pairs(CONDORCET_GENERATORS_4C)
        mat[:, 3 + j] .= condorcet_profile_differential_4c(r)
    end
    mat[:, 7] .= kernel_profile_4c()
    for (j, edge) in pairs(DOUBLE_REVERSAL_EDGE_ORDER_4C)
        mat[:, 7 + j] .= double_reversal_differential_4c(edge)
    end
    return mat
end


function _pairwise_margin_matrix_4c(p4)
    v = validate_profile_vector(p4, 24)
    margins = zeros(Float64, 4, 4)
    positions = zeros(Int, 4)

    @inbounds for (col, order) in pairs(CANONICAL_4C_IDS)
        for (rank, candidate_id) in pairs(order)
            positions[candidate_id] = rank
        end
        weight = v[col]
        for i in 1:3
            for j in (i + 1):4
                margin = positions[i] < positions[j] ? weight : -weight
                margins[i, j] += margin
                margins[j, i] -= margin
            end
        end
    end

    return margins
end

# Saari 2000, Mathematical Structure of Voting Paradoxes: II, Theorem 14.
# The rows/columns are indexed by CANONICAL_4C_IDS, the Figure 8 voter-type
# order. The source prints the coefficient transform as 1/24 times this integer
# matrix; basis_matrix() returns the inverse profile-differential basis.
const _T_INT = [
     0  0 -1 -2 -2 -1  2  1 -1 -2 -1  1  2  1  0  0  1  2  1 -1 -2 -1  1  2;
    -1 -2 -2 -1  0  0  2  1  0  0  1  2  1 -1 -2 -1  1  2  2  1 -1 -2 -1  1;
    -2 -1  0  0 -1 -2  1 -1 -2 -1  1  2  2  1 -1 -2 -1  1  2  1  0  0  1  2;
     0 -4  4  0 -4  4 -4 -4 -4  4  4  4  0  0  0 -4 -4 -4  4  4  4  0  0  0;
     4 -4  0  4 -4  0 -4 -4 -4  0  0  0  4  4  4 -4 -4 -4  0  0  0  4  4  4;
     0  0  4  4  4  0  0 -4  4  0 -4  4 -4 -4  0  0  0 -4 -4  4  4  4 -4 -4;
     4  4  0  0  0  4  4 -4  0  4 -4  0 -4 -4  4  4  4 -4 -4  0  0  0 -4 -4;
     4  0  0  0  4  4 -4  4  4  4 -4 -4  0 -4  4  0 -4  4 -4 -4  0  0  0 -4;
     0  4  4  4  0  0 -4  0  0  0 -4 -4  4 -4  0  4 -4  0 -4 -4  4  4  4 -4;
     4  4  4  0  0  0 -4 -4  0  0  0 -4 -4  4  4  4 -4 -4  0 -4  4  0 -4  4;
     0  0  0  4  4  4 -4 -4  4  4  4 -4 -4  0  0  0 -4 -4  4 -4  0  4 -4  0;
     3  3  2  1  1  2  2  1 -1 -2 -1  1 -1 -2 -3 -3 -2 -1  1 -1 -2 -1  1  2;
     2  1  1  2  3  3 -1 -2 -3 -3 -2 -1  1 -1 -2 -1  1  2  2  1 -1 -2 -1  1;
     1  2  3  3  2  1  1 -1 -2 -1  1  2  2  1 -1 -2 -1  1 -1 -2 -3 -3 -2 -1;
     3  0  0 -3  0  0  0 -3  0  0  3  0  0  0 -3  0  0  3 -3  0  0  3  0  0;
     0  0  3  0  0 -3 -3  0  0  3  0  0  0 -3  0  0  3  0  0  0 -3  0  0  3;
     0  3  0  0 -3  0  0  0  3  0  0 -3  3  0  0 -3  0  0  0  3  0  0 -3  0;
     1  1  1  1  1  1  1  1  1  1  1  1  1  1  1  1  1  1  1  1  1  1  1  1;
     3  6 -6 -3 -6  6 -6 -3 -6  6  3  6  0  0  3  0  0 -3  3  0  0 -3  0  0;
    -6  6  3  6 -6 -3  3  0  0 -3  0  0 -6 -3 -6  6  3  6  0  0  3  0  0 -3;
     6  3  6 -6 -3 -6  0  0 -3  0  0  3 -3  0  0  3  0  0  6  3  6 -6 -3 -6;
     0 -3  0  0  3  0 -6  6  3  6 -6 -3  3  6 -6 -3 -6  6  0 -3  0  0  3  0;
     3  0  0 -3  0  0  0 -3  0  0  3  0 -6  6  3  6 -6 -3  3  6 -6 -3 -6  6;
     0  0  3  0  0 -3  3  6 -6 -3 -6  6  0 -3  0  0  3  0 -6  6  3  6 -6 -3
]

transformation_matrix() = _T_INT ./ 24.0

function basis_matrix()
    inv_t = inv(transformation_matrix())
    rounded = round.(Int, inv_t)
    isapprox(inv_t, Float64.(rounded); atol = 1e-8) ||
        error("Saari decomposition inverse is not approximately integer-valued")
    return rounded
end

function decomposition_coefficients(p)
    v = validate_profile_vector(p, 24)
    return transformation_matrix() * v
end

function decompose_profile(p)
    v = validate_profile_vector(p, 24)
    coeffs = decomposition_coefficients(v)
    basis = Float64.(basis_matrix())
    components = zeros(Float64, 24, 24)
    @inbounds for j in 1:24
        components[:, j] .= coeffs[j] .* basis[:, j]
    end
    return Decomposition(v, coeffs, components)
end

function _component_indices(labels)
    requested = labels isa Symbol ? (labels,) : Tuple(labels)
    idx = Int[]
    for label in requested
        found = findfirst(==(label), COMPONENT_LABELS)
        found === nothing && throw(ArgumentError("unknown decomposition component label: $label"))
        push!(idx, found)
    end
    return idx
end

function reconstruct(dec::Decomposition; labels = nothing)
    if labels === nothing
        return vec(sum(dec.components; dims = 2))
    end
    idx = _component_indices(labels)
    return vec(sum(dec.components[:, idx]; dims = 2))
end

function group_component(dec::Decomposition, group::Symbol)
    haskey(COMPONENT_GROUPS, group) || throw(ArgumentError("unknown decomposition component group: $group"))
    return reconstruct(dec; labels = COMPONENT_GROUPS[group])
end

function check_basis_identity(; atol = 1e-8)
    t = transformation_matrix()
    b = Float64.(basis_matrix())
    ident = Matrix{Float64}(I, 24, 24)
    return isapprox(t * b, ident; atol = atol) && isapprox(b * t, ident; atol = atol)
end
