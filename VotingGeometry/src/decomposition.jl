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
