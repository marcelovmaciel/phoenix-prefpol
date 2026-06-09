const TRIANGLE_VERTICES = [
    (0.0, 0.0),
    (1.0, 0.0),
    (0.5, sqrt(3) / 2),
]

const TETRAHEDRON_VERTICES = [
    (0.0, 0.0, 0.0),
    (1.0, 0.0, 0.0),
    (0.5, sqrt(3) / 2, 0.0),
    (0.5, sqrt(3) / 6, sqrt(6) / 3),
]

function tern2cart(a, b, c)
    total = Float64(a) + Float64(b) + Float64(c)
    total != 0 || throw(ArgumentError("ternary coordinates must have nonzero total"))
    bf = Float64(b) / total
    cf = Float64(c) / total
    return (bf + cf / 2, (sqrt(3) / 2) * cf)
end

function barycentric_to_cartesian(weights; vertices = nothing)
    if vertices === nothing
        if length(weights) == 3
            vertices = TRIANGLE_VERTICES
        elseif length(weights) == 4
            vertices = TETRAHEDRON_VERTICES
        else
            throw(ArgumentError("default vertices are available only for 3 or 4 weights"))
        end
    end

    length(weights) == length(vertices) ||
        throw(ArgumentError("weights and vertices must have the same length"))
    ws = [Float64(w) for w in weights]
    all(isfinite, ws) || throw(ArgumentError("weights must be finite"))
    total = sum(ws)
    total != 0 || throw(ArgumentError("barycentric weights must have nonzero total"))
    dim = length(vertices[1])
    coords = zeros(Float64, dim)
    @inbounds for i in eachindex(ws)
        alpha = ws[i] / total
        for d in 1:dim
            coords[d] += alpha * Float64(vertices[i][d])
        end
    end
    return Tuple(coords)
end

function triangle_points_from_profile(p3)
    v = validate_profile_counts(p3, 6)
    return Dict{Symbol,Tuple{Float64,Float64}}(
        :plurality => tern2cart(plurality_3a(v)...),
        :antiplurality => tern2cart(antiplurality_3a(v)...),
        :borda => tern2cart(borda_3a(v)...),
    )
end

function tetrahedron_points_from_profile(p4, q_functions)
    v = validate_profile_counts(p4, 24)
    point_for(f) = barycentric_to_cartesian(f(v))

    if q_functions isa AbstractDict
        return Dict(k => point_for(f) for (k, f) in q_functions)
    elseif q_functions isa NamedTuple
        return Dict(Symbol(k) => point_for(getfield(q_functions, k)) for k in keys(q_functions))
    else
        return [point_for(f) for f in q_functions]
    end
end

function opened_tetrahedron_vertices()
    h = sqrt(3) / 2
    return (
        A = (0.0, 0.0),
        B = (1.0, 0.0),
        C = (0.5, h),
        D_left = (-0.5, h),
        D_right = (1.5, h),
        D_bottom = (0.5, -h),
    )
end

function midpoint(p1, p2)
    length(p1) == length(p2) || throw(ArgumentError("points must have the same dimension"))
    return ntuple(i -> (Float64(p1[i]) + Float64(p2[i])) / 2, length(p1))
end
