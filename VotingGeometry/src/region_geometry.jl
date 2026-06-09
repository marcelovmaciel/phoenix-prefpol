const _POSITIONAL_PARAMETER_TRIANGLE_4C = [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0)]
const _POSITIONAL_PARAMETER_TRIANGLE_AREA_4C = 0.5

function _dedupe_polygon_vertices(points; atol = 1e-12)
    isempty(points) && return Tuple{Float64,Float64}[]
    deduped = Tuple{Float64,Float64}[]
    for point in points
        p = (Float64(point[1]), Float64(point[2]))
        if isempty(deduped) ||
                !(isapprox(p[1], deduped[end][1]; atol = atol, rtol = 0.0) &&
                  isapprox(p[2], deduped[end][2]; atol = atol, rtol = 0.0))
            push!(deduped, p)
        end
    end
    if length(deduped) > 1 &&
            isapprox(deduped[1][1], deduped[end][1]; atol = atol, rtol = 0.0) &&
            isapprox(deduped[1][2], deduped[end][2]; atol = atol, rtol = 0.0)
        pop!(deduped)
    end
    return deduped
end

_affine_value_2d(point, c0, c1, c2) =
    Float64(c0) + Float64(c1) * Float64(point[1]) + Float64(c2) * Float64(point[2])

function _segment_halfplane_intersection_2d(p, q, fp, fq)
    denom = fp - fq
    denom == 0.0 && return (Float64(p[1]), Float64(p[2]))
    t = fp / denom
    return (
        Float64(p[1]) + t * (Float64(q[1]) - Float64(p[1])),
        Float64(p[2]) + t * (Float64(q[2]) - Float64(p[2])),
    )
end

"""
    clip_polygon_halfplane_2d(polygon, c0, c1, c2; atol=1e-12)

Clip a two-dimensional polygon by the closed affine half-plane
`c0 + c1*x + c2*y >= 0`. Vertices are returned as `(x, y)` Float64 tuples in
the clipped polygon order.
"""
function clip_polygon_halfplane_2d(polygon, c0, c1, c2; atol = 1e-12)
    input = _dedupe_polygon_vertices(polygon; atol = atol)
    isempty(input) && return Tuple{Float64,Float64}[]

    output = Tuple{Float64,Float64}[]
    previous = input[end]
    previous_value = _affine_value_2d(previous, c0, c1, c2)
    previous_inside = previous_value >= -atol

    for current in input
        current_value = _affine_value_2d(current, c0, c1, c2)
        current_inside = current_value >= -atol

        if current_inside
            if !previous_inside
                push!(output, _segment_halfplane_intersection_2d(previous, current, previous_value, current_value))
            end
            push!(output, current)
        elseif previous_inside
            push!(output, _segment_halfplane_intersection_2d(previous, current, previous_value, current_value))
        end

        previous = current
        previous_value = current_value
        previous_inside = current_inside
    end

    return _dedupe_polygon_vertices(output; atol = atol)
end

"""
    polygon_area_2d(polygon)

Return the unsigned shoelace area of a two-dimensional polygon. Degenerate
inputs with fewer than three vertices have area zero.
"""
function polygon_area_2d(polygon)
    points = [(Float64(p[1]), Float64(p[2])) for p in polygon]
    length(points) >= 3 || return 0.0
    double_area = 0.0
    for i in eachindex(points)
        j = i == lastindex(points) ? firstindex(points) : i + 1
        double_area += points[i][1] * points[j][2] - points[j][1] * points[i][2]
    end
    return abs(double_area) / 2
end

function _line_polygon_intersection_2d(polygon, c0, c1, c2; atol = 1e-12)
    input = _dedupe_polygon_vertices(polygon; atol = atol)
    isempty(input) && return Tuple{Float64,Float64}[]

    points = Tuple{Float64,Float64}[]
    for i in eachindex(input)
        p = input[i]
        q = i == lastindex(input) ? input[firstindex(input)] : input[i + 1]
        fp = _affine_value_2d(p, c0, c1, c2)
        fq = _affine_value_2d(q, c0, c1, c2)
        abs(fp) <= atol && push!(points, p)
        if fp * fq < -atol^2
            push!(points, _segment_halfplane_intersection_2d(p, q, fp, fq))
        elseif abs(fp) <= atol && abs(fq) <= atol
            push!(points, q)
        end
    end
    return _dedupe_polygon_vertices(points; atol = atol)
end

function _is_zero_affine(c0, c1, c2; atol = 1e-12)
    return abs(Float64(c0)) <= atol && abs(Float64(c1)) <= atol && abs(Float64(c2)) <= atol
end

"""
    positional_score_affine_coefficients_4c(p4, labels, comparison)

Return the affine raw-score difference coefficients for a four-candidate
comparison over Saari's admissible positional-method triangle:
`Delta(s1, s2) = c0 + c1*s1 + c2*s2`.

`c0`, `c1`, and `c2` are respectively the top-, second-, and third-position
count differences between the left and right candidates. The returned named
tuple also includes the normalized comparison metadata.
"""
function positional_score_affine_coefficients_4c(p4, labels, comparison)
    v = validate_profile_counts(p4, 24)
    _, label_index = _label_index_map(labels)
    spec = _normalize_positional_comparison(comparison, label_index)
    coeffs = zeros(Float64, 3)

    @inbounds for (col, order) in pairs(CANONICAL_4C_IDS)
        weight = v[col]
        weight == 0.0 && continue
        for position in 1:3
            candidate = order[position]
            if candidate == spec.left_index
                coeffs[position] += weight
            elseif candidate == spec.right_index
                coeffs[position] -= weight
            end
        end
    end

    return (c0 = coeffs[1], c1 = coeffs[2], c2 = coeffs[3], comparison = spec)
end

function _comparison_region_coefficients(coeffs, op::Symbol)
    c0, c1, c2 = coeffs.c0, coeffs.c1, coeffs.c2
    if op == Symbol(">=") || op == Symbol(">")
        return (c0 = c0, c1 = c1, c2 = c2)
    elseif op == Symbol("<=") || op == Symbol("<")
        return (c0 = -c0, c1 = -c1, c2 = -c2)
    end
    throw(ArgumentError("comparison operator $op is not a half-plane inequality"))
end

function _empty_region_result(spec, coeffs, inequality_coeffs, boundary, region_kind)
    return (
        comparison = spec.label,
        left = spec.left,
        op = spec.op,
        right = spec.right,
        coefficients = (c0 = coeffs.c0, c1 = coeffs.c1, c2 = coeffs.c2),
        inequality_coefficients = inequality_coeffs,
        polygon = Tuple{Float64,Float64}[],
        boundary = boundary,
        area = 0.0,
        parameter_space_proportion = 0.0,
        region_kind = region_kind,
    )
end

"""
    positional_comparison_region_exact(p4, labels; comparison, atol=1e-12)

Compute the exact parameter-space region where a four-candidate positional
comparison holds over `0 <= s2 <= s1 <= 1`. The returned named tuple includes
the normalized comparison metadata, affine coefficients, polygon vertices,
area, and `parameter_space_proportion = area / 0.5`.

The common q-share denominator is positive, so raw-score affine differences
determine the same inequality. Nondegenerate strict inequalities (`>` and `<`)
use the closed half-plane closure for polygon area because the excluded boundary
has measure zero. If the affine difference is identically zero, strict
comparisons are false everywhere. Equality (`==`) returns the whole triangle
when the affine difference is identically zero; otherwise it returns the
zero-area boundary segment, if any.
"""
function positional_comparison_region_exact(p4, labels; comparison, atol = 1e-12)
    coeffs = positional_score_affine_coefficients_4c(p4, labels, comparison)
    spec = coeffs.comparison
    triangle = _POSITIONAL_PARAMETER_TRIANGLE_4C
    zero_affine = _is_zero_affine(coeffs.c0, coeffs.c1, coeffs.c2; atol = atol)

    if spec.op == Symbol("==")
        if zero_affine
            polygon = copy(triangle)
            area = polygon_area_2d(polygon)
            return (
                comparison = spec.label,
                left = spec.left,
                op = spec.op,
                right = spec.right,
                coefficients = (c0 = coeffs.c0, c1 = coeffs.c1, c2 = coeffs.c2),
                inequality_coefficients = nothing,
                polygon = polygon,
                boundary = polygon,
                area = area,
                parameter_space_proportion = area / _POSITIONAL_PARAMETER_TRIANGLE_AREA_4C,
                region_kind = :identically_true,
            )
        end

        boundary = _line_polygon_intersection_2d(
            triangle,
            coeffs.c0,
            coeffs.c1,
            coeffs.c2;
            atol = atol,
        )
        return _empty_region_result(spec, coeffs, nothing, boundary, :zero_area_boundary)
    end

    inequality_coeffs = _comparison_region_coefficients(coeffs, spec.op)
    if zero_affine && (spec.op == Symbol(">") || spec.op == Symbol("<"))
        return _empty_region_result(spec, coeffs, inequality_coeffs, triangle, :strict_identically_false)
    end

    polygon = clip_polygon_halfplane_2d(
        triangle,
        inequality_coeffs.c0,
        inequality_coeffs.c1,
        inequality_coeffs.c2;
        atol = atol,
    )
    area = polygon_area_2d(polygon)
    return (
        comparison = spec.label,
        left = spec.left,
        op = spec.op,
        right = spec.right,
        coefficients = (c0 = coeffs.c0, c1 = coeffs.c1, c2 = coeffs.c2),
        inequality_coefficients = inequality_coeffs,
        polygon = polygon,
        boundary = Tuple{Float64,Float64}[],
        area = area,
        parameter_space_proportion = area / _POSITIONAL_PARAMETER_TRIANGLE_AREA_4C,
        region_kind = spec.op == Symbol(">") || spec.op == Symbol("<") ? :strict_closure_area : :closed_halfplane,
    )
end

"""
    positional_comparison_region_exact_table(p4, labels; comparisons=nothing, atol=1e-12)

Return exact area/proportion rows for positional comparison regions over
Saari's four-candidate score-parameter triangle. Unlike
`positional_comparison_region_table`, these proportions are polygon areas, not
grid counts. When `comparisons === nothing`, all unordered pairwise weak
comparisons are used in label order. Pass explicit comparisons for
paper-specific claims.
"""
function positional_comparison_region_exact_table(p4, labels; comparisons = nothing, atol = 1e-12)
    label_vec, _ = _label_index_map(labels)
    comparison_input = comparisons === nothing ? _default_positional_comparisons(label_vec) : comparisons
    return [
        positional_comparison_region_exact(p4, labels; comparison = comparison, atol = atol)
        for comparison in comparison_input
    ]
end

"""
    plot_positional_comparison_regions_exact(p4, labels; comparisons=nothing, ax=nothing, ...)

Plot exact half-plane-clipped polygons for positional comparison regions over
Saari's parameter triangle `0 <= s2 <= s1 <= 1`. Zero-area equality boundaries are drawn as line segments
when present; positive-area regions are filled. When `comparisons === nothing`,
all unordered pairwise weak comparisons are used in label order. Pass explicit
comparisons, such as `ejpe_bolsonaro_comparison_specs()`, for paper-specific
plots.
"""
function plot_positional_comparison_regions_exact(
    p4,
    labels;
    comparisons = nothing,
    ax = nothing,
    alpha = 0.35,
    colors = nothing,
    linewidth = 1.4,
    title = "Exact half-plane clipped regions over Saari parameter space",
    atol = 1e-12,
)
    ax = _axis2d(ax)
    region_data = positional_comparison_region_exact_table(
        p4,
        labels;
        comparisons = comparisons,
        atol = atol,
    )
    region_colors = _comparison_region_colors(length(region_data), colors)

    for i in eachindex(region_data)
        region = region_data[i]
        color = region_colors[i]
        if length(region.polygon) >= 3 && region.area > 0.0
            xs = _closed_coordinate(region.polygon, 1)
            ys = _closed_coordinate(region.polygon, 2)
            ax.fill(xs, ys; color = color, alpha = alpha, label = region.comparison)
            ax.plot(xs, ys; color = color, linewidth = linewidth)
        elseif length(region.boundary) >= 2
            xs = [point[1] for point in region.boundary]
            ys = [point[2] for point in region.boundary]
            ax.plot(xs, ys; color = color, linewidth = linewidth, label = region.comparison)
        end
    end

    boundary = _closed_coordinate(_POSITIONAL_PARAMETER_TRIANGLE_4C, 1), _closed_coordinate(_POSITIONAL_PARAMETER_TRIANGLE_4C, 2)
    ax.plot(boundary[1], boundary[2]; color = "black", linewidth = 1)
    ax.set_xlabel("s1: second-place score")
    ax.set_ylabel("s2: third-place score")
    ax.set_xlim(-0.02, 1.02)
    ax.set_ylim(-0.02, 1.02)
    ax.set_title(title)
    try
        ax.legend(loc = "upper left", bbox_to_anchor = (1.02, 1.0))
    catch
    end
    return _set_equal_2d(ax)
end
