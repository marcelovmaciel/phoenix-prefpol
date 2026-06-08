function _axis2d(ax)
    ax !== nothing && return ax
    _, new_ax = PythonPlot.subplots()
    return new_ax
end

function _axis3d(ax)
    ax !== nothing && return ax
    fig = PythonPlot.figure()
    return fig.add_subplot(111, projection = "3d")
end

function _as_label(x)
    return x isa Symbol ? String(x) : string(x)
end

function _triangle_centroid(points)
    length(points) == 3 || throw(ArgumentError("triangle centroid requires three points"))
    dim = length(points[1])
    return ntuple(d -> sum(Float64(p[d]) for p in points) / 3, dim)
end

function _triangle_median_segments(points)
    length(points) == 3 || throw(ArgumentError("triangle medians require three points"))
    return [
        (points[1], midpoint(points[2], points[3])),
        (points[2], midpoint(points[1], points[3])),
        (points[3], midpoint(points[1], points[2])),
    ]
end

function _plot_triangle_medians(ax, points; kwargs...)
    for (vertex, opposite_midpoint) in _triangle_median_segments(points)
        ax.plot(
            [vertex[1], opposite_midpoint[1]],
            [vertex[2], opposite_midpoint[2]];
            kwargs...,
        )
    end
    return ax
end

function _opened_tetrahedron_faces(vertices = opened_tetrahedron_vertices())
    return (
        (vertices.A, vertices.B, vertices.C),
        (vertices.A, vertices.C, vertices.D_left),
        (vertices.B, vertices.C, vertices.D_right),
        (vertices.A, vertices.B, vertices.D_bottom),
    )
end

function _opened_tetrahedron_median_segments(vertices = opened_tetrahedron_vertices())
    return reduce(vcat, (_triangle_median_segments(face) for face in _opened_tetrahedron_faces(vertices)))
end

function _set_equal_2d(ax)
    ax.set_aspect("equal", adjustable = "box")
    return ax
end

function _plot_polygon(ax, points; kwargs...)
    xs = [p[1] for p in points]
    ys = [p[2] for p in points]
    push!(xs, points[1][1])
    push!(ys, points[1][2])
    ax.plot(xs, ys; kwargs...)
    return ax
end

function draw_plain_triangle(ax = nothing; labels = ("A", "B", "C"))
    ax = _axis2d(ax)
    _plot_polygon(ax, TRIANGLE_VERTICES; color = "black", linewidth = 1.4)
    _plot_triangle_medians(ax, TRIANGLE_VERTICES; color = "0.65", linewidth = 0.8)
    offsets = [(-0.04, -0.04), (0.03, -0.04), (0.0, 0.04)]
    for i in 1:3
        vertex = TRIANGLE_VERTICES[i]
        ax.text(
            vertex[1] + offsets[i][1],
            vertex[2] + offsets[i][2],
            _as_label(labels[i]),
            ha = "center",
            va = "center",
        )
    end
    ax.set_xlim(-0.08, 1.08)
    ax.set_ylim(-0.08, sqrt(3) / 2 + 0.12)
    ax.axis("off")
    return _set_equal_2d(ax)
end

function plot_saari_triangle(p3; labels = ("A", "B", "C"), ax = nothing)
    ax = draw_plain_triangle(ax; labels = labels)
    pts = triangle_points_from_profile(p3)

    plurality = pts[:plurality]
    antiplurality = pts[:antiplurality]
    borda = pts[:borda]

    ax.plot(
        [plurality[1], antiplurality[1]],
        [plurality[2], antiplurality[2]],
        color = "0.45",
        linestyle = "--",
        linewidth = 1.0,
        label = "Positional procedure line",
    )

    point_specs = (
        (:plurality, "Plurality", "tab:blue"),
        (:antiplurality, "Antiplurality", "tab:orange"),
        (:borda, "Borda", "tab:green"),
    )
    for (key, label, color) in point_specs
        point = pts[key]
        ax.scatter([point[1]], [point[2]], color = color, s = 32, label = label)
    end
    ax.legend(loc = "upper left", bbox_to_anchor = (1.02, 1.0))
    return ax
end

function draw_opened_tetrahedron(ax = nothing; labels = ("A", "B", "C", "D"))
    ax = _axis2d(ax)
    vertices = opened_tetrahedron_vertices()
    triangles = _opened_tetrahedron_faces(vertices)
    for tri in triangles
        _plot_polygon(ax, collect(tri); color = "black", linewidth = 1.0)
        _plot_triangle_medians(ax, tri; color = "0.68", linewidth = 0.7)
    end

    ax.text(vertices.A[1] - 0.04, vertices.A[2] - 0.04, _as_label(labels[1]), ha = "right", va = "top")
    ax.text(vertices.B[1] + 0.04, vertices.B[2] - 0.04, _as_label(labels[2]), ha = "left", va = "top")
    ax.text(vertices.C[1], vertices.C[2] + 0.05, _as_label(labels[3]), ha = "center", va = "bottom")
    for dpos in (vertices.D_left, vertices.D_right, vertices.D_bottom)
        ax.text(dpos[1], dpos[2], _as_label(labels[4]), ha = "center", va = "center")
    end
    ax.set_xlim(-0.62, 1.62)
    ax.set_ylim(-0.98, 1.0)
    ax.axis("off")
    return _set_equal_2d(ax)
end

function _profile_text_value(x, coerce_to_int)
    if coerce_to_int
        return string(round(Int, Float64(x)))
    end
    xf = Float64(x)
    return isinteger(xf) ? string(Int(xf)) : string(round(xf; digits = 3))
end

function plot_profile_tetrahedron_freqs(p4; ax = nothing, coerce_to_int = true, textsize = 9, title = "")
    v = validate_profile_vector(p4, 24)
    ax = draw_opened_tetrahedron(ax)
    for i in 1:24
        pos = TETRAHEDRON_TEXT_POSITIONS[i]
        ax.text(
            pos[1],
            pos[2],
            _profile_text_value(v[i], coerce_to_int),
            ha = "center",
            va = "center",
            fontsize = textsize,
        )
    end
    !isempty(title) && ax.set_title(title)
    return ax
end

const _DEFAULT_POSITIONAL_COMPARISONS = (
    ("Bolsonaro", Symbol(">="), "Ciro"),
    ("Ciro", Symbol(">="), "Haddad"),
    ("Alckmin", Symbol(">"), "Bolsonaro"),
)

const _POSITIONAL_COMPARISON_COLORS = (
    "tab:blue",
    "tab:orange",
    "tab:green",
    "tab:red",
    "tab:purple",
    "tab:brown",
    "tab:pink",
    "tab:gray",
)

function _comparison_op_label(op::Symbol)
    op == Symbol(">=") && return ">="
    op == Symbol(">") && return ">"
    op == Symbol("<=") && return "<="
    op == Symbol("<") && return "<"
    op == Symbol("==") && return "=="
    throw(ArgumentError("unsupported comparison operator $op"))
end

function _normalize_comparison_operator(op)
    op_string = string(op)
    op_string in (">=", "ge", "GE") && return Symbol(">=")
    op_string in (">", "gt", "GT") && return Symbol(">")
    op_string in ("<=", "le", "LE") && return Symbol("<=")
    op_string in ("<", "lt", "LT") && return Symbol("<")
    op_string in ("==", "=", "eq", "EQ") && return Symbol("==")
    throw(ArgumentError("unsupported comparison operator $op"))
end

function _compare_scores(left, op::Symbol, right)
    op == Symbol(">=") && return left >= right
    op == Symbol(">") && return left > right
    op == Symbol("<=") && return left <= right
    op == Symbol("<") && return left < right
    op == Symbol("==") && return left == right
    throw(ArgumentError("unsupported comparison operator $op"))
end

function _label_index_map(labels)
    label_vec = collect(labels)
    length(label_vec) == 4 || throw(ArgumentError("four candidate labels are required"))

    index = Dict{String,Int}()
    for (i, label) in pairs(label_vec)
        label_key = _as_label(label)
        haskey(index, label_key) && throw(ArgumentError("candidate labels must be unique"))
        index[label_key] = i
    end
    return label_vec, index
end

function _default_positional_comparisons(label_index)
    required_labels = ("Bolsonaro", "Ciro", "Haddad", "Alckmin")
    if all(label -> haskey(label_index, label), required_labels)
        return _DEFAULT_POSITIONAL_COMPARISONS
    end
    throw(ArgumentError("pass comparisons when labels do not include Bolsonaro, Ciro, Haddad, and Alckmin"))
end

function _comparison_parts(comparison)
    if comparison isa Pair
        return comparison.first, Symbol(">="), comparison.second
    elseif comparison isa Tuple
        if length(comparison) == 2
            return comparison[1], Symbol(">="), comparison[2]
        elseif length(comparison) == 3
            return comparison[1], comparison[2], comparison[3]
        end
    end
    throw(ArgumentError("comparisons must be pairs or tuples like (:A, :B) or (:A, >=, :B)"))
end

function _normalize_positional_comparison(comparison, label_index)
    left, op, right = _comparison_parts(comparison)
    left_label = _as_label(left)
    right_label = _as_label(right)
    haskey(label_index, left_label) ||
        throw(ArgumentError("unknown candidate label $left_label in comparison"))
    haskey(label_index, right_label) ||
        throw(ArgumentError("unknown candidate label $right_label in comparison"))

    op_symbol = _normalize_comparison_operator(op)
    return (
        left = left_label,
        op = op_symbol,
        right = right_label,
        left_index = label_index[left_label],
        right_index = label_index[right_label],
        label = string(left_label, " ", _comparison_op_label(op_symbol), " ", right_label),
    )
end

function positional_comparison_region_masks(p4, labels; comparisons = nothing, resolution::Integer = 101)
    resolution >= 2 || throw(ArgumentError("resolution must be at least 2"))
    v = validate_profile_vector(p4, 24)
    _, label_index = _label_index_map(labels)
    comparison_input = comparisons === nothing ? _default_positional_comparisons(label_index) : comparisons
    comparison_specs = [_normalize_positional_comparison(comp, label_index) for comp in comparison_input]

    s1_values = Float64[]
    s2_values = Float64[]
    masks = [BitVector() for _ in comparison_specs]
    grid = range(0, 1; length = resolution)

    for s1 in grid, s2 in grid
        s1f = Float64(s1)
        s2f = Float64(s2)
        if s2f <= s1f
            scores = q_s_4candidates(v, s1f, s2f)
            push!(s1_values, s1f)
            push!(s2_values, s2f)
            for (i, spec) in pairs(comparison_specs)
                push!(masks[i], _compare_scores(scores[spec.left_index], spec.op, scores[spec.right_index]))
            end
        end
    end

    return (s1 = s1_values, s2 = s2_values, comparisons = comparison_specs, masks = masks)
end

function _comparison_region_colors(n, colors)
    colors === nothing && return [_POSITIONAL_COMPARISON_COLORS[mod1(i, length(_POSITIONAL_COMPARISON_COLORS))] for i in 1:n]
    color_vec = collect(colors)
    length(color_vec) >= n || throw(ArgumentError("colors must provide at least one color per comparison"))
    return color_vec
end

function plot_positional_comparison_regions(
    p4,
    labels;
    comparisons = nothing,
    resolution::Integer = 101,
    ax = nothing,
    alpha = 0.35,
    colors = nothing,
    markersize = nothing,
    title = "Comparison regions over 0 <= s2 <= s1 <= 1",
)
    ax = _axis2d(ax)
    region_data = positional_comparison_region_masks(
        p4,
        labels;
        comparisons = comparisons,
        resolution = resolution,
    )
    region_colors = _comparison_region_colors(length(region_data.comparisons), colors)
    point_size = markersize === nothing ? max(4, 20000 / (resolution * resolution)) : markersize

    for i in eachindex(region_data.comparisons)
        mask = region_data.masks[i]
        ax.scatter(
            region_data.s1[mask],
            region_data.s2[mask];
            color = region_colors[i],
            alpha = alpha,
            s = point_size,
            marker = "s",
            linewidths = 0,
            label = region_data.comparisons[i].label,
        )
    end

    ax.plot([0, 1, 1, 0], [0, 0, 1, 0]; color = "black", linewidth = 1)
    ax.set_xlabel("s1: second-place score")
    ax.set_ylabel("s2: third-place score")
    ax.set_xlim(-0.02, 1.02)
    ax.set_ylim(-0.02, 1.02)
    ax.set_title(title)
    ax.legend(loc = "upper left", bbox_to_anchor = (1.02, 1.0))
    return _set_equal_2d(ax)
end

function plot_saari_tetrahedron3d(; labels = ("A", "B", "C", "D"), ax = nothing)
    ax = _axis3d(ax)
    vertices = TETRAHEDRON_VERTICES
    for i in 1:3
        for j in (i + 1):4
            ax.plot(
                [vertices[i][1], vertices[j][1]],
                [vertices[i][2], vertices[j][2]],
                [vertices[i][3], vertices[j][3]],
                color = "black",
                linewidth = 1.0,
            )
        end
    end
    for i in 1:4
        vertex = vertices[i]
        ax.scatter([vertex[1]], [vertex[2]], [vertex[3]], color = "tab:blue", s = 24)
        ax.text(vertex[1], vertex[2], vertex[3], _as_label(labels[i]))
    end
    ax.set_xlabel("x")
    ax.set_ylabel("y")
    ax.set_zlabel("z")
    try
        ax.set_box_aspect((1, 1, 1))
    catch
    end
    return ax
end
