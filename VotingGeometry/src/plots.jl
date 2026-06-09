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
    v = validate_profile_counts(p4, 24)
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

const _COMPONENT_GROUP_ORDER = (
    :departure_differentials,
    :subset_departures,
    :basic_profile_differentials,
    :condorcet,
    :kernel,
    :double_reversals,
)

function _component_summary_row(
    label_or_group,
    coefficient,
    component;
    norm = :l2,
    label = nothing,
    group = nothing,
    description = nothing,
    source = nothing,
    source_index = nothing,
)
    l1_norm = sum(abs, component)
    l2_norm = LinearAlgebra.norm(component)
    max_abs = maximum(abs, component)
    norm_value = norm == :l1 ? l1_norm : norm == :l2 ? l2_norm : max_abs
    return (
        label_or_group = label_or_group,
        coefficient = coefficient,
        l1_norm = l1_norm,
        l2_norm = l2_norm,
        max_abs = max_abs,
        norm_value = norm_value,
        sum_component = sum(component),
        label = label,
        group = group,
        description = description,
        source = source,
        source_index = source_index,
    )
end

function _check_component_label_style(label_style)
    label_style in (:symbol, :description) ||
        throw(ArgumentError("label_style must be :symbol or :description"))
    return label_style
end

function _component_label_or_description(meta, label_style)
    label_style == :symbol && return meta.label
    return meta.description
end

function _group_label_or_description(group, label_style)
    label_style == :symbol && return group
    return _COMPONENT_GROUP_DESCRIPTIONS[group]
end

"""
    component_summary(dec::Decomposition; by=:label, norm=:l2, label_style=:symbol)

Return vector-of-NamedTuple summaries for each decomposition label or aggregate
group. Group rows use `coefficient = nothing` because group components combine
multiple basis coefficients. `norm_value` stores the selected norm requested by
`norm`.

With `label_style=:description`, `label_or_group` uses source-anchored metadata
text instead of the canonical symbol. The canonical `label`, `group`,
`description`, `source`, and `source_index` fields remain available in each row.
"""
function component_summary(dec::Decomposition; by = :label, norm = :l2, label_style = :symbol)
    norm in (:l1, :l2, :max_abs) ||
        throw(ArgumentError("norm must be :l1, :l2, or :max_abs"))
    _check_component_label_style(label_style)

    if by == :label
        return [
            let meta = component_metadata(COMPONENT_LABELS[i])
                _component_summary_row(
                    _component_label_or_description(meta, label_style),
                    dec.coefficients[i],
                    dec.components[:, i];
                    norm = norm,
                    label = meta.label,
                    group = meta.group,
                    description = meta.description,
                    source = meta.source,
                    source_index = meta.source_index,
                )
            end
            for i in eachindex(COMPONENT_LABELS)
        ]
    elseif by == :group
        return [
            _component_summary_row(
                _group_label_or_description(group, label_style),
                nothing,
                group_component(dec, group);
                norm = norm,
                group = group,
                description = _COMPONENT_GROUP_DESCRIPTIONS[group],
            )
            for group in _COMPONENT_GROUP_ORDER
        ]
    end

    throw(ArgumentError("by must be :label or :group"))
end

"""
    plot_decomposition_coefficients(dec::Decomposition; by=:label,
        label_style=:symbol, ax=nothing, title="Saari decomposition coefficients")

Plot basis coefficients by label, or aggregate group L2 norms when `by=:group`.
Use `label_style=:description` to draw source-anchored metadata descriptions on
the x axis. Returns the PythonPlot axis.
"""
function plot_decomposition_coefficients(
    dec::Decomposition;
    by = :label,
    label_style = :symbol,
    ax = nothing,
    title = "Saari decomposition coefficients",
)
    ax = _axis2d(ax)
    rows = component_summary(dec; by = by, label_style = label_style)
    names = [string(row.label_or_group) for row in rows]
    xs = collect(1:length(rows))

    if by == :label
        values = [Float64(row.coefficient) for row in rows]
        ylabel = "coefficient"
        colors = [value >= 0 ? "tab:blue" : "tab:red" for value in values]
    elseif by == :group
        values = [row.l2_norm for row in rows]
        ylabel = "group L2 norm"
        colors = fill("tab:blue", length(values))
    else
        throw(ArgumentError("by must be :label or :group"))
    end

    ax.bar(xs, values; color = colors, alpha = 0.82)
    ax.axhline(0.0; color = "black", linewidth = 0.8)
    ax.set_xticks(xs)
    ax.set_xticklabels(names; rotation = 60, ha = "right")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    return ax
end

function _signed_profile_text_value(x)
    xf = Float64(x)
    isapprox(xf, 0.0; atol = 1e-12) && return "0"
    rounded = round(xf; digits = 3)
    return xf > 0 ? string("+", rounded) : string(rounded)
end

"""
    plot_signed_profile_tetrahedron(p4; ax=nothing, labels=("A", "B", "C", "D"),
        symmetric_scale=true, annotate=true, title="")

Plot a signed length-24 component on the opened tetrahedron layout. Positive and
negative entries use different colors and marker magnitudes; this helper is for
signed decomposition components, not profile frequencies.
"""
function plot_signed_profile_tetrahedron(
    p4;
    ax = nothing,
    labels = ("A", "B", "C", "D"),
    symmetric_scale = true,
    annotate = true,
    title = "",
)
    v = validate_profile_differential(p4, 24)
    ax = draw_opened_tetrahedron(ax; labels = labels)
    max_abs = maximum(abs, v)
    scale = max_abs == 0.0 ? 1.0 : max_abs
    symmetric_scale || (scale = maximum(abs, v) == 0.0 ? 1.0 : maximum(abs, v))

    for i in 1:24
        pos = TETRAHEDRON_TEXT_POSITIONS[i]
        value = v[i]
        color = value > 1e-12 ? "tab:blue" : value < -1e-12 ? "tab:red" : "0.65"
        marker_size = 18.0 + 260.0 * abs(value) / scale
        ax.scatter(
            [pos[1]],
            [pos[2]];
            s = marker_size,
            color = color,
            alpha = 0.25,
            edgecolors = "none",
        )
        if annotate
            ax.text(
                pos[1],
                pos[2],
                _signed_profile_text_value(value),
                ha = "center",
                va = "center",
                fontsize = 8,
                color = color,
            )
        end
    end

    !isempty(title) && ax.set_title(title)
    return ax
end

function _subplot_layout(n)
    ncols = min(3, n)
    nrows = cld(n, ncols)
    return nrows, ncols
end

function _created_subplot_axes(n; ax_or_fig = nothing)
    nrows, ncols = _subplot_layout(n)
    if ax_or_fig === nothing
        fig = PythonPlot.figure(figsize = (4.0 * ncols, 3.4 * nrows))
        axes = [fig.add_subplot(nrows, ncols, i) for i in 1:(nrows * ncols)]
        for i in (n + 1):length(axes)
            axes[i].axis("off")
        end
        return fig, axes[1:n]
    end

    if ax_or_fig isa AbstractArray
        axes = vec(collect(ax_or_fig))
        length(axes) >= n || throw(ArgumentError("not enough axes provided"))
        return nothing, axes[1:n]
    end

    try
        axes = [ax_or_fig.add_subplot(nrows, ncols, i) for i in 1:n]
        return ax_or_fig, axes
    catch
        n == 1 || throw(ArgumentError("single axis provided for a multi-panel plot"))
        return nothing, [ax_or_fig]
    end
end

function _prefixed_title(prefix, title)
    return isempty(prefix) ? title : string(prefix, title)
end

"""
    plot_decomposition_component_tetrahedra(dec::Decomposition;
        groups=keys(COMPONENT_GROUPS), labels=("A", "B", "C", "D"), ax_or_fig=nothing,
        title_prefix="")

Show the original profile and selected reconstructed group components on opened
tetrahedra. Components are plotted with `plot_signed_profile_tetrahedron` because
they can contain negative entries.
"""
function plot_decomposition_component_tetrahedra(
    dec::Decomposition;
    groups = keys(COMPONENT_GROUPS),
    labels = ("A", "B", "C", "D"),
    ax_or_fig = nothing,
    title_prefix = "",
)
    group_vec = collect(groups)
    nplots = 1 + length(group_vec)
    fig, axes = _created_subplot_axes(nplots; ax_or_fig = ax_or_fig)

    plot_signed_profile_tetrahedron(
        dec.profile;
        ax = axes[1],
        labels = labels,
        title = _prefixed_title(title_prefix, "original"),
    )
    for (i, group) in pairs(group_vec)
        plot_signed_profile_tetrahedron(
            group_component(dec, Symbol(group));
            ax = axes[i + 1],
            labels = labels,
            title = _prefixed_title(title_prefix, string(group)),
        )
    end

    return fig === nothing ? axes : fig
end

"""
    plot_decomposition_reconstruction_check(dec::Decomposition; labels=("A", "B", "C", "D"))

Plot original, reconstructed, and residual signed tetrahedra. The residual should
be numerically zero when the decomposition basis identity holds.
"""
function plot_decomposition_reconstruction_check(dec::Decomposition; labels = ("A", "B", "C", "D"))
    fig, axes = _created_subplot_axes(3)
    reconstructed = reconstruct(dec)
    residual = dec.profile .- reconstructed
    plot_signed_profile_tetrahedron(dec.profile; ax = axes[1], labels = labels, title = "original")
    plot_signed_profile_tetrahedron(reconstructed; ax = axes[2], labels = labels, title = "reconstructed")
    plot_signed_profile_tetrahedron(residual; ax = axes[3], labels = labels, title = "residual")
    return fig
end

const _EJPE_BOLSONARO_POSITIONAL_COMPARISONS = (
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

"""
    ejpe_bolsonaro_comparison_specs()

Return the explicit positional comparison specs used by the EJPE Bolsonaro 2018
paper examples. Generic comparison-region APIs default to all unordered
pairwise weak comparisons among the supplied labels; pass this helper via
`comparisons=ejpe_bolsonaro_comparison_specs()` for the paper-specific claims.
"""
function ejpe_bolsonaro_comparison_specs()
    return collect(_EJPE_BOLSONARO_POSITIONAL_COMPARISONS)
end

function _default_positional_comparisons(label_vec)
    comparisons = Tuple{Any,Symbol,Any}[]
    for i in 1:(length(label_vec) - 1)
        for j in (i + 1):length(label_vec)
            push!(comparisons, (label_vec[i], Symbol(">="), label_vec[j]))
        end
    end
    return comparisons
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

"""
    positional_comparison_region_masks(p4, labels; comparisons=nothing, resolution=101)

Sample diagnostic grid masks over `0 <= s2 <= s1 <= 1` for four-candidate
positional comparisons. When `comparisons === nothing`, all unordered pairwise
weak comparisons are used in label order: `label_i >= label_j` for `i < j`.
Pass explicit comparisons for paper-specific claims, for example
`comparisons=ejpe_bolsonaro_comparison_specs()`.
"""
function positional_comparison_region_masks(p4, labels; comparisons = nothing, resolution::Integer = 101)
    resolution >= 2 || throw(ArgumentError("resolution must be at least 2"))
    v = validate_profile_counts(p4, 24)
    label_vec, label_index = _label_index_map(labels)
    comparison_input = comparisons === nothing ? _default_positional_comparisons(label_vec) : comparisons
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

"""
    positional_comparison_region_table(p4, labels; comparisons=nothing, resolution=101)

Summarize `positional_comparison_region_masks` as diagnostic grid
proportions over `0 <= s2 <= s1 <= 1`. Use `grid_proportion` for the sampled
approximation; use `positional_comparison_region_exact_table` for rigorous
parameter-space area proportions. When `comparisons === nothing`, all
unordered pairwise weak comparisons are used in label order. Pass explicit
comparisons for paper-specific claims.
"""
function positional_comparison_region_table(p4, labels; comparisons = nothing, resolution::Integer = 101)
    region_data = positional_comparison_region_masks(
        p4,
        labels;
        comparisons = comparisons,
        resolution = resolution,
    )
    grid_count = length(region_data.s1)
    return [
        let true_count = count(region_data.masks[i]),
            grid_proportion = count(region_data.masks[i]) / grid_count
            (
                comparison = spec.label,
                left = spec.left,
                op = spec.op,
                right = spec.right,
                true_count = true_count,
                grid_count = grid_count,
                grid_proportion = grid_proportion,
                parameter_space_proportion = grid_proportion,
            )
        end
        for (i, spec) in pairs(region_data.comparisons)
    ]
end

function _comparison_region_colors(n, colors)
    colors === nothing && return [_POSITIONAL_COMPARISON_COLORS[mod1(i, length(_POSITIONAL_COMPARISON_COLORS))] for i in 1:n]
    color_vec = collect(colors)
    length(color_vec) >= n || throw(ArgumentError("colors must provide at least one color per comparison"))
    return color_vec
end

"""
    plot_positional_comparison_regions(p4, labels; comparisons=nothing, resolution=101, ax=nothing, ...)

Plot diagnostic grid comparison regions over `0 <= s2 <= s1 <= 1`. When
`comparisons === nothing`, all unordered pairwise weak comparisons are used in
label order. Pass explicit comparisons, such as
`ejpe_bolsonaro_comparison_specs()`, for paper-specific plots.
"""
function plot_positional_comparison_regions(
    p4,
    labels;
    comparisons = nothing,
    resolution::Integer = 101,
    ax = nothing,
    alpha = 0.35,
    colors = nothing,
    markersize = nothing,
    title = "Diagnostic grid comparison regions over Saari parameter space",
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

function plot_candidate_tally_tetrahedron(; labels = ("A", "B", "C", "D"), ax = nothing)
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

plot_saari_tetrahedron3d(; labels = ("A", "B", "C", "D"), ax = nothing) =
    plot_candidate_tally_tetrahedron(; labels = labels, ax = ax)

plot_opened_representation_tetrahedron(ax = nothing; labels = ("A", "B", "C", "D")) =
    draw_opened_tetrahedron(ax; labels = labels)

plot_profile_on_opened_tetrahedron(p4; kwargs...) = plot_profile_tetrahedron_freqs(p4; kwargs...)

function _closed_coordinate(points, dim)
    values = [Float64(point[dim]) for point in points]
    push!(values, Float64(points[1][dim]))
    return values
end

"""
    plot_procedure_hull_q_image_4c(p4; labels=("A", "B", "C", "D"), ax=nothing,
        show_borda=true, show_vertices=true, annotate=true,
        title="q-normalized image of the 4-candidate Saari procedure hull")

Draw the q-normalized image of the four-candidate Saari procedure hull inside
the candidate-tally tetrahedron. The triangle vertices and Borda point are true
candidate score shares, so this is a display embedding of the procedure hull,
not Saari's score-vector parameter triangle.
"""
function plot_procedure_hull_q_image_4c(
    p4;
    labels = ("A", "B", "C", "D"),
    ax = nothing,
    show_borda = true,
    show_vertices = true,
    annotate = true,
    title = "q-normalized image of the 4-candidate Saari procedure hull",
)
    hull = procedure_hull_q_image_4c(p4; labels = labels)
    ax = plot_candidate_tally_tetrahedron(; labels = labels, ax = ax)
    points = [
        hull.vertices_cartesian.vote_for_one,
        hull.vertices_cartesian.vote_for_two,
        hull.vertices_cartesian.vote_for_three,
    ]
    xs = [point[1] for point in points]
    ys = [point[2] for point in points]
    zs = [point[3] for point in points]

    try
        ax.plot_trisurf(xs, ys, zs; color = "tab:cyan", alpha = 0.22, linewidth = 0.5, edgecolor = "0.45")
    catch
        # Degenerate profiles can collapse the hull triangle; the outline and
        # markers still communicate the endpoint locations.
    end
    ax.plot(
        _closed_coordinate(points, 1),
        _closed_coordinate(points, 2),
        _closed_coordinate(points, 3);
        color = "tab:cyan",
        linewidth = 1.6,
    )

    if show_vertices
        vertex_specs = (
            (points[1], "vote-for-one", "tab:blue"),
            (points[2], "vote-for-two", "tab:orange"),
            (points[3], "vote-for-three", "tab:green"),
        )
        for (point, label, color) in vertex_specs
            ax.scatter([point[1]], [point[2]], [point[3]]; color = color, s = 38, label = label)
            annotate && ax.text(point[1], point[2], point[3], label)
        end
    end

    if show_borda
        point = hull.borda_cartesian
        ax.scatter([point[1]], [point[2]], [point[3]]; color = "black", s = 46, marker = "x", label = "Borda")
        annotate && ax.text(point[1], point[2], point[3], "Borda")
    end

    !isempty(title) && ax.set_title(title)
    try
        ax.legend(loc = "upper left", bbox_to_anchor = (1.02, 1.0))
    catch
    end
    return ax
end

"""
    plot_procedure_hull_4c(p4; labels=("A", "B", "C", "D"), ax=nothing,
        convention=:saari, show_borda=true, show_vertices=true, annotate=true,
        title="q-normalized image of the 4-candidate Saari procedure hull")

Compatibility wrapper for `plot_procedure_hull_q_image_4c`. Despite the legacy
name, this draws the q-normalized display image inside the candidate-tally
tetrahedron, not the Saari score-vector procedure triangle. The `convention`
keyword is accepted only for migration compatibility.
"""
function plot_procedure_hull_4c(
    p4;
    labels = ("A", "B", "C", "D"),
    ax = nothing,
    convention = :saari,
    show_borda = true,
    show_vertices = true,
    annotate = true,
    title = "q-normalized image of the 4-candidate Saari procedure hull",
)
    _normalize_hull_convention(convention)
    return plot_procedure_hull_q_image_4c(
        p4;
        labels = labels,
        ax = ax,
        show_borda = show_borda,
        show_vertices = show_vertices,
        annotate = annotate,
        title = title,
    )
end

"""
    plot_procedure_hull_parameter_triangle(; ax=nothing, show_borda=true)

Draw the Saari score-parameter triangle `0 <= s2 <= s1 <= 1`. This is a
parameter-space plot, not a uniform-area plot of the procedure hull q-image.
"""
function plot_procedure_hull_parameter_triangle(; ax = nothing, show_borda = true)
    ax = _axis2d(ax)
    points = [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0)]
    xs = _closed_coordinate(points, 1)
    ys = _closed_coordinate(points, 2)
    ax.fill(xs, ys; color = "0.85", alpha = 0.35)
    ax.plot(xs, ys; color = "black", linewidth = 1.1)

    endpoint_specs = (
        ((0.0, 0.0), "plurality"),
        ((1.0, 0.0), "vote-for-two"),
        ((1.0, 1.0), "antiplurality"),
    )
    for (point, label) in endpoint_specs
        ax.scatter([point[1]], [point[2]]; color = "tab:blue", s = 28)
        ax.text(point[1], point[2], label; ha = "left", va = "bottom")
    end

    if show_borda
        borda = (2 / 3, 1 / 3)
        ax.scatter([borda[1]], [borda[2]]; color = "black", marker = "x", s = 42, label = "Borda")
        ax.text(borda[1], borda[2], "Borda"; ha = "left", va = "bottom")
    end

    ax.set_xlabel("s1: second-place score")
    ax.set_ylabel("s2: third-place score")
    ax.set_xlim(-0.04, 1.08)
    ax.set_ylim(-0.04, 1.08)
    ax.set_title("4-candidate Saari parameter space")
    return _set_equal_2d(ax)
end
