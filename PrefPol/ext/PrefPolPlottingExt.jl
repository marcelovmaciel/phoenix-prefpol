module PrefPolPlottingExt

using CairoMakie
using DataFrames
using Dates
using PrefPol
using Printf
using Statistics: median, quantile
using TextWrap

const Makie = CairoMakie.Makie
const _cairomakie_save = CairoMakie.save

function __init__()
    if !isdefined(PrefPol, :Makie)
        Core.eval(PrefPol, :(const Makie = $Makie))
    end

    if !isdefined(PrefPol, :save)
        Core.eval(PrefPol, :(save(args...; kwargs...) = $_cairomakie_save(args...; kwargs...)))
    end
end

@inline _wong_colors() = Makie.wong_colors()

@inline _measure_label(measure::Symbol) =
    measure === :Psi ? "Ψ" :
    measure === :D_median ? "D" :
    String(measure)

const _CANONICAL_GROUP_HEATMAP_MEASURES = (:C, :D_median, :O, :Gsep)
const _CANONICAL_GROUP_HEATMAP_COMPLEMENTS = (:O,)
const _CANONICAL_GROUP_HEATMAP_LABELS = Dict(
    :C => "C",
    :D_median => "D",
    :O => "1 - O",
    :Gsep => "G",
)
const _GROUP_TRIPLET_PANEL_MEASURES = (:C, :O, :S)
const _GROUP_TRIPLET_PANEL_COMPLEMENTS = (:O,)
const _GROUP_TRIPLET_PANEL_LABELS = Dict(
    :C => "C",
    :O => "1 - O",
    :S => "S",
)
const _GROUP_TRIPLET_PANEL_COLORRANGE = (0.0f0, 1.0f0)
const _PAPER_GROUP_HEATMAP_MEASURES = (:C, :D, :O, :S)
const _PAPER_GROUP_HEATMAP_COMPLEMENTS = (:O,)
const _PAPER_GROUP_HEATMAP_LABELS = Dict(
    :C => "C",
    :D => "D",
    :O => "1 - O",
    :S => "S",
)
const _PAPER_O_SMOOTHED_MEASURES = (:O_smoothed,)
const _PAPER_O_SMOOTHED_COMPLEMENTS = (:O_smoothed,)
const _PAPER_O_SMOOTHED_LABELS = Dict(:O_smoothed => "1 - O_smoothed")
const _PAPER_GROUP_HEATMAP_COLORRANGE = (0.0f0, 1.0f0)
const _VARIANCE_DOTWHISKER_COMPONENTS = (:bootstrap, :imputation, :linearization, :total)
const _VARIANCE_DOTWHISKER_COMPONENT_LABELS = Dict(
    :bootstrap => "Bootstrap",
    :imputation => "Imputation",
    :linearization => "Linearization",
    :total => "Total",
)
const _VARIANCE_COMPONENT_OFFSETS = Dict(
    1 => [0.0],
    2 => [-0.16, 0.16],
    3 => [-0.24, 0.0, 0.24],
    4 => [-0.30, -0.10, 0.10, 0.30],
)

function _plot_measure_label(measure::Symbol, measure_labels)
    if measure_labels !== nothing && haskey(measure_labels, measure)
        return String(measure_labels[measure])
    end

    return _measure_label(measure)
end

function _normalized_measure_set(measures)
    measures === nothing && return Set{Symbol}()
    raw = measures isa Symbol || measures isa AbstractString ? (measures,) : Tuple(measures)
    return Set(Symbol(measure) for measure in raw)
end

@inline _is_canonical_group_heatmap(measures) =
    Tuple(PrefPol._normalize_measure_list(measures)) == _CANONICAL_GROUP_HEATMAP_MEASURES

function _heatmap_panel_matrix(data, measure::Symbol, complement_measures::Set{Symbol})
    matrix = Float32.(data.matrices[measure])
    return measure in complement_measures ? Float32.(1.0 .- matrix) : matrix
end

function _resolve_heatmap_colorrange(allvals;
                                     fixed_colorrange::Bool = false,
                                     fixed_colorrange_limits = nothing)
    data_min, data_max = isempty(allvals) ? (0.0f0, 1.0f0) : extrema(allvals)

    if fixed_colorrange_limits !== nothing
        lo, hi = fixed_colorrange_limits
        lo < hi || throw(ArgumentError("fixed_colorrange_limits must satisfy lo < hi."))
        return (Float32(lo), Float32(hi)), data_min, data_max
    end

    colorrange = fixed_colorrange ? (0.0f0, 1.0f0) : (data_min, data_max)
    return colorrange, data_min, data_max
end

function _overlay_heatmap_values!(ax, xs_m, n_groups::Int, z)
    n_m = length(xs_m)
    xs = repeat(xs_m, inner = n_groups)
    ys = repeat(collect(1:n_groups), outer = n_m)
    labels = [isnan(z[i, j]) ? "NA" : @sprintf("%.3f", z[i, j]) for i in 1:n_m for j in 1:n_groups]
    text!(ax, xs, ys; text = labels, align = (:center, :center), color = :black, fontsize = 8)
    return nothing
end

function _add_heatmap_colorbar!(parent_layout, row::Int, col::Int, hm_ref, data_min, data_max;
                                label::AbstractString)
    cbgrid = GridLayout()
    parent_layout[row, col] = cbgrid
    Label(cbgrid[1, 1]; text = "max found = $(round(data_max; digits = 3))", halign = :center)
    Colorbar(cbgrid[2, 1], hm_ref; label = label)
    Label(cbgrid[3, 1]; text = "min found = $(round(data_min; digits = 3))", halign = :center)
    rowsize!(cbgrid, 1, Auto(0.15))
    rowsize!(cbgrid, 3, Auto(0.15))
    return cbgrid
end

function _plot_title(rows::AbstractDataFrame;
                     year = nothing,
                     scenario_name = nothing,
                     imputer_backend = nothing)
    year_label = year === nothing ?
                 (hasproperty(rows, :year) ? string(rows[1, :year]) : string(rows[1, :wave_id])) :
                 string(year)
    scenario_label = scenario_name === nothing && hasproperty(rows, :scenario_name) ?
                     string(rows[1, :scenario_name]) :
                     string(scenario_name)
    backend_label = imputer_backend === nothing ?
                    string(rows[1, :imputer_backend]) :
                    String(Symbol(imputer_backend))
    return string(
        "Year ",
        year_label,
        " • scenario = ",
        scenario_label,
        " • backend = ",
        backend_label,
        " • B = ",
        rows[1, :B],
        ", R = ",
        rows[1, :R],
        ", K = ",
        rows[1, :K],
        " • draws = ",
        rows[1, :n_draws],
    )
end

function _plot_paper_title(rows::AbstractDataFrame;
                           year = nothing,
                           wave_id = nothing)
    year_label = year === nothing ?
                 (hasproperty(rows, :year) ? string(rows[1, :year]) :
                  wave_id === nothing ? string(rows[1, :wave_id]) : string(wave_id)) :
                 string(year)
    return string(
        "Year ",
        year_label,
        " • B = ",
        rows[1, :B],
        ", R = ",
        rows[1, :R],
        ", K = ",
        rows[1, :K],
    )
end

function lines_alt_by_variant(measures_over_m::AbstractDict;
                              variants = PrefPol.DEFAULT_PIPELINE_IMPUTATION_VARIANTS,
                              palette = nothing,
                              figsize = (1000, 900),
                              candidate_label::String = "",
                              year)
    palette === nothing && (palette = _wong_colors())

    ms = sort(collect(keys(measures_over_m)))
    measures = sort(collect(keys(first(values(measures_over_m)))))
    sample_variant_map = first(values(first(values(measures_over_m))))
    variant_syms = collect(PrefPol._select_available_imputation_variants(
        keys(sample_variant_map),
        variants;
        context = "scenario measures",
    ))
    nv = length(variant_syms)

    mlabels = Dict(
        :calc_reversal_HHI => "HHI",
        :calc_total_reversal_component => "R",
        :fast_reversal_geometric => "RHHI",
    )

    fig = Figure(resolution = figsize)
    rowgap!(fig.layout, 18)
    colgap!(fig.layout, 4)
    fig[1, 2] = GridLayout()
    colsize!(fig.layout, 2, 100)

    first_m, last_m = first(ms), last(ms)
    first_var = first(variant_syms)
    n_bootstrap = length(sample_variant_map[first_var])

    titlegrid = GridLayout(tellwidth = true)
    fig[0, 1] = titlegrid
    Label(
        titlegrid[1, 1];
        text = "Year = $(year)   •   Number of alternatives = $first_m … $last_m   •   $n_bootstrap pseudo-profiles",
        fontsize = 18,
        halign = :left,
    )
    Label(titlegrid[2, 1]; text = candidate_label, fontsize = 14, halign = :left)

    axes = [Axis(
        fig[i, 1];
        title = string(variant_syms[i]),
        xlabel = "number of alternatives",
        ylabel = "value",
        yticks = (0:0.1:1, string.(0:0.1:1)),
        xticks = (ms, string.(ms)),
    ) for i in 1:nv]

    legend_handles = Lines[]
    legend_labels = AbstractString[]

    for (row, var) in enumerate(variant_syms)
        ax = axes[row]

        for (j, meas) in enumerate(measures)
            col = palette[(j - 1) % length(palette) + 1]

            meds = Float64[]
            q25s = Float64[]
            q75s = Float64[]
            p05s = Float64[]
            p95s = Float64[]

            for m in ms
                vals = measures_over_m[m][meas][var]
                push!(meds, median(vals))
                push!(q25s, quantile(vals, 0.25))
                push!(q75s, quantile(vals, 0.75))
                push!(p05s, quantile(vals, 0.05))
                push!(p95s, quantile(vals, 0.95))
            end

            band!(ax, ms, p05s, p95s; color = (col, 0.12))
            band!(ax, ms, q25s, q75s; color = (col, 0.25))

            ln = lines!(ax, ms, meds; color = col, label = get(mlabels, meas, string(meas)))

            if row == 1
                push!(legend_handles, ln)
                push!(legend_labels, get(mlabels, meas, string(meas)))
            end
        end
    end

    Legend(fig[1:nv, 2], legend_handles, legend_labels)
    resize_to_layout!(fig)
    return fig
end

function dotwhisker_alt_by_variant(measures_over_m::AbstractDict;
                                   variants = PrefPol.DEFAULT_PIPELINE_IMPUTATION_VARIANTS,
                                   palette = nothing,
                                   figsize = (1000, 900),
                                   candidate_label::String = "",
                                   year,
                                   whiskerwidth_outer = 10,
                                   whiskerwidth_inner = 6,
                                   linewidth_outer = 1.5,
                                   linewidth_inner = 3.0,
                                   dot_size = 8,
                                   dodge = 0.18,
                                   connect_lines::Bool = false,
                                   connect_linewidth = 1.5)
    palette === nothing && (palette = _wong_colors())

    ms = sort(collect(keys(measures_over_m)))
    measures = sort(collect(keys(first(values(measures_over_m)))))
    sample_variant_map = first(values(first(values(measures_over_m))))
    variant_syms = collect(PrefPol._select_available_imputation_variants(
        keys(sample_variant_map),
        variants;
        context = "scenario measures",
    ))
    nv = length(variant_syms)

    mlabels = Dict(
        :calc_reversal_HHI => "HHI",
        :calc_total_reversal_component => "R",
        :fast_reversal_geometric => "RHHI",
    )

    fig = Figure(resolution = figsize)
    rowgap!(fig.layout, 18)
    colgap!(fig.layout, 4)
    fig[1, 2] = GridLayout()
    colsize!(fig.layout, 2, 100)

    first_m, last_m = first(ms), last(ms)
    first_var = first(variant_syms)
    n_bootstrap = length(sample_variant_map[first_var])

    titlegrid = GridLayout(tellwidth = true)
    fig[0, 1] = titlegrid
    Label(
        titlegrid[1, 1];
        text = "Year = $(year)   •   Number of alternatives = $first_m … $last_m   •   $n_bootstrap pseudo-profiles",
        fontsize = 18,
        halign = :left,
    )
    Label(titlegrid[2, 1]; text = candidate_label, fontsize = 14, halign = :left)

    axes = [Axis(
        fig[i, 1];
        title = string(variant_syms[i]),
        xlabel = "number of alternatives",
        ylabel = "value",
        yticks = (0:0.1:1, string.(0:0.1:1)),
        xticks = (ms, string.(ms)),
    ) for i in 1:nv]

    legend_handles = Any[]
    legend_labels = AbstractString[]

    spacing = length(ms) > 1 ? float(minimum(diff(ms))) : 1.0
    offsets = length(measures) > 1 ? collect(range(-dodge, dodge; length = length(measures))) : [0.0]

    for (row, var) in enumerate(variant_syms)
        ax = axes[row]

        for (j, meas) in enumerate(measures)
            col = palette[(j - 1) % length(palette) + 1]
            xpos = ms .+ offsets[j] * spacing

            meds = Float64[]
            q25s = Float64[]
            q75s = Float64[]
            p05s = Float64[]
            p95s = Float64[]

            for m in ms
                vals = measures_over_m[m][meas][var]
                push!(meds, median(vals))
                push!(q25s, quantile(vals, 0.25))
                push!(q75s, quantile(vals, 0.75))
                push!(p05s, quantile(vals, 0.05))
                push!(p95s, quantile(vals, 0.95))
            end

            rangebars!(
                ax,
                xpos,
                p05s,
                p95s;
                direction = :y,
                color = (col, 0.6),
                linewidth = linewidth_outer,
                whiskerwidth = whiskerwidth_outer,
            )

            rangebars!(
                ax,
                xpos,
                q25s,
                q75s;
                direction = :y,
                color = (col, 0.9),
                linewidth = linewidth_inner,
                whiskerwidth = whiskerwidth_inner,
            )

            sc = scatter!(
                ax,
                xpos,
                meds;
                color = col,
                markersize = dot_size,
                label = get(mlabels, meas, string(meas)),
            )

            connect_lines && lines!(ax, xpos, meds; color = col, linewidth = connect_linewidth)

            if row == 1
                push!(legend_handles, sc)
                push!(legend_labels, get(mlabels, meas, string(meas)))
            end
        end
    end

    Legend(fig[1:nv, 2], legend_handles, legend_labels)
    resize_to_layout!(fig)
    return fig
end

function plot_scenario_year(year,
                            scenario,
                            f3,
                            all_meas;
                            variant = "mice",
                            palette = nothing,
                            figsize = (500, 400),
                            plot_kind::Symbol = :lines,
                            connect_lines::Bool = false)
    f3_entry = f3[year]
    cfg = f3_entry.cfg
    meas_map = all_meas[year][scenario]
    scen_obj = PrefPol._lookup_scenario(cfg, scenario)

    candidate_label = PrefPol.describe_candidate_set(PrefPol._full_candidate_list(cfg, scen_obj))

    if plot_kind == :lines
        return lines_alt_by_variant(
            meas_map;
            variants = [variant],
            palette = palette,
            figsize = figsize,
            year = year,
            candidate_label = candidate_label,
        )
    elseif plot_kind == :dotwhisker
        return dotwhisker_alt_by_variant(
            meas_map;
            variants = [variant],
            palette = palette,
            figsize = figsize,
            year = year,
            candidate_label = candidate_label,
            connect_lines = connect_lines,
        )
    end

    error("Unknown plot_kind=$(plot_kind). Use :lines or :dotwhisker.")
end

function plot_group_demographics_lines(all_gm,
                                       f3,
                                       year::Int,
                                       scenario::String;
                                       variants = PrefPol.DEFAULT_PIPELINE_IMPUTATION_VARIANTS,
                                       measures = [:C, :D, :G],
                                       maxcols::Int = 3,
                                       n_yticks::Int = 5,
                                       ytick_step = nothing,
                                       palette = nothing,
                                       clist_size = 60,
                                       demographics = f3[year].cfg.demographics)
    palette === nothing && (palette = _wong_colors())

    gm = all_gm[year][scenario]
    m_values_int = sort(collect(keys(gm)))
    xs_m = Float32.(m_values_int)
    n_demo = length(demographics)

    scenobj = PrefPol._lookup_scenario(f3[year].cfg, scenario)
    cand_lbl = PrefPol.describe_candidate_set(PrefPol._full_candidate_list(f3[year].cfg, scenobj))

    sample_slice = gm[first(m_values_int)][Symbol(first(demographics))]
    variant_syms = collect(PrefPol._select_available_imputation_variants(
        keys(sample_slice),
        variants;
        context = "group metrics for year $year",
    ))
    n_boot = length(sample_slice[variant_syms[1]][:C])

    measure_cols = Dict(measures[i] => palette[i] for i in eachindex(measures))
    variant_styles = Dict(:zero => :solid, :random => :dash, :mice => :dot)

    ncol = min(maxcols, n_demo)
    nrow = ceil(Int, n_demo / ncol)
    title_txt = "Year $(year) • $(n_boot) pseudo-profiles • m = $(first(m_values_int)) … $(last(m_values_int))"
    fig_width = max(300 * ncol, 10 * length(title_txt) + 60)
    fig_height = 300 * nrow

    fig = Figure(resolution = (fig_width, fig_height))
    rowgap!(fig.layout, 24)
    colgap!(fig.layout, 24)

    fig[1, 1:ncol] = Label(fig, title_txt; fontsize = 20, halign = :left)
    fig[2, 1:ncol] = Label(fig, join(TextWrap.wrap("$cand_lbl"; width = clist_size)); fontsize = 14, halign = :left)
    header_rows = 2

    legend_handles = Any[]
    legend_labels = String[]

    for (idx, demo) in enumerate(demographics)
        r, c = fldmod1(idx, ncol)
        ax = Axis(
            fig[r + header_rows, c];
            title = demo,
            xlabel = "number of alternatives",
            ylabel = "value",
            xticks = (xs_m, string.(m_values_int)),
        )

        allvals = Float32[]

        for meas in measures, var in variant_syms
            vals_per_m = map(m_values_int) do m
                v = gm[m][Symbol(demo)][var]
                arr = meas === :G ? sqrt.(v[:C] .* v[:D]) : v[meas]
                Float32.(arr)
            end

            append!(allvals, vcat(vals_per_m...))

            meds32 = Float32.(median.(vals_per_m))
            q25s32 = Float32.(map(x -> quantile(x, 0.25f0), vals_per_m))
            q75s32 = Float32.(map(x -> quantile(x, 0.75f0), vals_per_m))

            col = measure_cols[meas]
            sty = variant_styles[var]

            band!(ax, xs_m, q25s32, q75s32; color = (col, 0.20))
            ln = lines!(ax, xs_m, meds32; color = col, linestyle = sty)

            if idx == 1
                push!(legend_handles, ln)
                push!(legend_labels, "$(meas) • $(var)")
            end
        end

        y_min, y_max = extrema(allvals)
        ticks = if ytick_step === nothing
            collect(range(y_min, y_max; length = n_yticks))
        else
            ytick_step <= 0 && error("ytick_step must be > 0")
            computed = collect(y_min:ytick_step:y_max)
            length(computed) < 2 ? collect(range(y_min, y_max; length = 2)) : computed
        end
        ax.yticks[] = (ticks, string.(round.(ticks; digits = 3)))
    end

    Legend(fig[header_rows + 1:header_rows + nrow, ncol + 1], legend_handles, legend_labels; tellheight = false)
    colsize!(fig.layout, ncol + 1, Relative(0.25))

    resize_to_layout!(fig)
    return fig
end

function plot_group_demographics_heatmap(all_gm,
                                         f3,
                                         year::Int,
                                         scenario::String;
                                         variants = PrefPol.DEFAULT_PIPELINE_IMPUTATION_VARIANTS,
                                         measures = [:C, :D, :G],
                                         modified_C::Bool = false,
                                         modified_G::Bool = false,
                                         groupings = f3[year].cfg.demographics,
                                         maxcols::Int = 3,
                                         colormap = :viridis,
                                         colormaps = nothing,
                                         fixed_colorrange::Bool = false,
                                         show_values::Bool = false,
                                         simplified_labels::Bool = false,
                                         clist_size = 60)
    colormaps !== nothing && (colormap = colormaps)

    gm = all_gm[year][scenario]
    m_values_int = sort(collect(keys(gm)))
    xs_m = Float32.(m_values_int)

    groupings_vec = collect(groupings)
    group_syms = Symbol.(groupings_vec)
    group_labels = string.(groupings_vec)
    n_groups = length(groupings_vec)

    scenobj = PrefPol._lookup_scenario(f3[year].cfg, scenario)
    cand_lbl = PrefPol.describe_candidate_set(PrefPol._full_candidate_list(f3[year].cfg, scenobj))

    sample_slice = gm[first(m_values_int)][group_syms[1]]
    variant_syms = collect(PrefPol._select_available_imputation_variants(
        keys(sample_slice),
        variants;
        context = "group metrics for year $year",
    ))
    n_boot = length(sample_slice[variant_syms[1]][:C])

    panel_vals = Dict{Tuple{Symbol,Symbol}, Matrix{Float32}}()
    allvals = Float32[]

    for meas in measures, var in variant_syms
        z = Matrix{Float32}(undef, length(m_values_int), n_groups)
        for (mi, m) in enumerate(m_values_int)
            for (gi, demo_sym) in enumerate(group_syms)
                v = gm[m][demo_sym][var]
                c_raw = v[:C]
                c_vals = modified_C ? (2 .* c_raw .- 1) : c_raw
                c_for_g = modified_G ? (2 .* c_raw .- 1) : c_vals

                arr = if meas === :G
                    sqrt.(max.(c_for_g, 0.0) .* v[:D])
                elseif meas === :C
                    c_vals
                else
                    v[meas]
                end
                z[mi, gi] = Float32(median(arr))
            end
        end
        panel_vals[(meas, var)] = z
        append!(allvals, vec(z))
    end

    data_min, data_max = extrema(allvals)
    data_labels = string.(round.([data_min, data_max]; digits = 3))
    min_label = "min found = $(data_labels[1])"
    max_label = "max found = $(data_labels[2])"
    colorrange = fixed_colorrange ? (0.0f0, 1.0f0) : (data_min, data_max)

    n_panels = length(measures) * length(variant_syms)
    ncol = min(maxcols, n_panels)
    nrow = ceil(Int, n_panels / ncol)
    title_txt = "Year $(year) • $(n_boot) pseudo-profiles • m = $(first(m_values_int)) … $(last(m_values_int))"
    fig_width = max(320 * ncol, 10 * length(title_txt) + 60)
    fig_height = 320 * nrow

    fig = Figure(resolution = (fig_width, fig_height))
    rowgap!(fig.layout, 24)
    colgap!(fig.layout, 24)

    fig[1, 1:ncol] = Label(fig, title_txt; fontsize = 20, halign = :left)
    fig[2, 1:ncol] = Label(fig, join(TextWrap.wrap("$cand_lbl"; width = clist_size)); fontsize = 14, halign = :left)
    header_rows = 2

    hm_ref = nothing
    panel_idx = 0
    for meas in measures, var in variant_syms
        panel_idx += 1
        r, c = fldmod1(panel_idx, ncol)
        default_xlabel = "number of alternatives"
        default_ylabel = "grouping"
        if simplified_labels && length(measures) > 1
            mid_idx = ceil(Int, length(measures) / 2)
            xlabel_txt = meas == measures[mid_idx] ? default_xlabel : ""
            ylabel_txt = meas == first(measures) ? default_ylabel : ""
        else
            xlabel_txt = default_xlabel
            ylabel_txt = default_ylabel
        end

        ax = Axis(
            fig[r + header_rows, c];
            title = "$(meas)",
            xlabel = xlabel_txt,
            ylabel = ylabel_txt,
            xticks = (xs_m, string.(m_values_int)),
            yticks = (1:n_groups, group_labels),
        )

        z = panel_vals[(meas, var)]
        hm = heatmap!(ax, xs_m, 1:n_groups, z; colormap = colormap, colorrange = colorrange)

        if show_values
            n_m = length(xs_m)
            xs = repeat(xs_m, inner = n_groups)
            ys = repeat(collect(1:n_groups), outer = n_m)
            labels = [@sprintf("%.3f", z[i, j]) for i in 1:n_m for j in 1:n_groups]
            text!(ax, xs, ys; text = labels, align = (:center, :center), color = :black, fontsize = 8)
        end

        hm_ref === nothing && (hm_ref = hm)
    end

    if hm_ref !== nothing
        cbgrid = GridLayout()
        fig[header_rows + 1:header_rows + nrow, ncol + 1] = cbgrid

        Label(cbgrid[1, 1]; text = max_label, halign = :center)
        Colorbar(cbgrid[2, 1], hm_ref; label = "median")
        Label(cbgrid[3, 1]; text = min_label, halign = :center)

        rowsize!(cbgrid, 1, Auto(0.15))
        rowsize!(cbgrid, 3, Auto(0.15))
        colsize!(fig.layout, ncol + 1, Relative(0.25))
    end

    resize_to_layout!(fig)
    return fig
end

function plot_pipeline_scenario(result_or_results;
                                year = nothing,
                                wave_id = nothing,
                                scenario_name = nothing,
                                imputer_backend = :mice,
                                measures = [:Psi, :R, :HHI, :RHHI],
                                palette = nothing,
                                figsize = (700, 450),
                                plot_kind::Symbol = :lines,
                                connect_lines::Bool = false,
                                ytick_step = nothing)
    palette === nothing && (palette = _wong_colors())
    data = PrefPol.pipeline_scenario_plot_data(
        result_or_results;
        year = year,
        wave_id = wave_id,
        scenario_name = scenario_name,
        imputer_backend = imputer_backend,
        measures = measures,
    )
    rows = data.rows
    m_values = Float32.(data.m_values)
    wanted_measures = [_measure for _measure in PrefPol._normalize_measure_list(measures)]

    fig = Figure(resolution = figsize)
    ax = Axis(
        fig[1, 1];
        xlabel = "number of alternatives",
        ylabel = "value",
        xticks = (m_values, string.(Int.(m_values))),
    )
    if ytick_step !== nothing
        ticks = collect(0.0:Float64(ytick_step):1.0)
        ax.yticks[] = (ticks, [@sprintf("%.1f", tick) for tick in ticks])
    end

    titlegrid = GridLayout(tellwidth = true)
    fig[0, 1] = titlegrid
    Label(
        titlegrid[1, 1];
        text = _plot_title(
            rows;
            year = year,
            scenario_name = scenario_name,
            imputer_backend = imputer_backend,
        ),
        fontsize = 18,
        halign = :left,
    )
    Label(
        titlegrid[2, 1];
        text = data.candidate_label,
        fontsize = 14,
        halign = :left,
    )

    legend_handles = Any[]
    legend_labels = String[]

    for (idx, measure) in enumerate(wanted_measures)
        subdf = sort(rows[rows.measure .== measure, :], :n_candidates)
        isempty(subdf) && continue
        col = palette[(idx - 1) % length(palette) + 1]
        xs = Float32.(subdf.n_candidates)
        estimate = Float32.(subdf.estimate)
        q05 = Float32.(subdf.q05)
        q25 = Float32.(subdf.q25)
        q75 = Float32.(subdf.q75)
        q95 = Float32.(subdf.q95)
        label = _measure_label(measure)

        if plot_kind === :lines
            band!(ax, xs, q05, q95; color = (col, 0.12))
            band!(ax, xs, q25, q75; color = (col, 0.25))
            ln = lines!(ax, xs, estimate; color = col, linewidth = 2.5)
            push!(legend_handles, ln)
            push!(legend_labels, label)
        elseif plot_kind === :dotwhisker
            rangebars!(
                ax,
                xs,
                q05,
                q95;
                direction = :y,
                color = (col, 0.55),
                linewidth = 1.5,
                whiskerwidth = 10,
            )
            rangebars!(
                ax,
                xs,
                q25,
                q75;
                direction = :y,
                color = (col, 0.9),
                linewidth = 3.0,
                whiskerwidth = 6,
            )
            sc = scatter!(ax, xs, estimate; color = col, markersize = 9)
            connect_lines && lines!(ax, xs, estimate; color = col, linewidth = 1.5)
            push!(legend_handles, sc)
            push!(legend_labels, label)
        else
            error("Unknown plot_kind=$(plot_kind). Use :lines or :dotwhisker.")
        end
    end

    Legend(fig[1, 2], legend_handles, legend_labels)
    resize_to_layout!(fig)
    return fig
end

function plot_pipeline_group_lines(result_or_results;
                                   year = nothing,
                                   wave_id = nothing,
                                   scenario_name = nothing,
                                   imputer_backend = :mice,
                                   measures = [:C, :D, :G],
                                   groupings = nothing,
                                   maxcols::Int = 3,
                                   n_yticks::Int = 5,
                                   ytick_step = nothing,
                                   palette = nothing,
                                   clist_size = 60)
    palette === nothing && (palette = _wong_colors())
    data = PrefPol.pipeline_group_plot_data(
        result_or_results;
        year = year,
        wave_id = wave_id,
        scenario_name = scenario_name,
        imputer_backend = imputer_backend,
        measures = measures,
        groupings = groupings,
    )
    rows = data.rows
    m_values_int = Int.(data.m_values)
    xs_m = Float32.(m_values_int)
    grouping_values = data.grouping_values
    n_groupings = length(grouping_values)
    wanted_measures = PrefPol._normalize_measure_list(measures)

    ncol = min(maxcols, n_groupings)
    nrow = ceil(Int, n_groupings / ncol)
    title_txt = _plot_title(
        rows;
        year = year,
        scenario_name = scenario_name,
        imputer_backend = imputer_backend,
    )

    fig = Figure(resolution = (max(320 * ncol, 10 * length(title_txt) + 60), 300 * nrow))
    rowgap!(fig.layout, 24)
    colgap!(fig.layout, 24)
    fig[1, 1:ncol] = Label(fig, title_txt; fontsize = 20, halign = :left)
    fig[2, 1:ncol] = Label(
        fig,
        join(TextWrap.wrap(data.candidate_label; width = clist_size));
        fontsize = 14,
        halign = :left,
    )
    header_rows = 2

    legend_handles = Any[]
    legend_labels = String[]

    for (idx, grouping) in enumerate(grouping_values)
        r, c = fldmod1(idx, ncol)
        ax = Axis(
            fig[r + header_rows, c];
            title = String(grouping),
            xlabel = "number of alternatives",
            ylabel = "value",
            xticks = (xs_m, string.(m_values_int)),
        )

        allvals = Float32[]

        for (measure_idx, measure) in enumerate(wanted_measures)
            subdf = sort(
                rows[(rows.measure .== measure) .& coalesce.(rows.grouping .== grouping, false), :],
                :n_candidates,
            )
            isempty(subdf) && continue
            col = palette[(measure_idx - 1) % length(palette) + 1]
            estimate = Float32.(subdf.estimate)
            q05 = Float32.(subdf.q05)
            q25 = Float32.(subdf.q25)
            q75 = Float32.(subdf.q75)
            q95 = Float32.(subdf.q95)

            append!(allvals, vcat(q05, q95, estimate))
            band!(ax, xs_m, q05, q95; color = (col, 0.10))
            band!(ax, xs_m, q25, q75; color = (col, 0.22))
            ln = lines!(ax, xs_m, estimate; color = col, linewidth = 2.3)

            if idx == 1
                push!(legend_handles, ln)
                push!(legend_labels, _measure_label(measure))
            end
        end

        if !isempty(allvals)
            y_min, y_max = extrema(allvals)
            ticks = if ytick_step === nothing
                collect(range(y_min, y_max; length = n_yticks))
            else
                ytick_step <= 0 && error("ytick_step must be > 0")
                computed = collect(y_min:ytick_step:y_max)
                length(computed) < 2 ? collect(range(y_min, y_max; length = 2)) : computed
            end
            ax.yticks[] = (ticks, string.(round.(ticks; digits = 3)))
        end
    end

    Legend(fig[header_rows + 1:header_rows + nrow, ncol + 1], legend_handles, legend_labels; tellheight = false)
    colsize!(fig.layout, ncol + 1, Relative(0.25))
    resize_to_layout!(fig)
    return fig
end

function plot_pipeline_group_heatmap(result_or_results;
                                     year = nothing,
                                     wave_id = nothing,
                                     scenario_name = nothing,
                                     imputer_backend = :mice,
                                     measures = collect(_CANONICAL_GROUP_HEATMAP_MEASURES),
                                     statistic::Symbol = :median,
                                     groupings = nothing,
                                     complement_measures = nothing,
                                     measure_labels = nothing,
                                     maxcols::Int = 2,
                                     colormap = Makie.Reverse(:RdBu),
                                     fixed_colorrange::Bool = false,
                                     fixed_colorrange_limits = nothing,
                                     show_values::Bool = false,
                                     colorbar_label = nothing,
                                     simplified_labels::Bool = false,
                                     clist_size = 60)
    data = PrefPol.pipeline_group_heatmap_values(
        result_or_results;
        year = year,
        wave_id = wave_id,
        scenario_name = scenario_name,
        imputer_backend = imputer_backend,
        measures = measures,
        groupings = groupings,
        statistic = statistic,
    )
    rows = data.rows
    m_values_int = Int.(data.m_values)
    xs_m = Float32.(m_values_int)
    group_syms = data.grouping_values
    group_label_map = Dict(:LulaScoreGroup => "Lula score")
    group_labels = [get(group_label_map, group, String(group)) for group in group_syms]
    wanted_measures = PrefPol._normalize_measure_list(measures)
    canonical_group_heatmap = _is_canonical_group_heatmap(wanted_measures)
    complement_measures === nothing && canonical_group_heatmap &&
        (complement_measures = _CANONICAL_GROUP_HEATMAP_COMPLEMENTS)
    measure_labels === nothing && canonical_group_heatmap &&
        (measure_labels = _CANONICAL_GROUP_HEATMAP_LABELS)
    complemented = _normalized_measure_set(complement_measures)

    allvals = Float32[]
    for measure in wanted_measures
        append!(allvals, Float32.(filter(!isnan, vec(_heatmap_panel_matrix(data, measure, complemented)))))
    end
    colorrange, data_min, data_max = _resolve_heatmap_colorrange(
        allvals;
        fixed_colorrange = fixed_colorrange,
        fixed_colorrange_limits = fixed_colorrange_limits,
    )

    n_panels = length(wanted_measures)
    ncol = min(maxcols, n_panels)
    nrow = ceil(Int, n_panels / ncol)
    title_txt = _plot_title(
        rows;
        year = year,
        scenario_name = scenario_name,
        imputer_backend = imputer_backend,
    )

    fig = Figure(resolution = (max(320 * ncol, 10 * length(title_txt) + 60), 320 * nrow))
    rowgap!(fig.layout, 24)
    colgap!(fig.layout, 24)
    fig[1, 1:ncol] = Label(fig, title_txt; fontsize = 20, halign = :left)
    fig[2, 1:ncol] = Label(
        fig,
        join(TextWrap.wrap(data.candidate_label; width = clist_size));
        fontsize = 14,
        halign = :left,
    )
    header_rows = 2

    hm_ref = nothing
    for (panel_idx, measure) in enumerate(wanted_measures)
        r, c = fldmod1(panel_idx, ncol)
        default_xlabel = "number of alternatives"
        default_ylabel = "grouping"
        if simplified_labels && length(wanted_measures) > 1
            mid_idx = ceil(Int, length(wanted_measures) / 2)
            xlabel_txt = panel_idx == mid_idx ? default_xlabel : ""
            ylabel_txt = panel_idx == 1 ? default_ylabel : ""
        else
            xlabel_txt = default_xlabel
            ylabel_txt = default_ylabel
        end

        ax = Axis(
            fig[r + header_rows, c];
            title = _plot_measure_label(measure, measure_labels),
            xlabel = xlabel_txt,
            ylabel = ylabel_txt,
            xticks = (xs_m, string.(m_values_int)),
            yticks = (1:length(group_syms), group_labels),
        )
        z = _heatmap_panel_matrix(data, measure, complemented)
        hm = heatmap!(ax, xs_m, 1:length(group_syms), z; colormap = colormap, colorrange = colorrange)

        if show_values
            n_m = length(xs_m)
            xs = repeat(xs_m, inner = length(group_syms))
            ys = repeat(collect(1:length(group_syms)), outer = n_m)
            labels = [isnan(z[i, j]) ? "NA" : @sprintf("%.3f", z[i, j]) for i in 1:n_m for j in 1:length(group_syms)]
            text!(ax, xs, ys; text = labels, align = (:center, :center), color = :black, fontsize = 8)
        end

        hm_ref === nothing && (hm_ref = hm)
    end

    if hm_ref !== nothing
        cbgrid = GridLayout()
        fig[header_rows + 1:header_rows + nrow, ncol + 1] = cbgrid
        Label(cbgrid[1, 1]; text = "max found = $(round(data_max; digits = 3))", halign = :center)
        Colorbar(cbgrid[2, 1], hm_ref; label = something(colorbar_label, String(data.statistic)))
        Label(cbgrid[3, 1]; text = "min found = $(round(data_min; digits = 3))", halign = :center)
        rowsize!(cbgrid, 1, Auto(0.15))
        rowsize!(cbgrid, 3, Auto(0.15))
        colsize!(fig.layout, ncol + 1, Relative(0.25))
    end

    resize_to_layout!(fig)
    return fig
end

function plot_pipeline_group_triplet_panel(result_or_results;
                                           year = nothing,
                                           wave_id = nothing,
                                           scenario_name = nothing,
                                           imputer_backend = :mice,
                                           statistic::Symbol = :median,
                                           groupings = nothing,
                                           colormap = Makie.Reverse(:RdBu),
                                           show_values::Bool = false,
                                           colorbar_label = nothing,
                                           simplified_labels::Bool = false,
                                           clist_size = 60)
    data = PrefPol.pipeline_group_heatmap_values(
        result_or_results;
        year = year,
        wave_id = wave_id,
        scenario_name = scenario_name,
        imputer_backend = imputer_backend,
        measures = collect(_GROUP_TRIPLET_PANEL_MEASURES),
        groupings = groupings,
        statistic = statistic,
    )
    rows = data.rows
    m_values_int = Int.(data.m_values)
    xs_m = Float32.(m_values_int)
    group_syms = data.grouping_values
    group_labels = string.(group_syms)
    complemented = _normalized_measure_set(_GROUP_TRIPLET_PANEL_COMPLEMENTS)

    # Replacement for the old compact grouped summary slot: plot C | 1 - O | S
    # on one shared [0, 1] scale so this specific panel is no longer split.
    matrices = Dict{Symbol,Matrix{Float32}}()
    allvals = Float32[]
    for measure in _GROUP_TRIPLET_PANEL_MEASURES
        z = clamp.(_heatmap_panel_matrix(data, measure, complemented), 0.0f0, 1.0f0)
        matrices[measure] = z
        append!(allvals, filter(!isnan, vec(z)))
    end
    data_min, data_max = isempty(allvals) ? _GROUP_TRIPLET_PANEL_COLORRANGE : extrema(allvals)

    title_txt = _plot_title(
        rows;
        year = year,
        scenario_name = scenario_name,
        imputer_backend = imputer_backend,
    )

    fig_height = max(420, 160 + 55 * length(group_syms))
    fig = Figure(size = (max(1180, 10 * length(title_txt) + 80), fig_height))
    rowgap!(fig.layout, 24)
    colgap!(fig.layout, 24)
    fig[1, 1:4] = Label(fig, title_txt; fontsize = 20, halign = :left)
    fig[2, 1:4] = Label(
        fig,
        join(TextWrap.wrap(data.candidate_label; width = clist_size));
        fontsize = 14,
        halign = :left,
    )

    body = GridLayout()
    fig[3, 1:4] = body
    colgap!(body, 24)

    hm_ref = nothing
    for (idx, measure) in enumerate(_GROUP_TRIPLET_PANEL_MEASURES)
        xlabel_txt = simplified_labels ? (idx == 2 ? "number of alternatives" : "") : "number of alternatives"
        ylabel_txt = simplified_labels ? (idx == 1 ? "grouping" : "") : "grouping"
        ax = Axis(
            body[1, idx];
            title = _GROUP_TRIPLET_PANEL_LABELS[measure],
            xlabel = xlabel_txt,
            ylabel = ylabel_txt,
            xticks = (xs_m, string.(m_values_int)),
            yticks = (1:length(group_syms), group_labels),
        )
        hm = heatmap!(
            ax,
            xs_m,
            1:length(group_syms),
            matrices[measure];
            colormap = colormap,
            colorrange = _GROUP_TRIPLET_PANEL_COLORRANGE,
        )
        show_values && _overlay_heatmap_values!(ax, xs_m, length(group_syms), matrices[measure])
        hm_ref === nothing && (hm_ref = hm)
    end

    _add_heatmap_colorbar!(
        body,
        1,
        4,
        hm_ref,
        data_min,
        data_max;
        label = something(colorbar_label, "grouped value"),
    )
    rowsize!(body, 1, Relative(1))
    colsize!(body, 4, Relative(0.24))

    resize_to_layout!(fig)
    return fig
end

function plot_pipeline_group_paper_heatmap(result_or_results;
                                           year = nothing,
                                           wave_id = nothing,
                                           scenario_name = nothing,
                                           imputer_backend = :mice,
                                           measures = collect(_PAPER_GROUP_HEATMAP_MEASURES),
                                           statistic::Symbol = :median,
                                           groupings = nothing,
                                           complement_measures = collect(_PAPER_GROUP_HEATMAP_COMPLEMENTS),
                                           measure_labels = _PAPER_GROUP_HEATMAP_LABELS,
                                           maxcols::Int = 2,
                                           colormap = Makie.Reverse(:RdBu),
                                           fixed_colorrange_limits = _PAPER_GROUP_HEATMAP_COLORRANGE,
                                           show_values::Bool = true,
                                           colorbar_label = nothing,
                                           clist_size = 60)
    data = PrefPol.pipeline_group_heatmap_values(
        result_or_results;
        year = year,
        wave_id = wave_id,
        scenario_name = scenario_name,
        imputer_backend = imputer_backend,
        measures = measures,
        groupings = groupings,
        statistic = statistic,
    )
    rows = data.rows
    m_values_int = Int.(data.m_values)
    xs_m = Float32.(m_values_int)
    group_syms = data.grouping_values
    group_labels = string.(group_syms)
    wanted_measures = PrefPol._normalize_measure_list(measures)
    complemented = _normalized_measure_set(complement_measures)

    lo, hi = fixed_colorrange_limits
    lo < hi || throw(ArgumentError("fixed_colorrange_limits must satisfy lo < hi."))
    clamp_lo = Float32(lo)
    clamp_hi = Float32(hi)

    matrices = Dict{Symbol,Matrix{Float32}}()
    allvals = Float32[]
    for measure in wanted_measures
        z = clamp.(_heatmap_panel_matrix(data, measure, complemented), clamp_lo, clamp_hi)
        matrices[measure] = z
        append!(allvals, filter(!isnan, vec(z)))
    end

    colorrange, _, _ = _resolve_heatmap_colorrange(
        allvals;
        fixed_colorrange_limits = fixed_colorrange_limits,
    )

    n_panels = length(wanted_measures)
    n_panels >= 1 || error("plot_pipeline_group_paper_heatmap requires at least one measure.")
    ncol = min(maxcols, n_panels)
    nrow = ceil(Int, n_panels / ncol)
    title_txt = _plot_paper_title(rows; year = year, wave_id = wave_id)

    fig_width = max(980, 360 * ncol + 160)
    fig_height = max(520, 190 + 210 * nrow + 30 * length(group_syms))
    fig = Figure(size = (fig_width, fig_height))
    rowgap!(fig.layout, 22)
    colgap!(fig.layout, 22)
    fig[1, 1:ncol] = Label(fig, title_txt; fontsize = 20, halign = :left)
    fig[2, 1:ncol] = Label(
        fig,
        join(TextWrap.wrap(data.candidate_label; width = clist_size));
        fontsize = 14,
        halign = :left,
    )
    header_rows = 2

    hm_ref = nothing
    for (panel_idx, measure) in enumerate(wanted_measures)
        r, c = fldmod1(panel_idx, ncol)
        show_xlabel = nrow == 1 || r == nrow
        show_ylabel = ncol == 1 || c == 1

        ax = Axis(
            fig[r + header_rows, c];
            title = _plot_measure_label(measure, measure_labels),
            xlabel = show_xlabel ? "m" : "",
            ylabel = show_ylabel ? "grouping" : "",
            xticks = (xs_m, string.(m_values_int)),
            yticks = (1:length(group_syms), group_labels),
        )
        hm = heatmap!(
            ax,
            xs_m,
            1:length(group_syms),
            matrices[measure];
            colormap = colormap,
            colorrange = colorrange,
        )
        show_values && _overlay_heatmap_values!(ax, xs_m, length(group_syms), matrices[measure])
        hm_ref === nothing && (hm_ref = hm)
    end

    if hm_ref !== nothing
        Colorbar(
            fig[header_rows + 1:header_rows + nrow, ncol + 1],
            hm_ref;
            label = something(colorbar_label, String(data.statistic)),
            ticks = 0.0:0.2:1.0,
        )
        colsize!(fig.layout, ncol + 1, Auto())
    end

    resize_to_layout!(fig)
    return fig
end

function plot_pipeline_group_paper_osmoothed_heatmap(result_or_results;
                                                     year = nothing,
                                                     wave_id = nothing,
                                                     scenario_name = nothing,
                                                     imputer_backend = :mice,
                                                     statistic::Symbol = :median,
                                                     groupings = nothing,
                                                     colormap = Makie.Reverse(:RdBu),
                                                     show_values::Bool = true,
                                                     colorbar_label = nothing,
                                                     clist_size = 60)
    return plot_pipeline_group_paper_heatmap(
        result_or_results;
        year = year,
        wave_id = wave_id,
        scenario_name = scenario_name,
        imputer_backend = imputer_backend,
        measures = collect(_PAPER_O_SMOOTHED_MEASURES),
        statistic = statistic,
        groupings = groupings,
        complement_measures = collect(_PAPER_O_SMOOTHED_COMPLEMENTS),
        measure_labels = _PAPER_O_SMOOTHED_LABELS,
        maxcols = 1,
        colormap = colormap,
        fixed_colorrange_limits = _PAPER_GROUP_HEATMAP_COLORRANGE,
        show_values = show_values,
        colorbar_label = colorbar_label,
        clist_size = clist_size,
    )
end

function _variance_plot_rows(pooled_table::AbstractDataFrame, measures, components, caller::AbstractString)
    required = [:measure, :component, :q25, :median, :q75]
    missing_cols = setdiff(required, Symbol.(names(pooled_table)))
    isempty(missing_cols) || throw(ArgumentError(
        "$caller requires columns $(required); missing $(missing_cols).",
    ))

    rows = DataFrame(pooled_table)
    wanted_measures = [PrefPol.normalize_variance_measure(measure) for measure in collect(measures)]
    wanted_components = Symbol.(collect(components))
    measure_order = Dict(measure => idx for (idx, measure) in enumerate(wanted_measures))
    component_order = Dict(component => idx for (idx, component) in enumerate(wanted_components))

    rows[!, :measure] = [PrefPol.normalize_variance_measure(measure) for measure in rows.measure]
    rows[!, :component] = Symbol.(rows.component)
    filter!(:measure => measure -> haskey(measure_order, measure), rows)
    filter!(:component => component -> haskey(component_order, component), rows)
    sort!(rows, [
        order(:measure, by = measure -> measure_order[measure]),
        order(:component, by = component -> component_order[component]),
    ])

    return rows, wanted_measures, wanted_components, measure_order, component_order
end

function _variance_component_offsets(n_components::Int)
    haskey(_VARIANCE_COMPONENT_OFFSETS, n_components) && return _VARIANCE_COMPONENT_OFFSETS[n_components]
    return collect(range(-0.34, 0.34; length = n_components))
end

function _summarize_variance_plot_rows(rows::AbstractDataFrame)
    summaries = NamedTuple[]

    for sub in groupby(rows, [:measure, :component])
        if nrow(sub) == 1
            q25 = Float64(sub[1, :q25])
            q50 = Float64(sub[1, :median])
            q75 = Float64(sub[1, :q75])
            values = Float64[q50]
        else
            values = Float64.(sub.median)
            q25 = quantile(values, 0.25)
            q50 = median(values)
            q75 = quantile(values, 0.75)
        end

        push!(summaries, (
            measure = Symbol(sub[1, :measure]),
            component = Symbol(sub[1, :component]),
            q25 = q25,
            median = q50,
            q75 = q75,
            values = values,
        ))
    end

    return DataFrame(summaries)
end

function plot_variance_decomposition_dotwhisker(pooled_table::AbstractDataFrame;
                                                measures = PrefPol.DEFAULT_PAPER_VARIANCE_MEASURES,
                                                components = _VARIANCE_DOTWHISKER_COMPONENTS,
                                                figsize = (900, 520),
                                                title = "Variance decomposition",
                                                xlabel = "absolute variance component",
                                                outfile = nothing,
                                                palette = nothing)
    palette === nothing && (palette = _wong_colors())
    rows, wanted_measures, wanted_components, measure_order, _ = _variance_plot_rows(
        pooled_table,
        measures,
        components,
        "plot_variance_decomposition_dotwhisker",
    )
    summary = _summarize_variance_plot_rows(rows)

    fig = Figure(size = figsize)
    ax = Axis(
        fig[1, 1];
        title = title,
        xlabel = xlabel,
        yticks = (
            1:length(wanted_measures),
            [get(PrefPol.DEFAULT_PAPER_VARIANCE_MEASURE_LABELS, measure, String(measure))
             for measure in wanted_measures],
        ),
    )

    n_components = max(length(wanted_components), 1)
    offsets = _variance_component_offsets(n_components)
    handles = Makie.LineElement[]
    labels = String[]

    for (component_idx, component) in enumerate(wanted_components)
        sub = summary[summary.component .== component, :]
        isempty(sub) && continue
        color = palette[(component_idx - 1) % length(palette) + 1]
        y = [measure_order[measure] + offsets[component_idx] for measure in sub.measure]
        x = Float64.(sub.median)
        lo = Float64.(sub.q25)
        hi = Float64.(sub.q75)

        for i in eachindex(x)
            lines!(ax, [lo[i], hi[i]], [y[i], y[i]]; color = color, linewidth = 2)
        end
        scatter!(ax, x, y; color = color, markersize = 10)

        push!(handles, Makie.LineElement(; color = color, linewidth = 3))
        push!(labels, get(_VARIANCE_DOTWHISKER_COMPONENT_LABELS, component, string(component)))
    end

    ylims!(ax, 0.5, length(wanted_measures) + 0.5)
    Legend(fig[1, 2], handles, labels)
    resize_to_layout!(fig)

    outfile !== nothing && save(outfile, fig; px_per_unit = 4)
    return fig
end

function _boxplot_stats(values::AbstractVector{<:Real})
    vals = sort(Float64.(values))
    isempty(vals) && return nothing

    q25 = quantile(vals, 0.25)
    q50 = median(vals)
    q75 = quantile(vals, 0.75)
    iqr = q75 - q25
    lo_fence = q25 - 1.5 * iqr
    hi_fence = q75 + 1.5 * iqr
    inside = [value for value in vals if lo_fence <= value <= hi_fence]
    outliers = [value for value in vals if value < lo_fence || value > hi_fence]
    whisker_lo = isempty(inside) ? minimum(vals) : minimum(inside)
    whisker_hi = isempty(inside) ? maximum(vals) : maximum(inside)

    return (
        q25 = q25,
        median = q50,
        q75 = q75,
        whisker_lo = whisker_lo,
        whisker_hi = whisker_hi,
        outliers = outliers,
    )
end

function plot_variance_decomposition_boxplot(pooled_table::AbstractDataFrame;
                                             measures = PrefPol.DEFAULT_PAPER_VARIANCE_MEASURES,
                                             components = _VARIANCE_DOTWHISKER_COMPONENTS,
                                             figsize = (900, 560),
                                             title = "Variance decomposition boxplot",
                                             xlabel = "absolute variance component",
                                             outfile = nothing,
                                             palette = nothing)
    palette === nothing && (palette = _wong_colors())
    rows, wanted_measures, wanted_components, measure_order, _ = _variance_plot_rows(
        pooled_table,
        measures,
        components,
        "plot_variance_decomposition_boxplot",
    )

    fig = Figure(size = figsize)
    ax = Axis(
        fig[1, 1];
        title = title,
        xlabel = xlabel,
        yticks = (
            1:length(wanted_measures),
            [get(PrefPol.DEFAULT_PAPER_VARIANCE_MEASURE_LABELS, measure, String(measure))
             for measure in wanted_measures],
        ),
    )

    n_components = max(length(wanted_components), 1)
    offsets = _variance_component_offsets(n_components)
    box_half_height = min(0.075, 0.26 / n_components)
    handles = Makie.LineElement[]
    labels = String[]

    for (component_idx, component) in enumerate(wanted_components)
        sub = rows[rows.component .== component, :]
        isempty(sub) && continue
        color = palette[(component_idx - 1) % length(palette) + 1]

        for measure in wanted_measures
            measure_rows = sub[sub.measure .== measure, :]
            isempty(measure_rows) && continue
            values = nrow(measure_rows) == 1 ?
                     Float64[measure_rows[1, :q25], measure_rows[1, :median], measure_rows[1, :q75]] :
                     Float64.(measure_rows.median)
            stats = _boxplot_stats(values)
            stats === nothing && continue

            y = measure_order[measure] + offsets[component_idx]
            poly!(
                ax,
                Makie.Point2f[
                    (stats.q25, y - box_half_height),
                    (stats.q75, y - box_half_height),
                    (stats.q75, y + box_half_height),
                    (stats.q25, y + box_half_height),
                ];
                color = (color, 0.18),
                strokecolor = color,
                strokewidth = 1.5,
            )
            lines!(ax, [stats.whisker_lo, stats.q25], [y, y]; color = color, linewidth = 1.7)
            lines!(ax, [stats.q75, stats.whisker_hi], [y, y]; color = color, linewidth = 1.7)
            lines!(ax, [stats.whisker_lo, stats.whisker_lo], [y - box_half_height / 2, y + box_half_height / 2];
                   color = color, linewidth = 1.7)
            lines!(ax, [stats.whisker_hi, stats.whisker_hi], [y - box_half_height / 2, y + box_half_height / 2];
                   color = color, linewidth = 1.7)
            lines!(ax, [stats.median, stats.median], [y - box_half_height, y + box_half_height];
                   color = color, linewidth = 2.4)

            if !isempty(stats.outliers)
                scatter!(
                    ax,
                    stats.outliers,
                    fill(y, length(stats.outliers));
                    color = (color, 0.28),
                    markersize = 6,
                )
            end
        end

        push!(handles, Makie.LineElement(; color = color, linewidth = 3))
        push!(labels, get(_VARIANCE_DOTWHISKER_COMPONENT_LABELS, component, string(component)))
    end

    ylims!(ax, 0.5, length(wanted_measures) + 0.5)
    Legend(fig[1, 2], handles, labels)
    resize_to_layout!(fig)

    outfile !== nothing && save(outfile, fig; px_per_unit = 4)
    return fig
end

function save_pipeline_plot(fig,
                            stem::AbstractString;
                            dir::AbstractString = "imgs",
                            ext::AbstractString = ".png")
    mkpath(dir)
    time_stamp = Dates.format(now(), "yyyymmdd-HHMMSS")
    fname = joinpath(dir, string(stem, "_", time_stamp, ext))
    save(fname, fig; px_per_unit = 4)
    @info "saved plot → $fname"
    return fname
end

function save_plot(fig,
                   year::Int,
                   scenario::AbstractString,
                   cfg;
                   variant::AbstractString,
                   dir::AbstractString = "imgs",
                   ext::AbstractString = ".png")
    mkpath(dir)

    time_stamp = Dates.format(now(), "yyyymmdd-HHMMSS")
    max_m = maximum(cfg.m_values_range)
    fname = joinpath(
        dir,
        string(
            year,
            '_',
            scenario,
            '_',
            variant,
            "_B",
            cfg.n_bootstrap,
            "_M",
            max_m,
            '_',
            time_stamp,
            ext,
        ),
    )

    save(fname, fig; px_per_unit = 4)
    @info "saved plot → $fname"
    return fname
end

end
