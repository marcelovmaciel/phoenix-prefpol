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

if !isdefined(PrefPol, :Makie)
    @eval PrefPol const Makie = $Makie
end

if !isdefined(PrefPol, :save)
    @eval PrefPol save(args...; kwargs...) = $_cairomakie_save(args...; kwargs...)
end

@inline _wong_colors() = Makie.wong_colors()

@inline _measure_label(measure::Symbol) =
    measure === :Psi ? "Ψ" : String(measure)

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
                                connect_lines::Bool = false)
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
                                     measures = [:C, :D, :G],
                                     statistic::Symbol = :median,
                                     groupings = nothing,
                                     maxcols::Int = 3,
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
    group_labels = string.(group_syms)
    wanted_measures = PrefPol._normalize_measure_list(measures)

    allvals = Float32[]
    for measure in wanted_measures
        append!(allvals, Float32.(filter(!isnan, vec(data.matrices[measure]))))
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
            title = _measure_label(measure),
            xlabel = xlabel_txt,
            ylabel = ylabel_txt,
            xticks = (xs_m, string.(m_values_int)),
            yticks = (1:length(group_syms), group_labels),
        )
        z = Float32.(data.matrices[measure])
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
