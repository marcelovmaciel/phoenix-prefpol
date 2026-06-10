#!/usr/bin/env julia

const PREFPOL_PROJECT_DIR = normpath(joinpath(@__DIR__, "..", ".."))
PREFPOL_PROJECT_DIR in LOAD_PATH || pushfirst!(LOAD_PATH, PREFPOL_PROJECT_DIR)

include(joinpath(@__DIR__, "..", "stage_common.jl"))

try
    @eval using CairoMakie
catch err
    throw(ArgumentError(
        "Manuscript figure replotting requires CairoMakie. Run with the plotting environment, for example:\n" *
        "  julia +1.11.9 --project=PrefPol/running/plotting_env " *
        "PrefPol/composable_running/stages/12_replot_manuscript_figures.jl"
    ))
end

Base.get_extension(PrefPol, :PrefPolPlottingExt) === nothing && throw(ArgumentError(
    "PrefPolPlottingExt is not active. Run with Julia 1.11.9 and the plotting environment."
))

using Printf
using Statistics: quantile
using TextWrap

const Makie = CairoMakie.Makie
const DEFAULT_REPLOT_OUTPUT_ROOT = "PrefPol/composable_running/output/paper_b30_r10_k10"
const DEFAULT_WRITING_IMGS_ROOT = "writing/imgs"
const PAPER_AXIS_LABELSIZE = 15
const PAPER_TICK_LABELSIZE = 13
const PAPER_LEGEND_FONTSIZE = 13
const PAPER_NOTE_FONTSIZE = 14

function parse_replot_args(args)
    if any(arg -> arg in ("--help", "-h"), args)
        println("""
        Usage:
          julia +1.11.9 --project=PrefPol/running/plotting_env PrefPol/composable_running/stages/12_replot_manuscript_figures.jl [--config PATH] [--output-root PATH] [--writing-imgs-root PATH] [--dry-run]

        Replots manuscript-facing figures from compact paper plot CSVs without
        rerunning bootstrap, imputation, linearization, or measure computation.
        """)
        exit(0)
    end

    opts = Dict{String,Any}(
        "config" => nothing,
        "output-root" => DEFAULT_REPLOT_OUTPUT_ROOT,
        "writing-imgs-root" => DEFAULT_WRITING_IMGS_ROOT,
        "dry-run" => false,
    )

    i = 1
    while i <= length(args)
        arg = args[i]
        if arg == "--dry-run"
            opts["dry-run"] = true
        elseif arg in ("--config", "--output-root", "--writing-imgs-root")
            i == length(args) && error("$(arg) requires a value.")
            opts[arg[3:end]] = args[i + 1]
            i += 1
        else
            error("Unknown argument: $(arg)")
        end
        i += 1
    end
    return opts
end

function replot_settings(opts)
    cfg = load_orchestration_config(opts["config"])
    run_cfg = get(cfg, "run", Dict{String,Any}())
    output_root = resolve_path(String(config_value(run_cfg, "output_root", opts["output-root"])))
    opts["output-root"] != DEFAULT_REPLOT_OUTPUT_ROOT &&
        (output_root = resolve_path(String(opts["output-root"])))
    return (
        output_root = output_root,
        writing_imgs_root = resolve_path(String(opts["writing-imgs-root"])),
        dry_run = Bool(opts["dry-run"]),
    )
end

function measure_label(measure::Symbol)
    measure === :Psi && return Makie.LaTeXString(raw"$\Psi$")
    measure === :HHI && return Makie.LaTeXString(raw"$\kappa$")
    measure === :D_median && return "D"
    return String(measure)
end

function grouping_label(group)
    sym = Symbol(group)
    sym === :LulaScoreGroup && return "Lula's score"
    return String(group)
end

function candidate_sequence_from_label(label)
    text = strip(String(label))
    text = replace(text, r"^Candidates:\s*" => "")
    return text
end

function candidate_sequence_note(label; width = 86)
    sequence = candidate_sequence_from_label(label)
    isempty(sequence) && return ""
    text = "Candidate sequence: $(sequence)."
    return join(TextWrap.wrap(text; width = width), "\n")
end

function candidate_labels_by_year(output_root::AbstractString)
    manifest_path = joinpath(output_root, "manifests", "run_manifest.csv")
    isfile(manifest_path) || return Dict{Int,String}()
    manifest = CSV.read(manifest_path, DataFrame)
    out = Dict{Int,String}()
    for year in sort(unique(Int.(manifest.year)))
        rows = manifest[Int.(manifest.year) .== year, :]
        :analysis_role in propertynames(rows) && (rows = rows[string.(rows.analysis_role) .== "main", :])
        :imputer_backend in propertynames(rows) && (rows = rows[string.(rows.imputer_backend) .== "mice", :])
        :linearizer_policy in propertynames(rows) && (rows = rows[string.(rows.linearizer_policy) .== "pattern_conditional", :])
        isempty(rows) && continue
        mcol = :m in propertynames(rows) ? :m : :n_candidates
        row = rows[argmax(Int.(rows[!, mcol])), :]
        label = :candidate_label in propertynames(rows) && !ismissing(row.candidate_label) ?
                String(row.candidate_label) : PrefPol.describe_candidate_set(split(String(row.active_candidates_key), '|'))
        out[year] = label
    end
    return out
end

function matching_file(dir::AbstractString; prefix::AbstractString, suffix::AbstractString)
    isdir(dir) || error("Directory not found: $(dir)")
    matches = sort([joinpath(dir, file) for file in readdir(dir)
                    if startswith(file, prefix) && endswith(file, suffix)])
    isempty(matches) && error("No file under $(dir) matched $(prefix)*$(suffix)")
    return last(matches)
end

function copy_for_manuscript(src::AbstractString, settings, destination_filename::AbstractString)
    destinations = [
        joinpath(settings.output_root, "paper_artifacts", destination_filename),
        joinpath(settings.writing_imgs_root, destination_filename),
    ]
    for dst in destinations
        settings.dry_run && (println("would copy ", src, " -> ", dst); continue)
        mkpath(dirname(dst))
        cp(src, dst; force = true)
    end
    return destinations
end

function save_png(fig, path::AbstractString, settings)
    if settings.dry_run
        println("would write ", path)
        return path
    end
    mkpath(dirname(path))
    CairoMakie.save(path, fig; px_per_unit = 4)
    return path
end

function global_plot_data_path(output_root::AbstractString, year::Int)
    dir = joinpath(output_root, "plots", "global", string(year), "main_$(year)")
    prefix = "global_measures_year-$(year)_scenario-main_$(year)_backend-mice_linearizer-pattern_conditional"
    return matching_file(dir; prefix = prefix, suffix = "_plot_data.csv")
end

function global_plot_output_path(output_root::AbstractString, year::Int)
    dir = joinpath(output_root, "plots", "global", string(year), "main_$(year)")
    prefix = "global_measures_year-$(year)_scenario-main_$(year)_backend-mice_linearizer-pattern_conditional"
    return matching_file(dir; prefix = prefix, suffix = ".png")
end

function ungrouped_mask(df::DataFrame)
    :grouping in propertynames(df) || return trues(nrow(df))
    return [ismissing(value) || isempty(strip(String(value))) for value in df.grouping]
end

function plot_global_from_table(df::DataFrame; candidate_label::AbstractString)
    rows = df[ungrouped_mask(df), :]
    rows[!, :measure_sym] = Symbol.(String.(rows.measure))
    wanted = [:Psi, :R, :HHI, :RHHI]
    rows = rows[in.(rows.measure_sym, Ref(Set(wanted))), :]
    m_values = sort(unique(Int.(rows.n_candidates)))
    palette = Makie.wong_colors()

    fig = Figure(size = (780, 560), fontsize = 14)
    Label(fig[1, 1:2]; text = "Global measures • Year = $(first(rows.year))",
          fontsize = 16, halign = :center, tellwidth = false)
    Label(fig[2, 1:2]; text = candidate_sequence_note(candidate_label; width = 95),
          fontsize = PAPER_NOTE_FONTSIZE, halign = :left, justification = :left,
          tellwidth = false)
    ax = Axis(
        fig[3, 1];
        xlabel = "m",
        ylabel = "value",
        xticks = (m_values, string.(m_values)),
        xlabelsize = PAPER_AXIS_LABELSIZE,
        ylabelsize = PAPER_AXIS_LABELSIZE,
        xticklabelsize = PAPER_TICK_LABELSIZE,
        yticklabelsize = PAPER_TICK_LABELSIZE,
    )
    ticks = collect(0.0:0.1:1.0)
    ax.yticks[] = (ticks, [@sprintf("%.1f", tick) for tick in ticks])

    handles = Any[]
    labels = Any[]
    for (idx, measure) in enumerate(wanted)
        sub = sort(rows[rows.measure_sym .== measure, :], :n_candidates)
        isempty(sub) && continue
        color = palette[(idx - 1) % length(palette) + 1]
        xs = Float64.(sub.n_candidates)
        q05 = Float64.(sub.q05)
        q25 = Float64.(sub.q25)
        q75 = Float64.(sub.q75)
        q95 = Float64.(sub.q95)
        estimate = Float64.(sub.estimate)
        rangebars!(ax, xs, q05, q95; direction = :y, color = (color, 0.55), linewidth = 1.5, whiskerwidth = 10)
        rangebars!(ax, xs, q25, q75; direction = :y, color = (color, 0.9), linewidth = 3.0, whiskerwidth = 6)
        sc = scatter!(ax, xs, estimate; color = color, markersize = 9)
        lines!(ax, xs, estimate; color = color, linewidth = 1.5)
        push!(handles, sc)
        push!(labels, measure_label(measure))
    end
    Legend(fig[3, 2], handles, labels; labelsize = PAPER_LEGEND_FONTSIZE)
    colsize!(fig.layout, 1, Relative(0.80))
    colsize!(fig.layout, 2, Relative(0.20))
    rowsize!(fig.layout, 3, Relative(1.0))
    return fig
end

function replot_global(year::Int, settings, candidate_labels)
    data_path = global_plot_data_path(settings.output_root, year)
    output_path = global_plot_output_path(settings.output_root, year)
    df = CSV.read(data_path, DataFrame)
    label = get(candidate_labels, year, df[argmax(Int.(df.n_candidates)), :candidate_label])
    fig = plot_global_from_table(df; candidate_label = String(label))
    save_png(fig, output_path, settings)
    copy_for_manuscript(output_path, settings, "$(year)_global_main.png")
    return output_path
end

function group_table_path(output_root::AbstractString, year::Int)
    return joinpath(
        output_root,
        "plots", "group", string(year), "main_$(year)",
        "paper_group_heatmap_panel_C_D_table_mice_pattern_conditional.csv",
    )
end

function group_plot_output_path(output_root::AbstractString, year::Int)
    dir = joinpath(output_root, "plots", "group", string(year), "main_$(year)")
    prefix = "paper_group_heatmap_panel_C_D_year-$(year)_scenario-main_$(year)_backend-mice_linearizer-pattern_conditional"
    return matching_file(dir; prefix = prefix, suffix = ".png")
end

function heatmap_matrix(rows::DataFrame, measure::Symbol, m_values, group_values)
    z = fill(Float32(NaN), length(m_values), length(group_values))
    for (mi, m) in enumerate(m_values), (gi, group) in enumerate(group_values)
        sub = rows[(Int.(rows.m) .== m) .& (String.(rows.grouping) .== group) .&
                   (Symbol.(String.(rows.measure)) .== measure), :]
        isempty(sub) || (z[mi, gi] = Float32(sub[1, :value]))
    end
    return z
end

function overlay_heatmap_values!(ax, xs_m, n_groups::Int, z)
    xs = repeat(xs_m, inner = n_groups)
    ys = repeat(collect(1:n_groups), outer = length(xs_m))
    labels = [isnan(z[i, j]) ? "NA" : @sprintf("%.3f", z[i, j])
              for i in 1:length(xs_m) for j in 1:n_groups]
    text!(ax, xs, ys; text = labels, align = (:center, :center), color = :black, fontsize = 9)
    return nothing
end

function plot_group_from_table(df::DataFrame; candidate_label::AbstractString)
    rows = df[in.(Symbol.(String.(df.measure)), Ref(Set([:C, :D]))), :]
    measures = [:C, :D]
    m_values = sort(unique(Int.(rows.m)))
    xs_m = Float32.(m_values)
    group_values = unique(String.(rows.grouping))
    group_labels = grouping_label.(group_values)
    matrices = Dict(measure => clamp.(heatmap_matrix(rows, measure, m_values, group_values), 0.0f0, 1.0f0)
                    for measure in measures)

    fig = Figure(size = (1020, max(560, 260 + 30 * length(group_values))), fontsize = 14)
    fig[1, 1:2] = Label(fig, "Group diagnostics • Year = $(first(rows.year))"; fontsize = 20, halign = :center)
    fig[2, 1:2] = Label(fig, candidate_sequence_note(candidate_label; width = 112);
                         fontsize = PAPER_NOTE_FONTSIZE, halign = :left,
                         justification = :left, tellwidth = false)

    hm_ref = nothing
    for (idx, measure) in enumerate(measures)
        ax = Axis(
            fig[3, idx];
            title = String(measure),
            xlabel = "m",
            ylabel = idx == 1 ? "grouping" : "",
            xticks = (xs_m, string.(m_values)),
            yticks = (1:length(group_values), group_labels),
            xlabelsize = PAPER_AXIS_LABELSIZE,
            ylabelsize = PAPER_AXIS_LABELSIZE,
            xticklabelsize = PAPER_TICK_LABELSIZE,
            yticklabelsize = PAPER_TICK_LABELSIZE,
            titlesize = PAPER_AXIS_LABELSIZE,
        )
        hm = heatmap!(ax, xs_m, 1:length(group_values), matrices[measure];
                      colormap = Makie.Reverse(:RdBu), colorrange = (0.0f0, 1.0f0))
        overlay_heatmap_values!(ax, xs_m, length(group_values), matrices[measure])
        hm_ref === nothing && (hm_ref = hm)
    end
    Colorbar(fig[3, 3], hm_ref; label = "median", ticks = 0.0:0.2:1.0,
             labelsize = PAPER_AXIS_LABELSIZE, ticklabelsize = PAPER_TICK_LABELSIZE)
    colsize!(fig.layout, 1, Relative(0.45))
    colsize!(fig.layout, 2, Relative(0.45))
    colsize!(fig.layout, 3, Relative(0.10))
    rowsize!(fig.layout, 3, Relative(1.0))
    return fig
end

function replot_group(year::Int, settings, candidate_labels)
    table_path = group_table_path(settings.output_root, year)
    output_path = group_plot_output_path(settings.output_root, year)
    df = CSV.read(table_path, DataFrame)
    label = get(candidate_labels, year, "")
    fig = plot_group_from_table(df; candidate_label = label)
    save_png(fig, output_path, settings)
    copy_for_manuscript(output_path, settings, "$(year)_group_C_D.png")
    return output_path
end

function regular_ticks(values, step::Real)
    vals = collect(skipmissing(values))
    isempty(vals) && return 0:step:step
    upper = step * ceil(maximum(Float64.(vals)) / step)
    return 0:step:upper
end

function effective_table(output_root::AbstractString)
    dir = joinpath(output_root, "tables", "effective_rankings")
    files = [joinpath(dir, file) for file in readdir(dir)
             if startswith(file, "effective_rankings_evolution_") && endswith(file, ".csv")]
    isempty(files) && error("No effective-ranking evolution CSVs found in $(dir).")
    dfs = DataFrame[]
    for file in sort(files)
        df = CSV.read(file, DataFrame)
        if !(:ENRP_median in propertynames(df)) && :ER_median in propertynames(df)
            rename!(df, :ER_median => :ENRP_median)
        end
        push!(dfs, df)
    end
    return sort(vcat(dfs...; cols = :union), [:year, :m])
end

function effective_candidate_notes(df::DataFrame, candidate_labels)
    notes = String[]
    for year in sort(unique(Int.(df.year)))
        haskey(candidate_labels, year) || continue
        push!(notes, "$(year) candidate sequence: " * candidate_sequence_from_label(candidate_labels[year]))
    end
    return notes
end

function plot_effective(df::DataFrame; candidate_notes = String[])
    years = sort(unique(Int.(df.year)))
    palette = (RGBf(0.13, 0.37, 0.66), RGBf(0.82, 0.33, 0.16), RGBf(0.20, 0.55, 0.28), RGBf(0.45, 0.33, 0.62))
    colors = Dict(year => palette[(idx - 1) % length(palette) + 1] for (idx, year) in enumerate(years))
    fig = Figure(size = (1050, 540), fontsize = 14)
    axis_kwargs = (xlabelsize = PAPER_AXIS_LABELSIZE, ylabelsize = PAPER_AXIS_LABELSIZE,
                   xticklabelsize = PAPER_TICK_LABELSIZE, yticklabelsize = PAPER_TICK_LABELSIZE,
                   titlesize = PAPER_AXIS_LABELSIZE)
    ax_eo = Axis(fig[1, 1]; xlabel = "m", ylabel = "median EO",
                 title = "Effective number of rankings", xticks = 2:5,
                 yticks = regular_ticks(df.EO_median, 5), axis_kwargs...)
    ax_enrp = Axis(fig[1, 2]; xlabel = "m", ylabel = "median ENRP",
                   title = "Effective reversal pairs", xticks = 2:5,
                   yticks = regular_ticks(df.ENRP_median, 2.5), axis_kwargs...)
    for year in years
        rows = sort(df[Int.(df.year) .== year, :], :m)
        color = colors[year]
        lines!(ax_eo, rows.m, rows.EO_median; color, linewidth = 2.5, label = string(year))
        scatter!(ax_eo, rows.m, rows.EO_median; color, markersize = 9)
        lines!(ax_enrp, rows.m, rows.ENRP_median; color, linewidth = 2.5, label = string(year))
        scatter!(ax_enrp, rows.m, rows.ENRP_median; color, markersize = 9)
    end
    axislegend(ax_eo, "year"; position = :lt, framevisible = false, labelsize = PAPER_LEGEND_FONTSIZE, titlesize = PAPER_LEGEND_FONTSIZE)
    Label(fig[2, 1:2]; text = join(candidate_notes, "\n"), fontsize = 12, halign = :left, justification = :left)
    linkxaxes!(ax_eo, ax_enrp)
    resize_to_layout!(fig)
    return fig
end

function replot_effective(settings, candidate_labels)
    df = effective_table(settings.output_root)
    out_dir = joinpath(settings.output_root, "extra_plots", "effective_rankings")
    output_path = joinpath(out_dir, "effective_rankings_evolution_1x2.png")
    fig = plot_effective(df; candidate_notes = effective_candidate_notes(df, candidate_labels))
    save_png(fig, output_path, settings)
    copy_for_manuscript(output_path, settings, "effective_rankings_evolution_1x2.png")
    data_path = joinpath(out_dir, "effective_rankings_evolution_plot_data.csv")
    settings.dry_run || CSV.write(data_path, df)
    return output_path
end


function replot_global_group_with_matplotlib(settings)
    script = joinpath(@__DIR__, "12_replot_manuscript_global_group.py")
    isfile(script) || error("Global/group replot helper not found: $(script)")
    cmd = `python3 $script --output-root $(settings.output_root) --writing-imgs-root $(settings.writing_imgs_root)`
    settings.dry_run && (cmd = `python3 $script --output-root $(settings.output_root) --writing-imgs-root $(settings.writing_imgs_root) --dry-run`)
    run(cmd)
    outputs = String[]
    for year in (2006, 2018, 2022)
        push!(outputs, joinpath(settings.output_root, "plots", "global", string(year), "main_$(year)"))
        push!(outputs, joinpath(settings.output_root, "plots", "group", string(year), "main_$(year)"))
    end
    return outputs
end

function replot_variance(settings)
    table_path = joinpath(settings.output_root, "measures", "decomposition_table.csv")
    isfile(table_path) || error("Variance decomposition source not found: $(table_path)")
    table = CSV.read(table_path, DataFrame)
    out_dir = joinpath(settings.output_root, "extra_plots", "variance_decomposition")
    output_path = joinpath(out_dir, "variance_decomposition_2022.png")
    if settings.dry_run
        println("would write ", output_path)
    else
        PrefPol.plot_variance_decomposition_year_scenario_boxplots(
            table;
            year = 2022,
            scenario_name = "main_2022",
            value_kind = :variance,
            maxcols = 3,
            outfile = output_path,
        )
        plot_rows = PrefPol.variance_decomposition_year_scenario_boxplot_table(
            table;
            year = 2022,
            scenario_name = "main_2022",
            value_kind = :variance,
        )
        CSV.write(joinpath(out_dir, "variance_decomposition_2022_plot_data.csv"), plot_rows)
    end
    copy_for_manuscript(output_path, settings, "variance_decomposition_2022.png")
    return output_path
end

function main(args = ARGS)
    opts = parse_replot_args(args)
    settings = replot_settings(opts)
    candidate_labels = candidate_labels_by_year(settings.output_root)

    println("Manuscript figure replot plan:")
    println("  output_root=", settings.output_root)
    println("  writing_imgs_root=", settings.writing_imgs_root)
    println("  dry_run=", settings.dry_run)

    outputs = String[]
    append!(outputs, replot_global_group_with_matplotlib(settings))
    push!(outputs, replot_effective(settings, candidate_labels))
    push!(outputs, replot_variance(settings))

    println("Replotted manuscript figures:")
    foreach(path -> println("  ", path), outputs)
    return nothing
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
