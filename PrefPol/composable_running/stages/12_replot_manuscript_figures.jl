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

        Replots manuscript-facing figures from cached PipelineResult files
        without rerunning bootstrap, imputation, linearization, or measure computation.
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

const _PLOT_EXT = Base.get_extension(PrefPol, :PrefPolPlottingExt)
const REPLOT_BACKEND = :mice
const REPLOT_LINEARIZER = :pattern_conditional
const REPLOT_ANALYSIS_ROLE = "main"
const REPLOT_M_VALUES = 2:5
const REPLOT_GLOBAL_MEASURES = [:Psi, :R, :HHI, :RHHI]
const REPLOT_GROUP_MEASURES = [:C, :D]
const REPLOT_GROUP_LABELS = Dict(:C => "C", :D => "D")

function candidate_sequence_from_label(label)
    text = strip(String(label))
    text = replace(text, r"^Candidates:\s*" => "")
    return text
end

function candidate_labels_by_year(output_root::AbstractString)
    manifest_path = joinpath(output_root, "manifests", "run_manifest.csv")
    isfile(manifest_path) || return Dict{Int,String}()
    manifest = CSV.read(manifest_path, DataFrame)
    out = Dict{Int,String}()
    for year in sort(unique(Int.(manifest.year)))
        rows = manifest[Int.(manifest.year) .== year, :]
        :analysis_role in propertynames(rows) && (rows = rows[string.(rows.analysis_role) .== REPLOT_ANALYSIS_ROLE, :])
        :imputer_backend in propertynames(rows) && (rows = rows[Symbol.(rows.imputer_backend) .== REPLOT_BACKEND, :])
        :linearizer_policy in propertynames(rows) && (rows = rows[Symbol.(rows.linearizer_policy) .== REPLOT_LINEARIZER, :])
        isempty(rows) && continue
        mcol = :m in propertynames(rows) ? :m : :n_candidates
        row = rows[argmax(Int.(rows[!, mcol])), :]
        label = :candidate_label in propertynames(rows) && !ismissing(row.candidate_label) ?
                String(row.candidate_label) : PrefPol.describe_candidate_set(split(String(row.active_candidates_key), '|'))
        out[year] = label
    end
    return out
end

function parse_manifest_columns!(manifest::DataFrame)
    for col in (:year, :m, :B, :R, :K, :n_candidates, :batch_index)
        col in propertynames(manifest) || continue
        manifest[!, col] = Int.(manifest[!, col])
    end
    return manifest
end

function successful_run_manifest(path::AbstractString)
    isfile(path) || error("Run manifest not found: $(path)")
    manifest = parse_manifest_columns!(CSV.read(path, DataFrame))
    :status in propertynames(manifest) || return manifest
    return manifest[string.(manifest.status) .== "success", :]
end

function replot_run_manifest(output_root::AbstractString)
    manifest = successful_run_manifest(joinpath(output_root, "manifests", "run_manifest.csv"))
    rows = manifest[
        (string.(manifest.analysis_role) .== REPLOT_ANALYSIS_ROLE) .&
        (Symbol.(manifest.imputer_backend) .== REPLOT_BACKEND) .&
        (Symbol.(manifest.linearizer_policy) .== REPLOT_LINEARIZER) .&
        in.(Int.(manifest.m), Ref(Set(REPLOT_M_VALUES))),
        :,
    ]
    isempty(rows) && error("No cached main/mice/pattern_conditional results found for manuscript replotting.")
    return rows
end

function metadata_from_manifest_row(row)
    allowed = [
        :year, :scenario_name, :m, :analysis_role, :scenario_dir,
        :base_scenario_dir, :is_diagnostic, :active_candidates,
        :candidate_label,
    ]
    pairs = Pair{Symbol,Any}[]
    for key in allowed
        key in propertynames(row) || continue
        push!(pairs, key => row[key])
    end
    return (; pairs...)
end

function load_results_from_manifest(manifest::DataFrame)
    items = pp.StudyBatchItem[]
    results = pp.PipelineResult[]
    metadata = NamedTuple[]

    for row in eachrow(manifest)
        result_path = existing_manifest_path(row.result_path; label = "Cached PipelineResult")
        result = pp.load_pipeline_result(result_path)
        :spec_hash in propertynames(row) && string(row.spec_hash) != basename(result.cache_dir) && error(
            "Manifest/result hash mismatch for $(String(row.result_path)) resolved to $(result_path).",
        )
        meta = metadata_from_manifest_row(row)
        push!(items, pp.StudyBatchItem(result.spec; meta...))
        push!(results, result)
        push!(metadata, meta)
    end

    return pp.BatchRunResult(pp.StudyBatchSpec(items), results, metadata)
end

function subset_results(results::pp.BatchRunResult;
                        wave_id,
                        scenario_name,
                        analysis_role = REPLOT_ANALYSIS_ROLE,
                        imputer_backend = REPLOT_BACKEND,
                        linearizer_policy = REPLOT_LINEARIZER,
                        m_values = REPLOT_M_VALUES)
    items = pp.StudyBatchItem[]
    subset = pp.PipelineResult[]
    metadata = NamedTuple[]
    mset = Set(Int.(m_values))

    for (idx, item) in enumerate(results.batch.items)
        spec = item.spec
        meta = results.metadata[idx]
        spec.wave_id != string(wave_id) && continue
        (!hasproperty(meta, :scenario_name) || string(meta.scenario_name) != string(scenario_name)) && continue
        (!hasproperty(meta, :analysis_role) || string(meta.analysis_role) != string(analysis_role)) && continue
        spec.imputer_backend != Symbol(imputer_backend) && continue
        spec.linearizer_policy != Symbol(linearizer_policy) && continue
        (!hasproperty(meta, :m) || Int(meta.m) ∉ mset) && continue

        push!(items, item)
        push!(subset, results.results[idx])
        push!(metadata, meta)
    end

    isempty(items) && error("No cached results matched $(wave_id)/$(scenario_name) for manuscript replotting.")
    return pp.BatchRunResult(pp.StudyBatchSpec(items), subset, metadata)
end

function matching_file(dir::AbstractString; prefix::AbstractString, suffix::AbstractString)
    isdir(dir) || error("Directory not found: $(dir)")
    matches = sort([joinpath(dir, file) for file in readdir(dir)
                    if startswith(file, prefix) && endswith(file, suffix)])
    isempty(matches) && error("No file under $(dir) matched $(prefix)*$(suffix)")
    return last(matches)
end

function global_plot_output_path(output_root::AbstractString, year::Int)
    dir = joinpath(output_root, "plots", "global", string(year), "main_$(year)")
    prefix = "global_measures_year-$(year)_scenario-main_$(year)_backend-mice_linearizer-pattern_conditional"
    return matching_file(dir; prefix = prefix, suffix = ".png")
end

function group_plot_output_path(output_root::AbstractString, year::Int)
    dir = joinpath(output_root, "plots", "group", string(year), "main_$(year)")
    prefix = "paper_group_heatmap_panel_C_D_year-$(year)_scenario-main_$(year)_backend-mice_linearizer-pattern_conditional"
    return matching_file(dir; prefix = prefix, suffix = ".png")
end

function load_groupings_by_year()
    specs_path = joinpath(DEFAULT_CONFIG_DIR, "plot_specs.toml")
    isfile(specs_path) || return Dict{Int,Vector{Symbol}}()
    specs = TOML.parsefile(specs_path)
    group_cfg = get(get(specs, "group_plots", Dict{String,Any}()), "groupings_by_wave", Dict{String,Any}())
    return Dict(parse(Int, String(year)) => Symbol.(String.(groups)) for (year, groups) in group_cfg)
end

function replot_global(year::Int, settings, results)
    output_path = global_plot_output_path(settings.output_root, year)
    combo_results = subset_results(
        results;
        wave_id = string(year),
        scenario_name = "main_$(year)",
    )
    fig = _PLOT_EXT.plot_pipeline_scenario(
        combo_results;
        wave_id = string(year),
        scenario_name = "main_$(year)",
        imputer_backend = REPLOT_BACKEND,
        measures = REPLOT_GLOBAL_MEASURES,
        figsize = (780, 560),
        plot_kind = :dotwhisker,
        connect_lines = true,
        ytick_step = 0.1,
    )
    save_png(fig, output_path, settings)
    copy_for_manuscript(output_path, settings, "$(year)_global_main.png")
    return output_path
end

function replot_group(year::Int, settings, results, groupings_by_year)
    output_path = group_plot_output_path(settings.output_root, year)
    combo_results = subset_results(
        results;
        wave_id = string(year),
        scenario_name = "main_$(year)",
    )
    fig = _PLOT_EXT.plot_pipeline_group_paper_heatmap(
        combo_results;
        wave_id = string(year),
        scenario_name = "main_$(year)",
        imputer_backend = REPLOT_BACKEND,
        measures = REPLOT_GROUP_MEASURES,
        statistic = :median,
        groupings = get(groupings_by_year, year, nothing),
        complement_measures = Symbol[],
        measure_labels = REPLOT_GROUP_LABELS,
        maxcols = 2,
        colormap = Makie.Reverse(:RdBu),
        fixed_colorrange_limits = (0.0, 1.0),
        show_values = true,
        colorbar_label = "median",
        clist_size = 60,
        candidate_wrap_width = 112,
    )
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
    run_manifest = replot_run_manifest(settings.output_root)
    results = settings.dry_run ? nothing : load_results_from_manifest(run_manifest)
    groupings_by_year = load_groupings_by_year()

    println("Manuscript figure replot plan:")
    println("  output_root=", settings.output_root)
    println("  writing_imgs_root=", settings.writing_imgs_root)
    println("  dry_run=", settings.dry_run)

    outputs = String[]
    for year in (2006, 2018, 2022)
        if settings.dry_run
            push!(outputs, global_plot_output_path(settings.output_root, year))
            push!(outputs, group_plot_output_path(settings.output_root, year))
        else
            push!(outputs, replot_global(year, settings, results))
            push!(outputs, replot_group(year, settings, results, groupings_by_year))
        end
    end
    push!(outputs, replot_effective(settings, candidate_labels))
    push!(outputs, replot_variance(settings))

    println("Replotted manuscript figures:")
    foreach(path -> println("  ", path), outputs)
    return nothing
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
