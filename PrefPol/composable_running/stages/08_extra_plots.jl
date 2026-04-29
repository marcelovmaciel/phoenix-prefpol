#!/usr/bin/env julia

const PREFPOL_PROJECT_DIR = normpath(joinpath(@__DIR__, "..", ".."))
PREFPOL_PROJECT_DIR in LOAD_PATH || pushfirst!(LOAD_PATH, PREFPOL_PROJECT_DIR)

include(joinpath(@__DIR__, "04_measures.jl"))

try
    @eval using CairoMakie
catch err
    throw(ArgumentError(
        "Extra plotting requires CairoMakie. Run with Julia 1.11.9 and the plotting environment:\n" *
        "  julia +1.11.9 --project=PrefPol/running/plotting_env PrefPol/composable_running/stages/08_extra_plots.jl"
    ))
end

Base.get_extension(PrefPol, :PrefPolPlottingExt) === nothing && throw(ArgumentError(
    "PrefPolPlottingExt is not active. Run with Julia 1.11.9 and the plotting environment."
))

const DEFAULT_PLOT_SPECS_PATH = joinpath(DEFAULT_CONFIG_DIR, "plot_specs.toml")

function parse_extra_plot_args(args)
    if any(arg -> arg in ("--help", "-h"), args)
        println("""
        Usage:
          julia +1.11.9 --project=PrefPol/running/plotting_env PrefPol/composable_running/stages/08_extra_plots.jl [--config PATH] [--year YEAR] [--scenario NAME] [--force] [--dry-run] [--smoke-test]

        Phase 8:
          Builds effective-ranking evolution plots and optional variance
          decomposition plots from table and measure outputs.
        """)
        exit(0)
    end
    return parse_args(args)
end

function normalize_format(fmt)
    text = lowercase(String(fmt))
    startswith(text, ".") ? text : "." * text
end

function load_plot_specs_config()
    isfile(DEFAULT_PLOT_SPECS_PATH) || return Dict{String,Any}()
    return TOML.parsefile(DEFAULT_PLOT_SPECS_PATH)
end

function merged_extra_plot_section(cfg)
    specs = load_plot_specs_config()
    section = Dict{String,Any}()
    haskey(specs, "outputs") && merge!(section, specs["outputs"])
    haskey(specs, "extra_plots") && merge!(section, specs["extra_plots"])
    haskey(cfg, "extra_plots") && merge!(section, cfg["extra_plots"])
    return section
end

function extra_plot_settings(cfg, opts)
    run_cfg = get(cfg, "run", Dict{String,Any}())
    plot_cfg = merged_extra_plot_section(cfg)
    output_root = resolve_path(String(config_value(plot_cfg, "output_root",
                                 config_value(run_cfg, "output_root", DEFAULT_OUTPUT_ROOT))))
    variance_cfg = get(plot_cfg, "variance_decomposition", Dict{String,Any}())
    return (
        output_root = output_root,
        extra_plot_root = resolve_path(String(config_value(plot_cfg, "extra_plot_root",
                                  joinpath(output_root, "extra_plots")))),
        table_root = resolve_path(String(config_value(plot_cfg, "table_root",
                             joinpath(output_root, "tables", "effective_rankings")))),
        decomposition_table = resolve_path(String(config_value(variance_cfg, "source",
                                      joinpath(output_root, "measures", "decomposition_table.csv")))),
        extra_plot_manifest = resolve_path(String(config_value(plot_cfg, "extra_plot_manifest",
                                      joinpath(output_root, "manifests", "extra_plot_manifest.csv")))),
        formats = normalize_format.(config_value(plot_cfg, "formats", ["png", "svg"])),
        effective_enabled = Bool(config_value(plot_cfg, "effective_rankings_enabled", true)),
        variance_enabled = Bool(config_value(variance_cfg, "enabled", true)),
        variance_year = Int(config_value(variance_cfg, "year", 2022)),
        variance_scenario = String(config_value(variance_cfg, "scenario_name", "main_2022")),
        variance_filename = String(config_value(variance_cfg, "filename", "variance_decomposition_2022.png")),
        force = Bool(opts["force"]) || Bool(config_value(plot_cfg, "force", false)),
        dry_run = Bool(opts["dry-run"]) || Bool(config_value(plot_cfg, "dry_run", false)),
    )
end

function load_evolution_tables(input_dir::AbstractString)
    isdir(input_dir) || error("Effective-ranking table directory not found: $(input_dir). Run 09_tables first.")
    files = [
        joinpath(input_dir, file)
        for file in readdir(input_dir)
        if startswith(file, "effective_rankings_evolution_") && endswith(file, ".csv")
    ]
    isempty(files) && error("No effective-ranking evolution CSVs found in $(input_dir). Run 09_tables first.")
    tables = DataFrame[]
    for file in sort(files)
        df = CSV.read(file, DataFrame)
        if !(:ENRP_median in propertynames(df)) && :ER_median in propertynames(df)
            rename!(df, :ER_median => :ENRP_median)
        end
        required = [:year, :m, :EO_median, :ENRP_median]
        missing_cols = setdiff(required, propertynames(df))
        isempty(missing_cols) || error("$(file) is missing required columns $(missing_cols).")
        push!(tables, df)
    end
    return sort(vcat(tables...; cols = :union), [:year, :m])
end

function regular_ticks(values, step::Real)
    vals = collect(skipmissing(values))
    isempty(vals) && return 0:step:step
    upper = step * ceil(maximum(Float64.(vals)) / step)
    return 0:step:upper
end

function plot_evolution(df::DataFrame)
    years = sort(unique(Int.(df.year)))
    palette = (RGBf(0.13, 0.37, 0.66), RGBf(0.82, 0.33, 0.16), RGBf(0.20, 0.55, 0.28), RGBf(0.45, 0.33, 0.62))
    colors = Dict(year => palette[(idx - 1) % length(palette) + 1] for (idx, year) in enumerate(years))

    fig = Figure(size = (1050, 430), fontsize = 14)
    ax_eo = Axis(fig[1, 1], xlabel = "m", ylabel = "median EO",
                 title = "Effective number of rankings", xticks = 2:5,
                 yticks = regular_ticks(df.EO_median, 5))
    ax_enrp = Axis(fig[1, 2], xlabel = "m", ylabel = "median ENRP",
                   title = "Effective reversal pairs", xticks = 2:5,
                   yticks = regular_ticks(df.ENRP_median, 2.5))

    for year in years
        rows = sort(df[Int.(df.year) .== year, :], :m)
        color = colors[year]
        label = string(year)
        lines!(ax_eo, rows.m, rows.EO_median; color, linewidth = 2.5, label)
        scatter!(ax_eo, rows.m, rows.EO_median; color, markersize = 9)
        lines!(ax_enrp, rows.m, rows.ENRP_median; color, linewidth = 2.5, label)
        scatter!(ax_enrp, rows.m, rows.ENRP_median; color, markersize = 9)
    end
    axislegend(ax_eo, "year"; position = :lt, framevisible = false)
    linkxaxes!(ax_eo, ax_enrp)
    return fig
end

function save_all_formats(fig, stem, dir, formats)
    mkpath(dir)
    paths = String[]
    for ext in formats
        path = joinpath(dir, stem * ext)
        CairoMakie.save(path, fig; px_per_unit = 4)
        push!(paths, path)
    end
    return paths
end

function write_effective_plot(settings)
    df = load_evolution_tables(settings.table_root)
    out_dir = joinpath(settings.extra_plot_root, "effective_rankings")
    fig = plot_evolution(df)
    paths = save_all_formats(fig, "effective_rankings_evolution_1x2", out_dir, settings.formats)
    data_path = joinpath(out_dir, "effective_rankings_evolution_plot_data.csv")
    CSV.write(data_path, df)
    return vcat(paths, [data_path])
end

function write_variance_plot(settings)
    settings.variance_enabled || return String[]
    if !isfile(settings.decomposition_table)
        @warn "Variance decomposition table not found; skipping variance plot." path=settings.decomposition_table
        return String[]
    end
    table = CSV.read(settings.decomposition_table, DataFrame)
    out_dir = joinpath(settings.extra_plot_root, "variance_decomposition")
    mkpath(out_dir)
    output_path = joinpath(out_dir, settings.variance_filename)
    plot_rows = try
        pp.variance_decomposition_year_scenario_boxplot_table(
            table;
            year = settings.variance_year,
            scenario_name = settings.variance_scenario,
        )
    catch err
        @warn "Skipping variance decomposition plot because selected rows are unavailable." exception=(err, catch_backtrace())
        return String[]
    end
    fig = pp.plot_variance_decomposition_year_scenario_boxplots(
        table;
        year = settings.variance_year,
        scenario_name = settings.variance_scenario,
        outfile = output_path,
    )
    data_path = joinpath(out_dir, splitext(settings.variance_filename)[1] * "_plot_data.csv")
    CSV.write(data_path, plot_rows)
    return [output_path, data_path]
end

function manifest_rows(paths, settings)
    return DataFrame([(
        stage = "extra_plots",
        artifact_id = splitext(basename(path))[1],
        input_path = endswith(path, ".csv") ? settings.table_root : settings.output_root,
        output_path = path,
        format = lowercase(splitext(path)[2][2:end]),
        source_manifest_hash = "",
        status = "success",
        error = "",
        timestamp = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS"),
    ) for path in paths])
end

function main(args = ARGS)
    opts = parse_extra_plot_args(args)
    cfg = load_orchestration_config(opts["config"])
    settings = extra_plot_settings(cfg, opts)
    println("Extra plot stage plan:")
    println("  table_root=", settings.table_root)
    println("  extra_plot_root=", settings.extra_plot_root)
    println("  formats=", join(settings.formats, ","))
    println("  force=", settings.force, " dry_run=", settings.dry_run)
    settings.dry_run && return nothing

    paths = String[]
    settings.effective_enabled && append!(paths, write_effective_plot(settings))
    append!(paths, write_variance_plot(settings))
    isempty(paths) && error("No extra plot outputs were produced.")
    write_csv(settings.extra_plot_manifest, manifest_rows(paths, settings))
    println("Wrote extra plots under ", settings.extra_plot_root)
    println("Wrote extra plot manifest to ", settings.extra_plot_manifest)
    return nothing
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
