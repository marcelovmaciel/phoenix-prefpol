#!/usr/bin/env julia

# Run from the repository root with the plotting environment, for example:
#
#   julia +1.11.9 --project=PrefPol/running/plotting_env \
#     PrefPol/composable_running/make_single_peakedness_report_artifacts.jl \
#     --config PrefPol/config/single_peakedness_report_artifacts.toml
#
# This script is orchestration-only. Plotting and table artifact logic lives in
# PreferencePlots.

const SCRIPT_DIR = @__DIR__
const REPO_ROOT = normpath(joinpath(SCRIPT_DIR, "..", ".."))

using DataFrames
using PreferencePlots
using TOML

function parse_args(args)
    config = joinpath(REPO_ROOT, "PrefPol", "config", "single_peakedness_report_artifacts.toml")
    i = 1
    while i <= length(args)
        arg = args[i]
        if arg == "--config"
            i == length(args) && error("--config requires a value")
            config = args[i + 1]
            i += 1
        elseif arg in ("--help", "-h")
            println("""
            Usage:
              julia +1.11.9 --project=PrefPol/running/plotting_env PrefPol/composable_running/make_single_peakedness_report_artifacts.jl [--config PATH]

            Generates single-peakedness report figures and tables from the CSV
            outputs created by run_single_peakedness.jl. It does not write a PDF.
            """)
            exit(0)
        else
            error("Unknown argument: $(arg)")
        end
        i += 1
    end
    return config
end

resolve_path(path::AbstractString) = isabspath(path) ? String(path) : joinpath(REPO_ROOT, path)

enabled(artifacts, key::AbstractString) = Bool(get(artifacts, key, false))

function generated!(paths, path)
    push!(paths, path)
    println(path)
    return path
end

function demo_covariate_specs(cfg)
    return get(cfg, "demo_covariate", Any[])
end

function main()
    config_path = resolve_path(parse_args(ARGS))
    cfg = TOML.parsefile(config_path)
    input = get(cfg, "input", Dict{String,Any}())
    output = get(cfg, "output", Dict{String,Any}())
    artifacts = get(cfg, "artifacts", Dict{String,Any}())

    dirs = [resolve_path(String(dir)) for dir in get(input, "dirs", String[])]
    isempty(dirs) && error("Config [input].dirs must contain at least one single-peakedness output directory.")

    figures_dir = resolve_path(String(get(output, "figures_dir", joinpath("PrefPol", "composable_running", "output", "single_peakedness_report", "figures"))))
    tables_dir = resolve_path(String(get(output, "tables_dir", joinpath("PrefPol", "composable_running", "output", "single_peakedness_report", "tables"))))
    mkpath(figures_dir)
    mkpath(tables_dir)

    outputs = load_single_peakedness_outputs(dirs)
    generated = String[]

    println("Generated single-peakedness report artifacts:")

    if enabled(artifacts, "sp_mass_by_m_year")
        path = joinpath(figures_dir, "sp_mass_by_m_year.jpg")
        plot_sp_mass_by_m_year(outputs.axis_summary; output_path=path)
        generated!(generated, path)
    end
    if enabled(artifacts, "ratio_uniform_by_m_year")
        path = joinpath(figures_dir, "ratio_uniform_by_m_year.jpg")
        plot_ratio_uniform_by_m_year(outputs.axis_summary; output_path=path)
        generated!(generated, path)
    end
    if enabled(artifacts, "l1_by_m_year")
        path = joinpath(figures_dir, "l1_by_m_year.jpg")
        plot_l1_by_m_year(outputs.axis_summary; output_path=path)
        generated!(generated, path)
    end
    if enabled(artifacts, "distance_distribution")
        path = joinpath(figures_dir, "distance_distribution.jpg")
        plot_distance_distribution(outputs; output_path=path)
        generated!(generated, path)
    end
    if enabled(artifacts, "pipeline_effects")
        path = joinpath(figures_dir, "pipeline_effects.jpg")
        plot_pipeline_effects(outputs.axis_summary; output_path=path)
        generated!(generated, path)
    end
    if enabled(artifacts, "ideology_by_year_m") && :Ideology ∈ propertynames(outputs.row_classification)
        path = joinpath(figures_dir, "ideology_by_year_m.jpg")
        plot_ideology_by_year_m(outputs.row_classification; output_path=path)
        generated!(generated, path)
    end
    if enabled(artifacts, "demo_covariates")
        for spec in demo_covariate_specs(cfg)
            variable = Symbol(String(spec["variable"]))
            if variable ∉ propertynames(outputs.row_classification)
                @warn "Skipping demo covariate; variable is not present in row classification." variable
                continue
            end
            rows = outputs.row_classification[
                (outputs.row_classification.year .== Int(spec["year"])) .&
                (outputs.row_classification.m .== Int(spec["m"])),
                :,
            ]
            if isempty(rows)
                @warn "Skipping demo covariate; no rows match year and m." year=Int(spec["year"]) m=Int(spec["m"])
                continue
            end
            path = joinpath(figures_dir, String(spec["filename"]))
            plot_covariate_exact_fit(
                outputs.row_classification;
                year = Int(spec["year"]),
                m = Int(spec["m"]),
                variable = variable,
                title = get(spec, "title", nothing),
                output_path = path,
            )
            generated!(generated, path)
        end
    end

    if enabled(artifacts, "main_values_table")
        path = joinpath(tables_dir, "single_peakedness_main_values.csv")
        table_single_peakedness_main_values(outputs.axis_summary; output_path=path)
        generated!(generated, path)
    end
    if enabled(artifacts, "distance_table")
        path = joinpath(tables_dir, "single_peakedness_distance_distribution.csv")
        table_single_peakedness_distance_distribution(outputs; output_path=path)
        generated!(generated, path)
    end
    if enabled(artifacts, "modal_axes_table")
        path = joinpath(tables_dir, "single_peakedness_modal_axes.csv")
        table_single_peakedness_modal_axes(outputs.best_axes; output_path=path)
        generated!(generated, path)
    end
    if enabled(artifacts, "axis_gaps_table")
        path = joinpath(tables_dir, "single_peakedness_axis_gaps.csv")
        table_single_peakedness_axis_gaps(outputs.axis_summary; output_path=path)
        generated!(generated, path)
    end
    if enabled(artifacts, "pipeline_variation_table")
        path = joinpath(tables_dir, "single_peakedness_pipeline_variation.csv")
        table_single_peakedness_pipeline_variation(outputs.axis_summary; output_path=path)
        generated!(generated, path)
    end
    if enabled(artifacts, "covariates_table")
        variables = unique(Symbol(String(spec["variable"])) for spec in demo_covariate_specs(cfg))
        if !isempty(variables)
            path = joinpath(tables_dir, "single_peakedness_covariates.csv")
            table_single_peakedness_covariates(outputs.row_classification, collect(variables); output_path=path)
            generated!(generated, path)
        end
    end

    println("Total artifacts: $(length(generated))")
end

main()
