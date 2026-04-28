#!/usr/bin/env julia

using Dates
using PrefPol
using TOML
import PrefPol as pp

const DEFAULT_CONFIG_DIR = joinpath(pp.project_root, "config")

function parse_args(args)
    opts = Dict{String,Any}(
        "config" => nothing,
        "year" => nothing,
        "scenario" => nothing,
        "dry-run" => false,
        "smoke-test" => false,
    )

    i = 1
    while i <= length(args)
        arg = args[i]
        if arg in ("--dry-run", "--smoke-test")
            opts[arg[3:end]] = true
        elseif arg in ("--config", "--year", "--scenario")
            i == length(args) && error("$(arg) requires a value.")
            opts[arg[3:end]] = args[i + 1]
            i += 1
        elseif arg in ("--help", "-h")
            println("""
            Usage:
              julia --project=PrefPol PrefPol/composable_running/stages/00_validate_configs.jl [--config PATH] [--year YEAR] [--scenario NAME]

            Validates wave TOMLs and, if present, the orchestration TOML shape used by Phase 4.
            """)
            exit(0)
        else
            error("Unknown argument: $(arg)")
        end
        i += 1
    end

    return opts
end

function year_config_paths(config_dir::AbstractString)
    paths = sort(filter(path -> occursin(r"/\d{4}\.toml$", path),
                        readdir(config_dir; join = true)))
    isempty(paths) && error("No year TOML files found under $(config_dir).")
    return paths
end

function validate_orchestration_config(path)
    path === nothing && return nothing
    isfile(path) || error("Orchestration config not found: $(path)")

    cfg = TOML.parsefile(path)
    haskey(cfg, "run") || @warn "Config has no [run] table; Phase 4 defaults will be used."
    haskey(cfg, "targets") || @warn "Config has no [[targets]] entries; Phase 4 defaults will be used."
    return cfg
end

function main(args = ARGS)
    opts = parse_args(args)
    config_dir = DEFAULT_CONFIG_DIR
    validate_orchestration_config(opts["config"])

    waves = PrefPol.SurveyWaveConfig[]
    for path in year_config_paths(config_dir)
        push!(waves, pp.load_survey_wave_config(path))
    end

    registry = pp.build_source_registry(waves)
    timestamp = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS")
    println("Validated $(length(waves)) wave config(s) at $(timestamp):")

    for wave in waves
        if opts["year"] !== nothing && string(wave.year) != string(opts["year"]) &&
           string(wave.wave_id) != string(opts["year"])
            continue
        end

        scenarios = sort(collect(keys(wave.scenario_candidates)))
        if opts["scenario"] !== nothing
            string(opts["scenario"]) in scenarios || error(
                "Scenario $(opts["scenario"]) is not configured for wave $(wave.wave_id).",
            )
            scenarios = [string(opts["scenario"])]
        end

        println(
            "  wave=", wave.wave_id,
            " year=", wave.year,
            " max_candidates=", wave.max_candidates,
            " scenarios=", join(scenarios, ","),
            " groupings=", join(wave.demographic_cols, ","),
        )
    end

    isempty(registry) && error("Config registry is empty.")
    return nothing
end

main()
