#!/usr/bin/env julia

using Dates
using PrefPol
using TOML
import PrefPol as pp

const DEFAULT_CONFIG_DIR = joinpath(pp.project_root, "config")
const REQUIRED_SCHEMA_FILES = [
    "plot_specs.toml",
    "table_specs.toml",
    "paper_artifacts.toml",
]
const SUPPORTED_PLOT_FORMATS = Set(["png", "svg", "pdf"])
const SUPPORTED_MEASURES = Set(String.([
    :Psi, :R, :HHI, :RHHI, :C, :D, :O, :O_smoothed, :S, :S_old,
    :W, :lambda_sep, :Sep, :G, :Gsep, :D_median,
]))

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

function required_config_path(name::AbstractString)
    path = joinpath(DEFAULT_CONFIG_DIR, name)
    isfile(path) || error("Required Phase 2b config file not found: $(path)")
    return path
end

function load_required_schema_configs()
    configs = Dict{String,Any}()
    for name in REQUIRED_SCHEMA_FILES
        path = required_config_path(name)
        configs[name] = TOML.parsefile(path)
    end
    return configs
end

function known_wave_maps(waves)
    by_id = Dict(string(wave.wave_id) => wave for wave in waves)
    for wave in waves
        by_id[string(wave.year)] = wave
    end
    return by_id
end

function validate_target!(target, wave_by_id; context::AbstractString)
    wave_id = string(get(target, "wave_id", get(target, "year", "")))
    scenario_name = string(get(target, "scenario_name", ""))
    isempty(wave_id) && error("$(context) target is missing wave_id/year.")
    isempty(scenario_name) && error("$(context) target for wave $(wave_id) is missing scenario_name.")
    haskey(wave_by_id, wave_id) || error("$(context) target references unknown wave $(wave_id).")
    wave = wave_by_id[wave_id]
    haskey(wave.scenario_candidates, scenario_name) || error(
        "$(context) target references unknown scenario $(scenario_name) for wave $(wave.wave_id).",
    )
    return wave
end

function validate_measures(values, context::AbstractString)
    for measure in string.(collect(values))
        measure in SUPPORTED_MEASURES || error("$(context) references unsupported measure $(measure).")
    end
    return true
end

function normalize_format(fmt)
    text = lowercase(string(fmt))
    startswith(text, ".") && (text = text[2:end])
    return text
end

function validate_formats(values, context::AbstractString)
    for fmt in normalize_format.(collect(values))
        fmt in SUPPORTED_PLOT_FORMATS || error("$(context) references unsupported plot format $(fmt).")
    end
    return true
end

function validate_groupings(values, wave, context::AbstractString)
    configured = Set(string.(wave.demographic_cols))
    for grouping in string.(collect(values))
        grouping in configured || error(
            "$(context) references grouping $(grouping), which is not configured for wave $(wave.wave_id).",
        )
    end
    return true
end

function validate_plot_specs(plot_specs, waves)
    wave_by_id = known_wave_maps(waves)
    haskey(plot_specs, "global_plots") || error("plot_specs.toml is missing [global_plots].")
    haskey(plot_specs, "group_plots") || error("plot_specs.toml is missing [group_plots].")

    global_cfg = plot_specs["global_plots"]
    group_cfg = plot_specs["group_plots"]
    validate_measures(get(global_cfg, "measures", Any[]), "[global_plots].measures")
    validate_measures(get(group_cfg, "measures", Any[]), "[group_plots].measures")
    validate_formats(get(global_cfg, "formats", get(get(plot_specs, "outputs", Dict{String,Any}()), "formats", ["png"])),
                     "[global_plots].formats")
    validate_formats(get(group_cfg, "formats", get(get(plot_specs, "outputs", Dict{String,Any}()), "formats", ["png"])),
                     "[group_plots].formats")

    if haskey(group_cfg, "o_smoothed")
        o_cfg = group_cfg["o_smoothed"]
        validate_measures(get(o_cfg, "measures", Any[]), "[group_plots.o_smoothed].measures")
    end

    for target in get(global_cfg, "targets", Any[])
        validate_target!(target, wave_by_id; context = "[global_plots.targets]")
    end
    for target in get(global_cfg, "diagnostic_targets", Any[])
        validate_target!(target, wave_by_id; context = "[global_plots.diagnostic_targets]")
    end
    for target in get(group_cfg, "targets", Any[])
        validate_target!(target, wave_by_id; context = "[group_plots.targets]")
    end

    groupings_by_wave = get(group_cfg, "groupings_by_wave", Dict{String,Any}())
    for (wave_key, groupings) in groupings_by_wave
        haskey(wave_by_id, string(wave_key)) || error("[group_plots.groupings_by_wave] references unknown wave $(wave_key).")
        validate_groupings(groupings, wave_by_id[string(wave_key)], "[group_plots.groupings_by_wave].$(wave_key)")
    end

    return true
end

function validate_table_specs(table_specs, waves)
    haskey(table_specs, "lambda_table") || error("table_specs.toml is missing [lambda_table].")
    lambda_cfg = table_specs["lambda_table"]
    string(get(lambda_cfg, "definition", "")) == "Lambda = D / W" || error(
        "[lambda_table].definition must be \"Lambda = D / W\".",
    )
    wave_by_id = known_wave_maps(waves)
    for target in get(lambda_cfg, "targets", Any[])
        wave = validate_target!(target, wave_by_id; context = "[lambda_table.targets]")
        haskey(target, "groupings") && validate_groupings(target["groupings"], wave, "[lambda_table.targets].groupings")
    end
    return true
end

function validate_paper_artifacts(paper_specs)
    haskey(paper_specs, "collection") || error("paper_artifacts.toml is missing [collection].")
    haskey(paper_specs, "artifacts") || error("paper_artifacts.toml is missing [[artifacts]].")
    required = Set([
        "2006_global_main.png",
        "2018_global_main.png",
        "2022_global_main.png",
        "2006_group.png",
        "2018_group.png",
        "2022_group.png",
        "effective_rankings_evolution_1x2.png",
        "effective_rankings.tex",
        "variance_decomposition_2022.png",
        "appendix_lambda_grouping_tables.tex",
    ])
    observed = Set(string(get(artifact, "destination_filename", "")) for artifact in paper_specs["artifacts"])
    missing = setdiff(required, observed)
    isempty(missing) || error("paper_artifacts.toml is missing destination filenames: $(sort(collect(missing))).")
    return true
end

function main(args = ARGS)
    opts = parse_args(args)
    config_dir = DEFAULT_CONFIG_DIR
    validate_orchestration_config(opts["config"])
    schema_configs = load_required_schema_configs()

    waves = PrefPol.SurveyWaveConfig[]
    for path in year_config_paths(config_dir)
        push!(waves, pp.load_survey_wave_config(path))
    end

    validate_plot_specs(schema_configs["plot_specs.toml"], waves)
    validate_table_specs(schema_configs["table_specs.toml"], waves)
    validate_paper_artifacts(schema_configs["paper_artifacts.toml"])

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
