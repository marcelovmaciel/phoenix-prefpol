#!/usr/bin/env julia

using CSV
using DataFrames
using Dates
using TOML

const COMPOSABLE_ROOT = @__DIR__
const REPO_ROOT = normpath(joinpath(COMPOSABLE_ROOT, "..", ".."))
const DEFAULT_CONFIG = joinpath(REPO_ROOT, "PrefPol", "config", "smoke_test.toml")
const DEFAULT_OUTPUT_ROOT = joinpath(COMPOSABLE_ROOT, "output")
const DEFAULT_YEAR = nothing
const DEFAULT_SCENARIO = nothing
const DEFAULT_M = "2:3"
const DEFAULT_BACKEND = "mice"
const DEFAULT_LINEARIZER = "pattern_conditional"

function parse_smoke_args(args)
    opts = Dict{String,Any}(
        "config" => DEFAULT_CONFIG,
        "year" => DEFAULT_YEAR,
        "scenario" => DEFAULT_SCENARIO,
        "m" => DEFAULT_M,
        "backend" => DEFAULT_BACKEND,
        "linearizer" => DEFAULT_LINEARIZER,
        "skip-plots" => false,
        "skip-collection" => false,
    )

    i = 1
    while i <= length(args)
        arg = args[i]
        if arg in ("--skip-plots", "--skip-collection")
            opts[arg[3:end]] = true
        elseif arg in ("--config", "--year", "--scenario", "--m", "--backend", "--linearizer")
            i == length(args) && error("$(arg) requires a value.")
            opts[arg[3:end]] = args[i + 1]
            i += 1
        elseif arg in ("--help", "-h")
            println("""
            Usage:
              julia +1.11.9 --project=PrefPol PrefPol/composable_running/run_all_smoke.jl [--config PATH] [--year YEAR] [--scenario NAME] [--m VALUE_OR_RANGE] [--backend NAME] [--linearizer NAME] [--skip-plots] [--skip-collection]

            Phase 10:
              Runs the B=R=K=2 composable smoke workflow and validates the
              required manifests, plots, tables, Lambda outputs, and collected
              paper artifacts. By default this runs every target configured in
              PrefPol/config/smoke_test.toml.
            """)
            exit(0)
        else
            error("Unknown argument: $(arg)")
        end
        i += 1
    end
    return opts
end

function resolve_repo_path(path::AbstractString)
    isabspath(path) && return normpath(path)
    return normpath(joinpath(REPO_ROOT, path))
end

function output_root_from_config(config_path::AbstractString)
    cfg = TOML.parsefile(config_path)
    run_cfg = get(cfg, "run", Dict{String,Any}())
    return resolve_repo_path(String(get(run_cfg, "output_root", "PrefPol/composable_running/output")))
end

function julia_cmd(project::AbstractString, script::AbstractString, args::Vector{String})
    return `julia +1.11.9 --project=$project $script $args`
end

function run_step(label::AbstractString, cmd::Cmd)
    println()
    println("==> ", label)
    println("    ", cmd)
    ok = success(pipeline(cmd; stdout = stdout, stderr = stderr))
    ok || error("Smoke step failed: $(label)")
    return nothing
end

function stage_args(opts)
    args = [
        "--config", String(opts["config"]),
        "--m", String(opts["m"]),
        "--backend", String(opts["backend"]),
        "--linearizer", String(opts["linearizer"]),
        "--smoke-test",
    ]
    opts["year"] === nothing || append!(args, ["--year", String(opts["year"])])
    opts["scenario"] === nothing || append!(args, ["--scenario", String(opts["scenario"])])
    return args
end

function smoke_artifact_config_path(output_root::AbstractString)
    path = joinpath(output_root, "manifests", "smoke_paper_artifacts.toml")
    mkpath(dirname(path))
    open(path, "w") do io
        println(io, """
        [collection]
        destination_root = "PrefPol/composable_running/output/paper_artifacts"
        copy_mode = "copy"
        update_writing_imgs = false
        paper_artifact_manifest = "PrefPol/composable_running/output/manifests/paper_artifact_manifest.csv"
        default_backend = "$(DEFAULT_BACKEND)"
        default_linearizer = "$(DEFAULT_LINEARIZER)"
        require_all = true

        [[artifacts]]
        id = "global_2006_main"
        destination_filename = "2006_global_main.png"
        source_stage = "plot_global"

        [[artifacts]]
        id = "global_2018_main"
        destination_filename = "2018_global_main.png"
        source_stage = "plot_global"

        [[artifacts]]
        id = "global_2022_main"
        destination_filename = "2022_global_main.png"
        source_stage = "plot_global"

        [[artifacts]]
        id = "group_2006_main"
        destination_filename = "2006_group.png"
        source_stage = "plot_group"

        [[artifacts]]
        id = "group_2018_main"
        destination_filename = "2018_group.png"
        source_stage = "plot_group"

        [[artifacts]]
        id = "group_2022_main"
        destination_filename = "2022_group.png"
        source_stage = "plot_group"

        [[artifacts]]
        id = "effective_rankings_table"
        destination_filename = "effective_rankings.tex"
        source_stage = "tables"

        [[artifacts]]
        id = "appendix_lambda_grouping_tables"
        destination_filename = "appendix_lambda_grouping_tables.tex"
        source_stage = "lambda_table"
        """)
    end
    return path
end

function validate_csv(path::AbstractString; allow_skipped::Bool = true)
    isfile(path) || error("Missing smoke manifest: $(path)")
    df = CSV.read(path, DataFrame)
    isempty(df) && error("Smoke manifest is empty: $(path)")
    if :status in propertynames(df)
        accepted = allow_skipped ? Set(["success", "skipped", "unchanged"]) : Set(["success"])
        bad = df[.!in.(string.(df.status), Ref(accepted)), :]
        isempty(bad) || error("Smoke manifest has failing rows: $(path)")
    end
    return df
end

function assert_file(path::AbstractString)
    isfile(path) || error("Missing smoke output: $(path)")
    filesize(path) > 0 || error("Smoke output is empty: $(path)")
    return path
end

function assert_any_file(root::AbstractString, predicate, description::AbstractString)
    isdir(root) || error("Missing smoke output directory: $(root)")
    matches = String[]
    for (dir, _, files) in walkdir(root)
        for file in files
            path = joinpath(dir, file)
            predicate(path) && filesize(path) > 0 && push!(matches, path)
        end
    end
    isempty(matches) && error("No smoke output matched $(description) under $(root).")
    return sort(matches)
end

function validate_smoke_outputs(output_root::AbstractString; plots::Bool, collection::Bool)
    manifest_dir = joinpath(output_root, "manifests")
    for file in (
        "bootstrap_manifest.csv",
        "imputation_manifest.csv",
        "linearization_manifest.csv",
        "measure_manifest.csv",
        "run_manifest.csv",
        "extra_measure_manifest.csv",
        "table_manifest.csv",
        "lambda_table_manifest.csv",
    )
        validate_csv(joinpath(manifest_dir, file))
    end

    for file in ("measure_table.csv", "summary_table.csv", "panel_table.csv", "decomposition_table.csv")
        assert_file(joinpath(output_root, "measures", file))
    end
    assert_file(joinpath(output_root, "tables", "effective_rankings", "effective_rankings.tex"))
    assert_file(joinpath(output_root, "appendices", "lambda", "appendix_lambda_table.csv"))
    assert_file(joinpath(output_root, "appendices", "lambda", "appendix_lambda_grouping_tables.tex"))

    if plots
        validate_csv(joinpath(manifest_dir, "plot_manifest.csv"))
        validate_csv(joinpath(manifest_dir, "group_plot_manifest.csv"))
        validate_csv(joinpath(manifest_dir, "extra_plot_manifest.csv"))
        assert_any_file(joinpath(output_root, "plots", "global"), path -> endswith(path, ".png"), "global PNG")
        assert_any_file(joinpath(output_root, "plots", "group"), path -> endswith(path, ".png"), "group PNG")
        assert_any_file(joinpath(output_root, "extra_plots"), path -> endswith(path, ".png"), "extra plot PNG")
    end

    if collection
        validate_csv(joinpath(manifest_dir, "paper_artifact_manifest.csv"))
        collected = joinpath(output_root, "paper_artifacts")
        assert_any_file(collected, path -> endswith(path, ".png"), "collected PNG")
        assert_any_file(collected, path -> endswith(path, ".tex"), "collected TeX table")
    end
    return nothing
end

function main(args = ARGS)
    opts = parse_smoke_args(args)
    opts["config"] = resolve_repo_path(String(opts["config"]))
    output_root = output_root_from_config(String(opts["config"]))
    stage_dir = joinpath(REPO_ROOT, "PrefPol", "composable_running", "stages")
    base_args = stage_args(opts)
    project = joinpath(REPO_ROOT, "PrefPol")
    plotting_project = joinpath(REPO_ROOT, "PrefPol", "running", "plotting_env")

    println("Composable smoke run:")
    println("  config=", opts["config"])
    println("  output_root=", output_root)
    println("  target=", opts["year"] === nothing ? "configured years" : opts["year"],
            " / ", opts["scenario"] === nothing ? "configured scenarios" : opts["scenario"],
            " / m=", opts["m"])
    println("  backend=", opts["backend"], " linearizer=", opts["linearizer"])

    run_step("validate configs", julia_cmd(project, joinpath(stage_dir, "00_validate_configs.jl"), ["--config", String(opts["config"])]))
    run_step("bootstrap", julia_cmd(project, joinpath(stage_dir, "01_bootstrap.jl"), base_args))
    run_step("impute", julia_cmd(project, joinpath(stage_dir, "02_impute.jl"), base_args))
    run_step("linearize", julia_cmd(project, joinpath(stage_dir, "03_linearize.jl"), base_args))
    run_step("measures", julia_cmd(project, joinpath(stage_dir, "04_measures.jl"), base_args))

    if !Bool(opts["skip-plots"])
        run_step("global plots", julia_cmd(plotting_project, joinpath(stage_dir, "05_plot_global.jl"), base_args))
        run_step("group plots", julia_cmd(plotting_project, joinpath(stage_dir, "06_plot_group.jl"), base_args))
    end

    run_step("extra measures", julia_cmd(project, joinpath(stage_dir, "07_extra_measures.jl"), base_args))
    run_step("tables", julia_cmd(project, joinpath(stage_dir, "09_tables.jl"), base_args))

    if !Bool(opts["skip-plots"])
        run_step("extra plots", julia_cmd(plotting_project, joinpath(stage_dir, "08_extra_plots.jl"), base_args))
    end

    run_step("lambda table", julia_cmd(project, joinpath(stage_dir, "10_lambda_table.jl"), base_args))

    if !Bool(opts["skip-collection"]) && !Bool(opts["skip-plots"])
        artifact_config = smoke_artifact_config_path(output_root)
        collect_args = ["--config", String(opts["config"]), "--artifact-config", artifact_config, "--force"]
        run_step("collect paper artifacts", julia_cmd(project, joinpath(stage_dir, "11_collect_paper_artifacts.jl"), collect_args))
    end

    validate_smoke_outputs(output_root; plots = !Bool(opts["skip-plots"]), collection = !Bool(opts["skip-collection"]) && !Bool(opts["skip-plots"]))
    println()
    println("Smoke validation passed at ", Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS"))
    return nothing
end

main()
