#!/usr/bin/env julia

const COMPOSABLE_ROOT = @__DIR__
const REPO_ROOT = normpath(joinpath(COMPOSABLE_ROOT, "..", ".."))
const DEFAULT_CONFIG = joinpath(REPO_ROOT, "PrefPol", "config", "orchestration.toml")
const DEFAULT_ARTIFACT_CONFIG = joinpath(REPO_ROOT, "PrefPol", "config", "paper_artifacts.toml")

function parse_paper_args(args)
    opts = Dict{String,Any}(
        "config" => DEFAULT_CONFIG,
        "artifact-config" => DEFAULT_ARTIFACT_CONFIG,
        "year" => nothing,
        "scenario" => nothing,
        "m" => nothing,
        "backend" => nothing,
        "linearizer" => nothing,
        "force" => false,
        "dry-run" => false,
        "skip-plots" => false,
        "skip-collection" => false,
    )

    i = 1
    while i <= length(args)
        arg = args[i]
        if arg in ("--force", "--dry-run", "--skip-plots", "--skip-collection")
            opts[arg[3:end]] = true
        elseif arg in ("--config", "--artifact-config", "--year", "--scenario", "--m", "--backend", "--linearizer")
            i == length(args) && error("$(arg) requires a value.")
            opts[arg[3:end]] = args[i + 1]
            i += 1
        elseif arg in ("--help", "-h")
            println("""
            Usage:
              julia +1.11.9 --project=PrefPol PrefPol/composable_running/run_all_paper.jl [--config PATH] [--artifact-config PATH] [--year YEAR] [--scenario NAME] [--m VALUE_OR_RANGE] [--backend NAME] [--linearizer NAME] [--force] [--dry-run] [--skip-plots] [--skip-collection]

            Runs the full composable paper workflow in stage order using the
            configured targets from PrefPol/config/orchestration.toml by
            default. This wrapper does not run automatically; invoke it
            explicitly when a paper-scale run is intended.
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

function julia_cmd(project::AbstractString, script::AbstractString, args::Vector{String})
    return `julia +1.11.9 --project=$project $script $args`
end

function run_step(label::AbstractString, cmd::Cmd)
    println()
    println("==> ", label)
    println("    ", cmd)
    success(pipeline(cmd; stdout = stdout, stderr = stderr)) ||
        error("Paper workflow step failed: $(label)")
    return nothing
end

function stage_args(opts)
    args = ["--config", String(opts["config"])]
    opts["year"] === nothing || append!(args, ["--year", String(opts["year"])])
    opts["scenario"] === nothing || append!(args, ["--scenario", String(opts["scenario"])])
    opts["m"] === nothing || append!(args, ["--m", String(opts["m"])])
    opts["backend"] === nothing || append!(args, ["--backend", String(opts["backend"])])
    opts["linearizer"] === nothing || append!(args, ["--linearizer", String(opts["linearizer"])])
    Bool(opts["force"]) && push!(args, "--force")
    Bool(opts["dry-run"]) && push!(args, "--dry-run")
    return args
end

function main(args = ARGS)
    opts = parse_paper_args(args)
    opts["config"] = resolve_repo_path(String(opts["config"]))
    opts["artifact-config"] = resolve_repo_path(String(opts["artifact-config"]))

    stage_dir = joinpath(REPO_ROOT, "PrefPol", "composable_running", "stages")
    project = joinpath(REPO_ROOT, "PrefPol")
    plotting_project = joinpath(REPO_ROOT, "PrefPol", "running", "plotting_env")
    base_args = stage_args(opts)

    println("Composable paper workflow:")
    println("  config=", opts["config"])
    println("  artifact_config=", opts["artifact-config"])
    println("  target=", opts["year"] === nothing ? "configured years" : opts["year"],
            " / ", opts["scenario"] === nothing ? "configured scenarios" : opts["scenario"],
            opts["m"] === nothing ? " / configured m" : " / m=$(opts["m"])")
    println("  backend=", opts["backend"] === nothing ? "configured" : opts["backend"],
            " linearizer=", opts["linearizer"] === nothing ? "configured" : opts["linearizer"])
    println("  force=", opts["force"], " dry_run=", opts["dry-run"])

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
        collect_args = copy(base_args)
        append!(collect_args, ["--artifact-config", String(opts["artifact-config"])])
        run_step("collect paper artifacts", julia_cmd(project, joinpath(stage_dir, "11_collect_paper_artifacts.jl"), collect_args))
    end

    println()
    println("Composable paper workflow completed.")
    return nothing
end

main()
