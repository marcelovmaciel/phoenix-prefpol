#!/usr/bin/env julia

using CSV
using DataFrames
using TOML

const COMPOSABLE_ROOT = @__DIR__
const REPO_ROOT = normpath(joinpath(COMPOSABLE_ROOT, "..", ".."))
const PROJECT = joinpath(REPO_ROOT, "PrefPol")
const CONFIG = joinpath(PROJECT, "config", "publication_smoke_full.toml")
const ARTIFACT_CONFIG = joinpath(PROJECT, "config", "paper_artifacts.toml")
const OUTPUT_ROOT = joinpath(COMPOSABLE_ROOT, "output", "publication_smoke_full")
const PUBLICATION_OUTPUT_ROOT = joinpath(COMPOSABLE_ROOT, "output", "publication")

const EXPECTED_MANIFESTS = [
    "bootstrap_manifest.csv",
    "imputation_manifest.csv",
    "linearization_manifest.csv",
    "run_manifest.csv",
    "measure_manifest.csv",
    "plot_manifest.csv",
    "group_plot_manifest.csv",
    "extra_measure_manifest.csv",
    "table_manifest.csv",
    "extra_plot_manifest.csv",
    "paper_artifact_manifest.csv",
]

const EXPECTED_ARTIFACTS = [
    "2006_global_main.png",
    "2018_global_main.png",
    "2022_global_main.png",
    "2006_group.png",
    "2018_group.png",
    "2022_group.png",
    "effective_rankings_evolution_1x2.png",
    "effective_rankings.tex",
    "variance_decomposition_2022.png",
]

function assert_smoke_config_is_isolated()
    cfg = TOML.parsefile(CONFIG)
    run_cfg = cfg["run"]
    run_cfg["B"] == 1 || error("publication_smoke_full.toml must use B = 1.")
    run_cfg["R"] == 1 || error("publication_smoke_full.toml must use R = 1.")
    run_cfg["K"] == 2 || error("publication_smoke_full.toml must use K = 2.")
    run_cfg["output_root"] == "PrefPol/composable_running/output/publication_smoke_full" ||
        error("Smoke output_root must be isolated under publication_smoke_full.")
    run_cfg["cache_root"] == "PrefPol/composable_running/output/publication_smoke_full/cache" ||
        error("Smoke cache_root must be isolated under publication_smoke_full.")

    text = read(CONFIG, String)
    occursin("output/publication_smoke_full", text) ||
        error("Smoke config does not contain the smoke output root.")
    occursin("output/publication\"", text) &&
        error("Smoke config still references the real publication output root.")
    return nothing
end

function run_workflow()
    cmd = `julia +1.11.9 --project=$PROJECT $(joinpath(COMPOSABLE_ROOT, "run_all_paper.jl")) --config $CONFIG --artifact-config $ARTIFACT_CONFIG --force`
    println("Running smoke-full workflow:")
    println("  ", cmd)
    success(pipeline(cmd; stdout = stdout, stderr = stderr)) ||
        error("Smoke-full paper workflow failed.")
    return nothing
end

function assert_csv(path::AbstractString; accepted_statuses = Set(["success", "skipped", "unchanged"]))
    isfile(path) || error("Missing expected CSV: $(path)")
    filesize(path) > 0 || error("Expected CSV is empty: $(path)")
    df = CSV.read(path, DataFrame)
    isempty(df) && error("Expected CSV has no rows: $(path)")
    if :status in propertynames(df)
        bad = df[.!in.(string.(df.status), Ref(accepted_statuses)), :]
        isempty(bad) || error("Unexpected status values in $(path): $(unique(string.(bad.status)))")
    end
    return df
end

function assert_file(path::AbstractString)
    isfile(path) || error("Missing expected file: $(path)")
    filesize(path) > 0 || error("Expected file is empty: $(path)")
    return path
end

function validate_outputs()
    manifest_root = joinpath(OUTPUT_ROOT, "manifests")
    for manifest in EXPECTED_MANIFESTS
        assert_csv(joinpath(manifest_root, manifest))
    end

    artifact_manifest = assert_csv(joinpath(manifest_root, "paper_artifact_manifest.csv"))
    bad_artifacts = artifact_manifest[string.(artifact_manifest.status) .== "error", :]
    isempty(bad_artifacts) || error("Paper artifact collection produced errors.")

    artifact_root = joinpath(OUTPUT_ROOT, "paper_artifacts")
    for artifact in EXPECTED_ARTIFACTS
        assert_file(joinpath(artifact_root, artifact))
    end

    isdir(PUBLICATION_OUTPUT_ROOT) && @info "Real publication output directory exists; smoke run used isolated output." path=PUBLICATION_OUTPUT_ROOT
    return nothing
end

function main()
    assert_smoke_config_is_isolated()
    if isdir(OUTPUT_ROOT)
        println("Removing old smoke-full output: ", OUTPUT_ROOT)
        rm(OUTPUT_ROOT; recursive = true, force = true)
    end
    run_workflow()
    validate_outputs()
    println("Smoke-full workflow validation passed.")
    return nothing
end

main()
