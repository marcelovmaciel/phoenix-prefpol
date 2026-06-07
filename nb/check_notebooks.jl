#!/usr/bin/env julia

using Test

const NB_ROOT = @__DIR__
const REPO_ROOT = normpath(joinpath(NB_ROOT, ".."))

function notebook_source_files()
    return sort([joinpath(NB_ROOT, name) for name in readdir(NB_ROOT) if occursin(r"^\d{2}_.*\.jl$", name)])
end

function code_lines(path::AbstractString)
    out = String[]
    in_md = false
    for line in eachline(path)
        if in_md
            occursin("\"\"\"", line) && (in_md = false)
            continue
        end
        if occursin("md\"\"\"", line)
            count(==('"'), line) >= 6 || (in_md = true)
            continue
        end
        stripped = strip(line)
        isempty(stripped) && continue
        startswith(stripped, "#") && continue
        push!(out, line)
    end
    return out
end

function has_any(text::AbstractString, needles)
    return any(needle -> occursin(needle, text), needles)
end

function unqualified_dataframe_calls(lines)
    pattern = r"(^|[^A-Za-z0-9_.])(DataFrame|groupby|combine|select|nrow)\s*\("
    return [line for line in lines if occursin(pattern, line)]
end

@testset "Pluto serialization" begin
    validator = joinpath(REPO_ROOT, "scripts", "validate_pluto_notebooks.jl")
    @test isfile(validator)
    cmd = `$(Base.julia_cmd()) $validator`
    result = success(pipeline(cmd; stdout = devnull, stderr = devnull))
    @test result
end

@testset "notebook source parses and stays notebook-local" begin
    files = notebook_source_files()
    @test length(files) == 9
    for path in files
        rel = relpath(path, REPO_ROOT)
        text = read(path, String)
        code = code_lines(path)
        @testset "$rel" begin
            @test Meta.parseall(text) !== nothing
            @test !any(line -> occursin("publication.toml", line), code)
            @test !any(line -> occursin("run_all_paper.jl", line), code)
            @test !any(line -> occursin("PrefPol/composable_running/output/publication", line) && occursin(r"CSV\.write|write_notebook_csv|open\(|mkpath|output_root|cache_root|write\(", line), code)
            @test isempty(unqualified_dataframe_calls(code))
        end
    end
end

@testset "paper-bound visible objects" begin
    required = Dict(
        "00_setup_and_config.jl" => ["TableOfContents()", "selected_candidate_table", "selected_targets_table", "available_waves"],
        "01_targets_and_specs.jl" => ["selected_spec_rows", "active_candidate_table", "batch_table"],
        "02_resampling_and_imputation.jl" => ["observed_score_rows", "observed_missingness", "bootstrap_multiplicities", "imputation_row_comparison", "imputation_missingness_by_candidate"],
        "03_linearization.jl" => ["weak_to_strict_example", "selected_row_weak_to_strict", "unique_ranking_counts"],
        "04_measure_cubes.jl" => ["selected_leaf_measure_rows", "global_measure_rows", "grouped_measure_rows", "measure_distribution_summary"],
        "05_global_and_group_summaries.jl" => ["global_summary_table", "group_summary_table", "selected_group_summary_table", "scenario_plot_data_table"],
        "06_extra_diagnostics.jl" => ["effective_draws_compact", "effective_summary_compact", "example_reversal_pairs", "selected_effective_draw"],
        "07_plots_and_tables.jl" => ["effective_counts_summary", "plot_data", "effective_evolution_fig", "LocalResource"],
    )
    for (name, needles) in required
        text = read(joinpath(NB_ROOT, name), String)
        @testset "$name" begin
            for needle in needles
                @test occursin(needle, text)
            end
        end
    end
end

@testset "plot notebooks display plots inline" begin
    text = read(joinpath(NB_ROOT, "07_plots_and_tables.jl"), String)
    @test occursin("save(local_plot_path", text)
    @test occursin("effective_evolution_fig", text)
    @test occursin("LocalResource(local_plot_path)", text)
end

@testset "artifact map is debug/provenance" begin
    readme = read(joinpath(NB_ROOT, "README.md"), String)
    artifact_nb = read(joinpath(NB_ROOT, "08_artifact_map.jl"), String)
    @test occursin("debug", lowercase(readme)) || occursin("provenance", lowercase(readme))
    @test occursin("Debug / Provenance", artifact_nb)
end

@testset "notebook packages declared" begin
    project = read(joinpath(NB_ROOT, "Project.toml"), String)
    manifest = read(joinpath(NB_ROOT, "Manifest.toml"), String)
    for package in ["Pluto", "PlutoUI", "CairoMakie", "DataFrames", "CSV"]
        @test occursin(package * " =", project)
    end
    @test occursin("[[deps.PlutoUI]]", manifest)
    @test occursin("julia_version = \"1.11.9\"", manifest)
end

println("Notebook source quality checks passed.")
