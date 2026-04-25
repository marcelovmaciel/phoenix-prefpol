"""
    ranking_support_diagnostics_small.jl

Post-process the small all-scenarios run to quantify empirical ranking-support
sparsity as m grows. This script reads the saved small-run manifest and cached
linearized profile artifacts. It does not rerun the pipeline.

Run after `run_all_scenarios_small.jl`, for example:

    julia +1.11.9 --startup-file=no --project=PrefPol PrefPol/running/ranking_support_diagnostics_small.jl
"""

using DataFrames
using JLD2
using Statistics
using PrefPol
import PrefPol as pp
const prefs = pp.Preferences

const SMALL_OUTPUT_ROOT = joinpath(pp.project_root, "running", "output", "all_scenarios_small")
const MANIFEST_PATH = joinpath(SMALL_OUTPUT_ROOT, "run_manifest.csv")
const OUTPUT_DIR = joinpath(SMALL_OUTPUT_ROOT, "ranking_support")
const REQUIRED_FACTORIALS = Dict(2 => 2, 3 => 6, 4 => 24, 5 => 120, 6 => 720, 7 => 5040)

function csv_escape(value)
    raw = value === missing ? "" : string(value)
    return "\"" * replace(raw, "\"" => "\"\"") * "\""
end

function save_csv(path::AbstractString, df::AbstractDataFrame)
    mkpath(dirname(path))
    open(path, "w") do io
        println(io, join((csv_escape(name) for name in names(df)), ","))
        for row in eachrow(df)
            println(io, join((csv_escape(row[name]) for name in names(df)), ","))
        end
    end
    return path
end

function _parse_csv_line(line::AbstractString)
    fields = String[]
    buf = IOBuffer()
    in_quotes = false
    i = firstindex(line)
    last = lastindex(line)

    while i <= last
        ch = line[i]

        if ch == '"'
            if in_quotes
                next_i = nextind(line, i)
                if next_i <= last && line[next_i] == '"'
                    write(buf, '"')
                    i = next_i
                else
                    in_quotes = false
                end
            else
                in_quotes = true
            end
        elseif ch == ',' && !in_quotes
            push!(fields, String(take!(buf)))
        else
            write(buf, ch)
        end

        i = nextind(line, i)
    end

    push!(fields, String(take!(buf)))
    return fields
end

function read_csv_table(path::AbstractString)
    isfile(path) || error("CSV file not found: $(path)")
    lines = readlines(path)
    isempty(lines) && return DataFrame()

    header = Symbol.(_parse_csv_line(first(lines)))
    columns = Dict(name => String[] for name in header)

    for line in Iterators.drop(lines, 1)
        isempty(line) && continue
        fields = _parse_csv_line(line)
        length(fields) == length(header) || error(
            "Malformed CSV row in $(path): expected $(length(header)) fields, found $(length(fields)).",
        )

        for (name, value) in zip(header, fields)
            push!(columns[name], value)
        end
    end

    return DataFrame([name => columns[name] for name in header])
end

function load_manifest(path::AbstractString = MANIFEST_PATH)
    manifest = read_csv_table(path)
    isempty(manifest) && error("Saved small-run manifest at $(path) is empty.")

    for col in (:year, :m, :B, :R, :K, :batch_index)
        col in propertynames(manifest) && (manifest[!, col] = parse.(Int, manifest[!, col]))
    end

    :analysis_role in propertynames(manifest) || error(
        "Manifest $(path) is missing `analysis_role`. Rerun run_all_scenarios_small.jl with the role-aware batch.",
    )

    return manifest
end

function linearized_stage_rows(result::pp.PipelineResult)
    stage_manifest = result.stage_manifest
    :stage in propertynames(stage_manifest) || error("PipelineResult stage manifest is missing `stage`.")
    rows = stage_manifest[stage_manifest.stage .== :linearized, :]
    isempty(rows) && error(
        "PipelineResult at $(result.cache_dir) has no cached linearized profile artifacts. " *
        "Run the small batch with diagnostic-only generation enabled before running this script.",
    )
    return rows
end

function ranking_support_for_artifact(path::AbstractString, m::Integer)
    isfile(path) || error("Cached linearized profile artifact not found: $(path)")
    artifact = JLD2.load(path, "artifact")
    artifact isa AbstractDataFrame || error(
        "Expected cached linearized artifact $(path) to be a DataFrame; got $(typeof(artifact)).",
    )
    :profile in propertynames(artifact) || error("Cached linearized artifact $(path) is missing `profile`.")

    return prefs.ranking_support_diagnostics(artifact[!, :profile]; m = Int(m))
end

function validate_diagnostic_row(row)
    m = Int(row.m)
    if haskey(REQUIRED_FACTORIALS, m)
        row.possible_rankings == REQUIRED_FACTORIALS[m] || error(
            "Expected possible_rankings=$(REQUIRED_FACTORIALS[m]) for m=$(m), got $(row.possible_rankings).",
        )
    end

    for col in (
        :unique_share_of_possible,
        :unique_share_of_observations,
        :singleton_share_of_unique,
        :singleton_share_of_observations,
        :max_ranking_mass,
        :effective_share_of_possible,
        :support_saturation,
    )
        value = Float64(row[col])
        (0.0 <= value <= 1.0) || error("Diagnostic proportion $(col)=$(value) is outside [0, 1].")
    end

    row.EO <= row.n_unique_rankings + sqrt(eps(Float64)) || error(
        "EO=$(row.EO) exceeds n_unique_rankings=$(row.n_unique_rankings).",
    )
    row.n_unique_rankings <= min(row.n_observations, row.possible_rankings) || error(
        "n_unique_rankings=$(row.n_unique_rankings) exceeds min(n_observations, possible_rankings).",
    )

    return true
end

function diagnostic_draw_rows(manifest::DataFrame)
    rows = NamedTuple[]

    for row in eachrow(manifest)
        result = pp.load_pipeline_result(String(row.result_path))
        stages = linearized_stage_rows(result)

        for stage in eachrow(stages)
            stats = ranking_support_for_artifact(String(stage.path), Int(row.m))
            out = merge((
                batch_index = Int(row.batch_index),
                analysis_role = String(row.analysis_role),
                wave_id = String(row.wave_id),
                year = Int(row.year),
                scenario_name = String(row.scenario_name),
                imputer_backend = String(row.imputer_backend),
                linearizer_policy = String(row.linearizer_policy),
                B = Int(row.B),
                R = Int(row.R),
                K = Int(row.K),
                b = Int(stage.b),
                r = Int(stage.r),
                k = Int(stage.k),
                active_candidates = String(row.active_candidates),
                scenario_dir = :scenario_dir in propertynames(row) ? String(row.scenario_dir) : "",
                cache_dir = String(row.cache_dir),
                linearized_path = String(stage.path),
                diagnostic = "ranking_support",
            ), stats)
            validate_diagnostic_row(out)
            push!(rows, out)
        end
    end

    isempty(rows) && error("No ranking-support diagnostic rows were produced.")
    return DataFrame(rows)
end

function summarize_draws(draws::DataFrame)
    metric_cols = [
        :n_observations,
        :possible_rankings,
        :n_unique_rankings,
        :unique_share_of_possible,
        :unique_share_of_observations,
        :singleton_rankings,
        :singleton_share_of_unique,
        :singleton_observation_count,
        :singleton_share_of_observations,
        :max_ranking_mass,
        :EO,
        :effective_share_of_possible,
        :support_saturation,
        :sparsity_pressure,
    ]
    group_cols = [
        :analysis_role,
        :wave_id,
        :year,
        :scenario_name,
        :imputer_backend,
        :linearizer_policy,
        :m,
        :active_candidates,
        :diagnostic,
    ]

    rows = NamedTuple[]
    for subdf in groupby(draws, group_cols)
        base = NamedTuple(name => subdf[1, name] for name in group_cols)
        for metric in metric_cols
            values = Float64.(subdf[!, metric])
            push!(rows, merge(base, (
                metric = String(metric),
                n_draws = length(values),
                median = quantile(values, 0.50),
                q25 = quantile(values, 0.25),
                q75 = quantile(values, 0.75),
                q05 = quantile(values, 0.05),
                q95 = quantile(values, 0.95),
            )))
        end
    end

    return sort(DataFrame(rows), vcat(group_cols, [:metric]))
end

function validate_coverage(draws::DataFrame)
    required_by_year = Dict(2006 => Set(2:5), 2018 => Set(2:5), 2022 => Set(2:5))

    for subdf in groupby(draws, [:wave_id, :year, :scenario_name, :imputer_backend, :linearizer_policy])
        year = Int(subdf[1, :year])
        haskey(required_by_year, year) || continue
        required = required_by_year[year]
        observed = Set(Int.(subdf.m))
        missing_values = setdiff(required, observed)
        isempty(missing_values) || error(
            "Ranking-support diagnostics for wave=$(subdf[1, :wave_id]) scenario=$(subdf[1, :scenario_name]) " *
            "imputer=$(subdf[1, :imputer_backend]) linearizer=$(subdf[1, :linearizer_policy]) " *
            "are missing m values $(sort(collect(missing_values))). " *
            "Rerun run_all_scenarios_small.jl so diagnostic-only specs are generated.",
        )
    end

    main_rows = draws[draws.analysis_role .== "main", :]
    all(main_rows.m .<= 5) || error("Main-role diagnostic rows include m > 5.")

    return true
end

function write_scenario_copies(draws::DataFrame, summary::DataFrame)
    :scenario_dir in propertynames(draws) || return nothing

    for scenario_dir in unique(String.(draws.scenario_dir))
        isempty(scenario_dir) && continue
        dir = joinpath(SMALL_OUTPUT_ROOT, scenario_dir, "ranking_support")
        mask = draws.scenario_dir .== scenario_dir

        scenario_draws = draws[mask, :]
        key_cols = [:analysis_role, :wave_id, :year, :scenario_name, :imputer_backend, :linearizer_policy, :m, :active_candidates]
        scenario_summary = semijoin(summary, unique(select(scenario_draws, key_cols)); on = key_cols)

        save_csv(joinpath(dir, "ranking_support_draws.csv"), scenario_draws)
        save_csv(joinpath(dir, "ranking_support_summary.csv"), scenario_summary)
    end

    return nothing
end

function main()
    manifest = load_manifest()
    draws = diagnostic_draw_rows(manifest)
    validate_coverage(draws)
    summary = summarize_draws(draws)

    save_csv(joinpath(OUTPUT_DIR, "ranking_support_draws.csv"), draws)
    save_csv(joinpath(OUTPUT_DIR, "ranking_support_summary.csv"), summary)
    write_scenario_copies(draws, summary)

    println("Saved ranking-support draw table: ", joinpath(OUTPUT_DIR, "ranking_support_draws.csv"))
    println("Saved ranking-support summary table: ", joinpath(OUTPUT_DIR, "ranking_support_summary.csv"))
    return nothing
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
