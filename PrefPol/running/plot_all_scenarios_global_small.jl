"""
    plot_all_scenarios_global_small.jl

Load the saved exploratory nested-analysis outputs produced by
`running/run_all_scenarios_small.jl`, then generate canonical global scenario
plots and compact CSV tables for the configured target wave/scenario pairs
using the exported nested plotting/table helpers.

This script does not rerun the analysis pipeline. It reads the saved manifest at
`running/output/all_scenarios_small/run_manifest.csv`, loads the cached
`PipelineResult`s referenced there, and writes plots/tables under
`running/output/all_scenarios_small/global/`.

Run later with:

    julia +1.11.9 --startup-file=no --project=PrefPol/running/plotting_env -e 'using Pkg; Pkg.instantiate()'
    julia +1.11.9 --startup-file=no --project=PrefPol/running/plotting_env PrefPol/running/plot_all_scenarios_global_small.jl
"""

include(joinpath(@__DIR__, "plotting_setup.jl"))
ensure_prefpol_plotting_environment!()

using CairoMakie
using DataFrames
using PrefPol
import PrefPol as pp

const _PLOT_EXT = ensure_prefpol_plotting_extension!(pp)

const SMALL_OUTPUT_ROOT = joinpath(pp.project_root, "running", "output", "all_scenarios_small")
const MANIFEST_PATH = joinpath(SMALL_OUTPUT_ROOT, "run_manifest.csv")
const GLOBAL_OUTPUT_ROOT = joinpath(SMALL_OUTPUT_ROOT, "global")
const GLOBAL_MEASURES = [:Psi, :R, :HHI, :RHHI]
const ANALYSIS_ROLE_MAIN = "main"
# Top-level plot targets. `nothing` discovers all main-role wave/scenario pairs
# from the saved manifest. When this file is included by narrower wrapper
# scripts, those scripts' constants take precedence.
if !isdefined(@__MODULE__, :TARGETS)
    const TARGETS = nothing
end
if !isdefined(@__MODULE__, :TARGET_GROUPINGS_BY_WAVE)
    const TARGET_GROUPINGS_BY_WAVE = Dict("2018" => nothing)
end
if !isdefined(@__MODULE__, :TARGET_ANALYSIS_ROLES)
    const TARGET_ANALYSIS_ROLES = ["main", "o_smoothed_extension"]
end
const WRITE_GLOBAL_TABLES = lowercase(get(ENV, "PREFPOL_GLOBAL_SMALL_WRITE_TABLES", "true")) in ("1", "true", "yes")

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

function sanitize_path_component(value::AbstractString)
    return replace(String(value), r"[^A-Za-z0-9._-]+" => "_")
end

function global_plot_stem(rows::AbstractDataFrame, target, combo)
    row = rows[1, :]
    year_value = hasproperty(rows, :year) ? row.year : target.wave_id
    scenario_value = target.scenario_name
    backend_value = combo.imputer_backend
    linearizer_value = combo.linearizer_policy

    return join((
        "global_measures",
        "year-$(sanitize_path_component(string(year_value)))",
        "scenario-$(sanitize_path_component(string(scenario_value)))",
        "backend-$(sanitize_path_component(string(backend_value)))",
        "linearizer-$(sanitize_path_component(string(linearizer_value)))",
        "B-$(row.B)",
        "R-$(row.R)",
        "K-$(row.K)",
        "draws-$(row.n_draws)",
    ), "_")
end

function sorted_table(df::DataFrame)
    sort_cols = [
        col for col in (
            :wave_id, :year, :scenario_name,
            :imputer_backend, :linearizer_policy,
            :m, :measure, :grouping, :n_candidates,
        ) if col in propertynames(df)
    ]
    isempty(sort_cols) && return df
    return sort(df, sort_cols)
end

function _global_measure_mask(df::AbstractDataFrame)
    return [Symbol(row.measure) in Set(GLOBAL_MEASURES) for row in eachrow(df)]
end

function _ungrouped_mask(df::AbstractDataFrame)
    hasproperty(df, :grouping) || return fill(true, nrow(df))
    return ismissing.(df.grouping)
end

function filter_global_rows(df::AbstractDataFrame)
    mask = _global_measure_mask(df) .& _ungrouped_mask(df)
    return DataFrame(df[mask, :])
end

function load_small_run_manifest(path::AbstractString = MANIFEST_PATH)
    manifest = read_csv_table(path)
    isempty(manifest) && error("Saved exploratory run manifest at $(path) is empty.")

    for col in (:year, :m, :B, :R, :K)
        manifest[!, col] = parse.(Int, manifest[!, col])
    end

    return manifest
end

function _batch_metadata(row)
    return (
        year = Int(row.year),
        scenario_name = String(row.scenario_name),
        m = Int(row.m),
        analysis_role = :analysis_role in propertynames(row) ? String(row.analysis_role) : ANALYSIS_ROLE_MAIN,
        scenario_dir = :scenario_dir in propertynames(row) ? String(row.scenario_dir) : "",
        base_scenario_dir = :base_scenario_dir in propertynames(row) ? String(row.base_scenario_dir) : "",
        is_diagnostic = :is_diagnostic in propertynames(row) ? lowercase(String(row.is_diagnostic)) in ("1", "true", "yes") : false,
    )
end

function load_saved_small_results(manifest::DataFrame)
    items = pp.StudyBatchItem[]
    results = pp.PipelineResult[]
    metadata = NamedTuple[]
    has_manifest_spec_hash = :spec_hash in propertynames(manifest)

    for row in eachrow(manifest)
        result_path = String(row.result_path)
        isfile(result_path) || error("Saved PipelineResult file not found: $(result_path)")

        result = pp.load_pipeline_result(result_path)
        spec_hash = basename(result.cache_dir)
        has_manifest_spec_hash && String(row.spec_hash) != spec_hash && error(
            "Manifest/result hash mismatch for $(result_path): manifest=$(row.spec_hash), result=$(spec_hash).",
        )

        meta = _batch_metadata(row)
        push!(items, pp.StudyBatchItem(result.spec; meta...))
        push!(results, result)
        push!(metadata, meta)
    end

    batch = pp.StudyBatchSpec(items)
    return pp.BatchRunResult(batch, results, metadata)
end

function scenario_targets(manifest::DataFrame)
    manifest_rows = :analysis_role in propertynames(manifest) ?
                    manifest[manifest.analysis_role .== ANALYSIS_ROLE_MAIN, :] :
                    manifest
    rows = unique(select(manifest_rows, :wave_id, :year, :scenario_name))
    target_keys = TARGETS === nothing ? nothing :
                  Set((String(target.wave_id), String(target.scenario_name)) for target in TARGETS)
    targets = [
        (
            wave_id = String(row.wave_id),
            year = Int(row.year),
            scenario_name = String(row.scenario_name),
        ) for row in eachrow(rows)
        if target_keys === nothing || (String(row.wave_id), String(row.scenario_name)) in target_keys
    ]
    return sort(
        targets;
        by = target -> (target.year, target.wave_id, target.scenario_name),
    )
end

function scenario_output_dir(target)
    return joinpath(
        GLOBAL_OUTPUT_ROOT,
        sanitize_path_component(target.wave_id),
        sanitize_path_component(target.scenario_name),
    )
end

function cleanup_global_dir!(dir::AbstractString)
    isdir(dir) || return nothing

    for path in readdir(dir; join = true)
        isfile(path) || continue
        (endswith(path, ".png") || endswith(path, ".csv")) || continue
        rm(path; force = true)
    end

    return nothing
end

function _result_metadata(result::pp.PipelineResult, extra_meta::NamedTuple)
    spec = result.spec
    return merge((
        spec_hash = basename(result.cache_dir),
        wave_id = spec.wave_id,
        active_candidates_key = join(spec.active_candidates, "|"),
        n_candidates = length(spec.active_candidates),
        groupings_key = join(String.(spec.groupings), "|"),
        measures_key = join(String.(spec.measures), "|"),
        B = spec.B,
        R = spec.R,
        K = spec.K,
        resample_policy = String(spec.resample_policy),
        imputer_backend = String(spec.imputer_backend),
        linearizer_policy = String(spec.linearizer_policy),
        consensus_tie_policy = String(spec.consensus_tie_policy),
        cache_dir = result.cache_dir,
    ), extra_meta)
end

function decorate_table(df::AbstractDataFrame,
                        result::pp.PipelineResult,
                        extra_meta::NamedTuple)
    out = DataFrame(df)
    meta = _result_metadata(result, extra_meta)

    for name in propertynames(meta)
        out[!, name] = fill(getproperty(meta, name), nrow(out))
    end

    return out
end

function combined_decomposition_table(results::pp.BatchRunResult)
    tables = DataFrame[]

    for (idx, result) in enumerate(results.results)
        meta = merge(results.metadata[idx], (batch_index = idx,))
        push!(tables, decorate_table(pp.decomposition_table(result.decomposition), result, meta))
    end

    isempty(tables) && return DataFrame()
    return vcat(tables...; cols = :union)
end

function subset_batch_results(results::pp.BatchRunResult;
                              wave_id = nothing,
                              scenario_name = nothing,
                              analysis_role = nothing,
                              imputer_backend = nothing,
                              linearizer_policy = nothing)
    items = pp.StudyBatchItem[]
    subset_results = pp.PipelineResult[]
    subset_meta = NamedTuple[]

    for (idx, item) in enumerate(results.batch.items)
        spec = item.spec
        meta = results.metadata[idx]

        wave_id !== nothing && spec.wave_id != String(wave_id) && continue
        scenario_name !== nothing &&
            (!hasproperty(meta, :scenario_name) || String(meta.scenario_name) != String(scenario_name)) &&
            continue
        analysis_role !== nothing &&
            (!hasproperty(meta, :analysis_role) || String(meta.analysis_role) != String(analysis_role)) &&
            continue
        imputer_backend !== nothing && spec.imputer_backend != Symbol(imputer_backend) && continue
        linearizer_policy !== nothing && spec.linearizer_policy != Symbol(linearizer_policy) && continue

        push!(items, item)
        push!(subset_results, results.results[idx])
        push!(subset_meta, meta)
    end

    isempty(items) && error("No saved results matched the requested plotting subset.")
    return pp.BatchRunResult(pp.StudyBatchSpec(items), subset_results, subset_meta)
end

function backend_combinations(manifest::DataFrame, target; analysis_role = ANALYSIS_ROLE_MAIN)
    rows = manifest[
        (manifest.wave_id .== target.wave_id) .&
        (manifest.scenario_name .== target.scenario_name) .&
        (:analysis_role in propertynames(manifest) ? (manifest.analysis_role .== String(analysis_role)) : trues(nrow(manifest))),
        [:imputer_backend, :linearizer_policy],
    ]
    combos = unique(rows)

    return sort(
        [(
            imputer_backend = Symbol(row.imputer_backend),
            linearizer_policy = Symbol(row.linearizer_policy),
        ) for row in eachrow(combos)];
        by = combo -> (String(combo.imputer_backend), String(combo.linearizer_policy)),
    )
end

function write_scenario_outputs!(results::pp.BatchRunResult,
                                 manifest::DataFrame,
                                 target)
    dir = scenario_output_dir(target)
    mkpath(dir)
    cleanup_global_dir!(dir)

    scenario_results = subset_batch_results(
        results;
        wave_id = target.wave_id,
        scenario_name = target.scenario_name,
        analysis_role = ANALYSIS_ROLE_MAIN,
    )

    summary_table = filter_global_rows(pp.pipeline_summary_table(scenario_results))
    panel_table = filter_global_rows(pp.pipeline_panel_table(scenario_results))
    decomposition_table = filter_global_rows(combined_decomposition_table(scenario_results))

    if WRITE_GLOBAL_TABLES
        save_csv(joinpath(dir, "summary_table.csv"), sorted_table(summary_table))
        save_csv(joinpath(dir, "panel_table.csv"), sorted_table(panel_table))
        save_csv(joinpath(dir, "decomposition_table.csv"), sorted_table(decomposition_table))
    end

    for combo in backend_combinations(manifest, target)
        combo_results = subset_batch_results(
            scenario_results;
            imputer_backend = combo.imputer_backend,
            linearizer_policy = combo.linearizer_policy,
        )

        # The exported scenario plotting helper filters on imputer backend only,
        # so we subset to one explicit linearizer branch here rather than
        # reimplementing the figure logic.
        plot_data = pp.pipeline_scenario_plot_data(
            combo_results;
            wave_id = target.wave_id,
            scenario_name = target.scenario_name,
            imputer_backend = combo.imputer_backend,
            measures = GLOBAL_MEASURES,
        )

        fig = pp.plot_pipeline_scenario(
            combo_results;
            wave_id = target.wave_id,
            scenario_name = target.scenario_name,
            imputer_backend = combo.imputer_backend,
            measures = GLOBAL_MEASURES,
            plot_kind = :dotwhisker,
            connect_lines = true,
            ytick_step = 0.1,
        )
        stem = global_plot_stem(plot_data.rows, target, combo)
        pp.save_pipeline_plot(fig, stem; dir = dir)
    end

    return nothing
end

function main()
    manifest = load_small_run_manifest()
    results = load_saved_small_results(manifest)

    for target in scenario_targets(manifest)
        write_scenario_outputs!(results, manifest, target)
    end

    return nothing
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
