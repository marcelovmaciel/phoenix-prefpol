#!/usr/bin/env julia

using CSV
using DataFrames
using Dates
using Printf
using SHA
using Statistics
using TOML

include(joinpath(@__DIR__, "notebook_common.jl"))

const NB_CONFIG_PATH = joinpath(nb_root(), "notebook_config.toml")
const SMOKE_ROOT = joinpath(nb_root(), "output", "notebook_smoke")
const TEST_ROOT = joinpath(SMOKE_ROOT, "notebook_integrity")
const PROVENANCE_ROOT = joinpath(SMOKE_ROOT, "provenance")
const PROVENANCE_REPORT_PATH = joinpath(PROVENANCE_ROOT, "notebook_provenance_report.md")
const LOCAL_REPORT_PATH = joinpath(TEST_ROOT, "notebook_provenance_report.md")
const MAIN_MEASURES = [:Psi, :R, :HHI, :RHHI, :C, :D]

mutable struct AuditState
    blocks::Vector{NamedTuple}
    findings::Vector{NamedTuple}
    warnings::Vector{String}
    failures::Vector{String}
    context::Dict{String,Any}
end

AuditState() = AuditState(NamedTuple[], NamedTuple[], String[], String[], Dict{String,Any}())

function progress_message(message::AbstractString)
    println("[notebook-provenance] ", message)
    flush(stdout)
    return nothing
end

function record_block!(state::AuditState, name::AbstractString, status::AbstractString, detail::AbstractString)
    push!(state.blocks, (name = String(name), status = String(status), detail = String(detail)))
    status == "FAIL" && push!(state.failures, string(name, ": ", detail))
    return nothing
end

sha256_file(path::AbstractString) = bytes2hex(SHA.sha256(read(path)))

function maybe_sha256_file(path::AbstractString)
    isfile(path) || return "<missing>"
    try
        return sha256_file(path)
    catch err
        return "<unavailable: $(sprint(showerror, err))>"
    end
end

function git_commit()
    try
        return strip(read(`git -C $(repo_root()) rev-parse HEAD`, String))
    catch
        return "<unavailable>"
    end
end

function safe_rm_under!(path::AbstractString, parent::AbstractString)
    resolved = normpath(path)
    root = normpath(parent)
    rel = relpath(resolved, root)
    (rel == "." || startswith(rel, "..") || isabspath(rel)) && error(
        "Refusing to remove path outside $(root): $(resolved)",
    )
    isdir(resolved) && rm(resolved; recursive = true, force = true)
    return nothing
end

function notebook_source_files()
    files = [joinpath(nb_root(), name) for name in readdir(nb_root()) if occursin(r"^\d{2}_.*\.jl$", name)]
    push!(files, joinpath(nb_root(), "notebook_common.jl"))
    return sort(files)
end

function code_lines(path::AbstractString)
    out = NamedTuple[]
    in_md = false
    for (lineno, line) in enumerate(eachline(path))
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
        push!(out, (line = lineno, text = line))
    end
    return out
end

paren_delta(line::AbstractString) = count(==('('), line) - count(==(')'), line)
has_numeric_literal(text::AbstractString) = occursin(r"(?<![A-Za-z_])[-+]?\d+\.\d+(?:[eE][-+]?\d+)?", text)

function has_empirical_column_name(text::AbstractString)
    tokens = ("Psi", "RHHI", "HHI", "EO", "ENRP", "measure", "value", "estimate", "median", "q50")
    any(token -> occursin(token, text), tokens) || occursin(r"(?<![A-Za-z_])R(?![A-Za-z_])", text) ||
        occursin(r"(?<![A-Za-z_])C(?![A-Za-z_])", text) || occursin(r"(?<![A-Za-z_])D(?![A-Za-z_])", text)
end

function scan_static_findings()
    findings = NamedTuple[]
    forbidden_scripts = [
        "run_all_paper.jl",
        "00_validate_configs.jl",
        "01_bootstrap.jl",
        "02_impute.jl",
        "03_linearize.jl",
        "04_measures.jl",
        "05_plot_global.jl",
        "06_plot_group.jl",
        "07_extra_measures.jl",
        "08_tables.jl",
        "09_extra_plots.jl",
        "10_lambda_table.jl",
        "11_collect_paper_artifacts.jl",
    ]

    for path in notebook_source_files()
        lines = code_lines(path)
        rel = relpath(path, repo_root())

        for row in lines
            line = row.text
            if occursin("publication.toml", line)
                push!(findings, (path = rel, line = row.line, text = strip(line), reason = "Notebook code reads or references the production publication config."))
            end
            if occursin("PrefPol/composable_running/output/publication", line) &&
               occursin(r"CSV\.write|write_notebook_csv|open\(|mkpath|output_root|cache_root|write\(", line)
                push!(findings, (path = rel, line = row.line, text = strip(line), reason = "Notebook code writes or configures output inside the production publication tree."))
            end
            if any(script -> occursin(script, line), forbidden_scripts) &&
               occursin(r"include\(|run\(|read\(|open\(|Cmd|`|pipeline|julia|parsefile", line)
                push!(findings, (path = rel, line = row.line, text = strip(line), reason = "Notebook code appears to call or shell out to the production CLI/stage layer."))
            end
            if occursin(r"(?i)(Psi|RHHI|HHI|EO|ENRP|D_median|C_median|R_median|q50|median|estimate|value)\w*\s*(=|=>)\s*[-+]?\d+\.\d+\s*(,|\)|$)", line) ||
               occursin(r"(?i)(\"|:)(Psi|RHHI|HHI|EO|ENRP|C|D)(\"|)?\s*=>\s*[-+]?\d+\.\d+\s*(,|\)|$)", line)
                push!(findings, (path = rel, line = row.line, text = strip(line), reason = "Hardcoded floating point empirical value appears near a measure/statistic name."))
            end
            if occursin("CSV.write", line) && occursin("DataFrame(", line)
                push!(findings, (path = rel, line = row.line, text = strip(line), reason = "CSV.write appears to write a manually constructed DataFrame."))
            end
        end

        i = 1
        while i <= length(lines)
            row = lines[i]
            if occursin(r"DataFrame\s*\(", row.text)
                block = row.text
                balance = paren_delta(row.text)
                j = i
                while balance > 0 && j < length(lines)
                    j += 1
                    block *= "\n" * lines[j].text
                    balance += paren_delta(lines[j].text)
                end
                if occursin(r"DataFrame\s*\(\s*(\[|\()", block) && has_empirical_column_name(block) && has_numeric_literal(block)
                    push!(findings, (path = rel, line = row.line, text = strip(row.text), reason = "Manual DataFrame constructor mixes empirical result/statistic columns with numeric literals."))
                end
                i = j + 1
            else
                i += 1
            end
        end
    end

    return findings
end

function assert_tiny_scale!(settings)
    settings.B <= 2 || error("Notebook provenance test requires B <= 2, got B=$(settings.B).")
    settings.R <= 2 || error("Notebook provenance test requires R <= 2, got R=$(settings.R).")
    settings.K <= 2 || error("Notebook provenance test requires K <= 2, got K=$(settings.K).")
    return nothing
end

function selected_raw_paths!(state::AuditState, targets, wave_by_id)
    paths = String[]
    year_paths = String[]
    for target in targets
        haskey(wave_by_id, target.wave_id) || error("Notebook target wave $(target.wave_id) is not configured.")
        wave = wave_by_id[target.wave_id]
        push!(paths, wave.data_file)
        push!(year_paths, joinpath(prefpol_root(), "config", string(wave.year) * ".toml"))
    end
    paths = unique(paths)
    year_paths = unique(year_paths)
    state.context["raw_data_paths"] = paths
    state.context["raw_data_sha256"] = Dict(path => maybe_sha256_file(path) for path in paths)
    state.context["year_config_paths"] = year_paths
    state.context["year_config_sha256"] = Dict(path => maybe_sha256_file(path) for path in year_paths)
    missing = [path for path in paths if !isfile(path)]
    isempty(missing) || error(
        "Configured raw ESEB data required by the notebook target are missing: " *
        join(missing, ", ") * ". Update nb/notebook_config.toml or place the configured raw data at the listed path(s).",
    )
    return paths
end

function assert_raw_loader_dependencies!(paths::Vector{String})
    any(path -> endswith(lowercase(path), ".sav"), paths) || return nothing
    cmd = Cmd(vcat(Base.julia_cmd().exec, ["--startup-file=no", "--project=$(nb_root())", "-e", "using PrefPol; PrefPol._require_rcall!(); println(\"RCall ok\")"]))
    try
        read(cmd, String)
    catch err
        error(
            "Configured raw ESEB SPSS data exist, but RCall could not be loaded in the notebook environment. " *
            "Install/fix R and the required R packages (`haven`, `mice`, `PerMallows`) before running the real-data notebook provenance test. " *
            "Raw data path(s): " * join(paths, ", ") * ". RCall preflight error: " * sprint(showerror, err),
        )
    end
    return nothing
end

function decorate_manifest(manifest::DataFrame, item, pipeline)
    df = DataFrame(manifest)
    meta = item.metadata
    df[!, :wave_id] = fill(item.spec.wave_id, nrow(df))
    df[!, :year] = fill(meta.year, nrow(df))
    df[!, :scenario_name] = fill(meta.scenario_name, nrow(df))
    df[!, :m] = fill(meta.m, nrow(df))
    df[!, :imputer_backend] = fill(String(item.spec.imputer_backend), nrow(df))
    df[!, :linearizer_policy] = fill(String(item.spec.linearizer_policy), nrow(df))
    df[!, :cache_dir] = fill(pp.pipeline_cache_dir(pipeline, item.spec), nrow(df))
    return df
end

function expected_measure_rows(spec)
    global_count = count(measure -> measure in (:Psi, :R, :HHI, :RHHI), spec.measures)
    grouped_count = count(measure -> measure in (:C, :D), spec.measures)
    return spec.B * spec.R * spec.K * (global_count + grouped_count * length(spec.groupings))
end

function run_selected_pipeline!(state::AuditState, cfg; report_prefix = "")
    settings = notebook_settings(cfg)
    assert_tiny_scale!(settings)
    targets = notebook_targets(cfg)
    waves, source_registry, wave_by_id = load_notebook_waves()
    paths = selected_raw_paths!(state, targets, wave_by_id)
    assert_raw_loader_dependencies!(paths)

    batch = build_notebook_batch(cfg)
    item = batch.items[1]
    spec = item.spec
    selected_wave = wave_by_id[spec.wave_id]
    pipeline = pp.NestedStochasticPipeline(source_registry; cache_root = settings.cache_root)

    state.context[report_prefix * "selected_wave"] = spec.wave_id
    state.context[report_prefix * "selected_year"] = item.metadata.year
    state.context[report_prefix * "selected_scenario"] = item.metadata.scenario_name
    state.context[report_prefix * "selected_m"] = item.metadata.m
    state.context[report_prefix * "selected_backend"] = String(spec.imputer_backend)
    state.context[report_prefix * "selected_linearizer"] = String(spec.linearizer_policy)
    state.context[report_prefix * "active_candidates"] = spec.active_candidates
    state.context[report_prefix * "measure_set"] = String.(spec.measures)
    state.context[report_prefix * "groupings"] = String.(spec.groupings)
    state.context[report_prefix * "B"] = spec.B
    state.context[report_prefix * "R"] = spec.R
    state.context[report_prefix * "K"] = spec.K
    state.context[report_prefix * "output_root"] = settings.output_root
    state.context[report_prefix * "cache_root"] = settings.cache_root
    state.context[report_prefix * "year_config_path"] = joinpath(prefpol_root(), "config", string(selected_wave.year) * ".toml")

    progress_message("ensure_resamples! for wave=$(spec.wave_id), m=$(item.metadata.m), B=$(spec.B), R=$(spec.R), K=$(spec.K)")
    resample_manifest = pp.ensure_resamples!(pipeline, spec; force = true)
    progress_message("ensure_imputations!")
    imputation_manifest = pp.ensure_imputations!(pipeline, spec; force = true)
    progress_message("ensure_linearizations!")
    linearization_manifest = pp.ensure_linearizations!(pipeline, spec; force = true)
    progress_message("ensure_measures!")
    result = pp.ensure_measures!(pipeline, spec; force = true, progress = false)
    progress_message("pipeline complete")

    return (
        settings = settings,
        batch = batch,
        item = item,
        spec = spec,
        pipeline = pipeline,
        resample_manifest = DataFrame(resample_manifest),
        imputation_manifest = DataFrame(imputation_manifest),
        linearization_manifest = DataFrame(linearization_manifest),
        result = result,
    )
end

function first_linearized_support_rows(run)
    rows = run.linearization_manifest[run.linearization_manifest.stage .== :linearized, :]
    isempty(rows) && return 0
    artifact = pp.load_stage_artifact(rows[1, :path])
    return artifact isa DataFrame ? nrow(artifact) : 0
end

function run_signature(run)
    spec = run.spec
    cube = DataFrame(run.result.measure_cube)
    values = sort([round(Float64(v); digits = 10) for v in cube.value if !ismissing(v) && isfinite(Float64(v))])
    paths = sort(String.(run.result.stage_manifest.path))
    return (
        active_candidate_count = length(spec.active_candidates),
        possible_rankings = factorial(length(spec.active_candidates)),
        observed_ranking_rows = first_linearized_support_rows(run),
        measure_rows = nrow(cube),
        measure_values = values,
        leaf_count = spec.B * spec.R * spec.K,
        stage_artifact_paths = paths,
    )
end

function perturb_config(cfg, selected_spec)
    perturbed = deepcopy(cfg)
    run_cfg = get!(perturbed, "run", Dict{String,Any}())
    targets = get!(perturbed, "targets", Any[])
    isempty(targets) && error("Cannot perturb notebook config because it has no [[targets]] entries.")

    if length(selected_spec.active_candidates) >= 3 && get(targets[1], "m_values", [selected_spec.B])[1] != 3
        targets[1]["m_values"] = [3]
        return perturbed, "changed selected target m_values to [3]"
    end

    old_k = Int(get(run_cfg, "K", 1))
    run_cfg["K"] = old_k == 1 ? 2 : 1
    return perturbed, "changed K from $(old_k) to $(run_cfg["K"])"
end

function write_primary_outputs!(run)
    mkpath(TEST_ROOT)
    stage_manifest = decorate_manifest(DataFrame(run.result.stage_manifest), run.item, run.pipeline)
    measure_cube = DataFrame(run.result.measure_cube)
    global_rows = measure_cube[ismissing.(measure_cube.grouping), :]
    group_rows = measure_cube[.!ismissing.(measure_cube.grouping), :]

    CSV.write(joinpath(TEST_ROOT, "stage_manifest.csv"), stage_manifest)
    CSV.write(joinpath(TEST_ROOT, "measure_cube.csv"), measure_cube)
    CSV.write(joinpath(TEST_ROOT, "global_rows.csv"), global_rows)
    CSV.write(joinpath(TEST_ROOT, "group_rows.csv"), group_rows)
    return (stage_manifest = stage_manifest, measure_cube = measure_cube, global_rows = global_rows, group_rows = group_rows)
end

function assert_dynamic_outputs!(state::AuditState, run, outputs)
    spec = run.spec
    expected_leaves = spec.B * spec.R * spec.K
    nrow(run.resample_manifest) > 0 || error("Resample manifest has no rows.")
    nrow(run.imputation_manifest) > 0 || error("Imputation manifest has no rows.")
    nrow(run.linearization_manifest) > 0 || error("Linearization manifest has no rows.")
    nrow(outputs.measure_cube) > 0 || error("Measure cube has no rows.")

    stages = Set(Symbol.(outputs.stage_manifest.stage))
    expected_stages = Set([:spec, :observed, :resample, :imputed, :linearized, :measure])
    missing_stages = setdiff(expected_stages, stages)
    isempty(missing_stages) || error("Stage manifest is missing expected stages: $(collect(missing_stages)).")

    available = Set(Symbol.(outputs.measure_cube.measure))
    required = Set(MAIN_MEASURES)
    if isempty(spec.groupings)
        push!(state.warnings, "Selected spec has no grouping columns, so C/D grouped measures are mathematically unavailable.")
        required = Set([:Psi, :R, :HHI, :RHHI])
    end
    missing_measures = setdiff(required, available)
    isempty(missing_measures) || error("Measure cube is missing configured main measures: $(collect(missing_measures)).")

    linearized_rows = count(==(:linearized), Symbol.(run.linearization_manifest.stage))
    measure_artifact_rows = count(==(:measure), Symbol.(outputs.stage_manifest.stage))
    linearized_rows == expected_leaves || error("Linearized manifest leaf count $(linearized_rows) does not equal B*R*K=$(expected_leaves).")
    measure_artifact_rows == expected_leaves || error("Measure artifact count $(measure_artifact_rows) does not equal B*R*K=$(expected_leaves).")

    expected_rows = expected_measure_rows(spec)
    nrow(outputs.measure_cube) == expected_rows || error(
        "Measure cube row count $(nrow(outputs.measure_cube)) does not match expected B*R*K*(global measures + grouped measures*groupings)=$(expected_rows).",
    )

    state.context["resample_manifest_rows"] = nrow(run.resample_manifest)
    state.context["imputation_manifest_rows"] = nrow(run.imputation_manifest)
    state.context["linearization_manifest_rows"] = nrow(run.linearization_manifest)
    state.context["stage_manifest_rows"] = nrow(outputs.stage_manifest)
    state.context["measure_cube_rows"] = nrow(outputs.measure_cube)
    state.context["global_rows"] = nrow(outputs.global_rows)
    state.context["group_rows"] = nrow(outputs.group_rows)
    state.context["expected_leaves"] = expected_leaves
    state.context["expected_measure_rows"] = expected_rows
    return nothing
end

function numeric_trace_values(run)
    cube = DataFrame(run.result.measure_cube)
    summaries = DataFrame(run.result.pooled_summaries)
    values = Dict{Symbol,Vector{Float64}}()
    for row in eachrow(cube)
        measure = Symbol(row.measure)
        push!(get!(values, measure, Float64[]), Float64(row.value))
    end
    for sub in groupby(cube, [:measure, :grouping])
        measure = Symbol(sub[1, :measure])
        vals = Float64.(sub.value)
        append!(get!(values, measure, Float64[]), [mean(vals), median(vals)])
    end
    if :estimate in propertynames(summaries)
        for row in eachrow(summaries)
            push!(get!(values, Symbol(row.measure), Float64[]), Float64(row.estimate))
        end
    end
    return values
end

function reconcile_notebook_tables!(state::AuditState, run)
    table_dir = joinpath(run.settings.output_root, "notebook_tables")
    if !isdir(table_dir)
        push!(state.warnings, "No notebook_tables directory exists; notebook table reconciliation had no local notebook CSVs to inspect.")
        return "no notebook_tables directory"
    end

    csvs = String[]
    for (dir, _, files) in walkdir(table_dir)
        append!(csvs, [joinpath(dir, file) for file in files if endswith(file, ".csv")])
    end
    isempty(csvs) && (push!(state.warnings, "notebook_tables exists but contains no CSVs."); return "no notebook CSVs")

    trace = numeric_trace_values(run)
    checked = 0
    unmatched = NamedTuple[]
    selected = run.item.metadata
    spec = run.spec
    value_cols = [:value, :estimate, :median, :q50, :mean_value]

    for path in csvs
        df = try
            CSV.read(path, DataFrame)
        catch err
            push!(state.warnings, "Could not read notebook table $(path): $(sprint(showerror, err))")
            continue
        end
        :measure in propertynames(df) || continue
        present_value_cols = [col for col in value_cols if col in propertynames(df)]
        isempty(present_value_cols) && continue

        rows = df
        :wave_id in propertynames(rows) && (rows = rows[string.(rows.wave_id) .== spec.wave_id, :])
        :scenario_name in propertynames(rows) && (rows = rows[string.(rows.scenario_name) .== string(selected.scenario_name), :])
        :scenario in propertynames(rows) && (rows = rows[string.(rows.scenario) .== string(selected.scenario_name), :])
        :m in propertynames(rows) && (rows = rows[Int.(rows.m) .== Int(selected.m), :])
        isempty(rows) && continue

        for row in eachrow(rows)
            measure = Symbol(row.measure)
            haskey(trace, measure) || continue
            for col in present_value_cols
                v = row[col]
                (ismissing(v) || !(v isa Number) || !isfinite(Float64(v))) && continue
                checked += 1
                found = any(x -> isapprox(Float64(v), x; atol = 1e-8, rtol = 1e-8), trace[measure])
                found || push!(unmatched, (path = relpath(path, repo_root()), measure = measure, column = col, value = Float64(v)))
            end
        end
    end

    if checked == 0
        push!(state.warnings, "Notebook CSV reconciliation found no comparable empirical measure values for the selected spec; static literal-output scan remains the primary guard for untraceable tables.")
        return "no comparable values"
    end
    isempty(unmatched) || error("Notebook CSVs contain empirical measure values not traceable to the fresh PrefPol run: $(unmatched)")
    state.context["notebook_table_values_checked"] = checked
    return "checked $(checked) displayed empirical values"
end

function report_lines(state::AuditState)
    ctx = state.context
    lines = String[]
    push!(lines, "# Notebook Provenance Report")
    push!(lines, "")
    push!(lines, "Status: " * (isempty(state.failures) ? "PASS" : "FAIL"))
    push!(lines, "Timestamp: " * string(get(ctx, "timestamp", now())))
    push!(lines, "Git commit: " * string(get(ctx, "git_commit", "<unavailable>")))
    push!(lines, "")
    push!(lines, "## Test Blocks")
    for block in state.blocks
        push!(lines, "- $(block.status): $(block.name) - $(block.detail)")
    end
    push!(lines, "")
    push!(lines, "## Selected Config And Data")
    for key in [
        "config_path", "config_sha256", "selected_wave", "selected_year", "selected_scenario", "selected_m",
        "selected_backend", "selected_linearizer", "B", "R", "K", "active_candidates", "measure_set",
        "groupings", "output_root", "cache_root", "year_config_path", "expected_leaves",
    ]
        haskey(ctx, key) && push!(lines, "- $(key): $(ctx[key])")
    end
    if haskey(ctx, "raw_data_paths")
        push!(lines, "- raw_data_paths: $(ctx["raw_data_paths"])")
        push!(lines, "- raw_data_sha256: $(ctx["raw_data_sha256"])")
    end
    if haskey(ctx, "year_config_sha256")
        push!(lines, "- year_config_sha256: $(ctx["year_config_sha256"])")
    end
    push!(lines, "")
    push!(lines, "## Row Counts")
    for key in ["resample_manifest_rows", "imputation_manifest_rows", "linearization_manifest_rows", "stage_manifest_rows", "measure_cube_rows", "global_rows", "group_rows", "expected_measure_rows", "notebook_table_values_checked"]
        haskey(ctx, key) && push!(lines, "- $(key): $(ctx[key])")
    end
    push!(lines, "")
    push!(lines, "## Perturbation Summary")
    for key in ["perturbation", "primary_signature", "perturbed_signature", "changed_signature_fields"]
        haskey(ctx, key) && push!(lines, "- $(key): $(ctx[key])")
    end
    push!(lines, "")
    push!(lines, "## Output Files")
    for path in [
        joinpath(TEST_ROOT, "stage_manifest.csv"),
        joinpath(TEST_ROOT, "measure_cube.csv"),
        joinpath(TEST_ROOT, "global_rows.csv"),
        joinpath(TEST_ROOT, "group_rows.csv"),
        LOCAL_REPORT_PATH,
        PROVENANCE_REPORT_PATH,
    ]
        push!(lines, "- " * path)
    end
    push!(lines, "")
    push!(lines, "## Static Findings")
    if isempty(state.findings)
        push!(lines, "No suspicious static lines were found.")
    else
        for f in state.findings
            push!(lines, "- $(f.path):$(f.line): $(f.reason)")
            push!(lines, "  `$(f.text)`")
        end
    end
    push!(lines, "")
    push!(lines, "## Warnings")
    if isempty(state.warnings)
        push!(lines, "No warnings.")
    else
        append!(lines, ["- " * warning for warning in state.warnings])
    end
    push!(lines, "")
    push!(lines, "## Production Output Guard")
    push!(lines, isempty(state.failures) ? "Confirmed: this test wrote derived outputs only under nb/output/notebook_smoke/notebook_integrity and the provenance report path." : "Not confirmed because one or more blocks failed; inspect failures above.")
    return lines
end

function write_report!(state::AuditState)
    mkpath(PROVENANCE_ROOT)
    mkpath(TEST_ROOT)
    text = join(report_lines(state), "\n") * "\n"
    write(PROVENANCE_REPORT_PATH, text)
    write(LOCAL_REPORT_PATH, text)
    println("Notebook provenance report: ", PROVENANCE_REPORT_PATH)
    return nothing
end

function main()
    state = AuditState()
    state.context["timestamp"] = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS")
    state.context["git_commit"] = git_commit()
    state.context["config_path"] = NB_CONFIG_PATH
    state.context["config_sha256"] = maybe_sha256_file(NB_CONFIG_PATH)

    mkpath(SMOKE_ROOT)
    safe_rm_under!(TEST_ROOT, SMOKE_ROOT)
    mkpath(TEST_ROOT)
    mkpath(PROVENANCE_ROOT)

    cfg = nothing
    primary = nothing

    try
        progress_message("static scan")
        findings = scan_static_findings()
        append!(state.findings, findings)
        if isempty(findings)
            record_block!(state, "static literal-output / production contamination / CLI wrapper scan", "PASS", "No suspicious notebook source lines found.")
        else
            record_block!(state, "static literal-output / production contamination / CLI wrapper scan", "FAIL", "Found $(length(findings)) suspicious source line(s).")
        end
    catch err
        record_block!(state, "static scan", "FAIL", sprint(showerror, err))
    end

    try
        progress_message("dynamic real PrefPol computation")
        cfg = load_notebook_config(NB_CONFIG_PATH)
        primary = run_selected_pipeline!(state, cfg)
        outputs = write_primary_outputs!(primary)
        assert_dynamic_outputs!(state, primary, outputs)
        state.context["primary_signature"] = run_signature(primary)
        record_block!(state, "dynamic real PrefPol computation", "PASS", "Computed resamples, imputations, linearizations, and measures from configured raw data.")
    catch err
        record_block!(state, "dynamic real PrefPol computation", "FAIL", sprint(showerror, err))
    end

    try
        progress_message("dynamic config perturbation")
        primary === nothing && error("Primary dynamic computation did not produce a result.")
        perturbed_cfg, perturbation = perturb_config(cfg, primary.spec)
        tmp_path = joinpath(TEST_ROOT, "tmp_config_perturbed.toml")
        open(tmp_path, "w") do io
            TOML.print(io, perturbed_cfg)
        end
        state.context["perturbation"] = perturbation * " via " * tmp_path
        perturbed_state = AuditState()
        perturbed = run_selected_pipeline!(perturbed_state, perturbed_cfg; report_prefix = "perturbed_")
        sig1 = run_signature(primary)
        sig2 = run_signature(perturbed)
        changed = [String(name) for name in propertynames(sig1) if getproperty(sig1, name) != getproperty(sig2, name)]
        state.context["perturbed_signature"] = sig2
        state.context["changed_signature_fields"] = changed
        isempty(changed) && error("Notebook workflow outputs did not respond to a meaningful config perturbation. This suggests hardcoding or an ineffective test.")
        record_block!(state, "dynamic config perturbation", "PASS", "Perturbation changed: " * join(changed, ", "))
    catch err
        record_block!(state, "dynamic config perturbation", "FAIL", sprint(showerror, err))
    end

    try
        progress_message("dynamic notebook table reconciliation")
        primary === nothing && error("Primary dynamic computation did not produce a result.")
        detail = reconcile_notebook_tables!(state, primary)
        record_block!(state, "dynamic notebook table reconciliation", "PASS", detail)
    catch err
        record_block!(state, "dynamic notebook table reconciliation", "FAIL", sprint(showerror, err))
    end

    write_report!(state)
    isempty(state.failures) || exit(1)
    return nothing
end

main()
