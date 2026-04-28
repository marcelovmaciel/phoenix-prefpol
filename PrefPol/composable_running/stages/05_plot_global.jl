#!/usr/bin/env julia

const PREFPOL_PROJECT_DIR = normpath(joinpath(@__DIR__, "..", ".."))
PREFPOL_PROJECT_DIR in LOAD_PATH || pushfirst!(LOAD_PATH, PREFPOL_PROJECT_DIR)

include(joinpath(@__DIR__, "04_measures.jl"))

using SHA

try
    @eval using CairoMakie
catch err
    throw(ArgumentError(
        "Global plotting requires CairoMakie. Run with the plotting environment, for example:\n" *
        "  julia +1.11.9 --project=PrefPol/running/plotting_env " *
        "PrefPol/composable_running/stages/05_plot_global.jl"
    ))
end

Base.get_extension(PrefPol, :PrefPolPlottingExt) === nothing && throw(ArgumentError(
    "PrefPolPlottingExt is not active. Run with Julia 1.11.9 and the plotting environment:\n" *
    "  julia +1.11.9 --project=PrefPol/running/plotting_env " *
    "PrefPol/composable_running/stages/05_plot_global.jl"
))

const DEFAULT_GLOBAL_MEASURES = [:Psi, :R, :HHI, :RHHI]
const ANALYSIS_ROLE_MAIN = "main"

function parse_plot_args(args)
    if any(arg -> arg in ("--help", "-h"), args)
        println("""
        Usage:
          julia +1.11.9 --project=PrefPol/running/plotting_env PrefPol/composable_running/stages/05_plot_global.jl [--config PATH] [--year YEAR] [--scenario NAME] [--m VALUE_OR_RANGE] [--backend NAME] [--linearizer NAME] [--force] [--dry-run] [--smoke-test]

        Phase 6:
          Loads cached PipelineResult files from the composable run manifest and
          writes global plots, compact plot CSVs, and plot_manifest.csv.

        Config:
          Optional [global_plots] keys: measures, formats, write_tables,
          output_root, plot_root, run_manifest, measure_manifest, analysis_role.
        """)
        exit(0)
    end
    return parse_args(args)
end

function read_manifest(path::AbstractString)
    isfile(path) || error("Required manifest not found: $(path)")
    df = CSV.read(path, DataFrame)
    isempty(df) && error("Manifest is empty: $(path)")
    return df
end

function normalize_format(fmt)
    text = lowercase(String(fmt))
    startswith(text, ".") ? text : "." * text
end

function plot_settings(cfg, opts)
    run_cfg = get(cfg, "run", Dict{String,Any}())
    plot_cfg = get(cfg, "global_plots", Dict{String,Any}())
    output_root = resolve_path(String(config_value(plot_cfg, "output_root",
                                 config_value(run_cfg, "output_root", DEFAULT_OUTPUT_ROOT))))

    return (
        output_root = output_root,
        plot_root = resolve_path(String(config_value(plot_cfg, "plot_root",
                             joinpath(output_root, "plots", "global")))),
        run_manifest = resolve_path(String(config_value(plot_cfg, "run_manifest",
                               joinpath(output_root, "manifests", "run_manifest.csv")))),
        measure_manifest = resolve_path(String(config_value(plot_cfg, "measure_manifest",
                                   joinpath(output_root, "manifests", "measure_manifest.csv")))),
        plot_manifest = resolve_path(String(config_value(plot_cfg, "plot_manifest",
                                  joinpath(output_root, "manifests", "plot_manifest.csv")))),
        measures = as_symbol_vector(config_value(plot_cfg, "measures", String.(DEFAULT_GLOBAL_MEASURES))),
        formats = normalize_format.(config_value(plot_cfg, "formats", ["png"])),
        write_tables = Bool(config_value(plot_cfg, "write_tables", true)),
        analysis_role = String(config_value(plot_cfg, "analysis_role", ANALYSIS_ROLE_MAIN)),
        force = Bool(opts["force"]) || Bool(config_value(plot_cfg, "force", false)),
        dry_run = Bool(opts["dry-run"]) || Bool(config_value(plot_cfg, "dry_run", false)),
    )
end

function manifest_hash(path::AbstractString)
    return bytes2hex(SHA.sha256(read(path)))
end

function sanitize_path_component(value)
    return replace(string(value), r"[^A-Za-z0-9._-]+" => "_")
end

function sorted_plot_table(df::DataFrame)
    preferred = [
        :wave_id, :year, :scenario_name, :analysis_role,
        :imputer_backend, :linearizer_policy, :m, :measure, :grouping,
        :n_candidates,
    ]
    cols = [col for col in preferred if col in propertynames(df)]
    isempty(cols) && return df
    return sort(df, cols)
end

function parse_manifest_columns!(manifest::DataFrame)
    for col in (:year, :m, :B, :R, :K, :n_candidates, :batch_index)
        col in propertynames(manifest) || continue
        manifest[!, col] = Int.(manifest[!, col])
    end
    return manifest
end

function successful_run_manifest(path::AbstractString)
    manifest = parse_manifest_columns!(read_manifest(path))
    :status in propertynames(manifest) || return manifest
    return manifest[String.(manifest.status) .== "success", :]
end

function plot_targets(manifest::DataFrame, cfg, opts, settings)
    role_mask = :analysis_role in propertynames(manifest) ?
                String.(manifest.analysis_role) .== settings.analysis_role :
                trues(nrow(manifest))
    rows = manifest[role_mask, :]

    if haskey(cfg, "targets")
        targets = configured_targets(cfg, Bool(opts["smoke-test"]))
        opts["year"] !== nothing && (targets = [target for target in targets if target.wave_id == string(opts["year"])])
        opts["scenario"] !== nothing && (targets = [target for target in targets if target.scenario_name == string(opts["scenario"])])
        override_m = parse_m_values(opts["m"])
        override_m !== nothing && (targets = [
            (wave_id = target.wave_id, scenario_name = target.scenario_name, m_values = override_m)
            for target in targets
        ])
        return targets
    end

    opts["year"] !== nothing && (rows = rows[String.(rows.wave_id) .== string(opts["year"]), :])
    opts["scenario"] !== nothing && (rows = rows[String.(rows.scenario_name) .== string(opts["scenario"]), :])
    override_m = parse_m_values(opts["m"])
    override_m !== nothing && (rows = rows[in.(rows.m, Ref(override_m)), :])

    target_rows = unique(select(rows, :wave_id, :scenario_name))
    targets = NamedTuple[]
    for row in eachrow(target_rows)
        target_rows_for_key = rows[
            (string.(rows.wave_id) .== string(row.wave_id)) .&
            (string.(rows.scenario_name) .== string(row.scenario_name)),
            :,
        ]
        push!(targets, (
            wave_id = string(row.wave_id),
            scenario_name = string(row.scenario_name),
            m_values = sort(unique(Int.(target_rows_for_key.m))),
        ))
    end

    isempty(targets) && error("No global plot targets selected from $(settings.run_manifest).")
    return sort(targets; by = target -> (target.wave_id, target.scenario_name))
end

function metadata_from_manifest_row(row)
    allowed = [
        :year, :scenario_name, :m, :analysis_role, :scenario_dir,
        :base_scenario_dir, :is_diagnostic, :active_candidates,
        :candidate_label,
    ]
    pairs = Pair{Symbol,Any}[]
    for key in allowed
        key in propertynames(row) || continue
        push!(pairs, key => row[key])
    end
    return (; pairs...)
end

function load_results_from_manifest(manifest::DataFrame)
    items = pp.StudyBatchItem[]
    results = pp.PipelineResult[]
    metadata = NamedTuple[]

    for row in eachrow(manifest)
        result_path = String(row.result_path)
        isfile(result_path) || error("Cached PipelineResult not found: $(result_path)")
        result = pp.load_pipeline_result(result_path)
        :spec_hash in propertynames(row) && string(row.spec_hash) != basename(result.cache_dir) && error(
            "Manifest/result hash mismatch for $(result_path).",
        )
        meta = metadata_from_manifest_row(row)
        push!(items, pp.StudyBatchItem(result.spec; meta...))
        push!(results, result)
        push!(metadata, meta)
    end

    return pp.BatchRunResult(pp.StudyBatchSpec(items), results, metadata)
end

function subset_results(results::pp.BatchRunResult;
                        wave_id = nothing,
                        scenario_name = nothing,
                        analysis_role = nothing,
                        imputer_backend = nothing,
                        linearizer_policy = nothing,
                        m_values = nothing)
    items = pp.StudyBatchItem[]
    subset = pp.PipelineResult[]
    metadata = NamedTuple[]
    mset = m_values === nothing ? nothing : Set(Int.(m_values))

    for (idx, item) in enumerate(results.batch.items)
        spec = item.spec
        meta = results.metadata[idx]
        wave_id !== nothing && spec.wave_id != string(wave_id) && continue
        scenario_name !== nothing &&
            (!hasproperty(meta, :scenario_name) || string(meta.scenario_name) != string(scenario_name)) &&
            continue
        analysis_role !== nothing &&
            (!hasproperty(meta, :analysis_role) || string(meta.analysis_role) != string(analysis_role)) &&
            continue
        imputer_backend !== nothing && spec.imputer_backend != Symbol(imputer_backend) && continue
        linearizer_policy !== nothing && spec.linearizer_policy != Symbol(linearizer_policy) && continue
        mset !== nothing && (!hasproperty(meta, :m) || Int(meta.m) ∉ mset) && continue

        push!(items, item)
        push!(subset, results.results[idx])
        push!(metadata, meta)
    end

    isempty(items) && error("No cached results matched the requested global plot subset.")
    return pp.BatchRunResult(pp.StudyBatchSpec(items), subset, metadata)
end

function backend_combinations(manifest::DataFrame, target, settings, opts)
    rows = manifest[
        (string.(manifest.wave_id) .== target.wave_id) .&
        (string.(manifest.scenario_name) .== target.scenario_name) .&
        (:analysis_role in propertynames(manifest) ? (string.(manifest.analysis_role) .== settings.analysis_role) : trues(nrow(manifest))) .&
        in.(manifest.m, Ref(target.m_values)),
        :,
    ]
    opts["backend"] !== nothing && (rows = rows[string.(rows.imputer_backend) .== string(opts["backend"]), :])
    opts["linearizer"] !== nothing && (rows = rows[string.(rows.linearizer_policy) .== string(opts["linearizer"]), :])
    combo_rows = unique(select(rows, :imputer_backend, :linearizer_policy))
    combos = [
        (imputer_backend = Symbol(row.imputer_backend), linearizer_policy = Symbol(row.linearizer_policy))
        for row in eachrow(combo_rows)
    ]
    isempty(combos) && error("No backend/linearizer combinations selected for $(target.wave_id)/$(target.scenario_name).")
    return sort(combos; by = combo -> (String(combo.imputer_backend), String(combo.linearizer_policy)))
end

function ungrouped_measure_mask(df::AbstractDataFrame, measures)
    wanted = Set(Symbol.(measures))
    grouping_mask = :grouping in propertynames(df) ? ismissing.(df.grouping) : trues(nrow(df))
    return [Symbol(row.measure) in wanted for row in eachrow(df)] .& grouping_mask
end

function global_table(df::AbstractDataFrame, measures)
    return sorted_plot_table(DataFrame(df[ungrouped_measure_mask(df, measures), :]))
end

function decorate_decomposition(df::DataFrame, result::pp.PipelineResult, meta::NamedTuple)
    out = copy(df)
    spec = result.spec
    fixed = merge((
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
    ), meta)

    for key in propertynames(fixed)
        out[!, key] = fill(getproperty(fixed, key), nrow(out))
    end
    return out
end

function decomposition_table(results::pp.BatchRunResult)
    tables = DataFrame[]
    for (idx, result) in enumerate(results.results)
        meta = merge(results.metadata[idx], (batch_index = idx,))
        push!(tables, decorate_decomposition(pp.decomposition_table(result.decomposition), result, meta))
    end
    return isempty(tables) ? DataFrame() : vcat(tables...; cols = :union)
end

function global_plot_stem(rows::AbstractDataFrame, target, combo)
    row = rows[1, :]
    return join((
        "global_measures",
        "year-$(sanitize_path_component(row.year))",
        "scenario-$(sanitize_path_component(target.scenario_name))",
        "backend-$(sanitize_path_component(combo.imputer_backend))",
        "linearizer-$(sanitize_path_component(combo.linearizer_policy))",
        "B-$(row.B)",
        "R-$(row.R)",
        "K-$(row.K)",
        "draws-$(row.n_draws)",
    ), "_")
end

function scenario_output_dir(settings, target)
    return joinpath(
        settings.plot_root,
        sanitize_path_component(target.wave_id),
        sanitize_path_component(target.scenario_name),
    )
end

function existing_manifest(path::AbstractString)
    isfile(path) || return DataFrame()
    return CSV.read(path, DataFrame)
end

function previous_success(existing::DataFrame, artifact_id::AbstractString, source_hash::AbstractString, outputs)
    isempty(existing) && return false
    all(col -> col in propertynames(existing), (:artifact_id, :source_manifest_hash, :status, :output_path)) ||
        return false
    required = Set(String.(outputs))
    rows = existing[
        (String.(existing.artifact_id) .== artifact_id) .&
        (String.(existing.source_manifest_hash) .== source_hash) .&
        (String.(existing.status) .== "success"),
        :,
    ]
    isempty(rows) && return false
    existing_outputs = Set(String.(rows.output_path))
    return all(path -> path in existing_outputs && isfile(path), required)
end

function save_figure_all(fig, stem::AbstractString, dir::AbstractString, formats)
    mkpath(dir)
    outputs = String[]
    for ext in formats
        path = joinpath(dir, stem * ext)
        CairoMakie.save(path, fig; px_per_unit = 4)
        push!(outputs, path)
    end
    return outputs
end

function manifest_row(; artifact_id, target, combo, measures, input_path, output_path,
                      format, status, error, source_hash)
    return (
        stage = "plot_global",
        artifact_id = artifact_id,
        wave_id = target.wave_id,
        year = target.wave_id,
        scenario_name = target.scenario_name,
        imputer_backend = String(combo.imputer_backend),
        linearizer_policy = String(combo.linearizer_policy),
        m_values = join(target.m_values, "|"),
        measures = join(String.(measures), "|"),
        input_path = input_path,
        output_path = output_path,
        format = format,
        source_manifest_hash = source_hash,
        status = status,
        error = error,
        timestamp = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS"),
    )
end

function write_scenario_tables!(dir, scenario_results, measures, settings, target, source_hash)
    settings.write_tables || return NamedTuple[]
    rows = NamedTuple[]
    tables = (
        summary_table = global_table(pp.pipeline_summary_table(scenario_results), measures),
        panel_table = global_table(pp.pipeline_panel_table(scenario_results), measures),
        decomposition_table = global_table(decomposition_table(scenario_results), measures),
    )
    combo = (imputer_backend = :all, linearizer_policy = :all)

    for (name, df) in pairs(tables)
        path = joinpath(dir, string(name, ".csv"))
        CSV.write(path, df)
        push!(rows, manifest_row(
            artifact_id = string(target.wave_id, "_", target.scenario_name, "_", name),
            target = target,
            combo = combo,
            measures = measures,
            input_path = settings.run_manifest,
            output_path = path,
            format = "csv",
            status = "success",
            error = "",
            source_hash = source_hash,
        ))
    end
    return rows
end

function write_plot_outputs!(results, manifest, target, settings, opts, existing, source_hash)
    dir = scenario_output_dir(settings, target)
    mkpath(dir)
    rows = NamedTuple[]
    scenario_results = subset_results(
        results;
        wave_id = target.wave_id,
        scenario_name = target.scenario_name,
        analysis_role = settings.analysis_role,
        m_values = target.m_values,
    )
    append!(rows, write_scenario_tables!(dir, scenario_results, settings.measures, settings, target, source_hash))

    for combo in backend_combinations(manifest, target, settings, opts)
        combo_results = subset_results(
            scenario_results;
            imputer_backend = combo.imputer_backend,
            linearizer_policy = combo.linearizer_policy,
        )
        plot_data = pp.pipeline_scenario_plot_data(
            combo_results;
            wave_id = target.wave_id,
            scenario_name = target.scenario_name,
            imputer_backend = combo.imputer_backend,
            measures = settings.measures,
        )
        stem = global_plot_stem(plot_data.rows, target, combo)
        artifact_id = stem
        output_paths = [joinpath(dir, stem * ext) for ext in settings.formats]
        plot_data_path = joinpath(dir, stem * "_plot_data.csv")
        all_outputs = vcat(output_paths, [plot_data_path])

        if !settings.force && previous_success(existing, artifact_id, source_hash, all_outputs)
            for path in all_outputs
                push!(rows, manifest_row(
                    artifact_id = artifact_id,
                    target = target,
                    combo = combo,
                    measures = settings.measures,
                    input_path = settings.run_manifest,
                    output_path = path,
                    format = lowercase(splitext(path)[2][2:end]),
                    status = "skipped",
                    error = "",
                    source_hash = source_hash,
                ))
            end
            continue
        end

        CSV.write(plot_data_path, sorted_plot_table(plot_data.rows))
        fig = pp.plot_pipeline_scenario(
            combo_results;
            wave_id = target.wave_id,
            scenario_name = target.scenario_name,
            imputer_backend = combo.imputer_backend,
            measures = settings.measures,
            plot_kind = :dotwhisker,
            connect_lines = true,
            ytick_step = 0.1,
        )
        saved = save_figure_all(fig, stem, dir, settings.formats)

        for path in vcat(saved, [plot_data_path])
            push!(rows, manifest_row(
                artifact_id = artifact_id,
                target = target,
                combo = combo,
                measures = settings.measures,
                input_path = settings.run_manifest,
                output_path = path,
                format = lowercase(splitext(path)[2][2:end]),
                status = "success",
                error = "",
                source_hash = source_hash,
            ))
        end
    end

    return rows
end

function print_plot_plan(targets, settings, opts)
    println("Global plot stage plan:")
    println("  run_manifest=", settings.run_manifest)
    println("  measure_manifest=", settings.measure_manifest)
    println("  plot_root=", settings.plot_root)
    println("  measures=", join(String.(settings.measures), ","))
    println("  formats=", join(settings.formats, ","))
    println("  force=", settings.force, " dry_run=", settings.dry_run)
    for (idx, target) in enumerate(targets)
        println(
            "  [", idx, "] wave=", target.wave_id,
            " scenario=", target.scenario_name,
            " m=", join(target.m_values, ","),
            opts["backend"] === nothing ? "" : " backend=$(opts["backend"])",
            opts["linearizer"] === nothing ? "" : " linearizer=$(opts["linearizer"])",
        )
    end
end

function main(args = ARGS)
    opts = parse_plot_args(args)
    cfg = load_orchestration_config(opts["config"])
    settings = plot_settings(cfg, opts)
    run_manifest = successful_run_manifest(settings.run_manifest)
    isfile(settings.measure_manifest) || error("Measure manifest not found: $(settings.measure_manifest). Run 04_measures first.")
    targets = plot_targets(run_manifest, cfg, opts, settings)

    print_plot_plan(targets, settings, opts)
    settings.dry_run && return nothing

    source_hash = manifest_hash(settings.run_manifest)
    results = load_results_from_manifest(run_manifest)
    existing = existing_manifest(settings.plot_manifest)
    rows = NamedTuple[]

    for target in targets
        append!(rows, write_plot_outputs!(results, run_manifest, target, settings, opts, existing, source_hash))
    end

    mkpath(dirname(settings.plot_manifest))
    CSV.write(settings.plot_manifest, DataFrame(rows))
    println("Wrote global plots under ", settings.plot_root)
    println("Wrote plot manifest to ", settings.plot_manifest)
    return nothing
end

main()
