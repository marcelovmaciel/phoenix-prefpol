#!/usr/bin/env julia

const PREFPOL_PROJECT_DIR = normpath(joinpath(@__DIR__, "..", ".."))
PREFPOL_PROJECT_DIR in LOAD_PATH || pushfirst!(LOAD_PATH, PREFPOL_PROJECT_DIR)

include(joinpath(@__DIR__, "04_measures.jl"))

using SHA

try
    @eval using CairoMakie
catch err
    throw(ArgumentError(
        "Group plotting requires CairoMakie. Run with the plotting environment, for example:\n" *
        "  julia +1.11.9 --project=PrefPol/running/plotting_env " *
        "PrefPol/composable_running/stages/06_plot_group.jl"
    ))
end

const _PLOT_EXT = Base.get_extension(PrefPol, :PrefPolPlottingExt)
_PLOT_EXT === nothing && throw(ArgumentError(
    "PrefPolPlottingExt is not active. Run with Julia 1.11.9 and the plotting environment:\n" *
    "  julia +1.11.9 --project=PrefPol/running/plotting_env " *
    "PrefPol/composable_running/stages/06_plot_group.jl"
))

const M = CairoMakie.Makie
const ANALYSIS_ROLE_MAIN = "main"
const ANALYSIS_ROLE_O_SMOOTHED_EXTENSION = "o_smoothed_extension"

# TODO(plot_specs.toml): keep only as a smoke-test safeguard if plot specs are absent.
const DEFAULT_GROUP_HEATMAP_MEASURES = [:C, :D, :O, :S]
const DEFAULT_GROUP_HEATMAP_LABELS = Dict(:C => "C", :D => "D", :O => "1 - O", :S => "S")
const DEFAULT_GROUP_HEATMAP_COMPLEMENTS = [:O]
const DEFAULT_GROUP_HEATMAP_BASENAME = "paper_group_heatmap_panel_C_D_1mO_S"
const DEFAULT_O_SMOOTHED_MEASURES = [:O_smoothed]
const DEFAULT_O_SMOOTHED_LABELS = Dict(:O_smoothed => "1 - O_smoothed")
const DEFAULT_O_SMOOTHED_COMPLEMENTS = [:O_smoothed]
const DEFAULT_O_SMOOTHED_BASENAME = "paper_o_smoothed_heatmap"
const DEFAULT_PLOT_SPECS_PATH = joinpath(DEFAULT_CONFIG_DIR, "plot_specs.toml")

function parse_group_plot_args(args)
    if any(arg -> arg in ("--help", "-h"), args)
        println("""
        Usage:
          julia +1.11.9 --project=PrefPol/running/plotting_env PrefPol/composable_running/stages/06_plot_group.jl [--config PATH] [--year YEAR] [--scenario NAME] [--m VALUE_OR_RANGE] [--backend NAME] [--linearizer NAME] [--force] [--dry-run] [--smoke-test]

        Phase 7:
          Loads cached PipelineResult files from the composable run manifest and
          writes grouped paper heatmaps, heatmap CSVs, and group plot manifests.

        Config:
          Plot settings are read from PrefPol/config/plot_specs.toml by default.
          Optional [group_plots] keys in --config override that file.
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

function manifest_hash(path::AbstractString)
    return bytes2hex(SHA.sha256(read(path)))
end

function sanitize_path_component(value)
    return replace(string(value), r"[^A-Za-z0-9._-]+" => "_")
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
    return manifest[string.(manifest.status) .== "success", :]
end

function symbol_dict(raw, default::Dict{Symbol,String})
    raw isa AbstractDict || return default
    return Dict(Symbol(String(key)) => String(value) for (key, value) in raw)
end

function optional_symbol_vector(raw)
    raw === nothing && return nothing
    raw == "configured" && return nothing
    return Symbol.(String.(collect(raw)))
end

function load_plot_specs_config()
    if !isfile(DEFAULT_PLOT_SPECS_PATH)
        @warn "Plot specs not found at $(DEFAULT_PLOT_SPECS_PATH); using temporary smoke-test fallback defaults. TODO(plot_specs.toml)"
        return Dict{String,Any}()
    end
    return TOML.parsefile(DEFAULT_PLOT_SPECS_PATH)
end

function merged_plot_section(cfg, name::AbstractString)
    specs = load_plot_specs_config()
    section = Dict{String,Any}()
    haskey(specs, "outputs") && merge!(section, specs["outputs"])
    haskey(specs, "filters") && merge!(section, specs["filters"])
    haskey(specs, name) && merge!(section, specs[name])
    haskey(cfg, name) && merge!(section, cfg[name])
    isempty(section) && @warn "No [$name] section found; using temporary smoke-test fallback defaults. TODO(plot_specs.toml)"
    return section
end

function configured_plot_targets(plot_cfg, key::AbstractString, smoke_test::Bool)
    raw_targets = get(plot_cfg, key, Any[])
    isempty(raw_targets) && return NamedTuple[]

    targets = NamedTuple[]
    for target in raw_targets
        wave_id = string(config_value(target, "wave_id", config_value(target, "year", "")))
        scenario_name = string(config_value(target, "scenario_name", ""))
        isempty(wave_id) && error("Every [group_plots] target needs wave_id or year.")
        isempty(scenario_name) && error("Every [group_plots] target needs scenario_name.")
        m_values = if haskey(target, "m_values")
            Int.(target["m_values"])
        elseif haskey(target, "m_range")
            range = Int.(target["m_range"])
            length(range) == 2 || error("m_range must contain [lo, hi].")
            collect(range[1]:range[2])
        else
            smoke_test ? [2] : collect(2:5)
        end
        push!(targets, (wave_id = wave_id, scenario_name = scenario_name, m_values = m_values))
    end
    return targets
end

function selected_filter_values(opts, plot_cfg, key::AbstractString)
    opts[key[1:end-1]] !== nothing && return [string(opts[key[1:end-1]])]
    haskey(plot_cfg, key) || return nothing
    values = string.(collect(plot_cfg[key]))
    return isempty(values) ? nothing : values
end

function group_plot_settings(cfg, opts)
    run_cfg = get(cfg, "run", Dict{String,Any}())
    plot_cfg = merged_plot_section(cfg, "group_plots")
    o_smoothed_cfg = get(plot_cfg, "o_smoothed", Dict{String,Any}())
    output_root = resolve_path(String(config_value(plot_cfg, "output_root",
                                 config_value(run_cfg, "output_root", DEFAULT_OUTPUT_ROOT))))

    return (
        output_root = output_root,
        plot_root = resolve_path(String(config_value(plot_cfg, "plot_root",
                             joinpath(output_root, "plots", "group")))),
        run_manifest = resolve_path(String(config_value(plot_cfg, "run_manifest",
                               joinpath(output_root, "manifests", "run_manifest.csv")))),
        measure_manifest = resolve_path(String(config_value(plot_cfg, "measure_manifest",
                                   joinpath(output_root, "manifests", "measure_manifest.csv")))),
        plot_manifest = resolve_path(String(config_value(plot_cfg, "plot_manifest",
                                  joinpath(output_root, "manifests", "plot_manifest.csv")))),
        group_plot_manifest = resolve_path(String(config_value(plot_cfg, "group_plot_manifest",
                                        joinpath(output_root, "manifests", "group_plot_manifest.csv")))),
        measures = as_symbol_vector(config_value(plot_cfg, "measures", String.(DEFAULT_GROUP_HEATMAP_MEASURES))),
        measure_labels = symbol_dict(config_value(plot_cfg, "measure_labels", Dict{String,Any}()),
                                     DEFAULT_GROUP_HEATMAP_LABELS),
        complement_measures = as_symbol_vector(config_value(plot_cfg, "complement_measures",
                                                   String.(DEFAULT_GROUP_HEATMAP_COMPLEMENTS))),
        basename = String(config_value(plot_cfg, "basename", DEFAULT_GROUP_HEATMAP_BASENAME)),
        formats = normalize_format.(config_value(plot_cfg, "formats", ["png"])),
        analysis_role = String(config_value(plot_cfg, "analysis_role", ANALYSIS_ROLE_MAIN)),
        groupings_by_wave = config_value(plot_cfg, "groupings_by_wave", Dict{String,Any}()),
        statistic = Symbol(String(config_value(plot_cfg, "statistic", "median"))),
        write_tables = Bool(config_value(plot_cfg, "write_tables", true)),
        targets = configured_plot_targets(plot_cfg, "targets", Bool(opts["smoke-test"])),
        backend_filter = selected_filter_values(opts, plot_cfg, "backends"),
        linearizer_filter = selected_filter_values(opts, plot_cfg, "linearizers"),
        o_smoothed_enabled = Bool(config_value(o_smoothed_cfg, "enabled",
                                      config_value(plot_cfg, "o_smoothed_enabled", true))),
        o_smoothed_analysis_role = String(config_value(o_smoothed_cfg, "analysis_role",
                                             config_value(plot_cfg, "o_smoothed_analysis_role",
                                             ANALYSIS_ROLE_O_SMOOTHED_EXTENSION))),
        o_smoothed_measures = as_symbol_vector(config_value(o_smoothed_cfg, "measures",
                                                  config_value(plot_cfg, "o_smoothed_measures",
                                                  String.(DEFAULT_O_SMOOTHED_MEASURES)))),
        o_smoothed_measure_labels = symbol_dict(config_value(o_smoothed_cfg, "measure_labels",
                                                    config_value(plot_cfg, "o_smoothed_measure_labels",
                                                    Dict{String,Any}())),
                                                DEFAULT_O_SMOOTHED_LABELS),
        o_smoothed_complement_measures = as_symbol_vector(config_value(
            o_smoothed_cfg,
            "complement_measures",
            config_value(plot_cfg, "o_smoothed_complement_measures", String.(DEFAULT_O_SMOOTHED_COMPLEMENTS)),
        )),
        o_smoothed_basename = String(config_value(o_smoothed_cfg, "basename",
                                        config_value(plot_cfg, "o_smoothed_basename",
                                        DEFAULT_O_SMOOTHED_BASENAME))),
        force = Bool(opts["force"]) || Bool(config_value(plot_cfg, "force", false)),
        dry_run = Bool(opts["dry-run"]) || Bool(config_value(plot_cfg, "dry_run", false)),
    )
end

function group_plot_targets(manifest::DataFrame, cfg, opts, settings)
    role_mask = :analysis_role in propertynames(manifest) ?
                string.(manifest.analysis_role) .== settings.analysis_role :
                trues(nrow(manifest))
    rows = manifest[role_mask, :]

    if !isempty(settings.targets)
        targets = settings.targets
        opts["year"] !== nothing && (targets = [target for target in targets if target.wave_id == string(opts["year"])])
        opts["scenario"] !== nothing && (targets = [target for target in targets if target.scenario_name == string(opts["scenario"])])
        override_m = parse_m_values(opts["m"])
        override_m !== nothing && (targets = [
            (wave_id = target.wave_id, scenario_name = target.scenario_name, m_values = override_m)
            for target in targets
        ])
        isempty(targets) && error("No group plot targets selected from plot_specs.toml.")
        return targets
    end

    @warn "No [group_plots.targets] found; deriving targets from run manifest as temporary smoke-test fallback. TODO(plot_specs.toml)"
    opts["year"] !== nothing && (rows = rows[string.(rows.wave_id) .== string(opts["year"]), :])
    opts["scenario"] !== nothing && (rows = rows[string.(rows.scenario_name) .== string(opts["scenario"]), :])
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

    isempty(targets) && error("No group plot targets selected from $(settings.run_manifest).")
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
                        m_values = nothing,
                        allow_empty::Bool = false)
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

    if isempty(items) && !allow_empty
        error("No cached results matched the requested group plot subset.")
    end
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
    settings.backend_filter !== nothing && (rows = rows[in.(string.(rows.imputer_backend), Ref(Set(settings.backend_filter))), :])
    settings.linearizer_filter !== nothing && (rows = rows[in.(string.(rows.linearizer_policy), Ref(Set(settings.linearizer_filter))), :])
    combo_rows = unique(select(rows, :imputer_backend, :linearizer_policy))
    combos = [
        (imputer_backend = Symbol(row.imputer_backend), linearizer_policy = Symbol(row.linearizer_policy))
        for row in eachrow(combo_rows)
    ]
    isempty(combos) && error("No backend/linearizer combinations selected for $(target.wave_id)/$(target.scenario_name).")
    return sort(combos; by = combo -> (String(combo.imputer_backend), String(combo.linearizer_policy)))
end

function selected_groupings(settings, target)
    raw = settings.groupings_by_wave isa AbstractDict ?
          get(settings.groupings_by_wave, target.wave_id, nothing) :
          nothing
    # TODO(plot_specs.toml): make grouping order explicit per paper target.
    return optional_symbol_vector(raw)
end

function _complement_group_plot_columns!(out::DataFrame, complemented::Set{Symbol})
    isempty(complemented) && return Set{Symbol}()

    transformed = Set{Symbol}()
    measure_mask = [Symbol(row.measure) in complemented for row in eachrow(out)]
    any(measure_mask) || return transformed

    for col in (:estimate, :mean_value)
        col in propertynames(out) || continue
        vals = Float64.(out[!, col])
        vals[measure_mask] .= 1.0 .- vals[measure_mask]
        out[!, col] = vals
        push!(transformed, col)
    end

    quantile_cols = (:q05, :q25, :q50, :q75, :q95)
    if all(col -> col in propertynames(out), quantile_cols)
        old_q05 = Float64.(out.q05)
        old_q25 = Float64.(out.q25)
        old_q50 = Float64.(out.q50)
        old_q75 = Float64.(out.q75)
        old_q95 = Float64.(out.q95)

        q05 = copy(old_q05)
        q25 = copy(old_q25)
        q50 = copy(old_q50)
        q75 = copy(old_q75)
        q95 = copy(old_q95)
        q05[measure_mask] .= 1.0 .- old_q95[measure_mask]
        q25[measure_mask] .= 1.0 .- old_q75[measure_mask]
        q50[measure_mask] .= 1.0 .- old_q50[measure_mask]
        q75[measure_mask] .= 1.0 .- old_q25[measure_mask]
        q95[measure_mask] .= 1.0 .- old_q05[measure_mask]

        out[!, :q05] = q05
        out[!, :q25] = q25
        out[!, :q50] = q50
        out[!, :q75] = q75
        out[!, :q95] = q95
        union!(transformed, quantile_cols)
    end

    if :min_value in propertynames(out) && :max_value in propertynames(out)
        old_min = Float64.(out.min_value)
        old_max = Float64.(out.max_value)
        min_vals = copy(old_min)
        max_vals = copy(old_max)
        min_vals[measure_mask] .= 1.0 .- old_max[measure_mask]
        max_vals[measure_mask] .= 1.0 .- old_min[measure_mask]
        out[!, :min_value] = min_vals
        out[!, :max_value] = max_vals
        push!(transformed, :min_value)
        push!(transformed, :max_value)
    end

    if :value_lo_min in propertynames(out) && :value_hi_max in propertynames(out)
        old_lo = Float64.(out.value_lo_min)
        old_hi = Float64.(out.value_hi_max)
        lo_vals = copy(old_lo)
        hi_vals = copy(old_hi)
        lo_vals[measure_mask] .= 1.0 .- old_hi[measure_mask]
        hi_vals[measure_mask] .= 1.0 .- old_lo[measure_mask]
        out[!, :value_lo_min] = lo_vals
        out[!, :value_hi_max] = hi_vals
        push!(transformed, :value_lo_min)
        push!(transformed, :value_hi_max)
    end

    return transformed
end

function prepare_group_plot_table(rows::AbstractDataFrame;
                                  value_column::Symbol = :estimate,
                                  complement_measures = Symbol[],
                                  measure_labels = nothing)
    out = DataFrame(rows)
    complemented = Set(Symbol.(collect(complement_measures)))
    transformed_cols = _complement_group_plot_columns!(out, complemented)

    if !(:m in propertynames(out)) && (:n_candidates in propertynames(out))
        out[!, :m] = Int.(out.n_candidates)
    end

    if value_column in propertynames(out)
        values = Float64.(out[!, value_column])
        if value_column in transformed_cols
            out[!, :value] = values
        else
            out[!, :value] = [
                Symbol(row.measure) in complemented ? 1.0 - values[idx] : values[idx]
                for (idx, row) in enumerate(eachrow(out))
            ]
        end
    elseif !(:value in propertynames(out)) && (:estimate in propertynames(out))
        out[!, :value] = Float64.(out.estimate)
    end

    if measure_labels !== nothing
        out[!, :display_measure] = [
            get(measure_labels, Symbol(row.measure), String(row.measure))
            for row in eachrow(out)
        ]
    end

    return sorted_table(out)
end

function prepare_paper_group_heatmap_table(rows::AbstractDataFrame;
                                           complement_measures = Symbol[],
                                           measure_labels)
    out = prepare_group_plot_table(
        rows;
        value_column = :q50,
        complement_measures = complement_measures,
        measure_labels = measure_labels,
    )

    if :display_measure in propertynames(out)
        out[!, :measure] = String.(out.display_measure)
    end

    keep_cols = [
        col for col in (
            :wave_id, :year, :scenario_name,
            :imputer_backend, :linearizer_policy,
            :B, :R, :K,
            :m, :n_candidates, :grouping, :measure,
            :n_draws,
            :value, :estimate, :mean_value,
            :q05, :q25, :q50, :q75, :q95,
            :min_value, :max_value, :value_lo_min, :value_hi_max,
        ) if col in propertynames(out)
    ]

    return sorted_table(select(out, keep_cols))
end

function scenario_output_dir(settings, target)
    return joinpath(
        settings.plot_root,
        sanitize_path_component(target.wave_id),
        sanitize_path_component(target.scenario_name),
    )
end

function combo_stem(combo)
    return string(
        sanitize_path_component(String(combo.imputer_backend)),
        "_",
        sanitize_path_component(String(combo.linearizer_policy)),
    )
end

function group_heatmap_plot_stem(rows::AbstractDataFrame, target, combo, basename::AbstractString)
    row = rows[1, :]
    year_value = :year in propertynames(rows) ? row.year : target.wave_id

    return join((
        basename,
        "year-$(sanitize_path_component(string(year_value)))",
        "scenario-$(sanitize_path_component(string(target.scenario_name)))",
        "backend-$(sanitize_path_component(string(combo.imputer_backend)))",
        "linearizer-$(sanitize_path_component(string(combo.linearizer_policy)))",
        "B-$(row.B)",
        "R-$(row.R)",
        "K-$(row.K)",
        "draws-$(row.n_draws)",
    ), "_")
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
        (string.(existing.artifact_id) .== artifact_id) .&
        (string.(existing.source_manifest_hash) .== source_hash) .&
        (string.(existing.status) .== "success"),
        :,
    ]
    isempty(rows) && return false
    existing_outputs = Set(string.(rows.output_path))
    return all(path -> path in existing_outputs && isfile(path), required)
end

function manifest_row(; artifact_id, target, combo, measures, input_path, output_path,
                      format, status, error, source_hash)
    return (
        stage = "plot_group",
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

function append_output_rows!(rows, artifact_id, target, combo, measures, input_path, paths,
                             status, source_hash)
    for path in paths
        ext = splitext(path)[2]
        fmt = isempty(ext) ? "" : lowercase(ext[2:end])
        push!(rows, manifest_row(
            artifact_id = artifact_id,
            target = target,
            combo = combo,
            measures = measures,
            input_path = input_path,
            output_path = path,
            format = fmt,
            status = status,
            error = "",
            source_hash = source_hash,
        ))
    end
    return rows
end

function write_main_group_outputs!(rows, combo_results, target, combo, dir, settings, groupings,
                                   existing, source_hash)
    heatmap_data = pp.pipeline_group_heatmap_values(
        combo_results;
        wave_id = target.wave_id,
        scenario_name = target.scenario_name,
        imputer_backend = combo.imputer_backend,
        measures = settings.measures,
        groupings = groupings,
        statistic = settings.statistic,
    )
    stem = group_heatmap_plot_stem(heatmap_data.rows, target, combo, settings.basename)
    table_path = joinpath(dir, settings.basename * "_table_" * combo_stem(combo) * ".csv")
    output_paths = vcat([joinpath(dir, stem * ext) for ext in settings.formats], [table_path])

    if !settings.force && previous_success(existing, stem, source_hash, output_paths)
        append_output_rows!(rows, stem, target, combo, settings.measures, settings.run_manifest,
                            output_paths, "skipped", source_hash)
        return rows
    end

    settings.write_tables && CSV.write(table_path, prepare_paper_group_heatmap_table(
        heatmap_data.rows;
        complement_measures = settings.complement_measures,
        measure_labels = settings.measure_labels,
    ))
    fig = _PLOT_EXT.plot_pipeline_group_paper_heatmap(
        combo_results;
        wave_id = target.wave_id,
        scenario_name = target.scenario_name,
        imputer_backend = combo.imputer_backend,
        measures = settings.measures,
        statistic = settings.statistic,
        groupings = groupings,
        complement_measures = settings.complement_measures,
        measure_labels = settings.measure_labels,
        colormap = M.Reverse(:RdBu),
        show_values = true,
        colorbar_label = "median value",
        clist_size = 60,
    )
    saved = save_figure_all(fig, stem, dir, settings.formats)
    append_output_rows!(rows, stem, target, combo, settings.measures, settings.run_manifest,
                        vcat(saved, settings.write_tables ? [table_path] : String[]),
                        "success", source_hash)
    return rows
end

function write_o_smoothed_outputs!(rows, o_smoothed_results, target, combo, dir, settings, groupings,
                                   existing, source_hash)
    o_combo_results = subset_results(
        o_smoothed_results;
        imputer_backend = combo.imputer_backend,
        linearizer_policy = combo.linearizer_policy,
        allow_empty = true,
    )
    isempty(o_combo_results.results) && return rows

    heatmap_data = pp.pipeline_group_heatmap_values(
        o_combo_results;
        wave_id = target.wave_id,
        scenario_name = target.scenario_name,
        imputer_backend = combo.imputer_backend,
        measures = settings.o_smoothed_measures,
        groupings = groupings,
        statistic = settings.statistic,
    )
    stem = group_heatmap_plot_stem(heatmap_data.rows, target, combo, settings.o_smoothed_basename)
    table_path = joinpath(dir, settings.o_smoothed_basename * "_table_" * combo_stem(combo) * ".csv")
    output_paths = vcat([joinpath(dir, stem * ext) for ext in settings.formats], [table_path])

    if !settings.force && previous_success(existing, stem, source_hash, output_paths)
        append_output_rows!(rows, stem, target, combo, settings.o_smoothed_measures,
                            settings.run_manifest, output_paths, "skipped", source_hash)
        return rows
    end

    settings.write_tables && CSV.write(table_path, prepare_paper_group_heatmap_table(
        heatmap_data.rows;
        complement_measures = settings.o_smoothed_complement_measures,
        measure_labels = settings.o_smoothed_measure_labels,
    ))
    fig = _PLOT_EXT.plot_pipeline_group_paper_osmoothed_heatmap(
        o_combo_results;
        wave_id = target.wave_id,
        scenario_name = target.scenario_name,
        imputer_backend = combo.imputer_backend,
        statistic = settings.statistic,
        groupings = groupings,
        colormap = M.Reverse(:RdBu),
        show_values = true,
        colorbar_label = "median 1 - O_smoothed",
        clist_size = 60,
    )
    saved = save_figure_all(fig, stem, dir, settings.formats)
    append_output_rows!(rows, stem, target, combo, settings.o_smoothed_measures,
                        settings.run_manifest,
                        vcat(saved, settings.write_tables ? [table_path] : String[]),
                        "success", source_hash)
    return rows
end

function write_group_outputs!(results, manifest, target, settings, opts, existing, source_hash)
    dir = scenario_output_dir(settings, target)
    mkpath(dir)
    rows = NamedTuple[]
    main_results = subset_results(
        results;
        wave_id = target.wave_id,
        scenario_name = target.scenario_name,
        analysis_role = settings.analysis_role,
        m_values = target.m_values,
    )
    o_smoothed_results = settings.o_smoothed_enabled ? subset_results(
        results;
        wave_id = target.wave_id,
        scenario_name = target.scenario_name,
        analysis_role = settings.o_smoothed_analysis_role,
        m_values = target.m_values,
        allow_empty = true,
    ) : pp.BatchRunResult(pp.StudyBatchSpec(pp.StudyBatchItem[]), pp.PipelineResult[], NamedTuple[])
    groupings = selected_groupings(settings, target)

    for combo in backend_combinations(manifest, target, settings, opts)
        combo_results = subset_results(
            main_results;
            imputer_backend = combo.imputer_backend,
            linearizer_policy = combo.linearizer_policy,
        )
        write_main_group_outputs!(rows, combo_results, target, combo, dir, settings, groupings,
                                  existing, source_hash)
        settings.o_smoothed_enabled && write_o_smoothed_outputs!(
            rows,
            o_smoothed_results,
            target,
            combo,
            dir,
            settings,
            groupings,
            existing,
            source_hash,
        )
    end

    return rows
end

function write_plot_manifests!(settings, rows)
    group_df = DataFrame(rows)
    mkpath(dirname(settings.group_plot_manifest))
    CSV.write(settings.group_plot_manifest, group_df)

    existing = isfile(settings.plot_manifest) ? CSV.read(settings.plot_manifest, DataFrame) : DataFrame()
    combined = if isempty(existing)
        group_df
    elseif :stage in propertynames(existing)
        vcat(existing[string.(existing.stage) .!= "plot_group", :], group_df; cols = :union)
    else
        vcat(existing, group_df; cols = :union)
    end
    mkpath(dirname(settings.plot_manifest))
    CSV.write(settings.plot_manifest, combined)
    return nothing
end

function print_group_plot_plan(targets, settings, opts)
    println("Group plot stage plan:")
    println("  run_manifest=", settings.run_manifest)
    println("  measure_manifest=", settings.measure_manifest)
    println("  plot_root=", settings.plot_root)
    println("  group_plot_manifest=", settings.group_plot_manifest)
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
    opts = parse_group_plot_args(args)
    cfg = load_orchestration_config(opts["config"])
    settings = group_plot_settings(cfg, opts)
    run_manifest = successful_run_manifest(settings.run_manifest)
    isfile(settings.measure_manifest) || error("Measure manifest not found: $(settings.measure_manifest). Run 04_measures first.")
    targets = group_plot_targets(run_manifest, cfg, opts, settings)

    print_group_plot_plan(targets, settings, opts)
    settings.dry_run && return nothing

    source_hash = manifest_hash(settings.run_manifest)
    results = load_results_from_manifest(run_manifest)
    existing = existing_manifest(settings.group_plot_manifest)
    rows = NamedTuple[]

    for target in targets
        append!(rows, write_group_outputs!(results, run_manifest, target, settings, opts, existing, source_hash))
    end

    isempty(rows) && error("No group plot outputs were produced.")
    write_plot_manifests!(settings, rows)
    println("Wrote group plots under ", settings.plot_root)
    println("Wrote group plot manifest to ", settings.group_plot_manifest)
    return nothing
end

main()
