#!/usr/bin/env julia

using CSV
using DataFrames
using Dates
using PrefPol
using TOML
import PrefPol as pp

const COMPOSABLE_ROOT = normpath(joinpath(@__DIR__, ".."))
const DEFAULT_OUTPUT_ROOT = joinpath(COMPOSABLE_ROOT, "output")
const DEFAULT_CACHE_ROOT = joinpath(DEFAULT_OUTPUT_ROOT, "cache")
const DEFAULT_CONFIG_DIR = joinpath(pp.project_root, "config")
const DEFAULT_MAIN_MEASURES = [:Psi, :R, :HHI, :RHHI, :C, :D, :O, :S]
const DEFAULT_SMOKE_MEASURES = [:Psi, :R, :RHHI, :C, :D, :O, :S, :lambda_sep]

function parse_args(args)
    opts = Dict{String,Any}(
        "config" => nothing,
        "year" => nothing,
        "scenario" => nothing,
        "m" => nothing,
        "backend" => nothing,
        "linearizer" => nothing,
        "force" => false,
        "dry-run" => false,
        "smoke-test" => false,
    )

    i = 1
    while i <= length(args)
        arg = args[i]
        if arg in ("--force", "--dry-run", "--smoke-test")
            opts[arg[3:end]] = true
        elseif arg in ("--config", "--year", "--scenario", "--m", "--backend", "--linearizer")
            i == length(args) && error("$(arg) requires a value.")
            opts[arg[3:end]] = args[i + 1]
            i += 1
        elseif arg in ("--help", "-h")
            println("""
            Usage:
              julia --project=PrefPol PrefPol/composable_running/stages/04_measures.jl [--config PATH] [--year YEAR] [--scenario NAME] [--m VALUE_OR_RANGE] [--backend NAME] [--linearizer NAME] [--force] [--dry-run] [--smoke-test]

            Phase 4 wrapper:
              Builds PipelineSpec jobs and calls PrefPol.run_batch. Upstream
              bootstrap, imputation, and linearization artifacts are produced
              by the existing nested pipeline until Phase 5 splits them out.
            """)
            exit(0)
        else
            error("Unknown argument: $(arg)")
        end
        i += 1
    end

    return opts
end

function parse_m_values(raw)
    raw === nothing && return nothing
    text = strip(String(raw))
    isempty(text) && return nothing

    if occursin(":", text)
        parts = split(text, ":")
        length(parts) == 2 || error("Invalid --m range: $(raw)")
        lo, hi = parse.(Int, strip.(parts))
        lo <= hi || error("Invalid --m range with lower bound greater than upper bound: $(raw)")
        return collect(lo:hi)
    end

    return [parse(Int, strip(part)) for part in split(text, ",") if !isempty(strip(part))]
end

function as_symbol_vector(values)
    return Symbol.(String.(collect(values)))
end

function config_value(table, key::AbstractString, default)
    table isa AbstractDict || return default
    return get(table, key, default)
end

function resolve_path(path::AbstractString)
    return isabspath(path) ? normpath(path) : normpath(joinpath(pp.project_root, path))
end

function load_orchestration_config(path)
    path === nothing && return Dict{String,Any}()
    isfile(path) || error("Config not found: $(path)")
    return TOML.parsefile(path)
end

function year_config_paths(config_dir::AbstractString)
    paths = sort(filter(path -> occursin(r"/\d{4}\.toml$", path),
                        readdir(config_dir; join = true)))
    isempty(paths) && error("No year TOML files found under $(config_dir).")
    return paths
end

function load_waves()
    waves = PrefPol.SurveyWaveConfig[]
    for path in year_config_paths(DEFAULT_CONFIG_DIR)
        push!(waves, pp.load_survey_wave_config(path))
    end
    return waves, pp.build_source_registry(waves), Dict(w.wave_id => w for w in waves)
end

function default_targets(smoke_test::Bool)
    if smoke_test
        return [(wave_id = "2018", scenario_name = "main_2018", m_values = [2])]
    end
    return [(wave_id = "2018", scenario_name = "main_2018", m_values = collect(2:5))]
end

function configured_targets(cfg, smoke_test::Bool)
    raw_targets = get(cfg, "targets", Any[])
    isempty(raw_targets) && return default_targets(smoke_test)

    targets = NamedTuple[]
    for target in raw_targets
        wave_id = string(config_value(target, "wave_id", config_value(target, "year", "")))
        scenario_name = string(config_value(target, "scenario_name", ""))
        isempty(wave_id) && error("Every [[targets]] entry needs wave_id or year.")
        isempty(scenario_name) && error("Every [[targets]] entry needs scenario_name.")

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

function selected_targets(cfg, opts)
    targets = configured_targets(cfg, Bool(opts["smoke-test"]))

    if opts["year"] !== nothing
        year = string(opts["year"])
        targets = [target for target in targets if target.wave_id == year]
    end
    if opts["scenario"] !== nothing
        scenario = string(opts["scenario"])
        targets = [target for target in targets if target.scenario_name == scenario]
    end

    override_m = parse_m_values(opts["m"])
    if override_m !== nothing
        targets = [(wave_id = target.wave_id,
                    scenario_name = target.scenario_name,
                    m_values = override_m) for target in targets]
    end

    isempty(targets) && error("No targets selected for measure stage.")
    return targets
end

function run_settings(cfg, opts)
    run_cfg = get(cfg, "run", Dict{String,Any}())
    smoke = Bool(opts["smoke-test"])

    B = Int(config_value(run_cfg, "B", smoke ? 2 : 8))
    R = Int(config_value(run_cfg, "R", smoke ? 2 : 2))
    K = Int(config_value(run_cfg, "K", smoke ? 2 : 2))
    force = Bool(opts["force"]) || Bool(config_value(run_cfg, "force", false))
    dry_run = Bool(opts["dry-run"]) || Bool(config_value(run_cfg, "dry_run", false))

    output_root = resolve_path(String(config_value(run_cfg, "output_root", DEFAULT_OUTPUT_ROOT)))
    cache_root = resolve_path(String(config_value(run_cfg, "cache_root", DEFAULT_CACHE_ROOT)))
    tie_policy = Symbol(String(config_value(run_cfg, "consensus_tie_policy", "average")))

    backends = opts["backend"] === nothing ?
               as_symbol_vector(config_value(run_cfg, "imputer_backends", smoke ? ["zero"] : ["random"])) :
               [Symbol(String(opts["backend"]))]
    linearizers = opts["linearizer"] === nothing ?
                  as_symbol_vector(config_value(run_cfg, "linearizer_policies", smoke ? ["pattern_conditional"] : ["random_ties", "pattern_conditional"])) :
                  [Symbol(String(opts["linearizer"]))]
    measures = as_symbol_vector(config_value(run_cfg, "measures", smoke ? String.(DEFAULT_SMOKE_MEASURES) : String.(DEFAULT_MAIN_MEASURES)))

    return (
        B = B,
        R = R,
        K = K,
        force = force,
        dry_run = dry_run,
        output_root = output_root,
        cache_root = cache_root,
        consensus_tie_policy = tie_policy,
        imputer_backends = backends,
        linearizer_policies = linearizers,
        measures = measures,
    )
end

function validate_target!(wave_by_id, target)
    haskey(wave_by_id, target.wave_id) || error(
        "Target wave $(target.wave_id) is not configured under $(DEFAULT_CONFIG_DIR).",
    )
    wave = wave_by_id[target.wave_id]
    haskey(wave.scenario_candidates, target.scenario_name) || error(
        "Target scenario $(target.scenario_name) is not configured for wave $(target.wave_id).",
    )
    return wave
end

function build_batch(targets, wave_by_id, settings)
    items = pp.StudyBatchItem[]

    for target in targets
        wave = validate_target!(wave_by_id, target)
        for m in target.m_values
            for backend in settings.imputer_backends
                for linearizer in settings.linearizer_policies
                    spec = pp.build_pipeline_spec(
                        wave;
                        scenario_name = target.scenario_name,
                        m = m,
                        groupings = Symbol.(wave.demographic_cols),
                        measures = settings.measures,
                        B = settings.B,
                        R = settings.R,
                        K = settings.K,
                        imputer_backend = backend,
                        linearizer_policy = linearizer,
                        consensus_tie_policy = settings.consensus_tie_policy,
                    )

                    push!(items, pp.StudyBatchItem(
                        spec;
                        year = wave.year,
                        scenario_name = target.scenario_name,
                        m = m,
                        analysis_role = "main",
                        active_candidates = join(spec.active_candidates, "|"),
                        candidate_label = pp.describe_candidate_set(spec.active_candidates),
                    ))
                end
            end
        end
    end

    isempty(items) && error("No measure-stage batch items were generated.")
    return pp.StudyBatchSpec(items)
end

function write_csv(path::AbstractString, df::AbstractDataFrame)
    mkpath(dirname(path))
    CSV.write(path, df)
    return path
end

function sorted_table(df::DataFrame)
    isempty(df) && return df
    preferred = [
        :wave_id, :year, :scenario_name, :analysis_role, :m,
        :imputer_backend, :linearizer_policy, :measure, :grouping,
        :b, :r, :k,
    ]
    cols = [col for col in preferred if col in propertynames(df)]
    isempty(cols) && return df
    return sort(df, cols)
end

function spec_metadata(result::pp.PipelineResult, meta::NamedTuple)
    spec = result.spec
    return merge((
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
        seed_namespace = spec.seed_namespace,
        schema_version = spec.schema_version,
        code_version = spec.code_version,
        cache_dir = result.cache_dir,
    ), meta)
end

function decorate!(df::DataFrame, meta::NamedTuple)
    for key in propertynames(meta)
        df[!, key] = fill(getproperty(meta, key), nrow(df))
    end
    return df
end

function decomposition_tables(results::pp.BatchRunResult)
    tables = DataFrame[]
    for (idx, result) in enumerate(results.results)
        meta = merge(results.metadata[idx], (batch_index = idx,))
        df = pp.decomposition_table(result.decomposition)
        decorate!(df, spec_metadata(result, meta))
        push!(tables, df)
    end
    return isempty(tables) ? DataFrame() : vcat(tables...; cols = :union)
end

function run_manifest(results::pp.BatchRunResult)
    rows = NamedTuple[]
    for (idx, result) in enumerate(results.results)
        meta = merge(results.metadata[idx], (batch_index = idx,))
        push!(rows, merge(spec_metadata(result, meta), (
            result_path = joinpath(result.cache_dir, "result.jld2"),
            stage_manifest_rows = nrow(result.stage_manifest),
            measure_rows = nrow(result.measure_cube),
            status = "success",
            timestamp = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS"),
        )))
    end
    return DataFrame(rows)
end

function measure_manifest(results::pp.BatchRunResult)
    rows = NamedTuple[]

    for (idx, result) in enumerate(results.results)
        meta = spec_metadata(result, merge(results.metadata[idx], (batch_index = idx,)))
        stage_manifest = result.stage_manifest
        measure_paths = Dict{Tuple{Int,Int,Int},String}()
        linearized_paths = Dict{Tuple{Int,Int,Int},String}()

        for row in eachrow(stage_manifest)
            key = (Int(row.b), Int(row.r), Int(row.k))
            stage_name = Symbol(row.stage)
            if stage_name === :measure
                measure_paths[key] = String(row.path)
            elseif stage_name === :linearized
                linearized_paths[key] = String(row.path)
            end
        end

        for row in eachrow(result.measure_cube)
            key = (Int(row.b), Int(row.r), Int(row.k))
            push!(rows, merge(meta, (
                stage = "measure",
                b = key[1],
                r = key[2],
                k = key[3],
                measure_id = String(row.measure),
                grouping = ismissing(row.grouping) ? "" : String(row.grouping),
                input_path = get(linearized_paths, key, ""),
                output_path = get(measure_paths, key, ""),
                status = "success",
                error = "",
                timestamp = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS"),
            )))
        end
    end

    return DataFrame(rows)
end

function print_plan(batch::pp.StudyBatchSpec, settings)
    println("Measure stage plan:")
    println("  cache_root=", settings.cache_root)
    println("  output_root=", settings.output_root)
    println("  B/R/K=", settings.B, "/", settings.R, "/", settings.K)
    println("  force=", settings.force, " dry_run=", settings.dry_run)
    for (idx, item) in enumerate(batch.items)
        meta = item.metadata
        spec = item.spec
        println(
            "  [", idx, "] wave=", spec.wave_id,
            " scenario=", meta.scenario_name,
            " m=", meta.m,
            " backend=", spec.imputer_backend,
            " linearizer=", spec.linearizer_policy,
            " measures=", join(String.(spec.measures), ","),
        )
    end
    return nothing
end

function main(args = ARGS)
    opts = parse_args(args)
    cfg = load_orchestration_config(opts["config"])
    settings = run_settings(cfg, opts)
    targets = selected_targets(cfg, opts)
    waves, registry, wave_by_id = load_waves()
    batch = build_batch(targets, wave_by_id, settings)

    print_plan(batch, settings)
    settings.dry_run && return nothing

    mkpath(settings.cache_root)
    mkpath(joinpath(settings.output_root, "measures"))
    mkpath(joinpath(settings.output_root, "manifests"))

    pipeline = pp.NestedStochasticPipeline(registry; cache_root = settings.cache_root)
    runner = pp.BatchRunner(pipeline)
    results = pp.run_batch(runner, batch; force = settings.force)

    measure_dir = joinpath(settings.output_root, "measures")
    manifest_dir = joinpath(settings.output_root, "manifests")

    write_csv(joinpath(measure_dir, "measure_table.csv"), sorted_table(pp.pipeline_measure_table(results)))
    write_csv(joinpath(measure_dir, "summary_table.csv"), sorted_table(pp.pipeline_summary_table(results)))
    write_csv(joinpath(measure_dir, "panel_table.csv"), sorted_table(pp.pipeline_panel_table(results)))
    write_csv(joinpath(measure_dir, "decomposition_table.csv"), sorted_table(decomposition_tables(results)))
    write_csv(joinpath(manifest_dir, "run_manifest.csv"), sorted_table(run_manifest(results)))
    write_csv(joinpath(manifest_dir, "measure_manifest.csv"), sorted_table(measure_manifest(results)))

    println("Wrote measure outputs under ", measure_dir)
    println("Wrote manifests under ", manifest_dir)
    return nothing
end

main()
