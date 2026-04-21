"""
    run_all_scenarios_small.jl

Lightweight exploratory entry point for the full exported nested analysis.
This script discovers every configured wave/scenario pair from `config/*.toml`,
runs the existing nested `bootstrap -> imputation -> linearization -> measures`
workflow with small BRK counts, and saves cache-backed `PipelineResult`s plus
CSV manifests/tables under `running/output/all_scenarios_small/`.

Run with the manifest-matched Julia for this repo, for example:

    julia +1.11 --startup-file=no --project=PrefPol PrefPol/running/run_all_scenarios_small.jl
"""

using DataFrames
using PrefPol
import PrefPol as pp

const CONFIG_DIR = joinpath(pp.project_root, "config")
const OUTPUT_ROOT = joinpath(pp.project_root, "running", "output", "all_scenarios_small")
const CACHE_ROOT = joinpath(OUTPUT_ROOT, "cache")

const B_SMALL = parse(Int, get(ENV, "PREFPOL_ALL_SCENARIOS_SMALL_B", "8"))
const R_SMALL = parse(Int, get(ENV, "PREFPOL_ALL_SCENARIOS_SMALL_R", "2"))
const K_SMALL = parse(Int, get(ENV, "PREFPOL_ALL_SCENARIOS_SMALL_K", "2"))
const FORCE_RUN = lowercase(get(ENV, "PREFPOL_ALL_SCENARIOS_SMALL_FORCE", "false")) in ("1", "true", "yes")

const CONSENSUS_TIE_POLICY = :average
const FULL_MEASURES = [
    :Psi, :R, :HHI, :RHHI,
    :C, :D, :D_median, :O, :O_smoothed, :Sep, :G, :Gsep, :S,
]
const BACKEND_COMBINATIONS = [
    (imputer_backend = :random, linearizer_policy = :random_ties),
    (imputer_backend = :random, linearizer_policy = :pattern_conditional),
    (imputer_backend = :mice, linearizer_policy = :random_ties),
    (imputer_backend = :mice, linearizer_policy = :pattern_conditional),
]

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

function sanitize_path_component(value::AbstractString)
    return replace(String(value), r"[^A-Za-z0-9._-]+" => "_")
end

function scenario_dir_rel(wcfg::pp.SurveyWaveConfig, scenario_name::AbstractString)
    return joinpath(sanitize_path_component(wcfg.wave_id), sanitize_path_component(scenario_name))
end

function scenario_dir_abs(wcfg::pp.SurveyWaveConfig, scenario_name::AbstractString)
    return joinpath(OUTPUT_ROOT, scenario_dir_rel(wcfg, scenario_name))
end

function load_all_wave_configs(cfgdir::AbstractString = CONFIG_DIR)
    paths = sort(filter(path -> endswith(path, ".toml"), readdir(cfgdir; join = true)))
    isempty(paths) && error("No TOML wave configs were found under $(cfgdir).")

    waves = pp.SurveyWaveConfig[]
    for path in paths
        push!(waves, pp.load_survey_wave_config(path))
    end

    registry = pp.build_source_registry(waves)
    wave_by_id = Dict(wave.wave_id => wave for wave in waves)
    return waves, registry, wave_by_id
end

function discover_supported_m_values(wcfg::pp.SurveyWaveConfig,
                                     scenario_name::AbstractString)
    supported = Int[]

    for m in 2:wcfg.max_candidates
        try
            pp.build_pipeline_spec(
                wcfg;
                scenario_name = scenario_name,
                m = m,
                measures = [:Psi],
                B = 1,
                R = 1,
                K = 1,
                imputer_backend = :random,
                linearizer_policy = :random_ties,
            )
            push!(supported, m)
        catch err
            println(
                "Skipping unsupported m=$(m) for wave=$(wcfg.wave_id) scenario=$(scenario_name): ",
                sprint(showerror, err),
            )
        end
    end

    isempty(supported) && error(
        "No supported m values were found for wave=$(wcfg.wave_id) scenario=$(scenario_name).",
    )

    return supported
end

function discover_all_targets(waves::Vector{pp.SurveyWaveConfig})
    targets = NamedTuple[]

    for wcfg in sort(waves; by = wave -> (wave.year, wave.wave_id))
        scenario_names = sort!(collect(keys(wcfg.scenario_candidates)))
        isempty(scenario_names) && error("Wave $(wcfg.wave_id) has no configured scenarios.")

        for scenario_name in scenario_names
            push!(targets, (
                wave_id = wcfg.wave_id,
                year = wcfg.year,
                scenario_name = scenario_name,
                m_values = discover_supported_m_values(wcfg, scenario_name),
                groupings = join(wcfg.demographic_cols, "|"),
                scenario_dir = scenario_dir_rel(wcfg, scenario_name),
            ))
        end
    end

    return targets
end

function build_batch(targets, wave_by_id)
    items = pp.StudyBatchItem[]

    for target in targets
        wcfg = wave_by_id[target.wave_id]

        for m in target.m_values
            for combo in BACKEND_COMBINATIONS
                spec = pp.build_pipeline_spec(
                    wcfg;
                    scenario_name = target.scenario_name,
                    m = m,
                    groupings = Symbol.(wcfg.demographic_cols),
                    measures = FULL_MEASURES,
                    B = B_SMALL,
                    R = R_SMALL,
                    K = K_SMALL,
                    imputer_backend = combo.imputer_backend,
                    linearizer_policy = combo.linearizer_policy,
                    consensus_tie_policy = CONSENSUS_TIE_POLICY,
                )

                push!(items, pp.StudyBatchItem(
                    spec;
                    year = target.year,
                    scenario_name = target.scenario_name,
                    m = m,
                    active_candidates = join(spec.active_candidates, "|"),
                    candidate_label = pp.describe_candidate_set(spec.active_candidates),
                    scenario_dir = target.scenario_dir,
                ))
            end
        end
    end

    isempty(items) && error("No batch items were generated for the exploratory runner.")
    return pp.StudyBatchSpec(items)
end

function result_metadata(result::pp.PipelineResult, extra_meta::NamedTuple = (;))
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
        cache_dir = result.cache_dir,
    ), extra_meta)
end

function decorate_table(df::AbstractDataFrame,
                        result::pp.PipelineResult,
                        extra_meta::NamedTuple = (;))
    out = DataFrame(df)
    meta = result_metadata(result, extra_meta)

    for name in propertynames(meta)
        out[!, name] = fill(getproperty(meta, name), nrow(out))
    end

    return out
end

function spec_decomposition_table(result::pp.PipelineResult,
                                  extra_meta::NamedTuple = (;))
    return decorate_table(pp.decomposition_table(result.decomposition), result, extra_meta)
end

function manifest_row(result::pp.PipelineResult, extra_meta::NamedTuple)
    spec = result.spec
    result_path = joinpath(result.cache_dir, "result.jld2")
    spec_stem = string(
        "m", lpad(string(getproperty(extra_meta, :m)), 2, '0'),
        "_", String(spec.imputer_backend),
        "_", String(spec.linearizer_policy),
        "_idx", lpad(string(getproperty(extra_meta, :batch_index)), 4, '0'),
    )
    scenario_root = joinpath(OUTPUT_ROOT, String(getproperty(extra_meta, :scenario_dir)))

    return (
        batch_index = getproperty(extra_meta, :batch_index),
        wave_id = spec.wave_id,
        year = getproperty(extra_meta, :year),
        scenario_name = getproperty(extra_meta, :scenario_name),
        m = getproperty(extra_meta, :m),
        active_candidates = join(spec.active_candidates, "|"),
        imputer_backend = String(spec.imputer_backend),
        linearizer_policy = String(spec.linearizer_policy),
        B = spec.B,
        R = spec.R,
        K = spec.K,
        output_dir = scenario_root,
        cache_dir = result.cache_dir,
        result_path = result_path,
        decomposition_csv = joinpath(scenario_root, "specs", spec_stem * "_decomposition.csv"),
    )
end

function sorted_table(df::DataFrame)
    sort_cols = [
        col for col in (
            :wave_id, :year, :scenario_name, :m,
            :imputer_backend, :linearizer_policy,
            :measure, :grouping, :b, :r, :k,
        ) if col in propertynames(df)
    ]
    isempty(sort_cols) && return df
    return sort(df, sort_cols)
end

function write_scenario_tables!(root_measure::DataFrame,
                                root_summary::DataFrame,
                                root_panel::DataFrame,
                                root_decomp::DataFrame,
                                manifest::DataFrame,
                                targets)
    for target in targets
        dir = joinpath(OUTPUT_ROOT, target.scenario_dir)
        mkpath(joinpath(dir, "specs"))

        mask = (manifest.wave_id .== target.wave_id) .&
               (manifest.scenario_name .== target.scenario_name)
        scenario_manifest = sorted_table(manifest[mask, :])
        save_csv(joinpath(dir, "scenario_manifest.csv"), scenario_manifest)

        measure_mask = (root_measure.wave_id .== target.wave_id) .&
                       (root_measure.scenario_name .== target.scenario_name)
        summary_mask = (root_summary.wave_id .== target.wave_id) .&
                       (root_summary.scenario_name .== target.scenario_name)
        panel_mask = (root_panel.wave_id .== target.wave_id) .&
                     (root_panel.scenario_name .== target.scenario_name)
        decomp_mask = (root_decomp.wave_id .== target.wave_id) .&
                      (root_decomp.scenario_name .== target.scenario_name)

        save_csv(joinpath(dir, "measure_table.csv"), sorted_table(root_measure[measure_mask, :]))
        save_csv(joinpath(dir, "summary_table.csv"), sorted_table(root_summary[summary_mask, :]))
        save_csv(joinpath(dir, "panel_table.csv"), sorted_table(root_panel[panel_mask, :]))
        save_csv(joinpath(dir, "decomposition_table.csv"), sorted_table(root_decomp[decomp_mask, :]))
    end

    return nothing
end

function print_run_summary(targets, batch::pp.StudyBatchSpec)
    println("="^80)
    println("run_all_scenarios_small")
    println("="^80)
    println("Config dir:             ", CONFIG_DIR)
    println("Output root:            ", OUTPUT_ROOT)
    println("Cache root:             ", CACHE_ROOT)
    println("B/R/K:                  ", B_SMALL, "/", R_SMALL, "/", K_SMALL)
    println("Force run:              ", FORCE_RUN)
    println("Backend combinations:   ", join([
        string(combo.imputer_backend, "+", combo.linearizer_policy)
        for combo in BACKEND_COMBINATIONS
    ], ", "))
    println("Targets:                ", length(targets), " wave/scenario pairs")
    println("Batch items:            ", length(batch.items))

    for target in targets
        println(
            "  - wave=", target.wave_id,
            " scenario=", target.scenario_name,
            " m=", join(target.m_values, ","),
            " groupings=", target.groupings,
        )
    end

    println()
    return nothing
end

function main()
    mkpath(CACHE_ROOT)

    waves, registry, wave_by_id = load_all_wave_configs()
    targets = discover_all_targets(waves)
    batch = build_batch(targets, wave_by_id)
    print_run_summary(targets, batch)

    pipeline = pp.NestedStochasticPipeline(registry; cache_root = CACHE_ROOT)
    runner = pp.BatchRunner(pipeline)
    results = pp.run_batch(runner, batch; force = FORCE_RUN)

    root_measure = sorted_table(pp.pipeline_measure_table(results))
    root_summary = sorted_table(pp.pipeline_summary_table(results))
    root_panel = sorted_table(pp.pipeline_panel_table(results))

    manifest_rows = NamedTuple[]
    decomp_tables = DataFrame[]

    for (batch_index, result) in enumerate(results.results)
        item = results.batch.items[batch_index]
        meta = merge(results.metadata[batch_index], (batch_index = batch_index,))
        scenario_root = joinpath(OUTPUT_ROOT, String(getproperty(meta, :scenario_dir)))
        mkpath(joinpath(scenario_root, "specs"))

        decomp = sorted_table(spec_decomposition_table(result, meta))
        push!(decomp_tables, decomp)

        row = manifest_row(result, meta)
        push!(manifest_rows, row)
        save_csv(row.decomposition_csv, decomp)
    end

    root_manifest = sorted_table(DataFrame(manifest_rows))
    root_decomp = isempty(decomp_tables) ? DataFrame() : sorted_table(vcat(decomp_tables...; cols = :union))

    save_csv(joinpath(OUTPUT_ROOT, "run_manifest.csv"), root_manifest)
    save_csv(joinpath(OUTPUT_ROOT, "measure_table.csv"), root_measure)
    save_csv(joinpath(OUTPUT_ROOT, "summary_table.csv"), root_summary)
    save_csv(joinpath(OUTPUT_ROOT, "panel_table.csv"), root_panel)
    save_csv(joinpath(OUTPUT_ROOT, "decomposition_table.csv"), root_decomp)

    write_scenario_tables!(root_measure, root_summary, root_panel, root_decomp, root_manifest, targets)

    println("Saved run manifest: ", joinpath(OUTPUT_ROOT, "run_manifest.csv"))
    println("Saved measure table: ", joinpath(OUTPUT_ROOT, "measure_table.csv"))
    println("Saved summary table: ", joinpath(OUTPUT_ROOT, "summary_table.csv"))
    println("Saved panel table: ", joinpath(OUTPUT_ROOT, "panel_table.csv"))
    println("Saved decomposition table: ", joinpath(OUTPUT_ROOT, "decomposition_table.csv"))
    println("Cache-backed results live under: ", CACHE_ROOT)
    return nothing
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end


main()

