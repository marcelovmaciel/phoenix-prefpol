#!/usr/bin/env julia

include(joinpath(@__DIR__, "04_measures.jl"))

function main(args = ARGS)
    if any(arg -> arg in ("--help", "-h"), args)
        println("""
        Usage:
          julia --project=PrefPol PrefPol/composable_running/stages/01_bootstrap.jl [--config PATH] [--year YEAR] [--scenario NAME] [--m VALUE_OR_RANGE] [--backend NAME] [--linearizer NAME] [--force] [--dry-run] [--smoke-test]

        Phase 5:
          Builds PipelineSpec jobs and calls PrefPol.ensure_resamples! without
          running imputation, linearization, or measurement.
        """)
        return nothing
    end

    opts = parse_args(args)
    cfg = load_orchestration_config(opts["config"])
    settings = run_settings(cfg, opts)
    targets = selected_targets(cfg, opts)
    waves, registry, wave_by_id = load_waves()
    batch = build_batch(targets, wave_by_id, settings)

    print_plan(batch, settings; stage_name = "Bootstrap")
    settings.dry_run && return nothing

    mkpath(settings.cache_root)
    mkpath(joinpath(settings.output_root, "bootstraps"))
    mkpath(joinpath(settings.output_root, "manifests"))

    pipeline = pp.NestedStochasticPipeline(registry; cache_root = settings.cache_root)
    manifests = run_stage_manifests(pipeline, batch, :resample; force = settings.force)
    manifest = sorted_table(stage_manifest(manifests, batch, pipeline))

    write_csv(joinpath(settings.output_root, "bootstraps", "bootstrap_manifest.csv"), manifest)
    write_csv(joinpath(settings.output_root, "manifests", "bootstrap_manifest.csv"), manifest)

    println("Wrote bootstrap manifests under ", settings.output_root)
    return nothing
end

main()
