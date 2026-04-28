#!/usr/bin/env julia

include(joinpath(@__DIR__, "04_measures.jl"))

function main(args = ARGS)
    if any(arg -> arg in ("--help", "-h"), args)
        println("""
        Usage:
          julia --project=PrefPol PrefPol/composable_running/stages/03_linearize.jl [--config PATH] [--year YEAR] [--scenario NAME] [--m VALUE_OR_RANGE] [--backend NAME] [--linearizer NAME] [--force] [--dry-run] [--smoke-test]

        Phase 5:
          Builds PipelineSpec jobs and calls PrefPol.ensure_linearizations!
          without running measurement.
        """)
        return nothing
    end

    opts = parse_args(args)
    cfg = load_orchestration_config(opts["config"])
    settings = run_settings(cfg, opts)
    targets = selected_targets(cfg, opts)
    waves, registry, wave_by_id = load_waves()
    batch = build_batch(targets, wave_by_id, settings)

    print_plan(batch, settings; stage_name = "Linearization")
    settings.dry_run && return nothing

    mkpath(settings.cache_root)
    mkpath(joinpath(settings.output_root, "linearizations"))
    mkpath(joinpath(settings.output_root, "manifests"))

    pipeline = pp.NestedStochasticPipeline(registry; cache_root = settings.cache_root)
    manifests = run_stage_manifests(pipeline, batch, :linearized; force = settings.force)
    manifest = sorted_table(stage_manifest(manifests, batch, pipeline))

    write_csv(joinpath(settings.output_root, "linearizations", "linearization_manifest.csv"), manifest)
    write_csv(joinpath(settings.output_root, "manifests", "linearization_manifest.csv"), manifest)

    println("Wrote linearization manifests under ", settings.output_root)
    return nothing
end

main()
