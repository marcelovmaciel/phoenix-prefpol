#!/usr/bin/env julia

include(joinpath(@__DIR__, "..", "stage_common.jl"))

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
            result_path = portable_path(joinpath(result.cache_dir, "result.jld2")),
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
                input_path = portable_path(get(linearized_paths, key, "")),
                output_path = portable_path(get(measure_paths, key, "")),
                status = "success",
                error = "",
                timestamp = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS"),
            )))
        end
    end

    return DataFrame(rows)
end

function decomposition_table(result::pp.PipelineResult, meta::NamedTuple)
    df = pp.decomposition_table(result.decomposition)
    decorate!(df, spec_metadata(result, meta))
    return df
end

function run_manifest(result::pp.PipelineResult, meta::NamedTuple)
    return DataFrame([merge(spec_metadata(result, meta), (
        result_path = portable_path(joinpath(result.cache_dir, "result.jld2")),
        stage_manifest_rows = nrow(result.stage_manifest),
        measure_rows = nrow(result.measure_cube),
        status = "success",
        timestamp = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS"),
    ))])
end

function measure_manifest(result::pp.PipelineResult, meta::NamedTuple)
    rows = NamedTuple[]
    full_meta = spec_metadata(result, meta)
    measure_paths = Dict{Tuple{Int,Int,Int},String}()
    linearized_paths = Dict{Tuple{Int,Int,Int},String}()

    for row in eachrow(result.stage_manifest)
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
        push!(rows, merge(full_meta, (
            stage = "measure",
            b = key[1],
            r = key[2],
            k = key[3],
            measure_id = String(row.measure),
            grouping = ismissing(row.grouping) ? "" : String(row.grouping),
            input_path = portable_path(get(linearized_paths, key, "")),
            output_path = portable_path(get(measure_paths, key, "")),
            status = "success",
            error = "",
            timestamp = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS"),
        )))
    end

    return DataFrame(rows)
end

function run_measure_batch(pipeline::pp.NestedStochasticPipeline,
                           batch::pp.StudyBatchSpec;
                           force::Bool = false,
                           progress::Bool = true)
    results = pp.PipelineResult[]
    metadata = NamedTuple[]
    for item in batch.items
        push!(results, pp.ensure_measures!(pipeline, item.spec; force = force, progress = progress))
        push!(metadata, item.metadata)
    end
    return pp.BatchRunResult(batch, results, metadata)
end

function write_measure_outputs_streaming(pipeline::pp.NestedStochasticPipeline,
                                         batch::pp.StudyBatchSpec,
                                         settings;
                                         progress::Bool = true)
    measure_dir = joinpath(settings.output_root, "measures")
    manifest_dir = joinpath(settings.output_root, "manifests")

    paths = (
        measure_table = joinpath(measure_dir, "measure_table.csv"),
        summary_table = joinpath(measure_dir, "summary_table.csv"),
        panel_table = joinpath(measure_dir, "panel_table.csv"),
        decomposition_table = joinpath(measure_dir, "decomposition_table.csv"),
        run_manifest = joinpath(manifest_dir, "run_manifest.csv"),
        measure_manifest = joinpath(manifest_dir, "measure_manifest.csv"),
    )
    reset_output_files!(String[getproperty(paths, name) for name in propertynames(paths)])

    wrote = Dict(name => false for name in propertynames(paths))

    for (idx, item) in enumerate(batch.items)
        println(
            "  assembling [", idx, "/", length(batch.items), "] wave=", item.spec.wave_id,
            " scenario=", item.metadata.scenario_name,
            " m=", item.metadata.m,
            " backend=", item.spec.imputer_backend,
            " linearizer=", item.spec.linearizer_policy,
        )

        result = pp.ensure_measures!(pipeline, item.spec; force = settings.force, progress = progress)
        meta = merge(item.metadata, (batch_index = idx,))

        for (name, table) in (
            :measure_table => pp.pipeline_measure_table(result, meta),
            :summary_table => pp.pipeline_summary_table(result, meta),
            :panel_table => pp.pipeline_panel_table(result, meta),
            :decomposition_table => decomposition_table(result, meta),
            :run_manifest => run_manifest(result, meta),
            :measure_manifest => measure_manifest(result, meta),
        )
            append_csv(getproperty(paths, name), sorted_table(table); writeheader = !wrote[name])
            wrote[name] = true
        end

        result = nothing
        GC.gc()
    end

    return paths
end

function main(args = ARGS)
    opts = parse_args(args)
    cfg = load_orchestration_config(opts["config"])
    settings = run_settings(cfg, opts)
    targets = selected_targets(cfg, opts)
    waves, registry, wave_by_id = load_waves()
    batch = build_batch(targets, wave_by_id, settings)

    print_plan(batch, settings; stage_name = "Measure")
    settings.dry_run && return nothing

    mkpath(settings.cache_root)
    mkpath(joinpath(settings.output_root, "measures"))
    mkpath(joinpath(settings.output_root, "manifests"))

    pipeline = pp.NestedStochasticPipeline(registry; cache_root = settings.cache_root)
    write_measure_outputs_streaming(pipeline, batch, settings)

    println("Wrote measure outputs under ", joinpath(settings.output_root, "measures"))
    println("Wrote manifests under ", joinpath(settings.output_root, "manifests"))
    return nothing
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
