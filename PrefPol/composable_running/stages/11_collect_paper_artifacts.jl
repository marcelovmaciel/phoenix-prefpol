#!/usr/bin/env julia

const PREFPOL_PROJECT_DIR = normpath(joinpath(@__DIR__, "..", ".."))
PREFPOL_PROJECT_DIR in LOAD_PATH || pushfirst!(LOAD_PATH, PREFPOL_PROJECT_DIR)

include(joinpath(@__DIR__, "04_measures.jl"))

using SHA

const DEFAULT_PAPER_ARTIFACTS_PATH = joinpath(DEFAULT_CONFIG_DIR, "paper_artifacts.toml")

function parse_collect_args(args)
    if any(arg -> arg in ("--help", "-h"), args)
        println("""
        Usage:
          julia +1.11.9 --project=PrefPol PrefPol/composable_running/stages/11_collect_paper_artifacts.jl [--config PATH] [--artifact-config PATH] [--artifact ID] [--force] [--dry-run]

        Phase 9:
          Collects paper-facing generated plots and TeX tables into the
          configured paper artifact directory. By default this reads
          PrefPol/config/paper_artifacts.toml and writes
          PrefPol/composable_running/output/paper_artifacts.
        """)
        exit(0)
    end

    opts = Dict{String,Any}(
        "config" => nothing,
        "artifact-config" => DEFAULT_PAPER_ARTIFACTS_PATH,
        "artifact" => nothing,
        "force" => false,
        "dry-run" => false,
    )

    i = 1
    while i <= length(args)
        arg = args[i]
        if arg in ("--force", "--dry-run")
            opts[arg[3:end]] = true
        elseif arg in ("--config", "--artifact-config", "--artifact")
            i == length(args) && error("$(arg) requires a value.")
            opts[arg[3:end]] = args[i + 1]
            i += 1
        else
            error("Unknown argument: $(arg)")
        end
        i += 1
    end

    return opts
end

function load_artifact_config(path::AbstractString)
    resolved = resolve_path(path)
    isfile(resolved) || error("Paper artifact config not found: $(resolved)")
    return TOML.parsefile(resolved)
end

function merge_collection_config(artifact_cfg, orchestration_cfg)
    collection = Dict{String,Any}()
    haskey(artifact_cfg, "collection") && merge!(collection, artifact_cfg["collection"])

    if haskey(orchestration_cfg, "paper_artifacts")
        override = orchestration_cfg["paper_artifacts"]
        haskey(override, "collection") && merge!(collection, override["collection"])
        for (key, value) in override
            key == "collection" && continue
            collection[key] = value
        end
    end

    return collection
end

function collection_settings(artifact_cfg, orchestration_cfg, opts)
    collection = merge_collection_config(artifact_cfg, orchestration_cfg)
    output_root = resolve_path(String(config_value(collection, "output_root", DEFAULT_OUTPUT_ROOT)))
    destination_root = resolve_path(String(config_value(
        collection,
        "destination_root",
        joinpath(output_root, "paper_artifacts"),
    )))
    manifests_root = resolve_path(String(config_value(
        collection,
        "manifests_root",
        joinpath(output_root, "manifests"),
    )))

    return (
        output_root = output_root,
        destination_root = destination_root,
        copy_mode = String(config_value(collection, "copy_mode", "copy")),
        update_writing_imgs = Bool(config_value(collection, "update_writing_imgs", false)),
        writing_imgs_root = resolve_path(String(config_value(collection, "writing_imgs_root", "writing/imgs"))),
        paper_artifact_manifest = resolve_path(String(config_value(
            collection,
            "paper_artifact_manifest",
            joinpath(manifests_root, "paper_artifact_manifest.csv"),
        ))),
        default_backend = String(config_value(collection, "default_backend", "mice")),
        default_linearizer = String(config_value(collection, "default_linearizer", "pattern_conditional")),
        require_all = Bool(config_value(collection, "require_all", true)),
        source_manifests = Dict(
            "plot_global" => resolve_path(String(config_value(collection, "plot_manifest", joinpath(manifests_root, "plot_manifest.csv")))),
            "plot_group" => resolve_path(String(config_value(collection, "group_plot_manifest", joinpath(manifests_root, "group_plot_manifest.csv")))),
            "extra_plots" => resolve_path(String(config_value(collection, "extra_plot_manifest", joinpath(manifests_root, "extra_plot_manifest.csv")))),
            "tables" => resolve_path(String(config_value(collection, "table_manifest", joinpath(manifests_root, "table_manifest.csv")))),
            "lambda_table" => resolve_path(String(config_value(collection, "lambda_table_manifest", joinpath(manifests_root, "lambda_table_manifest.csv")))),
        ),
        force = Bool(opts["force"]) || Bool(config_value(collection, "force", false)),
        dry_run = Bool(opts["dry-run"]) || Bool(config_value(collection, "dry_run", false)),
    )
end

function artifact_specs(artifact_cfg, opts)
    specs = get(artifact_cfg, "artifacts", Any[])
    isempty(specs) && error("No [[artifacts]] entries found in paper artifact config.")
    if opts["artifact"] !== nothing
        wanted = String(opts["artifact"])
        specs = [spec for spec in specs if String(config_value(spec, "id", "")) == wanted]
        isempty(specs) && error("No paper artifact with id=$(wanted).")
    end
    return specs
end

function read_stage_manifest(path::AbstractString)
    isfile(path) || error("Required source manifest not found: $(path)")
    df = CSV.read(path, DataFrame)
    isempty(df) && error("Source manifest is empty: $(path)")
    return df
end

function path_hash(path::AbstractString)
    isfile(path) || return ""
    return bytes2hex(SHA.sha256(read(path)))
end

function infer_wave_id(spec)
    for key in ("wave_id", "year")
        value = config_value(spec, key, nothing)
        value === nothing || return String(value)
    end
    m = match(r"(19|20)\d{2}", String(config_value(spec, "id", config_value(spec, "destination_filename", ""))))
    return m === nothing ? nothing : m.match
end

function infer_scenario_name(spec, wave_id)
    value = config_value(spec, "scenario_name", nothing)
    value === nothing || return String(value)
    wave_id === nothing && return nothing
    return occursin("main", String(config_value(spec, "id", ""))) ? "main_$(wave_id)" : nothing
end

function infer_artifact_prefix(spec)
    prefix = config_value(spec, "artifact_prefix", nothing)
    prefix === nothing || return String(prefix)
    stage = String(config_value(spec, "source_stage", ""))
    if stage == "plot_global"
        return "global_measures"
    elseif stage == "plot_group"
        return "paper_group_heatmap_panel"
    end
    return nothing
end

function infer_source_artifact_id(spec)
    value = config_value(spec, "source_artifact_id", nothing)
    value === nothing || return String(value)
    stage = String(config_value(spec, "source_stage", ""))
    if stage in ("extra_plots", "tables", "lambda_table")
        return splitext(String(config_value(spec, "destination_filename", "")))[1]
    end
    return nothing
end

function filter_if_column(rows::DataFrame, col::Symbol, value)
    value === nothing && return rows
    col in propertynames(rows) || return rows
    return rows[string.(rows[!, col]) .== String(value), :]
end

function matching_manifest_rows(manifest::DataFrame, spec, settings)
    stage = String(config_value(spec, "source_stage", ""))
    ext = lowercase(splitext(String(config_value(spec, "destination_filename", "")))[2])
    fmt = isempty(ext) ? "" : ext[2:end]
    rows = manifest

    :status in propertynames(rows) && (rows = rows[in.(string.(rows.status), Ref(Set(["success", "skipped"]))), :])
    :stage in propertynames(rows) && (rows = rows[string.(rows.stage) .== stage, :])
    :format in propertynames(rows) && !isempty(fmt) && (rows = rows[lowercase.(string.(rows.format)) .== fmt, :])

    rows = filter_if_column(rows, :wave_id, infer_wave_id(spec))
    rows = filter_if_column(rows, :scenario_name, infer_scenario_name(spec, infer_wave_id(spec)))
    rows = filter_if_column(rows, :imputer_backend, config_value(spec, "backend", settings.default_backend))
    rows = filter_if_column(rows, :linearizer_policy, config_value(spec, "linearizer", settings.default_linearizer))

    source_id = infer_source_artifact_id(spec)
    source_id === nothing || (rows = rows[string.(rows.artifact_id) .== source_id, :])

    prefix = infer_artifact_prefix(spec)
    if prefix !== nothing && :artifact_id in propertynames(rows)
        rows = rows[startswith.(string.(rows.artifact_id), prefix), :]
    end

    source_basename = config_value(spec, "source_basename", nothing)
    if source_basename !== nothing && :output_path in propertynames(rows)
        rows = rows[basename.(string.(rows.output_path)) .== String(source_basename), :]
    end

    :output_path in propertynames(rows) || error("Source manifest for $(stage) has no output_path column.")
    rows = rows[isfile.(string.(rows.output_path)), :]
    return rows
end

function selected_source_path(spec, settings)
    source_path = config_value(spec, "source_path", nothing)
    if source_path !== nothing
        resolved = resolve_path(String(source_path))
        isfile(resolved) || error("Configured source_path does not exist: $(resolved)")
        return resolved
    end

    stage = String(config_value(spec, "source_stage", ""))
    haskey(settings.source_manifests, stage) || error("Unsupported source_stage=$(stage).")
    manifest_path = resolve_path(String(config_value(spec, "source_manifest", settings.source_manifests[stage])))
    candidates = matching_manifest_rows(read_stage_manifest(manifest_path), spec, settings)

    if isempty(candidates)
        error("No generated source artifact matched id=$(config_value(spec, "id", "")) in $(manifest_path).")
    elseif nrow(candidates) > 1
        sort!(candidates, [:timestamp, :output_path])
    end

    return String(candidates[end, :output_path])
end

function ensure_destination_write!(src::AbstractString, dst::AbstractString, settings)
    mkpath(dirname(dst))
    if isfile(dst) || islink(dst)
        if settings.copy_mode == "copy" && isfile(dst) && path_hash(src) == path_hash(dst)
            return "unchanged"
        end
        settings.force || error("Destination already exists and differs; rerun with --force: $(dst)")
        rm(dst; force = true)
    end

    if settings.copy_mode == "copy"
        cp(src, dst; force = true)
    elseif settings.copy_mode == "symlink"
        symlink(src, dst)
    else
        error("Unsupported paper artifact copy_mode=$(settings.copy_mode). Use copy or symlink.")
    end
    return "success"
end

function collect_one(spec, settings)
    artifact_id = String(config_value(spec, "id", ""))
    isempty(artifact_id) && error("Every paper artifact spec needs an id.")
    destination_filename = String(config_value(spec, "destination_filename", ""))
    isempty(destination_filename) && error("Artifact $(artifact_id) needs destination_filename.")

    src = selected_source_path(spec, settings)
    dst = joinpath(settings.destination_root, destination_filename)
    writing_dst = settings.update_writing_imgs ? joinpath(settings.writing_imgs_root, destination_filename) : ""

    status = ensure_destination_write!(src, dst, settings)
    if settings.update_writing_imgs
        ensure_destination_write!(src, writing_dst, settings)
    end

    return (
        stage = "paper_artifact_collection",
        artifact_id = artifact_id,
        source_stage = String(config_value(spec, "source_stage", "")),
        source_path = src,
        destination_path = dst,
        writing_imgs_path = writing_dst,
        copy_mode = settings.copy_mode,
        format = lowercase(splitext(destination_filename)[2][2:end]),
        source_hash = path_hash(src),
        destination_hash = path_hash(dst),
        status = status,
        error = "",
        timestamp = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS"),
    )
end

function main(args = ARGS)
    opts = parse_collect_args(args)
    artifact_cfg = load_artifact_config(String(opts["artifact-config"]))
    orchestration_cfg = load_orchestration_config(opts["config"])
    settings = collection_settings(artifact_cfg, orchestration_cfg, opts)
    specs = artifact_specs(artifact_cfg, opts)

    println("Paper artifact collection plan:")
    println("  destination_root=", settings.destination_root)
    println("  copy_mode=", settings.copy_mode)
    println("  update_writing_imgs=", settings.update_writing_imgs)
    println("  manifest=", settings.paper_artifact_manifest)
    println("  force=", settings.force, " dry_run=", settings.dry_run)
    for spec in specs
        println("  ", config_value(spec, "id", ""), " -> ", config_value(spec, "destination_filename", ""))
    end
    settings.dry_run && return nothing

    rows = NamedTuple[]
    errors = String[]
    for spec in specs
        try
            push!(rows, collect_one(spec, settings))
        catch err
            message = sprint(showerror, err)
            push!(errors, message)
            push!(rows, (
                stage = "paper_artifact_collection",
                artifact_id = String(config_value(spec, "id", "")),
                source_stage = String(config_value(spec, "source_stage", "")),
                source_path = "",
                destination_path = joinpath(settings.destination_root, String(config_value(spec, "destination_filename", ""))),
                writing_imgs_path = "",
                copy_mode = settings.copy_mode,
                format = lowercase(splitext(String(config_value(spec, "destination_filename", "")))[2][2:end]),
                source_hash = "",
                destination_hash = "",
                status = "error",
                error = message,
                timestamp = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS"),
            ))
        end
    end

    write_csv(settings.paper_artifact_manifest, DataFrame(rows))
    isempty(errors) || settings.require_all && error(join(errors, "\n"))
    println("Wrote paper artifacts under ", settings.destination_root)
    println("Wrote paper artifact manifest to ", settings.paper_artifact_manifest)
    return nothing
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
