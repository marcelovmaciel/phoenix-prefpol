#!/usr/bin/env julia

const PREFPOL_PROJECT_DIR = normpath(joinpath(@__DIR__, "..", ".."))
PREFPOL_PROJECT_DIR in LOAD_PATH || pushfirst!(LOAD_PATH, PREFPOL_PROJECT_DIR)

include(joinpath(@__DIR__, "04_measures.jl"))

using Printf
using SHA
using Statistics

const DEFAULT_TABLE_SPECS_PATH = joinpath(DEFAULT_CONFIG_DIR, "table_specs.toml")

function parse_lambda_args(args)
    if any(arg -> arg in ("--help", "-h"), args)
        println("""
        Usage:
          julia +1.11.9 --project=PrefPol PrefPol/composable_running/stages/10_lambda_table.jl [--config PATH] [--year YEAR] [--scenario NAME] [--m VALUE_OR_RANGE] [--backend NAME] [--linearizer NAME] [--force] [--dry-run] [--smoke-test]

        Phase 8:
          Builds appendix Lambda CSV/audit/TeX tables from cached PipelineResult
          measure cubes. Lambda is the aggregate D / W ratio.
        """)
        exit(0)
    end
    return parse_args(args)
end

function load_table_specs_config()
    isfile(DEFAULT_TABLE_SPECS_PATH) || error("Table specs not found: $(DEFAULT_TABLE_SPECS_PATH)")
    return TOML.parsefile(DEFAULT_TABLE_SPECS_PATH)
end

function lambda_settings(cfg, opts)
    specs = load_table_specs_config()
    run_cfg = get(cfg, "run", Dict{String,Any}())
    outputs = get(specs, "outputs", Dict{String,Any}())
    lambda_cfg = merge(get(specs, "lambda_table", Dict{String,Any}()), get(cfg, "lambda_table", Dict{String,Any}()))
    output_root = resolve_path(String(config_value(outputs, "output_root",
                                 config_value(run_cfg, "output_root", DEFAULT_OUTPUT_ROOT))))
    m_values = parse_m_values(opts["m"])
    m_values === nothing && (m_values = Int.(config_value(lambda_cfg, "m_values", [2, 3, 4, 5])))

    targets = NamedTuple[]
    for target in get(lambda_cfg, "targets", Any[])
        wave_id = string(config_value(target, "wave_id", config_value(target, "year", "")))
        scenario_name = string(config_value(target, "scenario_name", ""))
        groupings = String.(collect(config_value(target, "groupings", String[])))
        push!(targets, (wave_id = wave_id, year = parse(Int, wave_id), scenario_name = scenario_name, groupings = groupings))
    end

    return (
        output_root = output_root,
        run_manifest = resolve_path(String(config_value(lambda_cfg, "run_manifest",
                               joinpath(output_root, "manifests", "run_manifest.csv")))),
        appendix_dir = resolve_path(String(config_value(lambda_cfg, "appendix_dir",
                               joinpath(output_root, "appendices", "lambda")))),
        appendix_manifest = resolve_path(String(config_value(lambda_cfg, "appendix_manifest",
                                   joinpath(output_root, "manifests", "lambda_table_manifest.csv")))),
        targets = targets,
        analysis_role = String(config_value(lambda_cfg, "analysis_role", "main")),
        imputer_backend = opts["backend"] === nothing ? String(config_value(lambda_cfg, "backend", "mice")) : String(opts["backend"]),
        linearizer_policy = opts["linearizer"] === nothing ? String(config_value(lambda_cfg, "linearizer", "pattern_conditional")) : String(opts["linearizer"]),
        m_values = Set(Int.(m_values)),
        year_filter = opts["year"] === nothing ? nothing : String(opts["year"]),
        scenario_filter = opts["scenario"] === nothing ? nothing : String(opts["scenario"]),
        force = Bool(opts["force"]) || Bool(config_value(lambda_cfg, "force", false)),
        dry_run = Bool(opts["dry-run"]) || Bool(config_value(lambda_cfg, "dry_run", false)),
    )
end

function selected_lambda_rows(manifest::DataFrame, settings)
    target_keys = Set((target.wave_id, target.scenario_name) for target in settings.targets)
    rows = manifest[
        [((string(row.wave_id), string(row.scenario_name)) in target_keys) for row in eachrow(manifest)] .&
        (string.(manifest.analysis_role) .== settings.analysis_role) .&
        (string.(manifest.imputer_backend) .== settings.imputer_backend) .&
        (string.(manifest.linearizer_policy) .== settings.linearizer_policy) .&
        in.(Int.(manifest.m), Ref(settings.m_values)),
        :,
    ]
    settings.year_filter !== nothing && (rows = rows[string.(rows.wave_id) .== settings.year_filter, :])
    settings.scenario_filter !== nothing && (rows = rows[string.(rows.scenario_name) .== settings.scenario_filter, :])
    isempty(rows) && error("No manifest rows matched the appendix Lambda target specs.")
    return sort(rows, [:year, :scenario_name, :m])
end

function parse_manifest_columns!(manifest::DataFrame)
    for col in (:year, :m, :B, :R, :K, :n_candidates, :batch_index)
        col in propertynames(manifest) || continue
        manifest[!, col] = Int.(manifest[!, col])
    end
    return manifest
end

function read_manifest(path::AbstractString)
    isfile(path) || error("Required manifest not found: $(path)")
    df = CSV.read(path, DataFrame)
    isempty(df) && error("Manifest is empty: $(path)")
    return df
end

manifest_hash(path::AbstractString) = bytes2hex(SHA.sha256(read(path)))

function lambda_augmented_result(path::AbstractString)
    result = pp.load_pipeline_result(path)
    measures = Symbol.(result.measure_cube.measure)
    any(measures .== :lambda_sep) && return result
    return pp.augment_pipeline_result_with_lambda_sep(result; include_w = true)
end

function grouped_measure_values(result::pp.PipelineResult, measure::Symbol)
    cube = result.measure_cube
    rows = cube[(Symbol.(cube.measure) .== measure) .& .!ismissing.(cube.grouping), :]
    isempty(rows) && error("Result $(result.cache_dir) has no grouped $(measure) rows.")
    return rows
end

function summarize_grouped_measure(result::pp.PipelineResult, measure::Symbol, value_name::Symbol)
    rows = grouped_measure_values(result, measure)
    out = combine(groupby(rows, :grouping), :value => median => value_name)
    rename!(out, :grouping => :Grouping)
    return out
end

function summarize_grouped_w(result::pp.PipelineResult)
    if any(Symbol.(result.measure_cube.measure) .== :W)
        return summarize_grouped_measure(result, :W, :W)
    end
    c_rows = grouped_measure_values(result, :C)
    tmp = transform(c_rows, :value => ByRow(value -> (1.0 - Float64(value)) / 2.0) => :W_value)
    out = combine(groupby(tmp, :grouping), :W_value => median => :W)
    rename!(out, :grouping => :Grouping)
    return out
end

function summarize_lambda_result(row)
    result = lambda_augmented_result(string(row.result_path))
    lambda = summarize_grouped_measure(result, :lambda_sep, :Lambda)
    W = summarize_grouped_w(result)
    D = summarize_grouped_measure(result, :D, :D)
    S = any(Symbol.(result.measure_cube.measure) .== :S) ?
        summarize_grouped_measure(result, :S, :S) :
        transform(innerjoin(W, D; on = :Grouping), [:D, :W] => ByRow(-) => :S)[:, [:Grouping, :S]]

    audit = innerjoin(innerjoin(innerjoin(lambda, W; on = :Grouping), D; on = :Grouping), S; on = :Grouping)
    audit[!, :Year] = fill(Int(row.year), nrow(audit))
    audit[!, :Scenario] = fill(string(row.scenario_name), nrow(audit))
    audit[!, :m] = fill(Int(row.m), nrow(audit))
    audit[!, :source_result_path] = fill(string(row.result_path), nrow(audit))
    select!(audit, :Year, :Scenario, :m, :Grouping, :Lambda, :W, :D, :S, :source_result_path)
    return select(audit, :Year, :Scenario, :m, :Grouping, :Lambda), audit
end

function format_lambda(value)
    ismissing(value) && return "NA"
    x = Float64(value)
    isnan(x) && return "NA"
    isinf(x) && return x > 0 ? "Inf" : "-Inf"
    return @sprintf("%.3f", x)
end

function latex_escape(value)
    return replace(string(value), "\\" => "\\textbackslash{}", "_" => "\\_", "%" => "\\%", "&" => "\\&", "#" => "\\#")
end

function write_latex_table(path::AbstractString, table::DataFrame)
    open(path, "w") do io
        println(io, "\\begin{table}[!htbp]")
        println(io, "\\centering")
        println(io, "\\begin{tabular}{lllrl}")
        println(io, "\\hline")
        println(io, "Year & Scenario & m & Grouping & \$\\Lambda\$ \\\\")
        println(io, "\\hline")
        for row in eachrow(table)
            println(io, join((latex_escape(row.Year), latex_escape(row.Scenario), latex_escape(row.m),
                              latex_escape(row.Grouping), format_lambda(row.Lambda)), " & "), " \\\\")
        end
        println(io, "\\hline")
        println(io, "\\end{tabular}")
        println(io, "\\caption{Aggregate separation ratio by wave, scenario, candidate-set size, and grouping.}")
        println(io, "\\begin{flushleft}\\footnotesize")
        println(io, "\$\\Lambda = (\\sum_g \\pi_g D_g)/(\\sum_g \\pi_g W_g)\$. Values above 1 indicate excess separation.")
        println(io, "\\end{flushleft}")
        println(io, "\\end{table}")
    end
    return path
end

function grouping_order(settings, year, scenario)
    for target in settings.targets
        target.year == Int(year) && target.scenario_name == string(scenario) && return target.groupings
    end
    return sort(unique(String[]))
end

function lambda_lookup(rows::DataFrame)
    return Dict((Int(row.m), string(row.Grouping)) => Float64(row.Lambda) for row in eachrow(rows))
end

function write_grouping_tables(path::AbstractString, table::DataFrame, settings)
    open(path, "w") do io
        println(io, "% Generated by PrefPol/composable_running/stages/10_lambda_table.jl")
        println(io)
        for target in settings.targets
            rows = table[(Int.(table.Year) .== target.year) .& (string.(table.Scenario) .== target.scenario_name), :]
            isempty(rows) && continue
            groupings = isempty(target.groupings) ? sort(unique(String.(rows.Grouping))) : target.groupings
            ms = sort(unique(Int.(rows.m)))
            values = lambda_lookup(rows)
            println(io, "\\begin{table}[!htbp]")
            println(io, "\\centering")
            println(io, "\\caption{Aggregate separation ratio \$\\Lambda\$ for $(latex_escape(target.year)) / $(latex_escape(target.scenario_name)).}")
            println(io, "\\begin{tabular}{", repeat("r", length(groupings) + 1), "}")
            println(io, "\\hline")
            println(io, join(["\$m\$"; latex_escape.(groupings)], " & "), " \\\\")
            println(io, "\\hline")
            for m in ms
                row_values = [haskey(values, (m, grouping)) ? format_lambda(values[(m, grouping)]) : "NA" for grouping in groupings]
                println(io, join([string(m); row_values], " & "), " \\\\")
            end
            println(io, "\\hline")
            println(io, "\\end{tabular}")
            println(io, "\\end{table}")
            println(io)
        end
    end
    return path
end

function manifest_rows(paths, source)
    source_hash = isfile(source) ? manifest_hash(source) : ""
    return DataFrame([(
        stage = "lambda_table",
        artifact_id = splitext(basename(path))[1],
        input_path = source,
        output_path = path,
        format = lowercase(splitext(path)[2][2:end]),
        source_manifest_hash = source_hash,
        status = "success",
        error = "",
        timestamp = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS"),
    ) for path in paths])
end

function main(args = ARGS)
    opts = parse_lambda_args(args)
    cfg = load_orchestration_config(opts["config"])
    settings = lambda_settings(cfg, opts)

    println("Lambda table stage plan:")
    println("  run_manifest=", settings.run_manifest)
    println("  appendix_dir=", settings.appendix_dir)
    println("  backend=", settings.imputer_backend, " linearizer=", settings.linearizer_policy)
    println("  force=", settings.force, " dry_run=", settings.dry_run)
    settings.dry_run && return nothing

    manifest = parse_manifest_columns!(read_manifest(settings.run_manifest))
    rows = selected_lambda_rows(manifest, settings)
    tables = DataFrame[]
    audits = DataFrame[]
    for row in eachrow(rows)
        table, audit = summarize_lambda_result(row)
        push!(tables, table)
        push!(audits, audit)
    end

    table = sort(vcat(tables...; cols = :union), [:Year, :Scenario, :m, :Grouping])
    audit = sort(vcat(audits...; cols = :union), [:Year, :Scenario, :m, :Grouping])
    table_for_csv = transform(table, :Lambda => ByRow(format_lambda) => :Lambda_display)

    mkpath(settings.appendix_dir)
    paths = [
        joinpath(settings.appendix_dir, "appendix_lambda_table.csv"),
        joinpath(settings.appendix_dir, "appendix_lambda_audit.csv"),
        joinpath(settings.appendix_dir, "appendix_lambda_table.tex"),
        joinpath(settings.appendix_dir, "appendix_lambda_grouping_tables.tex"),
    ]
    CSV.write(paths[1], table_for_csv)
    CSV.write(paths[2], audit)
    write_latex_table(paths[3], table)
    write_grouping_tables(paths[4], table, settings)
    write_csv(settings.appendix_manifest, manifest_rows(paths, settings.run_manifest))

    println("Wrote Lambda outputs under ", settings.appendix_dir)
    println("Wrote Lambda manifest to ", settings.appendix_manifest)
    return nothing
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
