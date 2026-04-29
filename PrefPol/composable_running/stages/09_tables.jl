#!/usr/bin/env julia

const PREFPOL_PROJECT_DIR = normpath(joinpath(@__DIR__, "..", ".."))
PREFPOL_PROJECT_DIR in LOAD_PATH || pushfirst!(LOAD_PATH, PREFPOL_PROJECT_DIR)

include(joinpath(@__DIR__, "04_measures.jl"))

using Printf
using SHA

const DEFAULT_TABLE_SPECS_PATH = joinpath(DEFAULT_CONFIG_DIR, "table_specs.toml")
const EFFECTIVE_OUTPUT_COLS = [
    :year, :scenario_name, :imputer_backend, :linearizer_policy,
    :analysis_role, :B, :R, :K, :m,
    :n_rankings_observed_mean, :EO_median,
    :n_reversal_pairs_observed_mean, :ENRP_median,
    :max_rankings_possible, :max_reversal_pairs_possible,
]

function parse_table_args(args)
    if any(arg -> arg in ("--help", "-h"), args)
        println("""
        Usage:
          julia +1.11.9 --project=PrefPol PrefPol/composable_running/stages/09_tables.jl [--config PATH] [--year YEAR] [--scenario NAME] [--m VALUE_OR_RANGE] [--backend NAME] [--linearizer NAME] [--force] [--dry-run] [--smoke-test]

        Phase 8:
          Builds effective-ranking CSV, Markdown, and TeX tables from
          output/extra_measures/effective_counts/effective_counts_summary.csv.
        """)
        exit(0)
    end
    return parse_args(args)
end

function load_table_specs_config()
    isfile(DEFAULT_TABLE_SPECS_PATH) || error("Table specs not found: $(DEFAULT_TABLE_SPECS_PATH)")
    return TOML.parsefile(DEFAULT_TABLE_SPECS_PATH)
end

function merged_table_section(cfg, name::AbstractString)
    specs = load_table_specs_config()
    section = Dict{String,Any}()
    haskey(specs, "outputs") && merge!(section, specs["outputs"])
    haskey(specs, name) && merge!(section, specs[name])
    haskey(cfg, name) && merge!(section, cfg[name])
    return section
end

function configured_table_targets(table_cfg)
    raw_targets = get(table_cfg, "targets", Any[])
    if !isempty(raw_targets)
        return [(year = Int(config_value(target, "year", config_value(target, "wave_id", 0))),
                 wave_id = string(config_value(target, "wave_id", config_value(target, "year", ""))),
                 scenario_name = string(config_value(target, "scenario_name", "")))
                for target in raw_targets]
    end
    years = Int.(config_value(table_cfg, "years", [2006, 2018, 2022]))
    scenarios = get(table_cfg, "scenario_by_year", Dict{String,Any}())
    return [(year = year,
             wave_id = string(year),
             scenario_name = string(get(scenarios, string(year), "main_$(year)")))
            for year in years]
end

function table_settings(cfg, opts)
    run_cfg = get(cfg, "run", Dict{String,Any}())
    table_cfg = merged_table_section(cfg, "effective_rankings")
    output_root = resolve_path(String(config_value(table_cfg, "output_root",
                                 config_value(run_cfg, "output_root", DEFAULT_OUTPUT_ROOT))))
    m_values = parse_m_values(opts["m"])
    m_values === nothing && (m_values = Int.(config_value(table_cfg, "m_values", [2, 3, 4, 5])))

    return (
        output_root = output_root,
        table_root = resolve_path(String(config_value(table_cfg, "table_root",
                             joinpath(output_root, "tables")))),
        source = resolve_path(String(config_value(table_cfg, "source",
                        joinpath(output_root, "extra_measures", "effective_counts", "effective_counts_summary.csv")))),
        table_manifest = resolve_path(String(config_value(table_cfg, "table_manifest",
                                joinpath(output_root, "manifests", "table_manifest.csv")))),
        targets = configured_table_targets(table_cfg),
        analysis_role = String(config_value(table_cfg, "analysis_role", "main")),
        imputer_backend = opts["backend"] === nothing ? String(config_value(table_cfg, "backend", "mice")) : String(opts["backend"]),
        linearizer_policy = opts["linearizer"] === nothing ? String(config_value(table_cfg, "linearizer", "pattern_conditional")) : String(opts["linearizer"]),
        m_values = Set(Int.(m_values)),
        year_filter = opts["year"] === nothing ? nothing : String(opts["year"]),
        scenario_filter = opts["scenario"] === nothing ? nothing : String(opts["scenario"]),
        force = Bool(opts["force"]) || Bool(config_value(table_cfg, "force", false)),
        dry_run = Bool(opts["dry-run"]) || Bool(config_value(table_cfg, "dry_run", false)),
    )
end

function normalize_effective_columns!(df::DataFrame)
    if !(:ENRP_median in propertynames(df)) && :ER_median in propertynames(df)
        rename!(df, :ER_median => :ENRP_median)
    end
    return df
end

manifest_hash(path::AbstractString) = bytes2hex(SHA.sha256(read(path)))

function select_effective_rows(df::DataFrame, target, settings)
    rows = df[
        (Int.(df.year) .== target.year) .&
        (string.(df.scenario_name) .== target.scenario_name) .&
        (string.(df.analysis_role) .== settings.analysis_role) .&
        (string.(df.imputer_backend) .== settings.imputer_backend) .&
        (string.(df.linearizer_policy) .== settings.linearizer_policy) .&
        in.(Int.(df.m), Ref(settings.m_values)),
        :,
    ]
    isempty(rows) && error("No effective-count rows found for $(target.year) / $(target.scenario_name).")
    missing_m = setdiff(sort(collect(settings.m_values)), sort(unique(Int.(rows.m))))
    isempty(missing_m) || error("Missing m values $(missing_m) for $(target.year) / $(target.scenario_name).")
    missing_cols = setdiff(EFFECTIVE_OUTPUT_COLS, propertynames(rows))
    isempty(missing_cols) || error("Effective-count source is missing required columns $(missing_cols).")
    return sort(select(rows, EFFECTIVE_OUTPUT_COLS), :m)
end

fmt_num(value; digits::Int = 2) = ismissing(value) ? "" : @sprintf("%.*f", digits, Float64(value))
fmt_int(value) = ismissing(value) ? "" : string(Int(round(Float64(value))))

function latex_escape(value)
    escaped = replace(String(value), "\\" => "\\textbackslash{}")
    escaped = replace(escaped, "&" => "\\&")
    escaped = replace(escaped, "%" => "\\%")
    escaped = replace(escaped, "\$" => "\\\$")
    escaped = replace(escaped, "#" => "\\#")
    escaped = replace(escaped, "_" => "\\_")
    escaped = replace(escaped, "{" => "\\{")
    escaped = replace(escaped, "}" => "\\}")
    escaped = replace(escaped, "~" => "\\textasciitilde{}")
    escaped = replace(escaped, "^" => "\\textasciicircum{}")
    return escaped
end

function table_row_by_m(table::DataFrame, m::Integer)
    rows = table[Int.(table.m) .== Int(m), :]
    nrow(rows) == 1 || error("Expected exactly one row for m=$(m), found $(nrow(rows)).")
    return rows[1, :]
end

function presentation_rows(per_year_tables::Dict{Int,DataFrame}; paper_only::Bool)
    years = sort(collect(keys(per_year_tables)))
    headers = ["m"]
    for year in years
        append!(headers, paper_only ? ["EO $(year)", "ENRP $(year)"] :
                ["rankings $(year)", "EO $(year)", "reversal pairs $(year)", "ENRP $(year)"])
    end

    rows = Vector{String}[]
    for m in sort(unique(vcat([Int.(table.m) for table in values(per_year_tables)]...)))
        row_values = [string(m)]
        for year in years
            row = table_row_by_m(per_year_tables[year], m)
            if paper_only
                append!(row_values, [fmt_num(row.EO_median), fmt_num(row.ENRP_median)])
            else
                append!(row_values, [
                    fmt_num(row.n_rankings_observed_mean),
                    fmt_num(row.EO_median),
                    fmt_num(row.n_reversal_pairs_observed_mean),
                    fmt_num(row.ENRP_median),
                ])
            end
        end
        push!(rows, row_values)
    end
    return headers, rows
end

function write_markdown_matrix(path, title, headers, rows)
    open(path, "w") do io
        println(io, "# ", title)
        println(io)
        println(io, "| ", join(headers, " | "), " |")
        println(io, "|", join(fill("---:", length(headers)), "|"), "|")
        for row in rows
            println(io, "| ", join(row, " | "), " |")
        end
    end
    return path
end

function write_latex_tabular(path, caption, label, headers, rows)
    open(path, "w") do io
        println(io, "\\begin{table}[!htbp]")
        println(io, "\\centering")
        println(io, "\\caption{", latex_escape(caption), "}")
        println(io, "\\label{", latex_escape(label), "}")
        println(io, "\\begin{tabular}{", repeat("r", length(headers)), "}")
        println(io, "\\hline")
        println(io, join(latex_escape.(headers), " & "), " \\\\")
        println(io, "\\hline")
        for row in rows
            println(io, join(latex_escape.(row), " & "), " \\\\")
        end
        println(io, "\\hline")
        println(io, "\\end{tabular}")
        println(io, "\\end{table}")
    end
    return path
end

function manifest_rows(paths, source)
    source_hash = isfile(source) ? manifest_hash(source) : ""
    return DataFrame([(
        stage = "tables",
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
    opts = parse_table_args(args)
    cfg = load_orchestration_config(opts["config"])
    settings = table_settings(cfg, opts)
    targets = settings.targets
    settings.year_filter !== nothing && (targets = [target for target in targets if target.wave_id == settings.year_filter || string(target.year) == settings.year_filter])
    settings.scenario_filter !== nothing && (targets = [target for target in targets if target.scenario_name == settings.scenario_filter])
    isempty(targets) && error("No table targets selected.")

    println("Table stage plan:")
    println("  source=", settings.source)
    println("  table_root=", settings.table_root)
    println("  backend=", settings.imputer_backend, " linearizer=", settings.linearizer_policy)
    println("  force=", settings.force, " dry_run=", settings.dry_run)
    settings.dry_run && return nothing

    isfile(settings.source) || error("Effective-count summary not found: $(settings.source). Run 07_extra_measures first.")
    input = normalize_effective_columns!(CSV.read(settings.source, DataFrame))
    out_dir = joinpath(settings.table_root, "effective_rankings")
    mkpath(out_dir)
    per_year = Dict{Int,DataFrame}()
    paths = String[]

    for target in targets
        table = select_effective_rows(input, target, settings)
        per_year[target.year] = table
        csv_path = joinpath(out_dir, "effective_rankings_evolution_$(target.year)_$(target.scenario_name)__$(settings.imputer_backend)__$(settings.linearizer_policy)__$(settings.analysis_role).csv")
        CSV.write(csv_path, table)
        push!(paths, csv_path)
    end

    full_headers, full_rows = presentation_rows(per_year; paper_only = false)
    paper_headers, paper_rows = presentation_rows(per_year; paper_only = true)
    full_md = joinpath(out_dir, "effective_rankings_full.md")
    full_tex = joinpath(out_dir, "effective_rankings_full.tex")
    paper_md = joinpath(out_dir, "effective_rankings.md")
    paper_tex = joinpath(out_dir, "effective_rankings.tex")
    write_markdown_matrix(full_md, "Effective Ranking and Reversal-Pair Evolution, Full Table", full_headers, full_rows)
    write_latex_tabular(full_tex, "Effective ranking and reversal-pair evolution, full table", "tab:effective-ranking-evolution-full", full_headers, full_rows)
    write_markdown_matrix(paper_md, "Effective Ranking and Reversal-Pair Evolution", paper_headers, paper_rows)
    write_latex_tabular(paper_tex, "Effective ranking and reversal-pair evolution", "tab:effective-ranking-evolution", paper_headers, paper_rows)
    append!(paths, [full_md, full_tex, paper_md, paper_tex])

    CSV.write(joinpath(out_dir, "effective_rankings.csv"), vcat(values(per_year)...; cols = :union))
    push!(paths, joinpath(out_dir, "effective_rankings.csv"))
    write_csv(settings.table_manifest, manifest_rows(paths, settings.source))

    println("Wrote effective-ranking tables under ", out_dir)
    println("Wrote table manifest to ", settings.table_manifest)
    return nothing
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
