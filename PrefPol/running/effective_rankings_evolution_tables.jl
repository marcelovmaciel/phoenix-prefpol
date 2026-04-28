"""
    effective_rankings_evolution_tables.jl

Build one compact support/effective-number table per year from the cached
all-scenarios effective-number summary.

Default run:

    julia +1.11.9 --startup-file=no --project=PrefPol \
        PrefPol/running/effective_rankings_evolution_tables.jl

By "scenario" this script means the full cached analysis condition:
year-specific candidate scenario, imputation backend, linearization policy, and
analysis role. Override those filters with `key=value` arguments, e.g.

    julia +1.11.9 --startup-file=no --project=PrefPol \
        PrefPol/running/effective_rankings_evolution_tables.jl \
        imputer_backend=mice linearizer_policy=pattern_conditional \
        2022.scenario_name=no_forcing
"""

using CSV
using DataFrames
using Printf

const SMALL_OUTPUT_ROOT = normpath(joinpath(@__DIR__, "output", "all_scenarios_small"))
const INPUT_CSV = joinpath(SMALL_OUTPUT_ROOT, "effective_numbers_summary_table.csv")
const OUTPUT_DIR = joinpath(SMALL_OUTPUT_ROOT, "effective_rankings_evolution")

const DEFAULT_SCENARIO_BY_YEAR = Dict(
    2006 => "main_2006",
    2018 => "main_2018",
    2022 => "main_2022",
)

const DEFAULT_FILTERS = Dict(
    :analysis_role => "main",
    :imputer_backend => "mice",
    :linearizer_policy => "pattern_conditional",
)

const OUTPUT_COLS = [
    :year,
    :scenario_name,
    :imputer_backend,
    :linearizer_policy,
    :analysis_role,
    :B,
    :R,
    :K,
    :m,
    :n_rankings_observed_mean,
    :EO_median,
    :n_reversal_pairs_observed_mean,
    :ENRP_median,
    :max_rankings_possible,
    :max_reversal_pairs_possible,
]

const SUPPORTED_FILTER_KEYS = Set(keys(DEFAULT_FILTERS))

function _parse_year_scenario_key(raw_key::AbstractString)
    occursin(".", raw_key) || return nothing
    pieces = split(raw_key, "."; limit = 2)
    length(pieces) == 2 || return nothing
    pieces[2] == "scenario_name" || return nothing
    return parse(Int, pieces[1])
end

function parse_filters(args)
    scenario_by_year = copy(DEFAULT_SCENARIO_BY_YEAR)
    filters = copy(DEFAULT_FILTERS)

    for arg in args
        pieces = split(arg, "="; limit = 2)
        length(pieces) == 2 || error(
            "Expected filter arguments like `imputer_backend=mice` or " *
            "`2022.scenario_name=no_forcing`; got `$(arg)`.",
        )

        key = String(pieces[1])
        value = String(pieces[2])
        isempty(value) && error("Filter value for `$(key)` is empty.")

        year = _parse_year_scenario_key(key)
        if year !== nothing
            scenario_by_year[year] = value
            continue
        end

        # Backward-compatible shorthand: 2022=no_forcing.
        if all(isdigit, key)
            scenario_by_year[parse(Int, key)] = value
            continue
        end

        symkey = Symbol(key)
        symkey in SUPPORTED_FILTER_KEYS || error(
            "Unsupported filter `$(key)`. Supported filters are " *
            "$(sort(String.(collect(SUPPORTED_FILTER_KEYS)))) plus YEAR.scenario_name.",
        )

        filters[symkey] = value
    end

    return scenario_by_year, filters
end

function filter_label(scenario_name::AbstractString, filters::Dict{Symbol,String})
    parts = [
        scenario_name,
        filters[:imputer_backend],
        filters[:linearizer_policy],
        filters[:analysis_role],
    ]
    return join(parts, "__")
end

function normalize_effective_number_columns!(df::DataFrame)
    if !in(:ENRP_median, propertynames(df)) && :ER_median in propertynames(df)
        rename!(df, :ER_median => :ENRP_median)
    end
    return df
end

function choose_rows(df::DataFrame,
                     year::Int,
                     scenario_name::AbstractString,
                     filters::Dict{Symbol,String})
    rows = df[
        (df.year .== year) .&
        (df.scenario_name .== scenario_name) .&
        (df.analysis_role .== filters[:analysis_role]) .&
        (df.imputer_backend .== filters[:imputer_backend]) .&
        (df.linearizer_policy .== filters[:linearizer_policy]) .&
        in.(df.m, Ref(2:5)),
        :,
    ]

    if isempty(rows)
        available = unique(select(
            df[df.year .== year, :],
            [:year, :scenario_name, :analysis_role, :imputer_backend, :linearizer_policy],
        ))
        error(
            "No cached rows found for year=$(year), scenario_name=$(scenario_name), " *
            "analysis_role=$(filters[:analysis_role]), " *
            "imputer_backend=$(filters[:imputer_backend]), " *
            "linearizer_policy=$(filters[:linearizer_policy]).\n" *
            "Available cached conditions for year $(year):\n$(available)",
        )
    end

    observed_m = sort(unique(Int.(rows.m)))
    missing_m = setdiff(collect(2:5), observed_m)
    isempty(missing_m) || error(
        "Missing m values $(missing_m) for year=$(year), scenario_name=$(scenario_name), " *
        "analysis_role=$(filters[:analysis_role]), " *
        "imputer_backend=$(filters[:imputer_backend]), " *
        "linearizer_policy=$(filters[:linearizer_policy]).",
    )

    return sort(select(rows, OUTPUT_COLS), :m)
end

function write_markdown_table(io::IO, year::Int, scenario_name::AbstractString, filters, table::DataFrame)
    println(io, "## $(year) / $(scenario_name)")
    println(io)
    println(io, "- imputer_backend: `$(filters[:imputer_backend])`")
    println(io, "- linearizer_policy: `$(filters[:linearizer_policy])`")
    println(io, "- analysis_role: `$(filters[:analysis_role])`")
    println(io)
    println(
        io,
        "| m | observed rankings mean | EO median | observed reversal pairs mean | ENRP median | max rankings | max reversal pairs |",
    )
    println(io, "|---:|---:|---:|---:|---:|---:|---:|")

    for row in eachrow(table)
        println(
            io,
            "| $(row.m) | $(row.n_rankings_observed_mean) | $(row.EO_median) | " *
            "$(row.n_reversal_pairs_observed_mean) | $(row.ENRP_median) | " *
            "$(row.max_rankings_possible) | $(row.max_reversal_pairs_possible) |",
        )
    end

    println(io)
    return nothing
end

fmt_int(value) = string(Int(round(Float64(value))))

function fmt_num(value; digits::Int = 2)
    return @sprintf("%.*f", digits, Float64(value))
end

function latex_escape(value::AbstractString)
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
    rows = table[table.m .== m, :]
    nrow(rows) == 1 || error("Expected exactly one row for m=$(m), found $(nrow(rows)).")
    return rows[1, :]
end

function write_markdown_matrix(path::AbstractString,
                               title::AbstractString,
                               headers::Vector{String},
                               rows::Vector{Vector{String}})
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

function write_latex_tabular(path::AbstractString,
                             caption::AbstractString,
                             label::AbstractString,
                             headers::Vector{String},
                             rows::Vector{Vector{String}})
    alignment = "r" ^ length(headers)
    open(path, "w") do io
        println(io, "\\begin{table}[!htbp]")
        println(io, "\\centering")
        println(io, "\\caption{", latex_escape(caption), "}")
        println(io, "\\label{", latex_escape(label), "}")
        println(io, "\\begin{tabular}{", alignment, "}")
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

function presentation_rows(per_year_tables::Dict{Int,DataFrame};
                           paper_only::Bool)
    years = sort(collect(keys(per_year_tables)))
    headers = paper_only ? ["m"] : ["m"]

    for year in years
        if paper_only
            append!(headers, ["EO $(year)", "ENRP $(year)"])
        else
            append!(
                headers,
                [
                    "rankings $(year)",
                    "EO $(year)",
                    "reversal pairs $(year)",
                    "ENRP $(year)",
                ],
            )
        end
    end

    rows = Vector{String}[]
    for m in 2:5
        row_values = [string(m)]
        for year in years
            row = table_row_by_m(per_year_tables[year], m)
            if paper_only
                append!(row_values, [fmt_num(row.EO_median), fmt_num(row.ENRP_median)])
            else
                append!(
                    row_values,
                    [
                        fmt_num(row.n_rankings_observed_mean),
                        fmt_num(row.EO_median),
                        fmt_num(row.n_reversal_pairs_observed_mean),
                        fmt_num(row.ENRP_median),
                    ],
                )
            end
        end
        push!(rows, row_values)
    end

    return headers, rows
end

function write_presentation_tables(per_year_tables::Dict{Int,DataFrame},
                                   filters::Dict{Symbol,String})
    suffix = filter_label("selected_scenarios", filters)

    full_headers, full_rows = presentation_rows(per_year_tables; paper_only = false)
    paper_headers, paper_rows = presentation_rows(per_year_tables; paper_only = true)

    full_md = joinpath(OUTPUT_DIR, "effective_rankings_evolution_presentation_full_$(suffix).md")
    full_tex = joinpath(OUTPUT_DIR, "effective_rankings_evolution_presentation_full_$(suffix).tex")
    paper_md = joinpath(OUTPUT_DIR, "effective_rankings_evolution_paper_$(suffix).md")
    paper_tex = joinpath(OUTPUT_DIR, "effective_rankings_evolution_paper_$(suffix).tex")

    write_markdown_matrix(
        full_md,
        "Effective Ranking and Reversal-Pair Evolution, Full Table",
        full_headers,
        full_rows,
    )
    write_latex_tabular(
        full_tex,
        "Effective ranking and reversal-pair evolution, full table",
        "tab:effective-ranking-evolution-full",
        full_headers,
        full_rows,
    )
    write_markdown_matrix(
        paper_md,
        "Effective Ranking and Reversal-Pair Evolution",
        paper_headers,
        paper_rows,
    )
    write_latex_tabular(
        paper_tex,
        "Effective ranking and reversal-pair evolution",
        "tab:effective-ranking-evolution",
        paper_headers,
        paper_rows,
    )

    println("Saved presentation full markdown table: ", full_md)
    println("Saved presentation full LaTeX table: ", full_tex)
    println("Saved paper markdown table: ", paper_md)
    println("Saved paper LaTeX table: ", paper_tex)
    return nothing
end

function main(args = ARGS)
    isfile(INPUT_CSV) || error(
        "Input CSV not found: $(INPUT_CSV). Run `PrefPol/running/run_all_scenarios_small.jl` first.",
    )

    scenario_by_year, filters = parse_filters(args)
    df = normalize_effective_number_columns!(CSV.read(INPUT_CSV, DataFrame))
    mkpath(OUTPUT_DIR)

    markdown_path = joinpath(
        OUTPUT_DIR,
        "effective_rankings_evolution_tables_" *
        filter_label("selected_scenarios", filters) *
        ".md",
    )
    per_year_tables = Dict{Int,DataFrame}()
    open(markdown_path, "w") do io
        println(io, "# Effective Ranking and Reversal-Pair Evolution")
        println(io)
        println(io, "Source: `$(relpath(INPUT_CSV, pwd()))`")
        println(io)

        for year in sort(collect(keys(scenario_by_year)))
            scenario_name = scenario_by_year[year]
            table = choose_rows(df, year, scenario_name, filters)
            csv_path = joinpath(
                OUTPUT_DIR,
                "effective_rankings_evolution_$(year)_" *
                filter_label(scenario_name, filters) *
                ".csv",
            )

            CSV.write(csv_path, table)
            per_year_tables[year] = table
            write_markdown_table(io, year, scenario_name, filters, table)
            println("Saved table for $(year) / $(scenario_name): ", csv_path)
        end
    end

    write_presentation_tables(per_year_tables, filters)
    println("Saved markdown tables: ", markdown_path)
    return nothing
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
