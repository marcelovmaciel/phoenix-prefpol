### A Pluto.jl notebook ###
# v0.20.17

using Markdown

# ╔═╡ f3f55370-21e3-44f2-8fe4-6be130f24166
begin
    import Pkg
    Pkg.activate(@__DIR__)
end

# ╔═╡ c6101304-df6a-4f37-bf5e-c1a6a612e8f7
md"""
# Plots and Tables

This notebook explains the downstream table and simple plot layer without
becoming another production runner.

It corresponds conceptually to two CLI stages:

- `08_tables.jl` reads the effective-count summary produced by the extra
  diagnostics stage and turns it into table-ready CSV, Markdown, and TeX.
- `09_extra_plots.jl` reads those table CSVs and turns them into a compact
  effective-ranking evolution plot.

The notebook keeps the same data flow but uses local notebook-scale outputs
under `nb/output/notebook_smoke`. It does not attempt to reproduce publication
figure aesthetics.
"""

# ╔═╡ 8822521b-afab-4757-a03b-5375b0110644
begin
    # composable-running concept: shared setup from stage_common.jl.
    include(joinpath(@__DIR__, "notebook_common.jl"))
    using Printf
    using Statistics
end

# ╔═╡ 516fa1c8-5b38-434f-a4d2-eea3bce2dc4a
begin
    cfg = load_notebook_config()
    settings = notebook_settings(cfg)
    targets = notebook_targets(cfg)
end

# ╔═╡ 246f8d01-e5f2-4761-a894-73dc34cb25b9
begin
    @assert settings.B <= 5 "Notebook config should keep B tiny."
    @assert settings.R <= 5 "Notebook config should keep R tiny."
    @assert settings.K <= 5 "Notebook config should keep K tiny."
    ensure_not_publication_output!(settings.output_root)
    ensure_not_publication_output!(settings.cache_root)
end

# ╔═╡ cddfe01b-70ca-4f40-91f2-1fc80324289e
md"""
## Effective-Count Source

In the production pipeline, the table stage reads
`output/extra_measures/effective_counts/effective_counts_summary.csv`. In this
notebook, the preferred source is the compact draw table written by
`06_extra_diagnostics.jl`. If that local file is not present yet, the notebook
uses a tiny deterministic fallback based on the notebook target configuration
so the table-formatting layer can still be inspected.
"""

# ╔═╡ 0e51831a-841d-4f0a-a67b-a48be8ee95a17
begin
    notebook_table_dir = joinpath(settings.output_root, "notebook_tables")
    effective_draws_path = joinpath(
        notebook_table_dir,
        "extra_diagnostics_effective_draws_compact.csv",
    )
end

# ╔═╡ 1a3082cf-4597-43b0-ba58-9f121865dc85
function compact_median(values)
    xs = Float64.(collect(skipmissing(values)))
    return isempty(xs) ? missing : median(xs)
end

# ╔═╡ 4102cfe5-c626-42c5-8e12-f399d1da2bc5
function compact_mean(values)
    xs = Float64.(collect(skipmissing(values)))
    return isempty(xs) ? missing : mean(xs)
end

# ╔═╡ 8045fed9-e5a3-41ef-99c8-1d5e3fb8cd2e
function effective_summary_from_draws(draws::DataFrame)
    group_cols = [
        :year,
        :scenario_name,
        :m,
    ]
    base = combine(
        groupby(draws, group_cols),
        :n_rankings_observed => compact_mean => :n_rankings_observed_mean,
        :EO => compact_median => :EO_median,
        :n_reversal_pairs_observed => compact_mean => :n_reversal_pairs_observed_mean,
        :ENRP => compact_median => :ENRP_median,
        :max_rankings_possible => first => :max_rankings_possible,
        :max_reversal_pairs_possible => first => :max_reversal_pairs_possible,
    )
    base[!, :imputer_backend] = fill(String(first(settings.imputer_backends)), nrow(base))
    base[!, :linearizer_policy] = fill(String(first(settings.linearizer_policies)), nrow(base))
    base[!, :analysis_role] = fill("main", nrow(base))
    base[!, :B] = fill(settings.B, nrow(base))
    base[!, :R] = fill(settings.R, nrow(base))
    base[!, :K] = fill(settings.K, nrow(base))
    return sort(select(
        base,
        [
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
        ],
    ), [:year, :scenario_name, :m])
end

# ╔═╡ 06e31fda-903a-42e7-b592-19c90cf7efae
function fallback_effective_summary(targets)
    rows = NamedTuple[]
    for target in targets
        for m in target.m_values
            possible = factorial(Int(m))
            push!(rows, (
                year = parse(Int, target.wave_id),
                scenario_name = target.scenario_name,
                imputer_backend = String(first(settings.imputer_backends)),
                linearizer_policy = String(first(settings.linearizer_policies)),
                analysis_role = "main",
                B = settings.B,
                R = settings.R,
                K = settings.K,
                m = Int(m),
                n_rankings_observed_mean = min(possible, 2.0 + m),
                EO_median = min(possible, 1.5 + m / 2),
                n_reversal_pairs_observed_mean = min(div(possible, 2), max(0.0, m - 1.0)),
                ENRP_median = min(div(possible, 2), max(0.0, m / 2)),
                max_rankings_possible = possible,
                max_reversal_pairs_possible = div(possible, 2),
            ))
        end
    end
    return sort(DataFrame(rows), [:year, :scenario_name, :m])
end

# ╔═╡ d656a29f-0f9b-4888-a36b-a9cdb0245800
begin
    source_mode = isfile(effective_draws_path) ? "from 06_extra_diagnostics notebook CSV" : "tiny deterministic fallback"
    effective_counts_summary = isfile(effective_draws_path) ?
        effective_summary_from_draws(CSV.read(effective_draws_path, DataFrame)) :
        fallback_effective_summary(targets)
end

# ╔═╡ d6849655-7c1a-4ee4-bf8c-69c5636a5aa4
source_table = DataFrame(
    item = ["source mode", "source path", "rows"],
    value = [source_mode, effective_draws_path, string(nrow(effective_counts_summary))],
)

# ╔═╡ 08217786-04e0-4314-b045-7796c18aaadf
small_table(source_table; n = nrow(source_table))

# ╔═╡ 6ab869ca-1a12-42ca-9b3b-de23da4e104b
small_table(effective_counts_summary; n = min(12, nrow(effective_counts_summary)))

# ╔═╡ 1daac4ef-0f08-492c-8a3c-08bbf0ec6c65
md"""
## Table-Ready Outputs

The table stage selects rows for configured years/scenarios, writes per-target
CSV files, then derives Markdown and TeX matrices for the paper-facing table.
Here the same transformation is kept local and compact.
"""

# ╔═╡ c5285a79-bf7b-43f1-b8da-62eda84cfd60
fmt_num(value; digits::Int = 2) = ismissing(value) ? "" : @sprintf("%.*f", digits, Float64(value))

# ╔═╡ d1b9e636-a06f-4f50-80ff-8618063c9414
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

# ╔═╡ 1801bc15-2f1c-4a1d-af58-9514054dd0bc
function table_row_by_m(table::DataFrame, m::Integer)
    rows = table[Int.(table.m) .== Int(m), :]
    nrow(rows) == 1 || error("Expected exactly one row for m=$(m), found $(nrow(rows)).")
    return rows[1, :]
end

# ╔═╡ 13db0754-356f-4a01-b695-633983971157
function presentation_rows(per_year_tables::Dict{Int,DataFrame}; paper_only::Bool)
    years = sort(collect(keys(per_year_tables)))
    headers = ["m"]
    for year in years
        append!(headers, paper_only ?
            ["EO $(year)", "ENRP $(year)"] :
            ["rankings $(year)", "EO $(year)", "reversal pairs $(year)", "ENRP $(year)"])
    end

    rows = Vector{String}[]
    all_m = sort(unique(vcat([Int.(table.m) for table in values(per_year_tables)]...)))
    for m in all_m
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

# ╔═╡ 55de5e47-4d7b-4246-942d-5efb98a4d57c
function markdown_matrix(title, headers, rows)
    io = IOBuffer()
    println(io, "# ", title)
    println(io)
    println(io, "| ", join(headers, " | "), " |")
    println(io, "|", join(fill("---:", length(headers)), "|"), "|")
    for row in rows
        println(io, "| ", join(row, " | "), " |")
    end
    return String(take!(io))
end

# ╔═╡ 1880765c-dd72-4209-95d3-fe2e302de5b5
function latex_tabular(caption, label, headers, rows)
    io = IOBuffer()
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
    return String(take!(io))
end

# ╔═╡ 124a10af-06fb-40b3-bebe-37ad9df2a9d7
function write_notebook_text(path, text)
    resolved = ensure_not_publication_output!(path)
    mkpath(dirname(resolved))
    open(resolved, "w") do io
        write(io, text)
    end
    return resolved
end

# ╔═╡ ddc4af92-6d9c-49dd-a69f-ae71dfbdb3dd
begin
    local_effective_table_dir = joinpath(notebook_table_dir, "effective_rankings")
    per_year_tables = Dict{Int,DataFrame}()
    per_year_csv_paths = String[]

    for target in targets
        year = parse(Int, target.wave_id)
        rows = effective_counts_summary[
            (Int.(effective_counts_summary.year) .== year) .&
            (string.(effective_counts_summary.scenario_name) .== target.scenario_name),
            :,
        ]
        isempty(rows) && continue
        table = sort(rows, :m)
        per_year_tables[year] = table
        csv_path = write_notebook_csv(
            joinpath(local_effective_table_dir, "effective_rankings_evolution_$(year)_$(target.scenario_name).csv"),
            table,
        )
        push!(per_year_csv_paths, csv_path)
    end

    full_headers, full_rows = presentation_rows(per_year_tables; paper_only = false)
    paper_headers, paper_rows = presentation_rows(per_year_tables; paper_only = true)
    full_markdown = markdown_matrix("Effective Ranking and Reversal-Pair Evolution, Full Table", full_headers, full_rows)
    paper_markdown = markdown_matrix("Effective Ranking and Reversal-Pair Evolution", paper_headers, paper_rows)
    full_tex = latex_tabular(
        "Effective ranking and reversal-pair evolution, full table",
        "tab:notebook-effective-ranking-evolution-full",
        full_headers,
        full_rows,
    )
    paper_tex = latex_tabular(
        "Effective ranking and reversal-pair evolution",
        "tab:notebook-effective-ranking-evolution",
        paper_headers,
        paper_rows,
    )
end

# ╔═╡ a7a14476-2017-4f22-a4a4-b8329b86c7a5
begin
    full_md_path = write_notebook_text(joinpath(local_effective_table_dir, "effective_rankings_full.md"), full_markdown)
    paper_md_path = write_notebook_text(joinpath(local_effective_table_dir, "effective_rankings.md"), paper_markdown)
    full_tex_path = write_notebook_text(joinpath(local_effective_table_dir, "effective_rankings_full.tex"), full_tex)
    paper_tex_path = write_notebook_text(joinpath(local_effective_table_dir, "effective_rankings.tex"), paper_tex)
    combined_csv_path = write_notebook_csv(
        joinpath(local_effective_table_dir, "effective_rankings.csv"),
        vcat(values(per_year_tables)...; cols = :union),
    )
end

# ╔═╡ 811e435f-cc0d-426e-b11b-450b1502ce93
local_table_outputs = DataFrame(
    artifact = [
        "per-year evolution CSVs",
        "combined CSV",
        "full Markdown",
        "paper Markdown",
        "full TeX",
        "paper TeX",
    ],
    path = [
        join(per_year_csv_paths, "; "),
        combined_csv_path,
        full_md_path,
        paper_md_path,
        full_tex_path,
        paper_tex_path,
    ],
)

# ╔═╡ e41f1db4-6e3e-4130-a9ac-bbe59d4a8030
small_table(local_table_outputs; n = nrow(local_table_outputs))

# ╔═╡ 099ff0ce-b29f-457b-b79c-13446dce2729
md"""
## Plot Data

The extra-plot stage loads the table CSVs, keeps `year`, `m`, `EO_median`, and
`ENRP_median`, and draws an evolution figure. The local plot below intentionally
uses a plain layout: the point is to inspect the transformation from table rows
to plotting data.
"""

# ╔═╡ 01783069-b0fb-45ad-bcf0-417032c09b4f
function load_local_evolution_tables(input_dir::AbstractString)
    files = [
        joinpath(input_dir, file)
        for file in readdir(input_dir)
        if startswith(file, "effective_rankings_evolution_") && endswith(file, ".csv")
    ]
    tables = DataFrame[]
    for file in sort(files)
        df = CSV.read(file, DataFrame)
        required = [:year, :m, :EO_median, :ENRP_median]
        missing_cols = setdiff(required, propertynames(df))
        isempty(missing_cols) || error("$(file) is missing required columns $(missing_cols).")
        push!(tables, select(df, intersect([:year, :scenario_name, :m, :EO_median, :ENRP_median], propertynames(df))))
    end
    return sort(vcat(tables...; cols = :union), [:year, :m])
end

# ╔═╡ c77d7443-6b3b-4179-b0cf-ae9cb5d18a70
plot_data = load_local_evolution_tables(local_effective_table_dir)

# ╔═╡ 9dff55ab-50ce-47c1-8e83-060998888cf4
small_table(plot_data; n = min(12, nrow(plot_data)))

# ╔═╡ a5fc0fa3-b0e2-46fe-af45-d6057cbe314c
begin
    cairomakie_available = try
        @eval using CairoMakie
        true
    catch
        false
    end
end

# ╔═╡ 3f8c3139-4a14-4619-918c-858902b67dff
begin
    local_plot_dir = joinpath(settings.output_root, "notebook_plots", "effective_rankings")
    plot_data_csv_path = write_notebook_csv(
        joinpath(local_plot_dir, "effective_rankings_evolution_plot_data.csv"),
        plot_data,
    )
    local_plot_path = ""

    if cairomakie_available
        mkpath(local_plot_dir)
        fig = Figure(size = (760, 360), fontsize = 12)
        ax1 = Axis(fig[1, 1], xlabel = "m", ylabel = "median EO", title = "Effective rankings")
        ax2 = Axis(fig[1, 2], xlabel = "m", ylabel = "median ENRP", title = "Effective reversal pairs")

        for year in sort(unique(Int.(plot_data.year)))
            rows = plot_data[Int.(plot_data.year) .== year, :]
            sort!(rows, :m)
            lines!(ax1, rows.m, rows.EO_median; linewidth = 2, label = string(year))
            scatter!(ax1, rows.m, rows.EO_median; markersize = 8)
            lines!(ax2, rows.m, rows.ENRP_median; linewidth = 2, label = string(year))
            scatter!(ax2, rows.m, rows.ENRP_median; markersize = 8)
        end
        axislegend(ax1, "year"; position = :lt, framevisible = false)

        local_plot_path = joinpath(local_plot_dir, "effective_rankings_evolution_local.png")
        save(local_plot_path, fig; px_per_unit = 2)
    end
end

# ╔═╡ 118b4a83-cb79-4ae0-9a77-315e7b3178b2
plot_output_table = DataFrame(
    artifact = ["plot data CSV", "local plot PNG"],
    path = [plot_data_csv_path, isempty(local_plot_path) ? "(CairoMakie unavailable)" : local_plot_path],
)

# ╔═╡ ccc40dd0-c689-49fc-b6bd-e51db564ef20
small_table(plot_output_table; n = nrow(plot_output_table))

# ╔═╡ 43ca314e-c53a-48dd-a902-1f86de2a6fb2
md"""
## Local EO/ENRP Table

This final compact view is the paper-facing core of the effective ranking table:
`EO` and `ENRP` by `m`, for the notebook-scale target rows.
"""

# ╔═╡ 3f1ad613-8a04-4827-b573-5ac1b21795e7
local_eo_enrp_table = select(
    effective_counts_summary,
    [:year, :scenario_name, :m, :EO_median, :ENRP_median],
)

# ╔═╡ d751c8f2-d1ad-4f08-9fa0-87fa5908ac34
small_table(local_eo_enrp_table; n = min(12, nrow(local_eo_enrp_table)))

# ╔═╡ 4eff5688-a346-49db-baf2-4baaa3caee89
function is_under_directory(child_path::AbstractString, parent_path::AbstractString)
    relative_path = relpath(normpath(child_path), normpath(parent_path))
    return relative_path == "." || (!startswith(relative_path, "..") && !isabspath(relative_path))
end

# ╔═╡ 01208348-c961-4d93-a46b-edffb88f4b5b
begin
    output_paths_to_check = String[
        per_year_csv_paths...,
        combined_csv_path,
        full_md_path,
        paper_md_path,
        full_tex_path,
        paper_tex_path,
        plot_data_csv_path,
    ]
    isempty(local_plot_path) || push!(output_paths_to_check, local_plot_path)

    validation_table = DataFrame(
        check = [
            "table outputs under notebook output root",
            "plot outputs under notebook output root",
            "CairoMakie plot attempted",
        ],
        value = [
            string(all(path -> is_under_directory(path, settings.output_root), output_paths_to_check)),
            string(isempty(local_plot_path) || is_under_directory(local_plot_path, settings.output_root)),
            string(cairomakie_available),
        ],
    )
end

# ╔═╡ 843ece57-211f-49e8-806d-723d02c258b3
small_table(validation_table; n = nrow(validation_table))

# ╔═╡ Cell order:
# ╠═f3f55370-21e3-44f2-8fe4-6be130f24166
# ╟─c6101304-df6a-4f37-bf5e-c1a6a612e8f7
# ╠═8822521b-afab-4757-a03b-5375b0110644
# ╠═516fa1c8-5b38-434f-a4d2-eea3bce2dc4a
# ╠═246f8d01-e5f2-4761-a894-73dc34cb25b9
# ╟─cddfe01b-70ca-4f40-91f2-1fc80324289e
# ╠═0e51831a-841d-4f0a-a67b-a48be8ee95a17
# ╠═1a3082cf-4597-43b0-ba58-9f121865dc85
# ╠═4102cfe5-c626-42c5-8e12-f399d1da2bc5
# ╠═8045fed9-e5a3-41ef-99c8-1d5e3fb8cd2e
# ╠═06e31fda-903a-42e7-b592-19c90cf7efae
# ╠═d656a29f-0f9b-4888-a36b-a9cdb0245800
# ╠═d6849655-7c1a-4ee4-bf8c-69c5636a5aa4
# ╟─08217786-04e0-4314-b045-7796c18aaadf
# ╟─6ab869ca-1a12-42ca-9b3b-de23da4e104b
# ╟─1daac4ef-0f08-492c-8a3c-08bbf0ec6c65
# ╠═c5285a79-bf7b-43f1-b8da-62eda84cfd60
# ╠═d1b9e636-a06f-4f50-80ff-8618063c9414
# ╠═1801bc15-2f1c-4a1d-af58-9514054dd0bc
# ╠═13db0754-356f-4a01-b695-633983971157
# ╠═55de5e47-4d7b-4246-942d-5efb98a4d57c
# ╠═1880765c-dd72-4209-95d3-fe2e302de5b5
# ╠═124a10af-06fb-40b3-bebe-37ad9df2a9d7
# ╠═ddc4af92-6d9c-49dd-a69f-ae71dfbdb3dd
# ╠═a7a14476-2017-4f22-a4a4-b8329b86c7a5
# ╠═811e435f-cc0d-426e-b11b-450b1502ce93
# ╟─e41f1db4-6e3e-4130-a9ac-bbe59d4a8030
# ╟─099ff0ce-b29f-457b-b79c-13446dce2729
# ╠═01783069-b0fb-45ad-bcf0-417032c09b4f
# ╠═c77d7443-6b3b-4179-b0cf-ae9cb5d18a70
# ╟─9dff55ab-50ce-47c1-8e83-060998888cf4
# ╠═a5fc0fa3-b0e2-46fe-af45-d6057cbe314c
# ╠═3f8c3139-4a14-4619-918c-858902b67dff
# ╠═118b4a83-cb79-4ae0-9a77-315e7b3178b2
# ╟─ccc40dd0-c689-49fc-b6bd-e51db564ef20
# ╟─43ca314e-c53a-48dd-a902-1f86de2a6fb2
# ╠═3f1ad613-8a04-4827-b573-5ac1b21795e7
# ╟─d751c8f2-d1ad-4f08-9fa0-87fa5908ac34
# ╠═4eff5688-a346-49db-baf2-4baaa3caee89
# ╠═01208348-c961-4d93-a46b-edffb88f4b5b
# ╟─843ece57-211f-49e8-806d-723d02c258b3
