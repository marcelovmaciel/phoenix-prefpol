### A Pluto.jl notebook ###
# v1.0.1

using Markdown
using InteractiveUtils

# This Pluto notebook uses @bind for interactivity. When running this notebook outside of Pluto, the following 'mock version' of @bind gives bound variables a default value (instead of an error).
macro bind(def, element)
    #! format: off
    return quote
        local iv = try Base.loaded_modules[Base.PkgId(Base.UUID("6e696c72-6542-2067-7265-42206c756150"), "AbstractPlutoDingetjes")].Bonds.initial_value catch; b -> missing; end
        local el = $(esc(element))
        global $(esc(def)) = Core.applicable(Base.get, el) ? Base.get(el) : iv(el)
        el
    end
    #! format: on
end

# ╔═╡ 1b48c92a-5d58-4db7-9f55-f92c1a52a1f2
begin
    import Pkg
    Pkg.activate(@__DIR__)
end

# ╔═╡ 87c29c6b-04d2-4227-9b5c-ce14d610894e
using CairoMakie

# ╔═╡ 11efc93a-5930-4478-b110-f8c042d984d4
begin
    # Load shared notebook helpers and the local PrefPol package.
    include(joinpath(@__DIR__, "notebook_common.jl"))
end

# ╔═╡ 8ec3853a-a893-456a-b254-a3171433054b
md"""
# Global and Group Summaries

The measure cube is the leaf-level output of the nested stochastic pipeline:
each row is a measure value for one bootstrap, imputation, and linearization
leaf `(b, r, k)`. The paper figures do not plot those raw leaves directly.
They first pool and reshape the cube into scenario-level panel tables:
global measures summarize the full preference profile, while grouped measures
summarize configured demographic partitions.

This notebook corresponds conceptually to the production global and grouped
plot-data passes, but it does not call those stage scripts. Instead, it builds
the notebook-scale batch, computes or loads the tiny `PipelineResult`s, and
calls the same PrefPol reporting and plot-data APIs that the plotting stages use.

The priority here is inspectable tables and intermediate summaries. Any plots
below are small interactive checks, not publication figures and not attempts to
reproduce the paper aesthetics.
"""

# ╔═╡ 2cd34337-ae3e-4d7a-8611-c7724e10ff4f
begin
    cfg = load_notebook_config()
    settings = notebook_settings(cfg)
    targets = notebook_targets(cfg)
    plot_specs_cfg = TOML.parsefile(joinpath(prefpol_root(), "config", "plot_specs.toml"))
end

# ╔═╡ 5f3b0611-978f-4430-8043-95a681ef5302
begin
    notebook_global_measures = [:Psi, :R, :HHI, :RHHI]
    notebook_group_measures = [:C, :D]
    @assert notebook_global_measures == [:Psi, :R, :HHI, :RHHI]
    @assert notebook_group_measures == [:C, :D]
    @assert all(measure -> measure in settings.measures, vcat(notebook_global_measures, notebook_group_measures))
    @assert settings.B <= 5 "Notebook config keeps B <= 5."
    @assert settings.R <= 5 "Notebook config keeps R <= 5."
    @assert settings.K <= 5 "Notebook config keeps K <= 5."
    ensure_not_publication_output!(settings.output_root)
    ensure_not_publication_output!(settings.cache_root)
end

# ╔═╡ d50d563d-c9c5-4b29-aeca-76f1a5919e85
begin
    waves, source_registry, wave_by_id = load_notebook_waves()
    notebook_batch = build_notebook_batch(cfg)
    pipeline = pp.NestedStochasticPipeline(source_registry; cache_root = settings.cache_root)
end

# ╔═╡ 09e68af2-41fd-45a2-b0bf-efafbd12c484
notebook_batch_table = pp.DataFrame(
    batch_index = collect(eachindex(notebook_batch.items)),
    year = [item.metadata.year for item in notebook_batch.items],
    wave_id = [item.spec.wave_id for item in notebook_batch.items],
    scenario_name = [item.metadata.scenario_name for item in notebook_batch.items],
    m = [item.metadata.m for item in notebook_batch.items],
    imputer_backend = String.([item.spec.imputer_backend for item in notebook_batch.items]),
    linearizer_policy = String.([item.spec.linearizer_policy for item in notebook_batch.items]),
    B = [item.spec.B for item in notebook_batch.items],
    R = [item.spec.R for item in notebook_batch.items],
    K = [item.spec.K for item in notebook_batch.items],
    groupings = [join(String.(item.spec.groupings), ", ") for item in notebook_batch.items],
    measures = [join(String.(item.spec.measures), ", ") for item in notebook_batch.items],
)

# ╔═╡ 29ba8b54-a01b-4a90-8646-5feb394a2f82
small_table(notebook_batch_table; n = pp.nrow(notebook_batch_table))

# ╔═╡ 60a66ff2-c61b-4949-80f3-b4065b25cad3
md"""
## Compute or Load Tiny Pipeline Results

Each row in the batch table is one tiny pipeline spec. `PrefPol.ensure_measures!`
reuses notebook-local cache when available and otherwise ensures upstream
resampling, imputation, linearization, and measure artifacts. The resulting
objects are then wrapped in a `BatchRunResult`, matching the type consumed by
the plot-data construction APIs.
"""

# ╔═╡ 01b5e188-5b19-4b7f-9398-9b4750aa6694
begin
    notebook_results = pp.PipelineResult[]
    notebook_metadata = NamedTuple[]

    for item in notebook_batch.items
        push!(notebook_results, pp.ensure_measures!(
            pipeline,
            item.spec;
            force = settings.force,
            progress = false,
        ))
        push!(notebook_metadata, item.metadata)
    end

    batch_results = pp.BatchRunResult(notebook_batch, notebook_results, notebook_metadata)
end

# ╔═╡ 1399cbd4-b08a-4464-9843-6f66f1581cec
pipeline_result_table = pp.DataFrame(
    batch_index = collect(eachindex(batch_results.results)),
    cache_dir = [result.cache_dir for result in batch_results.results],
    measure_cube_rows = [pp.nrow(result.measure_cube) for result in batch_results.results],
    pooled_summary_rows = [pp.nrow(result.pooled_summaries) for result in batch_results.results],
)

# ╔═╡ a5e6d5ac-a579-44dc-9036-3d28ac354936
small_table(pipeline_result_table; n = pp.nrow(pipeline_result_table))

# ╔═╡ a25da9b6-96ff-4b63-bd54-4a9f91428a11
md"""
## Global Summary

The global plotting stage writes summary and panel CSVs before drawing figures.
Here we keep the table front and center. The rows below come from
`PrefPol.pipeline_panel_table` and `PrefPol.pipeline_scenario_plot_data`, then
are restricted to the paper's global measures: `Psi`, `R`, `HHI`, and `RHHI`.
"""

# ╔═╡ 6ba53d87-2161-4a78-84d8-c63316cc6eca
begin
    panel_table = pp.pipeline_panel_table(batch_results)
    summary_table = pp.pipeline_summary_table(batch_results)

    global_panel_rows = pp.select_pipeline_panel_rows(
        panel_table;
        measures = notebook_global_measures,
        include_grouped = false,
    )
    global_summary_rows = summary_table[
        [Symbol(row.measure) in Set(notebook_global_measures) for row in eachrow(summary_table)] .&
        ismissing.(summary_table.grouping),
        :,
    ]
end

# ╔═╡ a0265370-9bd1-48e1-8a16-7d35632c4aa3
global_summary_table = sorted_table(pp.select(
    global_panel_rows,
    :year,
    :wave_id,
    :scenario_name => :scenario,
    :m,
    :imputer_backend,
    :linearizer_policy,
    :measure,
    :estimate,
    :q50,
    :mean_value,
    :q25,
    :q75,
    :total_sd,
    :n_draws,
))

# ╔═╡ 11656802-f99b-466d-8d72-2a338e58d76a
small_table(global_summary_table; n = pp.nrow(global_summary_table))

# ╔═╡ 5f01469a-3f1d-4567-ab8e-796c7043de25
begin
    global_m_values = sort(unique(Int.(global_summary_table.m)))
    m_sweep_note = length(global_m_values) > 1 ?
        md"""
        The notebook config currently uses multiple `m` values:
        `$(join(global_m_values, ", "))`. This gives a miniature version of the
        paper's `m`-sweep.
        """ :
        md"""
        The notebook config currently uses only `m = $(only(global_m_values))`.
        Widening `m_values` in `nb/notebook_config.toml` gives a miniature
        version of the paper's `m`-sweep.
        """
end

# ╔═╡ fb87d454-a9e8-4be9-a3d9-f870dc089dfc
m_sweep_note

# ╔═╡ af12e60d-cee1-4b4b-9e07-72501d72473d
begin
    scenario_plot_data_tables = pp.DataFrame[]
    for row in eachrow(unique(pp.select(
        global_summary_table,
        [:wave_id, :scenario, :imputer_backend, :linearizer_policy],
    )))
        combo_results = let
            items = pp.StudyBatchItem[]
            results = pp.PipelineResult[]
            metadata = NamedTuple[]
            for (idx, item) in enumerate(batch_results.batch.items)
                item.spec.wave_id == string(row.wave_id) || continue
                string(item.metadata.scenario_name) == string(row.scenario) || continue
                string(item.spec.imputer_backend) == string(row.imputer_backend) || continue
                string(item.spec.linearizer_policy) == string(row.linearizer_policy) || continue
                push!(items, item)
                push!(results, batch_results.results[idx])
                push!(metadata, batch_results.metadata[idx])
            end
            pp.BatchRunResult(pp.StudyBatchSpec(items), results, metadata)
        end

        isempty(combo_results.results) && continue
        plot_data = pp.pipeline_scenario_plot_data(
            combo_results;
            wave_id = row.wave_id,
            scenario_name = row.scenario,
            imputer_backend = Symbol(row.imputer_backend),
            measures = notebook_global_measures,
        )
        push!(scenario_plot_data_tables, sorted_table(pp.select(
            plot_data.rows,
            intersect([
                :year,
                :wave_id,
                :scenario_name,
                :m,
                :imputer_backend,
                :linearizer_policy,
                :measure,
                :estimate,
                :q50,
                :mean_value,
                :total_sd,
            ], propertynames(plot_data.rows)),
        )))
    end

    scenario_plot_data_table = isempty(scenario_plot_data_tables) ?
        pp.DataFrame() :
        vcat(scenario_plot_data_tables...; cols = :union)
end

# ╔═╡ d6bd892b-dcb8-4646-87e5-4d5a1b375d38
md"""
### Plot-Data Rows

These rows are produced through `PrefPol.pipeline_scenario_plot_data`, the same
global plot-data constructor used before the stage draws the paper figure.
"""

# ╔═╡ 2fddc636-dba3-4994-a233-3621d25140d5
small_table(scenario_plot_data_table; n = min(20, pp.nrow(scenario_plot_data_table)))

# ╔═╡ 80c5f12d-7c5e-438d-852c-ac5c08f152ea
md"""
## Group Summary

The group plotting stage uses grouped panel rows and heatmap-ready values. This
notebook keeps only the publication-facing grouped measures, `C` and `D`.
The broader `plot_specs.toml` also contains `S` and `O` for internal plotting
defaults, but the notebook main measure set narrows the grouped panel to `C`
and `D`; this notebook follows that local setting and skips `S`/`O`.
"""

# ╔═╡ 722e21cc-2030-4873-bc4f-5519dfe71f81
begin
    group_panel_rows = pp.select_pipeline_panel_rows(
        panel_table;
        measures = notebook_group_measures,
        include_grouped = true,
    )

    groupings_by_wave = pp.combine(
        pp.groupby(group_panel_rows, [:year, :wave_id]),
        :grouping => (xs -> join(sort(unique(String.(skipmissing(xs)))), ", ")) => :groupings,
    )
    groupings_by_wave = sort(groupings_by_wave, [:year, :wave_id])
end

# ╔═╡ be5320e0-a27f-4511-a812-f790f5fc4b71
small_table(groupings_by_wave; n = pp.nrow(groupings_by_wave))

# ╔═╡ 4127132e-6657-4d1b-bcad-9fbc3206196b
begin
    heatmap_ready_tables = pp.DataFrame[]
    for row in eachrow(unique(pp.select(
        group_panel_rows,
        [:year, :wave_id, :scenario_name, :imputer_backend, :linearizer_policy],
    )))
        combo_results = let
            items = pp.StudyBatchItem[]
            results = pp.PipelineResult[]
            metadata = NamedTuple[]
            for (idx, item) in enumerate(batch_results.batch.items)
                item.spec.wave_id == string(row.wave_id) || continue
                string(item.metadata.scenario_name) == string(row.scenario_name) || continue
                string(item.spec.imputer_backend) == string(row.imputer_backend) || continue
                string(item.spec.linearizer_policy) == string(row.linearizer_policy) || continue
                push!(items, item)
                push!(results, batch_results.results[idx])
                push!(metadata, batch_results.metadata[idx])
            end
            pp.BatchRunResult(pp.StudyBatchSpec(items), results, metadata)
        end

        isempty(combo_results.results) && continue
        groupings = sort(unique(Symbol.(skipmissing(group_panel_rows[
            (String.(group_panel_rows.wave_id) .== string(row.wave_id)) .&
            (String.(group_panel_rows.scenario_name) .== string(row.scenario_name)),
            :grouping,
        ]))))
        heatmap_data = pp.pipeline_group_heatmap_values(
            combo_results;
            wave_id = row.wave_id,
            scenario_name = row.scenario_name,
            imputer_backend = Symbol(row.imputer_backend),
            measures = notebook_group_measures,
            groupings = groupings,
            statistic = :median,
        )

        compact = pp.select(
            heatmap_data.rows,
            :year,
            :wave_id,
            :scenario_name => :scenario,
            :m,
            :imputer_backend,
            :linearizer_policy,
            :grouping,
            :measure,
            :q50,
        )
        push!(heatmap_ready_tables, compact)
    end

    group_summary_table = isempty(heatmap_ready_tables) ?
        pp.DataFrame(
            year = Int[],
            wave_id = String[],
            scenario = String[],
            m = Int[],
            imputer_backend = String[],
            linearizer_policy = String[],
            grouping = Symbol[],
            measure = Symbol[],
            q50 = Float64[],
        ) :
        sorted_table(vcat(heatmap_ready_tables...; cols = :union))
end

# ╔═╡ 31179985-6b7b-447f-a627-b6a11844d737
md"""
### Heatmap-Ready Table

This compact table is the main grouped output of the notebook. It has one row
per year, scenario, `m`, grouping, and measure, with `q50` as the median value
used by the heatmap-oriented plot-data constructor.
"""

# ╔═╡ c81db0c1-8bcb-4f84-9b41-a555bcd4cc1a
small_table(group_summary_table; n = min(30, pp.nrow(group_summary_table)))

# ╔═╡ 3e0246fe-58e7-4c55-94ff-85e3474c818d
md"""
## Optional Small Plots

The tables above are the primary output. If `CairoMakie` is already loaded in
the notebook environment, the next cell writes small PNG checks under
`nb/output/notebook_smoke/notebook_plots`. If plotting is unavailable or
fails, the notebook keeps running and the CSV tables are still written.
"""

# ╔═╡ f0dc9bd9-175f-41df-a6cf-5b5912500048
function try_write_notebook_plots(global_df::pp.DataFrame, group_df::pp.DataFrame, plot_dir::AbstractString)
    rows = NamedTuple[]

    try
        if !isdefined(Main, :CairoMakie)
            push!(rows, (
                plot = "optional_plots",
                status = "skipped",
                path = "",
                message = "CairoMakie is not loaded. To enable plots, load CairoMakie in the notebook environment and rerun this cell.",
            ))
            return pp.DataFrame(rows)
        end

        mkpath(plot_dir)

        if !isempty(global_df)
            path = joinpath(plot_dir, "global_summary_by_m.png")
            fig = CairoMakie.Figure(size = (760, 420))
            ax = CairoMakie.Axis(fig[1, 1]; xlabel = "m", ylabel = "q50", title = "Global summaries")
            for subdf in pp.groupby(sort(global_df, [:measure, :m]), :measure)
                CairoMakie.lines!(ax, subdf.m, subdf.q50; label = String(subdf[1, :measure]))
                CairoMakie.scatter!(ax, subdf.m, subdf.q50)
            end
            CairoMakie.axislegend(ax; position = :rb)
            CairoMakie.save(path, fig; px_per_unit = 3)
            push!(rows, (plot = "global_summary_by_m", status = "success", path = path, message = ""))
        end

        if !isempty(group_df)
            path = joinpath(plot_dir, "group_summary_by_m.png")
            summary = pp.combine(
                pp.groupby(group_df, [:m, :measure]),
                :q50 => mean => :mean_q50,
            )
            fig = CairoMakie.Figure(size = (760, 420))
            ax = CairoMakie.Axis(fig[1, 1]; xlabel = "m", ylabel = "mean q50 across groupings", title = "Grouped summaries")
            for subdf in pp.groupby(sort(summary, [:measure, :m]), :measure)
                CairoMakie.lines!(ax, subdf.m, subdf.mean_q50; label = String(subdf[1, :measure]))
                CairoMakie.scatter!(ax, subdf.m, subdf.mean_q50)
            end
            CairoMakie.axislegend(ax; position = :rb)
            CairoMakie.save(path, fig; px_per_unit = 3)
            push!(rows, (plot = "group_summary_by_m", status = "success", path = path, message = ""))
        end
    catch err
        push!(rows, (
            plot = "optional_plots",
            status = "skipped",
            path = "",
            message = "CairoMakie plotting skipped: " * sprint(showerror, err) *
                      ". To enable plots, run this notebook in an environment where CairoMakie loads.",
        ))
    end

    return pp.DataFrame(rows)
end

# ╔═╡ 15d95ade-7bdb-4a3b-90ff-ea8f90788e13


# ╔═╡ 5b69ce9e-e173-4ed6-875d-41cdcb555d34
plot_status_table = try_write_notebook_plots(
    global_summary_table,
    group_summary_table,
    joinpath(settings.output_root, "notebook_plots"),
)

# ╔═╡ 2507eee6-961f-4e9d-be81-79bba975f9a8
small_table(plot_status_table; n = pp.nrow(plot_status_table))

# ╔═╡ 34b3996c-bf8b-433a-87b3-46fd5137ac8c
md"""
## Local Outputs

The notebook writes compact inspection CSVs under
`nb/output/notebook_smoke/notebook_tables`. These are notebook-local artifacts,
not publication outputs.
"""

# ╔═╡ e8bc70d6-0ba6-4836-9fc2-b8dc7ad09b3e
begin
    notebook_table_dir = joinpath(settings.output_root, "notebook_tables")
    global_summary_csv_path = write_notebook_csv(
        joinpath(notebook_table_dir, "global_summary_table.csv"),
        global_summary_table,
    )
    group_summary_csv_path = write_notebook_csv(
        joinpath(notebook_table_dir, "group_summary_table.csv"),
        group_summary_table,
    )
end

# ╔═╡ 4996dd6d-2b2f-41ba-a7f0-89ad330574dd
local_output_paths = pp.DataFrame(
    table = ["global summary", "group summary"],
    path = [global_summary_csv_path, group_summary_csv_path],
    rows = [pp.nrow(global_summary_table), pp.nrow(group_summary_table)],
)

# ╔═╡ d54b144d-63e5-4fa2-bf46-5b9a22fdf29a
small_table(local_output_paths; n = pp.nrow(local_output_paths))

# ╔═╡ 1c70a594-baad-4d53-ad5b-8ed5e2af6cf9
TableOfContents()

# ╔═╡ 6f4888f6-8ddf-448f-84f1-713a36cd1d10
begin
    summary_grouping_options = sort(unique(String.(skipmissing(group_summary_table.grouping))))
    @bind selected_grouping Select(summary_grouping_options)
end

# ╔═╡ 8a40fc31-79d9-4e24-b07a-39f462c03558
selected_group_summary_table = group_summary_table[String.(group_summary_table.grouping) .== selected_grouping, :]

# ╔═╡ 29f205ce-4c3a-45d9-a8bf-036b6761ee3c
small_table(selected_group_summary_table; n = min(20, pp.nrow(selected_group_summary_table)))

# ╔═╡ Cell order:
# ╠═1b48c92a-5d58-4db7-9f55-f92c1a52a1f2
# ╠═87c29c6b-04d2-4227-9b5c-ce14d610894e
# ╟─8ec3853a-a893-456a-b254-a3171433054b
# ╠═11efc93a-5930-4478-b110-f8c042d984d4
# ╠═2cd34337-ae3e-4d7a-8611-c7724e10ff4f
# ╠═5f3b0611-978f-4430-8043-95a681ef5302
# ╠═d50d563d-c9c5-4b29-aeca-76f1a5919e85
# ╠═09e68af2-41fd-45a2-b0bf-efafbd12c484
# ╠═29ba8b54-a01b-4a90-8646-5feb394a2f82
# ╟─60a66ff2-c61b-4949-80f3-b4065b25cad3
# ╠═01b5e188-5b19-4b7f-9398-9b4750aa6694
# ╠═1399cbd4-b08a-4464-9843-6f66f1581cec
# ╠═a5e6d5ac-a579-44dc-9036-3d28ac354936
# ╟─a25da9b6-96ff-4b63-bd54-4a9f91428a11
# ╠═6ba53d87-2161-4a78-84d8-c63316cc6eca
# ╠═a0265370-9bd1-48e1-8a16-7d35632c4aa3
# ╠═11656802-f99b-466d-8d72-2a338e58d76a
# ╠═5f01469a-3f1d-4567-ab8e-796c7043de25
# ╠═fb87d454-a9e8-4be9-a3d9-f870dc089dfc
# ╠═af12e60d-cee1-4b4b-9e07-72501d72473d
# ╟─d6bd892b-dcb8-4646-87e5-4d5a1b375d38
# ╠═2fddc636-dba3-4994-a233-3621d25140d5
# ╟─80c5f12d-7c5e-438d-852c-ac5c08f152ea
# ╠═722e21cc-2030-4873-bc4f-5519dfe71f81
# ╠═be5320e0-a27f-4511-a812-f790f5fc4b71
# ╠═4127132e-6657-4d1b-bcad-9fbc3206196b
# ╟─31179985-6b7b-447f-a627-b6a11844d737
# ╠═c81db0c1-8bcb-4f84-9b41-a555bcd4cc1a
# ╟─3e0246fe-58e7-4c55-94ff-85e3474c818d
# ╠═f0dc9bd9-175f-41df-a6cf-5b5912500048
# ╠═15d95ade-7bdb-4a3b-90ff-ea8f90788e13
# ╠═5b69ce9e-e173-4ed6-875d-41cdcb555d34
# ╠═2507eee6-961f-4e9d-be81-79bba975f9a8
# ╟─34b3996c-bf8b-433a-87b3-46fd5137ac8c
# ╠═e8bc70d6-0ba6-4836-9fc2-b8dc7ad09b3e
# ╠═4996dd6d-2b2f-41ba-a7f0-89ad330574dd
# ╠═d54b144d-63e5-4fa2-bf46-5b9a22fdf29a
# ╠═1c70a594-baad-4d53-ad5b-8ed5e2af6cf9
# ╠═6f4888f6-8ddf-448f-84f1-713a36cd1d10
# ╠═8a40fc31-79d9-4e24-b07a-39f462c03558
# ╠═29f205ce-4c3a-45d9-a8bf-036b6761ee3c
