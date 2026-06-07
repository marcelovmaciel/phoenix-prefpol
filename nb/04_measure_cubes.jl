### A Pluto.jl notebook ###
# v0.20.17

using Markdown

# ╔═╡ 7c3d90a4-f6bd-4a55-96c5-7d7a2c32d43f
begin
    import Pkg
    Pkg.activate(@__DIR__)
end

# ╔═╡ 7fe45a66-0cae-4b1f-a6a8-08c1db4e75cf
md"""
# Measure Cubes

The paper no longer uses `S` and `O` as main measures. The notebook main
measure set is `Psi`, `R`, `HHI`, `RHHI`, `C`, and `D`, matching the
publication-facing configuration.

`Psi`, `R`, `HHI`, and `RHHI` are **global measures**: each row summarizes the
whole strict preference profile for one stochastic leaf `(b, r, k)`, so
`grouping` is missing. `C` and `D` are **grouped measures**: each row is tied to
one configured demographic grouping, so `grouping` is present.

This corresponds to the production measure-computation pass: both instantiate a
`NestedStochasticPipeline` and call `PrefPol.ensure_measures!`. The notebook
uses `nb/notebook_config.toml`, avoids CLI shelling, and writes only notebook
inspection CSVs under `nb/output/notebook_smoke/notebook_tables`.
"""

# ╔═╡ c6dd4249-8a59-4875-b1ab-1d4d47a24b41
begin
    # Load shared notebook helpers and the local PrefPol package.
    include(joinpath(@__DIR__, "notebook_common.jl"))
end

# ╔═╡ 3dcf8706-0880-4dc7-9de5-7eff33fa3db4
begin
    # Load the notebook-scale orchestration config.
    cfg = load_notebook_config()
    settings = notebook_settings(cfg)
    targets = notebook_targets(cfg)
end

# ╔═╡ a792d836-9ed8-449c-a461-8ec8c75fb472
begin
    notebook_main_measures = [:Psi, :R, :HHI, :RHHI, :C, :D]
    @assert settings.measures == notebook_main_measures "Notebook measure path is Psi, R, HHI, RHHI, C, D."
    @assert settings.B <= 5 "Notebook config keeps B <= 5."
    @assert settings.R <= 5 "Notebook config keeps R <= 5."
    @assert settings.K <= 5 "Notebook config keeps K <= 5."
    ensure_not_publication_output!(settings.output_root)
    ensure_not_publication_output!(settings.cache_root)
end

# ╔═╡ 9439e72e-6282-4ae7-bd3b-1a934cb5f2dd
begin
    # Load survey-wave configs and construct the notebook batch.
    waves, source_registry, wave_by_id = load_notebook_waves()
    batch = build_notebook_batch(cfg)
end

# ╔═╡ 363f7b4f-58b6-432c-b9d6-4c697a0239eb
begin
    # The first batch item is the default tiny example.
    selected_index = 1
    selected_item = batch.items[selected_index]
    selected_spec = selected_item.spec
    selected_batch = pp.StudyBatchSpec([selected_item])
end

# ╔═╡ f7891308-c89f-42af-99f4-ac3d8d4b9b2a
selected_spec_table = DataFrame(
    item = [
        "batch index",
        "wave",
        "scenario",
        "m",
        "active candidates",
        "groupings",
        "measures",
        "B",
        "R",
        "K",
        "imputer backend",
        "linearizer policy",
        "cache root",
        "output root",
    ],
    value = [
        string(selected_index),
        selected_spec.wave_id,
        selected_item.metadata.scenario_name,
        string(selected_item.metadata.m),
        join(selected_spec.active_candidates, ", "),
        join(String.(selected_spec.groupings), ", "),
        join(String.(selected_spec.measures), ", "),
        string(selected_spec.B),
        string(selected_spec.R),
        string(selected_spec.K),
        String(selected_spec.imputer_backend),
        String(selected_spec.linearizer_policy),
        settings.cache_root,
        settings.output_root,
    ],
)

# ╔═╡ c36606b5-f7e9-42fe-bc08-96a2875cf3cc
small_table(selected_spec_table; n = nrow(selected_spec_table))

# ╔═╡ e55c7603-d62d-4ea6-8121-bbd077e69df0
begin
    # Instantiate the pipeline object used by the production workflow.
    pipeline = pp.NestedStochasticPipeline(source_registry; cache_root = settings.cache_root)
    cache_dir = pp.pipeline_cache_dir(pipeline, selected_spec)
end

# ╔═╡ eda029f2-3433-46f4-866c-0f67815c05c7
md"""
## Upstream Assurance

`PrefPol.ensure_measures!` internally calls `PrefPol.ensure_linearizations!`,
and `ensure_linearizations!` ensures its own upstream imputation and resampling
artifacts. The cells below call `ensure_resamples!`, `ensure_imputations!`, and
`ensure_linearizations!` explicitly anyway, so the notebook makes clear which
upstream stages are reused from the notebook cache or generated for the
selected tiny spec.
"""

# ╔═╡ cff60ea3-a88c-44f9-9687-e8e7054c13b8
begin
    resample_manifest_raw = pp.ensure_resamples!(
        pipeline,
        selected_spec;
        force = settings.force,
    )
    imputation_manifest_raw = pp.ensure_imputations!(
        pipeline,
        selected_spec;
        force = settings.force,
    )
    linearization_manifest_raw = pp.ensure_linearizations!(
        pipeline,
        selected_spec;
        force = settings.force,
    )
end

# ╔═╡ 4e254dd3-9b28-4840-9b07-2da39146511c
upstream_counts = DataFrame(
    stage = ["resample", "imputed", "linearized"],
    rows = [
        count(==(:resample), resample_manifest_raw.stage),
        count(==(:imputed), imputation_manifest_raw.stage),
        count(==(:linearized), linearization_manifest_raw.stage),
    ],
)

# ╔═╡ 267fd915-1f31-4dc7-9c36-02b11a245857
small_table(upstream_counts; n = nrow(upstream_counts))

# ╔═╡ e992a412-7f1e-4f8d-b1d4-52864ae435e8
md"""
## Measure Computation

`PrefPol.ensure_measures!` computes the configured measures for each
linearized leaf. It returns a `PipelineResult` containing the selected spec,
the spec-specific cache directory, the full stage manifest, the raw
`measure_cube`, pooled summaries, and variance decomposition objects.
"""

# ╔═╡ f71127af-e12c-4d5d-80df-feeef2a6166a
result = pp.ensure_measures!(
    pipeline,
    selected_spec;
    force = settings.force,
    progress = false,
)

# ╔═╡ 04de3092-d697-462a-a96a-1ad9897097cb
result_fields = DataFrame(
    field = [
        "spec",
        "cache_dir",
        "stage_manifest rows",
        "measure_cube rows",
    ],
    value = [
        "wave=$(result.spec.wave_id), m=$(length(result.spec.active_candidates)), B=$(result.spec.B), R=$(result.spec.R), K=$(result.spec.K)",
        result.cache_dir,
        string(nrow(result.stage_manifest)),
        string(nrow(result.measure_cube)),
    ],
)

# ╔═╡ 7b9a2c58-592e-4705-bf31-b047d3788fb6
small_table(result_fields; n = nrow(result_fields))

# ╔═╡ 83f864ef-04c8-43b5-9bdb-eaa18950b8a4
begin
    measure_cube_sample = sorted_table(select(
        result.measure_cube,
        intersect(
            [:b, :r, :k, :measure, :grouping, :value, :value_lo, :value_hi],
            propertynames(result.measure_cube),
        ),
    ))
    global_measure_rows = measure_cube_sample[ismissing.(measure_cube_sample.grouping), :]
    grouped_measure_rows = measure_cube_sample[.!ismissing.(measure_cube_sample.grouping), :]
end

# ╔═╡ a4e19da0-d2f4-4af3-8e40-8f0183df4003
md"""
### Global Rows

Global rows have missing `grouping` values because they summarize the full
profile rather than a demographic partition.
"""

# ╔═╡ 80e03543-a3c9-4c43-98f2-354571eab264
small_table(global_measure_rows; n = min(16, nrow(global_measure_rows)))

# ╔═╡ b079863d-6d96-4640-8843-1a7a3e9ee4055
md"""
### Grouped Rows

Grouped rows have present `grouping` values because each value is computed
against a configured demographic partition.
"""

# ╔═╡ 696895c3-1463-48f4-86c7-25c1c847d0f6
small_table(grouped_measure_rows; n = min(20, nrow(grouped_measure_rows)))

# ╔═╡ decc0ce4-7e72-44fc-82ff-7243ad2d32c2
begin
    panel_table = sorted_table(select(
        pp.pipeline_panel_table(result, selected_item.metadata),
        intersect(
            [
                :measure,
                :grouping,
                :n_draws,
                :mean_value,
                :q50,
                :q25,
                :q75,
                :estimate,
                :total_sd,
                :wave_id,
                :scenario_name,
                :m,
            ],
            propertynames(pp.pipeline_panel_table(result, selected_item.metadata)),
        ),
    ))
    global_measure_summary = panel_table[ismissing.(panel_table.grouping), :]
    group_measure_summary = panel_table[.!ismissing.(panel_table.grouping), :]
end

# ╔═╡ 3e88717b-b71b-4355-b197-c449ff651dbf
md"""
### Mean and Median Summaries

The table below comes from `PrefPol.pipeline_panel_table`, so median-like
quantiles and means are computed by existing PrefPol reporting APIs rather than
reimplemented in this notebook.
"""

# ╔═╡ 25355ff5-b7c7-430a-818f-0fbc415f680a
small_table(global_measure_summary; n = nrow(global_measure_summary))

# ╔═╡ 1222fe1e-dd42-4387-a295-b6df2a38be19
small_table(group_measure_summary; n = min(20, nrow(group_measure_summary)))

# ╔═╡ 1ad6bb0b-068a-49e0-aea3-e80244a943f1
md"""
## Conceptual Inspection

`Psi` is pairwise polarization: it summarizes how much pairwise ranking
disagreement appears in the strict profile.

`R` is exact reversal opposition: it captures mass in pairs of voters whose
rankings are exact reversals.

`HHI` and `RHHI` describe concentration among reversal pairs. `HHI` is the
concentration measure over reversal-pair mass, while `RHHI` combines reversal
mass with that concentration.

`C` is within-group coherence: it measures how tightly group members align
around their own group consensus.

`D` is external divergence from other-group consensuses: it measures how far
each group sits from the consensuses of the other groups.
"""

# ╔═╡ 33945f1b-6cc5-405b-a039-9e6d7d4552a2
md"""
## Local Outputs

The notebook writes compact inspection CSVs under
`nb/output/notebook_smoke/notebook_tables`. These are notebook-local outputs,
not publication outputs.
"""

# ╔═╡ 5735a929-e44d-42ce-b3e4-9601b6f53487
begin
    notebook_table_dir = joinpath(settings.output_root, "notebook_tables")
    sample_csv_path = write_notebook_csv(
        joinpath(notebook_table_dir, "measure_cube_sample.csv"),
        measure_cube_sample,
    )
    global_summary_csv_path = write_notebook_csv(
        joinpath(notebook_table_dir, "global_measure_summary.csv"),
        global_measure_summary,
    )
    group_summary_csv_path = write_notebook_csv(
        joinpath(notebook_table_dir, "group_measure_summary.csv"),
        group_measure_summary,
    )
end

# ╔═╡ b85fb599-00d9-4ec2-aef0-88ef1da87d9e
local_output_paths = DataFrame(
    table = [
        "measure cube sample",
        "global measure summary",
        "group measure summary",
    ],
    path = [
        sample_csv_path,
        global_summary_csv_path,
        group_summary_csv_path,
    ],
    rows = [
        nrow(measure_cube_sample),
        nrow(global_measure_summary),
        nrow(group_measure_summary),
    ],
)

# ╔═╡ b9ec8997-34e7-44d8-8eba-3cb596f30d0b
small_table(local_output_paths; n = nrow(local_output_paths))

# ╔═╡ Cell order:
# ╠═7c3d90a4-f6bd-4a55-96c5-7d7a2c32d43f
# ╟─7fe45a66-0cae-4b1f-a6a8-08c1db4e75cf
# ╠═c6dd4249-8a59-4875-b1ab-1d4d47a24b41
# ╠═3dcf8706-0880-4dc7-9de5-7eff33fa3db4
# ╠═a792d836-9ed8-449c-a461-8ec8c75fb472
# ╠═9439e72e-6282-4ae7-bd3b-1a934cb5f2dd
# ╠═363f7b4f-58b6-432c-b9d6-4c697a0239eb
# ╠═f7891308-c89f-42af-99f4-ac3d8d4b9b2a
# ╠═c36606b5-f7e9-42fe-bc08-96a2875cf3cc
# ╠═e55c7603-d62d-4ea6-8121-bbd077e69df0
# ╟─eda029f2-3433-46f4-866c-0f67815c05c7
# ╠═cff60ea3-a88c-44f9-9687-e8e7054c13b8
# ╠═4e254dd3-9b28-4840-9b07-2da39146511c
# ╠═267fd915-1f31-4dc7-9c36-02b11a245857
# ╟─e992a412-7f1e-4f8d-b1d4-52864ae435e8
# ╠═f71127af-e12c-4d5d-80df-feeef2a6166a
# ╠═04de3092-d697-462a-a96a-1ad9897097cb
# ╠═7b9a2c58-592e-4705-bf31-b047d3788fb6
# ╠═83f864ef-04c8-43b5-9bdb-eaa18950b8a4
# ╟─a4e19da0-d2f4-4af3-8e40-8f0183df4003
# ╠═80e03543-a3c9-4c43-98f2-354571eab264
# ╟─b079863d-6d96-4640-8843-1a7a3e9ee4055
# ╠═696895c3-1463-48f4-86c7-25c1c847d0f6
# ╠═decc0ce4-7e72-44fc-82ff-7243ad2d32c2
# ╟─3e88717b-b71b-4355-b197-c449ff651dbf
# ╠═25355ff5-b7c7-430a-818f-0fbc415f680a
# ╠═1222fe1e-dd42-4387-a295-b6df2a38be19
# ╟─1ad6bb0b-068a-49e0-aea3-e80244a943f1
# ╟─33945f1b-6cc5-405b-a039-9e6d7d4552a2
# ╠═5735a929-e44d-42ce-b3e4-9601b6f53487
# ╠═b85fb599-00d9-4ec2-aef0-88ef1da87d9e
# ╠═b9ec8997-34e7-44d8-8eba-3cb596f30d0b
