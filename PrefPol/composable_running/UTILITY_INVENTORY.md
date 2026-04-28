# Phase 2 Utility Inventory

This file is the Phase 2 implementation artifact for the composable running
refactor. It identifies the config, path, result, and manifest utilities that
already exist in `PrefPol/src/`, and records what later stage scripts should
reuse instead of copying from `PrefPol/running/`.

Phase 2 does not add stage-only pipeline APIs. Those belong to Phase 3.

## Existing Public Utilities To Reuse

The following functions and types are exported by `PrefPol` and are safe for
`PrefPol/composable_running/stages/*.jl` scripts to call directly.

### Config and Wave Loading

- `load_survey_wave_config(path; wave_id = nothing)`
- `build_source_registry(waves)`
- `SurveyWaveConfig`

Use these for year TOMLs such as `PrefPol/config/2006.toml`. Do not duplicate
TOML parsing in stage scripts unless the file is an orchestration-level TOML
that has no loader yet.

### Candidate and Pipeline Spec Construction

- `resolve_active_candidate_set(wcfg; active_candidates, scenario_name, m, data)`
- `build_pipeline_spec(wcfg; ...)`
- `PipelineSpec`
- `NestedStochasticPipeline`
- `StudyBatchItem`
- `StudyBatchSpec`
- `BatchRunner`
- `BatchRunResult`

Use these to translate orchestration targets into executable nested-pipeline
specs. Do not rebuild candidate filtering or scenario lookup logic in
`composable_running/`.

### Pipeline Execution and Result IO

- `run_pipeline(pipeline, spec; force = false, progress = true)`
- `run_batch(runner, batch; force = false, progress = true)`
- `load_pipeline_result(path)`
- `load_pipeline_result(pipeline, spec)`
- `save_pipeline_result(path, result; overwrite = false)`

Current limitation: `run_pipeline` is still all-in-one. Independent bootstrap,
imputation, linearization, and measure stage runners require the Phase 3
`ensure_*` APIs described in `PLAN.md`.

### Result Tables

- `pipeline_measure_table(result_or_results)`
- `pipeline_summary_table(result_or_results)`
- `pipeline_variance_decomposition_table(result_or_results)`
- `pipeline_panel_table(result_or_results)`
- `decomposition_table(decomposition)`
- `tree_variance_decomposition_table(...)`
- `save_pipeline_variance_decomposition_csv(path, result_or_results)`

Use these for measure, summary, panel, and decomposition outputs. Avoid copying
the table-building loops from `PrefPol/running/run_all_scenarios_small.jl`.

### Plot and Heatmap Data

- `pipeline_scenario_plot_data(...)`
- `pipeline_group_plot_data(...)`
- `pipeline_group_heatmap_values(...)`
- `select_pipeline_panel_rows(...)`
- `pipeline_candidate_label(...)`

Use these to prepare data for plotting stages. Rendering still belongs to the
CairoMakie extension.

### Plotting Extension Wrappers

These are exported through `PrefPol` but require `using CairoMakie` in the
active environment:

- `plot_pipeline_scenario(...)`
- `plot_pipeline_group_lines(...)`
- `plot_pipeline_group_heatmap(...)`
- `save_pipeline_plot(...)`

More specialized paper heatmaps live in `PrefPol/ext/PrefPolPlottingExt.jl`.
If stage scripts need reusable table preparation for those heatmaps, promote
that helper to `PrefPol/src/` or the extension during Phase 3 or Phase 7.

### Lambda and Variance Helpers

- `augment_pipeline_result_with_lambda_sep(result; include_w = false)`
- `compute_group_measure_details(...)`
- variance decomposition report/table/plot helpers exported from
  `variance_decomposition_report.jl`

Use the existing Lambda implementation. It defines `lambda_sep` as aggregate
`D / W`, not as a mean of group-level ratios.

## Existing Private Utilities

These are useful implementation facts but should not be called from
`composable_running/` until promoted in Phase 3:

- `_stage_path(cache_dir, stage; b = 0, r = 0, k = 0)`
- `_save_stage_artifact(cache_dir, stage, artifact; b = 0, r = 0, k = 0)`
- `_manifest_dataframe(rows)`
- `_audit_dataframe(rows)`
- `_write_table_csv(path, df)`

They confirm the current cache layout and CSV-writing behavior, but scripts
should stay on the public `PrefPol` API boundary.

## CSV and Manifest Policy

`PrefPol/Project.toml` already depends on `CSV.jl` and `DataFrames.jl`. New
stage scripts should use:

```julia
using CSV
using DataFrames

CSV.read(path, DataFrame)
CSV.write(path, df)
```

Do not copy the hand-written `csv_escape`, `_parse_csv_line`, `read_csv_table`,
or `save_csv` helpers from the old scripts. Those duplicated helpers currently
appear in scripts such as:

- `PrefPol/running/run_all_scenarios_small.jl`
- `PrefPol/running/plot_all_scenarios_global_small.jl`
- `PrefPol/running/effective_counts_small.jl`

If later stages need a manifest-specific wrapper, add one small public helper
to `PrefPol/src/` in Phase 3. Until then, stage scripts should call `CSV.jl`
directly and keep manifest schemas explicit in each stage.

## Path Policy

Existing public APIs preserve these cache/result paths:

```text
cache/<spec_hash>/
  spec.jld2
  observed.jld2
  resample/b0001.jld2
  imputed/b0001_r0001.jld2
  linearized/b0001_r0001_k0001.jld2
  measure/b0001_r0001_k0001.jld2
  result.jld2
```

New orchestration outputs should stay under
`PrefPol/composable_running/output/`. Stage scripts should create parent
directories with `mkpath(dirname(path))` immediately before writing files, not
at module load time.

Avoid adding `PrefPol/composable_running/lib/`. If a path or manifest helper
becomes shared by multiple scripts, promote it to `PrefPol/src/`.

## Missing Utilities Deferred To Phase 3

The following are not currently public and should be implemented in Phase 3
before independent stage scripts are made fully separable:

- `pipeline_cache_dir(pipeline, spec)`
- `pipeline_stage_paths(pipeline, spec)`
- `load_stage_artifact(path)`
- `ensure_observed!(pipeline, spec; force = false)`
- `ensure_resamples!(pipeline, spec; force = false)`
- `ensure_imputations!(pipeline, spec; force = false)`
- `ensure_linearizations!(pipeline, spec; force = false)`
- `ensure_measures!(pipeline, spec; force = false)`
- optional manifest reader/writer helpers if direct `CSV.jl` calls become
  repetitive

