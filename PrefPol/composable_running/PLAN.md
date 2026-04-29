# Plan: Composable Running Orchestration

This is a planning document only. Do not implement the refactor from this file
without a separate implementation task.

The current operational source of truth is the collection of small scripts in
`PrefPol/running/`, plus the paper artifact usage in root `writing/`. Do not
treat `PrefPol/running/running.jl` as the current main workflow. It is useful
legacy context, but the active workflow is distributed across the smaller
scripts.

## Goal

Create a new orchestration-only folder:

```text
PrefPol/composable_running/
```

The new scripts should be small, stage-specific, explicitly ordered, callable
independently, config-driven, and predictable about outputs. They should call
existing PrefPol functions from `PrefPol/src/` and the CairoMakie plotting
extension. They should not become a new internal library or framework.

Do not create:

```text
PrefPol/composable_running/lib/
```

If reusable logic is needed, add it to `PrefPol/src/` or
`PrefPol/ext/PrefPolPlottingExt.jl`, then keep the orchestration scripts thin.

## Proposed Folder Layout

```text
PrefPol/composable_running/
  PLAN.md
  README.md
  run_all_smoke.jl
  run_all_paper.jl
  stages/
    00_validate_configs.jl
    01_bootstrap.jl
    02_impute.jl
    03_linearize.jl
    04_measures.jl
    05_plot_global.jl
    06_plot_group.jl
    07_extra_measures.jl
    08_extra_plots.jl
    09_tables.jl
    10_lambda_table.jl
    11_collect_paper_artifacts.jl
  output/
    bootstraps/
    imputations/
    linearizations/
    measures/
    extra_measures/
    plots/
    extra_plots/
    tables/
    appendices/
    paper_artifacts/
    manifests/
    logs/
```

`PLAN.md` is the only file created by this task. Later agents should create the
remaining files in phases.

## Repository Inventory

### `PrefPol/config/`

Files:

- `2006.toml`
- `2018.toml`
- `2022.toml`

Current fields:

- `year`
- `data_loader`
- `data_file`
- `max_candidates`
- `m_values_range`
- `n_bootstrap`
- `n_alternatives`
- `force_include`
- `rng_seed`
- `candidates`
- `demographics`
- `forced_scenarios`

Current configured paper scenarios:

- 2006: `main_2006`
- 2018: `main_2018`
- 2022: `main_2022`, plus `no_forcing` as a diagnostic scenario

Current paper-facing m range:

- `m = 2:5` in all three year TOMLs

Values not currently captured in year TOMLs:

- `R`
- `K`
- cache root
- output root
- force/cache behavior
- dry-run behavior
- imputer backend list
- linearizer policy list
- consensus tie policy
- analysis roles
- plot specs
- table specs
- artifact collection specs

### `PrefPol/running/`

Current operational scripts:

| File | Role | Current status |
|---|---|---|
| `run_all_scenarios_small.jl` | Current full small nested analysis runner. Builds `StudyBatchSpec`s, calls `NestedStochasticPipeline`, writes root/per-scenario tables and manifests. | Too large; decompose into stage scripts and move reusable helpers to `PrefPol/src/`. |
| `plot_all_scenarios_global_small.jl` | Reads saved `run_manifest.csv` and cached `PipelineResult`s; writes global plots and compact tables. | Good stage candidate, but should read plot targets from config. |
| `plot_all_scenarios_group_small.jl` | Reads saved results; writes paper group heatmaps and O-smoothed heatmaps/tables. | Good stage candidate, but should move table-prep helpers to `PrefPol/src/` or plotting extension. |
| `plot_2022_no_forcing_global_small.jl` | Thin wrapper around global plotting for 2022 `no_forcing`. | Replace with CLI/config filters. |
| `ranking_support_diagnostics_small.jl` | Post-processes cached linearized artifacts into ranking-support diagnostics. | Extra-measures stage. |
| `effective_counts_small.jl` | Post-processes cached linearized artifacts into effective count diagnostics. | Extra-measures stage. |
| `effective_rankings_evolution_tables.jl` | Builds effective-ranking CSV/Markdown/TeX tables for selected scenarios. | Tables stage; paper-facing appendix/main support. |
| `plot_effective_rankings_evolution.jl` | Plots EO/ENRP evolution from effective-ranking tables. | Extra-plots stage. |
| `appendix_lambda_table.jl` | Builds appendix Lambda table and audit from cached grouped C/D/W/lambda rows. | Lambda table stage. |
| `appendix_lambda_grouping_tables.jl` | Reformats Lambda CSV into per-year LaTeX grouping tables. | Lambda table or appendix tables stage. |
| `augment_grouped_S_cache.jl` | One-off cache migration to append grouped S to old cached results. | Do not port as normal stage; keep as migration utility or move API use into `PrefPol/src/` tests. |
| `clear_running_caches.jl` | Deletes targeted generated outputs. | Replace with config-driven `--force` and documented cache cleanup; keep deletion outside normal DAG. |
| `plotting_setup.jl` | Plotting environment bootstrap for `PrefPol/running/plotting_env`. | Reuse pattern or document plotting environment; avoid duplicating as a library. |
| `running.jl` | Legacy broad operational script. | Background only. |
| `scenario_refactor_inventory_before.md` | Historical inventory notes. | Documentation context. |
| `scenario_refactor_report.md` | Paper scenario/refactor notes. | Paper requirements context. |

### `PrefPol/src/nested_pipeline.jl`

Important exported concepts:

- `SurveyWaveConfig`
- `PipelineSpec`
- `NestedStochasticPipeline`
- `ObservedData`
- `Resample`
- `ImputedData`
- `LinearizedProfile`
- `MeasureResult`
- `PipelineResult`
- `StudyBatchItem`
- `StudyBatchSpec`
- `BatchRunner`
- `BatchRunResult`

Important exported functions:

- `load_survey_wave_config`
- `build_source_registry`
- `resolve_active_candidate_set`
- `build_pipeline_spec`
- `load_observed_data`
- `compute_group_measure_details`
- `run_pipeline`
- `run_batch`
- `load_pipeline_result`
- `save_pipeline_result`
- `augment_pipeline_result_with_lambda_sep`
- `pipeline_measure_table`
- `pipeline_summary_table`
- `pipeline_panel_table`
- `pipeline_group_plot_data`
- `pipeline_group_heatmap_values`
- `decomposition_table`

Important private stage functions:

- `_draw_resample`
- `_impute_resample`
- `_linearize_imputed`
- `_measure_results_for_profile`
- `_save_stage_artifact`
- `_stage_path`

Key finding: `run_pipeline` currently performs the entire stage sequence in one
call:

```text
spec -> observed -> resample -> impute -> linearize -> measure -> aggregate PipelineResult
```

It writes per-stage JLD2 artifacts and a `stage_manifest`, but the public API
does not expose stage-only runners. Later stages can inspect or reuse completed
artifacts through `PipelineResult.stage_manifest`, but they cannot cleanly run
only "bootstrap", only "impute", only "linearize", or only "measures" through a
public API.

Answer to the architectural question:

- The current nested pipeline is internally staged, and the cache contains
  separate stage artifacts.
- The current public API is not stage-separable enough for independent
  stage-running scripts.
- Minimal API additions are needed in `PrefPol/src/nested_pipeline.jl`.
- Do not rewrite the statistical pipeline.

Minimal API additions to propose:

- `pipeline_cache_dir(pipeline, spec) -> String`
- `pipeline_stage_paths(pipeline, spec) -> NamedTuple` or a DataFrame
- `ensure_observed!(pipeline, spec; force=false) -> ObservedData`
- `ensure_resamples!(pipeline, spec; force=false) -> DataFrame manifest`
- `ensure_imputations!(pipeline, spec; force=false) -> DataFrame manifest`
- `ensure_linearizations!(pipeline, spec; force=false) -> DataFrame manifest`
- `ensure_measures!(pipeline, spec; force=false) -> PipelineResult`
- `load_stage_artifact(path) -> artifact`
- optional `rebuild_pipeline_result_from_stage_cache(pipeline, spec) -> PipelineResult`

These should reuse the existing private logic and file layout rather than
creating a parallel cache format.

### `PrefPol/src/pipeline.jl`

This file contains older pipeline APIs and legacy helpers. Several functions
explicitly warn that newer code should use `SurveyWaveConfig`,
`NestedStochasticPipeline`, `StudyBatchSpec`, `run_pipeline`, and `run_batch`.
The composable refactor should avoid building new stages on the old
`save_all_bootstraps`, `impute_from_f3`, `generate_profiles_for_year_*`, and
`save_or_load_*` APIs unless a specific legacy artifact still depends on them.

Still useful:

- `load_election_cfg`
- `ElectionConfig`
- `describe_candidate_set`
- plotting shims are legacy wrappers around newer nested plotting functions

### `PrefPol/src/PrefPol.jl`

This is the public export boundary. New reusable orchestration helpers should
be exported here if intended for scripts. Avoid calling private underscored
functions from `composable_running/` unless the implementation task first
promotes them to public APIs.

### `PrefPol/ext/PrefPolPlottingExt.jl`

Current plotting extension includes:

- `plot_pipeline_scenario`
- `plot_pipeline_group_lines`
- `plot_pipeline_group_heatmap`
- `plot_pipeline_group_triplet_panel`
- `plot_pipeline_group_paper_heatmap`
- `plot_pipeline_group_paper_osmoothed_heatmap`
- variance decomposition plotting functions
- `save_pipeline_plot`

Reusable plot-table preparation and paper heatmap conventions should be moved
here or to `PrefPol/src/` if they are needed by multiple scripts.

### Writing and Artifact Needs

The prompt mentions `PrefPol/writing/`, but this checkout has paper files in
root `writing/`, not `PrefPol/writing/`.

Important paper references from `writing/main.tex`:

- `writing/imgs/2006_global_main.png`
- `writing/imgs/2018_global_main.png`
- `writing/imgs/2022_global_main.png`
- `writing/imgs/effective_rankings_evolution_1x2.png`
- `writing/imgs/2006_group.png`
- `writing/imgs/2018_group.png`
- `writing/imgs/2022_group.png`
- `writing/imgs/variance_decomposition_2022.png`
- `writing/imgs/effective_rankings.tex`
- `writing/imgs/appendix_lambda_grouping_tables.tex`

The final collection stage should copy or symlink generated artifacts into:

```text
PrefPol/composable_running/output/paper_artifacts/
```

and optionally into `writing/imgs/` only when a config flag explicitly asks for
that destination.

### Existing Generated Output Roots

Current small-run outputs live under:

```text
PrefPol/running/output/all_scenarios_small/
```

Current nested cache layout includes:

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

This should be preserved or wrapped, not replaced, unless a later API change
has a strong reason to migrate it.

## Current Script Classification Details

### `run_all_scenarios_small.jl`

Reads:

- `PrefPol/config/*.toml`
- raw data files through configured loaders
- existing `run_manifest.csv` when preserving non-target rows
- cached `PipelineResult`s

Writes:

- `PrefPol/running/output/all_scenarios_small/run_manifest.csv`
- root `measure_table.csv`, `summary_table.csv`, `panel_table.csv`,
  `decomposition_table.csv`, `effective_numbers_table.csv`,
  `effective_numbers_summary_table.csv`
- per-scenario and per-role copies
- per-spec decomposition CSVs under `specs/`
- nested JLD2 stage artifacts under `cache/`

Years/scenarios:

- Current constants target `(2018, main_2018)` only in the checked-in script.
- Historical/report context and generated outputs show paper runs for
  `(2006, main_2006)`, `(2018, main_2018)`, `(2022, main_2022)`, plus
  `(2022, no_forcing)` diagnostic.

m ranges:

- Main paper: `2:5`
- O-smoothed extension: `max_candidates`, currently `5`
- Ranking-support diagnostic: supported values outside main/extension if role
  enabled

Backends:

- `:random`
- `:mice`

Linearizers:

- `:random_ties`
- `:pattern_conditional`

Measures:

- Main: `:Psi`, `:R`, `:HHI`, `:RHHI`, `:C`, `:D`, `:O`, `:S`
- O-smoothed extension: `:O_smoothed`
- Ranking support diagnostic role: `:Psi`

Groupings:

- Configured `demographics` from each wave TOML unless overridden by
  `TARGET_GROUPINGS_BY_WAVE`

Hard-coded values to move to config:

- `B_SMALL`, `R_SMALL`, `K_SMALL`
- `TARGETS`
- `TARGET_GROUPINGS_BY_WAVE`
- `TARGET_ANALYSIS_ROLES`
- `CONSENSUS_TIE_POLICY`
- `MAIN_PAPER_MEASURES`
- `O_SMOOTHED_EXTENSION_MEASURES`
- `RANKING_SUPPORT_DIAGNOSTIC_MEASURES`
- `MAIN_PAPER_M_VALUES`
- `DIAGNOSTIC_SCENARIOS`
- `BACKEND_COMBINATIONS`
- output/cache roots

Functions called:

- `load_survey_wave_config`
- `build_source_registry`
- `build_pipeline_spec`
- `NestedStochasticPipeline`
- `BatchRunner`
- `run_batch`
- `pipeline_measure_table`
- `pipeline_summary_table`
- `pipeline_panel_table`
- `decomposition_table`
- `load_pipeline_result`
- `describe_candidate_set`
- `dataframe_to_annotated_profile`
- Preferences diagnostics for effective counts

Decomposition:

- Should be split. Building specs, running stages, writing root tables,
  effective-number post-processing, manifest merging, and CSV helpers should
  not all live in one script.

### `plot_all_scenarios_global_small.jl`

Reads:

- `run_manifest.csv`
- cached `PipelineResult.result.jld2`

Writes:

- `global/<wave>/<scenario>/*.png`
- `summary_table.csv`
- `panel_table.csv`
- `decomposition_table.csv`

Years/scenarios:

- Defaults to all main-role targets discovered from manifest unless `TARGETS`
  is defined by wrapper.

m ranges:

- Whatever rows are in manifest, usually `2:5`.

Backends/linearizers:

- Discovered from manifest, typically `random/mice` crossed with
  `random_ties/pattern_conditional`.

Measures:

- `:Psi`, `:R`, `:HHI`, `:RHHI`

Hard-coded values to move to config:

- `GLOBAL_MEASURES`
- output root
- write-table toggle
- target filters

Functions called:

- `load_pipeline_result`
- `pipeline_summary_table`
- `pipeline_panel_table`
- `pipeline_variance_decomposition_table`
- `plot_pipeline_scenario`
- `save_pipeline_plot`

Decomposition:

- Already close to a stage script, but CSV parsing and result loading helpers
  should move to `PrefPol/src/` if shared.

### `plot_all_scenarios_group_small.jl`

Reads:

- `run_manifest.csv`
- cached `PipelineResult`s from manifest
- O-smoothed extension results

Writes:

- `paper_group/<wave>/<scenario>/*.png`
- `paper_group/<wave>/<scenario>/*.csv`

Years/scenarios:

- Currently hard-coded to `(2018, main_2018)`, but generated output and paper
  use 2006/2018/2022 group figures.

m ranges:

- From manifest, usually `2:5`.

Backends/linearizers:

- From manifest; paper tables/figures mainly use `mice` with
  `pattern_conditional`, but script can process all combos.

Measures:

- Paper heatmap: `:C`, `:D`, `:O`, `:S`
- O-smoothed: `:O_smoothed`

Groupings:

- Config/group target order. 2018 paper order includes `LulaScoreGroup`.

Hard-coded values to move to config:

- heatmap measures and labels
- complement rules for `1 - O` and `1 - O_smoothed`
- basenames
- target wave/scenario/groupings
- output root

Functions called:

- `pipeline_group_heatmap_values`
- `plot_pipeline_group_paper_heatmap`
- `plot_pipeline_group_paper_osmoothed_heatmap`
- `save_pipeline_plot`

Decomposition:

- Stage script should be shorter. Move `prepare_group_plot_table`-style
  reusable table logic to `PrefPol/src/` or plotting extension.

### `ranking_support_diagnostics_small.jl`

Reads:

- `run_manifest.csv`
- cached `PipelineResult`s
- `linearized` stage artifacts

Writes:

- `ranking_support/ranking_support_draws.csv`
- `ranking_support/ranking_support_summary.csv`
- per-scenario copies

Years/scenarios:

- Whatever manifest contains. Validation expects 2006/2018/2022 coverage for
  `m = 2:5`.

Measures:

- Not pipeline measures. Uses `Preferences.ranking_support_diagnostics` on
  linearized profiles.

Hard-coded values to move to config:

- required years
- required m values
- output root

Functions called:

- `load_pipeline_result`
- `JLD2.load`
- `Preferences.ranking_support_diagnostics`

Decomposition:

- Extra-measures stage. CSV helpers should not be duplicated.

### `effective_counts_small.jl`

Reads:

- `run_manifest.csv`
- cached `PipelineResult`s
- `linearized` stage artifacts

Writes:

- `effective_counts/effective_counts_draws.csv`
- `effective_counts/effective_counts_summary.csv`
- per-scenario copies

Years/scenarios:

- Whatever manifest contains, filtered to `analysis_role == "main"`.

m ranges:

- `2:5`

Measures:

- Effective reversal/ranking diagnostics: `ENRP`, `EO`,
  `reversal_to_ranking_effective_ratio`

Hard-coded values to move to config:

- `EFFECTIVE_COUNT_M_VALUES`
- output root
- analysis role

Functions called:

- `load_pipeline_result`
- `JLD2.load`
- `Preferences.effective_reversal_ranking_diagnostics`

Decomposition:

- Extra-measures stage.

### `effective_rankings_evolution_tables.jl`

Reads:

- `effective_numbers_summary_table.csv`

Writes:

- per-year CSVs
- full Markdown and TeX tables
- paper Markdown and TeX tables

Years/scenarios:

- Defaults: 2006 `main_2006`, 2018 `main_2018`, 2022 `main_2022`
- CLI supports `YEAR.scenario_name=...`

Backends/linearizers:

- Defaults: `mice`, `pattern_conditional`

Analysis role:

- `main`

m ranges:

- `2:5`

Hard-coded values to move to config:

- `DEFAULT_SCENARIO_BY_YEAR`
- `DEFAULT_FILTERS`
- `OUTPUT_COLS`
- output naming

Decomposition:

- Tables stage. Keep as one stage-specific script or split table builders into
  `PrefPol/src/` if reused.

### `plot_effective_rankings_evolution.jl`

Reads:

- CSVs from `effective_rankings_evolution/`

Writes:

- `effective_rankings_evolution_1x2.png`
- `effective_rankings_evolution_1x2.svg`

Years/scenarios:

- Whatever table CSVs exist; paper expects 2006/2018/2022 main scenarios.

Hard-coded values to move to config:

- output names
- colors
- y-tick rules if paper-specific

Decomposition:

- Extra-plots stage. Plot function could move to plotting extension.

### `appendix_lambda_table.jl`

This is the distinct Lambda table script. Do not conflate it with
`effective_rankings_evolution_tables.jl`.

Reads:

- `run_manifest.csv`
- cached `PipelineResult`s

Writes:

- `appendix_lambda_table/appendix_lambda_table.csv`
- `appendix_lambda_table/appendix_lambda_audit.csv`
- `appendix_lambda_table/appendix_lambda_table.tex`

Computes or reads:

- If `:lambda_sep` exists in `result.measure_cube`, it reads it.
- Otherwise calls `augment_pipeline_result_with_lambda_sep(result; include_w=true)`.
- Uses grouped `:W`, `:D`, `:S`, and `:lambda_sep` rows.

Aggregate or group-level:

- The script summarizes grouped `:lambda_sep` by grouping using medians.
- The underlying definition in `nested_pipeline.jl` uses aggregate
  group-size-weighted `D / W` inside `compute_group_measure_details`; it is not
  a naive average of group-level ratios.
- The audit table writes grouped medians for `Lambda`, `W`, `D`, and `S`.

Years/scenarios:

- 2006 `main_2006`
- 2018 `main_2018`
- 2022 `main_2022`

m ranges:

- All matching manifest rows, expected `2:5`.

Grouping partitions:

- All grouped rows present in the selected results. Ordering is handled by
  `appendix_lambda_grouping_tables.jl`.

Backends/linearizers:

- `mice`
- `pattern_conditional`

Definition consistency:

- Current source and tests define `lambda_sep = D / W`, with `W = 1 - C_star`.
- `appendix_lambda_table.jl` describes
  `Lambda = (sum_g pi_g D_g) / (sum_g pi_g W_g)`.
- `nested_pipeline_tests.jl` includes explicit checks that Lambda is `D / W`
  and not a weighted mean of group ratios.

Format:

- CSV, audit CSV, TeX.

Hard-coded values to move to config:

- `TARGET_SPECS`
- `TARGET_ANALYSIS_ROLE`
- `TARGET_IMPUTER_BACKEND`
- `TARGET_LINEARIZER_POLICY`
- output directory
- format/digits/caption policy

Classification:

- `tables_appendix`, unless the manuscript changes to use the table in the
  main text. Current `writing/main.tex` includes it under appendix/supporting
  results.

### `appendix_lambda_grouping_tables.jl`

Reads:

- `appendix_lambda_table.csv`

Writes:

- `appendix_lambda_grouping_tables.tex`

Years/scenarios:

- 2006 `main_2006`
- 2018 `main_2018`
- 2022 `main_2022`

Hard-coded values to move to config:

- grouping order by year/scenario
- output TeX path
- caption text
- numeric formatting

Classification:

- `tables_appendix`, downstream of `10_lambda_table.jl`.

### `augment_grouped_S_cache.jl`

Reads:

- a cache root or a manifest
- cached `PipelineResult`s

Writes:

- sibling `result_with_grouped_S.jld2` or in-place overwritten result, depending
  on CLI flags

Role:

- One-off migration utility for old caches that lack grouped `S`.
- Do not include in normal paper DAG.

Reusable logic:

- Already exists in `PrefPol.src`: `augment_pipeline_result_with_grouped_s`
  exists but is not exported in `PrefPol.jl`. If needed, export it explicitly.

### `clear_running_caches.jl`

Role:

- Manual cleanup utility.

Future handling:

- Replace normal use with stage-level `--force`.
- If deletion remains useful, make a separate documented cleanup script, not
  part of `run_all_paper.jl`.

## Configuration Model

Keep year-level TOMLs focused on data and wave definitions. Add
orchestration-level TOMLs for run behavior and paper specs.

Recommended new files:

```text
PrefPol/config/orchestration.toml
PrefPol/config/smoke_test.toml
PrefPol/config/plot_specs.toml
PrefPol/config/table_specs.toml
PrefPol/config/paper_artifacts.toml
```

Alternative: merge plot/table/artifact specs into `orchestration.toml` once
the schema stabilizes. Start separate for readability.

### Year-Level TOMLs

Keep in `2006.toml`, `2018.toml`, `2022.toml`:

- `year`
- `wave_id`, if added
- `data_loader`
- `data_file`
- `candidates`
- `demographics`
- `forced_scenarios`
- `max_candidates`
- `m_values_range`
- `rng_seed`

Do not put raw-data absolute machine paths into committed TOMLs.

### `orchestration.toml`

Should own:

- `B`
- `R`
- `K`
- `force`
- `dry_run`
- `cache_root`
- `output_root`
- `imputer_backends`
- `linearizer_policies`
- `consensus_tie_policy`
- `analysis_roles`
- `measure_sets`
- `scenario_targets`
- `diagnostic_scenarios`

Example shape:

```toml
[run]
B = 1000
R = 5
K = 5
force = false
dry_run = false
cache_root = "PrefPol/composable_running/output/cache"
output_root = "PrefPol/composable_running/output"
consensus_tie_policy = "average"
imputer_backends = ["mice", "random"]
linearizer_policies = ["pattern_conditional", "random_ties"]

[[targets]]
wave_id = "2006"
scenario_name = "main_2006"
m_values = [2, 3, 4, 5]
groupings = "configured"

[[targets]]
wave_id = "2018"
scenario_name = "main_2018"
m_values = [2, 3, 4, 5]
groupings = "configured"

[[targets]]
wave_id = "2022"
scenario_name = "main_2022"
m_values = [2, 3, 4, 5]
groupings = "configured"

[[diagnostic_targets]]
wave_id = "2022"
scenario_name = "no_forcing"
m_values = [2, 3, 4, 5]
```

### `smoke_test.toml`

Must be concrete and small:

```toml
[run]
B = 2
R = 2
K = 2
force = true
dry_run = false
cache_root = "PrefPol/composable_running/output/cache_smoke"
output_root = "PrefPol/composable_running/output"
consensus_tie_policy = "average"
imputer_backends = ["mice"]
linearizer_policies = ["pattern_conditional"]

[[targets]]
wave_id = "2018"
scenario_name = "main_2018"
m_values = [2, 3]
groupings = "configured"

[measure_sets]
main = ["Psi", "R", "RHHI", "C", "D", "O", "S", "lambda_sep"]
```

If runtime is still too high, reduce to `m_values = [2]` and optionally use
`imputer_backends = ["zero"]` only for a dependency-light smoke test. The
preferred smoke test should include `mice` when R/RCall dependencies are
available, because the paper workflow uses `mice`.

### `plot_specs.toml`

Should own:

- global plot targets
- global measures
- group plot targets
- group heatmap measures
- complements such as `O -> 1 - O`
- O-smoothed plot specs
- variance decomposition plot specs
- output formats: `png`, `svg`, `pdf`
- paper filenames such as `2006_global_main.png`

### `table_specs.toml`

Should own:

- effective-ranking table specs
- Lambda table specs
- appendix table specs
- table output formats: CSV, TeX, Markdown
- rounding and display names
- grouping order by year/scenario

### `paper_artifacts.toml`

Should own:

- final artifact IDs
- source stage output paths or selection rules
- destination names under `paper_artifacts/`
- optional additional destination `writing/imgs/`
- copy versus symlink behavior

Example artifact IDs:

- `global_2006_main`
- `global_2018_main`
- `global_2022_main`
- `group_2006_main`
- `group_2018_main`
- `group_2022_main`
- `effective_rankings_plot`
- `effective_rankings_table`
- `variance_decomposition_2022`
- `appendix_lambda_grouping_tables`

## Stage DAG

Precise DAG:

```text
validate_configs
  -> bootstrap
  -> impute
  -> linearize
  -> measures

measures
  -> plot_global
  -> plot_group
  -> tables
  -> lambda_table
  -> variance_decomposition_outputs

linearize
  -> extra_measures

extra_measures
  -> extra_plots
  -> tables

plot_global + plot_group + extra_plots + tables + lambda_table
  -> collect_paper_artifacts
```

`run_all_paper.jl` should run the DAG in order. Individual stage scripts should
be independently callable and should fail clearly if required upstream
manifest rows or cache artifacts are missing.

## Stage Specifications

### `00_validate_configs.jl`

Inputs:

- year TOMLs
- orchestration TOML
- plot/table/artifact TOMLs

Outputs:

- `output/manifests/config_validation_manifest.csv`
- optional `output/logs/config_validation.log`

Manifest fields:

- `stage`
- `config_path`
- `wave_id`
- `year`
- `scenario_name`
- `m`
- `grouping`
- `status`
- `error_message`
- `timestamp`

Checks:

- all target wave configs exist
- all target scenarios exist
- all target m values are supported
- all target groupings exist in the wave config
- all backends and linearizer policies are supported by `nested_pipeline.jl`
- output and cache roots are inside expected generated-output directories
- `writing/imgs` destinations are opt-in

Idempotency:

- Always rerunnable.

Failure mode:

- Fail before any expensive computation.

Missing reusable functions:

- Add `PrefPol.validate_orchestration_config(...)` only if the validation logic
  gets too long for a stage script.

### `01_bootstrap.jl`

Inputs:

- validated config
- `PipelineSpec`s for targets
- raw data through configured loaders

Outputs:

- observed artifacts
- resample artifacts
- `output/manifests/bootstrap_manifest.csv`

Manifest fields:

- `stage`
- `wave_id`
- `year`
- `scenario_name`
- `m`
- `active_candidates`
- `candidate_label`
- `B`
- `b`
- `input_path`
- `output_path`
- `config_path`
- `timestamp`
- `status`
- `error_message`

Cache/naming:

- Reuse `cache/<spec_hash>/observed.jld2`
- Reuse `cache/<spec_hash>/resample/b0001.jld2`

Idempotency:

- If artifacts exist and `force=false`, skip and record `status=skipped`.
- If `force=true`, regenerate selected artifacts and downstream stages should
  be considered stale.

Can run independently:

- Yes, after `validate_configs`.

Existing functions:

- `build_pipeline_spec`
- `load_observed_data`
- new public `ensure_resamples!` should wrap `_draw_resample`.

### `02_impute.jl`

Inputs:

- bootstrap manifest
- observed/resample artifacts
- config backend list

Outputs:

- `cache/<spec_hash>/imputed/b####_r####.jld2`
- `output/manifests/imputation_manifest.csv`

Manifest fields:

- stage, wave/year/scenario, m, active candidates, imputer backend, B, R, b, r,
  input path, output path, config path, timestamp, status, error

Idempotency:

- Skip existing imputed artifacts unless `force=true`.

Can run independently:

- Yes, if bootstrap artifacts exist.

Existing functions:

- new public `ensure_imputations!` should wrap `_impute_resample`.

Failure mode:

- If RCall/R/mice is unavailable for `mice`, fail that backend with a clear
  message and record in manifest.

### `03_linearize.jl`

Inputs:

- imputation manifest
- imputed artifacts
- linearizer policies

Outputs:

- `cache/<spec_hash>/linearized/b####_r####_k####.jld2`
- `output/manifests/linearization_manifest.csv`

Manifest fields:

- stage, wave/year/scenario, m, imputer backend, linearizer policy, B, R, K,
  b, r, k, consensus tie policy, input path, output path, status, error

Idempotency:

- Skip existing linearized artifacts unless `force=true`.

Existing functions:

- new public `ensure_linearizations!` should wrap `_linearize_imputed`.

Failure mode:

- Missing imputed artifacts fail only selected rows; stage exits nonzero if any
  required row failed.

### `04_measures.jl`

Inputs:

- linearization manifest
- linearized artifacts
- measure list

Outputs:

- `cache/<spec_hash>/measure/b####_r####_k####.jld2`
- `cache/<spec_hash>/result.jld2`
- `output/measures/measure_table.csv`
- `output/measures/summary_table.csv`
- `output/measures/panel_table.csv`
- `output/measures/decomposition_table.csv`
- `output/manifests/measure_manifest.csv`

Manifest fields:

- stage, wave/year/scenario, m, backend, linearizer, tie policy, B/R/K/b/r/k,
  measure_id, grouping, input path, output path, status, error

Idempotency:

- Skip existing measure artifacts and `PipelineResult` unless `force=true`.

Existing functions:

- new public `ensure_measures!`
- `pipeline_measure_table`
- `pipeline_summary_table`
- `pipeline_panel_table`
- `decomposition_table`

Failure mode:

- Missing linearized rows should be reported before measure computation starts.

### `05_plot_global.jl`

Inputs:

- measure manifest
- `PipelineResult`s
- `plot_specs.toml`

Outputs:

- `output/plots/global/.../*.png`
- optional `.svg` and `.pdf`
- compact CSVs used for each plot
- `output/manifests/plot_manifest.csv`

Manifest fields:

- stage, artifact_id, wave/year/scenario, m range, backend, linearizer,
  measure list, input path, output path, format, status, error

Existing functions:

- `pipeline_scenario_plot_data`
- `plot_pipeline_scenario`
- `save_pipeline_plot`

Idempotency:

- Skip if all requested formats exist and source manifest hash is unchanged,
  unless `force=true`.

### `06_plot_group.jl`

Inputs:

- measure manifest
- `PipelineResult`s
- `plot_specs.toml`

Outputs:

- `output/plots/group/.../*.png`
- heatmap CSVs
- O-smoothed heatmap outputs if configured
- `output/manifests/plot_manifest.csv` appended or separate
  `group_plot_manifest.csv`

Existing functions:

- `pipeline_group_heatmap_values`
- `plot_pipeline_group_paper_heatmap`
- `plot_pipeline_group_paper_osmoothed_heatmap`
- `save_pipeline_plot`

Hard-coded values to move:

- `PAPER_GROUP_HEATMAP_MEASURES`
- `PAPER_GROUP_HEATMAP_LABELS`
- `PAPER_GROUP_HEATMAP_COMPLEMENTS`
- O-smoothed labels/complements
- target groupings and output basenames

Missing reusable function:

- Move `prepare_group_plot_table` and complement handling to `PrefPol/src/` or
  `PrefPol/ext/PrefPolPlottingExt.jl`.

### `07_extra_measures.jl`

Inputs:

- linearization manifest
- `PipelineResult.stage_manifest`
- extra measure specs

Outputs:

- `output/extra_measures/ranking_support/*.csv`
- `output/extra_measures/effective_counts/*.csv`
- `output/manifests/extra_measure_manifest.csv`

Current scripts to port:

- `ranking_support_diagnostics_small.jl`
- `effective_counts_small.jl`

Existing functions:

- `Preferences.ranking_support_diagnostics`
- `Preferences.effective_reversal_ranking_diagnostics`

Missing reusable function:

- Public helper in `PrefPol/src/` to iterate linearized artifact rows from a
  `PipelineResult` or manifest.

### `08_extra_plots.jl`

Inputs:

- extra measure outputs
- table outputs
- plot specs

Outputs:

- `output/extra_plots/effective_rankings_evolution_1x2.png`
- optional `.svg`
- variance decomposition plots
- `output/manifests/extra_plot_manifest.csv`

Current scripts to port:

- `plot_effective_rankings_evolution.jl`
- variance decomposition plot generation currently implied by writing artifact
  `variance_decomposition_2022.png` and plotting extension functions

Missing reusable functions:

- Move effective-ranking evolution plot function to plotting extension if it is
  reused.

### `09_tables.jl`

Inputs:

- measures
- extra measures
- table specs

Outputs:

- `output/tables/*.csv`
- `output/tables/*.tex`
- `output/tables/*.md`
- `output/manifests/table_manifest.csv`

Current scripts to port:

- `effective_rankings_evolution_tables.jl`
- non-Lambda appendix tables
- any table generation implied by writing artifacts

Paper tables:

- `effective_rankings.tex` is included by `writing/main.tex`.

### `10_lambda_table.jl`

Inputs:

- measure manifest
- selected cached `PipelineResult`s
- `table_specs.toml`

Outputs:

- `output/appendices/lambda/appendix_lambda_table.csv`
- `output/appendices/lambda/appendix_lambda_audit.csv`
- `output/appendices/lambda/appendix_lambda_table.tex`
- `output/appendices/lambda/appendix_lambda_grouping_tables.tex`
- `output/manifests/lambda_table_manifest.csv`

Current scripts to port:

- `appendix_lambda_table.jl`
- `appendix_lambda_grouping_tables.jl`

Canonical spec:

- years/scenarios: 2006 `main_2006`, 2018 `main_2018`, 2022 `main_2022`
- backend: `mice`
- linearizer: `pattern_conditional`
- m range: `2:5`
- groupings: configured groupings, with configured display order
- definition: aggregate `Lambda = D / W`
- formats: CSV, audit CSV, TeX

Idempotency:

- Skip if selected result paths and table spec hash match prior manifest,
  unless `force=true`.

Failure mode:

- If neither `:lambda_sep` nor derivable grouped `C/D/W` rows exist, fail with
  an actionable message.

Missing reusable functions:

- Public helper for `lambda_table_from_results(...)` should live in
  `PrefPol/src/`, not in `composable_running/`.

### `11_collect_paper_artifacts.jl`

Inputs:

- plot manifests
- table manifest
- lambda manifest
- `paper_artifacts.toml`

Outputs:

- `output/paper_artifacts/*`
- `output/manifests/paper_artifact_manifest.csv`

Current paper artifact mapping from `writing/main.tex`:

| Artifact ID | Destination filename |
|---|---|
| `global_2006_main` | `2006_global_main.png` |
| `global_2018_main` | `2018_global_main.png` |
| `global_2022_main` | `2022_global_main.png` |
| `group_2006_main` | `2006_group.png` |
| `group_2018_main` | `2018_group.png` |
| `group_2022_main` | `2022_group.png` |
| `effective_rankings_evolution` | `effective_rankings_evolution_1x2.png` |
| `effective_rankings_table` | `effective_rankings.tex` |
| `variance_decomposition_2022` | `variance_decomposition_2022.png` |
| `appendix_lambda_grouping_tables` | `appendix_lambda_grouping_tables.tex` |

Collection behavior:

- Default: copy into `PrefPol/composable_running/output/paper_artifacts/`.
- Optional: symlink instead of copy if configured.
- Optional: also update `writing/imgs/` if configured.
- Manifest should record source path, destination path, copy/symlink, timestamp,
  and status.

## Output and Manifest Design

Every stage writes a manifest under:

```text
PrefPol/composable_running/output/manifests/
```

Suggested manifest files:

- `config_validation_manifest.csv`
- `bootstrap_manifest.csv`
- `imputation_manifest.csv`
- `linearization_manifest.csv`
- `measure_manifest.csv`
- `extra_measure_manifest.csv`
- `plot_manifest.csv`
- `table_manifest.csv`
- `lambda_table_manifest.csv`
- `paper_artifact_manifest.csv`

Common fields:

- `stage`
- `year`
- `wave_id`
- `scenario_name`
- `m`
- `active_candidates`
- `candidate_label`
- `imputer_backend`
- `linearizer_policy`
- `consensus_tie_policy`
- `B`
- `R`
- `K`
- `b`
- `r`
- `k`
- `measure_id`
- `grouping`
- `table_id`
- `artifact_id`
- `input_path`
- `output_path`
- `config_path`
- `timestamp`
- `status`
- `error_message`

Formats:

- CSV for manifests and tabular summaries.
- JLD2 for complex Julia objects and nested stage artifacts.
- PNG for manuscript figures.
- SVG/PDF optionally for vector/export workflows.
- TeX for manuscript tables.
- Markdown for human-readable table previews.

Naming convention:

```text
<stage>/<wave_id>/<scenario_name>/<backend>/<linearizer>/m<m>/<artifact>.<ext>
```

For plot files, include enough metadata to avoid ambiguity:

```text
global_measures_year-2022_scenario-main_2022_backend-mice_linearizer-pattern_conditional_B-1000_R-5_K-5.png
```

Paper collection can rename these long source filenames into short manuscript
filenames.

## Minimal CLI

Each stage should support:

- `--config PATH`
- `--year YEAR`
- `--scenario NAME`
- `--m VALUE_OR_RANGE`
- `--backend NAME`
- `--force`
- `--dry-run`
- `--smoke-test`

Keep parsing simple. Do not build a framework. If CLI parsing grows, add one
small exported helper in `PrefPol/src/`, not a `composable_running/lib`.

Example commands:

```bash
julia --project=PrefPol PrefPol/composable_running/stages/01_bootstrap.jl --config PrefPol/config/smoke_test.toml
julia --project=PrefPol PrefPol/composable_running/stages/04_measures.jl --config PrefPol/config/orchestration.toml
julia --project=PrefPol PrefPol/composable_running/stages/09_tables.jl --config PrefPol/config/orchestration.toml
julia --project=PrefPol PrefPol/composable_running/stages/10_lambda_table.jl --config PrefPol/config/orchestration.toml
julia --project=PrefPol PrefPol/composable_running/stages/11_collect_paper_artifacts.jl --config PrefPol/config/orchestration.toml
julia --project=PrefPol PrefPol/composable_running/run_all_paper.jl --config PrefPol/config/orchestration.toml
```

## Smoke Test

Concrete smoke test:

- `B = 2`
- `R = 2`
- `K = 2`
- one year: prefer 2018 because it exercises `LulaScoreGroup`
- scenario: `main_2018`
- m: `2:3`; reduce to `2` only if runtime is a problem
- imputer backend: `mice` if R/RCall is available, otherwise `zero` as fallback
- linearizer policy: `pattern_conditional`
- measures: `:Psi`, `:R`, `:RHHI`, `:C`, `:D`, `:O`, `:S`, `:lambda_sep`

Smoke command:

```bash
julia --project=PrefPol PrefPol/composable_running/run_all_smoke.jl --config PrefPol/config/smoke_test.toml
```

Smoke success criteria:

- bootstrap manifest exists and has successful rows
- imputation manifest exists and has successful rows
- linearization manifest exists and has successful rows
- measure manifest exists and has successful rows
- global plot exists
- group plot or heatmap exists
- at least one regular table exists
- Lambda table exists
- collection stage creates:

```text
PrefPol/composable_running/output/paper_artifacts/
```

with at least one plot and one table.

## Migration Strategy

Phase numbers are migration-order labels. Stage numbers are file-order labels.
They are related, but they are not the same namespace.

Phase 7 means: implement group plotting by creating or completing
`PrefPol/composable_running/stages/06_plot_group.jl`.

Stage `07_extra_measures.jl` belongs to Phase 8 and must not be implemented as
part of Phase 7.

Phase 0: Inventory current small scripts in `PrefPol/running/`.

- Done in this planning task.
- Treat `running.jl` as legacy context only.

Phase 1: Create `PrefPol/composable_running/` structure.

- Add `README.md`, `run_all_smoke.jl`, `run_all_paper.jl`, `stages/`, and
  `output/` subfolders.

Phase 2: Identify config/path/manifest utilities already in `PrefPol/src/`.

- Reuse `load_survey_wave_config`, `build_source_registry`,
  `build_pipeline_spec`, and result table helpers.
- Avoid duplicated CSV readers/writers across scripts.

Phase 2b: Config schema completion.

- Create `PrefPol/config/orchestration.toml`, `PrefPol/config/smoke_test.toml`,
  `PrefPol/config/plot_specs.toml`, `PrefPol/config/table_specs.toml`, and
  `PrefPol/config/paper_artifacts.toml`.
- Plotting, table, and artifact stages must not rely on permanent fallback
  defaults.
- Fallback defaults are allowed only as temporary smoke-test safeguards and
  must be marked with `TODO(<config-file>.toml)` naming the config that should
  replace them.

Phase 3: Add minimal missing reusable helpers to `PrefPol/src/` or
`PrefPol/ext/`.

- Stage-only nested pipeline APIs.
- Result/manifest loading helpers.
- Shared CSV/manifest writer if needed.
- Group heatmap table preparation helper if needed.
- Lambda table helper if needed.

Phase 4: Create stage scripts that initially wrap existing functions.

- First version may call `run_pipeline` from `04_measures.jl` if stage-only API
  is not ready, but the stage should clearly mark that bootstrap/impute/
  linearize are not independently executable yet.

Phase 5: Separate bootstrap/imputation/linearization once API permits.

- Implement public `ensure_*` APIs in `nested_pipeline.jl`.
- Update stages 01-04 to use them.

Phase 6: Port global plots.

- Port `plot_all_scenarios_global_small.jl` behavior to `05_plot_global.jl`.
- Move target and measure constants to config.

Phase 7: Port group plots and heatmaps.

- Port `plot_all_scenarios_group_small.jl` behavior to `06_plot_group.jl`.
- Keep complement rules config-driven.

Phase 8: Port extra measures, diagnostics, appendix tables, and Lambda.

- Port ranking support, effective counts, effective-ranking tables, Lambda
  tables, and variance decomposition outputs.

Phase 9: Create artifact collection based on root `writing/main.tex`.

- Map generated outputs to manuscript filenames.
- Keep destination configurable.

Phase 10: Run B=R=K=2 smoke test.

- Validate all required stage outputs and manifests.

Phase 11: Decide whether old `PrefPol/running/` scripts should be deprecated.

- Do not delete them during the refactor.
- Add deprecation notes only after smoke and paper workflows pass.

## Multi-Agent Implementation Plan

### Agent A: Running Inventory and Parity

Assigned files:

- `PrefPol/running/*.jl`
- `PrefPol/running/*.md`

Expected output:

- A parity checklist mapping every current script output to a new stage output.

Tests:

- No code tests required; verify file existence and output mapping.

Do not touch:

- `PrefPol/src/`
- generated caches

### Agent B: Config Schema

Assigned files:

- `PrefPol/config/orchestration.toml`
- `PrefPol/config/smoke_test.toml`
- `PrefPol/config/plot_specs.toml`
- `PrefPol/config/table_specs.toml`
- `PrefPol/config/paper_artifacts.toml`
- possible config parsing helpers in `PrefPol/src/`

Expected output:

- TOML schema and loader/validator.

Tests:

- Add focused config validation tests.

Do not touch:

- statistical pipeline behavior

### Agent C: Minimal PrefPol API Helpers

Assigned files:

- `PrefPol/src/nested_pipeline.jl`
- `PrefPol/src/PrefPol.jl`
- tests in `PrefPol/test/`

Expected output:

- Public stage-only APIs reusing existing private internals.

Tests:

```bash
julia --project=PrefPol -e 'using Pkg; Pkg.test()'
```

Do not touch:

- plotting scripts
- paper artifacts

### Agent D: Bootstrap/Impute/Linearize Stages

Assigned files:

- `PrefPol/composable_running/stages/01_bootstrap.jl`
- `PrefPol/composable_running/stages/02_impute.jl`
- `PrefPol/composable_running/stages/03_linearize.jl`

Expected output:

- Stage scripts with manifests and idempotency.

Tests:

- Run smoke through linearization.

Do not touch:

- plotting extension

### Agent E: Measures and Extra Measures

Assigned files:

- `PrefPol/composable_running/stages/04_measures.jl`
- `PrefPol/composable_running/stages/07_extra_measures.jl`
- supporting helpers in `PrefPol/src/` if needed

Expected output:

- Measure tables, effective counts, ranking support diagnostics.

Tests:

- Smoke measure stage and check CSV manifests.

Do not touch:

- plot rendering code

### Agent F: Global/Group Plots and Heatmaps

Assigned files:

- `PrefPol/composable_running/stages/05_plot_global.jl`
- `PrefPol/composable_running/stages/06_plot_group.jl`
- `PrefPol/composable_running/stages/08_extra_plots.jl`
- `PrefPol/ext/PrefPolPlottingExt.jl` if reusable plotting helpers are needed

Expected output:

- Paper-equivalent global/group/effective/variance plots.

Tests:

- Run plotting smoke using CairoMakie environment.
- Verify PNG files are nonempty.

Do not touch:

- nested pipeline internals

### Agent G: Tables and Lambda

Assigned files:

- `PrefPol/composable_running/stages/09_tables.jl`
- `PrefPol/composable_running/stages/10_lambda_table.jl`
- table helpers in `PrefPol/src/` if needed

Expected output:

- Effective-ranking tables.
- Lambda CSV/audit/TeX/grouping tables.

Tests:

- Validate Lambda uses `D / W` and selected rows match config.

Do not touch:

- plotting stages

### Agent H: Paper Artifact Collection

Assigned files:

- `PrefPol/composable_running/stages/11_collect_paper_artifacts.jl`
- `PrefPol/config/paper_artifacts.toml`
- `PrefPol/composable_running/README.md`

Expected output:

- Paper artifact manifest and collected files.

Tests:

- Run collection after smoke and verify all configured artifacts exist.

Do not touch:

- `writing/main.tex` unless explicitly requested.

### Agent I: Smoke Test and README

Assigned files:

- `PrefPol/composable_running/run_all_smoke.jl`
- `PrefPol/composable_running/run_all_paper.jl`
- `PrefPol/composable_running/README.md`

Expected output:

- End-to-end smoke command.
- Paper command sequence.
- Troubleshooting notes for R/RCall/CairoMakie.

Tests:

- `B=R=K=2` smoke run.

Do not touch:

- old `PrefPol/running/` scripts except to reference them.

## Non-Negotiable Style Constraints

The new scripts must be:

- small
- short
- human-readable
- explicit
- composable
- config-driven
- easy to run from the shell
- easy to inspect
- minimally abstract
- not giant nested loops
- not full of hard-coded paper constants
- not dependent on hidden global mutable state

Reusable logic belongs in:

- `PrefPol/src/`
- `PrefPol/ext/PrefPolPlottingExt.jl`

Not in:

- `PrefPol/composable_running/lib/`

## Notes on the Understanding Graph

An existing `.understand-anything/knowledge-graph.json` is present, but its
metadata shows it was last analyzed at commit
`aef1eca5379d54cecf0db501dffb9b9ec5f73663`, while this checkout is currently
at `6ffc823c9c5686ebb6048906802e7cb84ea43fe2`. Do not rely on that stale graph
for implementation facts. If later agents use the understanding skill, rebuild
or update the graph first.
