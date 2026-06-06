# Outputs

```@meta
CurrentModule = PrefPol
```

The publication workflow writes generated files under:

```text
PrefPol/composable_running/output/publication/
```

Do not commit generated cache, plot, table, or paper-artifact outputs unless a
change is explicitly intended to update checked-in results.

## Main Directories

| Directory | Contents |
|---|---|
| `cache/` | Spec-specific nested pipeline cache directories and saved `PipelineResult` files. |
| `bootstraps/`, `imputations/`, `linearizations/` | Stage manifests for stochastic branch creation. |
| `measures/` | Measure cubes, summary tables, and decomposition tables. |
| `plots/global/` | Global measure plots and plot-data CSVs. |
| `plots/group/` | Group heatmaps and grouped plot-data CSVs. |
| `extra_measures/` | Effective-ranking and ranking-support summaries. |
| `extra_plots/` | Secondary plots such as variance-decomposition and effective-ranking figures. |
| `tables/` | TeX, Markdown, and CSV tables produced by table stages. |
| `appendices/` | Optional appendix artifacts when enabled. |
| `paper_artifacts/` | Reviewer-facing collected figures and tables. |
| `manifests/` | Stage-level provenance, status, and output-path manifests. |

For final manuscript-facing outputs, start in `paper_artifacts/`. For failed or
partial runs, start in `manifests/` and follow recorded paths into cache,
measure, plot, or table directories.

## Tables

`pipeline_measure_table` returns the leaf-level measure cube. It includes
`b`, `r`, `k`, measure id, grouping, value, interval fields where available,
and diagnostics. `pipeline_summary_table` returns pooled measure summaries.
`pipeline_variance_decomposition_table` and `decomposition_table` expose the
bootstrap, imputation, and linearization variance components.

`pipeline_panel_table`, `pipeline_scenario_plot_data`, `pipeline_group_plot_data`,
and `pipeline_group_heatmap_values` are report-oriented table builders used by
plotting and publication stages.

## Artifact Collection

`PrefPol/config/paper_artifacts.toml` declares which generated plots and tables
are collected into `paper_artifacts/`. `11_collect_paper_artifacts.jl` reads
the relevant stage manifests, filters by wave/scenario/backend/linearizer, and
copies the configured artifacts into the collection directory.

## API

```@docs; canonical=false
pipeline_measure_table
pipeline_summary_table
pipeline_variance_decomposition_table
pipeline_panel_table
select_pipeline_panel_rows
pipeline_scenario_plot_data
pipeline_group_plot_data
pipeline_group_heatmap_values
decomposition_table
save_pipeline_variance_decomposition_csv
```
