# API Reference

```@meta
CurrentModule = PrefPol
```

This page lists the `PrefPol` APIs used by the public Brazil/ESEB article
replication path. Broader formal preference APIs are documented in the sibling
`Preferences` package.

## Survey Configuration

```@docs; canonical=false
SurveyWaveConfig
load_survey_wave_config
build_source_registry
resolve_active_candidate_set
available_election_years
default_config_path
```

## Raw Data and Profiles

```@docs; canonical=false
load_raw_pref_data
build_profile
```

## Manuscript-Facing Measures

The publication-facing replication config,
`PrefPol/config/publication.toml`, runs the article's measure set:
`Psi`, `R`, `HHI`, `RHHI`, `C`, and `D`. In PrefPol, `Psi`, `R`, `HHI`,
and `RHHI` are selected and executed by the nested pipeline, delegating the
formal definitions to `Preferences.can_polarization`,
`Preferences.total_reversal_component`, `Preferences.reversal_hhi`, and
`Preferences.reversal_geometric`. `C` and `D` are computed by the
grouped-measure pipeline through `compute_group_measure_details`.

```@docs; canonical=false
compute_group_measure_details
```

Diagnostic and legacy grouped measures remain available in the pipeline
registry for explicit extended runs. They are not part of the publication
default set unless a separate config requests them.

## Nested Pipeline Types

```@docs; canonical=false
PipelineSpec
build_pipeline_spec
NestedStochasticPipeline
ObservedData
Resample
ImputedData
LinearizedProfile
MeasureResult
VarianceComponentSummary
VarianceDecomposition
PipelineResult
StudyBatchItem
StudyBatchSpec
BatchRunner
BatchRunResult
```

## Pipeline Stages and Cache

```@docs; canonical=false
load_observed_data
pipeline_cache_dir
pipeline_stage_paths
ensure_observed!
ensure_resamples!
ensure_imputations!
ensure_linearizations!
ensure_measures!
load_stage_artifact
rebuild_pipeline_result_from_stage_cache
run_pipeline
load_pipeline_result
save_pipeline_result
run_batch
```

## Pipeline Tables

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

## Variance-Decomposition Reports

```@docs; canonical=false
VarianceDecompositionReportSpec
DEFAULT_PAPER_VARIANCE_MEASURES
DEFAULT_PAPER_VARIANCE_MEASURE_LABELS
normalize_variance_measure
variance_decomposition_report
variance_decomposition_fine_table
variance_decomposition_pooled_table
variance_decomposition_by_m_plot_table
variance_decomposition_year_scenario_boxplot_table
```

## Plotting Helpers

These functions are available when the `CairoMakie` extension is loaded.

```@docs; canonical=false
plot_variance_decomposition_by_m
plot_variance_decomposition_year_scenario_boxplots
plot_variance_decomposition_dotwhisker
plot_variance_decomposition_boxplot
plot_pipeline_scenario
plot_pipeline_group_lines
plot_pipeline_group_heatmap
save_pipeline_plot
```

## Public Index

```@index
Modules = [PrefPol]
```
