# API Reference

```@meta
CurrentModule = PrefPol
```

## Survey Configuration

```@docs; canonical=false
SurveyWaveConfig
load_survey_wave_config
build_source_registry
resolve_active_candidate_set
```

## Raw Data and Profiles

```@docs; canonical=false
load_raw_pref_data
build_profile
profile_pattern_proportions
ranked_count
has_ties
ranking_type_support
ranking_type_template
profile_ranksize_summary
profile_ranking_type_proportions
pretty_print_profile_patterns
pretty_print_ranksize_summary
pretty_print_ranking_type_proportions
```

## Manuscript-Facing Measures

The publication-facing replication config,
`PrefPol/config/publication.toml`, runs the article's measure set:
`Psi`, `R`, `HHI`, `RHHI`, `C`, and `D`. In PrefPol, `Psi`, `R`, `HHI`,
and `RHHI` are global profile measures, while `C` and `D` are computed by
the grouped-measure pipeline through `compute_group_measure_details`.

```@docs; canonical=false
Ψ
calc_total_reversal_component
calc_reversal_HHI
fast_reversal_geometric
compute_group_measure_details
```

## Extended and Diagnostic Measures

These helpers remain part of the supported API for diagnostics, appendices,
and legacy comparisons. They are not the main manuscript-facing measure set
unless a manuscript config explicitly requests them.

```@docs; canonical=false
normalized_consensus_separation
consensus_excess_separation
group_E
aggregate_E
E
S
S_old
```

The nested pipeline can also compute grouped `:O`, `:O_smoothed`, `:Sep`,
`:G`, `:Gsep`, `:W`, `:E`, `:S`, `:S_old`, and `:lambda_sep` rows when an
extended config requests them. Their formal definitions are delegated to
`Preferences`; PrefPol's role is to construct applied survey profiles and
cache/report the requested rows.

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
pipeline_candidate_label
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

## Internal Pipeline Augmentations

These result-level helpers derive diagnostic rows from already cached grouped
`C`, `D`, and `W` rows. They are useful for older caches and extended reports,
but they do not define the publication-facing measure set and do not rewrite
stage cache artifacts.

```@docs; canonical=false
augment_pipeline_result_with_E
augment_pipeline_result_with_lambda_sep
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

## Full Public API

```@index
Modules = [PrefPol]
```

```@autodocs
Modules = [PrefPol]
Public = true
Private = false
```
