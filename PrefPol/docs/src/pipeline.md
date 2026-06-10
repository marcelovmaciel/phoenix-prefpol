# Pipeline

```@meta
CurrentModule = PrefPol
```

The nested pipeline turns configured survey waves into strict `PreferenceProfiles`
profiles and scalar measure outputs. The pipeline tree is:

1. observed data for a fixed survey wave, candidate set, and grouping set,
2. `B` weighted bootstrap branches,
3. `R` imputation branches within each bootstrap branch,
4. `K` strict linearizations within each imputed weak profile, and
5. global and grouped measure evaluation at each `(b, r, k)` leaf.

The stable serialization of `PipelineSpec` defines the cache key. Changing the
wave, active candidates, groupings, measures, branch counts, imputation backend,
linearizer policy, consensus tie policy, seed namespace, schema version, or code
version creates a distinct cache directory.

## Data Adaptation

`preprocessing_general.jl` contains generic score preprocessing, SPSS/R
integration, imputation helpers, weighted bootstrap helpers, and score-to-rank
adapters. `preprocessing_specific.jl` contains the ESEB-specific loaders and
year recodes. `eseb_semantics.jl` centralizes ESEB score missing-code handling
and derived Lula-score grouping helpers.

`profile_adapters.jl` and the raw-profile helpers in `survey_config.jl` build
formal `PreferenceProfiles` profile objects from survey-derived tables. PrefPol does
not redefine the mathematical profile layer; it adapts ESEB data into the
formal objects.

## Measures

Global measures call formal `PreferenceProfiles` functions on strict profiles:

- `Psi`: `PreferenceProfiles.can_polarization`,
- `R`: `PreferenceProfiles.total_reversal_component`,
- `HHI`: `PreferenceProfiles.reversal_hhi`, and
- `RHHI`: `PreferenceProfiles.reversal_geometric`.

Grouped manuscript measures are computed through
`compute_group_measure_details`:

- `C`: consensus-relative within-group coherence, and
- `D`: member-to-other-consensus divergence.

The pipeline registry also supports explicitly requested diagnostic or legacy
grouped measures, including overlap, old separation, and lambda-style
quantities. Those are not run by the publication config unless a separate
config opts into them.

## Cache Stages

The stage helpers are idempotent around the cache:

- `ensure_observed!` caches the observed survey table restricted to the active
  candidates and requested groupings.
- `ensure_resamples!` writes bootstrap branches.
- `ensure_imputations!` writes imputed tables.
- `ensure_linearizations!` writes strict annotated profiles.
- `ensure_measures!` writes the leaf measure cube and summary result.

`run_pipeline` runs the full sequence for one `PipelineSpec`; `run_batch` runs
an ordered `StudyBatchSpec` and appends reporting metadata to result tables.

## API

```@docs; canonical=false
PipelineSpec
build_pipeline_spec
NestedStochasticPipeline
ObservedData
Resample
ImputedData
LinearizedProfile
MeasureResult
PipelineResult
StudyBatchItem
StudyBatchSpec
BatchRunner
load_observed_data
compute_group_measure_details
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
