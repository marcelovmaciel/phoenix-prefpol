# PrefPol.jl

`PrefPol.jl` is the applied Brazil/ESEB replication package in this monorepo.
It owns survey-wave configuration, raw survey loading, candidate-set selection,
bootstrap/imputation/linearization orchestration, cache layout, applied measure
execution, variance decomposition, and paper-facing tables and figures.

Formal preference representations and mathematical definitions live in
`PreferenceProfiles.jl`; PrefPol builds those objects from survey data and runs the
replication pipeline around them.

## Package Boundary

`PreferenceProfiles` owns candidate pools, rankings, profiles, consensus, reversal
diagnostics, polarization diagnostics, majority support, and group diagnostics
as formal objects. `PrefPol` owns the applied Brazil/ESEB layer: TOML
configuration, ESEB-specific preprocessing, raw survey loading,
candidate-set selection, bootstrap/imputation/linearization orchestration,
cache layout, applied measure execution, variance reports, and publication
artifacts. `PreferencePlots` and the `PrefPol` CairoMakie extension provide
plotting functionality; plotting is not the core formal API.

## Publication-facing replication

From the repository root, reproduce the publication-facing run with:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/run_all_paper.jl \
  --config PrefPol/config/publication.toml
```

Validate the publication configuration with:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/stages/00_validate_configs.jl \
  --config PrefPol/config/publication.toml
```

`PrefPol/config/publication.toml` is the clean manuscript-facing entry point.
It runs only the measures used in the article: `Psi`, `R`, `HHI`, `RHHI`, `C`,
and `D`. Outputs are isolated under
`PrefPol/composable_running/output/publication/`.

## Source Map

| File | Role |
|---|---|
| `survey_config.jl` | TOML parsing, `SurveyWaveConfig`, source registries, candidate-set resolution, and raw profile entry points. |
| `preprocessing_general.jl` | SPSS/R integration, generic score preprocessing, imputation helpers, bootstrap helpers, and ranking adapters. |
| `preprocessing_specific.jl` | ESEB-specific loaders, year transformations, and wave-level recodes. |
| `profile_adapters.jl` | Construction of formal `PreferenceProfiles` profile objects from survey-derived tables. |
| `nested_pipeline.jl` | BRK orchestration, deterministic cache keys, stage artifacts, measure execution, and result tables. |
| `variance_decomposition.jl` | Explicit bootstrap-imputation-linearization variance decomposition for scalar leaf outputs. |
| `variance_decomposition_report.jl` | Report-facing variance tables and plot-table helpers. |
| `ext/PrefPolPlottingExt.jl` | CairoMakie-backed plotting helpers loaded only when `CairoMakie` is available. |

```@contents
Depth = 2
```

## Module

```@docs; canonical=false
PrefPol
```
