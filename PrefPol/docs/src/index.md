# PrefPol.jl

`PrefPol.jl` is the applied Brazil/ESEB replication package in this monorepo.
It owns survey-wave configuration, raw survey loading, candidate-set selection,
bootstrap/imputation/linearization orchestration, cache layout, applied measure
execution, variance decomposition, and paper-facing tables and figures.

Formal preference representations and mathematical definitions live in
`Preferences.jl`; PrefPol builds those objects from survey data and runs the
replication pipeline around them.

## Publication-facing replication

From the repository root, reproduce the publication-facing run with:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/run_all_paper.jl \
  --config PrefPol/config/publication.toml
```

`PrefPol/config/publication.toml` is the clean manuscript-facing entry point.
It runs only the measures used in the article: `Psi`, `R`, `HHI`, `RHHI`, `C`,
and `D`. Outputs are isolated under
`PrefPol/composable_running/output/publication/`.

Extended configs such as `PrefPol/config/orchestration_b30_r10_k10.toml` remain
available for the author's full working pipeline and diagnostics, but are not
the primary article reproduction path. `PrefPol/config/smoke_test.toml` is for
mechanical validation, not article reproduction.

```@contents
Depth = 2
```

## Module

```@docs; canonical=false
PrefPol
```
