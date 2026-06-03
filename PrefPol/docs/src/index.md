# PrefPol.jl

`PrefPol.jl` is the applied Brazil/ESEB replication package in this monorepo.
It owns survey-wave configuration, raw survey loading, candidate-set selection,
bootstrap/imputation/linearization orchestration, cache layout, applied measure
execution, variance decomposition, and paper-facing tables and figures.

Formal preference representations and mathematical definitions live in
`Preferences.jl`; PrefPol builds those objects from survey data and runs the
replication pipeline around them.

```@contents
Depth = 2
```

## Module

```@docs; canonical=false
PrefPol
```
