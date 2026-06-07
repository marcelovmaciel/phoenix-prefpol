# PrefPol Paper Companion Notebooks

The `nb/` notebooks are paper-bound replication and inspection notebooks for the PrefPol polarization manuscript. They follow the paper pipeline at notebook scale: configuration, target expansion, resampling, imputation, linearization, measures, summaries, diagnostics, plots/tables, and final artifact provenance. They are not a general social-choice or profile-geometry explorer.

## Start Pluto

Use Julia 1.11.9, matching `nb/Manifest.toml`:

```bash
julia +1.11.9 --project=nb -e 'using Pkg; Pkg.instantiate(); using Pluto; Pluto.run()'
```

Open the notebooks in order:

1. `00_setup_and_config.jl` shows configured notebook targets, B/R/K, measures, backends, linearizers, waves, and active candidates for a selected target.
2. `01_targets_and_specs.jl` shows target expansion into concrete `PipelineSpec` batch items, active candidates, groupings, and measures.
3. `02_resampling_and_imputation.jl` shows observed score rows, candidate missingness, bootstrap multiplicities, before/after imputation rows, and imputation missingness.
4. `03_linearization.jl` shows weak score patterns, selected strict rankings, and ranking frequencies for a selected stochastic leaf.
5. `04_measure_cubes.jl` shows draw-level paper measures (`Psi`, `R`, `HHI`, `RHHI`, `C`, `D`) and compact summaries.
6. `05_global_and_group_summaries.jl` shows paper-facing global summaries and grouped `C`/`D` summary rows.
7. `06_extra_diagnostics.jl` shows effective ranking and effective reversal-pair diagnostics used by the manuscript.
8. `07_plots_and_tables.jl` shows effective-ranking table data and displays the local effective-ranking plot inline, with export paths only as provenance.
9. `08_artifact_map.jl` is debug/provenance only: it maps source manifests to paper-facing artifact names.

## Configuration and Outputs

Notebook runs use `nb/notebook_config.toml`, which defaults to a small real-data run under:

```text
nb/output/notebook_smoke
nb/output/notebook_smoke/cache
```

The notebooks guard against writing to the production publication output tree. To widen the run, edit `nb/notebook_config.toml` deliberately: add paper years/scenarios, expand `m_values`, or enable additional configured backends/linearizers. Do not add toy data or non-paper exploratory objects.

## Validation

Run these from the repository root:

```bash
julia +1.11.9 --project=nb -e 'using Pkg; Pkg.instantiate()'
julia scripts/validate_pluto_notebooks.jl
julia +1.11.9 --project=nb nb/check_notebooks.jl
```

Then verify Pluto parsing:

```julia
import Pluto
for file in filter(f -> endswith(f, ".jl"), readdir("nb"; join=true))
    Pluto.load_notebook(file)
end
```

If Julia reports a manifest/version mismatch, repair the `nb/` environment with Julia 1.11.9 rather than mixing versions.

## Production Replication

The official paper pipeline remains:

```bash
julia +1.11.9 --project=PrefPol PrefPol/composable_running/run_all_paper.jl --config PrefPol/config/publication.toml
```

Publication artifacts belong under `PrefPol/composable_running/output/`; notebook artifacts remain under `nb/output/notebook_smoke`.
