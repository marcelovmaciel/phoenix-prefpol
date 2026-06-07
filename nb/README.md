# PrefPol Notebooks

The `nb/` directory contains Pluto notebooks for interactively inspecting the
PrefPol replication workflow at a small scale. They expose intermediate
configuration, profile, cache, measure, and table objects so readers can inspect
the analysis layer without running the full publication pipeline.

## Start Pluto

Run Pluto from the repository root with the notebook environment:

```bash
julia +1.11.9 --project=nb -e 'using Pkg; Pkg.instantiate(); using Pluto; Pluto.run()'
```

Then open the notebooks in numeric order:

```text
00_setup_and_config.jl
01_targets_and_specs.jl
02_resampling_and_imputation.jl
03_linearization.jl
04_measure_cubes.jl
05_global_and_group_summaries.jl
06_extra_diagnostics.jl
07_plots_and_tables.jl
08_artifact_map.jl
```

## Configuration

Notebook runs use `nb/notebook_config.toml`. The default configuration keeps the
run small (`B=2`, `R=2`, `K=2`), uses the zero imputer by default, and selects
the `2018 / main_2018` target. Edit that file to widen the notebook run.

The notebooks share path helpers and compact display utilities from
`nb/notebook_common.jl`.

## Outputs

Notebook artifacts are isolated under:

```text
nb/output/notebook_smoke
nb/output/notebook_smoke/cache
```

`nb/notebook_common.jl` resolves paths relative to the repository and guards
against writing notebook artifacts into the publication output tree.

## Relation to the Production Workflow

The notebooks are for inspection, explanation, and local debugging. They use the
same PrefPol package APIs as the production workflow, but they are not the
official replication entry point and they do not run the publication-scale
configuration.

Use the production runner for paper reproduction:

```bash
julia +1.11.9 --project=PrefPol PrefPol/composable_running/run_all_paper.jl --config PrefPol/config/publication.toml
```

Publication artifacts are written under `PrefPol/composable_running/output/`;
notebook artifacts remain under `nb/output/notebook_smoke`.
