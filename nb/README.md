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

## Notebook provenance and integrity checks

Run the notebook checks before relying on notebook outputs in analysis or reporting. These checks verify that notebook summaries are derived from the configured survey data through PrefPol APIs and that notebook outputs remain isolated from the production publication workflow.

Run the lightweight source check from the repository root:

```bash
julia +1.11.9 --project=nb nb/check_notebooks.jl
```

Run the provenance and dynamic integrity check from the repository root:

```bash
julia +1.11.9 --project=nb nb/test_notebook_provenance.jl
```

The provenance check uses the real survey data configured through `nb/notebook_config.toml` and the same notebook helpers used by the notebooks. It does not use toy data, and it fails clearly if the configured raw ESEB data path is missing. It scans notebook code for manually embedded empirical result values, manually assembled result tables, production-output writes, and calls into the production CLI stage layer. It also runs a tiny real PrefPol pipeline and verifies that outputs respond to a meaningful config perturbation.

The provenance report is written to:

```text
nb/output/notebook_smoke/provenance/notebook_provenance_report.md
```

Passing this check does not prove the paper is correct. It verifies the narrower notebook-integrity property that displayed notebook results are connected to the configured data and PrefPol workflow.

## Julia 1.11.9 environment repair

Use Julia 1.11.9 for the notebook environment. If Pluto reports that `nb/Manifest.toml` was resolved with another Julia version, repair the environment with Julia 1.11.9 from the repository root:

```bash
julia +1.11.9 --project=nb -e 'using Pkg; Pkg.resolve(); Pkg.instantiate()'
```

If the error mentions that package source code cannot be located after switching Julia versions, clear the compiled cache for Julia 1.11 and retry. This removes only compiled cache files; packages and manifests remain intact.

```bash
mv ~/.julia/compiled/v1.11 /tmp/julia-compiled-v1.11-backup-$(date +%Y%m%d%H%M%S)
julia +1.11.9 --project=nb -e 'using Pkg; Pkg.instantiate()'
```

Then start Pluto:

```bash
julia +1.11.9 --project=nb -e 'using Pkg; Pkg.instantiate(); using Pluto; Pluto.run()'
```

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
