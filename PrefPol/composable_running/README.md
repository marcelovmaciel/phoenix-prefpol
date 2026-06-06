# Brazil Article Replication Workflow

`PrefPol/composable_running/` contains the staged runner for the applied
ESEB/Brazil article replication. It is an orchestration layer around the
statistical code in `PrefPol/src/` and the plotting extension; the formal
preference objects and social-choice definitions live in `Preferences`.

From the repository root, reproduce the article figures and tables with:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/run_all_paper.jl \
  --config PrefPol/config/publication.toml
```

Validate the publication configuration before a full run with:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/stages/00_validate_configs.jl \
  --config PrefPol/config/publication.toml
```

Use Julia 1.11.9 explicitly. In this repository, plain `julia` may resolve to a
newer Julia and fail because of depot or package compatibility.

## Stage Organization

Numbered stage files are ordered by dependency, so the numeric order is the
topological order used by `run_all_paper.jl`. Shared stage runtime code lives in
the unnumbered helper `PrefPol/composable_running/stage_common.jl`; numbered
stages should not include other numbered stages. Individual stage scripts remain
debugging entry points for a failed wrapper run, while the publication entry
point remains:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/run_all_paper.jl \
  --config PrefPol/config/publication.toml
```

## Full Smoke Validation

For CI-style validation of the full composable paper workflow without running
the publication-scale B=30/R=10/K=10 job, use:

```bash
julia +1.11.9 --project=PrefPol PrefPol/composable_running/test_full_smoke_run.jl
```

The script uses `PrefPol/config/publication_smoke_full.toml`, deletes only
`PrefPol/composable_running/output/publication_smoke_full`, runs the wrapper
with B=1/R=1/K=2, and checks manifests plus collected paper artifacts.

## Publication Config

`PrefPol/config/publication.toml` is the public entry point for the article. It
selects the three Brazil/ESEB targets used by the paper:

- `2006 / main_2006`
- `2018 / main_2018`
- `2022 / main_2022`

The config drives the paper's survey-wave loading, candidate-set selection,
resampling, imputation, linearization, measure computation, plots, tables, and
artifact collection. Generated outputs are isolated under:

```text
PrefPol/composable_running/output/publication/
```

## Inputs

The year configs under `PrefPol/config/` define the raw ESEB data source for
each wave. Download the survey files listed in `PrefPol/README.md`, then update
the corresponding `data_file` paths before running the workflow.

R-backed imputation requires R plus the project R packages documented in
`PrefPol/README.md`.

## Outputs

The wrapper writes stage manifests, cached intermediate data, figures, tables,
and collected paper artifacts below the publication output root. The main
reviewer-facing artifact directory is:

```text
PrefPol/composable_running/output/publication/paper_artifacts/
```

Inspect collected artifacts with:

```bash
find PrefPol/composable_running/output/publication/paper_artifacts -maxdepth 1 -type f -print | sort
```

Inspect stage manifests with:

```bash
find PrefPol/composable_running/output/publication/manifests -type f -print | sort
```

## Troubleshooting

If imputation fails, check that R is on `PATH` and that the required R packages
are installed.

If plotting fails when running a stage directly, run plotting stages through
`run_all_paper.jl` or use the plotting environment at
`--project=PrefPol/running/plotting_env`. The wrapper selects that environment
automatically for plot stages.

`10_lambda_table.jl` is optional. It is skipped unless `[lambda_table].enabled`
is true in the orchestration config.

If a downstream stage reports a missing upstream manifest, rerun from the
earliest missing stage or rerun the full wrapper. Use `--force` only when
intentionally regenerating publication outputs or replacing existing cached
state.
