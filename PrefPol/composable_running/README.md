# Composable Running Operational Manual

`PrefPol/composable_running/` is the orchestration-only layer for the
replication workflow. It turns configured survey waves, scenarios, backends,
linearizers, measures, plot specs, table specs, Lambda specs, and artifact
collection specs into independently callable stages.

This layer calls the statistical implementation in `PrefPol/src/` and the
plotting extension loaded through the plotting environment. It does not replace
the statistical pipeline or redefine measures. The older workflow scripts in
`PrefPol/running/` are obsolete and should not guide new refactoring. The only
temporary dependency on `PrefPol/running/` is the plotting environment at
`PrefPol/running/plotting_env`, which should be moved before removing the
directory.

Do not add `PrefPol/composable_running/lib/`. Shared stage behavior should live
in `PrefPol/src/` or in the plotting extension when it belongs there.

## Julia Command

Use Julia 1.11.9 explicitly in this repository:

```bash
julia +1.11.9 --project=PrefPol
```

In this environment, plain `julia` may resolve to Julia 1.12.2 and can fail
because of depot/package issues such as `MbedTLS_jll`. The wrapper scripts use
`julia +1.11.9` internally.

When running plotting stages directly, use the plotting environment:

```bash
julia +1.11.9 --project=PrefPol/running/plotting_env
```

The paper wrapper selects the plotting environment automatically for
plot stages. This environment currently lives under the obsolete `running/`
folder only as a transitional location; future cleanup should move it to a
non-legacy path such as `PrefPol/plotting_env/` and then update these commands.

## Config Files

`PrefPol/config/2006.toml`, `PrefPol/config/2018.toml`, and
`PrefPol/config/2022.toml` define the survey waves: data sources, candidate
sets, scenarios, demographic groupings, and year-specific metadata.

`PrefPol/config/publication.toml` is the clean manuscript-facing orchestration
config. It controls the submitted article target years/scenarios, m ranges,
B/R/K, cache/output roots, imputer backends, linearizer policies, and the
publication measure set.

`PrefPol/config/plot_specs.toml` controls global plot targets, group heatmap
targets, plotting filters, formats, measure labels, complement rules, extra
plot settings, and paper-facing plot filenames.

`PrefPol/config/table_specs.toml` controls effective-ranking tables and Lambda
appendix tables. Lambda remains `Lambda = D / W`, using the aggregate or
group-size-weighted quantities produced by the current pipeline.

`PrefPol/config/paper_artifacts.toml` maps generated outputs to manuscript
artifact filenames and controls copy behavior, destination roots, and whether
`writing/imgs/` is updated.

In short:

- Wave/data definitions: `2006.toml`, `2018.toml`, `2022.toml`
- Publication target years/scenarios: `publication.toml`
- B/R/K: `publication.toml`
- Imputer backends: `[run].imputer_backends`
- Linearizer policies: `[run].linearizer_policies`
- Measures: `[run].measures` and `[measure_sets]`
- Global plot targets: `plot_specs.toml` plus orchestration overrides
- Group plot targets: `plot_specs.toml` plus orchestration overrides
- Table specs: `table_specs.toml` plus orchestration overrides
- Lambda table specs: `[lambda_table]` in `table_specs.toml` plus overrides
- Artifact collection: `paper_artifacts.toml` plus `[paper_artifacts.collection]` overrides

## Standard Workflows

Publication-facing replication:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/run_all_paper.jl \
  --config PrefPol/config/publication.toml
```

`publication.toml` is the clean manuscript-facing entry point. It runs only the
measures used in the article: `Psi`, `R`, `HHI`, `RHHI`, `C`, and `D`. It
isolates outputs, cache, manifests, and collected paper artifacts under:

```text
PrefPol/composable_running/output/publication/
```

## Stage Commands

Use stage-by-stage execution when debugging, rerunning one failed stage,
validating intermediate outputs, or avoiding a full wrapper rerun. The commands
below use the publication-facing config.

```bash
julia +1.11.9 --project=PrefPol PrefPol/composable_running/stages/00_validate_configs.jl --config PrefPol/config/publication.toml
julia +1.11.9 --project=PrefPol PrefPol/composable_running/stages/01_bootstrap.jl --config PrefPol/config/publication.toml
julia +1.11.9 --project=PrefPol PrefPol/composable_running/stages/02_impute.jl --config PrefPol/config/publication.toml
julia +1.11.9 --project=PrefPol PrefPol/composable_running/stages/03_linearize.jl --config PrefPol/config/publication.toml
julia +1.11.9 --project=PrefPol PrefPol/composable_running/stages/04_measures.jl --config PrefPol/config/publication.toml
julia +1.11.9 --project=PrefPol/running/plotting_env PrefPol/composable_running/stages/05_plot_global.jl --config PrefPol/config/publication.toml
julia +1.11.9 --project=PrefPol/running/plotting_env PrefPol/composable_running/stages/06_plot_group.jl --config PrefPol/config/publication.toml
julia +1.11.9 --project=PrefPol PrefPol/composable_running/stages/07_extra_measures.jl --config PrefPol/config/publication.toml
julia +1.11.9 --project=PrefPol/running/plotting_env PrefPol/composable_running/stages/08_extra_plots.jl --config PrefPol/config/publication.toml
julia +1.11.9 --project=PrefPol PrefPol/composable_running/stages/09_tables.jl --config PrefPol/config/publication.toml
julia +1.11.9 --project=PrefPol PrefPol/composable_running/stages/10_lambda_table.jl --config PrefPol/config/publication.toml
julia +1.11.9 --project=PrefPol PrefPol/composable_running/stages/11_collect_paper_artifacts.jl --config PrefPol/config/publication.toml --artifact-config PrefPol/config/paper_artifacts.toml
```

The wrapper order is:

```text
00_validate_configs.jl
01_bootstrap.jl
02_impute.jl
03_linearize.jl
04_measures.jl
05_plot_global.jl
06_plot_group.jl
07_extra_measures.jl
09_tables.jl
08_extra_plots.jl
10_lambda_table.jl
11_collect_paper_artifacts.jl
```

`08_extra_plots.jl` runs after `09_tables.jl` in the wrapper because the
effective-ranking evolution plot consumes table outputs.

## Stage DAG

Core statistical stages are linear:

```text
validate_configs
-> bootstrap
-> impute
-> linearize
-> measures
```

Downstream artifact stages branch from those outputs:

```text
measures -> global plots
measures -> group plots
linearizations/results -> extra measures
extra measures/results -> tables
extra measures/results/tables -> extra plots
measures -> Lambda table
plots/tables/Lambda -> paper artifact collection
```

## Output Structure

Default generated state lives under:

```text
PrefPol/composable_running/output/
```

The publication-facing run uses:

```text
PrefPol/composable_running/output/publication/
```

Important subfolders:

- `bootstraps/`: bootstrap stage manifest copy
- `imputations/`: imputation stage manifest copy
- `linearizations/`: linearization stage manifest copy
- `measures/`: measure tables and decomposition outputs
- `plots/`: global and group plot outputs
- `extra_measures/`: ranking support and effective-count diagnostics
- `extra_plots/`: effective-ranking plots and the absolute variance-decomposition plot
- `tables/`: effective-ranking CSV, Markdown, and TeX tables
- `appendices/`: Lambda appendix CSV, audit, and TeX outputs
- `paper_artifacts/`: collected manuscript-facing artifacts
- `manifests/`: stage manifests and collection manifests
- `logs/`: reserved for logs
- `cache/`: pipeline cache for observed, resample, imputed, linearized, measure, and result artifacts

## Manifests

Manifests record which stage ran, what inputs and settings were used, and where
outputs landed. They are the first place to inspect when a downstream stage
cannot find inputs.

Expected manifests include:

- `config_validation_manifest.csv`
- `bootstrap_manifest.csv`
- `imputation_manifest.csv`
- `linearization_manifest.csv`
- `measure_manifest.csv`
- `run_manifest.csv`
- `plot_manifest.csv`
- `group_plot_manifest.csv`
- `extra_measure_manifest.csv`
- `extra_plot_manifest.csv`
- `table_manifest.csv`
- `lambda_table_manifest.csv`
- `paper_artifact_manifest.csv`

Inspect all manifests:

```bash
find PrefPol/composable_running/output -path '*manifests*' -type f -print
```

Inspect publication manifests:

```bash
find PrefPol/composable_running/output/publication/manifests -type f -print | sort
```

Inspect a manifest directly:

```bash
julia +1.11.9 --project=PrefPol -e 'using CSV, DataFrames; println(CSV.read("PrefPol/composable_running/output/publication/manifests/run_manifest.csv", DataFrame))'
```

## Paper Artifact Collection

`PrefPol/config/paper_artifacts.toml` maps generated stage outputs to
manuscript-facing filenames. For example, plot outputs become
`2006_global_main.png`, `2018_group.png`, and related filenames in the
collection destination.

The default collection destination is:

```text
PrefPol/composable_running/output/paper_artifacts/
```

For the publication-facing config, the override destination is:

```text
PrefPol/composable_running/output/publication/paper_artifacts/
```

The current artifact spec uses `copy_mode = "copy"` and
`update_writing_imgs = false`, so the publication run does not update
`writing/imgs/`.

Inspect collected artifacts:

```bash
find PrefPol/composable_running/output/publication/paper_artifacts -type f -maxdepth 1 -print | sort
```

## Troubleshooting

Use `julia +1.11.9`, not plain `julia`, when running this repository in the
current environment.

If imputation fails, check R, `RCall`, and R package availability. MICE-backed
paths require the R-side dependencies used by the project, including `haven`,
`mice`, and `PerMallows` where relevant.

If a plot stage fails immediately, make sure it is run with
`--project=PrefPol/running/plotting_env` or through a wrapper. The plotting
environment must load `CairoMakie` and activate `PrefPolPlottingExt`. The
`running/plotting_env` path is transitional; it should be moved before deleting
the obsolete `PrefPol/running/` workflow directory.

If a plot stage warns that it is using fallback defaults, check
`PrefPol/config/plot_specs.toml` and any orchestration overrides. Paper runs
should use configured plot targets.

If a table stage fails, check `PrefPol/config/table_specs.toml`, the
`extra_measure_manifest.csv`, and the effective-count source under
`extra_measures/effective_counts/`.

If a downstream stage reports a missing upstream manifest, rerun from the
earliest missing stage rather than only rerunning the failing stage.

If the Lambda table fails because `:lambda_sep` is missing, confirm that
`lambda_sep` is included in `[run].measures` and that `04_measures.jl`
completed for the selected backend, linearizer, targets, and m values.

If paper artifact collection reports a missing source artifact, inspect the
source manifest named in the error and confirm the artifact filename, backend,
linearizer, and target filters match `paper_artifacts.toml`.

Use `--force` when intentionally regenerating stage outputs or replacing an
existing cache/output set. Avoid `--force` for casual inspection because it can
increase runtime and rewrite generated state.

For a failed wrapper run, rerun the failed stage directly after applying a fix.
If the fix changes upstream data or manifests, rerun from the earliest affected
stage or rerun the wrapper.

## Current Validation Status

Latest known status:

- Publication-facing config validation: `00_validate_configs.jl --config
  PrefPol/config/publication.toml` passed on 2026-06-04.
- Full publication-facing workflow: not recorded here.

The publication-facing config uses the three main article targets:

- `2006 / main_2006`
- `2018 / main_2018`
- `2022 / main_2022`
