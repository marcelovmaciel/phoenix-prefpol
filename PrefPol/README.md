# PrefPol Brazil/ESEB Replication

`PrefPol` is the applied ESEB/Brazil replication package in this repository. It
loads the survey waves, applies the article's candidate-set and preprocessing
rules, runs bootstrap/imputation/linearization, computes the paper measures, and
collects the paper-facing figures and tables. Formal preference representations
and social-choice definitions live in the sibling `Preferences` package.

From the repository root, reproduce the Brazil polarization article with:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/run_all_paper.jl \
  --config PrefPol/config/publication.toml
```

To validate the publication configuration before a full run:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/stages/00_validate_configs.jl \
  --config PrefPol/config/publication.toml
```

## Dependencies

- **Julia 1.11.9**. `PrefPol/Manifest.toml` records this version for the
  publication environment. Instantiate it before reproducing the workflow:
  ```bash
  julia +1.11.9 --project=PrefPol -e 'using Pkg; Pkg.instantiate()'
  ```
- **R on `PATH`**. `PrefPol` uses `RCall` for SPSS loading and, when configured,
  MICE imputation. Run `R --version` from the same shell that starts Julia.
- **R packages used by the active workflow**:
  ```r
  install.packages(c("haven", "mice"))
  ```
  `haven` reads the raw SPSS `.sav` survey files. `mice` powers the `:mice`
  imputation backend used in the publication configuration. `PerMallows` is not
  used by the active publication workflow in this repository; install it only
  for legacy or experimental scripts that explicitly require it.

Plotting stages run under `PrefPol/running/plotting_env` through
`run_all_paper.jl`, so the wrapper is the preferred public entry point for
figures. That environment should provide `CairoMakie` and related plotting
dependencies.

## Data

Each election year is configured by a TOML file in `PrefPol/config/`. Within
each year TOML, the `data_file` field points to the raw dataset on disk. Obtain
the datasets from:

- 2006: <https://www.cesop.unicamp.br/por/banco_de_dados/v/1583>
- 2018: <https://www.cesop.unicamp.br/por/banco_de_dados/v/4538>
- 2022: <https://www.cesop.unicamp.br/por/banco_de_dados/v/4680>

Copy the downloaded `.sav` files to a convenient local location and update the
`data_file` paths used by the year configs. Do not commit raw survey files or
machine-specific data paths.

## How the Workflow Works

The year configs (`2006.toml`, `2018.toml`, and `2022.toml`) define raw data
loaders, `.sav` paths, candidates, demographic groupings, score variables, and
forced candidate-set scenarios. `PrefPol/config/publication.toml` selects the
public reproduction targets, `m_values = [2, 3, 4, 5]`, `B = 30`, `R = 10`,
`K = 10`, imputation backends, linearizer policies, measures, cache root, and
output root.

The uncertainty tree is nested:

- `B` draws weighted bootstrap resamples from each configured survey target.
- `R` creates imputation replicates within each bootstrap branch.
- `K` creates strict linearizations for each imputed weak score profile.

For the current publication config, the backends are `mice` and `zero`, and the
linearizers are `pattern_conditional` and `random_ties`. The primary collected
paper artifacts default to `mice` with `pattern_conditional`.

## Staged Workflow

`PrefPol/composable_running/run_all_paper.jl` runs the stages in order:

1. `00_validate_configs.jl` validates publication, year, plot, table, and
   artifact configs.
2. `01_bootstrap.jl` loads survey data, applies candidate-set logic, and draws
   weighted resamples.
3. `02_impute.jl` fills score missingness using `mice`, `zero`, or `random`
   when those backends are configured.
4. `03_linearize.jl` converts complete weak score profiles to strict rankings
   with `pattern_conditional` or `random_ties`.
5. `04_measures.jl` computes global and group measures plus variance
   decomposition outputs.
6. `05_plot_global.jl` and `06_plot_group.jl` produce publication plots in the
   plotting environment.
7. `07_extra_measures.jl`, `08_extra_plots.jl`, and `09_tables.jl` produce
   effective-count summaries, extra plots, and tables.
8. `10_lambda_table.jl` runs only when `[lambda_table] enabled = true` in the
   orchestration config.
9. `11_collect_paper_artifacts.jl` collects the reviewer-facing paper artifacts
   unless plots or collection are explicitly skipped.

Use individual stage scripts when debugging a failed run. For normal
reproduction, use the wrapper so plotting and collection happen with the
intended environments and arguments.

## Outputs and Navigation

The publication config writes generated files under:

- `PrefPol/composable_running/output/publication/` for the main generated root.
- `PrefPol/composable_running/output/publication/cache/` for cached nested
  pipeline artifacts.
- `PrefPol/composable_running/output/publication/paper_artifacts/` for the
  reviewer-facing collected figures and tables.
- `PrefPol/composable_running/output/publication/manifests/` for run metadata.

The manifest directory records stage products and provenance, including config,
bootstrap, imputation, linearization, measure, plot, group plot, extra measure,
extra plot, table, and paper artifact manifests. When looking for final paper
outputs, start with `paper_artifacts/`; when diagnosing a failed or partial run,
start with `manifests/` and then follow the recorded paths into `cache/`,
`measures/`, `plots/`, `tables/`, or `extra_measures/`.

## Maintainer Orientation

- `PrefPol/src/preprocessing_general.jl` contains SPSS/R integration, R-backed
  MICE imputation helpers, generic score preprocessing, and linearization
  adapters.
- `PrefPol/src/preprocessing_specific.jl` contains year-specific ESEB recodes,
  loaders, and survey-wave transformations.
- `PrefPol/src/nested_pipeline.jl` contains the nested
  bootstrap/imputation/linearization pipeline and cache layout.
- `PrefPol/src/survey_config.jl` parses year configs, builds survey-wave
  configs, resolves candidate sets, loads raw survey data, and keeps the raw
  profile helper entry points used by tests and the pipeline.
- `Preferences/` owns the formal preference, profile, ranking, aggregation, and
  measure primitives used by `PrefPol`.

## Troubleshooting

- If `.sav` loading fails, check the year config's `data_file`, confirm `R` is
  on `PATH`, and verify that `haven` is installed in the R library visible to
  `RCall`.
- If `:mice` imputation fails, verify the R `mice` package installation and
  confirm that `RCall` is bound to the intended R installation.
- If plotting fails, use `run_all_paper.jl` rather than invoking plotting stages
  directly, or run the plotting stages with the `PrefPol/running/plotting_env`
  project.
- If downstream manifests are missing, rerun from the earliest missing stage or
  rerun the wrapper with the same `--config`. Add `--force` only when cached
  successful outputs should be regenerated.
