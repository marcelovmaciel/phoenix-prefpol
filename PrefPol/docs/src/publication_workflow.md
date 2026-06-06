# Publication Workflow

```@meta
CurrentModule = PrefPol
```

The public reproduction path is the staged workflow under
`PrefPol/composable_running/`. Run it from the repository root with Julia
1.11.9:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/run_all_paper.jl \
  --config PrefPol/config/publication.toml
```

Validate the configuration before a full run:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/stages/00_validate_configs.jl \
  --config PrefPol/config/publication.toml
```

`PrefPol/config/publication.toml` is the manuscript-facing entry point. It
selects the 2006, 2018, and 2022 ESEB targets, candidate-set sizes
`m = 2, 3, 4, 5`, the nested `B = 30`, `R = 10`, `K = 10` stochastic tree, and
the publication measure set `Psi`, `R`, `HHI`, `RHHI`, `C`, and `D`.

## Stage Order

`run_all_paper.jl` calls the stage scripts in dependency/topological order. Shared stage runtime helpers live in `PrefPol/composable_running/stage_common.jl`, while individual numbered stages are debugging entry points. The ordered stages are:

1. `00_validate_configs.jl` validates the publication, year, plot, table, and
   artifact config files.
2. `01_bootstrap.jl` loads survey data, applies candidate-set rules, and draws
   weighted bootstrap branches.
3. `02_impute.jl` creates imputation branches within each bootstrap branch.
4. `03_linearize.jl` converts imputed weak score profiles to strict ranking
   profiles.
5. `04_measures.jl` computes global and grouped measures and writes variance
   decomposition tables.
6. `05_plot_global.jl` and `06_plot_group.jl` run in the plotting environment
   when plots are not skipped.
7. `07_extra_measures.jl`, `08_tables.jl`, and `09_extra_plots.jl` generate
   secondary effective-ranking summaries, tables, and extra plots.
8. `10_lambda_table.jl` runs only when `[lambda_table].enabled = true`.
9. `11_collect_paper_artifacts.jl` collects configured paper artifacts unless
   collection or plotting is skipped.

Use individual stage scripts for debugging a failed run. For ordinary
reproduction, use the wrapper so plot stages run under the intended plotting
project and artifact collection receives the same filters as upstream stages.

## Optional Filters

The wrapper accepts filters for targeted reruns:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/run_all_paper.jl \
  --config PrefPol/config/publication.toml \
  --year 2022 --scenario main_2022 --m 5 \
  --backend mice --linearizer pattern_conditional
```

`--dry-run` prints the selected plan without writing stage outputs. `--force`
regenerates cached outputs and should be used only when replacing existing
publication artifacts intentionally. `--skip-plots` and `--skip-collection`
are useful when validating non-plot pipeline stages on machines without the
plotting environment.
