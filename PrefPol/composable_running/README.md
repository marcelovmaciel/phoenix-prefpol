# Composable Running

This folder is the scaffold for the composable replication orchestration
described in `PLAN.md`.

Phase 1 created the directory structure. Phase 2 identified the existing
`PrefPol/src/` utilities that later stage scripts should reuse; see
`UTILITY_INVENTORY.md`.

Phase 5 separates the statistical pipeline into independently callable
bootstrap, imputation, linearization, and measure stages. `stages/01_bootstrap.jl`
through `stages/03_linearize.jl` call the public nested-pipeline `ensure_*`
APIs, while `stages/04_measures.jl` finishes measurement and writes aggregate
tables plus manifests.

## Layout

```text
PrefPol/composable_running/
  PLAN.md
  README.md
  UTILITY_INVENTORY.md
  run_all_smoke.jl
  run_all_paper.jl
  stages/
  output/
```

The `output/` tree is reserved for generated artifacts and manifests. Treat it
as generated state, not source.

## Commands

Phase 4 supports config-driven runs when an orchestration TOML exists, but can
also run a small default plan for local smoke checks:

```bash
julia +1.11.9 --project=PrefPol PrefPol/composable_running/stages/00_validate_configs.jl
julia +1.11.9 --project=PrefPol PrefPol/composable_running/stages/01_bootstrap.jl --smoke-test --dry-run
julia +1.11.9 --project=PrefPol PrefPol/composable_running/stages/02_impute.jl --smoke-test --dry-run
julia +1.11.9 --project=PrefPol PrefPol/composable_running/stages/03_linearize.jl --smoke-test --dry-run
julia +1.11.9 --project=PrefPol PrefPol/composable_running/stages/04_measures.jl --smoke-test --dry-run
julia +1.11.9 --project=PrefPol/running/plotting_env PrefPol/composable_running/stages/05_plot_global.jl --smoke-test --dry-run
julia +1.11.9 --project=PrefPol/running/plotting_env PrefPol/composable_running/stages/06_plot_group.jl --smoke-test --dry-run
julia +1.11.9 --project=PrefPol PrefPol/composable_running/stages/07_extra_measures.jl --smoke-test --dry-run
julia +1.11.9 --project=PrefPol PrefPol/composable_running/stages/09_tables.jl --smoke-test --dry-run
julia +1.11.9 --project=PrefPol/running/plotting_env PrefPol/composable_running/stages/08_extra_plots.jl --smoke-test --dry-run
julia +1.11.9 --project=PrefPol PrefPol/composable_running/stages/10_lambda_table.jl --smoke-test --dry-run
julia +1.11.9 --project=PrefPol PrefPol/composable_running/stages/11_collect_paper_artifacts.jl --dry-run
julia +1.11.9 --project=PrefPol PrefPol/composable_running/stages/04_measures.jl --config PrefPol/config/orchestration.toml
julia +1.11.9 --project=PrefPol PrefPol/composable_running/run_all_smoke.jl --config PrefPol/config/smoke_test.toml
julia +1.11.9 --project=PrefPol PrefPol/composable_running/run_all_paper.jl --config PrefPol/config/orchestration.toml
```

## Utility Reuse Rules

Stage scripts should call the public `PrefPol` APIs listed in
`UTILITY_INVENTORY.md` for wave loading, spec construction, result IO, result
tables, plot data, and Lambda helpers.

For CSV manifests and tables, use `CSV.read(path, DataFrame)` and
`CSV.write(path, df)`. Do not copy the hand-written CSV parser/writer helpers
from `PrefPol/running/*.jl`.

Do not add `PrefPol/composable_running/lib/`. If multiple stages need the same
helper, add it to `PrefPol/src/` or the plotting extension in the relevant
later phase.
