# Composable Running

This folder is the scaffold for the composable replication orchestration
described in `PLAN.md`.

Phase 1 created the directory structure. Phase 2 identified the existing
`PrefPol/src/` utilities that later stage scripts should reuse; see
`UTILITY_INVENTORY.md`.

The runnable stage scripts and configuration-driven orchestration are
intentionally not implemented yet. Later phases should keep orchestration
scripts thin and put reusable Julia logic in `PrefPol/src/` or
`PrefPol/ext/PrefPolPlottingExt.jl`.

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

## Planned Commands

These entrypoints are placeholders until later phases implement the individual
stages:

```bash
julia --project=PrefPol PrefPol/composable_running/run_all_smoke.jl --config PrefPol/config/smoke_test.toml
julia --project=PrefPol PrefPol/composable_running/run_all_paper.jl --config PrefPol/config/orchestration.toml
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
