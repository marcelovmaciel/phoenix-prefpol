# Publication Stages

This directory contains thin stage entry points used by
`PrefPol/composable_running/run_all_paper.jl` for the Brazil/ESEB article
replication.

The public reproduction path is the wrapper:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/run_all_paper.jl \
  --config PrefPol/config/publication.toml
```

Validate the publication configuration directly with:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/stages/00_validate_configs.jl \
  --config PrefPol/config/publication.toml
```

Numbered stage files are in dependency/topological order. Shared stage code lives
in the unnumbered helper `PrefPol/composable_running/stage_common.jl`, and no
numbered stage should include another numbered stage.

Use individual stage scripts only when debugging a failed publication run. Stage
scripts should stay as shell entry points around `PrefPol` public APIs and use
`CSV.jl` for manifests and tables.

`10_lambda_table.jl` is optional in the publication wrapper. It runs, and its
paper artifact is collected, only when the orchestration config includes:

```toml
[lambda_table]
enabled = true
```
