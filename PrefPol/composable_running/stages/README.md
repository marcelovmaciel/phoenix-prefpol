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

Use individual stage scripts only when debugging a failed publication run. Stage
scripts should stay as shell entry points around `PrefPol` public APIs and use
`CSV.jl` for manifests and tables.
