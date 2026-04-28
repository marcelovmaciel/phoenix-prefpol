# Stages

This directory is reserved for the ordered composable stage scripts described
in `../PLAN.md`.

Phase 1 created the directory only. Phase 2 identified the public utility
surface that future stages should reuse; see `../UTILITY_INVENTORY.md`.

Later phases should add the planned stage entrypoints:

- `00_validate_configs.jl`
- `01_bootstrap.jl`
- `02_impute.jl`
- `03_linearize.jl`
- `04_measures.jl`
- `05_plot_global.jl`
- `06_plot_group.jl`
- `07_extra_measures.jl`
- `08_extra_plots.jl`
- `09_tables.jl`
- `10_lambda_table.jl`
- `11_collect_paper_artifacts.jl`

Stage scripts should be thin shell entrypoints around `PrefPol` public APIs.
Use `CSV.jl` for manifests and tables; do not copy CSV helper functions from
the old `PrefPol/running/` scripts.
