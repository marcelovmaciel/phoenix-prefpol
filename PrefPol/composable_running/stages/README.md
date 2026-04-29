# Stages

This directory is reserved for the ordered composable stage scripts described
in `../PLAN.md`.

Phase 1 created the directory. Phase 2 identified the public utility surface
that future stages should reuse; see `../UTILITY_INVENTORY.md`. Phase 5 wires
the first statistical stages to public nested-pipeline `ensure_*` APIs.

Current implemented entrypoints:

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

`01_bootstrap.jl`, `02_impute.jl`, and `03_linearize.jl` create their stage
artifacts independently through `PrefPol.ensure_resamples!`,
`PrefPol.ensure_imputations!`, and `PrefPol.ensure_linearizations!`.
`04_measures.jl` calls `PrefPol.ensure_measures!` and writes aggregate measure
tables plus manifests. `05_plot_global.jl` and `06_plot_group.jl` read those
manifests and cached `PipelineResult`s, then write plots and compact plot CSVs
through the CairoMakie plotting extension. Phase 8 adds extra diagnostics,
effective-ranking tables and plots, and Lambda appendix tables. Phase 9
collects paper-facing artifacts configured in `PrefPol/config/paper_artifacts.toml`.

Stage scripts should be thin shell entrypoints around `PrefPol` public APIs.
Use `CSV.jl` for manifests and tables; do not copy CSV helper functions from
the old `PrefPol/running/` scripts.
