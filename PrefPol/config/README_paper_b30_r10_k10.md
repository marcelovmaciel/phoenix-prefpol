# B30 R10 K10 Paper Run

This run uses the composable paper pipeline with:

- `B = 30`
- `R = 10`
- `K = 10`
- output root: `PrefPol/composable_running/output/paper_b30_r10_k10`
- cache root: `PrefPol/composable_running/output/paper_b30_r10_k10/cache`

The run config is:

```text
PrefPol/config/orchestration_b30_r10_k10.toml
```

The matching paper artifact collection config is:

```text
PrefPol/config/paper_artifacts_b30_r10_k10.toml
```

Use these two files together. Do not collect this run with
`PrefPol/config/paper_artifacts.toml`, because that default artifact config
targets the default output tree rather than the `paper_b30_r10_k10` output tree.

## What This Configuration Runs

The run covers the configured paper targets:

- 2006 / `main_2006`, `m = 2, 3, 4, 5`
- 2018 / `main_2018`, `m = 2, 3, 4, 5`
- 2022 / `main_2022`, `m = 2, 3, 4, 5`

The measure stage runs:

- imputer backends: `mice`, `zero`
- linearizers: `pattern_conditional`, `random_ties`
- measures: `Psi`, `R`, `HHI`, `RHHI`, `C`, `D`, `O`, `S`, `lambda_sep`

Paper-facing plots and tables are collected from the `mice` /
`pattern_conditional` rows, matching `paper_artifacts_b30_r10_k10.toml`.

The variance decomposition plot is configured as absolute variance:

```toml
[extra_plots.variance_decomposition]
value_kind = "variance"
```

## Full Pipeline Command

Run the whole paper pipeline from the repository root:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/run_all_paper.jl \
  --config PrefPol/config/orchestration_b30_r10_k10.toml \
  --artifact-config PrefPol/config/paper_artifacts_b30_r10_k10.toml \
  --force
```

The wrapper runs stages in this order:

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

## Final Paper Artifacts

After a successful run, the collected paper-facing outputs are in:

```text
PrefPol/composable_running/output/paper_b30_r10_k10/paper_artifacts/
```

Expected collected files:

```text
2006_global_main.png
2018_global_main.png
2022_global_main.png
2006_group.png
2018_group.png
2022_group.png
effective_rankings_evolution_1x2.png
effective_rankings.tex
variance_decomposition_2022.png
appendix_lambda_grouping_tables.tex
```

The collection manifest is:

```text
PrefPol/composable_running/output/paper_b30_r10_k10/manifests/paper_artifact_manifest.csv
```

Use it to verify that every collected source path is under
`PrefPol/composable_running/output/paper_b30_r10_k10/` and that source and
destination hashes match.

## Quick Verification Commands

After the run completes:

```bash
find PrefPol/composable_running/output/paper_b30_r10_k10/paper_artifacts \
  -maxdepth 1 -type f -print | sort
```

Check that collected artifacts came from this run:

```bash
julia --project=PrefPol -e 'using CSV, DataFrames; df = CSV.read("PrefPol/composable_running/output/paper_b30_r10_k10/manifests/paper_artifact_manifest.csv", DataFrame); @assert all(occursin.("PrefPol/composable_running/output/paper_b30_r10_k10/", String.(df.source_path))); @assert all(String.(df.source_hash) .== String.(df.destination_hash)); println("paper_b30_r10_k10 artifact collection verified")'
```

Check BRK values in key downstream data:

```bash
julia --project=PrefPol -e 'using CSV, DataFrames; for p in ["PrefPol/composable_running/output/paper_b30_r10_k10/manifests/run_manifest.csv", "PrefPol/composable_running/output/paper_b30_r10_k10/tables/effective_rankings/effective_rankings.csv", "PrefPol/composable_running/output/paper_b30_r10_k10/extra_plots/effective_rankings/effective_rankings_evolution_plot_data.csv", "PrefPol/composable_running/output/paper_b30_r10_k10/extra_plots/variance_decomposition/variance_decomposition_2022_plot_data.csv"]; df = CSV.read(p, DataFrame); println(p, " B=", sort(unique(Int.(df.B))), " R=", sort(unique(Int.(df.R))), " K=", sort(unique(Int.(df.K)))); end'
```
