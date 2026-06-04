# Polarization Paper Execution Path

This note isolates the current minimal path for reproducing and updating the
polarization-paper artifacts. It is intentionally not a refactor plan. The
current paper workflow is `PrefPol/composable_running`, and the manuscript
currently consumes files under `writing/imgs/`.

I inspected this using the existing `.understand-anything/knowledge-graph.json`
as an architecture map, then checked the current repository files directly. The
understanding graph exists but was built for an older commit, so the concrete
paths and details below come from the current files.

## 1. Minimal Execution Path

The minimal polarization-paper workflow is the wrapper:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/run_all_paper.jl \
  --config PrefPol/config/orchestration.toml
```

That wrapper runs the paper path in this order:

1. `PrefPol/composable_running/stages/00_validate_configs.jl`
2. `PrefPol/composable_running/stages/01_bootstrap.jl`
3. `PrefPol/composable_running/stages/02_impute.jl`
4. `PrefPol/composable_running/stages/03_linearize.jl`
5. `PrefPol/composable_running/stages/04_measures.jl`
6. `PrefPol/composable_running/stages/05_plot_global.jl`
7. `PrefPol/composable_running/stages/06_plot_group.jl`
8. `PrefPol/composable_running/stages/07_extra_measures.jl`
9. `PrefPol/composable_running/stages/09_tables.jl`
10. `PrefPol/composable_running/stages/08_extra_plots.jl`
11. `PrefPol/composable_running/stages/10_lambda_table.jl`
12. `PrefPol/composable_running/stages/11_collect_paper_artifacts.jl`

Plotting stages use the plotting environment automatically through the wrapper:

```bash
julia +1.11.9 --project=PrefPol/running/plotting_env
```

This plotting environment path is transitional. The `PrefPol/running/` workflow
is obsolete; move `PrefPol/running/plotting_env` to a non-legacy path before
removing the old running directory.

Do not include these for paper 1 unless a specific manuscript need is found:
`PrefPol/composable_running/run_single_peakedness.jl`,
`PrefPol/composable_running/make_single_peakedness_report_artifacts.jl`,
`PrefPol/composable_running/run_majority_graph_support_reports.jl`,
`PrefPol/composable_running/majority_graph_support_*.jl`,
`PrefPol/running/*`, and `PrefPol/exploratory/*`.

## 2. Required Configs

Required configs for the polarization-paper path:

- `PrefPol/config/2006.toml`, `2018.toml`, `2022.toml`: survey waves, raw data path, data loader, candidate universe, forced paper scenario, demographic groupings, `m_values_range`, and year metadata.
- `PrefPol/config/orchestration.toml`: default paper run shape: paper targets, `B`, `R`, `K`, output/cache roots, imputer backends, linearizer policies, consensus tie policy, measures, and extra-measure settings.
- `PrefPol/config/plot_specs.toml`: global plots, group heatmaps, plotting filters, plot measures, groupings by wave, O-complement display policy, O-smoothed settings, extra plots, and variance-decomposition plot target.
- `PrefPol/config/table_specs.toml`: effective rankings / ENRP table specs and Lambda appendix table specs.
- `PrefPol/config/paper_artifacts.toml`: final paper-facing artifact collection, destination filenames, copy behavior, default backend/linearizer, and whether `writing/imgs` is updated.

Control map:

- Years/waves: `2006.toml`, `2018.toml`, `2022.toml`.
- Scenarios: `forced_scenarios` in the year TOMLs; selected by `[[targets]]` in `orchestration.toml`, and plot/table/lambda target sections.
- Candidate sets: `candidates`, `max_candidates`, and `forced_scenarios[*].candidates` in the year TOMLs. Main paper sets are `main_2006`, `main_2018`, `main_2022`.
- Groupings: `demographics` in year TOMLs; paper plot order in `[group_plots.groupings_by_wave]` in `plot_specs.toml`; Lambda groupings in `[lambda_table]` targets in `table_specs.toml`.
- Measures: `[run].measures` and `[measure_sets]` in `orchestration.toml`; plot subsets in `plot_specs.toml`; Lambda table requires `lambda_sep` or can augment it from cached results.
- Imputation backend: `[run].imputer_backends` in `orchestration.toml`; paper-facing collection defaults to `mice` in `paper_artifacts.toml`.
- Linearization policy: `[run].linearizer_policies` in `orchestration.toml`; paper-facing collection defaults to `pattern_conditional` in `paper_artifacts.toml`.
- `B`, `R`, `K`: `[run]` in the selected orchestration config.
- Output locations: `[run].output_root` and `[run].cache_root` in the selected orchestration config; stage-specific overrides exist in `paper_b2.toml`; final collection destination is `[collection].destination_root` in `paper_artifacts.toml` plus overrides under `[paper_artifacts.collection]`.

## 3. Parameters To Change

`B`, `R`, and `K` are TOML-controlled, not environment variables. They are read
in `PrefPol/composable_running/stages/04_measures.jl` by `run_settings`, and
then used by all stage scripts because stages `01` to `03` include
`04_measures.jl`.

Current settings:

- `PrefPol/config/orchestration.toml`: `B = 8`, `R = 2`, `K = 2`; output root `PrefPol/composable_running/output`; cache root `PrefPol/composable_running/output/cache`.
- `PrefPol/config/paper_b2.toml`: `B = 2`, `R = 2`, `K = 2`; isolated output/cache under `PrefPol/composable_running/output/paper_b2`.
- `PrefPol/config/smoke_test.toml`: `B = 2`, `R = 2`, `K = 2`; smoke-sized `m = 2,3`, output root `PrefPol/composable_running/output`, cache root `cache_smoke`.

There are command-line filters for `--year`, `--scenario`, `--m`, `--backend`,
and `--linearizer`, but no CLI flags for `B`, `R`, or `K`.

Safest final-run procedure:

1. Leave `paper_b2.toml` and `smoke_test.toml` as validation configs.
2. Change only `[run].B`, `[run].R`, and `[run].K` in `PrefPol/config/orchestration.toml` for the final paper run.
3. Keep `output_root` and `cache_root` isolated from old valuable outputs if you need to preserve them. A conservative final-run option is to copy `orchestration.toml` to a temporary local config such as `PrefPol/config/orchestration_final_local.toml` and set `output_root`/`cache_root` to a timestamped local output directory. Do not commit that local config if it contains only run settings.
4. Run without `--force` first if reusing an existing cache. Use `--force` only after deciding that existing cached artifacts for the same specs should be overwritten.

## 4. Artifact Checklist

Essential paper-facing artifacts are defined in `PrefPol/config/paper_artifacts.toml` and collected by stage `11`.

| Artifact class | Producer | Required input/cache | Output path | File type | Status |
|---|---|---|---|---|---|
| Global measure plots | `05_plot_global.jl` | `manifests/run_manifest.csv`, `manifests/measure_manifest.csv`, cached `result.jld2` files from `04_measures.jl` | `output/plots/global/<year>/<scenario>/global_measures_*.png`; collected as `output/paper_artifacts/{2006,2018,2022}_global_main.png` | PNG and plot-data CSV | Essential |
| Global summary/panel/decomposition tables | `04_measures.jl`; per-scenario compact tables also from `05_plot_global.jl` | cached measure outputs | `output/measures/{measure_table,summary_table,panel_table,decomposition_table}.csv`; `output/plots/global/<year>/<scenario>/*.csv` | CSV | Essential as checks, not currently collected |
| Group heatmaps | `06_plot_group.jl` | `run_manifest.csv`, `measure_manifest.csv`, cached results from `04_measures.jl` | `output/plots/group/<year>/<scenario>/paper_group_heatmap_panel_C_D_1mO_S_*.png`; collected as `output/paper_artifacts/{2006,2018,2022}_group.png` | PNG and CSV table | Essential |
| `S`, `O`, `lambda_sep` outputs | `04_measures.jl`; `10_lambda_table.jl` for Lambda appendix | measure cache and `run_manifest.csv` | `output/measures/measure_table.csv`; `output/appendices/lambda/*`; collected `appendix_lambda_grouping_tables.tex` | CSV, TeX | Essential for Lambda appendix; `O_smoothed` is optional/diagnostic |
| Variance decomposition figure | `08_extra_plots.jl` | `output/measures/decomposition_table.csv` | `output/extra_plots/variance_decomposition/variance_decomposition_2022.png`; collected `output/paper_artifacts/variance_decomposition_2022.png` | PNG and plot-data CSV | Essential if appendix/robustness section remains in draft |
| Variance decomposition tables | `04_measures.jl`; plot data from `08_extra_plots.jl` | cached measure results | `output/measures/decomposition_table.csv`; `output/extra_plots/variance_decomposition/variance_decomposition_2022_plot_data.csv` | CSV | Essential as checks/supporting data |
| Effective rankings / ENRP outputs | `07_extra_measures.jl`, `09_tables.jl`, `08_extra_plots.jl` | linearized stage artifacts and `run_manifest.csv` | `output/extra_measures/effective_counts/*`; `output/tables/effective_rankings/*`; `output/extra_plots/effective_rankings/effective_rankings_evolution_1x2.png`; collected `effective_rankings.tex` and `effective_rankings_evolution_1x2.png` | CSV, Markdown, TeX, PNG, SVG | Essential |
| Appendix tables | `09_tables.jl`, `10_lambda_table.jl` | effective-count summary and cached measure results | `output/tables/effective_rankings/*`; `output/appendices/lambda/*`; collected TeX files | CSV, Markdown, TeX | Essential if included in manuscript |
| Paper-facing exports | `11_collect_paper_artifacts.jl` | stage manifests from plots/tables/lambda | `output/paper_artifacts/*`; optional `writing/imgs/*` if enabled | PNG, TeX | Essential final handoff |
| O-smoothed group heatmaps | `06_plot_group.jl` if `o_smoothed` results exist | run manifest rows with `analysis_role = o_smoothed_extension` | `output/plots/group/<year>/<scenario>/paper_o_smoothed_heatmap_*` | PNG, CSV | Optional/diagnostic; not collected by `paper_artifacts.toml` |
| Majority-graph support outputs | `majority_graph_support_*.jl` | separate support workflow | `output/majority_graph_support_*` | PNG, PDF, TOML, report files | Not part of paper 1 execution path |
| Single-peakedness outputs | `run_single_peakedness.jl`, `make_single_peakedness_report_artifacts.jl` | separate single-peakedness configs | `output/single_peakedness/*`, `output/paper_b2/single_peakedness_m3_m4.zip` | CSV, ZIP | Not part of paper 1 execution path |

Current manuscript references in `writing/main.tex` are:

- `imgs/2006_global_main.png`
- `imgs/2018_global_main.png`
- `imgs/2022_global_main.png`
- `imgs/effective_rankings_evolution_1x2.png`
- `imgs/2006_group.png`
- `imgs/2018_group.png`
- `imgs/2022_group.png`
- `imgs/variance_decomposition_2022.png`
- `imgs/effective_rankings.tex`
- `imgs/appendix_lambda_grouping_tables.tex`

`writing/imgs/variance_decomposition_boxplot.png` and
`writing/imgs/condorcet_profile.png` exist but are not part of the current
composable artifact collection.

## 5. Safe Rerun Order

Safest order:

1. Snapshot or rename existing important outputs before a final run, especially `PrefPol/composable_running/output/paper_artifacts` and `writing/imgs` if you plan to update manuscript images.
2. Validate configs:
   ```bash
   julia +1.11.9 --project=PrefPol \
     PrefPol/composable_running/stages/00_validate_configs.jl \
     --config PrefPol/config/orchestration.toml
   ```
3. Run a low-replication paper-scope check if needed:
   ```bash
   julia +1.11.9 --project=PrefPol \
     PrefPol/composable_running/run_all_paper.jl \
     --config PrefPol/config/paper_b2.toml
   ```
4. Run the final paper workflow:
   ```bash
   julia +1.11.9 --project=PrefPol \
     PrefPol/composable_running/run_all_paper.jl \
     --config PrefPol/config/orchestration.toml
   ```
5. If only a downstream artifact failed, rerun only the failed stage and its downstream dependents. For example, if only extra plots failed, rerun `08_extra_plots.jl`, `10_lambda_table.jl` if needed, and `11_collect_paper_artifacts.jl`.

Cache/output precautions:

- `--force` forces stages to recreate matching cached outputs. Avoid it on a final run unless you are intentionally replacing existing results.
- `paper_b2.toml` is isolated under `PrefPol/composable_running/output/paper_b2`; use it for operational validation.
- `orchestration.toml` writes to `PrefPol/composable_running/output`. If existing outputs there matter, move or copy them before the final run or use a local final config with a new `output_root` and `cache_root`.
- `paper_artifacts.toml` has `update_writing_imgs = false`; final manuscript images under `writing/imgs` are not overwritten unless this is changed or the files are copied manually after review.

## 6. Cleanup Boundaries

Use these boundaries for cleanup and refactoring:

- `PrefPol/running/`: obsolete old workflow and old paper artifact scripts. Remove or archive `PrefPol/running/*.jl`, `PrefPol/running/*.md`, and `PrefPol/running/output/**` once the staged pipeline is accepted. First move `PrefPol/running/plotting_env` to a non-legacy path and update wrapper/docs references.
- `PrefPol/exploratory/`: exploratory diagnostics and experiments.
- `PrefPol/composable_running/run_single_peakedness.jl`, `make_single_peakedness_report_artifacts.jl`, and configs `single_peakedness*.toml`: paper 2 path.
- `PrefPol/composable_running/run_majority_graph_support_reports.jl`, `majority_graph_support_*.jl`, `majority_graph_report_common.jl`, and `README_majority_graph_support.md`: paper 3 path.
- `majority_graph_musings/` and `single_peakedness_musings/`: non-paper-1 material.
- `PrefPol/src/OLD_PIPELINE.jl`, `PrefPol/src/nested_pipeline.jl`, `PrefPol/src/pipeline.jl`, plotting extension internals, and package exports unless a concrete paper-blocking bug is found.
- Directory layout under `PrefPol/composable_running/`; no new workflow layer is needed for paper 1.

## 7. Verification Steps

After rerunning, check file existence:

```bash
test -s PrefPol/composable_running/output/manifests/run_manifest.csv
test -s PrefPol/composable_running/output/manifests/measure_manifest.csv
test -s PrefPol/composable_running/output/manifests/plot_manifest.csv
test -s PrefPol/composable_running/output/manifests/group_plot_manifest.csv
test -s PrefPol/composable_running/output/manifests/table_manifest.csv
test -s PrefPol/composable_running/output/manifests/lambda_table_manifest.csv
test -s PrefPol/composable_running/output/manifests/paper_artifact_manifest.csv
test -s PrefPol/composable_running/output/paper_artifacts/2006_global_main.png
test -s PrefPol/composable_running/output/paper_artifacts/2018_global_main.png
test -s PrefPol/composable_running/output/paper_artifacts/2022_global_main.png
test -s PrefPol/composable_running/output/paper_artifacts/2006_group.png
test -s PrefPol/composable_running/output/paper_artifacts/2018_group.png
test -s PrefPol/composable_running/output/paper_artifacts/2022_group.png
test -s PrefPol/composable_running/output/paper_artifacts/effective_rankings_evolution_1x2.png
test -s PrefPol/composable_running/output/paper_artifacts/effective_rankings.tex
test -s PrefPol/composable_running/output/paper_artifacts/variance_decomposition_2022.png
test -s PrefPol/composable_running/output/paper_artifacts/appendix_lambda_grouping_tables.tex
```

Check expected manifest scope and `B/R/K`:

```bash
julia +1.11.9 --project=PrefPol -e '
using CSV, DataFrames
run = CSV.read("PrefPol/composable_running/output/manifests/run_manifest.csv", DataFrame)
println(unique(select(run, :wave_id, :scenario_name, :m, :imputer_backend, :linearizer_policy, :B, :R, :K)))
'
```

Expected paper targets are `2006/main_2006`, `2018/main_2018`,
`2022/main_2022`, with `m = 2, 3, 4, 5`. If `diagnostic_targets` are ever wired
into the main stage, `2022/no_forcing` should be treated as diagnostic, not
paper-facing.

Check required table columns:

```bash
julia +1.11.9 --project=PrefPol -e '
using CSV, DataFrames
for path in [
  "PrefPol/composable_running/output/measures/measure_table.csv",
  "PrefPol/composable_running/output/measures/summary_table.csv",
  "PrefPol/composable_running/output/measures/panel_table.csv",
  "PrefPol/composable_running/output/measures/decomposition_table.csv",
  "PrefPol/composable_running/output/extra_measures/effective_counts/effective_counts_summary.csv",
  "PrefPol/composable_running/output/appendices/lambda/appendix_lambda_audit.csv",
]
  df = CSV.read(path, DataFrame)
  println(path, " rows=", nrow(df), " cols=", join(String.(names(df)), ","))
end
'
```

Check plots are nonempty and valid images:

```bash
file PrefPol/composable_running/output/paper_artifacts/*.png
find PrefPol/composable_running/output/paper_artifacts -name '*.png' -size +10k -print
```

Check paper artifact collection matched the intended backend/linearizer and
did not collect stale files:

```bash
julia +1.11.9 --project=PrefPol -e '
using CSV, DataFrames
m = CSV.read("PrefPol/composable_running/output/manifests/paper_artifact_manifest.csv", DataFrame)
println(m[:, [:artifact_id, :source_stage, :source_path, :destination_path, :status]])
'
```

Check candidate lists against the draft contract:

- 2006 `main_2006`: `Lula`, `Geraldo_Alckmin`, `Heloísa_Helena`, `José_Serra`, `Cristóvam_Buarque`.
- 2018 `main_2018`: `Fernando_Haddad`, `Jair_Bolsonaro`, `Ciro_Gomes`, `Geraldo_Alckmin`, `Marina_Silva`.
- 2022 `main_2022`: `LULA`, `BOLSONARO`, `CIRO_GOMES`, `SIMONE_TEBET`, `MARINA_SILVA`.

The run manifest `active_candidates` column should reflect the prefix of these
scenario candidate lists for each `m`.

## 8. Immediate Next Commands

From the repository root:

```bash
julia +1.11.9 --project=Preferences -e 'using Pkg; Pkg.instantiate(); Pkg.test()'
julia +1.11.9 --project=PrefPol -e 'using Pkg; Pkg.instantiate(); Pkg.test()'
```

Validate the paper configs:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/stages/00_validate_configs.jl \
  --config PrefPol/config/orchestration.toml
```

Run the low-replication paper-scope path:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/run_all_paper.jl \
  --config PrefPol/config/paper_b2.toml
```

Run the final paper path after setting final `B`, `R`, and `K` in
`PrefPol/config/orchestration.toml`:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/run_all_paper.jl \
  --config PrefPol/config/orchestration.toml
```

Uncertain final step: updating `writing/imgs`. The current artifact config has
`update_writing_imgs = false`, so the final run updates
`PrefPol/composable_running/output/paper_artifacts` only. Before touching
`writing/imgs`, compare the collected artifacts, then either temporarily set
`update_writing_imgs = true` for the collection stage or copy the reviewed
files into `writing/imgs`.

Collection-only command if the upstream outputs are already correct:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/stages/11_collect_paper_artifacts.jl \
  --config PrefPol/config/orchestration.toml \
  --artifact-config PrefPol/config/paper_artifacts.toml
```

## 9. Post-Paper Refactor Note

The broader three-paper refactor should wait until after the polarization paper
is finished. For now, this document is the artifact contract for paper 1:
preserve these inputs, outputs, filenames, configs, and commands. Any later
reorganization for the profile-space polarization, single-peakedness /
dimensionality, and majority-architecture papers should start from this
execution-path contract rather than changing it before the paper is submitted.
