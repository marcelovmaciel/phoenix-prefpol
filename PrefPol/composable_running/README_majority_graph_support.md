# Majority-Graph Support 2022 Workflow

This workflow replaces the prototype Python report generator with composable Julia code.

- `Preferences` contains the general social-choice objects and tables: voter-type bases, majority-graph support, edge/core/overlap/breaker tables, group majority-graph tables, and generic plurality/switch-amenability tables.
- `PreferenceGraphPlots` contains generic PythonPlot plotting functions for those result objects and tables. It has no PrefPol-specific candidate names.
- `PrefPol/composable_running/majority_graph_support_2022.jl` is the project-specific script. It wires the 2022 CSV rank columns, labels `Lu/Bo/Ci/Te`, reference order `Lu > Te > Ci > Bo`, target Lula, opponent Bolsonaro, partitions, output paths, and report text.

Run from the repository root:

```bash
julia --project=PrefPol PrefPol/composable_running/majority_graph_support_2022.jl
```

Or pass explicit paths:

```bash
julia --project=PrefPol PrefPol/composable_running/majority_graph_support_2022.jl \
  --input path/to/m04_mice_pattern_conditional_2022_augmented_linearized_profile.csv \
  --output PrefPol/composable_running/output/majority_graph_support_2022
```

Outputs are written under the output directory:

- `tables/`: CSV tables produced by `Preferences`.
- `figures/`: PNG and PDF figures produced by `PreferenceGraphPlots`.
- `report/majority_graph_support_2022.tex`: lightweight LaTeX report, compiled to PDF when `latexmk` or `tectonic` is available.
- `manifest.toml`: input path, candidate mapping, partitions, and generation metadata.

The old `make_report.py` is treated as a prototype/reference only. The Julia workflow does not call it, does not recompute analysis in Python, and keeps generic preference logic, plotting logic, and project-specific 2022 wiring in separate packages/scripts.
