# Plotting

```@meta
CurrentModule = PrefPol
```

PrefPol plotting helpers are extension APIs. They are available only after
`CairoMakie` is present in the active environment and loaded:

```julia
using PrefPol
using CairoMakie
```

If `CairoMakie` is not loaded, calls such as `plot_pipeline_scenario` throw an
argument error explaining that the CairoMakie extension is required.

## Publication Plots

`run_all_paper.jl` runs plot stages through
`PrefPol/running/plotting_env`, which is the preferred route for publication
figures. Direct calls are useful for interactive inspection of already computed
`PipelineResult` objects or tables.

Global plot helpers consume one or more pipeline results and use
`pipeline_scenario_plot_data` internally. Group plot helpers consume grouped
measure rows and use `pipeline_group_plot_data` or
`pipeline_group_heatmap_values`.

Variance-decomposition plotting helpers consume report tables from
`variance_decomposition_by_m_plot_table`,
`variance_decomposition_year_scenario_boxplot_table`, or related report
functions. They do not recompute the nested pipeline.

## Troubleshooting

If a direct plotting call fails, first check that `CairoMakie` is installed in
the active project and loaded in the session. If a stage script fails when run
directly, run the full wrapper or start Julia with
`--project=PrefPol/running/plotting_env`.

## API

```@docs; canonical=false
plot_pipeline_scenario
plot_pipeline_group_lines
plot_pipeline_group_heatmap
plot_variance_decomposition_by_m
plot_variance_decomposition_year_scenario_boxplots
plot_variance_decomposition_dotwhisker
plot_variance_decomposition_boxplot
save_pipeline_plot
```
