# PrefPol Replication

This repository provides Julia code to replicate the preference polarization analysis from the PrefPol project. The entry point for reproducing the figures and tables of the paper is [`running/running.jl`](running/running.jl), which orchestrates the entire data pipeline and visualization workflow.

## Prerequisites

- **Julia 1.12.2** – download from [julialang.org](https://julialang.org/install/).
- **R 4.3.3** – download from [CRAN](https://cran.r-project.org/).
- Required R packages: `PerMallows`, `haven`, and `MICE`.
  ```r
  install.packages(c("PerMallows", "haven", "MICE"))
  ```

## Data

Each election year is configured by a TOML file.  Within each TOML file the `data_file` field points to the raw dataset on disk.  Obtain the datasets from:

- 2006: <https://www.cesop.unicamp.br/por/banco_de_dados/v/1583>
- 2018: <https://www.cesop.unicamp.br/por/banco_de_dados/v/4538>
- 2022: <https://www.cesop.unicamp.br/por/banco_de_dados/v/4680>

Copy the downloaded files to a convenient location and update the `data_file` path in the corresponding TOML configuration.

> **Note**: The example configuration sets the number of pseudo‑profiles to **3** for quick runs.  The paper uses **1000** pseudo‑profiles.

Any extra dataset must add a preprocessing_specific function to the preprocessing_specific.jl file and a TOML configuration file at config.

## Running the pipeline

1. Activate and instantiate the project

2. Execute the main script running/running.jl
 
I run it interactively in a text editor (emacs, vscode). 


## What `running.jl` does

The script performs the full replication workflow:

1. **Resampling** – `PrefPol.save_all_bootstraps` generates and stores pseudoprofiles (resamples) of each year.
2. **Imputation** – `PrefPol.impute_from_f3` fills missing rankings using multiple imputation.
3. **Profile generation** – `PrefPol.generate_profiles_for_year_streamed_from_index` creates preference profiles for every year/scenario combination.
4. **Global measures** – `PrefPol.save_or_load_measures_for_year` computes global polarization metrics for each election year (Ψ, RHHI)
5. **Group metrics** – `PrefPol.save_or_load_group_metrics_for_year` calculates group metrics (C,D,G)
6. **Plots** – `PrefPol.plot_scenario_year` and `PrefPol.plot_group_demographics_lines` produce scenario‑specific and group figures which are saved via `PrefPol.save_plot`.

## R integration

Certain preprocessing steps rely on R.  Ensure R can find the required packages and that `R` is available on your system `PATH` before running the Julia script.

## Testing

Run the unit tests with ] test in the repl.

The tests exercise the preprocessing and polarization measures modules.

## Developer note: Julia 1.12 `Pointf` warning

With Julia 1.12, precompiling `GraphMakie` 0.5.14 can emit a deprecation warning because `GraphMakie/src/utils.jl` extends `Pointf(...)` without explicit qualification/import (Julia assumes `GeometryBasics.Pointf`, which is now deprecated behavior).

`PrefPol` does not call `GraphMakie` APIs in `src/`, `test/`, or `exploratory/`, so the direct `GraphMakie` dependency was removed from `Project.toml`. This is safe because it only drops an unused dependency and avoids triggering the upstream deprecation during `Pkg.precompile()`.
