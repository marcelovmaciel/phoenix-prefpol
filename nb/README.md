# PrefPol Notebook Layer

The `PrefPol/composable_running/` directory is the production CLI replication
workflow for the paper. It runs numbered stages in dependency order, writes
manifests and artifacts, and is the right entry point for reproducible
publication-scale execution.

The `nb/` directory is for interactive inspection, explanation, debugging, and
teaching the same workflow. Notebooks should reproduce the logic step by step in
a humanly inspectable form. They should expose intermediate objects directly in
Julia cells, tables, and small summaries instead of hiding work behind shell
commands.

Notebook runs are pedagogical. They are not the official replication path for
the manuscript; use `PrefPol/composable_running/run_all_paper.jl` with
`PrefPol/config/publication.toml` for publication-scale reproduction.

Notebooks must not be thin wrappers around
`PrefPol/composable_running/run_all_paper.jl`, and they must not run the full
publication sweep from `PrefPol/config/publication.toml`.

## Setup

Start Pluto from the repository root with the notebook environment:

```bash
julia +1.11.9 --project=nb -e 'using Pkg; Pkg.instantiate(); using Pluto; Pluto.run()'
```

Open notebooks from `nb/` in the order listed below. Each notebook should begin
with:

```julia
include("notebook_common.jl")
cfg = load_notebook_config()
```

The default config is `nb/notebook_config.toml`. It keeps execution small
(`B=2`, `R=2`, `K=2`), uses the zero imputer by default, and selects only
`2018 / main_2018`.

## Output Isolation

Notebook artifacts are written under:

```text
nb/output/notebook_smoke
nb/output/notebook_smoke/cache
```

`nb/notebook_common.jl` resolves notebook paths relative to this repository and
guards against writes into:

```text
PrefPol/composable_running/output/publication
```

Do not point notebooks at `PrefPol/config/publication.toml` or the publication
output tree. Widen notebook runs only by editing `nb/notebook_config.toml`.

## Proposed Notebook Sequence

The notebook layer should be organized as a short ordered sequence:

1. `00_setup_and_config.jl`
2. `01_targets_and_specs.jl`
3. `02_resampling_and_imputation.jl`
4. `03_linearization.jl`
5. `04_measure_cubes.jl`
6. `05_global_and_group_summaries.jl`
7. `06_extra_diagnostics.jl`
8. `07_plots_and_tables.jl`
9. Optional: `08_artifact_map.jl`

The sequence should follow the same conceptual order as the CLI workflow:

```text
00_validate_configs
01_bootstrap
02_impute
03_linearize
04_measures
05_plot_global
06_plot_group
07_extra_measures
08_tables
09_extra_plots
optional 10_lambda_table
11_collect_paper_artifacts
```

The notebook sequence may combine CLI stages when that improves exposition, but
it should always state which CLI stage or stages are being mirrored.

## Notebook Responsibilities

### `00_setup_and_config.jl`

Purpose: establish the notebook environment, load `PrefPol`, read a
notebook-scale config, resolve paths, and show the selected execution scale.

Corresponds to CLI stage: `00_validate_configs`, plus the setup work normally
handled by the wrapper and `stage_common.jl`.

Expected objects to expose:

- Repository, package, config, and output paths.
- Parsed notebook config.
- Selected years, scenarios, `m` values, imputation backends, and linearizers.
- Effective resampling parameters, especially `B`, `R`, and `K`.

### `01_targets_and_specs.jl`

Purpose: explain the paper targets and candidate/profile specifications before
any expensive computation starts.

Corresponds to CLI stage: target/spec portions of `00_validate_configs`,
`01_bootstrap`, and downstream stage planning.

Expected objects to expose:

- Survey-wave configs.
- Target/scenario definitions.
- Candidate sets and ranking/profile specifications.
- The small target grid selected for notebook execution.

### `02_resampling_and_imputation.jl`

Purpose: demonstrate how raw survey inputs become bootstrap samples and imputed
profile data.

Corresponds to CLI stages: `01_bootstrap` and `02_impute`.

Expected objects to expose:

- Raw or preprocessed survey records for the selected year.
- Resampling plan and bootstrap sample identifiers.
- Imputation input tables.
- Imputed profile outputs for each selected backend.
- Any imputation diagnostics available at notebook scale.

### `03_linearization.jl`

Purpose: show how imputed preference data are converted into linearized profile
objects used by the measure pipeline.

Corresponds to CLI stage: `03_linearize`.

Expected objects to expose:

- Imputed profile tables.
- Linearizer configuration.
- Linearized profiles.
- Profile-level sanity checks and small tabular previews.

### `04_measure_cubes.jl`

Purpose: compute and inspect the main polarization and preference-summary
measure cubes before plotting or table generation.

Corresponds to CLI stage: `04_measures`.

Expected objects to expose:

- Measure specifications.
- Per-bootstrap and per-imputation measure tables.
- Aggregated measure cubes.
- Missingness, dimensions, and grouping checks.

### `05_global_and_group_summaries.jl`

Purpose: turn measure cubes into the global and group summaries used by the
paper's main figure layer.

Corresponds to CLI stages: `05_plot_global` and `06_plot_group`, but should
show the summary tables before rendering plots.

Expected objects to expose:

- Global summary tables.
- Group summary tables.
- Plot-ready data frames.
- Checks for expected years, scenarios, groups, and measures.

### `06_extra_diagnostics.jl`

Purpose: compute and explain additional diagnostics and robustness summaries
without mixing them into the main measure-cube notebook.

Corresponds to CLI stages: `07_extra_measures` and optional
`10_lambda_table`.

Expected objects to expose:

- Extra-measure specifications.
- Diagnostic tables.
- Any lambda-table inputs and outputs when explicitly enabled.
- Notes on which diagnostics are omitted at notebook scale.

### `07_plots_and_tables.jl`

Purpose: render notebook-scale versions of publication plots and tables from
already inspected summary objects.

Corresponds to CLI stages: `05_plot_global`, `06_plot_group`, `08_tables`, and
`09_extra_plots`.

Expected objects to expose:

- Plot-ready data.
- Table-ready data.
- Rendered figures.
- Rendered tables.
- Output paths for notebook-generated artifacts.

### `08_artifact_map.jl` Optional

Purpose: map notebook-generated artifacts back to the paper artifact collection
without performing a publication artifact collection.

Corresponds to CLI stage: `11_collect_paper_artifacts`.

Expected objects to expose:

- Notebook artifact inventory.
- Corresponding publication artifact names, when applicable.
- Missing artifacts that are intentionally excluded from the notebook run.

## Pedagogical Contract

Each notebook must answer these questions near the top, before computation:

- What stage of the paper workflow is being reproduced?
- What are the inputs?
- What are the main objects?
- What is computed?
- What artifacts are written?
- How does this correspond to the CLI stage?
- What can the user inspect before continuing?

Each notebook should also include short "inspect before continuing" checkpoints
after major objects are created. These checkpoints should prefer displayed
tables, dimensions, object names, and compact summaries over prose-only
explanations.

## Execution Scale

Notebook defaults must use a notebook-specific config, not
`PrefPol/config/publication.toml`.

The default notebook run should be intentionally small:

- Prefer `B=2`, `R=2`, `K=2` when the added cost is still interactive.
- Use `B=1`, `R=1`, `K=2` when the notebook is meant to be a smoke path or when
  R-backed imputation is expensive.
- Restrict defaults to one year, one scenario, one `m`, one imputer backend, and
  one linearizer.

The notebook layer may permit opt-in widening, but widening must be explicit in
the config or in clearly labeled cells. Examples of acceptable opt-ins include:

- All three paper years: `2006`, `2018`, and `2022`.
- Multiple `m` values, such as `m=2:5`.
- Multiple scenarios.
- Multiple imputer backends.
- Multiple linearizers.
- Larger `B`, `R`, or `K`.

No notebook should default to the publication-scale `B=30`, `R=10`, `K=10`
sweep.

## Output Isolation

Notebook-generated outputs must be isolated from production publication outputs.
Acceptable output roots include:

```text
nb/output/
PrefPol/composable_running/output/notebook_smoke/
```

The selected root should be defined in the notebook-specific config and printed
in `00_setup_and_config.jl`.

Notebooks must not write into:

```text
PrefPol/composable_running/output/publication/
```

If a notebook needs to compare against existing publication artifacts, it should
read them explicitly and label them as external comparison inputs. It should not
overwrite, regenerate, or partially update them.

## Dependency Policy

Pluto notebooks should use the same `PrefPol` package APIs as the CLI workflow.
The notebook layer may include a small helper file, for example:

```text
nb/notebook_common.jl
```

That helper should be limited to notebook ergonomics: path resolution, compact
display helpers, notebook config loading, small validation wrappers, and shared
table-preview utilities.

Notebook code should avoid directly including numbered stage files from
`PrefPol/composable_running/stages/`. Numbered stage files are CLI entry points,
not notebook APIs.

Notebook code should not call:

```text
PrefPol/composable_running/run_all_paper.jl
```

Calling small functions from `PrefPol/composable_running/stage_common.jl` is
acceptable only when it improves consistency without hiding the computation. For
example, shared path or manifest helpers may be reasonable; a helper that
performs an entire stage should be avoided in notebooks.

## Review Criteria

Notebook reviews should prioritize human inspectability over maximal automation.
Use the following criteria:

- Short cells.
- Tables before plots.
- Explicit object names.
- Minimal global mutation.
- No duplicate large logic across notebooks.
- Clear comments such as `# Corresponds to CLI stage 03_linearize`.
- Intermediate objects are visible and inspectable.
- Outputs are isolated under the notebook output root.
- Defaults remain small and do not use publication-scale parameters.
- The notebook sequence teaches the workflow rather than merely launching it.

