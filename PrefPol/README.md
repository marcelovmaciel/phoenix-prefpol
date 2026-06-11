# PrefPol Brazil/ESEB Replication Package

## Purpose

`PrefPol` is the applied replication package for the article on the structure of
political dissent in Brazilian multiparty candidate preferences. It reproduces
the article's ESEB-based empirical workflow: construction of ranked preference
profiles, stochastic treatment of missingness and ties, computation of profile
measures, and collection of manuscript-facing figures and tables.

The empirical object is not a one-dimensional ideology distribution. The package
studies ranked-preference profiles constructed from respondents' feeling
thermometer evaluations of candidates and political leaders. The formal
preference, profile, ranking, aggregation, and measure primitives live in the
sibling `PreferenceProfiles/` package; `PrefPol` applies those primitives to the
Brazil/ESEB replication design.

## Empirical Object

The units are ESEB survey respondents. The replicated waves are 2006, 2018, and
2022. The raw inputs are candidate or leader thermometer scores in the year
survey files.

For each configured year and candidate set, the workflow first constructs weak
preference orders over the selected candidates. Equal thermometer scores produce
ties. The stochastic linearization stages then convert complete weak profiles
into strict rankings so that measures defined on rankings can be computed.

The construction uses within-person ordinal information: who a respondent ranks
above whom, including ties before linearization. It does not treat thermometer
distances as interpersonally comparable quantities, and it does not interpret
absolute score gaps as cardinal utilities shared across respondents.

## Data Sources

Each election year is configured by a TOML file in `PrefPol/config/`. Within
each year TOML, the `data_file` field points to the raw dataset on disk. Obtain
the datasets from CESOP:

- 2006: <https://www.cesop.unicamp.br/por/banco_de_dados/v/1583>
- 2018: <https://www.cesop.unicamp.br/por/banco_de_dados/v/4538>
- 2022: <https://www.cesop.unicamp.br/por/banco_de_dados/v/4680>

Copy the downloaded `.sav` files to a convenient local location and update the
`data_file` paths used by the year configs. Do not commit raw survey files or
machine-specific data paths. Local data provenance is therefore supplied through
the year TOML configs, not through checked-in raw data.

## Candidate Sets and Preference Construction

Candidate sets are year-specific and substantively selected. They are not
mechanically chosen only by lowest missingness. The publication configuration
uses forced candidate-set scenarios for the ESEB waves and evaluates the main
replication across `m = 2, 3, 4, 5`.

Changing `m` changes the ranking space. Larger candidate sets include more
preference relations, but they also become increasingly sparse because more
respondents have missing scores or tied evaluations among at least one selected
candidate. For that reason, results for larger `m` depend more visibly on the
configured imputation and linearization design.

## Measurement Strategy

The main global measures describe the structure of opposition in the profile:

- `Psi`: pairwise antagonism across rankings.
- `R`: exact reversal mass, or the share of ranking mass opposed by exact
  reversals.
- `HHI` and kappa: concentration of reversal opposition.
- `RHHI`: a conjunctive summary that combines opposition mass and
  concentration.

The group measures describe how configured partitions relate to ranking
structure:

- `C`: within-group coherence around group consensus rankings.
- `D`: distance from each group to other groups' consensus rankings.

Groupings are diagnostic partitions of the profile. They are not causal
treatments, and differences across groups should not be read as estimates of
causal effects.

## Stochastic Replication Design

`PrefPol/config/publication.toml` defines the nested stochastic replication
design used for the article. The active publication settings are `B = 30`,
`R = 10`, and `K = 10`.

- `B`: weighted bootstrap pseudo-profiles drawn from each configured survey
  target.
- `R`: imputation replicates inside each bootstrap branch.
- `K`: stochastic strict linearizations inside each imputed weak profile.

For the current publication config, the active imputation backends are `mice`
and `zero`. The active linearizers are `pattern_conditional` and `random_ties`.
Primary manuscript artifacts default to the `mice` imputer plus the
`pattern_conditional` linearizer.

The nested design is part of the replication provenance. It records how much of
the reported descriptive variation is induced by bootstrap resampling,
imputation, and tie linearization within the configured workflow.

## What the Main Run Reproduces

The publication run reproduces the article-facing empirical artifacts:

- Figures and tables for global profile measures.
- Group heatmaps for `C` and `D`.
- Effective-ranking and effective-reversal summaries.
- Variance-decomposition outputs for the nested stochastic pipeline.
- Collected manuscript-facing outputs under `paper_artifacts/`.

These outputs are generated from the configured survey files and the
publication TOML settings. They should be interpreted as a reproducible
measurement exercise for the article, not as a stand-alone causal model of
Brazilian electoral behavior.

## How to Reproduce the Article

Run commands from the repository root. First instantiate the `PrefPol`
environment:

```bash
julia +1.11.9 --project=PrefPol -e 'using Pkg; Pkg.instantiate()'
```

To validate the publication configuration before a full run:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/stages/00_validate_configs.jl \
  --config PrefPol/config/publication.toml
```

To reproduce the Brazil polarization article:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/run_all_paper.jl \
  --config PrefPol/config/publication.toml
```

`PrefPol/composable_running/run_all_paper.jl` is the preferred public entry
point. It runs validation, bootstrap, imputation, linearization, measure,
plotting, table, extra-measure, extra-plot, and artifact-collection stages in
the intended order. Plotting uses `PrefPol/running/plotting_env`, which should
provide `CairoMakie` and related plotting dependencies.

Use individual stage scripts only when diagnosing a failed or partial run. For
normal reproduction, use the wrapper so plotting and artifact collection happen
with the intended environments and arguments.

## Expected Outputs

The publication config writes generated files under
`PrefPol/composable_running/output/publication/`.

| Path under output root | Scientific role |
| --- | --- |
| `cache/` | Cached bootstrap, imputation, and linearization products for the nested profile pipeline. |
| `measures/` | Global and group measure outputs used to make tables, figures, and diagnostics. |
| `plots/global/` | Global profile-measure figures for manuscript and review. |
| `plots/group/` | Group heatmaps and related visual summaries for `C` and `D`. |
| `extra_measures/` | Effective-ranking, effective-reversal, and auxiliary summary measures. |
| `extra_plots/` | Additional figures derived from the extra-measure outputs. |
| `tables/` | Generated tables for the publication workflow. |
| `paper_artifacts/` | Reviewer-facing collected figures and tables for the article. |
| `manifests/` | Stage manifests recording products, config provenance, and paths for diagnosis. |

When looking for final article outputs, start with `paper_artifacts/`. When
diagnosing a failed or partial run, start with `manifests/` and then follow the
recorded paths into `cache/`, `measures/`, `plots/`, `tables/`, or
`extra_measures/`.

## Interpreting the Outputs

The bootstrap, imputation, and linearization summaries are descriptive
replication diagnostics. They do not define a causal model and should not be
read as estimates of treatment effects or structural parameters.

The variance-decomposition outputs partition simulation variability induced by
the configured pipeline. They help show whether variation in an artifact is
mainly associated with resampling, imputation, or tie linearization under the
specified design.

Comparisons across `m` require care because changing `m` changes the ranking
space. A result for `m = 5` is not a simple refinement of a result for `m = 2`;
it is computed over a different candidate set, a different set of possible
rankings, and usually a different pattern of missingness and ties.

## Repository Structure

- `PrefPol/src/preprocessing_general.jl` contains SPSS/R integration, R-backed
  MICE imputation helpers, generic score preprocessing, and linearization
  adapters.
- `PrefPol/src/preprocessing_specific.jl` contains year-specific ESEB recodes,
  loaders, and survey-wave transformations.
- `PrefPol/src/nested_pipeline.jl` contains the nested
  bootstrap/imputation/linearization pipeline and cache layout.
- `PrefPol/src/survey_config.jl` parses year configs, builds survey-wave
  configs, resolves candidate sets, loads raw survey data, and keeps the raw
  profile helper entry points used by tests and the pipeline.
- `PreferenceProfiles/` owns the formal preference, profile, ranking,
  aggregation, and measure primitives used by `PrefPol`.

Root `intermediate_data/`, package-local output folders, and
`PrefPol/composable_running/output/` should be treated as generated artifacts
unless a change is intentionally updating checked-in results.

## Requirements

- **Julia 1.11.9**. `PrefPol/Manifest.toml` records this version for the
  publication environment. Use `julia +1.11.9` for validation and reproduction.
- **R on `PATH`**. `PrefPol` uses `RCall` for SPSS loading and, when configured,
  MICE imputation. Run `R --version` from the same shell that starts Julia.
- **R packages used by the active workflow**:
  ```r
  install.packages(c("haven", "mice"))
  ```
  `haven` reads the raw SPSS `.sav` survey files. `mice` powers the `:mice`
  imputation backend used in the publication configuration. `PerMallows` is not
  used by the active publication workflow in this repository; install it only
  for legacy or experimental scripts that explicitly require it.
- **Plotting environment**. Plotting stages run through
  `PrefPol/running/plotting_env`, which should provide `CairoMakie` and related
  plotting dependencies.

## Troubleshooting

- If `.sav` loading fails, check the year config's `data_file`, confirm `R` is
  on `PATH`, and verify that `haven` is installed in the R library visible to
  `RCall`.
- If `:mice` imputation fails, verify the R `mice` package installation and
  confirm that `RCall` is bound to the intended R installation.
- If plotting fails, use `run_all_paper.jl` rather than invoking plotting stages
  directly, or run the plotting stages with the `PrefPol/running/plotting_env`
  project.
- If downstream manifests are missing, rerun from the earliest missing stage or
  rerun the wrapper with the same `--config`. Add `--force` only when cached
  successful outputs should be regenerated.
