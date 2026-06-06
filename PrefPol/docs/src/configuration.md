# Configuration

```@meta
CurrentModule = PrefPol
```

PrefPol separates year-level survey configuration from the publication
orchestration file.

## Year TOMLs

The numeric files in `PrefPol/config/`, such as `2006.toml`, `2018.toml`, and
`2022.toml`, define one ESEB survey wave. They record the raw data loader,
`data_file` path, candidate universe, demographic grouping columns, default
candidate counts, random seeds, and named forced-candidate scenarios.

`load_survey_wave_config(path)` parses a year TOML into `SurveyWaveConfig`.
This is a configuration object only; it does not read survey data or write
cache artifacts. `build_source_registry(waves)` then indexes one or more waves
by `wave_id` for `NestedStochasticPipeline`.

Raw survey files and machine-specific `data_file` paths should not be committed.
Keep local path changes out of tracked configs unless the change is meant to be
shared.

## Candidate Sets

`resolve_active_candidate_set` applies PrefPol's candidate-set rule for a
survey wave. If an exact `active_candidates` list is supplied, that set is used.
Otherwise PrefPol computes a weighted missingness ordering from the raw survey
table, force-includes the candidates named by `scenario_name`, and trims the
ordered list to `m` alternatives.

`build_pipeline_spec` uses the same resolver when constructing `PipelineSpec`,
so raw-profile helpers and the staged pipeline use the same candidate-set
contract.

## Publication TOML

`PrefPol/config/publication.toml` selects the manuscript-facing run:

- targets: `2006/main_2006`, `2018/main_2018`, and `2022/main_2022`,
- candidate-set sizes: `m_values = [2, 3, 4, 5]`,
- nested branch counts: `B = 30`, `R = 10`, `K = 10`,
- imputation backends: `mice` and `zero`,
- linearizers: `pattern_conditional` and `random_ties`,
- measures: `Psi`, `R`, `HHI`, `RHHI`, `C`, and `D`,
- output root: `PrefPol/composable_running/output/publication`, and
- cache root: `PrefPol/composable_running/output/publication/cache`.

Diagnostic measures such as `O`, `S`, `E`, `S_old`, and `lambda_sep` remain
supported by the pipeline registry for explicit extended runs. They are not
part of the core manuscript-facing measure set in `publication.toml`.

## Supporting Configs

`plot_specs.toml`, `table_specs.toml`, and `paper_artifacts.toml` define plot
selection, table outputs, and artifact collection. The validation stage checks
that these support files exist and that configured targets, groupings,
measures, and output formats are consistent with the survey-wave configs.

## API

```@docs; canonical=false
SurveyWaveConfig
load_survey_wave_config
build_source_registry
resolve_active_candidate_set
available_election_years
default_config_path
```
