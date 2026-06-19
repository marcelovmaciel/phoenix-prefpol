# Single-pass survey-weighted m = 4 tetrahedra implementation status

## Current branch

`exploratory/single-pass-survey-weighted-m4-tetrahedra`

## Initial safety state

- Required `git status --short` was run on `main` before branching.
- The worktree contained pre-existing untracked local files and generated artifacts.
- The user clarified that those untracked files should remain untracked.
- No pre-existing untracked files have been overwritten or discarded.
- Branch was created with `git switch -c exploratory/single-pass-survey-weighted-m4-tetrahedra`.

## Relevant files inspected

- `PrefPol/config/2006.toml`
- `PrefPol/config/2018.toml`
- `PrefPol/config/2022.toml`
- `PrefPol/src/survey_config.jl`
- `PrefPol/src/preprocessing_general.jl`
- `PrefPol/src/preprocessing_specific.jl`
- `PreferenceProfiles/src/PreferenceTabularProfiles.jl`
- `PreferenceProfiles/src/PreferenceProfile.jl`
- `PreferenceProfiles/src/PreferenceLinearization.jl`
- `PreferenceProfiles/test/pattern_conditional_linearizer_tests.jl`
- `VotingGeometry/src/profile_vectors.jl`
- `VotingGeometry/src/plots.jl`
- `VotingGeometry/src/VotingGeometry.jl`
- `VotingGeometry/test/runtests.jl`
- `.gitignore`

## Discovered standard source paths

The configured respondent-level sources are SPSS `.sav` files, not stable repository-local CSVs. The user confirmed this during inspection.

- 2006: `PrefPol/data/02489/1_02489.sav` from `PrefPol/config/2006.toml` (`data/02489/1_02489.sav`).
- 2018: `PrefPol/data/eseb_2018/04622/04622.sav` from `PrefPol/config/2018.toml` (`data/eseb_2018/04622/04622.sav`).
- 2022: `PrefPol/data/04810/04810.sav` from `PrefPol/config/2022.toml` (`data/04810/04810.sav`).

No stable repository-local respondent-level CSV was found. Existing CSVs inspected by filename/path are generated manifests, summaries, plot data, or linearized/imputed/profile outputs.

## Candidate columns and weight columns

Configured main scenarios, truncated to m = 4:

- 2006 `main_2006`: `Lula`, `Geraldo_Alckmin`, `Heloísa_Helena`, `José_Serra`.
- 2018 `main_2018`: `Fernando_Haddad`, `Jair_Bolsonaro`, `Ciro_Gomes`, `Geraldo_Alckmin`.
- 2022 `main_2022`: `LULA`, `BOLSONARO`, `CIRO_GOMES`, `SIMONE_TEBET`.

Weight resolver convention already exists in PrefPol/PreferenceProfiles and checks `:peso`, `:weight`, `:weights`. Runtime source loading still needs to confirm the concrete column and row counts for each year.

## Proposed implementation stages

1. Inspect PrefPol configs, data path conventions, MICE helpers, score preparation, and profile construction APIs.
2. Inspect VotingGeometry opened tetrahedron plotting and profile-vector APIs.
3. Add a proportion-oriented opened tetrahedron plotting method without changing existing public methods.
4. Add focused VotingGeometry tests for plotting validation and weighted profile-vector behavior.
5. Add an isolated exploratory Julia environment if the existing PrefPol environment cannot load all needed packages cleanly.
6. Add `PrefPol/exploratory/plot_single_pass_survey_weighted_m4_tetrahedra.jl`.
7. Run VotingGeometry tests and, if standard CSVs are resolvable locally, run the exploratory script.
8. Keep generated outputs under `PrefPol/exploratory/output/` and ensure that path is ignored.

## Current phase

Phase 5: validation complete; ready for scoped commits.

## Last command run

`julia +1.11.9 --project=VotingGeometry -e 'using Pkg; Pkg.test()'`

## Test status

- Exploratory environment smoke load: passed after one transient PythonPlot/VotingGeometry precompile retry.
- `julia +1.11.9 --project=VotingGeometry -e 'using Pkg; Pkg.test()'`: passed.
- `julia +1.11.9 --project=PrefPol/exploratory PrefPol/exploratory/plot_single_pass_survey_weighted_m4_tetrahedra.jl`: passed against the configured `.sav` sources.
- `git diff --check`: passed before the final status update; needs one final rerun after this note update.

Generated outputs under ignored `PrefPol/exploratory/output/single_pass_survey_weighted_m4_tetrahedra/`:

- `opened_tetrahedron_single_pass_survey_weighted_m4_2006.png`
- `opened_tetrahedron_single_pass_survey_weighted_m4_2018.png`
- `opened_tetrahedron_single_pass_survey_weighted_m4_2022.png`
- `survey_weighted_m4_ranking_proportions.csv`
- `single_pass_survey_weighted_m4_metadata.csv`

Run metadata from the completed script:

- 2006: 1000 input rows, 1000 retained rows, weight column `peso`, total retained weight 1000.0000000000003, MICE seed 200601, linearization seed 200641.
- 2018: 2506 input rows, 2506 retained rows, weight column `peso`, total retained weight 2506.0, MICE seed 201801, linearization seed 201841.
- 2022: 2001 input rows, 2001 retained rows, weight column `peso`, total retained weight 2001.0000000000011, MICE seed 202201, linearization seed 202241.

All three MICE runs completed the four active score columns with zero logged events and no dropped predictors. Manual weighted aggregation matched `VotingGeometry.profile_vector` for all years.

## Exact next action

Run final `git diff --check` and `git status --short`, then create scoped commits for the status note, VotingGeometry plotting/tests, and exploratory pipeline files. Do not stage generated output files.
