# Raw Profiles Refactor Summary

## What moved to `Preferences`

Generic tabular profile-construction and summary logic moved from
`PrefPol/src/raw_profiles.jl` into `Preferences/src/PreferenceTabularProfiles.jl`:

- Candidate-name helpers:
  - `humanize_candidate_name`
  - `canonical_candidate_key`
  - `candidate_display_symbols`
  - `resolve_candidate_cols_from_set`
  - `guess_weight_col`
- Generic score/weight row handling:
  - `normalize_numeric_score`
  - `normalize_nonnegative_weight`
  - `row_to_weak_rank_from_scores`
  - `build_profile_from_scores`
- Generic build diagnostics metadata:
  - `profile_build_meta` (and internal metadata storage)
- Generic pattern/ranking summaries over `Profile`/`WeightedProfile`:
  - `profile_pattern_proportions`
  - `ranked_count`, `has_ties`
  - `ranking_type_support`, `ranking_type_template`
  - `profile_ranksize_summary`
  - `profile_ranking_type_proportions`
  - pretty-print helpers

`Preferences/src/Preferences.jl` now includes/exports this functionality.

## What stayed in `PrefPol`

`PrefPol/src/raw_profiles.jl` now keeps PrefPol-specific orchestration:

- supported years/constants and config-file path resolution
- TOML config loading and year validation
- scenario resolution and `compute_candidate_set` orchestration
- wrappers:
  - `load_raw_pref_data(...)`
  - `build_profile(df, year::Int; ...)`
  - `build_profile(raw::NamedTuple; ...)`

## What was split

- Candidate subset resolution:
  - Generic canonical matching moved to `Preferences.resolve_candidate_cols_from_set`.
  - PrefPol keeps candidate-universe/scenario decisions.
- Weight-column detection:
  - Generic matcher in `Preferences.guess_weight_col`.
  - PrefPol wrapper preserves preference ordering including `:peso`.
- Score normalization:
  - Chosen design: **Option A**.
  - ESEB-specific score rules (`[0,10]`, `96-99` missing codes) remain in PrefPol (`_normalize_score`).
  - PrefPol passes this callback into `Preferences.build_profile_from_scores`.
- Metadata and zero-rank recovery:
  - Generic build diagnostics metadata now belongs to `Preferences`.
  - PrefPol summary wrappers rely on the generic metadata behavior.

## Internal API/import changes

- `Preferences/Project.toml` updated with direct dependencies used by moved code:
  - `DataFrames`
  - `OrderedCollections`
  - `Printf` (stdlib entry)
- `PrefPol/src/raw_profiles.jl` simplified into thin wrappers over `Preferences` for generic operations.
- Public PrefPol call surface for raw-profile helpers remains the same.

## Compatibility checks performed

1. `Preferences` test suite:
   - Command: `julia --project=Preferences Preferences/test/runtests.jl` (run via Emacs batch)
   - Result: **pass**
2. Focused PrefPol raw-profile compatibility tests:
   - New file: `PrefPol/test/raw_profiles_tests.jl`
   - Command: `julia --project=PrefPol PrefPol/test/raw_profiles_tests.jl` (run via Emacs batch)
   - Result: **pass**
   - Covers:
     - DataFrame-to-weak-rank profile conversion
     - weighted vs unweighted behavior
     - ESEB missing/invalid score handling via PrefPol normalizer
     - candidate subset/canonical-name behavior
     - pattern summary/zero-rank metadata recovery behavior
3. PrefPol module load check:
   - Command: `julia --project=PrefPol -e 'using PrefPol; println("PREFPOL_OK")'`
   - Result: **pass**

Note on full PrefPol suite:
- `PrefPol/test/runtests.jl` currently errors in existing `processing_tests.jl` on
  assignment to `PrefPol.dont_know_her` under Julia 1.12 (`setproperty!` on missing global).
- This failure is pre-existing/unrelated to this raw-profiles refactor and occurs before
  the new raw-profile tests would run through the full suite entrypoint.
