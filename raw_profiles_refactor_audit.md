# Raw Profiles Refactor Audit

Scope: `PrefPol/src/raw_profiles.jl`

This audit classifies each constant/function as:
- MOVE TO PREFERENCES
- KEEP IN PREFPOL
- SPLIT: GENERIC CORE IN PREFERENCES + PREFPOL WRAPPER

## KEEP IN PREFPOL

- `RAW_PROFILE_SUPPORTED_YEARS`
  - Year support is a project policy tied to PrefPol’s configured election files.

- `_RAW_PROFILE_CFG_DIR`
  - This is path orchestration based on `project_root` and should remain local to PrefPol.

- `_LOCAL_PREFERENCES_SRC`
  - Local monorepo include path management is PrefPol runtime wiring, not a reusable preference abstraction.

- `_ensure_preferences_module()`
  - In this repo it protects PrefPol’s local include setup. The general package should not carry project-local module loading checks.

- `_load_raw_profile_cfg(year; config_path=nothing)`
  - Reads PrefPol election TOML and enforces PrefPol-supported years.

- `_infer_m(cfg)`
  - Depends on PrefPol `ElectionConfig` semantics (`n_alternatives`/`max_candidates`).

- `_scenario_for_year(cfg, scenario_name)`
  - Directly tied to PrefPol scenario definitions in TOML.

- `_resolve_candidate_cols(df, cfg; ...)`
  - Orchestrates PrefPol candidate-selection policy (`scenario_name`, `compute_candidate_set`, config universe).

- `load_raw_pref_data(year::Int; ...)`
  - End-to-end PrefPol data/config loader wrapper and return-shape contract for exploratory usage.

- `build_profile(df, year::Int; ...)`
  - Should remain as PrefPol convenience API that wires year/config/scenario choices to generic profile builders.

## MOVE TO PREFERENCES

- `_PROFILE_BUILD_META`
  - Metadata is generic profile-construction diagnostics (rows kept/skipped, weighting context).

- `_profile_build_meta(profile)`
  - Generic metadata accessor for `Profile`/`WeightedProfile` diagnostics.

- `_store_profile_build_meta!(profile; ...)`
  - Generic bookkeeping for construction outcomes and skip reasons.

- `_humanize_candidate_name(name)`
  - Generic label normalization (`_` to space + strip) not tied to ESEB.

- `_canonical_candidate_key(x)`
  - Generic candidate key normalization useful for robust candidate matching.

- `_candidate_display_symbols(candidate_cols)`
  - Generic conversion from candidate labels to display symbols with uniqueness check.

- `_normalize_weight(v)`
  - Generic nonnegative finite weight parser.

- `_row_to_weak_rank(...)`
  - Core score-row to `WeakRank` conversion is reusable if score normalization is injected.

- `_build_profile_with_candidates(...)`
  - Generic ballot/profile assembly from rows, candidate columns, and normalization hooks.

- `_ballot_pattern_string(...)`
  - Generic rendering of strict/weak ballots into pattern strings.

- `_validate_profile_input(profile)`
  - Generic type guard for `Profile`/`WeightedProfile` operations.

- `_profile_pattern_mass_table(...)`
  - Generic aggregation of ballots into pattern mass table.

- `_pattern_block_sizes(pattern)`
  - Generic parser for tie-block shape from pattern strings.

- `ranked_count(...)`
  - Generic ranking-size helper over pattern strings or block vectors.

- `has_ties(...)`
  - Generic tie-detection helper over pattern strings or block vectors.

- `_profile_pattern_features(...)`
  - Generic feature engineering over pattern tables.

- `_empty_ranksize_rows()`
  - Generic empty-table constructor for summary outputs.

- `profile_pattern_proportions(...)`
  - Generic pattern-proportion summarization over profiles.

- `ranking_type_support(r)`
  - Generic ordered-composition support generator.

- `ranking_type_template(blocks)`
  - Generic canonical rendering of ranking block types.

- `profile_ranksize_summary(...)`
  - Generic ranking-size navigation summary.

- `profile_ranking_type_proportions(...)`
  - Generic support-complete ranking-type decomposition.

- `_print_pattern_rows(...)`
  - Generic pretty-print helper for summary rows.

- `pretty_print_ranksize_summary(...)`
  - Generic formatter for rank-size summaries.

- `pretty_print_ranking_type_proportions(...)`
  - Generic formatter for ranking-type summaries.

- `pretty_print_profile_patterns(...)`
  - Generic formatter for pattern tables.

## SPLIT: GENERIC CORE IN PREFERENCES + PREFPOL WRAPPER

- `_resolve_candidate_cols_from_set(df, universe_cols, candidate_set)`
  - Core name/canonical matching is generic and should live in Preferences; PrefPol keeps the wrapper that chooses the configured universe.

- `_guess_weight_col(df)`
  - Generic column-name matching belongs in Preferences if parameterized. PrefPol wrapper should preserve current default preference including `peso`.

- `_normalize_score(v)`
  - Keep ESEB-specific rules (range `[0,10]`, missing codes `96-99`) in PrefPol.
  - Generic builder in Preferences should accept a `score_normalizer` callback (Option A).

- `build_profile(raw::NamedTuple; ...)`
  - Generic construction path from already-resolved `raw.candidate_cols` should delegate to Preferences.
  - PrefPol still owns year/scenario fallback behavior and exact argument compatibility.

- `_resolve_zero_rank_mass(profile; ...)`
  - Core metadata recovery logic is generic and should move to Preferences.
  - PrefPol keeps only policy-level choice about when to include this category (public API keyword behavior stays unchanged).

## Metadata decision

Decision: move metadata machinery to `Preferences`.

Rationale:
- The stored fields (`total_rows`, `kept_rows`, skipped categories, weighted context, zero-ranked dropped mass) are generic diagnostics of profile construction from tabular scores.
- These diagnostics are useful outside PrefPol whenever users need accountability for row filtering.
- PrefPol-specific preprocessing conventions (ESEB score codes) remain in the PrefPol score-normalizer callback, so metadata storage itself remains domain-agnostic.
