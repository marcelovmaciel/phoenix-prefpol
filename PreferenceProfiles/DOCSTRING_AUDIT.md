# PreferenceProfiles docstring audit

This audit inspected `PreferenceProfiles/src/PreferenceProfiles.jl` and every file included
from it, in include order. It records exported names that currently lack a
Julia docstring and classifies the missing documentation work before any source
edits.

## Classification key

1. `semantic contract`: public types, protocols, conversions, mutators, and
   constructors whose behavior callers rely on.
2. `mathematical measure`: social-choice statistics, aggregations, diagnostics,
   or tables whose formula affects interpretation.
3. `empirical data adapter`: tabular, annotated-profile, or DataFrame-facing
   conversion layer.
4. `display/formatting helper`: pretty-printers, views, and table renderers.
5. `internal helper`: implementation-only helper that should use comments, not
   public API docstrings. No exported missing symbol below is a pure category-5
   helper; if one is later judged internal, the better fix is to unexport it or
   mark it as a legacy/private alias rather than write a full public contract.

## Priority order

1. Core social-choice contracts: `CandidatePool`, `StrictRank`, `WeakRank`,
   `Profile`, `WeightedProfile`, pairwise protocols, `pairwise_majority`, and
   weak-order linearization.
2. Interpretation-sensitive measures: reversal, polarization,
   single-peakedness result wrappers, consensus/group metrics, majority graph
   support, majority graph roles already documented, and plurality-switch
   tables.
3. Empirical adapters: annotated profiles, strict-profile conversion from
   DataFrames/vectors, and DataFrame metric overloads.
4. Display helpers and legacy aliases.

## Missing exported docstrings

| File | Exported names without docstrings | Category | Public docstring plan |
| --- | --- | --- | --- |
| `PreferenceCore.jl` | DONE: `CandidatePool`, `labels`, `getlabel`, `candid`, `candidates`, `to_cmap` | semantic contract | Done in `PreferenceCore.jl`: documented canonical candidate universe/order, pool-relative 1-based candidate IDs, duplicate/empty rejection, and copy/accessor behavior. |
| `PreferencePolicy.jl` | DONE: `ExtensionPolicy`, `BottomPolicyMissing`, `NonePolicyMissing`, `compare_maybe` | semantic contract | Done in `PreferencePolicy.jl`: documented extension-policy protocol, `+1/-1/0/missing`, tie behavior, and missing-rank treatment for both policies. |
| `PreferencePairwise.jl` | DONE: `AbstractPairwise`, `score`, `isdefined`, `dense` | semantic contract | Done in `PreferencePairwise.jl`: documented pairwise interface, diagonal convention, `score(i,j)` orientation, missing/tie distinction, and exported `isdefined` extension. |
| `PreferenceBallot.jl` | DONE: `StrictRank`, `StrictRankDyn`, `WeakRank`, `WeakRankDyn` | semantic contract | Done in `PreferenceBallot.jl`: documented strict best-to-worst ID permutations, weak rank vectors indexed by candidate ID, `missing`, ties, rank direction, and storage aliases. |
| `PreferenceBallot.jl` | DONE: `perm`, `ranks`, `rank`, `prefers`, `indifferent`, `asdict` | semantic contract | Done in `PreferenceBallot.jl`: documented accessor return types, copy vs exposed storage, weak missing comparisons, tie behavior, and dictionary omission of unranked candidates. |
| `PreferenceBallot.jl` | DONE: `to_perm`, `ordered_candidates`, `to_weakorder`, `weakorder_symbol_groups`, `make_rank_bucket_linearizer` | semantic contract | Done in `PreferenceBallot.jl`: documented weak-order levels, final unranked group, unranked append behavior in `to_perm`, candidate-symbol mapping, and bucket linearizer strategies. |
| `PreferenceBallot.jl` | DONE: `PairwiseDense`, `pairwise_dense`, `to_pairwise`, `restrict` | semantic contract | Done in `PreferenceBallot.jl`: documented dense pairwise orientation, matrix accessor aliasing, extension-policy dependency, subset/backmap return values, weak-rank renormalization, and missing-pair preservation. `pairwise_dense` was newly discovered as an exported omission for this file. |
| `PreferenceDynamics.jl` | DONE: `StrictRankMutable`, `swap_positions!`, `swap_ids!`, `swap_and_update_pairwise!` | semantic contract | Done in `PreferenceDynamics.jl`: documented mutable rank storage as `ranks[id] = position`, mutation in place, position-vs-id distinction, and full pairwise recomputation after swaps. |
| `PreferenceDynamics.jl` | DONE: `PairwiseTriangularStatic`, `PairwiseTriangularMutable`, `PairwiseTriangularView`, `pairwise_view`, `pairwise_dense` | semantic contract | Done in `PreferenceDynamics.jl`: documented upper-triangle storage, view aliasing, mask-as-defined convention, sign symmetry, dense expansion type, fresh allocation, and diagonal zero convention. |
| `PreferenceTraits.jl` | DONE: `is_complete`, `is_strict`, `is_weak_order`, `is_transitive` | semantic contract | Done in `PreferenceTraits.jl`: documented default-false fallback and supported ballot/profile/pairwise interpretations, including `WeakRank` missingness conventions. |
| `PreferenceProfile.jl` | DONE: `Profile`, `WeightedProfile`, `nballots`, `weights`, `total_weight`, `validate` | semantic contract | Done in `PreferenceProfile.jl`: documented common-pool finite profiles, uniform concrete ballot type, survey/population weights rather than replicated rows, validation checks, and weight accessor behavior. |
| `PreferenceProfile.jl` | DONE: `resample_indices`, `bootstrap`, `bootstrap_counts`, `restrict` | semantic contract | Done in `PreferenceProfile.jl`: documented bootstrap sampling with replacement, weighted sampling from profile weights, unweighted bootstrap output for weighted profiles, invalid-weight errors, and profile restriction backmap semantics. |
| `PreferenceLinearization.jl` | DONE: `AbstractWeakOrderLinearizer`, `PatternConditionalLinearizer`, `linearize` | semantic contract | Done in `PreferenceLinearization.jl`: documented weak-order linearization as an interpretation step, tie-breaking options, `rng`, incomplete policies, pattern-conditional reference requirements, alpha smoothing, fallback, and cache semantics. |
| `PreferenceAnnotatedProfiles.jl` | DONE: `AnnotatedProfile`, `annotated_profile`, `dataframe_to_annotated_profile`, `annotated_profile_to_dataframe`, `linearize_annotated_profile`, `subset_annotated_profile`, `profile_to_ranking_dicts`, `strict_profile(::AnnotatedProfile)` | empirical data adapter | Done in `PreferenceAnnotatedProfiles.jl`: documented metadata row-count invariants, supported DataFrame encodings, `profile_kind` metadata, compact rank-vector decoding, metadata copying/subsetting, covariate preservation, and that `linearize_annotated_profile` forces `incomplete_policy = :error`. `strict_profile(::AnnotatedProfile)` was newly discovered as an exported overload in this file. |
| `PreferenceAnnotatedProfiles.jl` | DONE: DataFrame/annotated overloads of `overall_divergences`, `overall_overlaps`, `overall_overlaps_smoothed`, `overall_divergences_median`, `overall_separations`, `compute_group_metrics` | empirical data adapter | Done in `PreferenceAnnotatedProfiles.jl`: documented how annotated metadata groups are mapped to subgroup profiles, how weights are preserved and used in group proportions, and accepted consensus column names (`:consensus_result`, `:consensus_ranking`, `:x1`). |
| `PreferenceAggregationProcedures.jl` | DONE: `PairwiseMajority`, `pairwise_majority`, `pairwise_majority_counts`, `pairwise_majority_margins`, `pairwise_majority_wins` | mathematical measure | Done in `PreferenceAggregationProcedures.jl`: documented pairwise count orientation (`counts[i,j]` is mass preferring `i` to `j`), margins as count differences, wins as margin signs with ties zero, omission of weak ties/missing pairs and pairwise `0`/`missing`, and current public aggregation scope for `Profile`. |
| `PreferenceMeasures.jl` | DONE: `ranking_signature`, `ranking_proportions`, `reversal_pairs` | mathematical measure | Done in `PreferenceMeasures.jl`: documented canonical strict ranking signatures as candidate-symbol tuples, empirical ranking-type distributions for unweighted and weighted strict profiles, zero-mass ranking-proportion convention, observed-support ordering, and exact `r`/`reverse(r)` pairing. |
| `PreferenceMeasures.jl` | DONE: `kendall_tau_distance`, `average_normalized_distance` | mathematical measure | Done in `PreferenceMeasures.jl`: documented discordant-pair Kendall distance, range `0:binomial(m, 2)`, normalization by `binomial(m, 2)`, strict-rank inputs, positive-mass requirements, and weighted averaging. |
| `PreferenceMeasures.jl` | DONE: `can_polarization`, `total_reversal_component`, `reversal_hhi`, `reversal_geometric`, `effective_observed_rankings`, `effective_reversal_rankings`, `effective_reversal_ranking_diagnostics`, `ranking_support_diagnostics` | mathematical measure | Done in `PreferenceMeasures.jl`: documented `Ψ`, `R`, reversal HHI `κ`, `sqrt(R * κ)`, inverse-HHI effective observed rankings, inverse-HHI effective reversal rankings, diagnostics, support diagnostics, ranges, and empty/zero-mass or zero-reversal conventions. |
| `single_peakedness.jl` | DONE: `SinglePeakedAxisSummary`, `SinglePeakedSupportClassification`, `SinglePeakednessResult` | mathematical measure | Done in `single_peakedness.jl`: documented result fields, axis ids, axis support, off-axis rows, `L0`, unconditional normalized `L1`, conditional `L1_off_axis`, proportion sources, and `missing` convention when no off-axis mass exists. |
| `single_peakedness.jl` | DONE: `single_peakedness_L0`, `single_peakedness_L1`, `single_peakedness_L1_off_axis`, `best_single_peaked_axes` | mathematical measure | Done in `single_peakedness.jl`: documented wrappers as projections from `single_peakedness_summary`, including support-optimal `best_single_peaked_axes` and the distinction between axis support, full-profile L1 distortion, and off-axis L1 distortion. |
| `PreferenceMajorityGraphSupport.jl` | DONE: `MajoritySupportEdge`, `MajorityGraphSupportResult`, `GroupMajorityGraphSupportResult` | mathematical measure | Done in `PreferenceMajorityGraphSupport.jl`: documented result fields, total-mass normalization, majority-edge tie omission/error policy, support matrix orientation, overlap matrices, support-core semantics, reverse-core semantics, and group mass conventions including `missing => :NA`. |
| `PreferenceMajorityGraphSupport.jl` | DONE: `majority_edges_table`, `voter_type_table`, `edge_support_table`, `edge_overlap_table`, `core_table` | mathematical measure | Done in `PreferenceMajorityGraphSupport.jl`: documented table row units and columns, stable `type_index`/`edge_index`, support/opposition mass definitions, conditional-overlap orientation, Jaccard overlap, and integer flip-count convention. |
| `PreferenceMajorityGraphSupport.jl` | DONE: `edge_effective_type_composition_table`, `core_effective_type_table`, `reverse_core_effective_type_table`, `core_effective_type_composition_table`, `effective_type_diagnostics`, `support_core_effective_composition_table`, `support_core_above_threshold_type_table`, `reverse_core_effective_composition_table`, `reverse_core_above_threshold_type_table`, `edge_effective_composition_table`, `edge_above_threshold_type_table`, `countergraph_summary_table` | mathematical measure | Done in `PreferenceMajorityGraphSupport.jl`: documented HHI and inverse-HHI effective-number formulas, `1/neff` threshold rule with strict greater-than inclusion, support vs opposition side, reverse-core definition, thresholded effective composition, and compatibility reshapes. |
| `PreferenceMajorityGraphSupport.jl` | DONE: `amenability_weight`, `type_breaker_table`, `minimal_breaking_coalition_table` | mathematical measure | Done in `PreferenceMajorityGraphSupport.jl`: documented boundary-distance weighting modes, the relation being broken (`winner -> loser`), raw vs weighted breaking score, threshold `normalized_margin / 2`, sorting rule for minimal coalitions, and supporter-only behavior. |
| `PreferenceMajorityGraphSupport.jl` | DONE: `group_majority_graph_support`, `group_edge_power_table`, `group_breaker_table`, `group_anchor_table` | mathematical measure | Done in `PreferenceMajorityGraphSupport.jl`: documented group-label handling including `missing => :NA`, group mass normalization by total profile mass, conditional anchoring, group support/margin contribution, and group breaker score. |
| `PreferencePluralitySwitchTables.jl` | DONE: `plurality_scores_table`, `pairwise_vs_plurality_decomposition_table`, `candidate_position_by_current_first_table`, `one_swap_target_table`, `plurality_swing_value_table`, `exact_type_switch_table`, `group_target_switch_table` | mathematical measure | Done in `PreferencePluralitySwitchTables.jl`: documented row units and columns for plurality-switch tables, first-place mass vs share, current-first conditioning, target/opponent interpretation, one-swap definition (`target` in second position), per-voter swing (`2` when switching from opponent, otherwise `1`), group labels, candidate filters, and weight conventions; pairwise-majority effects are distinguished from plurality effects. |
| `PreferenceConsensus.jl` | DONE: `ConsensusResult`, `consensus_kendall`, `get_consensus_ranking`, `kendall_tau_dict` | mathematical measure | Done in `PreferenceConsensus.jl`: documented Kemeny/Kendall consensus search, exhaustive strict-order catalog, normalization by `binomial(m, 2)`, total-mass weighting, tied minimizers, SHA-based deterministic pseudorandom tie rule when `rng` is absent, and returned ranking formats. |
| `PreferenceConsensus.jl` | DONE: `strict_profile`, `consensus_for_group`, `group_avg_distance`, `weighted_coherence` | empirical data adapter | Done in `PreferenceConsensus.jl`: documented accepted formal profile, vector-of-dictionaries, and DataFrame inputs; candidate inference and empty-vector metadata requirements; group consensus outputs; group coherence `C_g`; and aggregate coherence `C = sum_g π_g C_g`. |
| `PreferenceConsensus.jl` | DONE: `pairwise_group_divergence`, `overall_divergence`, `overall_divergences`, `overall_divergences_median`, `overall_separations`, `compute_group_metrics`, `bootstrap_group_metrics` | mathematical measure | Done in `PreferenceConsensus.jl`: documented directed divergence `D_{i -> j}`, aggregate `D`, median-set divergence, separation `Sep`, DataFrame adapters, accepted consensus maps, returned `(C, D)` shorthand, bootstrap result keys, group-pair weights, and group-mass conventions. |
| `PreferenceConsensus.jl` | DONE: `overall_overlaps`, `overall_overlaps_smoothed` | mathematical measure | Done in `PreferenceConsensus.jl`: documented DataFrame adapters around exact and radius-1 smoothed overlap, grouping reconstruction, candidate-set requirements, zero-mass behavior, and returned overlap quantities. |
| `PreferenceConsensus.jl` | DONE: `S`, `S_old`, `consensus_excess_separation`, `group_E`, `aggregate_E`, `E` | mathematical measure | Done in `PreferenceConsensus.jl`: documented `S = D - (1 - C)/2` as the nonnegative excess-divergence component for admissible profile-derived `C,D`, `S_old` as legacy support separation, and `E`/related aliases as the normalized excess-divergence ratio `1 - W/D` in the same derived `C`-`D` decomposition family. |
| `Compat.jl` | DONE: `Pairwise`, `PairwiseBallotStatic`, `PairwiseBallotMutable`, `PairwiseBallotView` | semantic contract | Done in `Compat.jl`: documented each exported name as a backward-compatible alias only, with the canonical replacement type named. |
| `PreferenceDisplay.jl` | DONE: `StrictRankView`, `WeakOrderView`, `pretty`, `show_pairwise_preference_table_color`, `pretty_profile_table`, `show_profile_table_color`, `pretty_pairwise_majority_table`, `pretty_pairwise_majority`, `pretty_pairwise_majority_counts`, `pretty_pairwise_majority_margins`, `show_pairwise_majority_table_color` | display/formatting helper | Done in `PreferenceDisplay.jl`: documented output wrappers vs printing side effects, color/ANSI behavior, `hide_unranked`, accepted profile/pairwise inputs, majority table `kind = :wins/:counts/:margins`, and that display helpers do not define measurement semantics. `pretty_pairwise` was already documented. |

## Already documented exports needing convention expansion

These exported names have docstrings, but their docstrings should be expanded
because they are in the interpretation-critical path:

- DONE: `PreferenceBallot.jl`: `to_strict` now states bucket ordering,
  random/custom tie behavior, unranked handling, and error behavior.
- DONE: `PreferenceTabularProfiles.jl`: `row_to_weak_rank_from_scores` and
  `build_profile_from_scores` now spell out score ordering, tie handling,
  incomplete-row filtering, all-unranked rows, zero/invalid weights, and stored
  `profile_build_meta` conventions.
- DONE: `PreferenceTabularProfiles.jl`: `profile_pattern_proportions`,
  `profile_ranksize_summary`, and `profile_ranking_type_proportions` now make
  zero-ranked mass, weighted/unweighted totals, and unranked omission explicit.
- DONE: `PreferenceMajorityGraphSupport.jl`: `majority_graph_support` now documents edge construction, tie policy, voter-type support of majority edges, overlap/core quantities, and mass-normalization conventions.
- DONE: `single_peakedness.jl`: `single_peakedness_summary` now has expanded mathematical definitions for `L0`, `L1`, and `L1_off_axis`; exported result types now document field semantics, axis support, off-axis distance, and zero/off-axis conventions.
- DONE: `PreferenceConsensus.jl`: `S` is documented as the current derived excess-divergence diagnostic, `E` and aliases as normalized derived `C`-`D` diagnostics, and `S_old` as legacy; DataFrame overloads and thin group-overlap/divergence/separation docs were expanded.

## Internal comments vs public docs

Implementation helpers beginning with `_` across the inspected files should stay
out of public docstring work unless they are intentionally exported later.
Examples include `_ranking_masses`, `_profile_pattern_features`,
`_compatible_extension_distribution`, `_core_effective_row`, and
`_group_profiles_from_dataframe`. If their behavior is subtle, add short code
comments near the implementation rather than public docstrings.

Conversely, every exported name in the missing table should receive either a
public docstring or a deliberate export/deprecation decision before it is treated
as internal.


## Targeted representation-layer update

### Newly discovered undocumented public API

- DONE: `PreferenceBallot.jl`: `pairwise_dense(::PairwiseDense)` is exported in
  `PreferenceProfiles/src/PreferenceProfiles.jl` and defined in the target file, but was not
  listed in that file's audit row. It was added to the row above and documented
  with the dense-matrix aliasing and pairwise-orientation conventions.
- DONE: `PreferenceAnnotatedProfiles.jl`: `strict_profile(::AnnotatedProfile)`
  is an exported generic overload defined in this file but was not listed in the
  annotated-profile audit row. It was added to the row above and documented as a
  metadata-dropping adapter to the formal profile.

### Skipped symbols and reasons

- `PreferenceCore.jl`: `MAX_STATIC_N`, `CandidateId`, `_tlen`, and `_tidx` were
  skipped because they are unexported implementation details. `_tidx` already
  carries the non-obvious triangular indexing convention in a local code comment.
- `PreferenceBallot.jl`: `_strict_storage`, `_weakrank_storage`,
  `_resolve_bucket_linearizer`, `_pairwise_from_ranks!`, `_ranks_from_strict`,
  `_restrict_pool`, and `_restrict_weak_ranks` were skipped because they are
  unexported helpers. Their public conventions are now documented on
  `StrictRank`, `WeakRank`, `make_rank_bucket_linearizer`, `to_pairwise`, and
  `restrict`.
- `PreferenceLinearization.jl`: internal coercion, cache-key, extension
  enumeration, compatibility-distribution, sampling, normalization, and
  preserve-mode helpers were skipped because the exported contracts on
  `PatternConditionalLinearizer` and `linearize` now describe their public
  semantics.
- `PreferenceProfile.jl`: `_ballot_n` and `_normalize_ballots` were skipped as
  unexported construction helpers; the public construction invariant is
  documented on `Profile` and `WeightedProfile`.
- `PreferenceTabularProfiles.jl`: `_coerce_column_identifier`,
  `_coerce_column_vector`, `_validated_positive_weights`,
  `_coerce_missing_codes`, `_value_counts_as_missing`, profile pattern feature
  helpers, and print-row helpers were skipped because they are unexported
  implementation details. Their public conventions are now documented on
  `candidate_missingness_table`, `row_to_weak_rank_from_scores`,
  `build_profile_from_scores`, and the exported pattern-summary functions.
- `PreferenceAnnotatedProfiles.jl`: metadata validation/conversion helpers,
  compact rank-code helpers, group-index helpers, and
  `_compute_group_metric_details` were skipped because they are unexported
  implementation details. Public behavior is documented on `AnnotatedProfile`,
  DataFrame conversion functions, subsetting/linearization functions, and the
  annotated group-metric overloads.
- `PreferenceAnnotatedProfiles.jl`: `dict_profile_to_preferences` and
  `compact_profile_artifact_dataframe` were skipped because they are not
  exported in `PreferenceProfiles/src/PreferenceProfiles.jl`; `dataframe_to_annotated_profile`
  now documents the public decoding conventions that callers rely on.
- `Compat.jl`: `_labels_from` was skipped because it is an unexported legacy
  display helper.

### Needs human review

- `StrictRank(perm::AbstractVector)` and `WeakRank(ranks::AbstractVector{Union{Int,Missing}})`
  are reachable through exported type constructors but are not pool-aware and do
  less validation than the pool-aware constructors. The new type docstrings
  describe the representation invariant and call out pool-aware constructor
  errors, but a maintainer should decide whether unchecked constructors should
  remain public convenience constructors, gain validation, or be marked private.
- `pairwise_dense` is exported as a generic and also has triangular pairwise
  methods outside this task's file scope. This pass documented the
  `PairwiseDense` method in `PreferenceBallot.jl`; the `PreferenceDynamics.jl`
  audit row remains the controlling item for triangular-method semantics.
- `build_profile_from_scores` documents that custom `weight_normalizer`
  functions are expected to return finite nonnegative weights or `nothing`, but
  this is a caller contract rather than fully enforced by the current
  `WeightedProfile` constructor. A maintainer should decide whether construction
  should call strict validation or keep the adapter contract as-is.

## Targeted social-choice docstring update

### Newly discovered undocumented public API

- No additional exported public API lacking docstrings was discovered in the targeted files beyond the symbols already controlled by the audit rows above.

### Skipped symbols and reasons

- `PreferenceMeasures.jl`: `_ranking_masses`, `_canonical_ranking_signature`, `_local_reversal_values`, `_effective_observed_rankings`, `_effective_reversal_rankings`, `_ranking_support_diagnostics`, `_pairwise_preference_counts`, `_positive_mass_error`, and `possible_rankings_count` were skipped because they are unexported helpers. Their public semantics are documented on the exported ranking, reversal, effective-number, support-diagnostic, Kendall, and polarization measures.
- `single_peakedness.jl`: `SinglePeakedSupportEntry`, `_ranking_vector`, `_validate_strict_linear_order`, `_kendall_distance_vectors`, `_single_peaked_distance_strict_ids`, `_normalize_proportion_source`, `_selected_axis_ids`, and `_axis_to_ids` were skipped because they are unexported helpers or internal result rows. Their public conventions are documented on `profile_distribution`, `single_peakedness_summary`, exported result types, and wrapper functions.
- `PreferenceConsensus.jl`: `LinearOrderCatalog`, cache and stable-serialization helpers, consensus implementation helpers, group-profile reconstruction helpers, smoothing helpers, and private within/cross distance helpers were skipped because they are unexported implementation details. Public behavior is documented on `ConsensusResult`, `consensus_kendall`, group divergence/overlap/separation functions, DataFrame adapters, aliases, and bootstrap metrics.

### Needs human review

- `PreferenceConsensus.jl`: `pairwise_group_divergence(profile_i, consensus_j, m)` accepts `m`, but the implementation normalizes through `average_normalized_distance(strict, consensus_j)`, which uses `length(strict.pool)`; `m` is currently only checked to be at least `2`. The docstring describes the intended normalized Kendall formula, but a maintainer should decide whether `m` should be enforced against the profile size, removed, or kept as a legacy validation argument.
- `PreferenceMeasures.jl`: `reversal_hhi` is documented with manuscript label `κ` per the targeted task. If the manuscript assigns `κ` to a different reversal concentration statistic, the label should be revised before publication.


## Targeted majority-graph and role-analysis docstring update

### Newly discovered undocumented public API

- No additional exported public API lacking docstrings was discovered in the targeted files beyond the symbols already controlled by the audit rows above.
- `PreferenceMajorityGraphRoles.jl` exported table functions already had docstrings, but they were thin. They were expanded to document row units, columns, and the role-threshold classification rule.

### Skipped symbols and reasons

- `PreferenceAggregationProcedures.jl`: `_pairwise_n` and `_accumulate_pairwise_counts!` were skipped because they are unexported helpers; the public count, margin, tie, and missing-pair conventions are documented on `PairwiseMajority` and the exported pairwise-majority functions.
- `PreferenceMajorityGraphSupport.jl`: permutation construction, ranking-label, HHI/effective-number, threshold-row, top-type, integer-flip, and group-symbol helpers were skipped because they are unexported implementation details. The public conventions are documented on the result types and table-producing functions.
- `PreferenceMajorityGraphRoles.jl`: role validation, quantile, role-string, primary-role, and selected-edge helpers were skipped because they are unexported implementation details. The exported threshold type and role tables now document the public classification rules.
- `PreferencePluralitySwitchTables.jl`: candidate coercion, profile mass/weight, basis, ranking-position, and candidate-filter helpers were skipped because they are unexported implementation details. The exported plurality-switch tables now document candidate filters, row units, and mass/share conventions.

### Needs human review

- `PreferencePluralitySwitchTables.jl`: `group_target_switch_table` accepts a `basis` keyword but the current implementation works directly from ballots and does not use that keyword. This pass documented the table behavior without changing the signature or behavior.
- `PreferenceMajorityGraphRoles.jl`: `voter_type_role_table` marks edge breakers using `>= breaker_quantile` while the effective-type tables use strict `>` for above-effective-threshold membership. The docstrings state the current conventions, but maintainers may want to confirm that the inclusive vs strict threshold distinction is intentional.


## Final documentation consistency pass

### Final status

- All exported symbols listed in `PreferenceProfiles/src/PreferenceProfiles.jl` are now either documented by source docstrings or covered by explicit audit notes. No intentionally undocumented exported PreferenceProfiles symbols remain from this pass.
- The final open rows for `PreferenceDynamics.jl` and `PreferenceDisplay.jl` were completed with compact public docstrings. Internal mutation helpers such as `set!`, `clear!`, and pairwise construction helpers remain undocumented because they are not exported.
- Formula-label consistency was checked statically for `Ψ`, `R`, `κ`, `C`, `D`, `G`, effective rankings, majority support, and role classifications. No broad rewrites were made. One role-classification wording issue was corrected: `edge_breaker` now says the score meets or exceeds the quantile, matching the implementation's `>=` rule.
- Public `PreferenceProfiles` docstrings continue to describe reusable ranking/profile/measure semantics; applied pipeline and paper-workflow semantics remain in `PrefPol` docs.

### Remaining undocumented exports

- None. Runtime `Docs.doc(Docs.Binding(PreferenceProfiles, name))` discovery passed under Julia 1.11.8 with no missing exported names.

### Remaining needs human review

- `StrictRank(perm::AbstractVector)` and `WeakRank(ranks::AbstractVector{Union{Int,Missing}})` remain unchecked convenience constructors; maintainers should decide whether to keep, validate, or mark them private.
- `pairwise_dense` is exported as a generic with methods in both ballot and triangular-pairwise layers; semantics are now documented in both relevant rows, but maintainers may want to keep the method split explicit.
- `build_profile_from_scores` documents a caller contract for custom `weight_normalizer` outputs that is not fully enforced by `WeightedProfile` construction.
- `pairwise_group_divergence(profile_i, consensus_j, m)` keeps `m` as a legacy validation argument while normalization uses the strict profile's pool size.
- `reversal_hhi` uses manuscript label `κ`; confirm this label against the manuscript before publication.
- `group_target_switch_table` accepts a currently unused `basis` keyword.
- `voter_type_role_table` uses inclusive `>=` for breaker quantiles while effective-threshold tables use strict `>`; docstrings state the implemented conventions.

### Validation

- Exact project Julia `1.11.9` was reinstalled through Juliaup after the original `libjulia-codegen.so.1.11` / `libLLVM-16jl.so` loader failure, but the refreshed 1.11.9 sysimage remains unusable on this machine: even `julia +1.11.9 --startup-file=no -e 'println(1 - 1)'` fails inside Base with missing bindings such as `_deleteat!`/`findlast`.
- Validation was therefore run with Julia `1.11.8`, the nearest working 1.11 patch release installed through Juliaup. `julia +1.11.8 --project=PreferenceProfiles -e 'using PreferenceProfiles'` passed.
- The exported-name `Docs.doc(Docs.Binding(PreferenceProfiles, name))` discovery check passed under Julia 1.11.8 with no missing exported names.
- `Pkg.test()` under Julia 1.11.8 is blocked by the package's exact Julia compat requirement for 1.11.9. As a fallback, `julia +1.11.8 --project=PreferenceProfiles PreferenceProfiles/test/runtests.jl` ran successfully; all displayed testsets passed.
