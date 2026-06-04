# API Reference

This is a curated API map organized by the main workflow. For every exported
binding, see [Full Public API](api_full.md).

## Core Preference Objects

- `CandidatePool`
- `StrictRank`
- `WeakRank`
- `Profile`
- `WeightedProfile`
- `rank`
- `prefers`
- `indifferent`
- `asdict`
- `ordered_candidates`


## Empirical Profile Construction

- `humanize_candidate_name`
- `canonical_candidate_key`
- `candidate_display_symbols`
- `guess_weight_col`
- `resolve_candidate_cols_from_set`
- `candidate_missingness_table`
- `normalize_numeric_score`
- `normalize_nonnegative_weight`
- `row_to_weak_rank_from_scores`
- `build_profile_from_scores`
- `profile_build_meta`


## Weak Orders and Linearization

- `to_weakorder`
- `weakorder_symbol_groups`
- `ExtensionPolicy`
- `NonePolicyMissing`
- `BottomPolicyMissing`
- `to_pairwise`
- `to_strict`
- `linearize`
- `make_rank_bucket_linearizer`
- `PatternConditionalLinearizer`


## Annotated Profiles

- `AnnotatedProfile`
- `annotated_profile`
- `dataframe_to_annotated_profile`
- `annotated_profile_to_dataframe`
- `profile_to_ranking_dicts`
- `linearize_annotated_profile`
- `subset_annotated_profile`
- `strict_profile`
- `compute_group_metrics`


## Global Profile Diagnostics

- `ranking_proportions`
- `reversal_pairs`
- `can_polarization`
- `total_reversal_component`
- `reversal_hhi`
- `reversal_geometric`
- `effective_observed_rankings`
- `effective_reversal_rankings`
- `effective_reversal_ranking_diagnostics`
- `ranking_support_diagnostics`


## Consensus and Group Diagnostics

- `ConsensusResult`
- `kendall_tau_distance`
- `average_normalized_distance`
- `consensus_kendall`
- `get_consensus_ranking`
- `kendall_tau_dict`
- `consensus_for_group`
- `group_avg_distance`
- `weighted_coherence`
- `pairwise_group_divergence`
- `overall_divergence`
- `S`
- `normalized_consensus_separation`
- `consensus_excess_separation`
- `group_E`
- `aggregate_E`
- `E`


## Majority-Graph and Plurality Diagnostics

- `VoterTypeBasis`
- `voter_type_basis`
- `voter_type_masses`
- `MajoritySupportEdge`
- `MajorityGraphSupportResult`
- `majority_graph_support`
- `MajorityGraphRoleThresholds`
- `voter_type_role_table`
- `edge_type_role_table`
- `plurality_scores_table`
- `pairwise_vs_plurality_decomposition_table`
- `candidate_position_by_current_first_table`
- `one_swap_target_table`
- `plurality_swing_value_table`
- `exact_type_switch_table`
- `group_target_switch_table`


## Advanced Representations

- `labels`
- `getlabel`
- `candid`
- `candidates`
- `to_cmap`
- `perm`
- `ranks`
- `AbstractPairwise`
- `PairwiseDense`
- `PairwiseTriangularStatic`
- `PairwiseTriangularMutable`
- `PairwiseTriangularView`
- `pairwise_view`
- `score`
- `isdefined`
- `pairwise_dense`
- `dense`
- `StrictRankMutable`
- `swap_positions!`
- `swap_ids!`
- `swap_and_update_pairwise!`

