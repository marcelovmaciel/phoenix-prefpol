# =====================================
# src/Preferences.jl
# =====================================

module Preferences

using Combinatorics
using StaticArrays
using Random
using SHA
using PrettyTables
using Crayons
using DataFrames
using OrderedCollections: OrderedDict
using PooledArrays: PooledArray
using Printf
using StatsBase: proportionmap

# Include core components (keep your load order)
include("PreferenceCore.jl")
include("PreferencePairwise.jl")
include("PreferencePolicy.jl")
include("PreferenceBallot.jl")
include("PreferenceDynamics.jl")
include("PreferenceTraits.jl")
include("PreferenceProfile.jl")
include("PreferenceLinearization.jl")
include("PreferenceTabularProfiles.jl")
include("PreferenceAggregationProcedures.jl")
include("PreferenceMeasures.jl")
include("PreferenceMajorityGraphSupport.jl")
include("PreferenceMajorityGraphRoles.jl")
include("PreferencePluralitySwitchTables.jl")
include("single_peakedness.jl")
include("PreferenceConsensus.jl")
include("PreferenceAnnotatedProfiles.jl")
include("PreferenceDisplay.jl")
include("Compat.jl")


# ------------------------------------------------------------
# Exports (unified public surface)
# ------------------------------------------------------------

# Core pool API
export CandidatePool, labels, getlabel, candid, candidates, to_cmap

# Ballots & accessors
export StrictRank, StrictRankDyn, WeakRank, WeakRankDyn
export rank, prefers, indifferent, asdict
export perm, ranks
export to_perm, to_weakorder, ordered_candidates, weakorder_symbol_groups
export to_strict, make_rank_bucket_linearizer
export AbstractWeakOrderLinearizer, PatternConditionalLinearizer
export linearize

# Policies and pairwise (dense wrapper)
export ExtensionPolicy, BottomPolicyMissing, NonePolicyMissing, compare_maybe
export AbstractPairwise, PairwiseDense, to_pairwise
export Pairwise  # compat

# Triangular pairwise ballots (static/mutable/view) + helpers
export PairwiseTriangularStatic, PairwiseTriangularMutable, PairwiseTriangularView
export PairwiseBallotStatic, PairwiseBallotMutable, PairwiseBallotView  # compat
export pairwise_view, isdefined, score, pairwise_dense, dense
export is_complete, is_strict, is_weak_order, is_transitive

# Profiles
export Profile, WeightedProfile, nballots, weights, total_weight, validate
export resample_indices, bootstrap, bootstrap_counts
export restrict
export profile_build_meta

# Annotated profiles and tabular adapters
export AnnotatedProfile
export annotated_profile, dataframe_to_annotated_profile, annotated_profile_to_dataframe
export linearize_annotated_profile, subset_annotated_profile, profile_to_ranking_dicts

# Tabular-score profile construction and pattern summaries
export humanize_candidate_name, canonical_candidate_key
export candidate_display_symbols, guess_weight_col, resolve_candidate_cols_from_set
export candidate_missingness_table
export normalize_numeric_score, normalize_nonnegative_weight
export row_to_weak_rank_from_scores, build_profile_from_scores
export profile_pattern_proportions
export ranked_count, has_ties
export ranking_type_support, ranking_type_template
export profile_ranksize_summary, profile_ranking_type_proportions
export pretty_print_profile_patterns
export pretty_print_ranksize_summary, pretty_print_ranking_type_proportions

# Aggregation procedures
export PairwiseMajority, pairwise_majority
export pairwise_majority_counts, pairwise_majority_margins, pairwise_majority_wins

# Strict-profile measures
export ranking_signature, ranking_proportions, reversal_pairs
export kendall_tau_distance, average_normalized_distance
export axes_up_to_reversal, is_single_peaked, single_peaked_rankings
export single_peaked_distance, profile_distribution
export SinglePeakedAxisSummary, SinglePeakedSupportClassification, SinglePeakednessResult
export single_peakedness_summary, single_peakedness_L0, single_peakedness_L1
export single_peakedness_L1_off_axis
export best_single_peaked_axes
export can_polarization, total_reversal_component, reversal_hhi, reversal_geometric
export effective_observed_rankings, effective_reversal_rankings
export effective_reversal_ranking_diagnostics, ranking_support_diagnostics
export VoterTypeBasis, voter_type_basis, voter_type_masses
export MajoritySupportEdge, MajorityGraphSupportResult, majority_graph_support
export majority_edges_table, voter_type_table, edge_support_table, edge_overlap_table, core_table
export edge_effective_type_table, edge_effective_type_composition_table
export core_effective_type_table, reverse_core_effective_type_table
export core_effective_type_composition_table, effective_type_diagnostics
export support_core_effective_composition_table, support_core_above_threshold_type_table
export reverse_core_effective_composition_table, reverse_core_above_threshold_type_table
export edge_effective_composition_table, edge_above_threshold_type_table
export countergraph_summary_table
export boundary_distance_to_reverse, amenability_weight
export type_breaker_table, minimal_breaking_coalition_table
export GroupMajorityGraphSupportResult, group_majority_graph_support
export group_edge_power_table, group_breaker_table, group_anchor_table
export MajorityGraphRoleThresholds
export voter_type_role_table, edge_type_role_table
export role_mass_summary, primary_role_mass_summary, selected_edge_role_summary
export group_role_table, group_primary_role_table, group_role_power_table
export graph_role_summary, group_graph_role_summary
export plurality_scores_table, pairwise_vs_plurality_decomposition_table
export candidate_position_by_current_first_table, one_swap_target_table
export plurality_swing_value_table, exact_type_switch_table, group_target_switch_table
export strict_profile, ConsensusResult
export consensus_kendall, get_consensus_ranking, kendall_tau_dict
export consensus_for_group, group_avg_distance, weighted_coherence
export pairwise_group_divergence, overall_divergence, overall_divergences
export pairwise_group_overlap, pairwise_group_median_distance, pairwise_group_separation
export smoothed_overlap, overall_overlap, overall_overlap_smoothed
export overall_overlaps, overall_overlaps_smoothed
export overall_divergence_median, overall_divergences_median
export overall_separation, overall_separations, grouped_gsep
export normalized_consensus_separation, consensus_excess_separation
export group_E, aggregate_E, E
export S, S_old
export compute_group_metrics, bootstrap_group_metrics

# Dynamics / mutable strict
export StrictRankMutable, swap_positions!, swap_ids!, swap_and_update_pairwise!

# Display helpers
export StrictRankView, WeakOrderView, pretty, show_pairwise_preference_table_color, pretty_pairwise
export pretty_profile_table, show_profile_table_color
export pretty_pairwise_majority_table, pretty_pairwise_majority
export pretty_pairwise_majority_counts, pretty_pairwise_majority_margins
export show_pairwise_majority_table_color

end # module
