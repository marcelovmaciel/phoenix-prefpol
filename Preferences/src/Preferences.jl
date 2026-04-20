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
export can_polarization, total_reversal_component, reversal_hhi, reversal_geometric
export strict_profile, ConsensusResult
export consensus_kendall, get_consensus_ranking, kendall_tau_dict
export consensus_for_group, group_avg_distance, weighted_coherence
export pairwise_group_divergence, overall_divergence, overall_divergences
export pairwise_group_overlap, pairwise_group_median_distance, pairwise_group_separation
export overall_overlap, overall_overlaps
export overall_divergence_median, overall_divergences_median
export overall_separation, overall_separations, grouped_gsep
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
