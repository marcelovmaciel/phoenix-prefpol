# =====================================
# src/Preferences.jl
# =====================================

module Preferences

using StaticArrays
using Random
using PrettyTables
using Crayons
using DataFrames
using OrderedCollections: OrderedDict
using Printf

# Include core components (keep your load order)
include("PreferenceCore.jl")
include("PreferencePairwise.jl")
include("PreferencePolicy.jl")
include("PreferenceBallot.jl")
include("PreferenceDynamics.jl")
include("PreferenceTraits.jl")
include("PreferenceProfile.jl")
include("PreferenceTabularProfiles.jl")
include("PreferenceAggregationProcedures.jl")
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

# Dynamics / mutable strict
export StrictRankMutable, swap_positions!, swap_ids!, swap_and_update_pairwise!

# Display helpers
export StrictRankView, WeakOrderView, pretty, show_pairwise_preference_table_color, pretty_pairwise
export pretty_profile_table, show_profile_table_color
export pretty_pairwise_majority_table, pretty_pairwise_majority
export pretty_pairwise_majority_counts, pretty_pairwise_majority_margins
export show_pairwise_majority_table_color

end # module
