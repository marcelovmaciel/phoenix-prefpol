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
# Binding-level docs for public help/discovery
# ------------------------------------------------------------

@doc raw"""
    ConsensusResult

Result of a Kendall/Kemeny consensus search. Fields record the candidate tuple,
selected consensus permutation/ranking/ballot, objective value, average
normalized Kendall distance, total mass, tied-minimizer diagnostics, tie rule,
and all minimizing permutations when requested.
""" ConsensusResult

@doc raw"""
    SinglePeakedAxisSummary

Summary row for one candidate axis in `single_peakedness_summary`, including
axis identifiers, support, off-axis mass, and normalized distance quantities.
""" SinglePeakedAxisSummary

@doc raw"""
    SinglePeakedSupportClassification

Classification row connecting an observed ranking type to a candidate axis in
single-peakedness diagnostics.
""" SinglePeakedSupportClassification

@doc raw"""
    SinglePeakednessResult

Container returned by `single_peakedness_summary`, including axis summaries,
selected support classifications, observed support, and best-axis identifiers.
""" SinglePeakednessResult

@doc raw"""
    rank(x, pool, name)

Return a candidate's rank in a strict or weak ballot using the candidate IDs of
`pool`. Lower ranks are better; weak rankings may return `missing` for unranked
candidates.
""" rank

@doc raw"""
    prefers(x, pool, a, b)

Return whether ballot `x` ranks candidate `a` strictly above candidate `b`.
Weak-rank ties, missing entries, and unranked candidates follow the ballot's
comparison semantics.
""" prefers

@doc raw"""
    indifferent(x, pool, a, b)

Return whether candidates `a` and `b` are tied or otherwise indifferent in
ballot `x`, using pool-relative candidate IDs.
""" indifferent

@doc raw"""
    asdict(x, pool)

Convert a strict or weak ballot to a candidate-symbol dictionary. Strict ranks
map every candidate to its position; weak ranks omit unranked candidates.
""" asdict

@doc raw"""
    to_perm(x)

Return candidate IDs in best-to-worst order. For weak ranks this is a
deterministic ordering view with tied present ranks kept in candidate-ID order
and unranked candidates appended.
""" to_perm

@doc raw"""
    to_pairwise(x, ...)

Convert a ballot to pairwise-comparison form. Strict rankings produce complete
pairwise comparisons; weak rankings use the supplied extension policy for ties
and missing ranks.
""" to_pairwise

@doc raw"""
    ordered_candidates(x, pool)

Return candidate symbols in the order induced by ballot `x` and candidate pool
`pool`.
""" ordered_candidates

@doc raw"""
    weakorder_symbol_groups(x, pool)

Return weak-order groups as candidate symbols. Groups are ordered from best to
worst; the final group may contain unranked candidates depending on the weak
rank.
""" weakorder_symbol_groups

@doc raw"""
    make_rank_bucket_linearizer(strategy)

Construct a weak-rank bucket linearizer used by `to_strict`/`linearize` to break
ties or order rank buckets without changing the weak-rank representation.
""" make_rank_bucket_linearizer

@doc raw"""
    compare_maybe(policy, ra, rb, i, j, ranks, pool)

Compare two possibly missing weak-rank entries under an `ExtensionPolicy`.
Returns `1`, `-1`, `0`, or `missing` according to the policy's treatment of
unranked candidates and ties.
""" compare_maybe

@doc raw"""
    ranking_signature(ranking)

Return the canonical tuple signature of a strict ranking, using candidate
symbols when a pool is available.
""" ranking_signature

@doc raw"""
    ranking_proportions(profile)

Return empirical proportions of observed strict ranking types. Weighted profiles
use stored profile weights; unweighted profiles use ballot counts.
""" ranking_proportions

@doc raw"""
    reversal_pairs(unique_rankings)

Pair observed strict rankings with their exact reversals. Each pair is reported
once; unpaired rankings are omitted.
""" reversal_pairs

@doc raw"""
    kendall_tau_distance(x, y)

Return the Kendall tau distance between two complete strict rankings: the number
of unordered candidate pairs ordered differently by `x` and `y`.
""" kendall_tau_distance

@doc raw"""
    average_normalized_distance(profile, consensus)

Return average Kendall distance from profile ballots to a consensus, normalized
by `binomial(m, 2)` and weighted by profile mass where applicable.
""" average_normalized_distance

@doc raw"""
    axes_up_to_reversal(pool_or_candidates)

Enumerate candidate axes modulo reversal for single-peakedness calculations.
""" axes_up_to_reversal

@doc raw"""
    is_single_peaked(ranking, axis)

Return whether a strict ranking is single-peaked on the supplied candidate axis.
""" is_single_peaked

@doc raw"""
    single_peaked_rankings(axis)

Enumerate strict rankings that are single-peaked on a candidate axis.
""" single_peaked_rankings

@doc raw"""
    single_peaked_distance(ranking, axis)

Return the Kendall distance from a strict ranking to the nearest ranking that is
single-peaked on `axis`.
""" single_peaked_distance

@doc raw"""
    profile_distribution(profile)

Return the observed strict ranking-type distribution for a profile, preserving
weighted profile mass where applicable.
""" profile_distribution

@doc raw"""
    single_peakedness_summary(profile; kwargs...)

Compute single-peakedness diagnostics over candidate axes, including `L0`, full
profile normalized `L1`, conditional off-axis distance, and selected support
classifications.
""" single_peakedness_summary

@doc raw"""
    single_peakedness_L0(profile; kwargs...)

Return the best-axis off-axis mass from `single_peakedness_summary`.
""" single_peakedness_L0

@doc raw"""
    single_peakedness_L1(profile; kwargs...)

Return the unconditional normalized Kendall distortion to the selected
single-peaked axis.
""" single_peakedness_L1

@doc raw"""
    single_peakedness_L1_off_axis(profile; kwargs...)

Return normalized single-peakedness distortion conditional on off-axis mass;
returns `missing` when the selected axis has no off-axis mass.
""" single_peakedness_L1_off_axis

@doc raw"""
    best_single_peaked_axes(profile; kwargs...)

Return support-optimal candidate axes from `single_peakedness_summary`.
""" best_single_peaked_axes

@doc raw"""
    can_polarization(profile)

Return Can's pairwise-balance polarization statistic `Ψ`, computed from
pairwise preference counts over all unordered candidate pairs.
""" can_polarization

@doc raw"""
    total_reversal_component(profile)

Return the exact-reversal mass component `R`, summing local masses
`2min(p_r, p_reverse(r))` over observed reversal pairs.
""" total_reversal_component

@doc raw"""
    reversal_hhi(profile)

Return reversal concentration `κ`, the HHI of positive local exact-reversal
masses. Returns `0.0` when no reversal mass is present.
""" reversal_hhi

@doc raw"""
    reversal_geometric(profile)

Return the geometric exact-reversal index `sqrt(R * κ)`, with zero-reversal mass
mapped to `0.0`.
""" reversal_geometric

@doc raw"""
    effective_observed_rankings(profile)

Return the inverse-HHI effective number of observed strict ranking types.
""" effective_observed_rankings

@doc raw"""
    effective_reversal_rankings(profile)

Return the inverse-HHI effective number of exact reversal pairs among positive
local reversal masses.
""" effective_reversal_rankings

@doc raw"""
    effective_reversal_ranking_diagnostics(profile)

Return diagnostics for effective reversal rankings, including reversal mass,
concentration, effective count, and support information.
""" effective_reversal_ranking_diagnostics

@doc raw"""
    ranking_support_diagnostics(profile; kwargs...)

Return observed ranking-support diagnostics, including support size and
effective observed rankings.
""" ranking_support_diagnostics

@doc raw"""
    consensus_kendall(profile; kwargs...)

Compute a Kendall/Kemeny consensus over complete strict rankings by minimizing
total Kendall distance over all strict orders on the active candidate set.
""" consensus_kendall

@doc raw"""
    get_consensus_ranking(profile; kwargs...)

Return a consensus ranking dictionary selected by `consensus_kendall`.
""" get_consensus_ranking

@doc raw"""
    kendall_tau_dict(r1, r2)

Dictionary-facing Kendall tau distance for complete ranking dictionaries over a
common candidate set.
""" kendall_tau_dict

@doc raw"""
    consensus_for_group(subdf; kwargs...)

Compute group-level Kendall consensus information for data accepted by
`strict_profile`, returning ranking, permutation, result object, and tie
diagnostics.
""" consensus_for_group

@doc raw"""
    group_avg_distance(subdf; kwargs...)

Compute a group's average normalized distance to its Kendall consensus and the
corresponding coherence `C_g = 1 - W_g`.
""" group_avg_distance

@doc raw"""
    weighted_coherence(results_distance, proportion_map, key)

Aggregate group coherences as `C = sum_g π_g C_g`, using proportions supplied in
`proportion_map`.
""" weighted_coherence

@doc raw"""
    pairwise_group_divergence(profile_i, consensus_j, m)

Return directed normalized Kendall divergence `D_{i -> j}` from group `i`'s
profile to group `j`'s consensus.
""" pairwise_group_divergence

@doc raw"""
    pairwise_group_overlap(profile_g, profile_h)

Return exact overlap in empirical ranking distributions between two groups,
`sum_r min(p_g(r), p_h(r))`.
""" pairwise_group_overlap

@doc raw"""
    smoothed_overlap(profile_g, profile_h)

Return radius-1 Kendall-smoothed ranking-distribution overlap between two
groups.
""" smoothed_overlap

@doc raw"""
    pairwise_group_median_distance(...)

Return normalized Kendall distance between group consensus sets, averaging over
tied minimizers when consensus results contain multiple minimizers.
""" pairwise_group_median_distance

@doc raw"""
    pairwise_group_separation(...)

Return overlap-adjusted pairwise group separation, `D_median(g,h) * (1 - O(g,h))`.
""" pairwise_group_separation

@doc raw"""
    overall_divergence(group_profiles, consensus_map)

Aggregate directed cross-group divergence with manuscript label `D` using group
population shares.
""" overall_divergence

@doc raw"""
    overall_overlap(group_profiles)

Aggregate exact pairwise group overlaps over unordered group pairs with
population-pair weights.
""" overall_overlap

@doc raw"""
    overall_overlap_smoothed(group_profiles)

Aggregate radius-1 smoothed pairwise group overlaps using the same pair weights
as `overall_overlap`.
""" overall_overlap_smoothed

@doc raw"""
    overall_divergence_median(group_profiles, consensus_map)

Aggregate pairwise median-set distances over unordered group pairs.
""" overall_divergence_median

@doc raw"""
    overall_separation(group_profiles, consensus_map)

Aggregate overlap-adjusted pairwise separations over unordered group pairs.
""" overall_separation

@doc raw"""
    grouped_gsep(C, Sep)

Return `sqrt(C * Sep)` after validating both inputs on the unit interval.
""" grouped_gsep

@doc raw"""
    S(C, D)

Return cleaned excess separation `S = D - (1 - C) / 2`. This is not rebased to
`[0, 1]` and may be negative.
""" S

@doc raw"""
    normalized_consensus_separation(W, D; atol=1e-10)

Return normalized consensus separation `E = 1 - W/D`, with `D == 0` mapped to
`0.0` and small numerical excursions clamped.
""" normalized_consensus_separation

@doc raw"""
    consensus_excess_separation(W, D; kwargs...)

Compatibility alias for `normalized_consensus_separation`.
""" consensus_excess_separation

@doc raw"""
    group_E(W, D; kwargs...)

Group-level alias for normalized consensus separation `E = 1 - W/D`.
""" group_E

@doc raw"""
    aggregate_E(W, D; kwargs...)

Aggregate-level alias for normalized consensus separation `E = 1 - W/D`.
""" aggregate_E

@doc raw"""
    E(W, D; kwargs...)

Manuscript shorthand for normalized consensus separation `E = 1 - W/D`.
""" E

@doc raw"""
    S_old(group_profiles, group_sizes)

Legacy support-separation statistic based on cross-group normalized Kendall
distance minus mean within-group dispersion for unordered group pairs.
""" S_old

@doc raw"""
    bootstrap_group_metrics(bt_profiles, demo; kwargs...)

Compute group metrics over bootstrap profile draws and return the bootstrap
summary table produced by the group-metric pipeline.
""" bootstrap_group_metrics

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
