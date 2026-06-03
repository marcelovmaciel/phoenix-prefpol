# API Reference

For a compact walkthrough of common workflows, see [Examples](examples.md).

```@meta
CurrentModule = Preferences
```

## Core Pool and Ballot API

```@docs; canonical=false
CandidatePool
StrictRank
WeakRank
rank
prefers
indifferent
asdict
to_perm
to_weakorder
ordered_candidates
weakorder_symbol_groups
to_strict
make_rank_bucket_linearizer
```

## Profiles and Weighted Profiles

```@docs; canonical=false
Profile
WeightedProfile
nballots
weights
total_weight
validate
resample_indices
bootstrap
bootstrap_counts
restrict
```

## Pairwise Majority

```@docs; canonical=false
AbstractPairwise
PairwiseDense
to_pairwise
PairwiseMajority
pairwise_majority
pairwise_majority_counts
pairwise_majority_margins
pairwise_majority_wins
```

## Consensus and Kendall Distance

```@docs; canonical=false
ConsensusResult
kendall_tau_distance
average_normalized_distance
consensus_kendall
get_consensus_ranking
kendall_tau_dict
```

## Polarization and Reversal Measures

```@docs; canonical=false
can_polarization
total_reversal_component
reversal_hhi
reversal_geometric
effective_observed_rankings
effective_reversal_rankings
effective_reversal_ranking_diagnostics
ranking_support_diagnostics
```

## Group-Level Measures

```@docs; canonical=false
consensus_for_group
group_avg_distance
weighted_coherence
pairwise_group_divergence
pairwise_group_overlap
smoothed_overlap
pairwise_group_median_distance
pairwise_group_separation
overall_divergence
overall_overlap
overall_separation
overall_divergence_median
overall_overlap_smoothed
grouped_gsep
normalized_consensus_separation
consensus_excess_separation
group_E
aggregate_E
E
S
S_old
```

## Single-Peakedness

```@docs; canonical=false
SinglePeakedAxisSummary
SinglePeakedSupportClassification
SinglePeakednessResult
axes_up_to_reversal
is_single_peaked
single_peaked_rankings
single_peaked_distance
profile_distribution
single_peakedness_summary
single_peakedness_L0
single_peakedness_L1
single_peakedness_L1_off_axis
best_single_peaked_axes
```

## Majority-Graph Support

```@docs; canonical=false
VoterTypeBasis
voter_type_basis
voter_type_masses
MajoritySupportEdge
MajorityGraphSupportResult
majority_graph_support
majority_edges_table
voter_type_table
edge_support_table
edge_overlap_table
core_table
edge_effective_type_table
edge_effective_type_composition_table
core_effective_type_table
reverse_core_effective_type_table
core_effective_type_composition_table
effective_type_diagnostics
countergraph_summary_table
boundary_distance_to_reverse
amenability_weight
type_breaker_table
minimal_breaking_coalition_table
```

## Display Helpers

```@docs; canonical=false
StrictRankView
WeakOrderView
pretty
show_pairwise_preference_table_color
pretty_pairwise
pretty_profile_table
show_profile_table_color
profile_table_string_color
pretty_pairwise_majority_table
pretty_pairwise_majority
pretty_pairwise_majority_counts
pretty_pairwise_majority_margins
show_pairwise_majority_table_color
```

## Full Public API

```@index
Modules = [Preferences]
```

```@autodocs
Modules = [Preferences]
Public = true
Private = false
```
