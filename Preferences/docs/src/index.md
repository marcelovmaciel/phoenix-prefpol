# Preferences.jl

`Preferences.jl` is the reusable formal preference and social-choice package in
the monorepo. It defines the ranking, profile, and diagnostic objects used by
applied packages such as `PrefPol.jl`; it does not own survey-wave loading,
publication cache layout, or paper artifact collection.

The package covers:

- candidate pools and pool-relative candidate identifiers,
- strict rankings, weak rankings, and weak-order linearization,
- profiles and weighted profiles,
- annotated profiles with row-aligned metadata,
- tabular score-to-profile construction,
- global profile diagnostics,
- reversal structure and effective ranking support,
- single-peakedness diagnostics,
- group coherence and divergence,
- majority-graph support and majority-graph roles,
- plurality switch tables, and
- low-level pairwise representations.

The main workflow starts with ranked profiles, often built from survey score
columns, and then studies global preference structure, effective support,
within-group coherence, and member-to-other-consensus divergence. The package
keeps these layers separate: global diagnostics such as pairwise balance and
exact reversal structure answer profile-wide questions, while group diagnostics
based on `C` and `D` use Kendall/Kemeny consensuses to describe groups.

## Where to Start

| Task | Read this page |
|---|---|
| Build rankings and profiles | [Workflow](workflow.md) |
| Convert survey scores into weak rankings | [From Scores to Profiles](tabular_profiles.md) |
| Handle ties, missing evaluations, and linearization | [Weak Orders and Linearization](weak_orders.md) |
| Keep respondent metadata aligned with ballots | [Annotated Profiles](annotated_profiles.md) |
| Compute global profile diagnostics | [Global Profile Diagnostics](global_measures.md) |
| Check one-dimensional domain structure | [Single-Peakedness Diagnostics](single_peakedness.md) |
| Compute group coherence and divergence | [Group Diagnostics](group_measures.md) |
| Analyze majority-edge support | [Majority-Graph Support](majority_support.md) |
| Classify majority-support roles | [Majority-Graph Roles](majority_roles.md) |
| Analyze plurality-switch opportunities | [Plurality Switch Tables](plurality_switch.md) |
| Use low-level pairwise or mutable representations | [Advanced Representations](advanced_representations.md) |

```@contents
Depth = 2
```
