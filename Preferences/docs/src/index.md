# Preferences.jl

`Preferences.jl` is a formal and empirical toolkit for finite preference
profiles. It provides candidate pools, strict and weak rankings, unweighted and
weighted profiles, tabular score adapters, profile diagnostics, consensus-based
group diagnostics, and majority-graph exploration tools.

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
| Compute group coherence and divergence | [Group Diagnostics](group_measures.md) |
| Analyze majority-edge support | [Majority-Graph Support](majority_support.md) |
| Classify majority-support roles | [Majority-Graph Roles](majority_roles.md) |
| Analyze plurality-switch opportunities | [Plurality Switch Tables](plurality_switch.md) |
| Use low-level pairwise or mutable representations | [Advanced Representations](advanced_representations.md) |

```@contents
Depth = 2
```
