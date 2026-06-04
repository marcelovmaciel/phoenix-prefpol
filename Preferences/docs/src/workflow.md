# Workflow

The empirical workflow is:

1. Survey ratings or score columns.
2. Weak orders, where equal present scores are ties and missing scores are unranked.
3. Imputation or missingness handling, either before profile construction or in the caller's workflow.
4. Linearization into strict rankings when a strict-profile diagnostic requires complete rankings.
5. `Profile` or `WeightedProfile`, with weights stored as mass rather than row replication.
6. Global diagnostics for pairwise balance, exact reversal structure, and effective ranking support.
7. Group diagnostics based on Kendall/Kemeny consensuses, especially `C` and `D`.
8. Optional majority-support, majority-role, and plurality-switch diagnostics.

```julia
using DataFrames, Preferences

df = DataFrame(
    a = [10, 5, missing],
    b = [7, 5, 3],
    c = [0, missing, 8],
    weight = [1.0, 2.0, 1.5],
)

p = build_profile_from_scores(
    df,
    ["a", "b", "c"],
    [:a, :b, :c];
    weighted = true,
    weight_col = :weight,
    allow_ties = true,
    allow_incomplete = true,
)

(typeof(p), nballots(p), total_weight(p))
```

This produces a weighted weak-ranking profile. Read
[From Scores to Profiles](tabular_profiles.md) for score-column adapters and
[Weak Orders and Linearization](weak_orders.md) for the interpretive choices
needed before strict-profile diagnostics.

## Core Objects

- `CandidatePool`
- `StrictRank`
- `WeakRank`
- `Profile`
- `WeightedProfile`

