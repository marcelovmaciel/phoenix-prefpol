# Plurality Switch Tables

```@meta
CurrentModule = PreferenceProfiles
```

Plurality-switch tables are first-choice diagnostics. They are not
pairwise-majority diagnostics. They study how ranking types, one-swap moves,
and group-targeted changes affect plurality scores or first-choice margins.

Use them when the question is about first-choice support:

```julia
plurality_scores_table(profile)
pairwise_vs_plurality_decomposition_table(profile)
candidate_position_by_current_first_table(profile)
one_swap_target_table(profile, :a)
plurality_swing_value_table(profile)
exact_type_switch_table(profile, :a, :b)
group_target_switch_table(profile, groups, :a)
```

The majority relation may move differently from plurality scores, so interpret
these tables separately from [Majority-Graph Support](majority_support.md).

## API

```@docs
plurality_scores_table
pairwise_vs_plurality_decomposition_table
candidate_position_by_current_first_table
one_swap_target_table
plurality_swing_value_table
exact_type_switch_table
group_target_switch_table
```

