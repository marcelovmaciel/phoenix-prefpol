# Examples

```@meta
CurrentModule = Preferences
```

These examples use small three-candidate profiles so the objects can be inspected directly at the REPL.

## Constructing Profiles

```@example pref_examples
using Preferences

pool = CandidatePool([:a, :b, :c])
x = StrictRank(pool, [:b, :a, :c])
w = WeakRank(pool, Dict(:a => 1, :b => 1, :c => 2))

rank(x, pool, :b), prefers(x, pool, :b, :c), indifferent(w, pool, :a, :b)
```

```@example pref_examples
p = Profile(pool, [
    StrictRank(pool, [:a, :b, :c]),
    StrictRank(pool, [:a, :c, :b]),
    StrictRank(pool, [:b, :a, :c]),
])

nballots(p)
```

## Viewing Rankings and Profiles

```@example pref_examples
ordered_candidates(x, pool)
```

```@example pref_examples
pretty(x, pool)
```

```@example pref_examples
weakorder_symbol_groups(to_weakorder(w), pool)
```

For profile tables, prefer the string helper in scripts and logs:

```julia
print(pretty_profile_table(p))
```

Use `show_profile_table_color(p)` for a colorized PrettyTables view during interactive exploration.

## Pairwise Majority

A three-ranking Condorcet cycle has one majority edge in each direction around the cycle.

```@example pref_examples
cycle = Profile(pool, [
    StrictRank(pool, [:a, :b, :c]),
    StrictRank(pool, [:b, :c, :a]),
    StrictRank(pool, [:c, :a, :b]),
])

pm = pairwise_majority(cycle)
pairwise_majority_margins(pm)
```

```julia
pretty_pairwise_majority_margins(pm, pool)
show_pairwise_majority_table_color(pm; pool, kind = :margins)
```

## Kendall Consensus

```@example pref_examples
res = consensus_kendall(p)
pretty(res.consensus_ballot, pool)
```

```@example pref_examples
res.avg_normalized_distance
```

## Polarization and Reversal Measures

```@example pref_examples
unanimous = Profile(pool, [StrictRank(pool, [:a, :b, :c]) for _ in 1:3])
can_polarization(unanimous)
```

```@example pref_examples
opposed = Profile(pool, [
    StrictRank(pool, [:a, :b, :c]),
    StrictRank(pool, [:c, :b, :a]),
])

(total_reversal_component(opposed), reversal_hhi(opposed), reversal_geometric(opposed))
```

## Single-Peakedness

```@example pref_examples
axis = [:a, :b, :c]
(is_single_peaked([:a, :b, :c], axis), is_single_peaked([:b, :a, :c], axis))
```

```@example pref_examples
single_peaked_rankings(axis)
```

```@example pref_examples
sp = single_peakedness_summary(p; axes = [axis])
(sp.best_L0, sp.best_L1, sp.best_L1_off_axis)
```

## Majority-Graph Support

Majority-graph support asks which strict ranking types support which majority edges.
The table helpers return `DataFrame`s and are intended for exploratory inspection.

```julia
res = majority_graph_support(cycle)
majority_edges_table(res)
voter_type_table(res)
edge_support_table(res)
edge_overlap_table(res)
core_table(res)
```
