# Examples

```@meta
CurrentModule = PreferenceProfiles
```

These examples are intentionally small. See the deeper workflow pages for the
full conceptual map.

## Construct a Profile

```@example examples
using PreferenceProfiles

pool = CandidatePool([:a, :b, :c])
p = Profile(pool, [
    StrictRank(pool, [:a, :b, :c]),
    StrictRank(pool, [:a, :c, :b]),
    StrictRank(pool, [:b, :a, :c]),
])

nballots(p)
```

Use display helpers interactively when you want a readable table:

```julia
pretty_profile_table(p)
show_profile_table_color(p)  # interactive terminal use
```

```@example examples
print(pretty_profile_table(p))
```

## Global Diagnostics

```@example examples
opposed = Profile(pool, [
    StrictRank(pool, [:a, :b, :c]),
    StrictRank(pool, [:c, :b, :a]),
])

(
    Psi = can_polarization(opposed),
    R = total_reversal_component(opposed),
    kappa = reversal_hhi(opposed),
    RHHI = reversal_geometric(opposed),
    EO = effective_observed_rankings(opposed),
    ENRP = effective_reversal_rankings(opposed),
)
```

For interpretation, see [Global Profile Diagnostics](global_measures.md).

## Single-Peakedness

Single-peakedness checks whether rankings fit a one-dimensional candidate axis.

```@example examples
axis = [:a, :b, :c]

(
    is_sp = is_single_peaked([:b, :a, :c], axis),
    sp_rankings = single_peaked_rankings(axis),
)
```

See [Single-Peakedness Diagnostics](single_peakedness.md).

## Group `C` and `D`

```@example examples
g1 = Profile(pool, [
    StrictRank(pool, [:a, :b, :c]),
    StrictRank(pool, [:a, :c, :b]),
])
g2 = Profile(pool, [
    StrictRank(pool, [:c, :b, :a]),
    StrictRank(pool, [:c, :a, :b]),
])

c1 = consensus_kendall(g1)
c2 = consensus_kendall(g2)
group_profiles = Dict(:g1 => g1, :g2 => g2)
consensus_map = Dict(:g1 => c1, :g2 => c2)

D = overall_divergence(group_profiles, consensus_map)
Cstar = 0.5 * (group_avg_distance(g1).group_coherence +
               group_avg_distance(g2).group_coherence)
C = 2Cstar - 1

(C = C, D = D)
```

`C` and `D` should be read separately. `S(C,D)` remains available as an
optional derived excess-divergence diagnostic, but it is not needed for the
basic group summary. See [Group Diagnostics](group_measures.md).

## Majority Support Teaser

Majority-graph support asks which strict ranking types support which majority
edges.

```julia
cycle = Profile(pool, [
    StrictRank(pool, [:a, :b, :c]),
    StrictRank(pool, [:b, :c, :a]),
    StrictRank(pool, [:c, :a, :b]),
])

support = majority_graph_support(cycle)

(
    n_edges = length(support.edges),
    edges = majority_edges_table(support),
    core = core_table(support),
)
```

See [Majority-Graph Support](majority_support.md).
