# Majority-Graph Support

```@meta
CurrentModule = Preferences
```

Majority-graph support asks: which strict ranking types support which majority
edges? A type supports an edge `a -> b` when rankings of that type place `a`
above `b`.

```julia
using Preferences

pool = CandidatePool([:a, :b, :c])
cycle = Profile(pool, [
    StrictRank(pool, [:a, :b, :c]),
    StrictRank(pool, [:b, :c, :a]),
    StrictRank(pool, [:c, :a, :b]),
])

result = majority_graph_support(cycle)
length(result.edges)
```

The table helpers return `DataFrame`s for inspection:

```julia
majority_edges_table(result)
voter_type_table(result)
edge_support_table(result)
edge_overlap_table(result)
core_table(result)
```

## API

```@docs
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

