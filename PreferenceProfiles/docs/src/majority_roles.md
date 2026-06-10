# Majority-Graph Roles

```@meta
CurrentModule = PreferenceProfiles
```

Role tables classify voter types and groups relative to the majority graph of a
profile.

Role vocabulary:

- `anchor`: high-mass, high-coverage types that support much of the graph.
- `peripheral supporter`: lower-mass types with high edge coverage.
- `counter-graph type`: types with low coverage of majority edges.
- `edge breaker`: types positioned to break a fragile edge under the configured breaker rule.
- `fragile edge`: a majority edge with a comparatively small margin.
- `primary role`: a priority classification when multiple role flags apply.

```julia
support = majority_graph_support(profile)
roles = voter_type_role_table(support)
role_mass_summary(roles)
primary_role_mass_summary(roles)
```

Group role helpers use annotated or tabular group membership to summarize how
groups support, anchor, or threaten majority edges.

## API

```@docs
MajorityGraphRoleThresholds
voter_type_role_table
edge_type_role_table
role_mass_summary
primary_role_mass_summary
selected_edge_role_summary
graph_role_summary
group_majority_graph_support
group_edge_power_table
group_breaker_table
group_anchor_table
group_role_table
group_primary_role_table
group_role_power_table
group_graph_role_summary
```

