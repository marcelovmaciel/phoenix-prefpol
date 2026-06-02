# PreferenceMajorityGraphRoles.jl

using Statistics: quantile

"""
    MajorityGraphRoleThresholds(; high_mass_quantile=0.75, high_coverage_slack=1,
        counter_coverage_max=1, breaker_quantile=0.90, fragile_edge_quantile=0.25)

Threshold parameters for classifying voter types by their role in supporting or
breaking the majority relation.

A type is high mass when its selected mass measure is at least the
`high_mass_quantile` among positive-mass types. It is high coverage when it
supports at least `number_of_edges - high_coverage_slack` majority edges. It is
counter-graph when its support coverage is at most `counter_coverage_max`. It is
an edge breaker when its maximum breaking score is at least the
`breaker_quantile` among positive scores. Edge-level role tables mark fragile
edges whose normalized margin is at or below `fragile_edge_quantile` of observed
edge margins.
"""
Base.@kwdef struct MajorityGraphRoleThresholds
    high_mass_quantile::Float64 = 0.75
    high_coverage_slack::Int = 1
    counter_coverage_max::Int = 1
    breaker_quantile::Float64 = 0.90
    fragile_edge_quantile::Float64 = 0.25
end

function _validate_role_thresholds(thresholds::MajorityGraphRoleThresholds)
    0.0 <= thresholds.high_mass_quantile <= 1.0 ||
        throw(ArgumentError("high_mass_quantile must be in [0, 1]"))
    0.0 <= thresholds.breaker_quantile <= 1.0 ||
        throw(ArgumentError("breaker_quantile must be in [0, 1]"))
    0.0 <= thresholds.fragile_edge_quantile <= 1.0 ||
        throw(ArgumentError("fragile_edge_quantile must be in [0, 1]"))
    thresholds.high_coverage_slack >= 0 ||
        throw(ArgumentError("high_coverage_slack must be nonnegative"))
    thresholds.counter_coverage_max >= 0 ||
        throw(ArgumentError("counter_coverage_max must be nonnegative"))
    return true
end

function _role_quantile(xs, q)
    vals = Float64[]
    for x in xs
        ismissing(x) && continue
        xf = Float64(x)
        isnan(xf) && continue
        push!(vals, xf)
    end
    isempty(vals) && return missing
    return quantile(vals, q)
end

function _join_roles(anchor, peripheral, counter, edge_breaker)
    roles = String[]
    anchor && push!(roles, "anchor")
    peripheral && push!(roles, "peripheral_supporter")
    counter && push!(roles, "counter_graph")
    edge_breaker && push!(roles, "edge_breaker")
    isempty(roles) && return "mixed"
    return join(roles, ";")
end

function _primary_role(anchor, peripheral, counter, edge_breaker)
    anchor && edge_breaker && return "anchor_edge_breaker"
    anchor && return "anchor"
    edge_breaker && return "edge_breaker"
    peripheral && return "peripheral_supporter"
    counter && return "counter_graph"
    return "mixed"
end

function _selected_edge_index(result::MajorityGraphSupportResult, edge_index::Union{Nothing,Int})
    isempty(result.edges) && throw(ArgumentError("majority graph has no majority edges"))
    if edge_index === nothing
        return argmin([edge.normalized_margin for edge in result.edges])
    end
    1 <= edge_index <= length(result.edges) ||
        throw(ArgumentError("edge_index must be between 1 and $(length(result.edges))"))
    return edge_index
end

"""
    voter_type_role_table(result; thresholds=MajorityGraphRoleThresholds(),
        amenability=:inverse, lambda=1.0, mass_source=:proportion)

Classify strict voter types into reusable majority-graph roles.

Rows are voter types from `voter_type_table`. Roles are relative to the majority
graph induced by the profile, not intrinsic properties of rankings. A type is an
`anchor` when it is both high mass and high coverage; a
`peripheral_supporter` when it is not high mass but has high coverage;
`counter_graph` when coverage is below the counter threshold; and
`edge_breaker` when its best edge-breaking score exceeds the breaker quantile.
Roles are non-exclusive, and `primary_role` applies the priority encoded by
`_primary_role`.

Added columns include threshold flags (`high_mass`, `high_coverage`), role
booleans, maximum breaking score and edge, semicolon-joined `roles`, and
`primary_role`. `mass_source` chooses whether the high-mass quantile uses type
proportions or raw type mass.
"""
function voter_type_role_table(
    result::MajorityGraphSupportResult;
    thresholds::MajorityGraphRoleThresholds = MajorityGraphRoleThresholds(),
    amenability::Symbol = :inverse,
    lambda::Real = 1.0,
    mass_source::Symbol = :proportion,
)
    _validate_role_thresholds(thresholds)
    mass_for_threshold = if mass_source == :proportion
        result.type_proportion
    elseif mass_source == :mass
        result.type_mass
    else
        throw(ArgumentError("mass_source must be :proportion or :mass"))
    end

    tbl = voter_type_table(result)
    ntypes = length(result.basis.types)
    nedges = length(result.edges)

    positive_masses = [p for p in mass_for_threshold if p > 0]
    high_mass_threshold = _role_quantile(positive_masses, thresholds.high_mass_quantile)
    high_mass = ismissing(high_mass_threshold) ? fill(false, ntypes) :
        [mass_for_threshold[r] >= high_mass_threshold for r in 1:ntypes]

    high_coverage_min = nedges - thresholds.high_coverage_slack
    high_coverage = [result.coverage[r] >= high_coverage_min for r in 1:ntypes]
    anchor = high_mass .& high_coverage
    peripheral = .!high_mass .& high_coverage
    counter = [result.coverage[r] <= thresholds.counter_coverage_max for r in 1:ntypes]

    breakers = type_breaker_table(result; amenability=amenability, lambda=lambda,
                                  supporters_only=false)
    max_score = zeros(Float64, ntypes)
    max_edge_index = Vector{Union{Missing,Int}}(missing, ntypes)
    max_edge = Vector{Union{Missing,String}}(missing, ntypes)
    for row in eachrow(breakers)
        ismissing(row.breaking_score) && continue
        score = Float64(row.breaking_score)
        score > max_score[row.type_index] || continue
        max_score[row.type_index] = score
        max_edge_index[row.type_index] = row.edge_index
        max_edge[row.type_index] = string(row.winner, " -> ", row.loser)
    end

    positive_scores = [s for s in max_score if s > 0]
    breaker_threshold = _role_quantile(positive_scores, thresholds.breaker_quantile)
    edge_breaker = ismissing(breaker_threshold) ? fill(false, ntypes) :
        [max_score[r] >= breaker_threshold for r in 1:ntypes]

    tbl.high_mass = high_mass
    tbl.high_coverage = high_coverage
    tbl.anchor = anchor
    tbl.peripheral_supporter = peripheral
    tbl.counter_graph = counter
    tbl.max_breaking_score = max_score
    tbl.max_breaking_edge_index = max_edge_index
    tbl.max_breaking_edge = max_edge
    tbl.edge_breaker = edge_breaker
    tbl.roles = [_join_roles(anchor[r], peripheral[r], counter[r], edge_breaker[r])
                 for r in 1:ntypes]
    tbl.primary_role = [_primary_role(anchor[r], peripheral[r], counter[r], edge_breaker[r])
                        for r in 1:ntypes]
    sort!(tbl, :type_index)
    return tbl
end

"""
    edge_type_role_table(result; thresholds=MajorityGraphRoleThresholds(),
        amenability=:inverse, lambda=1.0)

Return one row per `(majority edge, voter type)` with edge-specific breaker roles.

Columns identify the edge and type, report normalized edge margin and
`flip_threshold = normalized_margin / 2`, type mass/proportion, support status,
boundary distance, amenability, raw and weighted breaking scores, whether the
type is an `edge_breaker_for_edge`, and whether the edge is `fragile_edge` under
the fragile-edge margin quantile.
"""
function edge_type_role_table(
    result::MajorityGraphSupportResult;
    thresholds::MajorityGraphRoleThresholds = MajorityGraphRoleThresholds(),
    amenability::Symbol = :inverse,
    lambda::Real = 1.0,
)
    _validate_role_thresholds(thresholds)
    breakers = type_breaker_table(result; amenability=amenability, lambda=lambda,
                                  supporters_only=false)
    nrows = nrow(breakers)
    nedges = length(result.edges)

    margins = [edge.normalized_margin for edge in result.edges]
    fragile_threshold = _role_quantile(margins, thresholds.fragile_edge_quantile)
    edge_thresholds = Vector{Union{Missing,Float64}}(missing, nedges)
    for eidx in 1:nedges
        scores = Float64[]
        for row in eachrow(breakers)
            row.edge_index == eidx || continue
            ismissing(row.breaking_score) && continue
            score = Float64(row.breaking_score)
            score > 0 && push!(scores, score)
        end
        edge_thresholds[eidx] = _role_quantile(scores, thresholds.breaker_quantile)
    end

    edge = Vector{String}(undef, nrows)
    margin = Vector{Float64}(undef, nrows)
    flip_threshold = Vector{Float64}(undef, nrows)
    coverage = Vector{Int}(undef, nrows)
    edge_breaker_for_edge = Vector{Bool}(undef, nrows)
    fragile_edge = Vector{Bool}(undef, nrows)
    for (i, row) in enumerate(eachrow(breakers))
        eidx = row.edge_index
        e = result.edges[eidx]
        edge[i] = _edge_label(result.pool, e)
        margin[i] = e.normalized_margin
        flip_threshold[i] = e.normalized_margin / 2
        coverage[i] = result.coverage[row.type_index]
        threshold = edge_thresholds[eidx]
        score = row.breaking_score
        edge_breaker_for_edge[i] = !ismissing(threshold) && !ismissing(score) &&
            Float64(score) >= threshold
        fragile_edge[i] = !ismissing(fragile_threshold) && e.normalized_margin <= fragile_threshold
    end

    out = DataFrame(
        edge_index = breakers.edge_index,
        edge = edge,
        winner = breakers.winner,
        loser = breakers.loser,
        margin = margin,
        normalized_margin = margin,
        flip_threshold = flip_threshold,
        type_index = breakers.type_index,
        ranking = breakers.ranking,
        type_mass = breakers.type_mass,
        type_proportion = breakers.type_proportion,
        coverage = coverage,
        supports = breakers.supports,
        boundary_distance = breakers.boundary_distance,
        amenability = breakers.amenability,
        raw_breaking_score = breakers.raw_breaking_score,
        breaking_score = breakers.breaking_score,
        edge_breaker_for_edge = edge_breaker_for_edge,
        fragile_edge = fragile_edge,
    )
    scores = [ismissing(x) ? -Inf : Float64(x) for x in out.breaking_score]
    order = sortperm(collect(1:nrow(out)); by = i -> (out.edge_index[i], -scores[i], out.type_index[i]))
    return out[order, :]
end

"""
    role_mass_summary(role_table; mass_col=:proportion)

Summarize mass by non-exclusive voter-type role.

Rows are role labels: `anchor`, `peripheral_supporter`, `edge_breaker`,
`counter_graph`, and `mixed`. Columns are `role`, `n_types`, total `mass` from
`mass_col`, and `share` of total selected mass. Role masses are not a partition:
the same type can contribute to multiple role rows. `mixed` counts only types
with no boolean role assigned.
"""
function role_mass_summary(role_table::DataFrame; mass_col::Symbol=:proportion)
    total = sum(Float64.(role_table[!, mass_col]))
    rows = [
        ("anchor", role_table.anchor),
        ("peripheral_supporter", role_table.peripheral_supporter),
        ("edge_breaker", role_table.edge_breaker),
        ("counter_graph", role_table.counter_graph),
    ]
    role = String[]
    n_types = Int[]
    mass = Float64[]
    for (name, mask) in rows
        push!(role, name)
        push!(n_types, count(mask))
        push!(mass, sum((Float64(role_table[i, mass_col]) for i in 1:nrow(role_table) if mask[i]); init=0.0))
    end
    mixed = .!(role_table.anchor .| role_table.peripheral_supporter .|
               role_table.edge_breaker .| role_table.counter_graph)
    push!(role, "mixed")
    push!(n_types, count(mixed))
    push!(mass, sum((Float64(role_table[i, mass_col]) for i in 1:nrow(role_table) if mixed[i]); init=0.0))
    share = total > 0 ? mass ./ total : zeros(Float64, length(mass))
    return DataFrame(role=role, n_types=n_types, mass=mass, share=share)
end

"""
    primary_role_mass_summary(role_table; mass_col=:proportion)

Summarize mass by mutually exclusive `primary_role`.

Rows are observed primary-role labels. Columns are `primary_role`, `n_types`,
`mass`, and `share` of total selected mass. Unlike `role_mass_summary`, this
forms a partition of type mass.
"""
function primary_role_mass_summary(role_table::DataFrame; mass_col::Symbol=:proportion)
    total = sum(Float64.(role_table[!, mass_col]))
    roles = unique(role_table.primary_role)
    primary_role = String[]
    n_types = Int[]
    mass = Float64[]
    for role in sort!(collect(roles))
        mask = role_table.primary_role .== role
        push!(primary_role, role)
        push!(n_types, count(mask))
        push!(mass, sum((Float64(role_table[i, mass_col]) for i in 1:nrow(role_table) if mask[i]); init=0.0))
    end
    share = total > 0 ? mass ./ total : zeros(Float64, length(mass))
    return DataFrame(primary_role=primary_role, n_types=n_types, mass=mass, share=share)
end

"""
    selected_edge_role_summary(result; edge_index=nothing, edge=nothing,
        thresholds=MajorityGraphRoleThresholds(), amenability=:inverse, lambda=1.0,
        top_n=10)

Return the top edge-breaking type rows for a selected majority edge.

The row schema is the same as `edge_type_role_table`, filtered to one edge and
sorted by descending `breaking_score`. If neither `edge_index` nor edge label is
provided, the weakest majority edge is selected by minimum normalized margin.
"""
function selected_edge_role_summary(
    result::MajorityGraphSupportResult;
    edge_index::Union{Nothing,Int}=nothing,
    edge::Union{Nothing,String}=nothing,
    thresholds::MajorityGraphRoleThresholds = MajorityGraphRoleThresholds(),
    amenability::Symbol = :inverse,
    lambda::Real = 1.0,
    top_n::Int = 10,
)
    top_n >= 0 || throw(ArgumentError("top_n must be nonnegative"))
    tbl = edge_type_role_table(result; thresholds=thresholds, amenability=amenability,
                               lambda=lambda)
    selected = if edge_index !== nothing
        _selected_edge_index(result, edge_index)
    elseif edge !== nothing
        matches = unique(tbl.edge_index[tbl.edge .== edge])
        isempty(matches) && throw(ArgumentError("edge label not found: $edge"))
        length(matches) == 1 || throw(ArgumentError("edge label is ambiguous: $edge"))
        matches[1]
    else
        _selected_edge_index(result, nothing)
    end
    rows = tbl[tbl.edge_index .== selected, :]
    scores = [ismissing(x) ? -Inf : Float64(x) for x in rows.breaking_score]
    order = sortperm(collect(1:nrow(rows)); by = i -> (-scores[i], rows.type_index[i]))
    return rows[order[1:min(top_n, length(order))], :]
end

"""
    group_role_table(group_result, role_table)

Aggregate non-exclusive voter-type roles by group.

Rows are groups. Columns include total `group_mass`, role masses for anchor,
peripheral supporter, counter-graph, edge-breaker, and mixed types, plus
conditional shares dividing each role mass by the group mass. Because roles are
non-exclusive, role masses need not sum to group mass.
"""
function group_role_table(
    group_result::GroupMajorityGraphSupportResult,
    role_table::DataFrame;
)
    ntypes = length(group_result.base.basis.types)
    role_by_type = Vector{Union{Nothing,DataFrameRow}}(nothing, ntypes)
    for row in eachrow(role_table)
        role_by_type[row.type_index] = row
    end

    group = group_result.groups
    group_mass = group_result.group_mass
    anchor_mass = zeros(Float64, length(group))
    peripheral_mass = zeros(Float64, length(group))
    counter_mass = zeros(Float64, length(group))
    breaker_mass = zeros(Float64, length(group))
    mixed_mass = zeros(Float64, length(group))

    for g in eachindex(group)
        for r in 1:ntypes
            row = role_by_type[r]
            row === nothing && throw(ArgumentError("role_table is missing type_index $r"))
            mass = group_result.group_type_mass[g, r]
            row.anchor && (anchor_mass[g] += mass)
            row.peripheral_supporter && (peripheral_mass[g] += mass)
            row.counter_graph && (counter_mass[g] += mass)
            row.edge_breaker && (breaker_mass[g] += mass)
            !(row.anchor || row.peripheral_supporter || row.counter_graph || row.edge_breaker) &&
                (mixed_mass[g] += mass)
        end
    end

    divshare(v, gm) = gm > 0 ? v / gm : 0.0
    return DataFrame(
        group = group,
        group_mass = group_mass,
        anchor_mass = anchor_mass,
        peripheral_supporter_mass = peripheral_mass,
        counter_graph_mass = counter_mass,
        edge_breaker_mass = breaker_mass,
        mixed_mass = mixed_mass,
        conditional_anchor_share = [divshare(anchor_mass[g], group_mass[g]) for g in eachindex(group)],
        conditional_peripheral_supporter_share = [divshare(peripheral_mass[g], group_mass[g]) for g in eachindex(group)],
        conditional_counter_graph_share = [divshare(counter_mass[g], group_mass[g]) for g in eachindex(group)],
        conditional_edge_breaker_share = [divshare(breaker_mass[g], group_mass[g]) for g in eachindex(group)],
        conditional_mixed_share = [divshare(mixed_mass[g], group_mass[g]) for g in eachindex(group)],
    )
end

"""
    group_primary_role_table(group_result, role_table)

Return a long table of group mass by mutually exclusive `primary_role`.

Rows are `(group, primary_role)` cells. Columns are group mass, role mass, and
`conditional_role_share = role_mass / group_mass`. Primary roles partition each
group's type mass.
"""
function group_primary_role_table(
    group_result::GroupMajorityGraphSupportResult,
    role_table::DataFrame;
)
    ntypes = length(group_result.base.basis.types)
    primary_by_type = Vector{String}(undef, ntypes)
    for row in eachrow(role_table)
        primary_by_type[row.type_index] = row.primary_role
    end
    primary_roles = sort!(unique(primary_by_type))

    group = Symbol[]
    group_mass = Float64[]
    primary_role = String[]
    role_mass = Float64[]
    conditional_role_share = Float64[]
    for g in eachindex(group_result.groups)
        gm = group_result.group_mass[g]
        for role in primary_roles
            rm = 0.0
            for r in 1:ntypes
                primary_by_type[r] == role || continue
                rm += group_result.group_type_mass[g, r]
            end
            push!(group, group_result.groups[g])
            push!(group_mass, gm)
            push!(primary_role, role)
            push!(role_mass, rm)
            push!(conditional_role_share, gm > 0 ? rm / gm : 0.0)
        end
    end
    return DataFrame(group=group, group_mass=group_mass, primary_role=primary_role,
                     role_mass=role_mass,
                     conditional_role_share=conditional_role_share)
end

"""
    group_role_power_table(group_result; edge_index=nothing, amenability=:inverse, lambda=1.0)

Decompose group role power for one majority edge.

Rows are groups. Columns combine graph anchoring, conditional anchoring, selected
edge identity, edge normalized margin, group margin contribution, group support,
group share within the edge-supporting coalition, and group edge-breaking score.
If `edge_index` is omitted, the weakest majority edge is selected.
"""
function group_role_power_table(
    group_result::GroupMajorityGraphSupportResult;
    edge_index::Union{Nothing,Int}=nothing,
    amenability::Symbol=:inverse,
    lambda::Real=1.0,
)
    base = group_result.base
    selected = _selected_edge_index(base, edge_index)
    edge = base.edges[selected]
    anchors = group_anchor_table(group_result)
    powers = group_edge_power_table(group_result)
    breakers = group_breaker_table(group_result; amenability=amenability, lambda=lambda)

    group = Symbol[]
    group_mass = Float64[]
    anchoring = Float64[]
    conditional_anchoring = Float64[]
    edge_index_col = Int[]
    edge_col = String[]
    winner = Symbol[]
    loser = Symbol[]
    edge_normalized_margin = Float64[]
    edge_margin_contribution = Float64[]
    edge_support = Float64[]
    edge_support_share = Float64[]
    edge_breaking_score = Float64[]

    for g in eachindex(group_result.groups)
        group_symbol = group_result.groups[g]
        anchor_row = anchors[anchors.group .== group_symbol, :][1, :]
        power_row = powers[(powers.group .== group_symbol) .& (powers.edge_index .== selected), :][1, :]
        breaker_row = breakers[(breakers.group .== group_symbol) .& (breakers.edge_index .== selected), :][1, :]
        push!(group, group_symbol)
        push!(group_mass, group_result.group_mass[g])
        push!(anchoring, anchor_row.anchoring)
        push!(conditional_anchoring, anchor_row.conditional_anchoring)
        push!(edge_index_col, selected)
        push!(edge_col, _edge_label(base.pool, edge))
        push!(winner, base.pool[edge.winner])
        push!(loser, base.pool[edge.loser])
        push!(edge_normalized_margin, edge.normalized_margin)
        push!(edge_margin_contribution, power_row.group_margin_contribution)
        push!(edge_support, power_row.group_support)
        push!(edge_support_share, power_row.group_support_share_within_edge)
        push!(edge_breaking_score, breaker_row.breaking_score)
    end

    return DataFrame(
        group = group,
        group_mass = group_mass,
        anchoring = anchoring,
        conditional_anchoring = conditional_anchoring,
        edge_index = edge_index_col,
        edge = edge_col,
        winner = winner,
        loser = loser,
        edge_normalized_margin = edge_normalized_margin,
        edge_margin_contribution = edge_margin_contribution,
        edge_support = edge_support,
        edge_support_share = edge_support_share,
        edge_breaking_score = edge_breaking_score,
    )
end

"""
    graph_role_summary(result; thresholds=MajorityGraphRoleThresholds(),
        amenability=:inverse, lambda=1.0)

Return a named tuple of voter-type role tables and summaries.

Fields are `role_table`, `role_mass_summary`, `primary_role_mass_summary`,
`edge_role_table`, and `weakest_edge_breakers`. These tables classify who
supports the majority relation, who is peripheral or counter-graph, and which
supporting types are closest to breaking majority edges.
"""
function graph_role_summary(
    result::MajorityGraphSupportResult;
    thresholds::MajorityGraphRoleThresholds = MajorityGraphRoleThresholds(),
    amenability::Symbol=:inverse,
    lambda::Real=1.0,
)
    role_table = voter_type_role_table(result; thresholds=thresholds,
                                       amenability=amenability, lambda=lambda)
    return (
        role_table = role_table,
        role_mass_summary = role_mass_summary(role_table),
        primary_role_mass_summary = primary_role_mass_summary(role_table),
        edge_role_table = edge_type_role_table(result; thresholds=thresholds,
                                               amenability=amenability, lambda=lambda),
        weakest_edge_breakers = selected_edge_role_summary(result; thresholds=thresholds,
                                                           amenability=amenability, lambda=lambda),
    )
end

"""
    group_graph_role_summary(group_result, role_table; edge_index=nothing,
        amenability=:inverse, lambda=1.0)

Return a named tuple of group-level majority-graph role summaries.

Fields are `group_role_table`, `group_primary_role_table`, and
`group_role_power_table`. Group masses are those stored in
`GroupMajorityGraphSupportResult`, normalized by total profile mass.
"""
function group_graph_role_summary(
    group_result::GroupMajorityGraphSupportResult,
    role_table::DataFrame;
    edge_index::Union{Nothing,Int}=nothing,
    amenability::Symbol=:inverse,
    lambda::Real=1.0,
)
    return (
        group_role_table = group_role_table(group_result, role_table),
        group_primary_role_table = group_primary_role_table(group_result, role_table),
        group_role_power_table = group_role_power_table(group_result; edge_index=edge_index,
                                                        amenability=amenability,
                                                        lambda=lambda),
    )
end
