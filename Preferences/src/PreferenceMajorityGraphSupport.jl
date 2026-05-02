# PreferenceMajorityGraphSupport.jl

"""
Canonical basis of strict voter types over a candidate pool.

The row index in `types`, `perms`, and `signatures` is stable and should be
treated as part of the mathematical object.
"""
struct VoterTypeBasis
    pool::CandidatePool
    types::Vector{StrictRank}
    perms::Vector{Vector{Int}}
    signatures::Vector{Tuple}
    index_by_perm::Dict{Tuple{Vararg{Int}},Int}
    index_by_signature::Dict{Tuple,Int}
    order_mode::Symbol
    reference_perm::Union{Nothing,Vector{Int}}
end

struct MajoritySupportEdge
    winner::Int
    loser::Int
    support_mass::Float64
    opposition_mass::Float64
    margin_mass::Float64
    normalized_margin::Float64
end

struct MajorityGraphSupportResult
    pool::CandidatePool
    basis::VoterTypeBasis
    type_mass::Vector{Float64}
    type_proportion::Vector{Float64}
    total_mass::Float64
    margins::Matrix{Float64}
    normalized_margins::Matrix{Float64}
    edges::Vector{MajoritySupportEdge}
    support_matrix::BitMatrix
    weighted_support::Matrix{Float64}
    coverage::Vector{Int}
    anchoring::Vector{Float64}
    normalized_anchoring::Vector{Float64}
    support_share::Matrix{Float64}
    overlap::Matrix{Float64}
    conditional_overlap::Matrix{Float64}
    jaccard_overlap::Matrix{Float64}
    core_mass_by_k::Dict{Int,Float64}
end

struct GroupMajorityGraphSupportResult
    base::MajorityGraphSupportResult
    groups::Vector{Symbol}
    group_mass::Vector{Float64}
    group_type_mass::Matrix{Float64}
    group_type_proportion::Matrix{Float64}
    group_edge_margin::Matrix{Float64}
    group_edge_support::Matrix{Float64}
    group_anchoring::Vector{Float64}
    group_conditional_anchoring::Vector{Float64}
end

@inline _pool_names_equal(a::CandidatePool, b::CandidatePool) =
    length(a) == length(b) && all(a[i] == b[i] for i in 1:length(a))

function _all_permutations(n::Int)
    return [collect(p) for p in permutations(collect(1:n))]
end

function _kendall_distance_perm(p::AbstractVector{<:Integer}, q::AbstractVector{<:Integer})
    length(p) == length(q) || throw(ArgumentError("Permutations must have the same length"))
    n = length(p)
    pos = zeros(Int, n)
    @inbounds for (rank, id) in enumerate(q)
        pos[Int(id)] = rank
    end

    d = 0
    @inbounds for i in 1:(n - 1)
        pi = pos[Int(p[i])]
        for j in (i + 1):n
            pi > pos[Int(p[j])] && (d += 1)
        end
    end
    return d
end

function _reference_perm(pool::CandidatePool, reference_order)
    if reference_order === nothing
        return collect(1:length(pool))
    elseif reference_order isa StrictRank
        perm = to_perm(reference_order)
    elseif reference_order isa AbstractVector{<:Integer}
        perm = Int[x for x in reference_order]
    elseif reference_order isa AbstractVector{Symbol}
        perm = Int[pool[x] for x in reference_order]
    else
        throw(ArgumentError("reference_order must be nothing, StrictRank, Vector{Int}, or Vector{Symbol}"))
    end
    StrictRank(pool, perm)
    return perm
end

function _build_voter_type_basis(pool::CandidatePool, perms::Vector{Vector{Int}}, order_mode::Symbol,
                                 reference_perm::Union{Nothing,Vector{Int}})
    types = StrictRank[StrictRank(pool, p) for p in perms]
    signatures = Tuple[Tuple(ordered_candidates(t, pool)) for t in types]
    index_by_perm = Dict{Tuple{Vararg{Int}},Int}()
    index_by_signature = Dict{Tuple,Int}()

    for (idx, p) in enumerate(perms)
        pkey = Tuple(p)
        skey = signatures[idx]
        haskey(index_by_perm, pkey) && throw(ArgumentError("Duplicate voter type permutation: $pkey"))
        haskey(index_by_signature, skey) && throw(ArgumentError("Duplicate voter type signature: $skey"))
        index_by_perm[pkey] = idx
        index_by_signature[skey] = idx
    end

    return VoterTypeBasis(pool, types, perms, signatures, index_by_perm,
                          index_by_signature, order_mode, reference_perm)
end

"""
    voter_type_basis(pool; order=:lex, reference_order=nothing)

Construct a deterministic full basis of strict rankings over `pool`.
"""
function voter_type_basis(pool::CandidatePool; order::Symbol=:lex, reference_order=nothing)
    n = length(pool)
    perms = _all_permutations(n)
    reference_perm = nothing

    if order == :lex
        sort!(perms)
    elseif order == :kendall_shell
        reference_perm = _reference_perm(pool, reference_order)
        sort!(perms; by = p -> (_kendall_distance_perm(p, reference_perm), Tuple(p)))
    elseif order == :given
        reference_order === nothing && throw(ArgumentError("order=:given requires reference_order as explicit permutations"))
        perms = [Int[x for x in p] for p in reference_order]
        for p in perms
            StrictRank(pool, p)
        end
    else
        throw(ArgumentError("Unsupported voter type basis order: $order"))
    end

    return _build_voter_type_basis(pool, perms, order, reference_perm)
end

function _validate_basis_pool(profile, basis::VoterTypeBasis)
    _pool_names_equal(profile.pool, basis.pool) || throw(ArgumentError("Profile pool must match basis pool"))
    return true
end

function _positive_total_mass(total::Real)
    total > 0 || throw(ArgumentError("Profile must contain positive total mass"))
end

"""
    voter_type_masses(profile, basis)

Return `(counts_or_mass, proportions, total_mass)` over every basis voter type.
"""
function voter_type_masses(p::Union{Profile{<:StrictRank},WeightedProfile{<:StrictRank}},
                           basis::VoterTypeBasis)
    _validate_basis_pool(p, basis)
    validate(p)

    masses = zeros(Float64, length(basis.types))
    if p isa Profile
        @inbounds for ballot in p.ballots
            idx = basis.index_by_perm[Tuple(to_perm(ballot))]
            masses[idx] += 1.0
        end
        total = Float64(nballots(p))
    else
        @inbounds for (ballot, weight) in zip(p.ballots, p.weights)
            idx = basis.index_by_perm[Tuple(to_perm(ballot))]
            masses[idx] += Float64(weight)
        end
        total = Float64(total_weight(p))
    end
    _positive_total_mass(total)
    return (counts_or_mass = masses, proportions = masses ./ total, total_mass = total)
end

function _rank_positions(perm::AbstractVector{Int})
    pos = zeros(Int, length(perm))
    @inbounds for (rank, id) in enumerate(perm)
        pos[id] = rank
    end
    return pos
end

@inline _supports(pos::AbstractVector{Int}, winner::Int, loser::Int) = pos[winner] < pos[loser]
@inline _edge_label(pool::CandidatePool, edge::MajoritySupportEdge) =
    string(pool[edge.winner], " -> ", pool[edge.loser])

"""
    majority_graph_support(profile; basis=nothing, basis_order=:lex, reference_order=nothing, tie_policy=:omit)

Compute the full voter-type support structure behind majority edges.
"""
function majority_graph_support(
    p::Union{Profile{<:StrictRank},WeightedProfile{<:StrictRank}};
    basis::Union{Nothing,VoterTypeBasis}=nothing,
    basis_order::Symbol=:lex,
    reference_order=nothing,
    tie_policy::Symbol=:omit,
)
    tie_policy in (:omit, :error) || throw(ArgumentError("tie_policy must be :omit or :error"))
    basis = basis === nothing ? voter_type_basis(p.pool; order=basis_order, reference_order=reference_order) : basis
    _validate_basis_pool(p, basis)
    masses = voter_type_masses(p, basis)

    ntypes = length(basis.types)
    n = length(p.pool)
    positions = [_rank_positions(basis.perms[r]) for r in 1:ntypes]

    support = zeros(Float64, n, n)
    @inbounds for r in 1:ntypes
        mass = masses.counts_or_mass[r]
        mass == 0.0 && continue
        perm = basis.perms[r]
        for pos_i in 1:(n - 1)
            i = perm[pos_i]
            for pos_j in (pos_i + 1):n
                j = perm[pos_j]
                support[i, j] += mass
            end
        end
    end

    margins = zeros(Float64, n, n)
    normalized_margins = zeros(Float64, n, n)
    edges = MajoritySupportEdge[]
    @inbounds for i in 1:(n - 1)
        for j in (i + 1):n
            margin = support[i, j] - support[j, i]
            margins[i, j] = margin
            margins[j, i] = -margin
            normalized_margins[i, j] = margin / masses.total_mass
            normalized_margins[j, i] = -margin / masses.total_mass

            if margin > 0
                push!(edges, MajoritySupportEdge(i, j, support[i, j], support[j, i],
                                                 margin, margin / masses.total_mass))
            elseif margin < 0
                push!(edges, MajoritySupportEdge(j, i, support[j, i], support[i, j],
                                                 -margin, -margin / masses.total_mass))
            elseif tie_policy == :error
                throw(ArgumentError("Pairwise tie for $(p.pool[i]) and $(p.pool[j])"))
            end
        end
    end

    nedges = length(edges)
    H = falses(ntypes, nedges)
    @inbounds for eidx in 1:nedges
        edge = edges[eidx]
        for r in 1:ntypes
            H[r, eidx] = _supports(positions[r], edge.winner, edge.loser)
        end
    end

    weighted_support = zeros(Float64, ntypes, nedges)
    support_share = fill(NaN, ntypes, nedges)
    coverage = zeros(Int, ntypes)
    @inbounds for r in 1:ntypes
        for eidx in 1:nedges
            if H[r, eidx]
                weighted_support[r, eidx] = masses.proportions[r]
                coverage[r] += 1
            end
        end
    end

    @inbounds for eidx in 1:nedges
        denom = edges[eidx].support_mass / masses.total_mass
        if denom > 0
            for r in 1:ntypes
                support_share[r, eidx] = weighted_support[r, eidx] / denom
            end
        end
    end

    anchoring = masses.proportions .* coverage
    normalized_anchoring = nedges == 0 ? zeros(Float64, ntypes) : anchoring ./ nedges

    overlap = zeros(Float64, nedges, nedges)
    conditional_overlap = fill(NaN, nedges, nedges)
    jaccard_overlap = fill(NaN, nedges, nedges)
    edge_support_prop = [edge.support_mass / masses.total_mass for edge in edges]

    @inbounds for e in 1:nedges
        for f in 1:nedges
            both = 0.0
            union_mass = 0.0
            for r in 1:ntypes
                he = H[r, e]
                hf = H[r, f]
                if he && hf
                    both += masses.proportions[r]
                end
                if he || hf
                    union_mass += masses.proportions[r]
                end
            end
            overlap[e, f] = both
            edge_support_prop[f] > 0 && (conditional_overlap[e, f] = both / edge_support_prop[f])
            union_mass > 0 && (jaccard_overlap[e, f] = both / union_mass)
        end
    end

    core_mass_by_k = Dict{Int,Float64}()
    for k in 0:nedges
        core_mass_by_k[k] = sum(masses.proportions[r] for r in 1:ntypes if coverage[r] >= k)
    end

    return MajorityGraphSupportResult(p.pool, basis, masses.counts_or_mass, masses.proportions,
                                      masses.total_mass, margins, normalized_margins, edges, H,
                                      weighted_support, coverage, anchoring, normalized_anchoring,
                                      support_share, overlap, conditional_overlap, jaccard_overlap,
                                      core_mass_by_k)
end

@inline _integer_flip_count(margin_mass::Float64) =
    isapprox(margin_mass, round(margin_mass); atol = sqrt(eps(Float64))) ? floor(Int, margin_mass / 2) + 1 : missing

function majority_edges_table(result::MajorityGraphSupportResult)
    return DataFrame(
        edge_index = collect(1:length(result.edges)),
        winner_id = [e.winner for e in result.edges],
        loser_id = [e.loser for e in result.edges],
        winner = [result.pool[e.winner] for e in result.edges],
        loser = [result.pool[e.loser] for e in result.edges],
        support_mass = [e.support_mass for e in result.edges],
        opposition_mass = [e.opposition_mass for e in result.edges],
        margin_mass = [e.margin_mass for e in result.edges],
        normalized_margin = [e.normalized_margin for e in result.edges],
        flip_threshold_mass = [e.margin_mass / 2 for e in result.edges],
        flip_threshold_proportion = [e.normalized_margin / 2 for e in result.edges],
        integer_flip_count = [_integer_flip_count(e.margin_mass) for e in result.edges],
    )
end

function _ranking_label(pool::CandidatePool, perm::AbstractVector{Int})
    return join((String(pool[id]) for id in perm), " > ")
end

function voter_type_table(result::MajorityGraphSupportResult)
    shell = result.basis.reference_perm === nothing ?
        fill(missing, length(result.basis.perms)) :
        [_kendall_distance_perm(p, result.basis.reference_perm) for p in result.basis.perms]

    return DataFrame(
        type_index = collect(1:length(result.basis.types)),
        ranking = [_ranking_label(result.pool, p) for p in result.basis.perms],
        perm = copy(result.basis.perms),
        shell = shell,
        mass = result.type_mass,
        proportion = result.type_proportion,
        coverage = result.coverage,
        anchoring = result.anchoring,
        normalized_anchoring = result.normalized_anchoring,
    )
end

function edge_support_table(result::MajorityGraphSupportResult)
    type_index = Int[]
    ranking = String[]
    edge_index = Int[]
    winner = Symbol[]
    loser = Symbol[]
    supports = Bool[]
    type_proportion = Float64[]
    weighted_support = Float64[]
    support_share = Float64[]

    for eidx in eachindex(result.edges)
        edge = result.edges[eidx]
        for ridx in eachindex(result.basis.types)
            push!(type_index, ridx)
            push!(ranking, _ranking_label(result.pool, result.basis.perms[ridx]))
            push!(edge_index, eidx)
            push!(winner, result.pool[edge.winner])
            push!(loser, result.pool[edge.loser])
            push!(supports, result.support_matrix[ridx, eidx])
            push!(type_proportion, result.type_proportion[ridx])
            push!(weighted_support, result.weighted_support[ridx, eidx])
            push!(support_share, result.support_share[ridx, eidx])
        end
    end

    return DataFrame(type_index = type_index, ranking = ranking, edge_index = edge_index,
                     winner = winner, loser = loser, supports = supports,
                     type_proportion = type_proportion, weighted_support = weighted_support,
                     support_share = support_share)
end

function edge_overlap_table(result::MajorityGraphSupportResult)
    edge_i = Int[]
    edge_j = Int[]
    edge_i_label = String[]
    edge_j_label = String[]
    overlap = Float64[]
    conditional_i_given_j = Float64[]
    conditional_j_given_i = Float64[]
    jaccard = Float64[]

    for i in eachindex(result.edges)
        for j in eachindex(result.edges)
            push!(edge_i, i)
            push!(edge_j, j)
            push!(edge_i_label, _edge_label(result.pool, result.edges[i]))
            push!(edge_j_label, _edge_label(result.pool, result.edges[j]))
            push!(overlap, result.overlap[i, j])
            push!(conditional_i_given_j, result.conditional_overlap[i, j])
            push!(conditional_j_given_i, result.conditional_overlap[j, i])
            push!(jaccard, result.jaccard_overlap[i, j])
        end
    end

    return DataFrame(edge_i = edge_i, edge_j = edge_j, edge_i_label = edge_i_label,
                     edge_j_label = edge_j_label, overlap = overlap,
                     conditional_i_given_j = conditional_i_given_j,
                     conditional_j_given_i = conditional_j_given_i,
                     jaccard = jaccard)
end

function core_table(result::MajorityGraphSupportResult)
    ks = sort!(collect(keys(result.core_mass_by_k)))
    return DataFrame(k = ks, core_mass = [result.core_mass_by_k[k] for k in ks])
end

"""
    boundary_distance_to_reverse(basis, type_index, winner, loser)

Adjacent-swap distance from a supporting type to one that reverses the edge.
Returns `missing` for non-supporting types.
"""
function boundary_distance_to_reverse(basis::VoterTypeBasis, type_index::Int, winner::Int, loser::Int)
    1 <= type_index <= length(basis.perms) || throw(BoundsError(basis.perms, type_index))
    perm = basis.perms[type_index]
    pos_w = findfirst(==(winner), perm)
    pos_l = findfirst(==(loser), perm)
    (pos_w === nothing || pos_l === nothing) && throw(ArgumentError("winner and loser must be in the basis pool"))
    return pos_w < pos_l ? pos_l - pos_w : missing
end

function amenability_weight(delta; mode::Symbol=:inverse, lambda::Real=1.0)
    ismissing(delta) && return missing
    mode == :inverse && return 1.0 / Float64(delta)
    mode == :exponential && return exp(-Float64(lambda) * Float64(delta))
    mode == :adjacent && return delta == 1 ? 1.0 : 0.0
    mode == :none && return 1.0
    throw(ArgumentError("Unsupported amenability mode: $mode"))
end

function type_breaker_table(
    result::MajorityGraphSupportResult;
    amenability::Symbol=:inverse,
    lambda::Real=1.0,
    supporters_only::Bool=true,
)
    edge_index = Int[]
    winner = Symbol[]
    loser = Symbol[]
    type_index = Int[]
    ranking = String[]
    type_mass = Float64[]
    type_proportion = Float64[]
    supports = Bool[]
    boundary_distance = Union{Missing,Int}[]
    amenability_col = Union{Missing,Float64}[]
    raw_breaking_score = Union{Missing,Float64}[]
    breaking_score = Union{Missing,Float64}[]

    for eidx in eachindex(result.edges)
        edge = result.edges[eidx]
        for ridx in eachindex(result.basis.types)
            supp = result.support_matrix[ridx, eidx]
            supporters_only && !supp && continue
            delta = supp ? boundary_distance_to_reverse(result.basis, ridx, edge.winner, edge.loser) : missing
            aw = supp ? amenability_weight(delta; mode=amenability, lambda=lambda) : missing
            raw = supp ? 2.0 * result.type_proportion[ridx] / edge.normalized_margin : missing
            score = (supp && !ismissing(aw)) ? raw * aw : missing
            push!(edge_index, eidx)
            push!(winner, result.pool[edge.winner])
            push!(loser, result.pool[edge.loser])
            push!(type_index, ridx)
            push!(ranking, _ranking_label(result.pool, result.basis.perms[ridx]))
            push!(type_mass, result.type_mass[ridx])
            push!(type_proportion, result.type_proportion[ridx])
            push!(supports, supp)
            push!(boundary_distance, delta)
            push!(amenability_col, aw)
            push!(raw_breaking_score, raw)
            push!(breaking_score, score)
        end
    end

    return DataFrame(edge_index = edge_index, winner = winner, loser = loser,
                     type_index = type_index, ranking = ranking,
                     type_mass = type_mass,
                     type_proportion = type_proportion, supports = supports,
                     boundary_distance = boundary_distance,
                     amenability = amenability_col,
                     raw_breaking_score = raw_breaking_score,
                     breaking_score = breaking_score)
end

function minimal_breaking_coalition_table(
    result::MajorityGraphSupportResult;
    by::Symbol=:mass,
    amenability::Symbol=:inverse,
    lambda::Real=1.0,
)
    by in (:mass, :breaking_score, :amenability) || throw(ArgumentError("by must be :mass, :breaking_score, or :amenability"))

    edge_index = Int[]
    winner = Symbol[]
    loser = Symbol[]
    threshold = Float64[]
    rank_in_coalition = Int[]
    type_index = Int[]
    ranking = String[]
    type_mass = Float64[]
    type_proportion = Float64[]
    boundary_distance = Int[]
    amenability_col = Float64[]
    cumulative_mass = Float64[]
    flips_edge = Bool[]

    for eidx in eachindex(result.edges)
        edge = result.edges[eidx]
        rows = Tuple{Int,Float64,Float64,Float64,Int}[]
        for ridx in eachindex(result.basis.types)
            result.support_matrix[ridx, eidx] || continue
            delta = boundary_distance_to_reverse(result.basis, ridx, edge.winner, edge.loser)
            aw = amenability_weight(delta; mode=amenability, lambda=lambda)
            raw = 2.0 * result.type_proportion[ridx] / edge.normalized_margin
            push!(rows, (ridx, result.type_proportion[ridx], raw * aw, aw, delta))
        end

        if by == :mass
            sort!(rows; by = x -> (-x[2], x[1]))
        elseif by == :breaking_score
            sort!(rows; by = x -> (-x[3], x[1]))
        else
            sort!(rows; by = x -> (-x[4], -x[2], x[1]))
        end

        thresh = edge.normalized_margin / 2
        accum = 0.0
        for (rank_idx, row) in enumerate(rows)
            ridx, prop, _, aw, delta = row
            accum += prop
            push!(edge_index, eidx)
            push!(winner, result.pool[edge.winner])
            push!(loser, result.pool[edge.loser])
            push!(threshold, thresh)
            push!(rank_in_coalition, rank_idx)
            push!(type_index, ridx)
            push!(ranking, _ranking_label(result.pool, result.basis.perms[ridx]))
            push!(type_mass, result.type_mass[ridx])
            push!(type_proportion, prop)
            push!(boundary_distance, delta)
            push!(amenability_col, aw)
            push!(cumulative_mass, accum)
            push!(flips_edge, accum > thresh)
            accum > thresh && break
        end
    end

    return DataFrame(edge_index = edge_index, winner = winner, loser = loser,
                     threshold = threshold, rank_in_coalition = rank_in_coalition,
                     type_index = type_index, ranking = ranking,
                     type_mass = type_mass, type_proportion = type_proportion,
                     boundary_distance = boundary_distance,
                     amenability = amenability_col,
                     cumulative_mass = cumulative_mass,
                     flips_edge = flips_edge)
end

function _group_symbols(group_labels::AbstractVector)
    groups = Symbol[]
    group_index = Dict{Symbol,Int}()
    labels = Vector{Symbol}(undef, length(group_labels))
    for (i, label) in enumerate(group_labels)
        sym = ismissing(label) ? :NA : Symbol(string(label))
        labels[i] = sym
        if !haskey(group_index, sym)
            group_index[sym] = length(groups) + 1
            push!(groups, sym)
        end
    end
    return labels, groups, group_index
end

function group_majority_graph_support(
    p::Union{Profile{<:StrictRank},WeightedProfile{<:StrictRank}},
    group_labels::AbstractVector;
    basis::Union{Nothing,VoterTypeBasis}=nothing,
    basis_order::Symbol=:lex,
    reference_order=nothing,
    tie_policy::Symbol=:omit,
)
    length(group_labels) == nballots(p) || throw(ArgumentError("group_labels length must equal number of ballots"))
    base = majority_graph_support(p; basis=basis, basis_order=basis_order,
                                  reference_order=reference_order, tie_policy=tie_policy)
    labels, groups, group_index = _group_symbols(group_labels)
    ngroups = length(groups)
    ntypes = length(base.basis.types)
    nedges = length(base.edges)

    raw_group_type_mass = zeros(Float64, ngroups, ntypes)
    @inbounds for i in 1:nballots(p)
        gidx = group_index[labels[i]]
        tidx = base.basis.index_by_perm[Tuple(to_perm(p.ballots[i]))]
        weight = p isa Profile ? 1.0 : Float64(p.weights[i])
        raw_group_type_mass[gidx, tidx] += weight
    end

    group_mass_raw = vec(sum(raw_group_type_mass; dims=2))
    group_mass = group_mass_raw ./ base.total_mass
    group_type_mass = raw_group_type_mass ./ base.total_mass
    group_type_proportion = zeros(Float64, ngroups, ntypes)
    @inbounds for g in 1:ngroups
        if group_mass_raw[g] > 0
            for r in 1:ntypes
                group_type_proportion[g, r] = raw_group_type_mass[g, r] / group_mass_raw[g]
            end
        end
    end

    group_edge_margin = zeros(Float64, ngroups, nedges)
    group_edge_support = zeros(Float64, ngroups, nedges)
    @inbounds for g in 1:ngroups
        for eidx in 1:nedges
            edge = base.edges[eidx]
            for r in 1:ntypes
                mass = group_type_mass[g, r]
                mass == 0.0 && continue
                if base.support_matrix[r, eidx]
                    group_edge_support[g, eidx] += mass
                    group_edge_margin[g, eidx] += mass
                else
                    group_edge_margin[g, eidx] -= mass
                end
            end
        end
    end

    group_anchoring = zeros(Float64, ngroups)
    @inbounds for g in 1:ngroups
        for r in 1:ntypes
            group_anchoring[g] += group_type_mass[g, r] * base.coverage[r]
        end
    end
    group_conditional_anchoring = [group_mass[g] > 0 ? group_anchoring[g] / group_mass[g] : 0.0
                                   for g in 1:ngroups]

    return GroupMajorityGraphSupportResult(base, groups, group_mass, group_type_mass,
                                           group_type_proportion, group_edge_margin,
                                           group_edge_support, group_anchoring,
                                           group_conditional_anchoring)
end

function group_edge_power_table(group_result::GroupMajorityGraphSupportResult)
    group = Symbol[]
    group_mass = Float64[]
    edge_index = Int[]
    winner = Symbol[]
    loser = Symbol[]
    group_support = Float64[]
    group_margin_contribution = Float64[]
    group_support_share_within_edge = Float64[]
    raw_group_breaking_score = Float64[]

    base = group_result.base
    for g in eachindex(group_result.groups)
        for eidx in eachindex(base.edges)
            edge = base.edges[eidx]
            push!(group, group_result.groups[g])
            push!(group_mass, group_result.group_mass[g])
            push!(edge_index, eidx)
            push!(winner, base.pool[edge.winner])
            push!(loser, base.pool[edge.loser])
            push!(group_support, group_result.group_edge_support[g, eidx])
            push!(group_margin_contribution, group_result.group_edge_margin[g, eidx])
            edge_support = edge.support_mass / base.total_mass
            push!(group_support_share_within_edge, edge_support > 0 ? group_result.group_edge_support[g, eidx] / edge_support : NaN)
            push!(raw_group_breaking_score, 2.0 * group_result.group_edge_support[g, eidx] / edge.normalized_margin)
        end
    end

    return DataFrame(group = group, group_mass = group_mass, edge_index = edge_index,
                     winner = winner, loser = loser, group_support = group_support,
                     group_margin_contribution = group_margin_contribution,
                     group_support_share_within_edge = group_support_share_within_edge,
                     raw_group_breaking_score = raw_group_breaking_score)
end

function group_breaker_table(group_result::GroupMajorityGraphSupportResult; amenability::Symbol=:inverse, lambda::Real=1.0)
    group = Symbol[]
    group_mass = Float64[]
    edge_index = Int[]
    winner = Symbol[]
    loser = Symbol[]
    amenability_weighted_support = Float64[]
    breaking_score = Float64[]

    base = group_result.base
    for g in eachindex(group_result.groups)
        for eidx in eachindex(base.edges)
            edge = base.edges[eidx]
            weighted = 0.0
            for r in eachindex(base.basis.types)
                base.support_matrix[r, eidx] || continue
                delta = boundary_distance_to_reverse(base.basis, r, edge.winner, edge.loser)
                weighted += group_result.group_type_mass[g, r] *
                            amenability_weight(delta; mode=amenability, lambda=lambda)
            end
            push!(group, group_result.groups[g])
            push!(group_mass, group_result.group_mass[g])
            push!(edge_index, eidx)
            push!(winner, base.pool[edge.winner])
            push!(loser, base.pool[edge.loser])
            push!(amenability_weighted_support, weighted)
            push!(breaking_score, 2.0 * weighted / edge.normalized_margin)
        end
    end

    return DataFrame(group = group, group_mass = group_mass, edge_index = edge_index,
                     winner = winner, loser = loser,
                     amenability_weighted_support = amenability_weighted_support,
                     breaking_score = breaking_score)
end

function group_anchor_table(group_result::GroupMajorityGraphSupportResult)
    return DataFrame(
        group = group_result.groups,
        group_mass = group_result.group_mass,
        anchoring = group_result.group_anchoring,
        conditional_anchoring = group_result.group_conditional_anchoring,
    )
end
