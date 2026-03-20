# PreferenceBallot.jl

using Random
using StaticArrays

###############
# Ballot types
###############

struct StrictRank{N,Storage<:AbstractVector{Int}}
    # permutation of ids (best → worst), length N
    perm::Storage
end

struct WeakRank{N,Storage<:AbstractVector{Union{Int,Missing}}}
    # ranks per id (1 = best), missing = unranked
    # length N; indices are candidate ids
    ranks::Storage
end

# Dynamic storage aliases (Vector-backed)
const StrictRankDyn = StrictRank{N,Vector{Int}} where {N}
const WeakRankDyn = WeakRank{N,Vector{Union{Int,Missing}}} where {N}

##############################
# Constructors (StrictRank)
##############################

# Storage helper
@inline function _strict_storage(vp::Vector{Int})
    N = length(vp)
    return N ≤ MAX_STATIC_N ? SVector{N,Int}(vp) : vp
end

# From ids permutation (best→worst), no validation
function StrictRank(perm::AbstractVector{<:Integer})
    vp = Int[perm[i] for i in eachindex(perm)]
    storage = _strict_storage(vp)
    N = length(vp)
    return StrictRank{N,typeof(storage)}(storage)
end

# From ids permutation (best→worst)
function StrictRank(pool::CandidatePool, perm::AbstractVector{<:Integer})
    N = length(pool)
    length(perm) == N || throw(ArgumentError("StrictRank: permutation length must be $N"))
    seen = falses(N)
    vp = Int[perm[i] for i in eachindex(perm)]
    for x in vp
        (1 ≤ x ≤ N) || throw(ArgumentError("StrictRank: id out of range: $x"))
        seen[x] && throw(ArgumentError("StrictRank: duplicate id $x"))
        seen[x] = true
    end
    storage = _strict_storage(vp)
    return StrictRank{N,typeof(storage)}(storage)
end

# From names permutation (best→worst)
function StrictRank(pool::CandidatePool, names::AbstractVector{Symbol})
    ids = [pool[s] for s in names]
    return StrictRank(pool, ids)
end

# Identity strict order from pool order
StrictRank(pool::CandidatePool) = StrictRank(collect(1:length(pool)))

##############################
# Constructors (WeakRank)
##############################

# Storage helper
@inline function _weakrank_storage(r::Vector{Union{Int,Missing}})
    N = length(r)
    return N ≤ MAX_STATIC_N ? SVector{N,Union{Int,Missing}}(r) : r
end

# From vector of ranks (Int or Missing)
function WeakRank(ranks::AbstractVector{Union{Int,Missing}})
    r = ranks isa Vector{Union{Int,Missing}} ? ranks : collect(ranks)
    storage = _weakrank_storage(r)
    N = length(r)
    return WeakRank{N,typeof(storage)}(storage)
end

# From Dict{name=>rank}
function WeakRank(pool::CandidatePool, d::Dict{Symbol,<:Integer})
    N = length(pool)
    r = Vector{Union{Int,Missing}}(undef, N)
    fill!(r, missing)
    for (nm, rk) in d
        id = pool[nm]
        rk ≥ 1 || throw(ArgumentError("WeakRank: ranks must be ≥ 1"))
        r[id] = Int(rk)
    end
    return WeakRank(r)
end

# From vector of ranks (Int or Missing); length == N
function WeakRank(pool::CandidatePool, v::AbstractVector)
    N = length(pool)
    length(v) == N || throw(ArgumentError("WeakRank: vector length must be $N"))
    r = Vector{Union{Int,Missing}}(undef, N)
    for i in 1:N
        vi = v[i]
        if vi === missing
            r[i] = missing
        else
            iv = Int(vi)
            iv ≥ 1 || throw(ArgumentError("WeakRank: ranks must be ≥ 1"))
            r[i] = iv
        end
    end
    return WeakRank(r)
end

##############################
# Accessors / predicates
##############################

# Accessors (public)
@inline perm(x::StrictRank) = copy(x.perm)

@inline function ranks(x::StrictRank)::Vector{Int}
    N = length(x.perm)
    r = Vector{Int}(undef, N)
    @inbounds for (pos, id) in enumerate(x.perm)
        r[id] = pos
    end
    return r
end

@inline ranks(x::WeakRank) = x.ranks

# WeakRank perm mirrors to_perm semantics (unranked appended at the end).
@inline perm(x::WeakRank) = to_perm(x)

# rank(::StrictRank, pool, :name) -> Int
function rank(x::StrictRank, pool::CandidatePool, nm::Symbol)::Int
    id = pool[nm]
    # find position in permutation
    @inbounds for (k, v) in pairs(x.perm)
        if v == id
            return k
        end
    end
    error("Inconsistent StrictRank permutation")
end

# rank(::WeakRank, pool, :name) -> Int | missing
@inline rank(x::WeakRank, pool::CandidatePool, nm::Symbol) = ranks(x)[pool[nm]]

# prefers(a,b): True iff rank(a) < rank(b)
function prefers(x::StrictRank, pool::CandidatePool, a::Symbol, b::Symbol)::Bool
    return rank(x, pool, a) < rank(x, pool, b)
end

function prefers(x::WeakRank, pool::CandidatePool, a::Symbol, b::Symbol)::Bool
    ra = rank(x, pool, a); rb = rank(x, pool, b)
    (ismissing(ra) || ismissing(rb)) && return false
    return ra < rb
end

# indifferent(a,b): True iff both present and equal rank
function indifferent(x::StrictRank, pool::CandidatePool, a::Symbol, b::Symbol)::Bool
    return rank(x, pool, a) == rank(x, pool, b)
end

function indifferent(x::WeakRank, pool::CandidatePool, a::Symbol, b::Symbol)::Bool
    ra = rank(x, pool, a); rb = rank(x, pool, b)
    return (!ismissing(ra) && !ismissing(rb) && ra == rb)
end

# asdict
function asdict(x::StrictRank, pool::CandidatePool)::Dict{Symbol,Int}
    N = length(pool)
    d = Dict{Symbol,Int}()
    for (pos, id) in enumerate(x.perm)
        d[pool[id]] = pos
    end
    return d
end

function asdict(x::WeakRank, pool::CandidatePool)::Dict{Symbol,Int}
    N = length(pool)
    rx = ranks(x)
    d = Dict{Symbol,Int}()
    @inbounds for id in 1:N
        rk = rx[id]
        if !ismissing(rk)
            d[pool[id]] = rk::Int
        end
    end
    return d
end

##################################
# Conversions and order views
##################################

# Strict → perm (ids best→worst)
@inline to_perm(x::StrictRank) = collect(perm(x))

# Weak → perm (ids best→worst; unranked appended at the end)
function to_perm(x::WeakRank)
    rx = ranks(x)
    N = length(rx)
    ranked = [(id, r) for id in 1:N for r = (rx[id],) if !ismissing(r)]
    sort!(ranked; by = tup -> tup[2])
    ids_ranked = [id for (id, _) in ranked]
    ids_unranked = [id for id in 1:N if ismissing(rx[id])]
    if !isempty(ids_unranked)
        @info "to_perm(::WeakRank): unranked candidates are appended at the end; use to_strict(...; tie_break=...) to control this."
    end
    return vcat(ids_ranked, ids_unranked)
end

# Ordered candidate names from StrictRank
ordered_candidates(x::StrictRank, pool::CandidatePool) = [pool[id] for id in x.perm]

# Weak-order levels (Vector of groups of ids). Last group = unranked (if any).

# Weak-order levels (Vector of groups of ids). Last group = unranked (if any).
function to_weakorder(x::WeakRank)::Vector{Vector{Int}}
    rx = ranks(x)
    N = length(rx)

    # collect ids by present rank
    byrank = Dict{Int, Vector{Int}}()
    unranked = Int[]

    @inbounds for id in 1:N
        rk = rx[id]
        if ismissing(rk)
            push!(unranked, id)
        else
            v = get!(byrank, rk::Int, Int[])
            push!(v, id)
        end
    end

    levels = Vector{Vector{Int}}()
    if !isempty(byrank)
        for rk in sort!(collect(keys(byrank)))
            push!(levels, byrank[rk])
        end
    end
    if !isempty(unranked)
        push!(levels, unranked)
    end
    return levels
end


# Map weak-order id groups to symbol groups
weakorder_symbol_groups(levels::Vector{Vector{Int}}, pool::CandidatePool) =
    [ [pool[id] for id in grp] for grp in levels ]

##################################
# Strictify with flexible tie-break
##################################

"""
to_strict(x::WeakRank; tie_break=:error | :random | f, rng=Random.GLOBAL_RNG)
- :error  => throw if ties/missing
- :random => break ties within each bucket randomly (uses rng); missing also linearized at the end
- f::Function => custom bucket linearizer: f(bucket_ids::Vector{Int}, pool, ranks)::Vector{Int}
"""
function to_strict(x::WeakRank; tie_break=:error, rng::AbstractRNG=Random.GLOBAL_RNG, pool::Union{Nothing,CandidatePool}=nothing)
    rx = ranks(x)
    N = length(rx)
    # collect buckets by rank
    buckets = Dict{Int,Vector{Int}}()
    unranked = Int[]
    for id in 1:N
        rk = rx[id]
        if ismissing(rk)
            push!(unranked, id)
        else
            v = get!(buckets, rk::Int, Int[])
            push!(v, id)
        end
    end
    isempty(unranked) || (tie_break == :error && throw(ArgumentError("to_strict: unranked present; choose tie_break != :error")))
    # sort ranks
    keys_sorted = sort!(collect(keys(buckets)))
    # bucket linearizer
    linearize = _resolve_bucket_linearizer(tie_break, rng, pool, rx)
    # build strict perm
    perm = Int[]
    for rk in keys_sorted
        grp = buckets[rk]
        if length(grp) == 1
            push!(perm, grp[1])
        else
            append!(perm, linearize(grp))
        end
    end
    if !isempty(unranked)
        append!(perm, linearize(unranked))
    end
    length(perm) == N || throw(AssertionError("to_strict: produced perm of wrong length"))
    return StrictRank(perm)
end

# Public helper to build linearizers
function make_rank_bucket_linearizer(strategy::Symbol; rng::AbstractRNG=Random.GLOBAL_RNG,
                                     pool::Union{Nothing,CandidatePool}=nothing,
                                     jitter::Real=0.5)
    if strategy === :random || strategy === :shuffle
        return ids -> begin
            v = copy(ids); Random.shuffle!(rng, v); v
        end
    elseif strategy === :by_name
        pool === nothing && throw(ArgumentError(":by_name requires pool="))
        return ids -> sort(ids; by = id -> pool[id])
    elseif strategy === :by_id
        return ids -> sort(ids)
    elseif strategy === :id_jitter || strategy === :by_id_jitter || strategy === :jitter
        jitter ≥ 0 || throw(ArgumentError("jitter must be ≥ 0"))
        j = float(jitter)
        return ids -> begin
            pairs = [(id, Float64(id) + j * rand(rng)) for id in ids]
            sort!(pairs; by = last)
            return [p[1] for p in pairs]
        end
    else
        throw(ArgumentError("Unknown strategy $strategy"))
    end
end

# internal: adapt tie_break argument to a function ids->Vector{Int}
function _resolve_bucket_linearizer(tie_break, rng::AbstractRNG, pool, ranks)
    if tie_break === :error
        return ids -> (length(ids) == 1 ? ids : throw(ArgumentError("Tie encountered; choose tie_break != :error")))
    elseif tie_break === :random
        return ids -> begin v = copy(ids); Random.shuffle!(rng, v); v end
    elseif tie_break isa Function
        return ids -> begin
            if applicable(tie_break, ids, pool, ranks)
                return tie_break(ids, pool, ranks)
            elseif applicable(tie_break, ids)
                return tie_break(ids)
            else
                throw(ArgumentError("tie_break must accept (ids) or (ids, pool, ranks)"))
            end
        end
    else
        throw(ArgumentError("Invalid tie_break option: $tie_break"))
    end
end

##############################
# Pairwise (dense wrapper)
##############################

struct PairwiseDense{T} <: AbstractPairwise
    matrix::Matrix{T}   # T = Union{Missing,Int8}
end

@inline dense(pw::PairwiseDense) = pw.matrix
@inline score(pw::PairwiseDense, i::Int, j::Int) = pw.matrix[i, j]
@inline isdefined(pw::PairwiseDense, i::Int, j::Int) = (i == j ? true : !ismissing(pw.matrix[i, j]))
@inline pairwise_dense(pw::PairwiseDense) = dense(pw)

# core engine: compute pairwise from a given ranks vector and policy
function _pairwise_from_ranks!(M::Matrix{Union{Missing,Int8}}, ranks::AbstractVector{Union{Int,Missing}},
                               pool::CandidatePool, policy::ExtensionPolicy)
    N = length(ranks)
    @assert size(M,1) == N && size(M,2) == N
    @inbounds for i in 1:N
        M[i,i] = 0
        for j in i+1:N
            ra = ranks[i]; rb = ranks[j]
            cmp = compare_maybe(policy, ra, rb, i, j, ranks, pool)  # defined in PreferencePolicy.jl
            M[i,j] = cmp
            M[j,i] = ismissing(cmp) ? missing : Int8(-cmp)
        end
    end
    return M
end

# StrictRank → ranks (no missing)
@inline _ranks_from_strict(x::StrictRank)::Vector{Union{Int,Missing}} = begin
    N = length(x.perm)
    r = Vector{Union{Int,Missing}}(undef, N)
    for (pos, id) in enumerate(x.perm)
        r[id] = pos
    end
    r
end

# Public constructor: WeakRank/StrictRank → PairwiseDense
function to_pairwise(x::WeakRank, pool::CandidatePool; policy::ExtensionPolicy)
    N = length(pool)
    M = Matrix{Union{Missing,Int8}}(undef, N, N)
    _pairwise_from_ranks!(M, ranks(x), pool, policy)
    return PairwiseDense{Union{Missing,Int8}}(M)
end

function to_pairwise(x::StrictRank, pool::CandidatePool; policy::ExtensionPolicy)
    N = length(pool)
    M = Matrix{Union{Missing,Int8}}(undef, N, N)
    r = _ranks_from_strict(x)
    _pairwise_from_ranks!(M, r, pool, policy)
    return PairwiseDense{Union{Missing,Int8}}(M)
end

##################################
# Restriction (return backmap)
##################################

# Helper: build new pool from subset of symbols (in given order)
function _restrict_pool(pool::CandidatePool, subset_syms::AbstractVector{Symbol})
    # validate symbols exist
    old_ids = Int[ pool[s] for s in subset_syms ]
    new_pool = CandidatePool(subset_syms)
    # backmap: new_id -> old_id
    backmap = old_ids
    return new_pool, backmap
end

# Remap ranks by backmap; renormalize ranks to 1..K within present ranks; keep missing
function _restrict_weak_ranks(ranks::AbstractVector{Union{Int,Missing}}, backmap::Vector{Int})
    Nn = length(backmap)
    newr = Vector{Union{Int,Missing}}(undef, Nn)
    # collect present ranks
    present = Int[]
    for (new_id, old_id) in enumerate(backmap)
        rk = ranks[old_id]
        newr[new_id] = rk
        if !ismissing(rk); push!(present, rk::Int); end
    end
    if isempty(present)
        return newr
    end
    uniq = sort!(unique(present))
    ren = Dict{Int,Int}(uniq[k] => k for k in eachindex(uniq))
    for i in 1:Nn
        rk = newr[i]
        if !ismissing(rk)
            newr[i] = ren[rk::Int]
        end
    end
    return newr
end

# StrictRank restrict
function restrict(x::StrictRank, pool::CandidatePool, subset_syms::AbstractVector{Symbol})
    new_pool, backmap = _restrict_pool(pool, subset_syms)
    # keep only ids in x.perm that are in backmap, preserve order
    keep = Set(backmap)
    new_perm_old_ids = [id for id in x.perm if id in keep]
    # translate old ids -> new ids
    inv = Dict{Int,Int}(backmap[new] => new for new in eachindex(backmap))
    new_perm = [inv[id] for id in new_perm_old_ids]
    return (StrictRank(new_perm), new_pool, backmap)
end

# WeakRank restrict
function restrict(x::WeakRank, pool::CandidatePool, subset_syms::AbstractVector{Symbol})
    new_pool, backmap = _restrict_pool(pool, subset_syms)
    newr = _restrict_weak_ranks(ranks(x), backmap)
    return (WeakRank(newr), new_pool, backmap)
end

# PairwiseDense restrict (dense matrix)
function restrict(pw::PairwiseDense, pool::CandidatePool, subset_syms::AbstractVector{Symbol})
    new_pool, backmap = _restrict_pool(pool, subset_syms)
    idx = backmap
    sub = pw.matrix[idx, idx]
    return (PairwiseDense{Union{Missing,Int8}}(sub), new_pool, backmap)
end
