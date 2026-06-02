# PreferenceBallot.jl

using Random
using StaticArrays

###############
# Ballot types
###############

"""
    StrictRank
    StrictRank(pool::CandidatePool, perm::AbstractVector{<:Integer}) -> StrictRank
    StrictRank(pool::CandidatePool, names::AbstractVector{Symbol}) -> StrictRank

Complete strict ranking over a candidate pool. The domain is either a
best-to-worst permutation of pool-relative candidate IDs or symbols from the
pool; the return value is a `StrictRank`.

The representation invariant is that `perm` is a complete permutation of
candidate IDs, where IDs are implementation-level 1-based positions in the
pool's canonical candidate order. Strict ranks have no missing entries and no
ties. Pool-aware constructors reject wrong lengths, out-of-range IDs, duplicate
IDs, and unknown names with `ArgumentError`.

Example: with `pool = CandidatePool([:a, :b, :c])`,
`ordered_candidates(StrictRank(pool, [:b, :a, :c]), pool) == [:b, :a, :c]`.
"""
struct StrictRank{N,Storage<:AbstractVector{Int}}
    # permutation of ids (best → worst), length N
    perm::Storage
end

"""
    WeakRank
    WeakRank(pool::CandidatePool, ranks::AbstractVector) -> WeakRank
    WeakRank(pool::CandidatePool, d::Dict{Symbol,<:Integer}) -> WeakRank

Weak or incomplete ranking over a candidate pool. The domain is a rank vector
indexed by candidate ID, or a dictionary from pool symbols to ranks; the return
value is a `WeakRank`.

The representation invariant is `ranks[id] = rank`, with lower rank numbers
better and `missing` meaning the candidate is unranked. Equal present ranks
encode ties. Candidate IDs are implementation-level positions in the common
`CandidatePool`. Pool-aware constructors reject wrong vector lengths, unknown
symbols, and ranks below `1` with `ArgumentError`.

Example: `WeakRank(CandidatePool([:a, :b]), Dict(:a => 1))` ranks `:a` and
leaves `:b` unranked.
"""
struct WeakRank{N,Storage<:AbstractVector{Union{Int,Missing}}}
    # ranks per id (1 = best), missing = unranked
    # length N; indices are candidate ids
    ranks::Storage
end

"""
    StrictRankDyn

Vector-backed alias for `StrictRank`. The domain and invariants are exactly
those of `StrictRank`: a complete best-to-worst permutation of candidate IDs
with no missing entries and no ties. The alias is an implementation/storage
detail and does not change return semantics or error behavior.
"""
const StrictRankDyn = StrictRank{N,Vector{Int}} where {N}

"""
    WeakRankDyn

Vector-backed alias for `WeakRank`. The domain and invariants are exactly those
of `WeakRank`: a rank vector indexed by candidate ID, with lower ranks better,
equal ranks tied, and `missing` unranked. The alias is an implementation/storage
detail and does not change return semantics or error behavior.
"""
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

"""
    perm(x::StrictRank) -> Vector{Int}
    perm(x::WeakRank) -> Vector{Int}

Return candidate IDs in best-to-worst order. For `StrictRank`, the return value
is a copy of the stored complete permutation. For `WeakRank`, the return value
is the deterministic `to_perm` ordering.

The invariant is that returned IDs are pool-relative implementation positions.
`StrictRank` has no missing or tie behavior. `WeakRank` sorts present ranks by
rank number, preserves ties by candidate ID/order from the rank vector, and
appends unranked IDs at the end; this is an ordering view, not a full
linearization contract. Inconsistent unchecked strict-rank storage may surface
through downstream errors.
"""
@inline perm(x::StrictRank) = copy(x.perm)

"""
    ranks(x::StrictRank) -> Vector{Int}
    ranks(x::WeakRank) -> AbstractVector{Union{Int,Missing}}

Return ranks indexed by candidate ID. For `StrictRank`, the return value is a
fresh `Vector{Int}` with positions derived from the permutation. For `WeakRank`,
the return value exposes the stored rank vector.

The invariant is that `ranks(x)[id]` is the candidate's rank in the pool, with
lower ranks better. `StrictRank` returns a complete strict rank vector with no
ties or missing values. `WeakRank` may contain equal ranks for ties and
`missing` for unranked candidates.
"""
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

"""
    rank(x, pool::CandidatePool, nm::Symbol) -> Union{Int,Missing}

Return the rank of candidate `nm` in ballot `x`. The domain is a `StrictRank` or
`WeakRank`, the common candidate pool, and a pool symbol; the return type is
`Int` for strict ranks and `Union{Int,Missing}` for weak ranks.

The invariant is that the pool maps `nm` to the candidate ID used by the ballot.
Lower rank numbers are better. Strict ranks have no missing/tie ranks; weak
ranks may return `missing` for unranked candidates and equal integers for ties.
Unknown candidate symbols throw `ArgumentError`.
"""
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

"""
    prefers(x, pool::CandidatePool, a::Symbol, b::Symbol) -> Bool

Return whether ballot `x` ranks `a` strictly above `b`. The domain is a
`StrictRank` or `WeakRank`, the common pool, and two candidate symbols; the
return type is `Bool`.

The invariant is comparison by pool-relative candidate IDs and lower rank
number as better. For weak ranks, pairs involving `missing` return `false`, and
equal present ranks also return `false` because they are ties. Unknown symbols
throw `ArgumentError`.
"""
# prefers(a,b): True iff rank(a) < rank(b)
function prefers(x::StrictRank, pool::CandidatePool, a::Symbol, b::Symbol)::Bool
    return rank(x, pool, a) < rank(x, pool, b)
end

function prefers(x::WeakRank, pool::CandidatePool, a::Symbol, b::Symbol)::Bool
    ra = rank(x, pool, a); rb = rank(x, pool, b)
    (ismissing(ra) || ismissing(rb)) && return false
    return ra < rb
end

"""
    indifferent(x, pool::CandidatePool, a::Symbol, b::Symbol) -> Bool

Return whether candidates `a` and `b` have the same rank in ballot `x`. The
domain is a `StrictRank` or `WeakRank`, the common pool, and two candidate
symbols; the return type is `Bool`.

The invariant is comparison by pool-relative candidate IDs. In a valid
`StrictRank`, two distinct candidates are never indifferent. In a `WeakRank`,
both candidates must be present and have equal ranks; comparisons involving
`missing` return `false`. Unknown symbols throw `ArgumentError`.
"""
# indifferent(a,b): True iff both present and equal rank
function indifferent(x::StrictRank, pool::CandidatePool, a::Symbol, b::Symbol)::Bool
    return rank(x, pool, a) == rank(x, pool, b)
end

function indifferent(x::WeakRank, pool::CandidatePool, a::Symbol, b::Symbol)::Bool
    ra = rank(x, pool, a); rb = rank(x, pool, b)
    return (!ismissing(ra) && !ismissing(rb) && ra == rb)
end

"""
    asdict(x, pool::CandidatePool) -> Dict{Symbol,Int}

Return a symbol-to-rank dictionary for ballot `x`. The domain is a `StrictRank`
or `WeakRank` and the common pool; the return type is `Dict{Symbol,Int}`.

The invariant is that dictionary keys are pool labels and values are rank
numbers with lower better. Strict ranks include every candidate exactly once.
Weak ranks omit candidates whose rank is `missing` and preserve equal values for
ties. Inconsistent ballot and pool sizes may throw indexing errors.
"""
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

"""
    to_perm(x) -> Vector{Int}

Return a best-to-worst vector of candidate IDs. The domain is a `StrictRank` or
`WeakRank`; the return type is `Vector{Int}`.

The invariant is that IDs are implementation-level positions in the associated
candidate pool. Strict ranks return the complete strict permutation. Weak ranks
sort present alternatives by increasing rank and append unranked alternatives
after all ranked alternatives; ties are ordered by candidate ID/order from the
rank vector. This is not a tie-resolving linearization.
"""
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

"""
    ordered_candidates(x::StrictRank, pool::CandidatePool) -> Vector{Symbol}

Return candidate symbols in the order encoded by a strict rank. The domain is a
`StrictRank` and its `CandidatePool`; the return type is `Vector{Symbol}`.

The invariant is that strict-rank candidate IDs index the pool's canonical
symbol order. There is no missing or tie behavior. Inconsistent ballot and pool
sizes may throw indexing errors.
"""
# Ordered candidate names from StrictRank
ordered_candidates(x::StrictRank, pool::CandidatePool) = [pool[id] for id in x.perm]

"""
    to_weakorder(x::WeakRank) -> Vector{Vector{Int}}

Return weak-order levels as groups of candidate IDs. The domain is a `WeakRank`;
the return type is `Vector{Vector{Int}}`.

The representation invariant is that each inner vector contains pool-relative
candidate IDs tied at the same present rank, ordered by candidate ID. Levels are
sorted by increasing rank number, so earlier levels are better. If any
candidates are `missing`, a final level contains the unranked IDs; this records
incompleteness and should not be read as an ordinary tie without an explicit
extension policy.
"""
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


"""
    weakorder_symbol_groups(levels::Vector{Vector{Int}}, pool::CandidatePool) -> Vector{Vector{Symbol}}

Map weak-order candidate-ID groups to symbol groups. The domain is the output of
`to_weakorder` and the common pool; the return type is `Vector{Vector{Symbol}}`.

The invariant is that every ID indexes the pool's canonical ordering. Ties and
the possible final unranked group are preserved structurally. Out-of-range IDs
throw `BoundsError`.
"""
# Map weak-order id groups to symbol groups
weakorder_symbol_groups(levels::Vector{Vector{Int}}, pool::CandidatePool) =
    [ [pool[id] for id in grp] for grp in levels ]

##################################
# Strictify with flexible tie-break
##################################

"""
    to_strict(x::WeakRank; tie_break=:error, rng=Random.GLOBAL_RNG,
              pool=nothing, incomplete_policy=:error) -> StrictRank

Linearize a weak or incomplete rank into a complete strict order. The domain is
a `WeakRank`; the return type is `StrictRank`. This is an interpretation step,
not an innocuous representation conversion.

The invariant of the result is a best-to-worst permutation of all candidate IDs.
Present rank buckets are processed by increasing rank. Singletons are kept as
is; tied buckets are resolved by `tie_break`: `:error` throws, `:random` shuffles
with `rng`, and a function may accept either `(ids)` or `(ids, pool, ranks)`.
Unranked candidates are an error by default; `incomplete_policy = :complete`
appends the unranked bucket and linearizes it with the same tie breaker. Other
incomplete policies throw `ArgumentError`.

Example: `to_strict(WeakRank([1, 1, 2]); tie_break=make_rank_bucket_linearizer(:by_id))`
returns the strict permutation `[1, 2, 3]`.
"""
function to_strict(x::WeakRank;
                   tie_break = :error,
                   rng::AbstractRNG = Random.GLOBAL_RNG,
                   pool::Union{Nothing,CandidatePool} = nothing,
                   incomplete_policy::Symbol = :error)
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
    if !isempty(unranked)
        if incomplete_policy === :error
            throw(ArgumentError(
                "to_strict: unranked present; pass incomplete_policy = :complete or use linearize(...; incomplete_policy = :preserve).",
            ))
        elseif incomplete_policy !== :complete
            throw(ArgumentError(
                "Unsupported incomplete_policy `$incomplete_policy`. Use :error or :complete with to_strict.",
            ))
        end
    end
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

"""
    make_rank_bucket_linearizer(strategy::Symbol; rng=Random.GLOBAL_RNG,
                                pool=nothing, jitter=0.5) -> Function

Build a deterministic or random tie-bucket linearizer for candidate-ID buckets.
The domain is a strategy symbol; the return value is a function
`ids::Vector{Int} -> Vector{Int}`.

The invariant is that the returned function reorders IDs within a tied rank
bucket and should return the same IDs exactly once. `:by_id` sorts by
implementation-level candidate ID, `:by_name` sorts by the pool's symbol labels,
`:random`/`:shuffle` shuffles with `rng`, and jitter strategies sort by
`id + jitter * rand(rng)`. `:by_name` without `pool`, negative `jitter`, and
unknown strategies throw `ArgumentError`.
"""
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

"""
    PairwiseDense(matrix) -> PairwiseDense

Dense pairwise ballot matrix indexed by candidate ID. The domain is an `N x N`
matrix, typically with entries `Union{Missing,Int8}`; the return value is a
`PairwiseDense` implementing `AbstractPairwise`.

The representation invariant is orientation: `matrix[i,j] == 1` means
candidate ID `i` is preferred to `j`, `-1` means the reverse, `0` means a tie or
the diagonal, and `missing` means undefined. Off-diagonal defined entries should
be antisymmetric. Missing entries are not ties. Constructors do not validate the
matrix shape or antisymmetry.
"""
struct PairwiseDense{T} <: AbstractPairwise
    matrix::Matrix{T}   # T = Union{Missing,Int8}
end

@inline dense(pw::PairwiseDense) = pw.matrix
@inline score(pw::PairwiseDense, i::Int, j::Int) = pw.matrix[i, j]
@inline isdefined(pw::PairwiseDense, i::Int, j::Int) = (i == j ? true : !ismissing(pw.matrix[i, j]))

"""
    pairwise_dense(pw::PairwiseDense) -> AbstractMatrix

Return the dense matrix backing a pairwise ballot. The domain is a
`PairwiseDense`; the return value is the stored matrix, with rows and columns
indexed by pool-relative candidate ID.

The invariant is the same as `dense`: diagonal entries are defined as `0`,
off-diagonal `missing` entries are undefined, and `0` off the diagonal is a
defined tie. This accessor returns the stored matrix rather than a copy.
"""
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

"""
    to_pairwise(x, pool::CandidatePool; policy::ExtensionPolicy) -> PairwiseDense

Convert a `StrictRank` or `WeakRank` into a dense pairwise ballot over `pool`.
The domain is a rank ballot and its common candidate pool; the return type is
`PairwiseDense{Union{Missing,Int8}}`.

The representation invariant is `score(i,j) == 1` when candidate ID `i` is
ranked above `j`, `-1` for the reverse, `0` for ties/diagonal, and `missing` for
undefined pairs. Strict ranks have no missing comparisons. Weak-rank missing
behavior is controlled by `policy`: for example, `NonePolicyMissing` preserves
undefined pairs while `BottomPolicyMissing` ranks present alternatives above
unranked ones. Pool size mismatches can cause indexing errors.
"""
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

"""
    restrict(x, pool::CandidatePool, subset_syms::AbstractVector{Symbol}) -> (new_x, new_pool, backmap)

Restrict a ballot or pairwise matrix to a candidate subset. The domain is a
`StrictRank`, `WeakRank`, or `PairwiseDense`, its original pool, and subset
symbols in the desired new order; the return value is the restricted object, a
new `CandidatePool`, and `backmap::Vector{Int}`.

The invariant is `backmap[new_id] == old_id`, so returned candidate IDs are
positions in `new_pool` while `backmap` recovers their original pool positions.
Strict ranks preserve the original relative order among retained candidates.
Weak ranks preserve missing entries and renormalize present ranks to contiguous
levels `1:K`, preserving ties. Pairwise restriction preserves scores and
missing pairs on the selected submatrix. Unknown subset symbols or duplicate
subset labels throw through `CandidatePool`/lookup errors.
"""
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
