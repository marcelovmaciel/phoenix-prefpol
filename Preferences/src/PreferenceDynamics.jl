# =====================================
# src/PreferenceDynamics.jl
# =====================================

# uses _tlen/_tidx defined in PreferenceCore.jl

##############################
# Mutable strict rank (ABM)
##############################

"""
    StrictRankMutable(x::StrictRank)
    StrictRankMutable{N}(r::Vector{UInt16})

Mutable complete strict ranking used by dynamics code. The storage convention is
`ranks[id] = position`, where candidate IDs are pool-relative positions and
lower positions are preferred. `StrictRankMutable(::StrictRank)` derives this
rank-vector representation from the immutable best-to-worst permutation.

The parametric constructor checks only length; callers that pass raw storage are
responsible for preserving the permutation invariant.
"""
mutable struct StrictRankMutable{N}
    # ranks[id] = position (1..N)
    ranks::Vector{UInt16}  # length N, permutation of 1..N
    function StrictRankMutable{N}(r::Vector{UInt16}) where {N}
        length(r) == N || throw(ArgumentError("Wrong length for StrictRankMutable"))
        new{N}(r)
    end
end

# Build from an immutable StrictRank (generic — works if StrictRank has perm or ranks)
function StrictRankMutable(x::StrictRank)
    # compute ranks from x without assuming its internal fields
    perm = to_perm(x)                      # best→worst (ids)
    N = length(perm)
    r = Vector{UInt16}(undef, N)
    @inbounds for (pos, id) in enumerate(perm)
        r[id] = UInt16(pos)
    end
    return StrictRankMutable{N}(r)
end

# Current permutation of ids (best→worst)
function to_perm(srm::StrictRankMutable{N}) where {N}
    perm = collect(1:N)
    sort!(perm; by = i -> srm.ranks[i])
    return perm
end

"""
    swap_positions!(srm, pos_i, pos_j) -> srm

Swap the candidates currently occupying ranking positions `pos_i` and `pos_j`.
Positions are 1-based best-to-worst ranks, not candidate IDs. The mutation is
in place and preserves the `ranks[id] = position` representation when the input
already satisfies that invariant.
"""
function swap_positions!(srm::StrictRankMutable{N}, pos_i::Integer, pos_j::Integer) where {N}
    (1 ≤ pos_i ≤ N && 1 ≤ pos_j ≤ N) || throw(BoundsError(srm, (pos_i, pos_j)))
    id_i = findfirst(r -> r == pos_i, srm.ranks)
    id_j = findfirst(r -> r == pos_j, srm.ranks)
    id_i === nothing && error("Invariant error: position $pos_i not present")
    id_j === nothing && error("Invariant error: position $pos_j not present")
    @inbounds begin
        srm.ranks[id_i] = UInt16(pos_j)
        srm.ranks[id_j] = UInt16(pos_i)
    end
    return srm
end

"""
    swap_ids!(srm, id_i, id_j) -> srm

Swap the ranking positions assigned to candidate IDs `id_i` and `id_j`.
Candidate IDs are 1-based pool-relative identifiers, not rank positions. The
mutation is in place.
"""
function swap_ids!(srm::StrictRankMutable{N}, id_i::Integer, id_j::Integer) where {N}
    (1 ≤ id_i ≤ N && 1 ≤ id_j ≤ N) || throw(BoundsError(srm, (id_i, id_j)))
    @inbounds begin
        ri = srm.ranks[id_i]
        srm.ranks[id_i] = srm.ranks[id_j]
        srm.ranks[id_j] = ri
    end
    return srm
end

##############################
# Triangular pairwise ballots
##############################

# We store strict upper triangle for pairs (i,j) with i<j:
#   vals[k] = score(i,j) ∈ T   where k = _tidx(i,j)
#   mask[k] = true if defined, false if missing
# Score symmetry:
#   score(j,i) = -score(i,j)
# Diagonal is always zero in displays.

"""
    PairwiseTriangularStatic{N,T}()
    PairwiseTriangularStatic{N,T}(vals, mask)

Immutable pairwise-preference ballot backed by strict upper-triangle storage.
For each pair `i < j`, `vals[_tidx(i,j)]` stores the oriented score for `i`
against `j` and `mask[_tidx(i,j)]` says whether that pair is defined.
`score(j,i)` is the negated stored score, missing pairs are represented by a
false mask entry, and the diagonal is defined as zero.

The outer constructor validates storage length but does not copy `vals` or
`mask`.
"""
struct PairwiseTriangularStatic{N,T<:Integer} <: AbstractPairwise
    vals::Vector{T}      # length _tlen(N)
    mask::BitVector      # length _tlen(N)
    function PairwiseTriangularStatic{N,T}(vals::Vector{T}, mask::BitVector) where {N,T<:Integer}
        _tlen(N) == length(vals) == length(mask) || throw(ArgumentError("Triangle storage wrong length"))
        new{N,T}(vals, mask)
    end
end

"""
    PairwiseTriangularMutable{N,T}()
    PairwiseTriangularMutable{N,T}(vals, mask)

Mutable upper-triangle pairwise ballot with the same orientation, missingness,
and diagonal conventions as `PairwiseTriangularStatic`. Mutating helpers update
the shared `vals` and `mask` vectors in place.
"""
mutable struct PairwiseTriangularMutable{N,T<:Integer} <: AbstractPairwise
    vals::Vector{T}      # length _tlen(N)
    mask::BitVector      # length _tlen(N)
    function PairwiseTriangularMutable{N,T}(vals::Vector{T}, mask::BitVector) where {N,T<:Integer}
        _tlen(N) == length(vals) == length(mask) || throw(ArgumentError("Triangle storage wrong length"))
        new{N,T}(vals, mask)
    end
end

"""
    PairwiseTriangularView{N,T}

Read-only protocol view over triangular pairwise storage. The view aliases the
underlying `vals` and `mask` vectors; it does not copy. Use `pairwise_view` to
construct views from static or mutable triangular ballots.
"""
struct PairwiseTriangularView{N,T<:Integer} <: AbstractPairwise
    vals::Vector{T}      # alias to underlying storage
    mask::BitVector
end

# Convenience constructors
function PairwiseTriangularStatic{N,T}() where {N,T<:Integer}
    L = _tlen(N)
    PairwiseTriangularStatic{N,T}(Vector{T}(undef, L), falses(L))
end
function PairwiseTriangularMutable{N,T}() where {N,T<:Integer}
    L = _tlen(N)
    PairwiseTriangularMutable{N,T}(zeros(T, L), falses(L))
end

"""
    pairwise_view(pb) -> PairwiseTriangularView

Return an aliasing view over a `PairwiseTriangularStatic` or
`PairwiseTriangularMutable`. The returned view follows the `AbstractPairwise`
`score`/`isdefined` protocol and reflects later mutations to mutable storage.
"""
pairwise_view(pbs::PairwiseTriangularStatic{N,T}) where {N,T<:Integer} = PairwiseTriangularView{N,T}(pbs.vals, pbs.mask)
pairwise_view(pbm::PairwiseTriangularMutable{N,T}) where {N,T<:Integer} = PairwiseTriangularView{N,T}(pbm.vals, pbm.mask)

# Definition predicates & access (View is the read-only protocol)
isdefined(pb::PairwiseTriangularView{N}, i::Int, j::Int) where {N} =
    (i == j ? true : pb.mask[i < j ? _tidx(i, j) : _tidx(j, i)])

function score(pb::PairwiseTriangularView{N,T}, i::Int, j::Int) where {N,T<:Integer}
    i == j && return zero(T)
    if i < j
        idx = _tidx(i, j)
        return pb.mask[idx] ? pb.vals[idx] : missing
    else
        idx = _tidx(j, i)
        return pb.mask[idx] ? -pb.vals[idx] : missing
    end
end

# Forwarders for Static/Mutable to the View protocol
isdefined(pb::PairwiseTriangularStatic{N}, i::Int, j::Int) where {N} = isdefined(pairwise_view(pb), i, j)
isdefined(pb::PairwiseTriangularMutable{N}, i::Int, j::Int) where {N} = isdefined(pairwise_view(pb), i, j)
score(pb::PairwiseTriangularStatic{N,T}, i::Int, j::Int) where {N,T<:Integer} = score(pairwise_view(pb), i, j)
score(pb::PairwiseTriangularMutable{N,T}, i::Int, j::Int) where {N,T<:Integer} = score(pairwise_view(pb), i, j)

"""
    pairwise_dense(pb) -> Matrix{Union{Missing,T}}

Expand triangular pairwise storage to a dense matrix. The returned matrix has
zero diagonal, `score(i,j)` in cell `(i,j)`, the negated score in `(j,i)`, and
`missing` in both orientations for undefined pairs. The matrix is a fresh
allocation and does not alias triangular storage.
"""
function pairwise_dense(pbs::PairwiseTriangularStatic{N,T}) where {N,T<:Integer}
    M = Matrix{Union{Missing,T}}(undef, N, N)
    @inbounds for i in 1:N
        M[i,i] = zero(T)
        for j in i+1:N
            idx = _tidx(i, j)
            if pbs.mask[idx]
                v = pbs.vals[idx]
                M[i,j] = v
                M[j,i] = -v
            else
                M[i,j] = missing
                M[j,i] = missing
            end
        end
    end
    return M
end

function pairwise_dense(pbv::PairwiseTriangularView{N,T}) where {N,T<:Integer}
    M = Matrix{Union{Missing,T}}(undef, N, N)
    @inbounds for i in 1:N
        M[i,i] = zero(T)
        for j in i+1:N
            v = score(pbv, i, j)
            if v === missing
                M[i,j] = missing
                M[j,i] = missing
            else
                M[i,j] = v
                M[j,i] = -v
            end
        end
    end
    return M
end

pairwise_dense(pbm::PairwiseTriangularMutable{N,T}) where {N,T<:Integer} =
    pairwise_dense(pairwise_view(pbm))

@inline dense(pbs::PairwiseTriangularStatic{N,T}) where {N,T<:Integer} = pairwise_dense(pbs)
@inline dense(pbv::PairwiseTriangularView{N,T}) where {N,T<:Integer} = pairwise_dense(pbv)
@inline dense(pbm::PairwiseTriangularMutable{N,T}) where {N,T<:Integer} = pairwise_dense(pbm)

##############################
# Mutation helpers
##############################

# Set a pair (i,j) with oriented score for i<j. If i>j, flip the sign.
function set!(pbm::PairwiseTriangularMutable{N,T}, i::Int, j::Int, v::T) where {N,T<:Integer}
    i == j && error("set!: diagonal is not settable")
    if i < j
        idx = _tidx(i, j)
        pbm.vals[idx] = v
        pbm.mask[idx] = true
    else
        idx = _tidx(j, i)
        pbm.vals[idx] = -v
        pbm.mask[idx] = true
    end
    return pbm
end

# Clear a pair (i,j) to missing (both orientations)
function clear!(pbm::PairwiseTriangularMutable{N}, i::Int, j::Int) where {N}
    i == j && return pbm
    idx = i < j ? _tidx(i, j) : _tidx(j, i)
    pbm.mask[idx] = false
    return pbm
end

# Wipe all entries to missing
function clear!(pbm::PairwiseTriangularMutable{N}) where {N}
    fill!(pbm.mask, false)
    return pbm
end

##############################
# Pairwise from ballots
##############################

# NOTE: Concrete policies and compare_maybe live in PreferencePolicy.jl
# compare_maybe(policy, ra, rb, i, j, ranks, pool) :: Union{Missing,Int8}

# Strict ballots have no missing; tie is impossible.
function pairwise_from_strict!(pbm::PairwiseTriangularMutable{N,Int8}, x::StrictRank) where {N}
    # derive ranks[id] from x generically
    perm = to_perm(x)                 # ids best→worst
    ranks = Vector{Int}(undef, N)
    @inbounds for (pos, id) in enumerate(perm)
        ranks[id] = pos
    end
    @inbounds for j in 2:N
        for i in 1:j-1
            v = ranks[i] < ranks[j] ? Int8(1) : Int8(-1)  # strict, so no ties
            idx = _tidx(i, j)
            pbm.vals[idx] = v
            pbm.mask[idx] = true
        end
    end
    return pbm
end

# Weak ballots: policy decides missing/ties/unranked treatment.
function pairwise_from_weak!(pbm::PairwiseTriangularMutable{N,Int8}, x::WeakRank,
                             pool::CandidatePool, policy::ExtensionPolicy) where {N}
    rx = ranks(x)   # AbstractVector{Union{Int,Missing}} from WeakRank
    @inbounds for j in 2:N
        for i in 1:j-1
            ra = rx[i]; rb = rx[j]
            cmp = compare_maybe(policy, ra, rb, i, j, rx, pool)
            idx = _tidx(i, j)
            if ismissing(cmp)
                pbm.mask[idx] = false
            else
                pbm.vals[idx] = cmp
                pbm.mask[idx] = true
            end
        end
    end
    return pbm
end

# Convenience allocators
pairwise_from_strict(x::StrictRank) = begin
    N = length(to_perm(x))
    pbm = PairwiseTriangularMutable{N,Int8}()
    pairwise_from_strict!(pbm, x)
    PairwiseTriangularStatic{N,Int8}(pbm.vals, pbm.mask)  # freeze by aliasing
end

pairwise_from_weak(x::WeakRank, pool::CandidatePool, policy::ExtensionPolicy) = begin
    N = length(pool)
    pbm = PairwiseTriangularMutable{N,Int8}()
    pairwise_from_weak!(pbm, x, pool, policy)
    PairwiseTriangularStatic{N,Int8}(pbm.vals, pbm.mask)
end

##############################
# Joint updates (strict + pw)
##############################

"""
    swap_and_update_pairwise!(pbm, srm, pos_i, pos_j) -> pbm

Swap two rank positions in `srm` with `swap_positions!` and then fully rebuild
the strict pairwise scores in `pbm`. The pairwise ballot must share the same
candidate count as the mutable strict rank.
"""
function swap_and_update_pairwise!(pbm::PairwiseTriangularMutable{N,Int8},
                                   srm::StrictRankMutable{N},
                                   pos_i::Integer, pos_j::Integer) where {N}
    swap_positions!(srm, pos_i, pos_j)
    # rebuild a StrictRank generically from mutable ranks
    perm = to_perm(srm)
    pairwise_from_strict!(pbm, StrictRank(perm))
    return pbm
end
