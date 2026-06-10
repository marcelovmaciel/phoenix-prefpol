# PreferenceCore.jl

using StaticArrays

# Core constants and helpers
const MAX_STATIC_N = 32
const CandidateId  = Int  # keep Int for ids

"""
    CandidatePool(names::AbstractVector{Symbol}) -> CandidatePool

Finite candidate universe with a canonical order of alternatives. The domain is
a nonempty vector of unique `Symbol` labels; the return value is a
`CandidatePool` whose candidate IDs are the 1-based implementation-level
positions in that canonical order.

The representation invariant is that labels are unique and stable for the life
of the pool. Candidate IDs are not semantic labels: they are positions in this
pool and should only be compared within the same pool. Empty pools and duplicate
labels throw `ArgumentError`.

Example:

```julia
using PreferenceProfiles

pool = CandidatePool([:a, :b, :c])
pool[:b]        # candidate id 2
pool[2]         # candidate label :b
candidates(pool)
```
"""
struct CandidatePool{N,Storage<:AbstractVector{Symbol}}
    names::Storage  # SVector{N,Symbol} if N ≤ MAX_STATIC_N, else Vector{Symbol}
    function CandidatePool(names::AbstractVector{Symbol})
        N = length(names)
        N == 0 && throw(ArgumentError("Candidate pool cannot be empty"))
        length(unique(names)) == N || throw(ArgumentError("Candidates must be unique"))
        storage = N ≤ MAX_STATIC_N ? SVector{N,Symbol}(Tuple(names)) : collect(names)
        return new{N,typeof(storage)}(storage)
    end
end

"""
    labels(pool::CandidatePool) -> Vector{String}

Return the canonical candidate labels as strings, in pool order. The domain is a
`CandidatePool`; the returned vector is a fresh string vector and does not
change the pool representation.

The invariant is that positions in the returned vector correspond to candidate
IDs. There is no missing or tie behavior. Bounds and duplicate-label errors are
handled by `CandidatePool` construction, not by this accessor.
"""
labels(pool::CandidatePool) = String.(pool.names)

"""
    getlabel(pool::CandidatePool, i::Integer) -> String

Return the string label for candidate ID `i`. The domain is a pool and a 1-based
implementation-level candidate position; the return type is `String`.

The representation invariant is that `i` indexes the pool's canonical ordering.
No missing or tie behavior applies. Out-of-range IDs use Julia indexing and
throw `BoundsError`.
"""
getlabel(pool::CandidatePool, i::Integer) = String(pool.names[i])

"""
    candid(pool::CandidatePool, s::Symbol) -> Int

Return the candidate ID for symbol `s` in `pool`. The domain is a candidate
label from the pool; the return type is the 1-based `Int` position in the
pool's canonical order.

Candidate IDs are implementation-level positions and are valid only relative to
the same `CandidatePool`. Missing labels are not represented: an unknown symbol
throws `ArgumentError`.

Example: `candid(CandidatePool([:left, :right]), :right) == 2`.
"""
function candid(pool::CandidatePool{N}, s::Symbol) where {N}
    idx = findfirst(==(s), pool.names)
    idx === nothing && throw(ArgumentError("Unknown candidate: $(String(s))"))
    return Int(idx)
end

# name → id (kept)
Base.getindex(pool::CandidatePool, s::Symbol) = candid(pool, s)

# key membership (Symbol)
Base.haskey(pool::CandidatePool, s::Symbol) = findfirst(==(s), pool.names) !== nothing

# === ADDED ergonomics ===

# id → name
@inline function Base.getindex(pool::CandidatePool, i::Integer)::Symbol
    ii = Int(i)
    @boundscheck (1 ≤ ii ≤ length(pool.names)) || throw(BoundsError(pool, ii))
    return pool.names[ii]
end

# length
@inline Base.length(pool::CandidatePool{N}) where {N} = N

# iterate names (Symbols) in canonical order
@inline Base.iterate(pool::CandidatePool) = iterate(pool.names)
@inline Base.iterate(pool::CandidatePool, state) = iterate(pool.names, state)

# keys = names iterator (Symbols)
Base.keys(pool::CandidatePool) = (pool.names[i] for i in eachindex(pool.names))

"""
    candidates(pool::CandidatePool) -> Vector{Symbol}

Return the canonical candidate symbols in pool order. The domain is a
`CandidatePool`; the return value is a fresh `Vector{Symbol}` copy.

The invariant is that vector index `i` names candidate ID `i`. There is no
missing or tie behavior, and construction-time duplicate/empty checks are the
only pool-level errors.
"""
candidates(pool::CandidatePool)::Vector{Symbol} = collect(pool.names)

"""
    to_cmap(pool::CandidatePool) -> Dict{Int,Symbol}

Return a dictionary mapping candidate ID to candidate symbol. The domain is a
`CandidatePool`; the return type is `Dict{Int,Symbol}`.

The invariant is that keys are the 1-based implementation positions in the
pool's canonical candidate order. There is no missing or tie behavior.

Example: `to_cmap(CandidatePool([:a, :b]))[1] == :a`.
"""
to_cmap(pool::CandidatePool)::Dict{Int,Symbol} = Dict(i => pool.names[i] for i in 1:length(pool.names))

# Triangular (strict upper) indexing helpers for pairwise storage
@inline _tlen(N::Int) = (N * (N - 1)) ÷ 2
@inline function _tidx(i::Int, j::Int)
    (1 ≤ i < j) || throw(ArgumentError("_tidx expects 1 ≤ i < j (got i=$i, j=$j)"))
    # indices for pairs in lexicographic order by j then i:
    # (1,2)->1 ; (1,3)->2, (2,3)->3 ; (1,4)->4, (2,4)->5, (3,4)->6 ; ...
    return (j - 1) * (j - 2) ÷ 2 + i
end
