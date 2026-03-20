# PreferenceCore.jl

using StaticArrays

# Core constants and helpers
const MAX_STATIC_N = 32
const CandidateId  = Int  # keep Int for ids

# CandidatePool: immutable logical pool of candidates with canonical order
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

# Label interface (no reflection in display)
labels(pool::CandidatePool) = String.(pool.names)
getlabel(pool::CandidatePool, i::Integer) = String(pool.names[i])

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

# convenience copies
candidates(pool::CandidatePool)::Vector{Symbol} = collect(pool.names)
to_cmap(pool::CandidatePool)::Dict{Int,Symbol} = Dict(i => pool.names[i] for i in 1:length(pool.names))

# Triangular (strict upper) indexing helpers for pairwise storage
@inline _tlen(N::Int) = (N * (N - 1)) ÷ 2
@inline function _tidx(i::Int, j::Int)
    (1 ≤ i < j) || throw(ArgumentError("_tidx expects 1 ≤ i < j (got i=$i, j=$j)"))
    # indices for pairs in lexicographic order by j then i:
    # (1,2)->1 ; (1,3)->2, (2,3)->3 ; (1,4)->4, (2,4)->5, (3,4)->6 ; ...
    return (j - 1) * (j - 2) ÷ 2 + i
end
