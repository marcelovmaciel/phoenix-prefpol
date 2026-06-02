# PreferencePairwise.jl

"""
    AbstractPairwise

Abstract interface for pairwise preference ballots over a finite candidate
pool. Concrete subtypes represent comparisons between candidate IDs and must
provide `score`, `isdefined`, and `dense`.

The representation invariant is orientation: `score(pw, i, j) == 1` means
candidate ID `i` is preferred to ID `j`, `-1` means `j` is preferred to `i`,
`0` means a tie/indifference, and `missing` means the pair is undefined. The
diagonal is conventionally defined as `0`. Candidate IDs are pool-relative
implementation positions.
"""
abstract type AbstractPairwise end

# Shared API surface (methods must be provided by concrete subtypes)
"""
    score(pw::AbstractPairwise, i::Int, j::Int) -> Union{Int8,Int,Missing}

Return the oriented pairwise comparison for candidate IDs `i` and `j`. The
domain is a concrete pairwise ballot and two pool-relative IDs; the return value
uses `+1/-1/0/missing` for `i ≻ j`, `j ≻ i`, tie, or undefined.

The invariant is antisymmetry for defined off-diagonal entries and diagonal
zero. Missing entries encode unavailable comparisons, not ties. The abstract
fallback throws `ArgumentError`; concrete subtypes may throw `BoundsError` for
out-of-range IDs.
"""
function score(::AbstractPairwise, ::Int, ::Int)
    throw(ArgumentError("score not implemented for this pairwise type"))
end

"""
    isdefined(pw::AbstractPairwise, i::Int, j::Int) -> Bool

Return whether the comparison between candidate IDs `i` and `j` is defined.
This intentionally extends and exports Julia's `isdefined` name for pairwise
ballots. The domain is a concrete pairwise ballot and two pool-relative IDs; the
return type is `Bool`.

The representation invariant is that diagonal comparisons are defined, while
off-diagonal `missing` scores are undefined. Ties are defined comparisons with
score `0`. The abstract fallback throws `ArgumentError`.
"""
function isdefined(::AbstractPairwise, ::Int, ::Int)
    throw(ArgumentError("isdefined not implemented for this pairwise type"))
end

"""
    dense(pw::AbstractPairwise) -> AbstractMatrix

Return a dense matrix view or representation of a pairwise ballot. The domain is
a concrete pairwise ballot; the return value is an `N x N` matrix indexed by
candidate ID.

The matrix invariant is `M[i,j] == score(pw, i, j)`, diagonal entries are `0`,
and undefined off-diagonal comparisons are `missing`. Tie entries are `0`, not
`missing`. The abstract fallback throws `ArgumentError`.
"""
function dense(::AbstractPairwise)
    throw(ArgumentError("dense not implemented for this pairwise type"))
end
