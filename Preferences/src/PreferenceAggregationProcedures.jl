# PreferenceAggregationProcedures.jl

##############################
# Pairwise majority aggregation
##############################

raw"""
    PairwiseMajority(counts)

Pairwise majority-count aggregate for a profile.

`counts[i, j]` is the voter mass preferring candidate `i` to candidate `j`,
using pool-relative candidate ids. The diagonal is conventionally zero.
Margins are `counts[i, j] - counts[j, i]`; wins are the sign of that margin,
with ties represented by zero.

The count matrix is not antisymmetric: both `counts[i,j]` and `counts[j,i]`
are meaningful directional support counts. The margin matrix

```math
M_{ij} = counts[i,j] - counts[j,i]
```

is antisymmetric, and a positive `M[i,j]` means candidate `i` beats candidate
`j` by majority margin.
"""
struct PairwiseMajority{T<:Integer}
    counts::Matrix{T}  # counts[i,j] = voters preferring i over j
    function PairwiseMajority{T}(counts::Matrix{T}) where {T<:Integer}
        size(counts, 1) == size(counts, 2) || throw(ArgumentError("PairwiseMajority requires a square counts matrix"))
        new{T}(counts)
    end
end

PairwiseMajority(counts::Matrix{T}) where {T<:Integer} = PairwiseMajority{T}(counts)

@inline _pairwise_n(pw::PairwiseDense) = size(pw.matrix, 1)
@inline _pairwise_n(pw::PairwiseTriangularStatic{N}) where {N} = N
@inline _pairwise_n(pw::PairwiseTriangularMutable{N}) where {N} = N
@inline _pairwise_n(pw::PairwiseTriangularView{N}) where {N} = N

function _accumulate_pairwise_counts!(counts::Matrix{Int}, x::StrictRank, pool::CandidatePool)
    perm = to_perm(x)
    N = length(perm)
    @inbounds for pos_i in 1:(N - 1)
        i = perm[pos_i]
        for pos_j in (pos_i + 1):N
            j = perm[pos_j]
            counts[i, j] += 1
        end
    end
    return counts
end

function _accumulate_pairwise_counts!(counts::Matrix{Int}, x::StrictRankMutable, pool::CandidatePool)
    perm = to_perm(x)
    N = length(perm)
    @inbounds for pos_i in 1:(N - 1)
        i = perm[pos_i]
        for pos_j in (pos_i + 1):N
            j = perm[pos_j]
            counts[i, j] += 1
        end
    end
    return counts
end

function _accumulate_pairwise_counts!(counts::Matrix{Int}, x::WeakRank, pool::CandidatePool)
    rx = ranks(x)
    N = length(rx)
    @inbounds for j in 2:N
        rj = rx[j]
        for i in 1:(j - 1)
            ri = rx[i]
            (ismissing(ri) || ismissing(rj) || ri == rj) && continue
            if ri < rj
                counts[i, j] += 1
            else
                counts[j, i] += 1
            end
        end
    end
    return counts
end

function _accumulate_pairwise_counts!(counts::Matrix{Int}, pw::AbstractPairwise, pool::CandidatePool)
    N = size(counts, 1)
    _pairwise_n(pw) == N || throw(ArgumentError("Pairwise ballot size mismatch with pool"))
    @inbounds for j in 2:N
        for i in 1:(j - 1)
            v = score(pw, i, j)
            (v === missing || v == 0) && continue
            if v > 0
                counts[i, j] += 1
            else
                counts[j, i] += 1
            end
        end
    end
    return counts
end

"""
    pairwise_majority(profile)
    pairwise_majority(pm::PairwiseMajority)

Aggregate a profile into pairwise majority counts.

For each ordered pair `(i, j)`, the returned `PairwiseMajority.counts[i, j]`
is the mass preferring candidate `i` to candidate `j`. Strict rankings
contribute to every ordered pair implied by their order. Weak-rank ties,
weak-rank missing pairs, pairwise ballot scores equal to `0`, and pairwise
ballot scores equal to `missing` do not contribute to either direction.

The current public aggregation method is for `Profile`; `WeightedProfile`
inputs are intentionally not defined here.

Example:

```julia
using Preferences

pool = CandidatePool([:a, :b, :c])
p = Profile(pool, [
    StrictRank(pool, [:a, :b, :c]),
    StrictRank(pool, [:b, :c, :a]),
    StrictRank(pool, [:c, :a, :b]),
])

pm = pairwise_majority(p)
pairwise_majority_margins(pm)
pretty_pairwise_majority_margins(pm, pool)
```
"""
function pairwise_majority(p::Profile)
    N = length(p.pool)
    counts = zeros(Int, N, N)
    @inbounds for b in p.ballots
        _accumulate_pairwise_counts!(counts, b, p.pool)
    end
    return PairwiseMajority(counts)
end

pairwise_majority(pm::PairwiseMajority) = pm

"""
    pairwise_majority_counts(pm_or_profile)

Return the pairwise majority-count matrix.

`counts[i, j]` is the mass preferring candidate `i` to candidate `j`.
For profile inputs, counts are computed by `pairwise_majority(profile)`.
The returned matrix is the stored matrix for `PairwiseMajority` inputs.

The count matrix records directional support, not net wins. Use
`pairwise_majority_margins` for the antisymmetric matrix of net pairwise
majorities.
"""
pairwise_majority_counts(pm::PairwiseMajority) = pm.counts
pairwise_majority_counts(p::Profile) = pairwise_majority_counts(pairwise_majority(p))

"""
    pairwise_majority_margins(pm_or_profile)

Return the antisymmetric pairwise majority-margin matrix.

Entry `[i, j]` is `counts[i, j] - counts[j, i]`: positive when candidate `i`
beats candidate `j`, negative when `j` beats `i`, and zero for ties. The
diagonal is zero.

For any pair `i != j`, `margins[i,j] == -margins[j,i]`. Positive entries are
therefore read row-over-column: row candidate beats column candidate.
"""
function pairwise_majority_margins(pm::PairwiseMajority)
    counts = pm.counts
    N = size(counts, 1)
    margins = Matrix{eltype(counts)}(undef, N, N)
    @inbounds for i in 1:N
        margins[i, i] = 0
        for j in (i + 1):N
            d = counts[i, j] - counts[j, i]
            margins[i, j] = d
            margins[j, i] = -d
        end
    end
    return margins
end

pairwise_majority_margins(p::Profile) = pairwise_majority_margins(pairwise_majority(p))

"""
    pairwise_majority_wins(pm_or_profile)

Return the signed pairwise majority-win matrix.

Entry `[i, j]` is `sign(counts[i, j] - counts[j, i])`, encoded as `1` when
candidate `i` beats candidate `j`, `-1` when `i` loses to `j`, and `0` for a
tie or diagonal entry.

This is the sign-only majority graph adjacency matrix. It discards margin size;
use `pairwise_majority_margins` when magnitude matters.
"""
function pairwise_majority_wins(pm::PairwiseMajority)
    counts = pm.counts
    N = size(counts, 1)
    wins = Matrix{Int8}(undef, N, N)
    @inbounds for i in 1:N
        wins[i, i] = 0
        for j in (i + 1):N
            d = counts[i, j] - counts[j, i]
            if d > 0
                wins[i, j] = 1
                wins[j, i] = -1
            elseif d < 0
                wins[i, j] = -1
                wins[j, i] = 1
            else
                wins[i, j] = 0
                wins[j, i] = 0
            end
        end
    end
    return wins
end

pairwise_majority_wins(p::Profile) = pairwise_majority_wins(pairwise_majority(p))
