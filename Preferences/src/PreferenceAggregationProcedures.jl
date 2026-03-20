# PreferenceAggregationProcedures.jl

##############################
# Pairwise majority aggregation
##############################

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

function pairwise_majority(p::Profile)
    N = length(p.pool)
    counts = zeros(Int, N, N)
    @inbounds for b in p.ballots
        _accumulate_pairwise_counts!(counts, b, p.pool)
    end
    return PairwiseMajority(counts)
end

pairwise_majority(pm::PairwiseMajority) = pm

pairwise_majority_counts(pm::PairwiseMajority) = pm.counts
pairwise_majority_counts(p::Profile) = pairwise_majority_counts(pairwise_majority(p))

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
