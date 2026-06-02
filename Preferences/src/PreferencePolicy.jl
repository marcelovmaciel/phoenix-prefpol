# PreferencePolicy.jl
# Missing-data extension policies + compare_maybe semantics

#########################
# Policy protocol        #
#########################

"""
    ExtensionPolicy

Abstract protocol for extending rank-vector ballots into pairwise comparisons.
The domain is a policy object used by `compare_maybe`; concrete policies return
`Union{Missing,Int8}` comparisons.

The invariant is the pairwise convention `+1` for candidate ID `i` preferred to
`j`, `-1` for `j` preferred to `i`, `0` for an explicit tie, and `missing` for
an undefined comparison. Missing-rank behavior is policy-specific. The abstract
fallback for `compare_maybe` throws `MethodError` for custom policies without a
method.
"""
abstract type ExtensionPolicy end

#########################
# Concrete policy types #
#########################

"""
    BottomPolicyMissing()

Missing-data extension policy for weak ranks. The domain is pairwise comparison
between rank entries `ra` and `rb`; `compare_maybe` returns `Union{Missing,Int8}`.

The representation invariant is lower rank number means better. A present rank
beats a `missing` rank, two missing ranks remain undefined, and equal present
ranks return `0` for a tie. No constructor errors are possible.
"""
struct BottomPolicyMissing <: ExtensionPolicy end

"""
    NonePolicyMissing()

Missing-data extension policy for weak ranks. The domain is pairwise comparison
between rank entries `ra` and `rb`; `compare_maybe` returns `Union{Missing,Int8}`.

The invariant is lower rank number means better. If either side is `missing`,
the pairwise comparison is `missing`; equal present ranks return `0` for a tie.
No constructor errors are possible.
"""
struct NonePolicyMissing   <: ExtensionPolicy end

#########################
# Helpers               #
#########################

@inline _cmp_from_ranks(ra::Int, rb::Int)::Int8 = begin
    d = rb - ra              # lower rank number = better
    d > 0 ? Int8(1) : (d < 0 ? Int8(-1) : Int8(0))
end

#########################
# compare_maybe API     #
#########################
# Signature expected by PairwiseDense engine:
#   compare_maybe(policy, ra, rb, i, j, ranks, pool) :: Union{Missing,Int8}
# Return +1 if i ≻ j, -1 if j ≻ i, 0 if indifferent, missing if undefined.

"""
    compare_maybe(policy::ExtensionPolicy, ra, rb, i, j, ranks, pool) -> Union{Missing,Int8}

Compare candidate IDs `i` and `j` under an extension policy. The domain is two
rank entries from a weak-rank vector, their candidate IDs, the full rank vector,
and the common `CandidatePool`; the return value is `+1`, `-1`, `0`, or
`missing`.

The invariant is that lower numerical ranks are better and candidate IDs are
pool-relative positions. `NonePolicyMissing` leaves any pair involving
`missing` undefined; `BottomPolicyMissing` ranks present alternatives above
unranked alternatives and leaves both-unranked pairs undefined. Equal present
ranks are ties. Unknown custom policies without a method throw `MethodError`.

Example: `compare_maybe(BottomPolicyMissing(), 1, missing, 1, 2, [1, missing], pool)`
returns `Int8(1)`.
"""
# "None" policy: any missing ⇒ missing; otherwise compare numerical ranks.
function compare_maybe(::NonePolicyMissing,
                       ra::Union{Int,Missing}, rb::Union{Int,Missing},
                       i::Int, j::Int, ranks::AbstractVector{Union{Int,Missing}},
                       pool::CandidatePool)
    (ismissing(ra) || ismissing(rb)) && return missing
    return _cmp_from_ranks(ra::Int, rb::Int)
end

# "Bottom" policy: ranked ≻ unranked; both unranked ⇒ missing; otherwise compare ranks.
function compare_maybe(::BottomPolicyMissing,
                       ra::Union{Int,Missing}, rb::Union{Int,Missing},
                       i::Int, j::Int, ranks::AbstractVector{Union{Int,Missing}},
                       pool::CandidatePool)
    if ismissing(ra)
        return ismissing(rb) ? missing : Int8(-1)  # j wins (ranked) over i (unranked)
    elseif ismissing(rb)
        return Int8(1)                             # i (ranked) wins over j (unranked)
    else
        return _cmp_from_ranks(ra::Int, rb::Int)
    end
end

# Optional default for custom policies: method must be provided by user.
function compare_maybe(::ExtensionPolicy,
                       ra::Union{Int,Missing}, rb::Union{Int,Missing},
                       i::Int, j::Int, ranks::AbstractVector{Union{Int,Missing}},
                       pool::CandidatePool)
    throw(MethodError(compare_maybe, (typeof(ra), typeof(rb), i, j, typeof(ranks), typeof(pool))))
end
