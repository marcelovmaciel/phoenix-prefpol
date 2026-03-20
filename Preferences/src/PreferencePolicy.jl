# PreferencePolicy.jl
# Missing-data extension policies + compare_maybe semantics

#########################
# Policy protocol        #
#########################

abstract type ExtensionPolicy end

#########################
# Concrete policy types #
#########################

struct BottomPolicyMissing <: ExtensionPolicy end
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
