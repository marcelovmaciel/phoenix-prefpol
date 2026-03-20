# PreferenceTraits.jl

##############################
# Trait API (defaults)
##############################

is_complete(::Any) = false
is_strict(::Any) = false
is_weak_order(::Any) = false
is_transitive(::Any) = false

##############################
# Core ballot traits
##############################

is_complete(::StrictRank) = true
is_strict(::StrictRank) = true
is_weak_order(::StrictRank) = true
is_transitive(::StrictRank) = true

is_complete(::StrictRankMutable) = true
is_strict(::StrictRankMutable) = true
is_weak_order(::StrictRankMutable) = true
is_transitive(::StrictRankMutable) = true

is_complete(x::WeakRank) = !any(ismissing, ranks(x))
is_strict(::WeakRank) = false
is_weak_order(::WeakRank) = true
is_transitive(::WeakRank) = true

##############################
# Pairwise traits
##############################

function is_complete(pw::PairwiseDense)
    M = pw.matrix
    @inbounds for i in axes(M, 1), j in axes(M, 2)
        i == j && continue
        ismissing(M[i, j]) && return false
    end
    return true
end

function is_strict(pw::PairwiseDense)
    M = pw.matrix
    @inbounds for i in axes(M, 1), j in axes(M, 2)
        i == j && continue
        v = M[i, j]
        (ismissing(v) || v == 0) && return false
    end
    return true
end

function is_complete(pw::PairwiseTriangularStatic)
    @inbounds for m in pw.mask
        m || return false
    end
    return true
end

function is_complete(pw::PairwiseTriangularMutable)
    @inbounds for m in pw.mask
        m || return false
    end
    return true
end

function is_complete(pw::PairwiseTriangularView)
    @inbounds for m in pw.mask
        m || return false
    end
    return true
end

function is_strict(pw::PairwiseTriangularStatic)
    @inbounds for (m, v) in zip(pw.mask, pw.vals)
        (!m || v == 0) && return false
    end
    return true
end

function is_strict(pw::PairwiseTriangularMutable)
    @inbounds for (m, v) in zip(pw.mask, pw.vals)
        (!m || v == 0) && return false
    end
    return true
end

function is_strict(pw::PairwiseTriangularView)
    @inbounds for (m, v) in zip(pw.mask, pw.vals)
        (!m || v == 0) && return false
    end
    return true
end
