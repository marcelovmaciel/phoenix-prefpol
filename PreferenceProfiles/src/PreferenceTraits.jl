# PreferenceTraits.jl

##############################
# Trait API (defaults)
##############################

"""
    is_complete(x) -> Bool

Return whether `x` represents all required comparisons or ranks. The domain is
any ballot/profile-like object; unsupported objects return `false`.

The invariant depends on representation: `StrictRank` is complete, `WeakRank` is
complete only when it has no `missing` ranks, pairwise ballots are complete only
when every off-diagonal comparison is defined, and profiles require all ballots
to be complete. Ties may still be complete. No errors are expected for supported
types with consistent storage.
"""
is_complete(::Any) = false

"""
    is_strict(x) -> Bool

Return whether `x` encodes strict preferences. The domain is any ballot/profile-
like object; unsupported objects return `false`.

The invariant is that strict representations have no ties and no undefined
comparisons. `StrictRank` is strict; `WeakRank` is not considered strict even if
its present ranks happen to be unique; pairwise ballots are strict only when
all off-diagonal entries are defined and nonzero. Profiles require all ballots
to be strict.
"""
is_strict(::Any) = false

"""
    is_weak_order(x) -> Bool

Return whether `x` is interpreted as a weak order. The domain is any
ballot/profile-like object; unsupported objects return `false`.

The invariant is that `StrictRank` is also a weak order and `WeakRank` is treated
as a weak order even with `missing` entries, because missingness is
incompleteness rather than intransitivity. Profiles require all ballots to be
weak orders. Missing entries do not by themselves make a `WeakRank` fail this
trait.
"""
is_weak_order(::Any) = false

"""
    is_transitive(x) -> Bool

Return whether `x` is interpreted as transitive. The domain is any
ballot/profile-like object; unsupported objects return `false`.

The invariant is that `StrictRank` and `WeakRank` rank-vector representations
are transitive by construction, including weak ranks with missing entries.
Profiles require all ballots to be transitive. Pairwise transitivity is not
certified by the default pairwise trait methods in this file.
"""
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
