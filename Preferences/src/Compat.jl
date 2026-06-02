

# =====================================
# src/Compat.jl
# =====================================

# Backward aliases and helpers
"""
    Pairwise

Backward-compatible alias for `PairwiseDense`.

This name is retained for older call sites only. New code should use
`PairwiseDense` and its documented pairwise-orientation conventions directly.
"""
const Pairwise = PairwiseDense

"""
    PairwiseBallotStatic

Backward-compatible alias for `PairwiseTriangularStatic`.

This name is retained for older call sites only. New code should use
`PairwiseTriangularStatic` directly.
"""
const PairwiseBallotStatic = PairwiseTriangularStatic

"""
    PairwiseBallotMutable

Backward-compatible alias for `PairwiseTriangularMutable`.

This name is retained for older call sites only. New code should use
`PairwiseTriangularMutable` directly.
"""
const PairwiseBallotMutable = PairwiseTriangularMutable

"""
    PairwiseBallotView

Backward-compatible alias for `PairwiseTriangularView`.

This name is retained for older call sites only. New code should use
`PairwiseTriangularView` directly.
"""
const PairwiseBallotView = PairwiseTriangularView

# Legacy display helper to retain old call sites
_labels_from(pool::CandidatePool, n::Int) = labels(pool)[1:n]
