

# =====================================
# src/Compat.jl
# =====================================

# Backward aliases and helpers
const Pairwise = PairwiseDense
const PairwiseBallotStatic = PairwiseTriangularStatic
const PairwiseBallotMutable = PairwiseTriangularMutable
const PairwiseBallotView = PairwiseTriangularView

# Legacy display helper to retain old call sites
_labels_from(pool::CandidatePool, n::Int) = labels(pool)[1:n]
