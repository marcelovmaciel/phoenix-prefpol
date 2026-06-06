# Advanced Representations

```@meta
CurrentModule = Preferences
```

These APIs are for advanced use: performance-sensitive code, dynamics, agent
based models, custom pairwise storage, or low-level representation work. Most
empirical workflows should start with `CandidatePool`, `StrictRank`,
`WeakRank`, `Profile`, and `WeightedProfile`.

Candidate IDs are pool-relative positions. Pairwise ballots use oriented
scores: `1` means row candidate beats column candidate, `-1` means the reverse,
`0` means a tie or diagonal, and `missing` means undefined.

Mutable strict rankings support local swaps and pairwise updates for simulation
or dynamics code:

```julia
x = StrictRankMutable(StrictRank(pool, [:a, :b, :c]))
swap_positions!(x, 1, 2)
```

## API

```@docs
labels
getlabel
candid
candidates
to_cmap
perm
ranks
AbstractPairwise
PairwiseDense
Pairwise
PairwiseTriangularStatic
PairwiseTriangularMutable
PairwiseTriangularView
PairwiseBallotStatic
PairwiseBallotMutable
PairwiseBallotView
pairwise_view
score
isdefined
pairwise_dense
dense
is_complete
is_strict
is_weak_order
is_transitive
StrictRankMutable
swap_positions!
swap_ids!
swap_and_update_pairwise!
```

