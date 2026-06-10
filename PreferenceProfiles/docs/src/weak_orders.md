# Weak Orders and Linearization

```@meta
CurrentModule = PreferenceProfiles
```

`WeakRank` represents weak or incomplete rankings over a candidate pool. Equal
present ranks are ties. `missing` means unranked, not tied. This distinction
matters because pairwise conversion of incomplete weak orders requires an
explicit interpretation of missingness.

`NonePolicyMissing()` leaves comparisons involving unranked candidates
undefined. `BottomPolicyMissing()` ranks present candidates above unranked
candidates and leaves only both-unranked comparisons undefined.

```julia
using PreferenceProfiles, Random

pool = CandidatePool([:a, :b, :c])
w = WeakRank(pool, Dict(:a => 1, :b => 1))

weakorder_symbol_groups(to_weakorder(w), pool)
```

```julia
pw = to_pairwise(w, pool; policy = BottomPolicyMissing())
dense(pw)
```

Linearization is an interpretive completion step, not a neutral identity map.
Random tie-breaking and pattern-conditional tie-breaking answer different
empirical questions: random tie-breaking treats compatible strict extensions as
exchangeable, while pattern-conditional tie-breaking learns extension weights
from a strict reference profile.

```julia
linearize(
    w;
    tie_break = :by_id,
    incomplete_policy = :complete,
    pool = pool,
) |> x -> ordered_candidates(x, pool)
```

## API

```@docs
WeakRank
to_weakorder
weakorder_symbol_groups
ExtensionPolicy
NonePolicyMissing
BottomPolicyMissing
to_pairwise
to_strict
linearize
make_rank_bucket_linearizer
PatternConditionalLinearizer
```

