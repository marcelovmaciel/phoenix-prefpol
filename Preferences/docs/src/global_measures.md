# Global Profile Diagnostics

Global diagnostics summarize the structure of one strict profile. The main
objects are pairwise balance, exact reversal mass, concentration of exact
reversal mass, and effective support of observed rankings or reversal pairs.

Pairwise balance:

```math
\Psi(p)
=
\frac{1}{n\binom{m}{2}}
\sum_{\{a,b\}}
\left(n - |n_{ab}-n_{ba}|\right).
```

Exact reversal mass:

```math
R(p)=\sum_{\{r,\bar r\}} 2\min(p(r),p(\bar r)).
```

Reversal concentration:

```math
\kappa(p)=\sum_i q_i^2,
\qquad
q_i=\frac{2\min(p(r_i),p(\bar r_i))}{R(p)}.
```

Geometric reversal index:

```math
RHHI(p)=\sqrt{R(p)\kappa(p)}.
```

When `R(p) == 0`, the implementation sets `kappa(p) = 0` and `RHHI(p) = 0`.

Interpretation:

- `Psi` measures pairwise balance.
- `R` measures exact reversal mass.
- `kappa` measures concentration of reversal mass.
- `RHHI` is conjunctive: it is high only when reversal mass and concentration are both high.
- `EO` measures effective observed ranking support.
- `ENRP` measures the effective number of reversal pairs carrying reversal mass.

```julia
using Preferences

pool = CandidatePool([:a, :b, :c])
unanimous = Profile(pool, [StrictRank(pool, [:a, :b, :c]) for _ in 1:3])
pure_inversion = Profile(pool, [
    StrictRank(pool, [:a, :b, :c]),
    StrictRank(pool, [:c, :b, :a]),
])
near_uniform = Profile(pool, [
    StrictRank(pool, [:a, :b, :c]),
    StrictRank(pool, [:a, :c, :b]),
    StrictRank(pool, [:b, :a, :c]),
    StrictRank(pool, [:b, :c, :a]),
    StrictRank(pool, [:c, :a, :b]),
    StrictRank(pool, [:c, :b, :a]),
])

(
    unanimous = can_polarization(unanimous),
    inversion = (
        R = total_reversal_component(pure_inversion),
        kappa = reversal_hhi(pure_inversion),
        RHHI = reversal_geometric(pure_inversion),
    ),
    support = (
        EO = effective_observed_rankings(near_uniform),
        ENRP = effective_reversal_rankings(near_uniform),
    ),
)
```

## API

- `ranking_proportions`
- `reversal_pairs`
- `can_polarization`
- `total_reversal_component`
- `reversal_hhi`
- `reversal_geometric`
- `effective_observed_rankings`
- `effective_reversal_rankings`
- `effective_reversal_ranking_diagnostics`
- `ranking_support_diagnostics`

