# Group Diagnostics

```@meta
CurrentModule = Preferences
```

Group diagnostics are consensus-relative. For each group, the package computes
a Kendall/Kemeny consensus, meaning a strict ranking that minimizes total
Kendall distance to the group's rankings. If there are multiple minimizers,
consensus-set quantities average across the minimizer set where the underlying
function is defined that way.

## Main Consensus-Relative Group Diagnostics

Within-group dispersion around the own-group consensus is:

```math
W_g =
\sum_r p_g(r)
\frac{d_\tau(r,\rho_g)}{\binom{m}{2}},
\qquad
C_g^\star = 1-W_g.
```

The rebased coherence is:

```math
C_g = 2C_g^\star - 1.
```

Aggregate coherence is:

```math
C = \sum_g \pi_g C_g.
```

Directed divergence from group members to other groups' consensuses is:

```math
D =
\frac{1}{K-1}
\sum_i \pi_i
\sum_{j\ne i}
\sum_r p_i(r)
\frac{d_\tau(r,\rho_j)}{\binom{m}{2}}.
```

`C` measures within-group organization around each group's own consensus. `D`
measures distance from group members to other groups' consensuses. `D` is not a
pure consensus-to-consensus distance and partly reflects internal dispersion,
so `C` and `D` should remain separate.

```julia
using Preferences

pool = CandidatePool([:a, :b, :c])
g1 = Profile(pool, [
    StrictRank(pool, [:a, :b, :c]),
    StrictRank(pool, [:a, :c, :b]),
])
g2 = Profile(pool, [
    StrictRank(pool, [:c, :b, :a]),
    StrictRank(pool, [:c, :a, :b]),
])

c1 = consensus_kendall(g1)
c2 = consensus_kendall(g2)
w1 = group_avg_distance(g1)
w2 = group_avg_distance(g2)
group_profiles = Dict(:g1 => g1, :g2 => g2)
consensus_map = Dict(:g1 => c1, :g2 => c2)

Cstar = 0.5 * (w1.group_coherence + w2.group_coherence)
C = 2Cstar - 1
D = overall_divergence(group_profiles, consensus_map)

(g1_consensus = c1.consensus_ranking,
 g2_consensus = c2.consensus_ranking,
 g1_avg_distance = w1.avg_distance,
 g2_avg_distance = w2.avg_distance,
 C = C,
 D = D)
```


## Derived Excess-Divergence Diagnostic

Let

```math
W = \frac{1-C}{2}
```

be the within-group dispersion baseline implied by aggregate coherence. The
current excess-divergence diagnostic is derived from `C` and directed
divergence `D`:

```math
S(C,D) = D - \frac{1-C}{2}.
```

Under the consensus-relative definitions of `C` and `D`, admissible
profile-derived inputs satisfy

```math
D \ge \frac{1-C}{2},
```

so `S(C,D) >= 0`. `S` is the nonnegative excess-divergence component in the
`C`-`D` decomposition. It is not a legacy measure and it is not an arbitrary
composite polarization score.

The raw numeric function validates only the unit-interval scale of `C` and `D`.
If a caller passes arbitrary numbers that violate `D >= (1-C)/2`, the raw value
can be negative; that should be interpreted as an invalid input for this
diagnostic, not as a possible value of `S` on admissible profiles.

The normalized excess-divergence ratio is

```math
E = 1 - \frac{W}{D}
```

when `D > 0`, with the package convention `E = 0` when `D == 0`. For `D > 0`,
`E = S/D`. The aliases `group_E`, `aggregate_E`,
`normalized_consensus_separation`, and `consensus_excess_separation` are in the
same derived `C`-`D` decomposition family.


## Distributional Distance Diagnostics

Consensus-relative quantities should be distinguished from pairwise
distributional quantities. Conceptually, average within-group pairwise distance
is:

```math
\bar W_g
=
\sum_{r,s}p_g(r)p_g(s)
\frac{d_\tau(r,s)}{\binom{m}{2}}.
```

Average between-group pairwise distance is:

```math
\bar D_{gh}
=
\sum_{r,s}p_g(r)p_h(s)
\frac{d_\tau(r,s)}{\binom{m}{2}}.
```

The current public API exposes these ideas mainly through legacy support
separation internals and overlap diagnostics rather than promoted named public
functions for `bar W_g` and `bar D_gh`.

## Legacy or Experimental Separation Diagnostics

Overlap `O` is useful as a distributional support diagnostic. `Sep` is an
overlap-adjusted consensus separation. `pairwise_group_separation`,
`overall_separation`, `grouped_gsep`, and the median/overlap separation
variants are legacy or experimental separation diagnostics, not the primary
`C`-`D` group diagnostics. `S(C,D)` and `E` remain available as derived
diagnostics from the current `C`-`D` decomposition. `S_old` is the legacy
support-separation statistic and is separate from current `S`.

## API

```@docs
ConsensusResult
kendall_tau_distance
consensus_kendall
get_consensus_ranking
kendall_tau_dict
consensus_for_group
group_avg_distance
weighted_coherence
pairwise_group_divergence
overall_divergence
S
normalized_consensus_separation
consensus_excess_separation
group_E
aggregate_E
E
pairwise_group_overlap
smoothed_overlap
pairwise_group_median_distance
pairwise_group_separation
overall_overlap
overall_overlap_smoothed
overall_divergence_median
overall_separation
grouped_gsep
S_old
```
