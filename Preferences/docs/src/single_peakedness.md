# Single-Peakedness Diagnostics

```@meta
CurrentModule = Preferences
```

Single-peakedness is a domain-structure diagnostic, not a polarization measure.
It asks whether observed rankings can be represented as single-peaked on some
linear candidate axis. When they cannot, the diagnostics summarize how much
observed ranking mass lies outside the closest single-peaked domain and how much
Kendall distortion is needed to move the profile into that domain.

## What Single-Peakedness Asks

A profile is single-peaked when every ranking can be arranged around a common
one-dimensional axis. Each voter has a peak, and alternatives become less
preferred as one moves away from that peak along the axis.

This is different from asking whether a profile is polarized. A profile can
depart from one-dimensional structure without having two opposed camps, and a
highly organized disagreement can still be compatible with a single axis. The
single-peakedness tools therefore diagnose domain structure: whether a
one-dimensional restriction fits the observed rankings, and how far the profile
is from such a restriction.

## Axes Up To Reversal

An axis and its reversal encode the same single-peaked domain. For candidates
`[:a, :b, :c]`, the axes `[:a, :b, :c]` and `[:c, :b, :a]` are treated as
equivalent. `axes_up_to_reversal` returns one deterministic representative from
each reversal-equivalence class.

```julia
using Preferences

axes_up_to_reversal([:a, :b, :c])
```

## Single Rankings On An Axis

At the ranking level, `is_single_peaked` checks whether one strict
best-to-worst ranking is compatible with a supplied axis. `single_peaked_rankings`
enumerates the full single-peaked domain for that axis.

```julia
using Preferences

axis = [:a, :b, :c]

is_single_peaked([:a, :b, :c], axis)
is_single_peaked([:b, :a, :c], axis)
single_peaked_rankings(axis)
```

`single_peaked_distance(ranking, axis)` returns the minimum Kendall tau distance
from a ranking to the rankings that are single-peaked on the axis.

## Profile-Level Diagnostics

For an axis `a`, let `SP(a)` be the set of rankings single-peaked on that axis.
For an observed ranking type `r`, define

```math
d_a(r)
=
\min_{s \in SP(a)}
\frac{d_\tau(r,s)}{\binom{m}{2}}.
```

The profile-level diagnostics use observed ranking proportions `p_r`:

```math
L_0(a)=\sum_{r \notin SP(a)} p_r
```

```math
L_1(a)=\sum_r p_r d_a(r)
```

```math
L_{1,off}(a)
=
\frac{\sum_{r \notin SP(a)}p_r d_a(r)}{L_0(a)}
```

when `L0(a) > 0`, otherwise `missing`.

`single_peakedness_summary` evaluates these quantities over supplied axes, or
over all axes up to reversal when no axes are supplied. It returns tied
minimizers separately for `L0`, `L1`, and `L1_off_axis`.

## Interpreting L0, L1, And L1_off_axis

- `L0` is off-axis mass: the share of observed ranking mass not exactly
  single-peaked on the axis.
- `L1` is unconditional normalized Kendall distortion: the expected normalized
  distance from observed rankings to the axis's single-peaked domain.
- `L1_off_axis` is distortion conditional on the off-axis part: the average
  normalized distance among rankings that violate the axis.

The convenience function `best_single_peaked_axes` returns support-optimal axes
by `L0`. Those axes need not be the same as the axes minimizing `L1`, because an
axis with more off-axis mass can still have rankings that are closer to its
single-peaked domain.

## Worked Examples

This three-candidate profile is evaluated against one supplied axis:

```julia
using Preferences

pool = CandidatePool([:a, :b, :c])

p = Profile(pool, [
    StrictRank(pool, [:a, :b, :c]),
    StrictRank(pool, [:b, :a, :c]),
    StrictRank(pool, [:c, :b, :a]),
])

res = single_peakedness_summary(p; axes = [[:a, :b, :c]])
(res.best_L0, res.best_L1, res.best_L1_off_axis)
```

A Condorcet-cycle profile is a useful stress case for one-dimensional structure:

```julia
cycle = Profile(pool, [
    StrictRank(pool, [:a, :b, :c]),
    StrictRank(pool, [:b, :c, :a]),
    StrictRank(pool, [:c, :a, :b]),
])

single_peakedness_summary(cycle)
```

The point is not that every cycle is polarized. The point is that this profile
violates, or departs from, one-dimensional single-peaked structure.

## API Reference

```@docs
axes_up_to_reversal
is_single_peaked
single_peaked_rankings
single_peaked_distance
profile_distribution
single_peakedness_summary
single_peakedness_L0
single_peakedness_L1
single_peakedness_L1_off_axis
best_single_peaked_axes
SinglePeakedAxisSummary
SinglePeakedSupportClassification
SinglePeakednessResult
```
