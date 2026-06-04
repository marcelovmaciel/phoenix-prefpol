# =====================================
# src/Preferences.jl
# =====================================

module Preferences

using Combinatorics
using StaticArrays
using Random
using SHA
using PrettyTables
using Crayons
using DataFrames
using OrderedCollections: OrderedDict
using PooledArrays: PooledArray
using Printf
using StatsBase: proportionmap

# Include core components (keep your load order)
include("PreferenceCore.jl")
include("PreferencePairwise.jl")
include("PreferencePolicy.jl")
include("PreferenceBallot.jl")
include("PreferenceDynamics.jl")
include("PreferenceTraits.jl")
include("PreferenceProfile.jl")
include("PreferenceLinearization.jl")
include("PreferenceTabularProfiles.jl")
include("PreferenceAggregationProcedures.jl")
include("PreferenceMeasures.jl")
include("PreferenceMajorityGraphSupport.jl")
include("PreferenceMajorityGraphRoles.jl")
include("PreferencePluralitySwitchTables.jl")
include("single_peakedness.jl")
include("PreferenceConsensus.jl")
include("PreferenceAnnotatedProfiles.jl")
include("PreferenceDisplay.jl")
include("Compat.jl")


# ------------------------------------------------------------
# Binding-level docs for public help/discovery
# ------------------------------------------------------------

@doc raw"""
    ConsensusResult

Result of a Kendall/Kemeny consensus search. Fields record the candidate tuple,
selected consensus permutation/ranking/ballot, objective value, average
normalized Kendall distance, total mass, tied-minimizer diagnostics, tie rule,
and all minimizing permutations when requested.
""" ConsensusResult

@doc raw"""
    SinglePeakedAxisSummary

Summary row for one candidate axis in `single_peakedness_summary`, including
axis identifiers, support, off-axis mass, and normalized distance quantities.
""" SinglePeakedAxisSummary

@doc raw"""
    SinglePeakedSupportClassification

Classification row connecting an observed ranking type to a candidate axis in
single-peakedness diagnostics.
""" SinglePeakedSupportClassification

@doc raw"""
    SinglePeakednessResult

Container returned by `single_peakedness_summary`, including axis summaries,
selected support classifications, observed support, and best-axis identifiers.
""" SinglePeakednessResult

@doc raw"""
    rank(x, pool, name)

Return a candidate's rank in a strict or weak ballot using the candidate IDs of
`pool`. Lower ranks are better. Strict rankings always return an `Int`; weak
rankings may return `missing` for unranked candidates and equal ranks for tied
candidates.

Example:

```julia
pool = CandidatePool([:a, :b, :c])
x = StrictRank(pool, [:b, :a, :c])

rank(x, pool, :b)  # 1
rank(x, pool, :c)  # 3
```
""" rank

@doc raw"""
    prefers(x, pool, a, b)

Return whether ballot `x` ranks candidate `a` strictly above candidate `b`.
For `WeakRank`, ties and pairs involving unranked candidates return `false`;
use `indifferent` to test present ties.

Example:

```julia
pool = CandidatePool([:a, :b, :c])
x = StrictRank(pool, [:b, :a, :c])

prefers(x, pool, :b, :c)  # true
prefers(x, pool, :c, :b)  # false
```
""" prefers

@doc raw"""
    indifferent(x, pool, a, b)

Return whether candidates `a` and `b` are tied in ballot `x`, using
pool-relative candidate IDs. Distinct candidates in a valid `StrictRank` are
never indifferent. For `WeakRank`, both candidates must be present and have the
same rank; missing comparisons return `false`.

Example:

```julia
pool = CandidatePool([:a, :b, :c])
w = WeakRank(pool, Dict(:a => 1, :b => 1, :c => 2))

indifferent(w, pool, :a, :b)  # true
```
""" indifferent

@doc raw"""
    asdict(x, pool)

Convert a strict or weak ballot to a candidate-symbol dictionary. Strict ranks
map every candidate to its position; weak ranks omit unranked candidates.
""" asdict

@doc raw"""
    to_perm(x)

Return candidate IDs in best-to-worst order. For a `StrictRank`, this is the
complete strict permutation. For a `WeakRank`, present rank buckets are sorted
by rank, tied candidates are ordered by candidate ID, and unranked candidates
are appended at the end. This is an ordering view, not a substantive
tie-breaking rule.

Example:

```julia
pool = CandidatePool([:a, :b, :c])
w = WeakRank(pool, Dict(:a => 1, :b => 1))

to_perm(w)
```
""" to_perm

@doc raw"""
    to_pairwise(x, pool; policy)

Convert a rank ballot to dense pairwise-comparison form. The returned
`PairwiseDense` has `score(pw, i, j) == 1` when candidate ID `i` is above `j`,
`-1` for the reverse, `0` for ties or the diagonal, and `missing` for undefined
comparisons.

Strict rankings produce complete pairwise comparisons. Weak rankings use the
supplied `ExtensionPolicy`: for example, `NonePolicyMissing()` preserves
undefined comparisons involving unranked candidates, while
`BottomPolicyMissing()` treats present candidates as above unranked candidates.

Example:

```julia
pool = CandidatePool([:a, :b, :c])
w = WeakRank(pool, Dict(:a => 1, :b => 1))

pw = to_pairwise(w, pool; policy = BottomPolicyMissing())
dense(pw)
```
""" to_pairwise

@doc raw"""
    ordered_candidates(x, pool)

Return candidate symbols in the best-to-worst order induced by strict ballot
`x` and candidate pool `pool`.

Example:

```julia
pool = CandidatePool([:a, :b, :c])
x = StrictRank(pool, [:b, :a, :c])

ordered_candidates(x, pool)  # [:b, :a, :c]
```
""" ordered_candidates

@doc raw"""
    weakorder_symbol_groups(levels, pool)

Map weak-order candidate-ID groups to candidate symbols. Pass the levels
returned by `to_weakorder(w)`. Groups are ordered best to worst; equal symbols
inside a group are tied, and the final group may contain unranked candidates.

Example:

```julia
pool = CandidatePool([:a, :b, :c])
w = WeakRank(pool, Dict(:a => 1, :b => 1, :c => 2))

weakorder_symbol_groups(to_weakorder(w), pool)
```
""" weakorder_symbol_groups

@doc raw"""
    make_rank_bucket_linearizer(strategy)

Construct a weak-rank bucket linearizer used by `to_strict`/`linearize` to break
ties or order rank buckets without changing the weak-rank representation.
""" make_rank_bucket_linearizer

@doc raw"""
    compare_maybe(policy, ra, rb, i, j, ranks, pool)

Compare two possibly missing weak-rank entries under an `ExtensionPolicy`.
Returns `1`, `-1`, `0`, or `missing` according to the policy's treatment of
unranked candidates and ties.
""" compare_maybe

@doc raw"""
    PairwiseMajority(counts)

Pairwise majority-count aggregate for a profile. `counts[i,j]` is the mass
preferring candidate ID `i` to candidate ID `j`; margins are
`counts[i,j] - counts[j,i]`, and wins are the sign of that margin.

The count matrix records directional support and is not antisymmetric. The
margin matrix is antisymmetric, and a positive margin `[i,j]` means row
candidate `i` beats column candidate `j`.
""" PairwiseMajority

@doc raw"""
    ranking_signature(ranking)

Return the canonical tuple signature of a strict ranking, using candidate
symbols when a pool is available.
""" ranking_signature

@doc raw"""
    ranking_proportions(profile)

Return the empirical distribution over observed strict ranking types. For a
strict profile, each row has unit mass; for a weighted strict profile, stored
weights are normalized to proportions:

```math
p_r = \frac{w_r}{\sum_s w_s}.
```

The result is a `Dict{Tuple,Float64}` keyed by best-to-worst candidate-symbol
tuples. Empty or zero-mass inputs return an empty dictionary. This distribution
is the input to reversal, overlap, and effective ranking support measures.
""" ranking_proportions

@doc raw"""
    reversal_pairs(unique_rankings)

Pair observed strict ranking signatures with their exact reversals. For a
ranking

```math
r = (a_1,\ldots,a_m),
```

its paired opposite is `reverse(r) = (a_m,\ldots,a_1)`. The function returns
`(paired, unpaired)`, pairing each exact reversal at most once.

This helper does not count near-reversals or pairwise opposition without an
observed exact reversed ranking type.
""" reversal_pairs

@doc raw"""
    kendall_tau_distance(x, y)

Return the Kendall tau distance between two complete strict rankings. For two
strict rankings `x` and `y` on `m` alternatives,

```math
d_\tau(x,y) =
\left|\left\{\{a,b\}: x \text{ and } y \text{ order } a,b
\text{ differently}\right\}\right|.
```

The range is `0:binomial(m, 2)`. A value of `0` means identical orders; the
maximum `binomial(m, 2)` means exact reversal.

Example:

```julia
pool = CandidatePool([:a, :b, :c])
x = StrictRank(pool, [:a, :b, :c])
y = StrictRank(pool, [:c, :b, :a])

kendall_tau_distance(x, y)  # 3 for m = 3
```
""" kendall_tau_distance

@doc raw"""
    average_normalized_distance(profile, consensus)

Return average Kendall distance from profile ballots to a consensus ranking,
normalized by the maximum pair count:

```math
\bar d(p,c) =
\frac{1}{W\binom{m}{2}} \sum_i w_i d_\tau(r_i,c),
```

where `w_i = 1` for `Profile`, `w_i` is the stored weight for
`WeightedProfile`, and `W = sum_i w_i`. The value lies in `[0, 1]` for valid
strict profiles with at least two candidates. It is a dispersion or incoherence
around the proposed consensus.
""" average_normalized_distance

@doc raw"""
    axes_up_to_reversal(candidates)

Enumerate one representative from each linear-axis equivalence class modulo
reversal. For example, `(:a, :b, :c)` and `(:c, :b, :a)` define the same
single-peaked axis and only one representative is returned.

```julia
axes_up_to_reversal([:a, :b, :c])
```
""" axes_up_to_reversal

@doc raw"""
    is_single_peaked(ranking, axis)

Return whether a strict best-to-worst ranking is single-peaked on `axis`. Axes
are linear orders of candidates and are interpreted modulo reversal in summary
functions. The ranking and axis must contain the same candidates exactly once.

Example:

```julia
axis = [:a, :b, :c]
is_single_peaked([:a, :b, :c], axis)
is_single_peaked([:b, :a, :c], axis)
```
""" is_single_peaked

@doc raw"""
    single_peaked_rankings(axis)

Enumerate all strict rankings that are single-peaked on a fixed axis. For `m`
candidates there are `2^(m-1)` such rankings.

```julia
axis = (:a, :b, :c)
single_peaked_rankings(axis)
```
""" single_peaked_rankings

@doc raw"""
    single_peaked_distance(ranking, axis)

Return the Kendall distance from `ranking` to the nearest strict ranking that is
single-peaked on `axis`:

```math
\min_{s \in SP(a)} d_\tau(r,s).
```

The returned distance is an unnormalized discordant-pair count. Summary
functions normalize by `binomial(m, 2)`.
""" single_peaked_distance

@doc raw"""
    profile_distribution(profile; proportion_source=:auto)

Compress a strict `Profile` or `WeightedProfile` into observed ranking types and
normalized proportions. Weighted profiles use stored survey/profile weights
when `proportion_source = :survey_weight` or `:auto`; unweighted profiles use
row counts. The result is used by single-peakedness summaries to score ranking
support, not to sample individuals.
""" profile_distribution

@doc raw"""
    single_peakedness_summary(profile; kwargs...)

Compute deviation from single-peakedness over candidate axes. For each axis
`a`, let `SP(a)` be the set of strict rankings single-peaked on that axis,
`p_r` the observed ranking-type proportion, and

```math
d_a(r) = \min_{s \in SP(a)} \frac{d_\tau(r,s)}{\binom{m}{2}}.
```

The per-axis measures are

```math
L_0(a) = \sum_{r \notin SP(a)} p_r, \qquad
L_1(a) = \sum_r p_r d_a(r),
```

and

```math
L_{1,off}(a) =
\frac{\sum_{r \notin SP(a)} p_r d_a(r)}{L_0(a)}
```

when `L0(a) > 0`, otherwise `missing`. `L0` is off-axis mass, `L1` is
unconditional normalized Kendall distortion, and `L1_off_axis` conditions that
distortion on the off-axis part. Axes are considered modulo reversal.

Example:

```julia
pool = CandidatePool([:a, :b, :c])
p = Profile(pool, [
    StrictRank(pool, [:a, :b, :c]),
    StrictRank(pool, [:b, :a, :c]),
])

res = single_peakedness_summary(p; axes = [[:a, :b, :c]])
res.best_L0
```
""" single_peakedness_summary

@doc raw"""
    single_peakedness_L0(profile; kwargs...)

Return the best-axis off-axis mass `min_a L0(a)` from
`single_peakedness_summary`. `L0 = 0` means all observed ranking mass is
single-peaked on at least one evaluated axis.
""" single_peakedness_L0

@doc raw"""
    single_peakedness_L1(profile; kwargs...)

Return the best-axis unconditional normalized Kendall distortion `min_a L1(a)`
from `single_peakedness_summary`. This counts both how much mass is off-axis and
how far that mass is from the nearest on-axis ranking.
""" single_peakedness_L1

@doc raw"""
    single_peakedness_L1_off_axis(profile; kwargs...)

Return the best conditional off-axis distortion from `single_peakedness_summary`.
This is `L1` restricted to rankings not single-peaked on the selected axis and
is `missing` when the selected axis has no off-axis mass.
""" single_peakedness_L1_off_axis

@doc raw"""
    best_single_peaked_axes(profile; kwargs...)

Return the axis IDs minimizing `L0`, the off-axis support mass, in
`single_peakedness_summary`. These are support-optimal axes; they need not be
the same axes minimizing `L1` or `L1_off_axis`.
""" best_single_peaked_axes

@doc raw"""
    can_polarization(profile)

Return Can's pairwise-balance polarization statistic `Ψ`. For each unordered
candidate pair `{a,b}`, let `n_ab` be the profile mass ranking `a` above `b`,
`n_ba` the reverse mass, `n` the total mass, and `m` the number of candidates:

```math
\Psi(p) =
\frac{1}{n\binom{m}{2}}
\sum_{\{a,b\}}\left(n - |n_{ab} - n_{ba}|\right).
```

`Ψ = 0` under pairwise unanimity for every pair. `Ψ = 1` when every pairwise
contest is exactly balanced. The measure is pairwise: it detects pairwise
balance but does not identify whether disagreement is organized as exact
reversal pairs.

Examples:

```julia
pool = CandidatePool([:a, :b, :c])
unanimous = Profile(pool, [StrictRank(pool, [:a, :b, :c]) for _ in 1:3])
can_polarization(unanimous)  # 0.0

cycle = Profile(pool, [
    StrictRank(pool, [:a, :b, :c]),
    StrictRank(pool, [:b, :c, :a]),
    StrictRank(pool, [:c, :a, :b]),
])
can_polarization(cycle)
```
""" can_polarization

@doc raw"""
    total_reversal_component(profile)

Return the exact-reversal mass component `R`. Let `p_r` be the profile
proportion on strict ranking type `r`, and let `\bar r` denote the exact reverse
ranking. Then

```math
R(p) = \sum_{\{r,\bar r\}} 2\min(p_r, p_{\bar r}).
```

`R` measures the amount of mass that can be matched into exact reversed ranking
pairs. It is not a general disagreement index: pairwise cycles and near
reversals do not contribute unless both exact ranking types are present.

Example:

```julia
pool = CandidatePool([:a, :b, :c])
p = Profile(pool, [
    StrictRank(pool, [:a, :b, :c]),
    StrictRank(pool, [:c, :b, :a]),
])

total_reversal_component(p)
```
""" total_reversal_component

@doc raw"""
    reversal_hhi(profile)

Return reversal concentration `κ`, the Herfindahl-Hirschman index of positive
local exact-reversal masses. If `v_i` is the local mass in reversal pair `i` and
`R = sum_i v_i`, define `s_i = v_i / R`; then

```math
\kappa(p) = \sum_i s_i^2.
```

For positive reversal mass, `κ` is high when reversal mass is concentrated in a
single exact opposition pair and lower when it is dispersed. The implementation
returns `0.0` when there is no positive exact-reversal mass.

```julia
pool = CandidatePool([:a, :b, :c])
p = Profile(pool, [
    StrictRank(pool, [:a, :b, :c]),
    StrictRank(pool, [:c, :b, :a]),
])

reversal_hhi(p)
```
""" reversal_hhi

@doc raw"""
    reversal_geometric(profile)

Return the geometric exact-reversal index

```math
G_R(p) = \sqrt{R(p)\kappa(p)},
```

where `R` is `total_reversal_component(p)` and `κ` is `reversal_hhi(p)`. The
index combines amount and concentration: high values require both substantial
exact-reversal mass and concentration of that mass in few reversal pairs. It
returns `0.0` when there is no positive exact-reversal mass.

```julia
pool = CandidatePool([:a, :b, :c])
p = Profile(pool, [
    StrictRank(pool, [:a, :b, :c]),
    StrictRank(pool, [:c, :b, :a]),
])

reversal_geometric(p)
```
""" reversal_geometric

@doc raw"""
    effective_observed_rankings(profile)

Return the inverse-HHI effective number of observed strict ranking types:

```math
E_O = \left(\sum_r p_r^2\right)^{-1}.
```

`E_O = 1` when all mass is on one observed ranking type, and larger values mean
mass is spread over more ranking types. Empty or zero-mass inputs throw.
""" effective_observed_rankings

@doc raw"""
    effective_reversal_rankings(profile)

Return the inverse-HHI effective number of exact reversal pairs carrying
positive local reversal mass. With

```math
v_q = 2\min(p_r,p_{\bar r}), \qquad R = \sum_q v_q,
```

the statistic is

```math
E_R = \left(\sum_q (v_q/R)^2\right)^{-1}.
```

It returns `0.0` when there is no exact reversal mass. Larger values mean the
reversal component is spread across more exact opposition pairs.
""" effective_reversal_rankings

@doc raw"""
    effective_reversal_ranking_diagnostics(profile)

Return a named tuple comparing effective exact-reversal support with effective
observed ranking support. Fields include `ENRP = effective_reversal_rankings`,
`EO = effective_observed_rankings`, and their ratio.

Use this to distinguish a profile with many observed ranking types from one
whose opposition is organized into many effective exact-reversal pairs.
""" effective_reversal_ranking_diagnostics

@doc raw"""
    ranking_support_diagnostics(profile; kwargs...)

Return descriptive diagnostics for observed strict-ranking support: possible
rankings `m!`, number of observed ranking types, singleton support, maximum
ranking mass, inverse-HHI effective support `EO`, and saturation/sparsity
summaries.

These diagnostics describe the empirical support of a profile; they are not a
polarization index by themselves.
""" ranking_support_diagnostics

@doc raw"""
    consensus_kendall(profile; kwargs...)

Compute a Kendall/Kemeny consensus over complete strict rankings. For strict
rankings `r_i` with masses `w_i`, it searches all strict orders `c` and solves

```math
\min_c \sum_i w_i d_\tau(r_i,c).
```

The returned `ConsensusResult` stores the selected consensus ballot, the total
objective, the average normalized distance, and all tied minimizers. Empty or
zero-mass profiles throw, and at least two candidates are required.

Example:

```julia
pool = CandidatePool([:a, :b, :c])
p = Profile(pool, [
    StrictRank(pool, [:a, :b, :c]),
    StrictRank(pool, [:a, :c, :b]),
    StrictRank(pool, [:b, :a, :c]),
])

res = consensus_kendall(p)
pretty(res.consensus_ballot, pool)
```
""" consensus_kendall

@doc raw"""
    get_consensus_ranking(profile; kwargs...)

Return the selected Kendall consensus as `(ordered_symbols, ranking_dict)`. This
is a convenience projection of `consensus_kendall`, not a different consensus
rule. Tie handling follows `consensus_kendall`.

```julia
ordered, ranks = get_consensus_ranking(p)
```
""" get_consensus_ranking

@doc raw"""
    kendall_tau_dict(r1, r2)

Dictionary-facing Kendall tau distance for complete ranking dictionaries over a
common candidate set. Each dictionary maps candidate symbols to strict rank
positions, and the result is the discordant-pair count in
`0:binomial(m, 2)`.
""" kendall_tau_dict

@doc raw"""
    consensus_for_group(subdf; kwargs...)

Compute one group's Kendall/Kemeny consensus using `strict_profile` input
coercion. The return value includes the consensus ranking dictionary,
permutation, full `ConsensusResult`, set of minimizing consensuses, and tie
diagnostics. This is the group consensus `ρ_g` used by coherence, divergence,
overlap, and separation summaries.
""" consensus_for_group

@doc raw"""
    group_avg_distance(subdf; kwargs...)

Compute a group's average normalized distance `W_g` to its Kendall consensus and
coherence `C_g = 1 - W_g`:

```math
W_g = \sum_r p_g(r)\frac{d_\tau(r,\rho_g)}{\binom{m}{2}},
\qquad C_g = 1 - W_g.
```

`C_g` is within-group coherence around the group consensus, not between-group
separation.
""" group_avg_distance

@doc raw"""
    weighted_coherence(results_distance, proportion_map, key)

Aggregate group coherences into aggregate quantity `C`:

```math
C = \sum_g \pi_g C_g,
\qquad C_g = 1 - W_g.
```

`results_distance` supplies `group_coherence`, while `proportion_map` supplies
group shares `π_g`. The helper aggregates the provided rows; it does not sample
individuals.
""" weighted_coherence

@doc raw"""
    pairwise_group_divergence(profile_i, consensus_j, m)

Return directed normalized Kendall divergence from group `i`'s profile to group
`j`'s consensus:

```math
D_{i\to j} = \sum_r p_i(r)\frac{d_\tau(r,\rho_j)}{\binom{m}{2}}.
```

The object is directed: distance from group `i` to group `j`'s consensus need
not equal the reverse. The function aggregates over the whole group/profile
with profile weights where applicable; it does not sample individuals.
""" pairwise_group_divergence

@doc raw"""
    pairwise_group_overlap(profile_g, profile_h)

Return exact ranking-distribution overlap between two groups:

```math
O(g,h) = \sum_r \min(p_g(r), p_h(r)).
```

`O = 1` for identical empirical ranking distributions and `O = 0` for disjoint
observed ranking support. This is distributional overlap, independent of where
the group consensuses sit in Kendall space.
""" pairwise_group_overlap

@doc raw"""
    smoothed_overlap(profile_g, profile_h)

Return radius-1 Kendall-smoothed overlap. Each observed ranking spreads its mass
uniformly over itself and its adjacent-swap Kendall neighbors before exact
overlap is computed. This treats near-identical rankings as partial shared
support while preserving the same group aggregation logic as `overall_overlap`.
""" smoothed_overlap

@doc raw"""
    pairwise_group_median_distance(consensus_g, consensus_h, pool)

Return normalized Kendall distance between group consensus sets. When a group
has tied Kemeny minimizers, the distance averages across all pairs of minimizers:

```math
D_{median}(g,h) = \frac{1}{|M_g||M_h|}\sum_{c\in M_g}\sum_{d\in M_h}
\frac{d_\tau(c,d)}{\binom{m}{2}}.
```

This is an undirected consensus-to-consensus distance.
""" pairwise_group_median_distance

@doc raw"""
    pairwise_group_separation(profile_g, consensus_g, profile_h, consensus_h)

Return overlap-adjusted pairwise group separation:

```math
Sep(g,h) = D_{median}(g,h)(1 - O(g,h)).
```

Groups are separated when their consensus sets are far apart and their exact
ranking distributions do not strongly overlap.
""" pairwise_group_separation

@doc raw"""
    overall_divergence(group_profiles, consensus_map)

Aggregate directed cross-group divergence as quantity `D`:

```math
D =
\frac{1}{K-1}
\sum_i \pi_i
\sum_{j \ne i}
\frac{1}{n_i}
\sum_{x \in G_i}
\frac{d_\tau(x,\rho_j)}{\binom{m}{2}},
```

where `ρ_j` is group `j`'s Kendall consensus and `π_i` is group `i`'s mass
share. The implementation sweeps or aggregates over the whole group/profile,
using profile weights where applicable; it does not sample individuals.
""" overall_divergence

@doc raw"""
    overall_overlap(group_profiles)

Aggregate exact pairwise group overlaps over unordered group pairs using
population-pair weights:

```math
\omega_{gh} = \frac{2\pi_g\pi_h}{1 - \sum_r \pi_r^2},
\qquad O = \sum_{g<h}\omega_{gh}O(g,h).
```

This summarizes shared ranking support between groups.
""" overall_overlap

@doc raw"""
    overall_overlap_smoothed(group_profiles)

Aggregate radius-1 Kendall-smoothed pairwise overlaps using the same pair
weights as `overall_overlap`. Only the pairwise overlap object changes from
exact support overlap to smoothed local support overlap.
""" overall_overlap_smoothed

@doc raw"""
    overall_divergence_median(group_profiles, consensus_map)

Aggregate pairwise median-set distances over unordered group pairs:

```math
D_{median} = \sum_{g<h}\omega_{gh}D_{median}(g,h),
```

where `ω_gh` are the population-pair weights from `overall_overlap`. This is a
symmetric consensus-set distance, unlike directed `overall_divergence`.
""" overall_divergence_median

@doc raw"""
    overall_separation(group_profiles, consensus_map)

Aggregate overlap-adjusted pairwise separations over unordered group pairs:

```math
Sep = \sum_{g<h}\omega_{gh}Sep(g,h),
\qquad Sep(g,h)=D_{median}(g,h)(1-O(g,h)).
```

This combines consensus distance with exact distributional overlap.
""" overall_separation

@doc raw"""
    grouped_gsep(C, Sep)

Return `sqrt(C * Sep)` after validating both inputs on the unit interval. This
combines within-group coherence `C` and overlap-adjusted between-group
separation `Sep`.
""" grouped_gsep

@doc raw"""
    S(C, D)

Return the excess-divergence component from the consensus-relative `C`-`D`
decomposition:

```math
S(C,D) = D - \frac{1-C}{2}.
```

For admissible profile-derived inputs, `D >= (1-C)/2`, so `S >= 0`. The raw
numeric function may return a negative value only when arbitrary inputs violate
that theoretical bound; treat those inputs as invalid for this diagnostic.
`S_old` is the separate legacy support-separation statistic.
""" S

@doc raw"""
    normalized_consensus_separation(W, D; atol=1e-10)

Return the normalized excess-divergence ratio

```math
E = 1 - \frac{W}{D},
```

with `D == 0` mapped to `0.0` and small floating-point excursions clamped. In
the `C`-`D` decomposition, `W = (1-C)/2`; when `D > 0`, `E = S/D`.
""" normalized_consensus_separation

@doc raw"""
    consensus_excess_separation(W, D; kwargs...)

Alias in the derived `C`-`D` decomposition family for
`normalized_consensus_separation(W, D; kwargs...)`.
""" consensus_excess_separation

@doc raw"""
    group_E(W, D; kwargs...)

Group-level alias in the derived `C`-`D` decomposition family for
`E = 1 - W/D`, where `W` is within-group dispersion and `D` is the relevant
directed outgroup distance.
""" group_E

@doc raw"""
    aggregate_E(W, D; kwargs...)

Aggregate-level alias in the derived `C`-`D` decomposition family for
`E = 1 - W/D`.
""" aggregate_E

@doc raw"""
    E(W, D; kwargs...)

Shorthand for the normalized excess-divergence ratio `E = 1 - W/D`. Use
`normalized_consensus_separation` for the explicit name.
""" E

@doc raw"""
    S_old(group_profiles, group_sizes)

Legacy support-separation statistic based on cross-group normalized Kendall
distance minus mean within-group dispersion for unordered group pairs.
""" S_old

@doc raw"""
    bootstrap_group_metrics(bt_profiles, demo; kwargs...)

Compute group metrics over bootstrap profile draws and return the bootstrap
summary table produced by the group-metric pipeline.
""" bootstrap_group_metrics

# ------------------------------------------------------------
# Exports (unified public surface)
# ------------------------------------------------------------

# Core pool API
export CandidatePool, labels, getlabel, candid, candidates, to_cmap

# Ballots & accessors
export StrictRank, StrictRankDyn, WeakRank, WeakRankDyn
export rank, prefers, indifferent, asdict
export perm, ranks
export to_perm, to_weakorder, ordered_candidates, weakorder_symbol_groups
export to_strict, make_rank_bucket_linearizer
export AbstractWeakOrderLinearizer, PatternConditionalLinearizer
export linearize

# Policies and pairwise (dense wrapper)
export ExtensionPolicy, BottomPolicyMissing, NonePolicyMissing, compare_maybe
export AbstractPairwise, PairwiseDense, to_pairwise
export Pairwise  # compat

# Triangular pairwise ballots (static/mutable/view) + helpers
export PairwiseTriangularStatic, PairwiseTriangularMutable, PairwiseTriangularView
export PairwiseBallotStatic, PairwiseBallotMutable, PairwiseBallotView  # compat
export pairwise_view, isdefined, score, pairwise_dense, dense
export is_complete, is_strict, is_weak_order, is_transitive

# Profiles
export Profile, WeightedProfile, nballots, weights, total_weight, validate
export resample_indices, bootstrap, bootstrap_counts
export restrict
export profile_build_meta

# Annotated profiles and tabular adapters
export AnnotatedProfile
export annotated_profile, dataframe_to_annotated_profile, annotated_profile_to_dataframe
export linearize_annotated_profile, subset_annotated_profile, profile_to_ranking_dicts

# Tabular-score profile construction and pattern summaries
export humanize_candidate_name, canonical_candidate_key
export candidate_display_symbols, guess_weight_col, resolve_candidate_cols_from_set
export candidate_missingness_table
export normalize_numeric_score, normalize_nonnegative_weight
export row_to_weak_rank_from_scores, build_profile_from_scores
export profile_pattern_proportions
export ranked_count, has_ties
export ranking_type_support, ranking_type_template
export profile_ranksize_summary, profile_ranking_type_proportions
export pretty_print_profile_patterns
export pretty_print_ranksize_summary, pretty_print_ranking_type_proportions

# Aggregation procedures
export PairwiseMajority, pairwise_majority
export pairwise_majority_counts, pairwise_majority_margins, pairwise_majority_wins

# Strict-profile measures
export ranking_signature, ranking_proportions, reversal_pairs
export kendall_tau_distance, average_normalized_distance
export axes_up_to_reversal, is_single_peaked, single_peaked_rankings
export single_peaked_distance, profile_distribution
export SinglePeakedAxisSummary, SinglePeakedSupportClassification, SinglePeakednessResult
export single_peakedness_summary, single_peakedness_L0, single_peakedness_L1
export single_peakedness_L1_off_axis
export best_single_peaked_axes
export can_polarization, total_reversal_component, reversal_hhi, reversal_geometric
export effective_observed_rankings, effective_reversal_rankings
export effective_reversal_ranking_diagnostics, ranking_support_diagnostics
export VoterTypeBasis, voter_type_basis, voter_type_masses
export MajoritySupportEdge, MajorityGraphSupportResult, majority_graph_support
export majority_edges_table, voter_type_table, edge_support_table, edge_overlap_table, core_table
export edge_effective_type_table, edge_effective_type_composition_table
export core_effective_type_table, reverse_core_effective_type_table
export core_effective_type_composition_table, effective_type_diagnostics
export support_core_effective_composition_table, support_core_above_threshold_type_table
export reverse_core_effective_composition_table, reverse_core_above_threshold_type_table
export edge_effective_composition_table, edge_above_threshold_type_table
export countergraph_summary_table
export boundary_distance_to_reverse, amenability_weight
export type_breaker_table, minimal_breaking_coalition_table
export GroupMajorityGraphSupportResult, group_majority_graph_support
export group_edge_power_table, group_breaker_table, group_anchor_table
export MajorityGraphRoleThresholds
export voter_type_role_table, edge_type_role_table
export role_mass_summary, primary_role_mass_summary, selected_edge_role_summary
export group_role_table, group_primary_role_table, group_role_power_table
export graph_role_summary, group_graph_role_summary
export plurality_scores_table, pairwise_vs_plurality_decomposition_table
export candidate_position_by_current_first_table, one_swap_target_table
export plurality_swing_value_table, exact_type_switch_table, group_target_switch_table
export strict_profile, ConsensusResult
export consensus_kendall, get_consensus_ranking, kendall_tau_dict
export consensus_for_group, group_avg_distance, weighted_coherence
export pairwise_group_divergence, overall_divergence, overall_divergences
export pairwise_group_overlap, pairwise_group_median_distance, pairwise_group_separation
export smoothed_overlap, overall_overlap, overall_overlap_smoothed
export overall_overlaps, overall_overlaps_smoothed
export overall_divergence_median, overall_divergences_median
export overall_separation, overall_separations, grouped_gsep
export normalized_consensus_separation, consensus_excess_separation
export group_E, aggregate_E, E
export S, S_old
export compute_group_metrics, bootstrap_group_metrics

# Dynamics / mutable strict
export StrictRankMutable, swap_positions!, swap_ids!, swap_and_update_pairwise!

# Display helpers
export StrictRankView, WeakOrderView, pretty, show_pairwise_preference_table_color, pretty_pairwise
export pretty_profile_table, show_profile_table_color
export pretty_pairwise_majority_table, pretty_pairwise_majority
export pretty_pairwise_majority_counts, pretty_pairwise_majority_margins
export show_pairwise_majority_table_color

end # module
