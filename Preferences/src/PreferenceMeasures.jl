# PreferenceMeasures.jl

raw"""
    ranking_signature(x::StrictRank, pool::CandidatePool)

Return the canonical strict-ranking representation used by the ranking-type
measures. For a strict ballot `x` over candidate pool `pool`, the signature is

```math
r(x) = (a_1,\ldots,a_m),
```

where `a_1 \succ_x \cdots \succ_x a_m` and each `a_i` is the candidate symbol
from `pool`.

Inputs are complete `StrictRank` ballots and the matching `CandidatePool`.
The returned value is a `Tuple` of candidate symbols, so equal tuples denote the
same observed strict ranking type. Empty profiles are not handled here; callers
that aggregate signatures define their own zero-mass conventions.

Interpretation: this removes storage details such as candidate IDs and gives the
canonical ranking type on which support, reversal, and effective-number
statistics are computed.
"""
@inline function ranking_signature(x::StrictRank, pool::CandidatePool)
    return Tuple(ordered_candidates(x, pool))
end

raw"""
    reversal_pairs(unique_rankings)

Pair observed ranking types with their exact reversals. Given a vector of
ranking signatures `r`, this searches for pairs

```math
r = (a_1,\ldots,a_m), \qquad \operatorname{rev}(r) = (a_m,\ldots,a_1).
```

Inputs are vectors of `Tuple` ranking signatures, usually the observed support
order returned by the internal ranking-mass compression. The function returns
`(paired, unpaired)`, where each paired row is
`(r, i, reverse(r), j)` and each unpaired row is `(r, i)`.

There is no normalization in this helper and no special empty-profile behavior:
an empty input returns empty paired and unpaired vectors. Interpretation: only
exact ranking reversals are counted, so near-reversals or pairwise opposition
without exact reversed ranking types are excluded.
"""
function reversal_pairs(unique_rankings::AbstractVector{<:Tuple})
    paired_accum = Tuple[]
    unpaired_accum = Tuple[]
    paired_indices = Set{Int}()

    for (i, ranking) in enumerate(unique_rankings)
        i in paired_indices && continue

        rev_ranking = reverse(ranking)
        found_index = nothing

        for j in (i + 1):length(unique_rankings)
            j in paired_indices && continue
            if unique_rankings[j] == rev_ranking
                found_index = j
                break
            end
        end

        if isnothing(found_index)
            push!(unpaired_accum, (ranking, i))
        else
            push!(paired_accum, (ranking, i, rev_ranking, found_index))
            push!(paired_indices, i)
            push!(paired_indices, found_index)
        end
    end

    return paired_accum, unpaired_accum
end

function _ranking_masses(p::Profile{<:StrictRank})
    masses = Dict{Tuple,Float64}()
    order = Tuple[]

    for ballot in p.ballots
        sig = ranking_signature(ballot, p.pool)
        if !haskey(masses, sig)
            push!(order, sig)
        end
        masses[sig] = get(masses, sig, 0.0) + 1.0
    end

    return masses, order, Float64(nballots(p))
end

function _ranking_masses(p::WeightedProfile{<:StrictRank})
    masses = Dict{Tuple,Float64}()
    order = Tuple[]

    @inbounds for (ballot, weight) in zip(p.ballots, p.weights)
        sig = ranking_signature(ballot, p.pool)
        if !haskey(masses, sig)
            push!(order, sig)
        end
        masses[sig] = get(masses, sig, 0.0) + Float64(weight)
    end

    return masses, order, Float64(total_weight(p))
end

raw"""
    ranking_proportions(p)

Return the empirical distribution over observed strict ranking types:

```math
p_r = \frac{w_r}{\sum_s w_s},
```

where `w_r` is the row count for a `Profile{<:StrictRank}` and the survey-weight
sum for a `WeightedProfile{<:StrictRank}`.

Inputs are strict `Profile` or `WeightedProfile` objects. The result is a
`Dict{Tuple,Float64}` keyed by `ranking_signature` tuples and normalized to sum
to `1` when total mass is positive. If the profile has no ballots or the
weighted profile has zero total mass, this returns an empty dictionary.

Interpretation: this is the observed ranking-type support distribution used by
reversal, overlap, and effective-number measures.
"""
function ranking_proportions(p::Union{Profile{<:StrictRank},WeightedProfile{<:StrictRank}})
    masses, _, total = _ranking_masses(p)
    total > 0 || return Dict{Tuple,Float64}()
    return Dict(sig => mass / total for (sig, mass) in masses)
end

function _canonical_ranking_signature(ranking::Tuple)
    return ranking
end

function _canonical_ranking_signature(ranking::AbstractDict)
    pairs = collect(ranking)
    sort!(pairs; by = pair -> (last(pair), string(first(pair))))
    return Tuple(first(pair) for pair in pairs)
end

function _canonical_ranking_signature(ranking::AbstractVector)
    return Tuple(ranking)
end

function _ranking_masses(rankings::AbstractVector)
    masses = Dict{Tuple,Float64}()
    order = Tuple[]

    for ranking in rankings
        sig = _canonical_ranking_signature(ranking)
        if !haskey(masses, sig)
            push!(order, sig)
        end
        masses[sig] = get(masses, sig, 0.0) + 1.0
    end

    return masses, order, Float64(length(rankings))
end

function _local_reversal_values(p::Union{Profile{<:StrictRank},WeightedProfile{<:StrictRank}})
    masses, order, total = _ranking_masses(p)
    total > 0 || return Float64[]

    paired, _ = reversal_pairs(order)
    values = Float64[]
    sizehint!(values, length(paired))

    for pair in paired
        sig = pair[1]
        rev = pair[3]
        push!(values, 2.0 * min(masses[sig], masses[rev]) / total)
    end

    return values
end

function _local_reversal_values(rankings::AbstractVector)
    masses, order, total = _ranking_masses(rankings)
    total > 0 || return Float64[]

    paired, _ = reversal_pairs(order)
    values = Float64[]
    sizehint!(values, length(paired))

    for pair in paired
        sig = pair[1]
        rev = pair[3]
        push!(values, 2.0 * min(masses[sig], masses[rev]) / total)
    end

    return values
end

_positive_mass_error() = throw(ArgumentError("Profile must contain positive mass"))

function _effective_observed_rankings(masses::Dict{Tuple,Float64}, total::Real)
    total > 0 || _positive_mass_error()
    hhi = sum((mass / total)^2 for mass in values(masses))
    return 1.0 / hhi
end

raw"""
    effective_observed_rankings(p)
    effective_observed_rankings(rankings)

Return the inverse Herfindahl-Hirschman index over observed ranking masses:

```math
E_O = \left(\sum_r p_r^2\right)^{-1}.
```

Inputs are strict `Profile`/`WeightedProfile` objects or a vector of rankings.
Vector entries may be tuples, ordered vectors, or dictionaries mapping candidate
symbols to rank positions; vector inputs are unweighted. The measure lies in
`[1, n_r]` for positive mass, where `n_r` is the number of observed ranking
types. Empty or zero-mass inputs throw `ArgumentError`.

Interpretation: larger values mean ranking mass is spread over more observed
strict ranking types; a value of `1` means all mass is on one ranking type.
"""
function effective_observed_rankings(p::Union{Profile{<:StrictRank},WeightedProfile{<:StrictRank}})
    masses, _, total = _ranking_masses(p)
    return _effective_observed_rankings(masses, total)
end

function effective_observed_rankings(rankings::AbstractVector)
    masses, _, total = _ranking_masses(rankings)
    return _effective_observed_rankings(masses, total)
end

function _effective_reversal_rankings(values::AbstractVector{<:Real})
    total = sum(values)
    # Same zero-reversal convention as reversal_hhi/reversal_geometric.
    total == 0.0 && return 0.0
    hhi = sum((value / total)^2 for value in values)
    return 1.0 / hhi
end

raw"""
    effective_reversal_rankings(p)
    effective_reversal_rankings(rankings)

Return the inverse HHI over local exact-reversal masses. For each observed
reversal pair `q = {r, reverse(r)}`, let

```math
v_q = 2\min(p_r, p_{reverse(r)}), \qquad R = \sum_q v_q.
```

The statistic is

```math
E_R = \left(\sum_q (v_q/R)^2\right)^{-1}.
```

Inputs are strict `Profile`/`WeightedProfile` objects or unweighted vectors of
rankings accepted by `effective_observed_rankings`. For positive reversal mass,
the range is `[1, n_q]`, where `n_q` is the number of reversal pairs with
positive local mass. If the input is empty, zero-mass, or has no exact reversal
pairs, the implementation returns `0.0`.

Interpretation: larger values mean reversal mass is dispersed across many
opposed ranking pairs rather than concentrated in one reversal cleavage.
"""
function effective_reversal_rankings(p::Union{Profile{<:StrictRank},WeightedProfile{<:StrictRank}})
    return _effective_reversal_rankings(_local_reversal_values(p))
end

function effective_reversal_rankings(rankings::AbstractVector)
    return _effective_reversal_rankings(_local_reversal_values(rankings))
end

raw"""
    effective_reversal_ranking_diagnostics(p)
    effective_reversal_ranking_diagnostics(rankings)

Return effective-number diagnostics for ranking support and exact reversals. The
returned named tuple contains `ENRP = effective_reversal_rankings(...)`,
`EO = effective_observed_rankings(...)`, and
`reversal_to_ranking_effective_ratio = ENRP / EO`.

Inputs are the same as `effective_observed_rankings`: strict profiles, weighted
strict profiles, or unweighted vectors of ranking representations. `EO` requires
positive observed mass and throws on empty or zero-mass inputs; `ENRP` follows
the zero-reversal convention and returns `0.0` when there is no reversal mass.

Interpretation: the ratio compares how many effective exact-reversal cleavages
are present relative to the effective number of observed ranking types.
"""
function effective_reversal_ranking_diagnostics(
    p::Union{Profile{<:StrictRank},WeightedProfile{<:StrictRank}},
)
    ENRP = effective_reversal_rankings(p)
    EO = effective_observed_rankings(p)
    return (
        ENRP = ENRP,
        EO = EO,
        reversal_to_ranking_effective_ratio = EO == 0.0 ? NaN : ENRP / EO,
    )
end

function effective_reversal_ranking_diagnostics(rankings::AbstractVector)
    ENRP = effective_reversal_rankings(rankings)
    EO = effective_observed_rankings(rankings)
    return (
        ENRP = ENRP,
        EO = EO,
        reversal_to_ranking_effective_ratio = EO == 0.0 ? NaN : ENRP / EO,
    )
end

possible_rankings_count(m::Integer) = factorial(Int(m))

function _ranking_support_diagnostics(masses::Dict{Tuple,Float64},
                                      total::Real,
                                      m::Integer)
    total > 0 || _positive_mass_error()
    possible = possible_rankings_count(m)
    n_unique = length(masses)
    singleton_rankings = count(==(1.0), values(masses))
    singleton_observation_count = singleton_rankings
    proportions = [mass / total for mass in values(masses)]
    EO = _effective_observed_rankings(masses, total)

    return (
        n_observations = isinteger(total) ? Int(total) : Float64(total),
        m = Int(m),
        possible_rankings = possible,
        n_unique_rankings = n_unique,
        unique_share_of_possible = n_unique / possible,
        unique_share_of_observations = n_unique / total,
        singleton_rankings = singleton_rankings,
        singleton_observation_count = singleton_observation_count,
        singleton_share_of_unique = singleton_rankings / n_unique,
        singleton_share_of_observations = singleton_observation_count / total,
        max_ranking_mass = maximum(proportions),
        EO = EO,
        effective_share_of_possible = EO / possible,
        support_saturation = n_unique / min(total, possible),
        sparsity_pressure = possible / total,
    )
end

raw"""
    ranking_support_diagnostics(p; m=length(p.pool))
    ranking_support_diagnostics(rankings; m=nothing)

Return descriptive diagnostics for strict-ranking support. Let `p_r` be the
observed ranking proportions and `m` the number of candidates. The output
includes the number of possible rankings `m!`, observed support size, singleton
counts, maximum ranking mass, inverse-HHI effective support `EO`, and support
saturation summaries.

Inputs are strict `Profile`/`WeightedProfile` objects or unweighted vectors of
ranking representations. Vector inputs infer `m` from the first ranking unless
the collection is empty, in which case `m` must be provided and positive. Empty
or zero-mass inputs throw `ArgumentError`.

Interpretation: these diagnostics describe whether the profile is concentrated
on a few ranking types, sparse relative to the full `m!` domain, or saturated
relative to the observed sample size.
"""
function ranking_support_diagnostics(
    p::Union{Profile{<:StrictRank},WeightedProfile{<:StrictRank}};
    m::Integer = length(p.pool),
)
    masses, _, total = _ranking_masses(p)
    return _ranking_support_diagnostics(masses, total, m)
end

function ranking_support_diagnostics(rankings::AbstractVector; m::Union{Integer,Nothing} = nothing)
    masses, order, total = _ranking_masses(rankings)
    inferred_m = isnothing(m) ? (isempty(order) ? 0 : length(first(order))) : Int(m)
    inferred_m > 0 || throw(ArgumentError("m must be provided for empty ranking collections"))
    return _ranking_support_diagnostics(masses, total, inferred_m)
end

raw"""
    kendall_tau_distance(x::StrictRank, y::StrictRank)

Return the Kendall tau distance between two strict rankings:

```math
d_K(x,y) =
\#\{\{a,b\}: a \succ_x b \text{ and } b \succ_y a\}.
```

Inputs are complete `StrictRank` ballots over the same candidate-id universe and
with the same length. The range is `0:binomial(m, 2)`, where `m` is the number
of ranked candidates. Candidate-set compatibility is enforced by length and the
strict-rank permutation representation; mismatched lengths throw
`ArgumentError`.

Interpretation: `0` means identical strict orders and `binomial(m, 2)` means one
order is the exact reversal of the other.
"""
function kendall_tau_distance(x::StrictRank, y::StrictRank)
    px = to_perm(x)
    py = to_perm(y)
    length(px) == length(py) || throw(ArgumentError("Ballots must have the same size"))

    n = length(px)
    pos = zeros(Int, n)
    @inbounds for (rank, id) in enumerate(py)
        pos[id] = rank
    end

    d = 0
    @inbounds for i in 1:(n - 1)
        pi = pos[px[i]]
        for j in (i + 1):n
            pj = pos[px[j]]
            pi > pj && (d += 1)
        end
    end

    return d
end

raw"""
    average_normalized_distance(p, consensus::StrictRank)

Return the mean normalized Kendall distance from profile ballots to a consensus
strict ranking:

```math
\bar d(p,c) =
\frac{1}{W\binom{m}{2}} \sum_i w_i d_K(r_i,c),
```

where `w_i = 1` for `Profile` and the stored weight for `WeightedProfile`, and
`W = \sum_i w_i`.

Inputs are `Profile{<:StrictRank}` or `WeightedProfile{<:StrictRank}` and a
`StrictRank` consensus. The result is in `[0, 1]` when the consensus is over the
same candidate universe. Empty profiles and zero-total-weight profiles throw
`ArgumentError`; at least two candidates are required.

Interpretation: this is a normalized within-profile dispersion or incoherence
around a proposed strict consensus, with `0` indicating perfect agreement.
"""
function average_normalized_distance(p::Profile{<:StrictRank}, consensus::StrictRank)
    n = nballots(p)
    n > 0 || throw(ArgumentError("Profile must contain at least one ballot"))

    m = length(p.pool)
    m ≥ 2 || throw(ArgumentError("At least two candidates are required"))
    norm_factor = binomial(m, 2)

    total = 0.0
    @inbounds for ballot in p.ballots
        total += kendall_tau_distance(ballot, consensus)
    end

    return total / (n * norm_factor)
end

function average_normalized_distance(p::WeightedProfile{<:StrictRank}, consensus::StrictRank)
    total = Float64(total_weight(p))
    total > 0 || throw(ArgumentError("WeightedProfile total weight must be positive"))

    m = length(p.pool)
    m ≥ 2 || throw(ArgumentError("At least two candidates are required"))
    norm_factor = binomial(m, 2)

    dist_mass = 0.0
    @inbounds for (ballot, weight) in zip(p.ballots, p.weights)
        dist_mass += Float64(weight) * kendall_tau_distance(ballot, consensus)
    end

    return dist_mass / (total * norm_factor)
end

function _pairwise_preference_counts(p::Profile{<:StrictRank})
    n = length(p.pool)
    counts = zeros(Float64, n, n)

    @inbounds for ballot in p.ballots
        perm = to_perm(ballot)
        for pos_i in 1:(n - 1)
            i = perm[pos_i]
            for pos_j in (pos_i + 1):n
                j = perm[pos_j]
                counts[i, j] += 1.0
            end
        end
    end

    return counts
end

function _pairwise_preference_counts(p::WeightedProfile{<:StrictRank})
    n = length(p.pool)
    counts = zeros(Float64, n, n)

    @inbounds for (ballot, weight) in zip(p.ballots, p.weights)
        perm = to_perm(ballot)
        w = Float64(weight)
        for pos_i in 1:(n - 1)
            i = perm[pos_i]
            for pos_j in (pos_i + 1):n
                j = perm[pos_j]
                counts[i, j] += w
            end
        end
    end

    return counts
end

raw"""
    can_polarization(p)

Return the pairwise candidate-balance polarization index `Ψ`. For each
unordered candidate pair `{a,b}`, let `n_{ab}` be the mass ranking `a` above
`b`, `n_{ba}` the reverse mass, and `W` total profile mass. Then

```math
\Psi =
\frac{1}{W\binom{m}{2}}
\sum_{a<b} \left(W - |n_{ab} - n_{ba}|\right).
```

Inputs are strict `Profile` or `WeightedProfile` objects. The statistic lies in
`[0, 1]`: `0` when every candidate pair is unanimously ordered in one direction,
and `1` when every pair is exactly balanced. Empty, zero-mass, or
single-candidate profiles throw `ArgumentError`.

Interpretation: `Ψ` measures pairwise balance across all unordered candidate
pairs and does not require exact reversed ranking types.
"""
function can_polarization(p::Union{Profile{<:StrictRank},WeightedProfile{<:StrictRank}})
    total = p isa WeightedProfile ? Float64(total_weight(p)) : Float64(nballots(p))
    total > 0 || throw(ArgumentError("Profile must contain positive mass"))

    m = length(p.pool)
    m ≥ 2 || throw(ArgumentError("At least two candidates are required"))
    pair_count = (m * (m - 1)) / 2

    counts = _pairwise_preference_counts(p)
    score = 0.0

    @inbounds for i in 1:(m - 1)
        for j in (i + 1):m
            dab = abs(counts[i, j] - counts[j, i])
            score += total - dab
        end
    end

    return score / (total * pair_count)
end

raw"""
    total_reversal_component(p)

Return the total exact-reversal component `R`. For each exact reversal pair
`q = {r, reverse(r)}` in the observed ranking support, define local reversal
mass

```math
v_q = 2\min(p_r, p_{reverse(r)}).
```

Then

```math
R = \sum_q v_q.
```

Inputs are strict `Profile` or `WeightedProfile` objects. The statistic is
normalized to `[0, 1]` because `p_r` are profile proportions. Empty or zero-mass
profiles return `0.0` through the local-reversal convention.

Interpretation: `R` is the profile mass that can be assigned to exact opposing
ranking pairs; unpaired rankings and unmatched excess mass do not contribute.
"""
function total_reversal_component(p::Union{Profile{<:StrictRank},WeightedProfile{<:StrictRank}})
    return sum(_local_reversal_values(p))
end

raw"""
    reversal_hhi(p)

Return the HHI concentration of local exact-reversal masses, manuscript label
`κ` when this measure is used as the reversal concentration term. With
`v_q = 2\min(p_r, p_{reverse(r)})` and `R = \sum_q v_q`,

```math
\kappa = \sum_q \left(\frac{v_q}{R}\right)^2.
```

Inputs are strict `Profile` or `WeightedProfile` objects. For positive reversal
mass, `κ \in [1/n_q, 1]`, where `n_q` is the number of exact reversal pairs
with positive local mass. If the profile is empty, has zero total mass, or has
no exact reversal mass, the implementation returns `0.0`.

Interpretation: higher `κ` means the exact reversal component is concentrated
in fewer opposing ranking pairs; lower `κ` means reversal mass is more diffuse.
"""
function reversal_hhi(p::Union{Profile{<:StrictRank},WeightedProfile{<:StrictRank}})
    values = _local_reversal_values(p)
    total = sum(values)
    total == 0.0 && return 0.0
    return sum((value / total)^2 for value in values)
end

raw"""
    reversal_geometric(p)

Return the geometric exact-reversal index

```math
\sqrt{R\kappa},
```

where `R = total_reversal_component(p)` and `κ = reversal_hhi(p)` is the HHI of
local exact-reversal masses. Algebraically, the implementation computes
`sqrt(sum_q v_q^2 / R)`, which is equal to `sqrt(R * κ)` for `R > 0`.

Inputs are strict `Profile` or `WeightedProfile` objects. The statistic lies in
`[0, 1]` for positive profile mass and returns `0.0` when the input has no
positive exact reversal mass, including empty or zero-mass profiles.

Interpretation: this combines the amount of exact reversal structure with its
concentration across reversal pairs.
"""
function reversal_geometric(p::Union{Profile{<:StrictRank},WeightedProfile{<:StrictRank}})
    values = _local_reversal_values(p)
    total = sum(values)
    total == 0.0 && return 0.0
    return sqrt(sum(value^2 for value in values) / total)
end
