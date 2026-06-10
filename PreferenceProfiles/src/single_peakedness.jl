# Single-peakedness measures for strict ranking distributions.

struct SinglePeakedSupportEntry{A,B}
    unique_ranking_id::Int
    ballot::B
    ranking::Vector{A}
    proportion::Float64
    raw_count::Union{Nothing,Int}
    survey_weight_sum::Union{Nothing,Float64}
    proportion_source::Symbol
end

@doc raw"""
    SinglePeakedAxisSummary

Per-axis summary for deviation from single-peakedness. For an axis `a`, let
`SP(a)` be the set of strict rankings single-peaked on that axis, `p_r` the
profile mass of ranking type `r`, and
`d_a(r) = min_{s in SP(a)} d_K(r,s) / binomial(m, 2)`. The fields store

* `L0 = sum_{r notin SP(a)} p_r`, the off-axis support mass;
* `L1 = sum_r p_r d_a(r)`, the unconditional normalized Kendall distortion;
* `L1_off_axis = sum_{r notin SP(a)} p_r d_a(r) / L0`, or `missing` when
  `L0` is numerically zero.

`axis` is a vector of candidate symbols, `axis_id` is its deterministic index
among evaluated axes, and support IDs refer to unique ranking rows. Values are
normalized by the ranking proportions generated from a strict `Profile` or
`WeightedProfile`; empty and zero-mass profiles are rejected before summaries
are constructed.

Interpretation: `L0` identifies how much ranking support lies outside the axis,
while `L1` and `L1_off_axis` quantify how far the full profile and off-axis
portion are from the single-peaked domain.
"""
struct SinglePeakedAxisSummary{A}
    axis_id::Int
    axis::Vector{A}
    L0::Float64
    L1::Float64
    L1_off_axis::Union{Missing,Float64}
    non_single_peaked_support_ids::Vector{Int}
    non_single_peaked_mass::Float64
    total_mass::Float64
end

@doc raw"""
    SinglePeakedSupportClassification

Classification of one observed ranking type against one evaluated axis. The row
contains the unique ranking ID, candidate-symbol ranking, normalized ranking
proportion, optional raw count or survey-weight sum, axis ID, a Boolean support
indicator `is_single_peaked`, and the unnormalized Kendall distance from the
ranking to the axis's single-peaked domain.

Inputs represented by these rows come from strict `Profile` or `WeightedProfile`
objects after compression to observed ranking types. Proportions sum to `1`
within the source profile, and empty or zero-mass profiles are rejected upstream.

Interpretation: these rows distinguish axis support (`is_single_peaked == true`)
from off-axis rankings and attach the distance used by the `L1` summaries.
"""
struct SinglePeakedSupportClassification{A}
    unique_ranking_id::Int
    ranking::Vector{A}
    proportion::Float64
    raw_count::Union{Nothing,Int}
    survey_weight_sum::Union{Nothing,Float64}
    proportion_source::Symbol
    axis_id::Int
    is_single_peaked::Bool
    distance::Float64
end

@doc raw"""
    SinglePeakednessResult

Complete result returned by `single_peakedness_summary`. The best fields are
minima over evaluated axes: `best_L0` for off-axis mass, `best_L1` for
unconditional normalized Kendall distortion, and `best_L1_off_axis` for
conditional off-axis distortion. The corresponding axis-ID vectors list all
tied minimizers under the supplied tolerance.

`axis_summaries` contains one `SinglePeakedAxisSummary` per axis,
`support_classifications` contains selected ranking-by-axis rows according to
`classify_axes`, and `support` stores the observed ranking-type distribution.
All measures use strict complete rankings and normalized row or survey-weight
proportions. Empty and zero-mass profiles throw before a result is created;
`best_L1_off_axis` is `missing` when every evaluated axis has zero off-axis
mass.

Interpretation: the result separates axis choice from profile support: a profile
can have low `L0` because most mass is on-axis, low `L1` because off-axis
rankings are close to the domain, or low `L1_off_axis` because the incompatible
portion is mild conditional on being incompatible.
"""
struct SinglePeakednessResult{A}
    best_L0::Float64
    best_L1::Float64
    best_L1_off_axis::Union{Missing,Float64}
    best_L0_axis_ids::Vector{Int}
    best_L1_axis_ids::Vector{Int}
    best_L1_off_axis_axis_ids::Vector{Int}
    axis_summaries::Vector{SinglePeakedAxisSummary{A}}
    support_classifications::Vector{SinglePeakedSupportClassification{A}}
    support::Vector
end

@doc raw"""
    axes_up_to_reversal(candidates)

Return one deterministic representative from each reversal-equivalence class of
linear axes over `candidates`. For `m > 1`, this returns `m! / 2` axes; for
`m == 1`, it returns one axis. Candidate objects are carried through unchanged,
while the canonical reversal choice is made by their input positions.
"""
function axes_up_to_reversal(candidates::AbstractVector)
    m = length(candidates)
    m >= 1 || throw(ArgumentError("At least one candidate is required."))
    m > 8 && @warn "Enumerating axes up to reversal for m=$m requires factorial work."

    axes = Vector{Vector{eltype(candidates)}}()
    for p in permutations(collect(1:m))
        pt = Tuple(p)
        pt <= Tuple(reverse(p)) || continue
        push!(axes, [candidates[i] for i in p])
    end
    return axes
end

axes_up_to_reversal(candidates) = axes_up_to_reversal(collect(candidates))

function _ranking_vector(ranking::AbstractVector)
    return collect(ranking)
end

function _ranking_vector(ranking::StrictRank)
    return to_perm(ranking)
end

function _validate_strict_linear_order(ranking_vec::AbstractVector,
                                       axis_vec::AbstractVector)
    length(ranking_vec) == length(axis_vec) || throw(ArgumentError(
        "Ranking and axis must contain the same number of alternatives.",
    ))
    Set(ranking_vec) == Set(axis_vec) || throw(ArgumentError(
        "Ranking and axis must contain the same candidate set with no missing or extra alternatives.",
    ))
    length(unique(ranking_vec)) == length(ranking_vec) || throw(ArgumentError(
        "Ranking must be a strict linear order with no ties or duplicate alternatives.",
    ))
    length(unique(axis_vec)) == length(axis_vec) || throw(ArgumentError(
        "Axis must contain each candidate exactly once.",
    ))
    return true
end

@doc raw"""
    is_single_peaked(ranking, axis)::Bool

Return whether a strict best-to-worst `ranking` is single-peaked on `axis`, using
the endpoint test. The function requires complete strict linear orders over the
same candidate set and fails loudly on ties, duplicates, missing alternatives, or
inconsistent candidate sets.
"""
function is_single_peaked(ranking, axis)::Bool
    ranking_vec = _ranking_vector(ranking)
    axis_vec = collect(axis)
    _validate_strict_linear_order(ranking_vec, axis_vec)

    left = firstindex(axis_vec)
    right = lastindex(axis_vec)
    for alt in Iterators.reverse(ranking_vec)
        if alt == axis_vec[left]
            left += 1
        elseif alt == axis_vec[right]
            right -= 1
        else
            return false
        end
    end
    return true
end

@doc raw"""
    single_peaked_rankings(axis)

Generate all `2^(m-1)` strict best-to-worst rankings that are single-peaked on a
fixed `axis`, using the endpoint construction from worst to best.
"""
function single_peaked_rankings(axis)
    axis_vec = collect(axis)
    length(unique(axis_vec)) == length(axis_vec) || throw(ArgumentError(
        "Axis must contain each candidate exactly once.",
    ))

    T = eltype(axis_vec)
    out = Vector{Vector{T}}()
    function build(left::Int, right::Int, worst_to_best)
        if left > right
            push!(out, reverse(worst_to_best))
            return nothing
        elseif left == right
            push!(out, reverse(vcat(worst_to_best, axis_vec[left])))
            return nothing
        end
        build(left + 1, right, vcat(worst_to_best, axis_vec[left]))
        build(left, right - 1, vcat(worst_to_best, axis_vec[right]))
        return nothing
    end
    build(firstindex(axis_vec), lastindex(axis_vec), T[])
    return out
end

function _kendall_distance_vectors(x::AbstractVector, y::AbstractVector)
    _validate_strict_linear_order(x, y)
    pos = Dict{eltype(y),Int}()
    for (rank, alt) in enumerate(y)
        pos[alt] = rank
    end
    d = 0
    for i in 1:(length(x) - 1)
        pi = pos[x[i]]
        for j in (i + 1):length(x)
            pi > pos[x[j]] && (d += 1)
        end
    end
    return d
end

@doc raw"""
    single_peaked_distance(ranking, axis)

Return the minimum Kendall tau distance from `ranking` to the single-peaked
domain induced by `axis`.
"""
function single_peaked_distance(ranking, axis)
    ranking_vec = _ranking_vector(ranking)
    axis_vec = collect(axis)
    _validate_strict_linear_order(ranking_vec, axis_vec)
    return minimum(_kendall_distance_vectors(ranking_vec, sp)
                   for sp in single_peaked_rankings(axis_vec))
end

function _single_peaked_distance_strict_ids(ranking::StrictRank, axis_ids::AbstractVector{<:Integer})
    pool = CandidatePool(Symbol.("x" .* string.(1:length(axis_ids))))
    sp_ballots = (StrictRank(pool, sp) for sp in single_peaked_rankings(collect(Int.(axis_ids))))
    return minimum(kendall_tau_distance(ranking, sp) for sp in sp_ballots)
end

function _normalize_proportion_source(profile, proportion_source::Symbol)
    if proportion_source === :auto
        return profile isa WeightedProfile ? :survey_weight : :unweighted_rows
    end
    proportion_source in (:survey_weight, :resampled_profile, :unweighted_rows) ||
        throw(ArgumentError(
            "proportion_source must be :auto, :survey_weight, :resampled_profile, or :unweighted_rows.",
        ))
    return proportion_source
end

@doc raw"""
    profile_distribution(profile; proportion_source=:auto)

Compress a strict `Profile` or `WeightedProfile` to unique strict rankings and
ranking proportions. Axis scoring in the single-peakedness measures uses only
the returned `proportion` field. Raw row counts and survey-weight sums are
diagnostic metadata; they are not used in the formulas. For `WeightedProfile`,
stored weights are interpreted as ranking masses or unnormalized ranking masses
and normalized internally.
"""
function profile_distribution(profile::Union{Profile,WeightedProfile};
                              proportion_source::Symbol = :auto)
    profile isa Union{Profile{<:StrictRank},WeightedProfile{<:StrictRank}} ||
        throw(ArgumentError(
            "single-peakedness measures require a Profile or WeightedProfile of StrictRank ballots; weak, tied, or incomplete orders must be linearized explicitly before calling.",
        ))
    validate(profile)
    nballots(profile) > 0 || throw(ArgumentError("Profile must contain at least one ballot."))

    source = _normalize_proportion_source(profile, proportion_source)
    use_weights = profile isa WeightedProfile && source === :survey_weight
    masses = Dict{Tuple{Vararg{Int}},Float64}()
    counts = Dict{Tuple{Vararg{Int}},Int}()
    exemplars = Dict{Tuple{Vararg{Int}},eltype(profile)}()
    order = Tuple{Vararg{Int}}[]

    for (idx, ballot) in enumerate(profile.ballots)
        key = Tuple(to_perm(ballot))
        if !haskey(masses, key)
            push!(order, key)
            masses[key] = 0.0
            counts[key] = 0
            exemplars[key] = ballot
        end
        row_mass = use_weights ? Float64(profile.weights[idx]) : 1.0
        isfinite(row_mass) && row_mass >= 0 || throw(ArgumentError(
            "Profile masses used for proportions must be finite and nonnegative.",
        ))
        masses[key] += row_mass
        counts[key] += 1
    end

    total = sum(masses[key] for key in order)
    total > 0 || throw(ArgumentError("Profile masses must sum to a positive value."))

    entries = SinglePeakedSupportEntry{Symbol,eltype(profile)}[]
    for (id, key) in enumerate(order)
        push!(entries, SinglePeakedSupportEntry(
            id,
            exemplars[key],
            [profile.pool[i] for i in key],
            masses[key] / total,
            counts[key],
            use_weights ? masses[key] : nothing,
            source,
        ))
    end

    mass_total = sum(entry.proportion for entry in entries)
    isapprox(mass_total, 1.0; atol = 1e-10, rtol = 1e-10) || throw(ArgumentError(
        "Ranking proportions must sum to 1; got $mass_total.",
    ))
    return entries
end

function _selected_axis_ids(result_axis_summaries, best_L0_axis_ids, best_L1_axis_ids,
                            best_L1_off_axis_axis_ids, classify_axes)
    if classify_axes === :best_L0
        return best_L0_axis_ids
    elseif classify_axes === :best_L1
        return best_L1_axis_ids
    elseif classify_axes === :best_L1_off_axis
        return best_L1_off_axis_axis_ids
    elseif classify_axes === :best
        return sort!(unique(vcat(best_L0_axis_ids, best_L1_axis_ids, best_L1_off_axis_axis_ids)))
    elseif classify_axes === :all
        return [summary.axis_id for summary in result_axis_summaries]
    else
        throw(ArgumentError("classify_axes must be :best_L0, :best_L1, :best_L1_off_axis, :best, or :all."))
    end
end

function _axis_to_ids(axis, pool::CandidatePool)
    axis_vec = collect(axis)
    if all(x -> x isa Integer, axis_vec)
        return Int.(axis_vec)
    elseif all(x -> x isa Symbol, axis_vec)
        return [pool[x] for x in axis_vec]
    else
        throw(ArgumentError(
            "Supplied axes for a Profile must contain either candidate ids or candidate symbols.",
        ))
    end
end

@doc raw"""
    single_peakedness_summary(profile; axes=nothing, proportion_source=:auto, classify_axes=:best)

Compute deviation-from-single-peakedness measures over a probability
distribution of unique strict rankings. For each axis `a`, let `SP(a)` be the
single-peaked domain, `p_r` the observed ranking proportions, and
`d_a(r) = min_{s in SP(a)} d_K(r,s) / binomial(m, 2)`. The per-axis quantities
are

```math
L_0(a) = \sum_{r \notin SP(a)} p_r, \qquad
L_1(a) = \sum_r p_r d_a(r),
```

and

```math
L_{1,off}(a) =
\frac{\sum_{r \notin SP(a)} p_r d_a(r)}{L_0(a)}
```

when `L0(a) > 0`, otherwise `missing`.

Inputs are strict `Profile` or `WeightedProfile` objects. Axes may be omitted,
in which case one representative from each reversal-equivalence class is
evaluated, or supplied as candidate IDs or symbols. Weak orders, ties, missing
alternatives, inconsistent candidate sets, empty profiles, zero-mass profiles,
and one-candidate profiles are rejected. All reported `L0` and `L1` values are
normalized to `[0, 1]`; `L1_off_axis` is either in `[0, 1]` or `missing`.

Interpretation: `L0` is axis support failure mass, `L1` is full-profile
Kendall distortion from the axis's single-peaked domain, and `L1_off_axis`
measures the severity of only the off-axis rankings. These criteria can have
different best axes, and the result stores all tied minimizers separately.
Support indices refer to unique ranking rows, not original respondents.
"""
function single_peakedness_summary(profile::Union{Profile,WeightedProfile};
                                   axes = nothing,
                                   proportion_source::Symbol = :auto,
                                   classify_axes::Symbol = :best,
                                   atol::Real = 1e-10)
    support = profile_distribution(profile; proportion_source = proportion_source)
    m = length(profile.pool)
    m >= 2 || throw(ArgumentError("At least two candidates are required for normalized Kendall distances."))
    m > 8 && @warn "Single-peakedness summary for m=$m enumerates factorially many axes."

    axis_ids = axes === nothing ? axes_up_to_reversal(collect(1:m)) :
               [_axis_to_ids(axis, profile.pool) for axis in axes]
    norm_factor = binomial(m, 2)
    summaries = SinglePeakedAxisSummary{Symbol}[]

    for (axis_id, axis) in enumerate(axis_ids)
        _validate_strict_linear_order(axis, collect(1:m))
        axis_symbols = [profile.pool[i] for i in axis]
        non_sp_ids = Int[]
        L0 = 0.0
        L1 = 0.0
        off_axis_distortion_mass = 0.0
        total_mass = 0.0

        for entry in support
            sp = is_single_peaked(to_perm(entry.ballot), axis)
            dist = _single_peaked_distance_strict_ids(entry.ballot, axis)
            normalized_dist = dist / norm_factor
            total_mass += entry.proportion
            if !sp
                push!(non_sp_ids, entry.unique_ranking_id)
                L0 += entry.proportion
                off_axis_distortion_mass += entry.proportion * normalized_dist
            end
            L1 += entry.proportion * normalized_dist
        end
        L1_off_axis = L0 > atol ? off_axis_distortion_mass / L0 : missing

        push!(summaries, SinglePeakedAxisSummary(
            axis_id,
            axis_symbols,
            L0,
            L1,
            L1_off_axis,
            non_sp_ids,
            L0,
            total_mass,
        ))
    end

    best_L0 = minimum(summary.L0 for summary in summaries)
    best_L1 = minimum(summary.L1 for summary in summaries)
    best_L0_axis_ids = [summary.axis_id for summary in summaries if isapprox(summary.L0, best_L0; atol = atol, rtol = atol)]
    best_L1_axis_ids = [summary.axis_id for summary in summaries if isapprox(summary.L1, best_L1; atol = atol, rtol = atol)]
    finite_off_axis_summaries = [summary for summary in summaries if !ismissing(summary.L1_off_axis)]
    if isempty(finite_off_axis_summaries)
        best_L1_off_axis = missing
        best_L1_off_axis_axis_ids = Int[]
    else
        best_L1_off_axis = minimum(summary.L1_off_axis for summary in finite_off_axis_summaries)
        best_L1_off_axis_axis_ids = [
            summary.axis_id for summary in finite_off_axis_summaries
            if isapprox(summary.L1_off_axis, best_L1_off_axis; atol = atol, rtol = atol)
        ]
    end

    selected_axis_ids = _selected_axis_ids(
        summaries,
        best_L0_axis_ids,
        best_L1_axis_ids,
        best_L1_off_axis_axis_ids,
        classify_axes,
    )
    axis_by_id = Dict(summary.axis_id => axis_ids[summary.axis_id] for summary in summaries)
    classifications = SinglePeakedSupportClassification{Symbol}[]
    for axis_id in selected_axis_ids
        axis = axis_by_id[axis_id]
        for entry in support
            push!(classifications, SinglePeakedSupportClassification(
                entry.unique_ranking_id,
                entry.ranking,
                entry.proportion,
                entry.raw_count,
                entry.survey_weight_sum,
                entry.proportion_source,
                axis_id,
                is_single_peaked(to_perm(entry.ballot), axis),
                Float64(_single_peaked_distance_strict_ids(entry.ballot, axis)),
            ))
        end
    end

    return SinglePeakednessResult(
        best_L0,
        best_L1,
        best_L1_off_axis,
        best_L0_axis_ids,
        best_L1_axis_ids,
        best_L1_off_axis_axis_ids,
        summaries,
        classifications,
        support,
    )
end

@doc raw"""
    single_peakedness_L0(profile; kwargs...)

Return only `best_L0` from `single_peakedness_summary`: the minimum off-axis
ranking mass `min_a L0(a)` over evaluated axes. Inputs, normalization, empty and
zero-mass behavior, and interpretation are the same as
`single_peakedness_summary`.
"""
single_peakedness_L0(profile; kwargs...) = single_peakedness_summary(profile; kwargs...).best_L0
@doc raw"""
    single_peakedness_L1(profile; kwargs...)

Return only `best_L1` from `single_peakedness_summary`: the minimum
unconditional expected Kendall distance to an axis's single-peaked domain,
normalized by `binomial(m, 2)`. Inputs, range, and zero-mass behavior are the
same as `single_peakedness_summary`.
"""
single_peakedness_L1(profile; kwargs...) = single_peakedness_summary(profile; kwargs...).best_L1
@doc raw"""
    single_peakedness_L1_off_axis(profile; kwargs...)

Return only `best_L1_off_axis` from `single_peakedness_summary`: the minimum
conditional normalized Kendall distance among rankings not supported by the
axis. The value is `missing` when every evaluated axis has zero off-axis mass;
empty and zero-mass profiles are rejected by `single_peakedness_summary`.
"""
single_peakedness_L1_off_axis(profile; kwargs...) = single_peakedness_summary(profile; kwargs...).best_L1_off_axis
@doc raw"""
    best_single_peaked_axes(profile; kwargs...)

Return the axis IDs that minimize `L0`, the off-axis support mass, in
`single_peakedness_summary`. This convenience wrapper reports support-optimal
axes, not necessarily the axes minimizing `L1` or `L1_off_axis`.
"""
best_single_peaked_axes(profile; kwargs...) = single_peakedness_summary(profile; kwargs...).best_L0_axis_ids
