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

struct SinglePeakedAxisSummary{A}
    axis_id::Int
    axis::Vector{A}
    L0::Float64
    L1::Float64
    non_single_peaked_support_ids::Vector{Int}
    non_single_peaked_mass::Float64
    total_mass::Float64
end

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

struct SinglePeakednessResult{A}
    best_L0::Float64
    best_L1::Float64
    best_L0_axis_ids::Vector{Int}
    best_L1_axis_ids::Vector{Int}
    axis_summaries::Vector{SinglePeakedAxisSummary{A}}
    support_classifications::Vector{SinglePeakedSupportClassification{A}}
    support::Vector
end

"""
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

"""
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

"""
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

"""
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

"""
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
                            classify_axes)
    if classify_axes === :best_L0
        return best_L0_axis_ids
    elseif classify_axes === :best_L1
        return best_L1_axis_ids
    elseif classify_axes === :best
        return sort!(unique(vcat(best_L0_axis_ids, best_L1_axis_ids)))
    elseif classify_axes === :all
        return [summary.axis_id for summary in result_axis_summaries]
    else
        throw(ArgumentError("classify_axes must be :best_L0, :best_L1, :best, or :all."))
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

"""
    single_peakedness_summary(profile; axes=nothing, proportion_source=:auto, classify_axes=:best)

Compute two deviation-from-single-peakedness measures over a probability
distribution of unique strict rankings:

* `best_L0` is the minimal profile mass incompatible with single-peakedness on
  the best-fitting axis.
* `best_L1` is the minimal expected Kendall distance to the single-peaked domain,
  normalized by `binomial(m, 2)`, on the best-fitting axis.

Axes are evaluated up to reversal. `L0` and normalized `L1` may have different
best axes, and the result stores their tied minimizers separately. Inputs must
be strict complete linear orders; weak orders, ties, missing alternatives, and
inconsistent candidate sets are rejected. Axis scoring is computed over unique
rankings and their `proportion`; raw counts and survey-weight sums are returned
only as metadata. Support indices refer to unique ranking rows, not original
respondents. Respondent-level classification requires callers, such as PrefPol,
to supply row identifiers or a row-to-ranking mapping.
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
        total_mass = 0.0

        for entry in support
            sp = is_single_peaked(to_perm(entry.ballot), axis)
            dist = _single_peaked_distance_strict_ids(entry.ballot, axis)
            total_mass += entry.proportion
            if !sp
                push!(non_sp_ids, entry.unique_ranking_id)
                L0 += entry.proportion
            end
            L1 += entry.proportion * dist / norm_factor
        end

        push!(summaries, SinglePeakedAxisSummary(
            axis_id,
            axis_symbols,
            L0,
            L1,
            non_sp_ids,
            L0,
            total_mass,
        ))
    end

    best_L0 = minimum(summary.L0 for summary in summaries)
    best_L1 = minimum(summary.L1 for summary in summaries)
    best_L0_axis_ids = [summary.axis_id for summary in summaries if isapprox(summary.L0, best_L0; atol = atol, rtol = atol)]
    best_L1_axis_ids = [summary.axis_id for summary in summaries if isapprox(summary.L1, best_L1; atol = atol, rtol = atol)]

    selected_axis_ids = _selected_axis_ids(summaries, best_L0_axis_ids, best_L1_axis_ids, classify_axes)
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
        best_L0_axis_ids,
        best_L1_axis_ids,
        summaries,
        classifications,
        support,
    )
end

single_peakedness_L0(profile; kwargs...) = single_peakedness_summary(profile; kwargs...).best_L0
single_peakedness_L1(profile; kwargs...) = single_peakedness_summary(profile; kwargs...).best_L1
best_single_peaked_axes(profile; kwargs...) = single_peakedness_summary(profile; kwargs...).best_L0_axis_ids
