# PreferenceMeasures.jl

@inline function ranking_signature(x::StrictRank, pool::CandidatePool)
    return Tuple(ordered_candidates(x, pool))
end

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

function ranking_proportions(p::Union{Profile{<:StrictRank},WeightedProfile{<:StrictRank}})
    masses, _, total = _ranking_masses(p)
    total > 0 || return Dict{Tuple,Float64}()
    return Dict(sig => mass / total for (sig, mass) in masses)
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

function total_reversal_component(p::Union{Profile{<:StrictRank},WeightedProfile{<:StrictRank}})
    return sum(_local_reversal_values(p))
end

function reversal_hhi(p::Union{Profile{<:StrictRank},WeightedProfile{<:StrictRank}})
    values = _local_reversal_values(p)
    total = sum(values)
    total == 0.0 && return 0.0
    return sum((value / total)^2 for value in values)
end

function reversal_geometric(p::Union{Profile{<:StrictRank},WeightedProfile{<:StrictRank}})
    values = _local_reversal_values(p)
    total = sum(values)
    total == 0.0 && return 0.0
    return sqrt(sum(value^2 for value in values) / total)
end
