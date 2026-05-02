# PreferencePluralitySwitchTables.jl

const StrictPreferenceProfile = Union{Profile{<:StrictRank},WeightedProfile{<:StrictRank}}

function _candidate_id(pool::CandidatePool, candidate)
    candidate isa Integer && return Int(candidate)
    candidate isa Symbol && return pool[candidate]
    candidate isa AbstractString && return pool[Symbol(candidate)]
    throw(ArgumentError("candidate must be an Int, Symbol, or String"))
end

_candidate_label(pool::CandidatePool, id::Integer) = pool[Int(id)]
_profile_total_mass(p::Profile) = Float64(nballots(p))
_profile_total_mass(p::WeightedProfile) = Float64(total_weight(p))
_profile_weight_at(p::Profile, i::Integer) = 1.0
_profile_weight_at(p::WeightedProfile, i::Integer) = Float64(p.weights[Int(i)])

function _basis_for_profile(p::StrictPreferenceProfile, basis)
    return basis === nothing ? voter_type_basis(p.pool) : basis
end

function _type_masses_for_profile(p::StrictPreferenceProfile, basis)
    b = _basis_for_profile(p, basis)
    return b, voter_type_masses(p, b)
end

function _ranking_position(perm::AbstractVector{<:Integer}, candidate_id::Integer)
    pos = findfirst(==(Int(candidate_id)), perm)
    pos === nothing && throw(ArgumentError("candidate id $candidate_id is not in ranking"))
    return Int(pos)
end

function _candidate_filter_ids(pool::CandidatePool, candidates)
    candidates === nothing && return nothing
    return Set(_candidate_id(pool, c) for c in candidates)
end

function plurality_scores_table(p::StrictPreferenceProfile; basis=nothing)
    validate(p)
    scores = zeros(Float64, length(p.pool))
    total = _profile_total_mass(p)

    if basis === nothing
        @inbounds for i in 1:nballots(p)
            first_id = to_perm(p.ballots[i])[1]
            scores[first_id] += _profile_weight_at(p, i)
        end
    else
        b, masses = _type_masses_for_profile(p, basis)
        @inbounds for tidx in eachindex(b.perms)
            first_id = b.perms[tidx][1]
            scores[first_id] += masses.counts_or_mass[tidx]
        end
    end

    return DataFrame(
        candidate_id = collect(1:length(p.pool)),
        candidate = [_candidate_label(p.pool, i) for i in 1:length(p.pool)],
        first_place_count = scores,
        first_place_share = scores ./ total,
    )
end

function pairwise_vs_plurality_decomposition_table(
    p::StrictPreferenceProfile,
    target,
    opponent;
    basis=nothing,
)
    validate(p)
    target_id = _candidate_id(p.pool, target)
    opponent_id = _candidate_id(p.pool, opponent)
    b, masses = _type_masses_for_profile(p, basis)

    target_over = zeros(Float64, length(p.pool))
    opponent_over = zeros(Float64, length(p.pool))
    first_target = zeros(Float64, length(p.pool))
    first_opponent = zeros(Float64, length(p.pool))

    @inbounds for tidx in eachindex(b.perms)
        mass = masses.counts_or_mass[tidx]
        mass == 0.0 && continue
        perm = b.perms[tidx]
        first_id = perm[1]
        if _ranking_position(perm, target_id) < _ranking_position(perm, opponent_id)
            target_over[first_id] += mass
        else
            opponent_over[first_id] += mass
        end
        first_id == target_id && (first_target[first_id] += mass)
        first_id == opponent_id && (first_opponent[first_id] += mass)
    end

    return DataFrame(
        current_first_id = collect(1:length(p.pool)),
        current_first = [_candidate_label(p.pool, i) for i in 1:length(p.pool)],
        target_over_opponent_mass = target_over,
        opponent_over_target_mass = opponent_over,
        pairwise_contribution = target_over .- opponent_over,
        first_place_target_mass = first_target,
        first_place_opponent_mass = first_opponent,
        plurality_contribution = first_target .- first_opponent,
    )
end

function candidate_position_by_current_first_table(
    p::StrictPreferenceProfile,
    target;
    basis=nothing,
)
    validate(p)
    target_id = _candidate_id(p.pool, target)
    b, masses = _type_masses_for_profile(p, basis)
    totals = zeros(Float64, length(p.pool))
    mass_by_first_pos = Dict{Tuple{Int,Int},Float64}()

    @inbounds for tidx in eachindex(b.perms)
        mass = masses.counts_or_mass[tidx]
        mass == 0.0 && continue
        perm = b.perms[tidx]
        first_id = perm[1]
        pos = _ranking_position(perm, target_id)
        totals[first_id] += mass
        key = (first_id, pos)
        mass_by_first_pos[key] = get(mass_by_first_pos, key, 0.0) + mass
    end

    current_first_id = Int[]
    current_first = Symbol[]
    target_position = Int[]
    mass = Float64[]
    share = Float64[]
    for first_id in 1:length(p.pool)
        for pos in 1:length(p.pool)
            m = get(mass_by_first_pos, (first_id, pos), 0.0)
            m == 0.0 && continue
            push!(current_first_id, first_id)
            push!(current_first, _candidate_label(p.pool, first_id))
            push!(target_position, pos)
            push!(mass, m)
            push!(share, totals[first_id] > 0 ? m / totals[first_id] : NaN)
        end
    end

    return DataFrame(current_first_id = current_first_id, current_first = current_first,
                     target_position = target_position, mass = mass,
                     share_within_current_first = share)
end

function one_swap_target_table(
    p::StrictPreferenceProfile,
    target;
    current_first_candidates=nothing,
    basis=nothing,
)
    validate(p)
    target_id = _candidate_id(p.pool, target)
    allowed = _candidate_filter_ids(p.pool, current_first_candidates)
    b, masses = _type_masses_for_profile(p, basis)
    totals = zeros(Float64, length(p.pool))
    one_swap = zeros(Float64, length(p.pool))

    @inbounds for tidx in eachindex(b.perms)
        mass = masses.counts_or_mass[tidx]
        mass == 0.0 && continue
        perm = b.perms[tidx]
        first_id = perm[1]
        first_id == target_id && continue
        allowed !== nothing && !(first_id in allowed) && continue
        totals[first_id] += mass
        perm[2] == target_id && (one_swap[first_id] += mass)
    end

    rows = [(i, one_swap[i]) for i in 1:length(p.pool) if one_swap[i] > 0]
    return DataFrame(
        current_first_id = [r[1] for r in rows],
        current_first = [_candidate_label(p.pool, r[1]) for r in rows],
        target_id = fill(target_id, length(rows)),
        target = fill(_candidate_label(p.pool, target_id), length(rows)),
        mass = [r[2] for r in rows],
        share_within_current_first = [totals[r[1]] > 0 ? r[2] / totals[r[1]] : NaN for r in rows],
    )
end

function plurality_swing_value_table(
    p::StrictPreferenceProfile,
    target,
    opponent;
    current_first_candidates=nothing,
    basis=nothing,
)
    target_id = _candidate_id(p.pool, target)
    opponent_id = _candidate_id(p.pool, opponent)
    scores = plurality_scores_table(p; basis=basis)
    score_vec = zeros(Float64, length(p.pool))
    @inbounds for row in eachrow(scores)
        score_vec[row.candidate_id] = row.first_place_count
    end
    baseline = Float64(score_vec[target_id] - score_vec[opponent_id])
    one_swap = one_swap_target_table(p, target; current_first_candidates=current_first_candidates, basis=basis)

    per_swing = Float64[]
    swing_value = Float64[]
    after = Float64[]
    for row in eachrow(one_swap)
        s = row.current_first_id == opponent_id ? 2.0 : 1.0
        value = row.mass * s
        push!(per_swing, s)
        push!(swing_value, value)
        push!(after, baseline + value)
    end

    one_swap.per_voter_swing = per_swing
    one_swap.plurality_swing_value = swing_value
    one_swap.target_opponent_margin_before = fill(baseline, nrow(one_swap))
    one_swap.target_opponent_margin_after_if_pool_switches = after
    return select(one_swap, :current_first_id, :current_first, :mass => :one_swap_mass,
                  :per_voter_swing, :plurality_swing_value,
                  :target_opponent_margin_before,
                  :target_opponent_margin_after_if_pool_switches)
end

function exact_type_switch_table(
    p::StrictPreferenceProfile,
    target;
    current_first_candidates=nothing,
    basis=nothing,
)
    validate(p)
    target_id = _candidate_id(p.pool, target)
    allowed = _candidate_filter_ids(p.pool, current_first_candidates)
    b, masses = _type_masses_for_profile(p, basis)

    type_index = Int[]
    ranking = String[]
    current_first = Symbol[]
    target_position = Int[]
    mass = Float64[]
    share = Float64[]

    @inbounds for tidx in eachindex(b.perms)
        m = masses.counts_or_mass[tidx]
        m == 0.0 && continue
        perm = b.perms[tidx]
        first_id = perm[1]
        first_id == target_id && continue
        allowed !== nothing && !(first_id in allowed) && continue
        push!(type_index, tidx)
        push!(ranking, _ranking_label(p.pool, perm))
        push!(current_first, _candidate_label(p.pool, first_id))
        push!(target_position, _ranking_position(perm, target_id))
        push!(mass, m)
        push!(share, m / masses.total_mass)
    end

    return DataFrame(type_index = type_index, ranking = ranking, current_first = current_first,
                     target_position = target_position, mass = mass, share = share)
end

function group_target_switch_table(
    p::StrictPreferenceProfile,
    group_labels::AbstractVector,
    target,
    opponent;
    current_first_candidates=nothing,
    basis=nothing,
)
    validate(p)
    length(group_labels) == nballots(p) || throw(ArgumentError("group_labels length must equal number of ballots"))
    target_id = _candidate_id(p.pool, target)
    opponent_id = _candidate_id(p.pool, opponent)
    allowed = _candidate_filter_ids(p.pool, current_first_candidates)
    total = _profile_total_mass(p)

    groups = Symbol[]
    group_index = Dict{Symbol,Int}()
    labels = Vector{Symbol}(undef, nballots(p))
    for i in 1:nballots(p)
        label = ismissing(group_labels[i]) ? :NA : Symbol(string(group_labels[i]))
        labels[i] = label
        if !haskey(group_index, label)
            group_index[label] = length(groups) + 1
            push!(groups, label)
        end
    end

    ng = length(groups)
    nc = length(p.pool)
    group_mass = zeros(Float64, ng)
    current_first_mass = zeros(Float64, ng, nc)
    target_second_mass = zeros(Float64, ng, nc)

    @inbounds for i in 1:nballots(p)
        weight = _profile_weight_at(p, i)
        g = group_index[labels[i]]
        perm = to_perm(p.ballots[i])
        first_id = perm[1]
        group_mass[g] += weight
        first_id == target_id && continue
        allowed !== nothing && !(first_id in allowed) && continue
        current_first_mass[g, first_id] += weight
        perm[2] == target_id && (target_second_mass[g, first_id] += weight)
    end

    group_col = Symbol[]
    group_mass_col = Float64[]
    current_first = Symbol[]
    current_first_mass_col = Float64[]
    target_second_mass_col = Float64[]
    share_col = Float64[]
    per_swing = Float64[]
    swing_value = Float64[]
    group_share = Float64[]

    for g in 1:ng
        for first_id in 1:nc
            cf_mass = current_first_mass[g, first_id]
            ts_mass = target_second_mass[g, first_id]
            (cf_mass == 0.0 && ts_mass == 0.0) && continue
            s = first_id == opponent_id ? 2.0 : 1.0
            push!(group_col, groups[g])
            push!(group_mass_col, group_mass[g])
            push!(current_first, _candidate_label(p.pool, first_id))
            push!(current_first_mass_col, cf_mass)
            push!(target_second_mass_col, ts_mass)
            push!(share_col, cf_mass > 0 ? ts_mass / cf_mass : NaN)
            push!(per_swing, s)
            push!(swing_value, ts_mass * s)
            push!(group_share, total > 0 ? ts_mass / total : NaN)
        end
    end

    return DataFrame(group = group_col, group_mass = group_mass_col,
                     current_first = current_first,
                     current_first_mass = current_first_mass_col,
                     target_second_mass = target_second_mass_col,
                     target_second_share_within_current_first = share_col,
                     per_voter_swing = per_swing,
                     plurality_swing_value = swing_value,
                     group_share_of_pool = group_share)
end
