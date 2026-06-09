function positional_method_3a(s, p)
    v = validate_profile_counts(p, 6)
    sf = Float64(s)
    return [
        v[1] + v[2] + (-v[1] - v[2] + v[3] + v[6]) * sf,
        v[6] + v[5] + (v[4] - v[5] + v[1] - v[6]) * sf,
        v[3] + v[4] + (v[2] - v[3] - v[4] + v[5]) * sf,
    ]
end

plurality_3a(p) = positional_method_3a(0, p)
borda_3a(p) = positional_method_3a(1 / 3, p)
antiplurality_3a(p) = positional_method_3a(1 / 2, p)

function standard_vote_matrix(s1, s2)
    scores_by_position = (1.0, Float64(s1), Float64(s2), 0.0)
    mat = zeros(Float64, 4, length(CANONICAL_4C_IDS))
    @inbounds for (col, order) in pairs(CANONICAL_4C_IDS)
        for (position, candidate_id) in pairs(order)
            mat[candidate_id, col] = scores_by_position[position]
        end
    end
    return mat
end

"""
    get_4c_w_s(p, s1, s2; allow_signed=false)

Return the raw four-candidate positional score tally
`F(p; s1, s2) = standard_vote_matrix(s1, s2) * p` for score vector
`(1, s1, s2, 0)`.

By default `p` is validated as an actual profile count/share vector:
nonnegative, finite, length 24, and positive mass. Set `allow_signed=true` for
signed profile differentials such as Saari decomposition components.
"""
function get_4c_w_s(p, s1, s2; allow_signed::Bool = false)
    v = allow_signed ? validate_profile_differential(p, 24) : validate_profile_counts(p, 24)
    return standard_vote_matrix(s1, s2) * v
end

"""
    raw_score_tally_4c(p, s1, s2; allow_signed=false)

Explicit alias for `get_4c_w_s`: return the raw positional score tally, not a
candidate score share.
"""
raw_score_tally_4c(p, s1, s2; allow_signed::Bool = false) =
    get_4c_w_s(p, s1, s2; allow_signed = allow_signed)

"""
    score_tally_per_rule_mass_4c(p, s1, s2; allow_signed=false)

Return the raw score tally divided only by the scoring-rule mass
`1 + s1 + s2`. For count profiles this vector sums to `sum(p)`, so it is not a
candidate score share unless `p` already has mass one.
"""
function score_tally_per_rule_mass_4c(p, s1, s2; allow_signed::Bool = false)
    rule_mass = 1 + Float64(s1) + Float64(s2)
    return get_4c_w_s(p, s1, s2; allow_signed = allow_signed) ./ rule_mass
end

"""
    candidate_score_share_4c(p, s1, s2)

Return the candidate score-share vector
`F(p; s1, s2) / (sum(p) * (1 + s1 + s2))`.

This helper is for actual electorates, so signed profile differentials and
zero-mass profiles are rejected. Multiplying `p` by a positive scalar leaves the
result unchanged.
"""
function candidate_score_share_4c(p, s1, s2)
    v = validate_profile_counts(p, 24)
    total_score = sum(v) * (1 + Float64(s1) + Float64(s2))
    return get_4c_w_s(v, s1, s2; allow_signed = true) ./ total_score
end

"""
    q_s_4candidates(p, s1, s2)

Compatibility name for `candidate_score_share_4c`.
"""
q_s_4candidates(p, s1, s2) = candidate_score_share_4c(p, s1, s2)

plurality_4c_q_s(p) = q_s_4candidates(p, 0, 0)
vote_for_two_4c_q_s(p) = q_s_4candidates(p, 1, 0)
antiplurality_4c_q_s(p) = q_s_4candidates(p, 1, 1)
borda_4c_q_s(p) = q_s_4candidates(p, 2 / 3, 1 / 3)

function winner_order(scores, labels)
    score_vec = [Float64(x) for x in scores]
    label_vec = collect(labels)
    length(score_vec) == length(label_vec) ||
        throw(ArgumentError("scores and labels must have the same length"))
    order = sortperm(collect(eachindex(score_vec)); by = i -> -score_vec[i], alg = MergeSort)
    return label_vec[order]
end

function _validate_profile_summary_4c_labels(labels)
    label_tuple = _validate_4c_labels(labels)
    length(unique(label_tuple)) == 4 || throw(ArgumentError("candidate labels must be unique"))
    return label_tuple
end

"""
    position_share_coefficients_4c(p4, labels)

Return a dictionary from each candidate label to the position-share coefficients
`(constant, s1, s2)` for a four-candidate strict profile vector.

For profile mass `M`, these are the first-, second-, and third-position shares
in the supplied profile. They satisfy
`raw_score_tally_4c(p4, s1, s2)[i] / M = constant + s1*c.s1 + s2*c.s2`
for candidate `i`, where `c` is the returned coefficient tuple for that
candidate. The fourth-position coefficient is omitted because the normalized
four-candidate score vector has fourth-place score zero.
"""
function position_share_coefficients_4c(p4, labels)
    v = validate_profile_counts(p4, 24)
    label_vec = _validate_profile_summary_4c_labels(labels)
    mass = sum(v)
    coeffs = zeros(Float64, 4, 3)

    @inbounds for (col, order) in pairs(CANONICAL_4C_IDS)
        weight = v[col] / mass
        weight == 0.0 && continue
        for position in 1:3
            coeffs[order[position], position] += weight
        end
    end

    return Dict(
        label_vec[i] => (constant = coeffs[i, 1], s1 = coeffs[i, 2], s2 = coeffs[i, 3])
        for i in 1:4
    )
end

function _pairwise_win_counts_4c(v)
    wins = zeros(Float64, 4, 4)
    positions = zeros(Int, 4)

    @inbounds for (col, order) in pairs(CANONICAL_4C_IDS)
        weight = v[col]
        weight == 0.0 && continue
        for position in 1:4
            positions[order[position]] = position
        end
        for a in 1:4, b in 1:4
            if a != b && positions[a] < positions[b]
                wins[a, b] += weight
            end
        end
    end

    return wins
end

"""
    pairwise_percentages_4c(p4, labels)

Return a dictionary mapping `(left_label, right_label)` to the percentage of
profile mass that ranks `left_label` above `right_label` in a four-candidate
strict profile vector.
"""
function pairwise_percentages_4c(p4, labels)
    v = validate_profile_counts(p4, 24)
    label_vec = _validate_profile_summary_4c_labels(labels)
    wins = _pairwise_win_counts_4c(v)
    mass = sum(v)

    return Dict(
        (label_vec[a], label_vec[b]) => 100 * wins[a, b] / mass
        for a in 1:4 for b in 1:4 if a != b
    )
end

"""
    condorcet_winner_4c(p4, labels; atol=1e-12)

Return the label of the candidate that strictly beats every other candidate by
pairwise majority, or `nothing` if no unique Condorcet winner exists.
"""
function condorcet_winner_4c(p4, labels; atol = 1e-12)
    v = validate_profile_counts(p4, 24)
    label_vec = _validate_profile_summary_4c_labels(labels)
    wins = _pairwise_win_counts_4c(v)
    majority = sum(v) / 2

    winners = [
        a for a in 1:4
        if all(b -> a == b || wins[a, b] > majority + atol, 1:4)
    ]
    return length(winners) == 1 ? label_vec[only(winners)] : nothing
end

"""
    condorcet_loser_4c(p4, labels; atol=1e-12)

Return the label of the candidate that strictly loses to every other candidate
by pairwise majority, or `nothing` if no unique Condorcet loser exists.
"""
function condorcet_loser_4c(p4, labels; atol = 1e-12)
    v = validate_profile_counts(p4, 24)
    label_vec = _validate_profile_summary_4c_labels(labels)
    wins = _pairwise_win_counts_4c(v)
    majority = sum(v) / 2

    losers = [
        a for a in 1:4
        if all(b -> a == b || wins[b, a] > majority + atol, 1:4)
    ]
    return length(losers) == 1 ? label_vec[only(losers)] : nothing
end
