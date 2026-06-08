function positional_method_3a(s, p)
    v = validate_profile_vector(p, 6)
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

function get_4c_w_s(p, s1, s2)
    v = validate_profile_vector(p, 24)
    return standard_vote_matrix(s1, s2) * v
end

function q_s_4candidates(p, s1, s2)
    return get_4c_w_s(p, s1, s2) ./ (1 + Float64(s1) + Float64(s2))
end

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
