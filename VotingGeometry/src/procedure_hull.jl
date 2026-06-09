function _validate_4c_scoring_parameters(s1, s2)
    s1f = Float64(s1)
    s2f = Float64(s2)
    isfinite(s1f) && isfinite(s2f) ||
        throw(ArgumentError("scoring parameters must be finite"))
    0.0 <= s2f <= s1f <= 1.0 ||
        throw(ArgumentError("expected admissible scores satisfying 0 <= s2 <= s1 <= 1"))
    return s1f, s2f
end

function _validate_4c_labels(labels)
    label_tuple = Tuple(labels)
    length(label_tuple) == 4 || throw(ArgumentError("four candidate labels are required"))
    return label_tuple
end

function _normalize_hull_convention(convention)
    conv = Symbol(convention)
    conv in (:saari, :candidate_share) ||
        throw(ArgumentError("convention must be :saari or :candidate_share"))
    return conv
end

function _hull_vertex_4c(v, s1, s2, convention::Symbol)
    if convention == :saari
        return get_4c_w_s(v, s1, s2)
    elseif convention == :candidate_share
        return q_s_4candidates(v, s1, s2)
    end
    throw(ArgumentError("unknown procedure hull convention $convention"))
end

"""
    procedure_hull_barycentric_4c(s1, s2; convention=:saari)

Return barycentric coordinates for the four-candidate score vector
`(1, s1, s2, 0)` relative to vote-for-one, vote-for-two, and vote-for-three.

With `convention=:saari`, this uses Saari's score-vector normalization:
`(1, s1, s2, 0) = (1-s1)E1 + (s1-s2)E2 + s2*E3`, so Borda
`(1, 2/3, 1/3, 0)` is `(1/3, 1/3, 1/3)`.

With `convention=:candidate_share`, coordinates are for the candidate-share
normalized q-vectors returned by `q_s_4candidates`; Borda is
`(1/6, 1/3, 1/2)` in that convention.
"""
function procedure_hull_barycentric_4c(s1, s2; convention = :saari)
    s1f, s2f = _validate_4c_scoring_parameters(s1, s2)
    conv = _normalize_hull_convention(convention)

    if conv == :saari
        return (1.0 - s1f, s1f - s2f, s2f)
    end

    denom = 1.0 + s1f + s2f
    return (
        (1.0 - s1f) / denom,
        2.0 * (s1f - s2f) / denom,
        3.0 * s2f / denom,
    )
end

candidate_share_hull_barycentric_4c(s1, s2) =
    procedure_hull_barycentric_4c(s1, s2; convention = :candidate_share)

"""
    procedure_hull_vertices_4c(p4; convention=:saari)

Return the vote-for-one, vote-for-two, and vote-for-three vertices of the
four-candidate positional procedure hull for a length-24 profile vector ordered by
`CANONICAL_4C_IDS`.

`convention=:saari` returns raw Saari score tallies `F(E_k^4, p)`. Use
`convention=:candidate_share` for q-normalized candidate-share vertices.
"""
function procedure_hull_vertices_4c(p4; convention = :saari)
    v = validate_profile_vector(p4, 24)
    conv = _normalize_hull_convention(convention)
    return (
        vote_for_one = _hull_vertex_4c(v, 0, 0, conv),
        vote_for_two = _hull_vertex_4c(v, 1, 0, conv),
        vote_for_three = _hull_vertex_4c(v, 1, 1, conv),
    )
end

candidate_share_hull_vertices_4c(p4) =
    procedure_hull_vertices_4c(p4; convention = :candidate_share)

"""
    procedure_hull_point_4c(p4, s1, s2; convention=:saari)

Evaluate the four-candidate positional procedure hull point for scoring vector
`(1, s1, s2, 0)`. Under `:saari` this equals the raw score tally
`get_4c_w_s(p4, s1, s2)`. Under `:candidate_share` it equals
`q_s_4candidates(p4, s1, s2)`.
"""
function procedure_hull_point_4c(p4, s1, s2; convention = :saari)
    vertices = procedure_hull_vertices_4c(p4; convention = convention)
    lambda1, lambda2, lambda3 = procedure_hull_barycentric_4c(s1, s2; convention = convention)
    return lambda1 .* vertices.vote_for_one .+
        lambda2 .* vertices.vote_for_two .+
        lambda3 .* vertices.vote_for_three
end

candidate_share_hull_point_4c(p4, s1, s2) =
    procedure_hull_point_4c(p4, s1, s2; convention = :candidate_share)

borda_procedure_hull_point_4c(p4; convention = :saari) =
    procedure_hull_point_4c(p4, 2 / 3, 1 / 3; convention = convention)

function _cartesian_candidate_tally_point(point)
    return barycentric_to_cartesian(point)
end

"""
    procedure_hull_4c(p4; labels=("A", "B", "C", "D"), convention=:saari)

Build a structured representation of the four-candidate procedure hull. The
`vertices` and `borda_point` fields use the requested convention; `vertices_q` and
`borda_q` are always candidate-share normalized for plotting inside the
candidate-tally tetrahedron.
"""
function procedure_hull_4c(p4; labels = ("A", "B", "C", "D"), convention = :saari)
    v = validate_profile_vector(p4, 24)
    conv = _normalize_hull_convention(convention)
    label_tuple = _validate_4c_labels(labels)
    vertices = procedure_hull_vertices_4c(v; convention = conv)
    vertices_q = candidate_share_hull_vertices_4c(v)
    vertices_cartesian = (
        vote_for_one = _cartesian_candidate_tally_point(vertices_q.vote_for_one),
        vote_for_two = _cartesian_candidate_tally_point(vertices_q.vote_for_two),
        vote_for_three = _cartesian_candidate_tally_point(vertices_q.vote_for_three),
    )
    borda_point = borda_procedure_hull_point_4c(v; convention = conv)
    borda_q = borda_procedure_hull_point_4c(v; convention = :candidate_share)
    return (
        convention = conv,
        vertices = vertices,
        vertices_q = vertices_q,
        vertices_cartesian = vertices_cartesian,
        borda_point = borda_point,
        borda_q = borda_q,
        borda_cartesian = _cartesian_candidate_tally_point(borda_q),
        borda_barycentric = procedure_hull_barycentric_4c(2 / 3, 1 / 3; convention = conv),
        candidate_share_borda_barycentric = candidate_share_hull_barycentric_4c(2 / 3, 1 / 3),
        labels = label_tuple,
    )
end
