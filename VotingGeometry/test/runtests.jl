ENV["MPLBACKEND"] = "Agg"

using Test
using LinearAlgebra
using StaticArrays
using Preferences
using VotingGeometry

function _same_point(p, q; atol = 1e-12)
    length(p) == length(q) || return false
    return all(isapprox(Float64(p[i]), Float64(q[i]); atol = atol) for i in eachindex(p))
end

function _axis_text_strings(ax)
    return [string(text.get_text()) for text in collect(ax.texts)]
end

function _legend_text_strings(ax)
    legend = ax.get_legend()
    return [string(text.get_text()) for text in collect(legend.get_texts())]
end

function _zeroish(x; atol = 1e-9)
    return all(abs(Float64(v)) <= atol for v in x)
end

function _component_index(label::Symbol)
    idx = findfirst(==(label), COMPONENT_LABELS)
    idx === nothing && error("unknown component label $label")
    return idx
end

function _profile_with_rankings(rank_counts)
    p = zeros(Float64, 24)
    for (ranking, count) in rank_counts
        idx = findfirst(==(SVector{4,Int}(ranking)), CANONICAL_4C_IDS)
        idx === nothing && error("ranking $ranking not in canonical order")
        p[idx] += Float64(count)
    end
    return p
end

function _all_subsets_4c()
    return (
        [1, 2], [1, 3], [1, 4], [2, 3], [2, 4], [3, 4],
        [1, 2, 3], [1, 2, 4], [1, 3, 4], [2, 3, 4],
        [1, 2, 3, 4],
    )
end

function _normalized_score_vectors(m::Integer)
    vectors = Vector{Vector{Float64}}()
    for k in 1:(m - 1)
        push!(vectors, [i <= k ? 1.0 : 0.0 for i in 1:m])
    end
    push!(vectors, [(m - i) / (m - 1) for i in 1:m])
    return vectors
end

function _subset_score_tally(p4, subset, scores)
    v = [Float64(x) for x in p4]
    subset_vec = collect(subset)
    length(scores) == length(subset_vec) || error("score vector length mismatch")
    tally = zeros(Float64, 4)
    subset_set = Set(subset_vec)
    for (col, order) in pairs(CANONICAL_4C_IDS)
        restricted = [candidate for candidate in order if candidate in subset_set]
        for (position, candidate) in pairs(restricted)
            tally[candidate] += Float64(scores[position]) * v[col]
        end
    end
    return tally
end

function _has_directed_cycle(margins; atol = 1e-9)
    candidates = 1:4
    for a in candidates, b in candidates, c in candidates
        if length(unique((a, b, c))) == 3 &&
                margins[a, b] > atol && margins[b, c] > atol && margins[c, a] > atol
            return true
        end
    end
    for a in candidates, b in candidates, c in candidates, d in candidates
        if length(unique((a, b, c, d))) == 4 &&
                margins[a, b] > atol && margins[b, c] > atol &&
                margins[c, d] > atol && margins[d, a] > atol
            return true
        end
    end
    return false
end

@testset "Canonical basis" begin
    @test length(CANONICAL_3C_IDS) == 6
    @test length(CANONICAL_4C_IDS) == 24
    @test CANONICAL_3C_IDS[1] == @SVector [1, 2, 3]
    @test CANONICAL_4C_IDS[1] == @SVector [1, 2, 3, 4]
    @test CANONICAL_4C_IDS[8] == @SVector [1, 4, 3, 2]
    @test CANONICAL_4C_IDS[18] == @SVector [2, 3, 4, 1]
    @test CANONICAL_4C_IDS[24] == @SVector [1, 2, 4, 3]
end

@testset "Profile vector through Preferences" begin
    pool = Preferences.CandidatePool([:Alckmin, :Bolsonaro, :Ciro, :Haddad])
    ballots = [
        Preferences.StrictRank(pool, [:Alckmin, :Bolsonaro, :Ciro, :Haddad]),
        Preferences.StrictRank(pool, [:Bolsonaro, :Alckmin, :Ciro, :Haddad]),
        Preferences.StrictRank(pool, [:Bolsonaro, :Alckmin, :Ciro, :Haddad]),
    ]
    p = Preferences.Profile(pool, ballots)
    basis = SaariBasis4(pool)
    v_freq = profile_vector(p, basis; normalize = false)
    v_prop = profile_vector(p, basis; normalize = true)
    @test v_freq[1] == 1
    @test v_freq[6] == 2
    @test sum(v_freq) == 3
    @test isapprox(sum(v_prop), 1.0)
    @test isapprox(v_prop[1], 1 / 3)
    @test isapprox(v_prop[6], 2 / 3)

    wp = Preferences.WeightedProfile(pool, ballots, [1.0, 2.0, 3.0])
    v = profile_vector(wp, basis; normalize = false)
    @test v[1] == 1.0
    @test v[6] == 5.0
    @test sum(v) == 6.0
end

@testset "3-candidate scoring" begin
    p3 = [0.20, 0.10, 0.25, 0.15, 0.20, 0.10]
    @test isapprox(sum(plurality_3a(p3)), 1.0)
    @test isapprox(sum(borda_3a(p3)), 1.0)
    @test isapprox(sum(antiplurality_3a(p3)), 1.0)
end

@testset "4-candidate standard vote matrix" begin
    m = standard_vote_matrix(0.25, 0.10)
    expected_first_row = [
        1, 1, 0.25, 0.10, 0.10, 0.25,
        1, 1, 0.25, 0.10, 0.10, 0.25,
        0, 0, 0, 0, 0, 0,
        0.25, 0.10, 0.10, 0.25, 1, 1,
    ]
    @test isapprox(m[1, :], expected_first_row)
end

@testset "4-candidate plurality" begin
    p4 = ones(24)
    @test isapprox(get_4c_w_s(p4, 0, 0), [6, 6, 6, 6])
    @test isapprox(plurality_4c_q_s(p4), [6, 6, 6, 6])
end

@testset "4-candidate procedure hull conventions" begin
    deterministic_profiles = [
        ones(24),
        collect(1.0:24.0),
        [i % 5 == 0 ? 0.0 : Float64(mod(i, 7) + 1) for i in 1:24],
    ]
    random_profiles = [
        [Float64(mod(17 * i + 11 * j, 23) + 1) / 23 for i in 1:24]
        for j in 1:5
    ]
    scoring_parameters = (
        (0.0, 0.0),
        (1.0, 0.0),
        (1.0, 1.0),
        (2 / 3, 1 / 3),
        (0.75, 0.25),
        (0.5, 0.5),
        (0.9, 0.0),
    )

    @test procedure_hull_barycentric_4c(0, 0) == (1.0, 0.0, 0.0)
    @test procedure_hull_barycentric_4c(1, 0) == (0.0, 1.0, 0.0)
    @test procedure_hull_barycentric_4c(1, 1) == (0.0, 0.0, 1.0)
    @test all(isapprox.(procedure_hull_barycentric_4c(2 / 3, 1 / 3), (1 / 3, 1 / 3, 1 / 3)))
    @test all(isapprox.(candidate_share_hull_barycentric_4c(2 / 3, 1 / 3), (1 / 6, 1 / 3, 1 / 2)))
    @test_throws ArgumentError procedure_hull_barycentric_4c(0.2, 0.3)
    @test_throws ArgumentError procedure_hull_barycentric_4c(1.2, 0.1)
    @test_throws ArgumentError procedure_hull_barycentric_4c(0.5, 0.25; convention = :unknown)
    @test_throws ArgumentError procedure_hull_vertices_4c(ones(23))

    for p4 in vcat(deterministic_profiles, random_profiles)
        saari_vertices = procedure_hull_vertices_4c(p4)
        q_vertices = candidate_share_hull_vertices_4c(p4)
        @test isapprox(saari_vertices.vote_for_one, get_4c_w_s(p4, 0, 0))
        @test isapprox(saari_vertices.vote_for_two, get_4c_w_s(p4, 1, 0))
        @test isapprox(saari_vertices.vote_for_three, get_4c_w_s(p4, 1, 1))
        @test isapprox(q_vertices.vote_for_one, plurality_4c_q_s(p4))
        @test isapprox(q_vertices.vote_for_two, vote_for_two_4c_q_s(p4))
        @test isapprox(q_vertices.vote_for_three, antiplurality_4c_q_s(p4))

        for (s1, s2) in scoring_parameters
            lambdas = procedure_hull_barycentric_4c(s1, s2)
            @test all(x -> x >= -1e-12, lambdas)
            @test isapprox(sum(lambdas), 1.0; atol = 1e-12)
            @test isapprox(
                procedure_hull_point_4c(p4, s1, s2),
                get_4c_w_s(p4, s1, s2);
                atol = 1e-10,
            )
            @test isapprox(
                candidate_share_hull_point_4c(p4, s1, s2),
                q_s_4candidates(p4, s1, s2);
                atol = 1e-10,
            )
        end

        borda_raw = borda_procedure_hull_point_4c(p4)
        @test isapprox(
            borda_raw,
            (saari_vertices.vote_for_one .+ saari_vertices.vote_for_two .+ saari_vertices.vote_for_three) ./ 3;
            atol = 1e-10,
        )
    end

    hull = procedure_hull_4c(collect(1.0:24.0); labels = (:A, :B, :C, :D))
    @test hull.convention == :saari
    @test hull.labels == (:A, :B, :C, :D)
    @test isapprox(hull.borda_point, get_4c_w_s(collect(1.0:24.0), 2 / 3, 1 / 3))
    @test isapprox(hull.borda_q, borda_4c_q_s(collect(1.0:24.0)))
    @test all(isapprox.(hull.borda_barycentric, (1 / 3, 1 / 3, 1 / 3)))
    @test all(isapprox.(hull.candidate_share_borda_barycentric, (1 / 6, 1 / 3, 1 / 2)))
    @test _same_point(hull.borda_cartesian, barycentric_to_cartesian(hull.borda_q))
end

@testset "Source procedure-hull example" begin
    # Saari 2002, Mathematical Social Sciences: An Oxymoron?, Sect. 2.3.1-2.3.2.
    p4 = _profile_with_rankings([
        ([1, 2, 3, 4], 2),
        ([1, 3, 4, 2], 1),
        ([1, 4, 3, 2], 2),
        ([3, 2, 4, 1], 2),
        ([4, 2, 3, 1], 3),
    ])
    labels = [:A, :B, :C, :D]
    @test first(winner_order(get_4c_w_s(p4, 0, 0), labels)) == :A
    @test first(winner_order(get_4c_w_s(p4, 1, 0), labels)) == :B
    @test first(winner_order(get_4c_w_s(p4, 1, 1), labels)) == :C
    @test first(winner_order(get_4c_w_s(p4, 2 / 3, 1 / 3), labels)) == :D
end

@testset "Geometry" begin
    p3 = [0.20, 0.10, 0.25, 0.15, 0.20, 0.10]
    @test tern2cart(1, 0, 0) == (0.0, 0.0)
    @test tern2cart(0, 1, 0) == (1.0, 0.0)
    @test isapprox(tern2cart(0, 0, 1)[1], 0.5)
    @test isapprox(tern2cart(0, 0, 1)[2], sqrt(3) / 2)
    pts = triangle_points_from_profile(p3)
    @test haskey(pts, :plurality)
    @test haskey(pts, :antiplurality)
    @test haskey(pts, :borda)

    triangle_centroid = VotingGeometry._triangle_centroid(TRIANGLE_VERTICES)
    @test isapprox(triangle_centroid[1], 0.5)
    @test isapprox(triangle_centroid[2], sqrt(3) / 6)

    opposite_indices = ((2, 3), (1, 3), (1, 2))
    triangle_medians = VotingGeometry._triangle_median_segments(TRIANGLE_VERTICES)
    @test length(triangle_medians) == 3
    for i in 1:3
        opposite = opposite_indices[i]
        @test _same_point(triangle_medians[i][1], TRIANGLE_VERTICES[i])
        @test _same_point(
            triangle_medians[i][2],
            midpoint(TRIANGLE_VERTICES[opposite[1]], TRIANGLE_VERTICES[opposite[2]]),
        )
    end

    vertices = opened_tetrahedron_vertices()
    opened_faces = VotingGeometry._opened_tetrahedron_faces(vertices)
    opened_medians = VotingGeometry._opened_tetrahedron_median_segments(vertices)
    @test length(opened_faces) == 4
    @test length(opened_medians) == 12
    for (face_index, face) in pairs(opened_faces)
        for (vertex_index, vertex) in pairs(face)
            opposite = opposite_indices[vertex_index]
            median = opened_medians[3 * (face_index - 1) + vertex_index]
            @test _same_point(median[1], vertex)
            @test _same_point(median[2], midpoint(face[opposite[1]], face[opposite[2]]))
        end
    end
end

@testset "Positional comparison regions" begin
    p4 = collect(1.0:24.0)
    labels = [:A, :B, :C, :D]
    comparisons = [(:A, ">=", :B), (:C, ">", :D)]
    data = positional_comparison_region_masks(p4, labels; comparisons = comparisons, resolution = 5)

    @test length(data.s1) == 15
    @test length(data.s2) == 15
    @test all(data.s2 .<= data.s1)
    @test length(data.masks) == 2
    @test all(length(mask) == length(data.s1) for mask in data.masks)

    for i in eachindex(data.s1)
        scores = q_s_4candidates(p4, data.s1[i], data.s2[i])
        @test data.masks[1][i] == (scores[1] >= scores[2])
        @test data.masks[2][i] == (scores[3] > scores[4])
    end

    overlap_data = positional_comparison_region_masks(
        ones(24),
        labels;
        comparisons = [(:A, ">=", :B), (:B, ">=", :A)],
        resolution = 3,
    )
    @test all(overlap_data.masks[1] .& overlap_data.masks[2])

    default_data = positional_comparison_region_masks(
        p4,
        [:Alckmin, :Bolsonaro, :Ciro, :Haddad];
        resolution = 3,
    )
    @test [spec.label for spec in default_data.comparisons] == [
        "Bolsonaro >= Ciro",
        "Ciro >= Haddad",
        "Alckmin > Bolsonaro",
    ]

    table = positional_comparison_region_table(p4, labels; comparisons = comparisons, resolution = 5)
    @test length(table) == 2
    @test [row.comparison for row in table] == ["A >= B", "C > D"]
    @test all(row.grid_count == length(data.s1) for row in table)
    @test table[1].true_count == count(data.masks[1])
    @test isapprox(table[1].parameter_space_proportion, count(data.masks[1]) / length(data.s1))
    @test_throws ArgumentError positional_comparison_region_masks(p4, labels; resolution = 3)
end

@testset "Saari source-derived decomposition" begin
    @test check_basis_identity()
    basis_profiles = Float64.(basis_matrix())
    source_basis = source_component_basis_matrix_4c()
    @test isapprox(basis_profiles[:, 12:14], source_basis[:, 1:3])
    @test isapprox(basis_profiles[:, 15:17], source_basis[:, 4:6])
    @test isapprox(basis_profiles[:, 18], source_basis[:, 7])
    @test isapprox(basis_profiles[:, 19:24], source_basis[:, 8:13])
    @test isapprox(basic_profile_differential_4c(4), -sum(basic_profile_differential_4c(i) for i in 1:3))
    @test !isapprox(borda_profile_differential_4c(1), basic_profile_differential_4c(1))

    deterministic_vectors = [
        collect(1.0:24.0),
        ones(24),
        [(-1.0)^i * i / 10 for i in 1:24],
    ]
    random_vectors = [
        [Float64(mod(13 * i + 7 * j, 29) - 14) / 5 for i in 1:24]
        for j in 1:3
    ]

    for p in vcat(deterministic_vectors, random_vectors)
        dec = decompose_profile(p)
        @test isapprox(reconstruct(dec), p; atol = 1e-8)
        @test isapprox(vec(sum(dec.components; dims = 2)), p; atol = 1e-8)
        @test isapprox(sum(dec.components[:, j] for j in 1:24), p; atol = 1e-8)
    end
end

@testset "Kernel and double reversals" begin
    basis_profiles = Float64.(basis_matrix())
    k_idx = _component_index(:K)
    coeffs = decomposition_coefficients(ones(24))
    nz = findall(x -> abs(x) > 1e-9, coeffs)
    @test nz == [k_idx]
    @test isapprox(coeffs[k_idx], 1.0)
    @test isapprox(basis_profiles[:, k_idx], ones(24))

    cases = [(basis_profiles[:, k_idx], true)]
    append!(cases, [(basis_profiles[:, _component_index(label)], false) for label in COMPONENT_GROUPS[:double_reversals]])
    for (v, is_kernel) in cases
        @test _zeroish(VotingGeometry._pairwise_margin_matrix_4c(v))
        for subset in _all_subsets_4c()
            for scores in _normalized_score_vectors(length(subset))
                tally = _subset_score_tally(v, subset, scores)
                if is_kernel
                    @test all(isapprox(tally[c], tally[subset[1]]; atol = 1e-9) for c in subset)
                else
                    @test _zeroish(tally)
                end
            end
        end
    end

    double_basis = basis_profiles[:, [_component_index(label) for label in COMPONENT_GROUPS[:double_reversals]]]
    @test LinearAlgebra.rank(double_basis) == 6
    @test LinearAlgebra.rank(hcat(basis_profiles[:, k_idx], double_basis)) == 7

    # Saari 2000, Eq. 3.4 double-reversal example, up to sign convention.
    eq34 = _profile_with_rankings([
        ([3, 4, 1, 2], 1),
        ([3, 4, 2, 1], -1),
        ([4, 3, 1, 2], -1),
        ([4, 3, 2, 1], 1),
    ])
    @test _zeroish(VotingGeometry._pairwise_margin_matrix_4c(eq34))
    @test all(_zeroish(get_4c_w_s(eq34, s1, s2)) for (s1, s2) in ((0, 0), (1, 0), (1, 1), (2 / 3, 1 / 3)))
end

@testset "Basic profiles" begin
    for c in 1:4
        component = basic_profile_differential_4c(c)
        for (i, order) in pairs(CANONICAL_4C_IDS)
            expected = order[1] == c ? 1.0 : order[4] == c ? -1.0 : 0.0
            @test component[i] == expected
        end

        margins = VotingGeometry._pairwise_margin_matrix_4c(component)
        a = zeros(Float64, 4)
        a[c] = 1.0
        for i in 1:4, j in 1:4
            @test isapprox(margins[i, j], 12.0 * (a[i] - a[j]); atol = 1e-9)
        end

        for subset in _all_subsets_4c()
            for scores in _normalized_score_vectors(length(subset))
                tally = _subset_score_tally(component, subset, scores)
                if c in subset
                    for candidate in subset
                        expected = candidate == c ? 6.0 : -6.0 / (length(subset) - 1)
                        @test isapprox(tally[candidate], expected; atol = 1e-9)
                    end
                else
                    @test _zeroish(tally)
                end
            end
        end
    end
end

@testset "Condorcet differentials" begin
    basis_profiles = Float64.(basis_matrix())
    basic_cols = [basic_profile_differential_4c(i) for i in 1:4]
    double_cols = [double_reversal_differential_4c(edge) for edge in DOUBLE_REVERSAL_EDGE_ORDER_4C]

    for label in COMPONENT_GROUPS[:condorcet]
        component = basis_profiles[:, _component_index(label)]
        for (s1, s2) in ((0, 0), (1, 0), (1, 1), (2 / 3, 1 / 3), (0.75, 0.25))
            @test _zeroish(get_4c_w_s(component, s1, s2))
        end
        margins = VotingGeometry._pairwise_margin_matrix_4c(component)
        @test !_zeroish(margins)
        @test _zeroish(vec(sum(margins; dims = 2)))
        @test _has_directed_cycle(margins) || _has_directed_cycle(-margins)
        @test all(isapprox(dot(component, b), 0.0; atol = 1e-9) for b in basic_cols)
        @test all(isapprox(dot(component, d), 0.0; atol = 1e-9) for d in double_cols)
        @test any(!_zeroish(_subset_score_tally(component, subset, [1.0, 0.0, 0.0])) for subset in _all_subsets_4c() if length(subset) == 3)
    end
end

@testset "Departure and positional deviations" begin
    basis_profiles = Float64.(basis_matrix())
    for label in [COMPONENT_GROUPS[:departure_differentials]...; COMPONENT_GROUPS[:subset_departures]...]
        component = basis_profiles[:, _component_index(label)]
        @test _zeroish(VotingGeometry._pairwise_margin_matrix_4c(component))
        @test _zeroish(get_4c_w_s(component, 2 / 3, 1 / 3))
        @test !_zeroish(get_4c_w_s(component, 0, 0)) || !_zeroish(get_4c_w_s(component, 1, 0))
    end
end

@testset "Plot smoke tests" begin
    p3 = [0.20, 0.10, 0.25, 0.15, 0.20, 0.10]
    ax1 = draw_plain_triangle()
    @test ax1 !== nothing
    ax2 = plot_saari_triangle(p3)
    @test ax2 !== nothing
    required_legend_labels = Set([
        "Plurality",
        "Borda",
        "Antiplurality",
        "Positional procedure line",
    ])
    @test issubset(required_legend_labels, Set(_legend_text_strings(ax2)))
    method_text_labels = Set(["Plurality", "Borda", "Antiplurality"])
    @test isempty(intersect(method_text_labels, Set(_axis_text_strings(ax2))))
    ax3 = draw_opened_tetrahedron()
    @test ax3 !== nothing
    ax3b = plot_opened_representation_tetrahedron()
    @test ax3b !== nothing
    ax4 = plot_profile_tetrahedron_freqs(ones(24))
    @test ax4 !== nothing
    ax4b = plot_profile_on_opened_tetrahedron(ones(24))
    @test ax4b !== nothing
    ax5 = plot_candidate_tally_tetrahedron()
    @test ax5 !== nothing
    ax5b = plot_saari_tetrahedron3d()
    @test ax5b !== nothing
    ax6 = plot_positional_comparison_regions(
        ones(24),
        [:Alckmin, :Bolsonaro, :Ciro, :Haddad];
        resolution = 7,
    )
    @test ax6 !== nothing

    dec = decompose_profile(collect(1.0:24.0))
    label_summary = component_summary(dec; by = :label)
    group_summary = component_summary(dec; by = :group)
    @test length(label_summary) == length(COMPONENT_LABELS)
    @test length(group_summary) == length(COMPONENT_GROUPS)
    @test label_summary[1].label_or_group == COMPONENT_LABELS[1]
    @test group_summary[1].coefficient === nothing
    l1_group_summary = component_summary(dec; by = :group, norm = :l1)
    @test l1_group_summary[1].norm_value == l1_group_summary[1].l1_norm
    @test_throws ArgumentError component_summary(dec; by = :unknown)

    ax7 = plot_decomposition_coefficients(dec)
    @test ax7 !== nothing
    ax8 = plot_decomposition_coefficients(dec; by = :group)
    @test ax8 !== nothing
    ax9 = plot_signed_profile_tetrahedron(group_component(dec, :condorcet))
    @test ax9 !== nothing
    fig1 = plot_decomposition_component_tetrahedra(dec; groups = (:kernel, :condorcet))
    @test fig1 !== nothing
    fig2 = plot_decomposition_reconstruction_check(dec)
    @test fig2 !== nothing
    ax10 = plot_procedure_hull_4c(collect(1.0:24.0))
    @test ax10 !== nothing
    ax10b = plot_procedure_hull_4c(collect(1.0:24.0); convention = :candidate_share)
    @test ax10b !== nothing
    ax11 = plot_procedure_hull_parameter_triangle()
    @test ax11 !== nothing

    VotingGeometry.PythonPlot.close("all")
end
