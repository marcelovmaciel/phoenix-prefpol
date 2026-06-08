ENV["MPLBACKEND"] = "Agg"

using Test
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

@testset "Canonical basis" begin
    @test length(CANONICAL_3C_IDS) == 6
    @test length(CANONICAL_4C_IDS) == 24
    @test CANONICAL_3C_IDS[1] == @SVector [1, 2, 3]
    @test CANONICAL_4C_IDS[1] == @SVector [1, 2, 3, 4]
    @test CANONICAL_4C_IDS[24] == @SVector [4, 1, 2, 3]
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
    @test v_freq[2] == 2
    @test sum(v_freq) == 3
    @test isapprox(sum(v_prop), 1.0)
    @test isapprox(v_prop[1], 1 / 3)
    @test isapprox(v_prop[2], 2 / 3)

    wp = Preferences.WeightedProfile(pool, ballots, [1.0, 2.0, 3.0])
    v = profile_vector(wp, basis; normalize = false)
    @test v[1] == 1.0
    @test v[2] == 5.0
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
        1, 0.25, 0.25, 1, 0.10, 0.10, 0, 0, 0, 0, 0, 0,
        0.25, 1, 0.10, 0.10, 1, 0.25, 0.25, 1, 0.10, 0.10, 1, 0.25,
    ]
    @test isapprox(m[1, :], expected_first_row)
end

@testset "4-candidate plurality" begin
    p4 = ones(24)
    @test isapprox(get_4c_w_s(p4, 0, 0), [6, 6, 6, 6])
    @test isapprox(plurality_4c_q_s(p4), [6, 6, 6, 6])
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
    @test any(overlap_data.masks[1] .& overlap_data.masks[2])
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
    @test_throws ArgumentError positional_comparison_region_masks(p4, labels; resolution = 3)
end

@testset "Decomposition" begin
    @test check_basis_identity()
    p = collect(1.0:24.0)
    dec = decompose_profile(p)
    @test isapprox(reconstruct(dec), p)
    cond = group_component(dec, :condorcet)
    @test length(cond) == 24
    coeffs = decomposition_coefficients(ones(24))
    nz = findall(x -> abs(x) > 1e-9, coeffs)
    @test nz == [18]
    @test isapprox(coeffs[18], 1.0)
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
    ax4 = plot_profile_tetrahedron_freqs(ones(24))
    @test ax4 !== nothing
    ax5 = plot_saari_tetrahedron3d()
    @test ax5 !== nothing
    ax6 = plot_positional_comparison_regions(
        ones(24),
        [:Alckmin, :Bolsonaro, :Ciro, :Haddad];
        resolution = 7,
    )
    @test ax6 !== nothing
end
