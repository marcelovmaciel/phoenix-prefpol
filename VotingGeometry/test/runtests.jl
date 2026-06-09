ENV["MPLBACKEND"] = "Agg"

using Test
using LinearAlgebra
using StaticArrays
using Preferences
using SHA
using TOML
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

_axis_title_string(ax) = string(ax.get_title())

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

const _SAARI_FIXTURE_DIR = joinpath(@__DIR__, "fixtures")

_fixture_path(name) = joinpath(_SAARI_FIXTURE_DIR, name)

function _sha256_hex(path)
    return bytes2hex(sha256(read(path)))
end

function _read_saari_order_fixture(path = _fixture_path("saari_fig8_order_4c.csv"))
    lines = readlines(path)
    isempty(lines) && error("empty Saari order fixture")
    lines[1] == "type,A,B,C,D" || error("unexpected Saari order fixture header")
    types = Int[]
    orders = SVector{4,Int}[]
    for line in lines[2:end]
        values = parse.(Int, split(line, ","))
        length(values) == 5 || error("expected 5 columns in Saari order fixture row: $line")
        push!(types, values[1])
        push!(orders, SVector{4,Int}(values[2], values[3], values[4], values[5]))
    end
    return types, orders
end

function _read_int_matrix_fixture(path)
    rows = [parse.(Int, split(line, ",")) for line in readlines(path) if !isempty(strip(line))]
    isempty(rows) && error("empty integer matrix fixture")
    ncols = length(rows[1])
    all(row -> length(row) == ncols, rows) || error("ragged integer matrix fixture")
    out = Matrix{Int}(undef, length(rows), ncols)
    for i in eachindex(rows)
        out[i, :] .= rows[i]
    end
    return out
end


@testset "Saari contract docs" begin
    contract_path = normpath(joinpath(@__DIR__, "..", "docs", "saari_contract.md"))
    @test isfile(contract_path)
    contract_text = read(contract_path, String)
    for needle in ("Saari", "q-space", "profile differentials", "0 <= s2 <= s1 <= 1")
        @test occursin(needle, contract_text)
    end
end

@testset "Saari Theorem 14 fixtures" begin
    manifest_path = _fixture_path("saari_theorem14_manifest.toml")
    manifest = TOML.parsefile(manifest_path)
    @test occursin("Saari 2000", manifest["source_note"])
    @test occursin("pending source recheck", manifest["provenance_note"])
    @test manifest["candidate_encoding"] == "A=1,B=2,C=3,D=4"

    order_meta = manifest["canonical_order"]
    order_path = _fixture_path(order_meta["file"])
    order_types, order_fixture = _read_saari_order_fixture(order_path)
    @test order_meta["rows"] == 24
    @test order_meta["columns"] == 5
    @test _sha256_hex(order_path) == order_meta["sha256"]
    @test order_types == collect(1:24)
    @test length(order_fixture) == order_meta["rows"]

    matrix_meta = manifest["theorem14_t_int"]
    matrix_path = _fixture_path(matrix_meta["file"])
    t_fixture = _read_int_matrix_fixture(matrix_path)
    @test matrix_meta["rows"] == 24
    @test matrix_meta["columns"] == 24
    @test matrix_meta["scale"] == "1/24"
    @test _sha256_hex(matrix_path) == matrix_meta["sha256"]
    @test size(t_fixture) == (matrix_meta["rows"], matrix_meta["columns"])
    @test t_fixture == VotingGeometry._T_INT
end

@testset "Canonical basis" begin
    @test length(CANONICAL_3C_IDS) == 6
    @test length(CANONICAL_4C_IDS) == 24
    @test CANONICAL_3C_IDS[1] == @SVector [1, 2, 3]
    order_types, order_fixture = _read_saari_order_fixture()
    @test order_types == collect(1:24)
    @test collect(CANONICAL_4C_IDS) == order_fixture
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

@testset "Profile vector validators" begin
    signed_zero_sum = [-1.0; 1.0; zeros(22)]
    signed_nonzero_sum = [-1.0; 2.0; zeros(22)]

    @test_throws ArgumentError validate_profile_counts([-1.0; ones(23)], 24)
    @test_throws ArgumentError validate_profile_counts(zeros(24), 24)
    @test validate_profile_counts(zeros(24), 24; require_positive_mass = false) == zeros(24)
    @test validate_profile_differential(signed_zero_sum, 24) == signed_zero_sum
    @test validate_profile_differential(signed_zero_sum, 24; require_zero_sum = true) == signed_zero_sum
    @test_throws ArgumentError validate_profile_differential(signed_nonzero_sum, 24; require_zero_sum = true)
    @test validate_profile_vector(signed_zero_sum, 24) == signed_zero_sum
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
    @test isapprox(raw_score_tally_4c(p4, 0, 0), [6, 6, 6, 6])
    @test isapprox(score_tally_per_rule_mass_4c(p4, 0, 0), [6, 6, 6, 6])
    @test isapprox(plurality_4c_q_s(p4), fill(1 / 4, 4))
    @test isapprox(vote_for_two_4c_q_s(p4), fill(1 / 4, 4))
    @test isapprox(antiplurality_4c_q_s(p4), fill(1 / 4, 4))
    @test isapprox(borda_4c_q_s(p4), fill(1 / 4, 4))
end

@testset "4-candidate q-share normalization" begin
    positive_profiles = [
        ones(24),
        collect(1.0:24.0),
        [Float64(mod(5 * i + 3, 17) + 1) for i in 1:24],
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
    labels = (:A, :B, :C, :D)

    for p4 in positive_profiles, (s1, s2) in scoring_parameters
        raw = get_4c_w_s(p4, s1, s2)
        q = q_s_4candidates(p4, s1, s2)
        rule_mass = 1 + Float64(s1) + Float64(s2)
        normalized_profile = p4 ./ sum(p4)

        @test isapprox(score_tally_per_rule_mass_4c(p4, s1, s2), raw ./ rule_mass; atol = 1e-12)
        @test isapprox(candidate_score_share_4c(p4, s1, s2), q; atol = 1e-12)
        @test isapprox(q, raw ./ (sum(p4) * rule_mass); atol = 1e-12)
        @test isapprox(sum(q), 1.0; atol = 1e-12)
        @test isapprox(q, q_s_4candidates(normalized_profile, s1, s2); atol = 1e-12)
        @test isapprox(q, q_s_4candidates(7.5 .* p4, s1, s2); atol = 1e-12)
        @test winner_order(q, labels) == winner_order(raw, labels)
    end
end


@testset "4-candidate position share and pairwise helpers" begin
    labels = [:A, :B, :C, :D]
    p4 = _profile_with_rankings([
        ([1, 2, 3, 4], 4),
        ([2, 1, 3, 4], 3),
        ([3, 1, 2, 4], 2),
    ])

    coeffs = position_share_coefficients_4c(p4, labels)
    @test Set(keys(coeffs)) == Set(labels)
    @test isapprox(coeffs[:A].constant, 4 / 9; atol = 1e-12)
    @test isapprox(coeffs[:A].s1, 5 / 9; atol = 1e-12)
    @test isapprox(coeffs[:A].s2, 0; atol = 1e-12)
    @test isapprox(coeffs[:B].constant, 3 / 9; atol = 1e-12)
    @test isapprox(coeffs[:B].s1, 4 / 9; atol = 1e-12)
    @test isapprox(coeffs[:B].s2, 2 / 9; atol = 1e-12)
    @test isapprox(coeffs[:C].constant, 2 / 9; atol = 1e-12)
    @test isapprox(coeffs[:C].s1, 0; atol = 1e-12)
    @test isapprox(coeffs[:C].s2, 7 / 9; atol = 1e-12)
    @test coeffs[:D] == (constant = 0.0, s1 = 0.0, s2 = 0.0)

    for (s1, s2) in ((0, 0), (2 / 3, 1 / 3), (3 / 4, 1 / 4))
        q = q_s_4candidates(p4, s1, s2)
        denominator = 1 + Float64(s1) + Float64(s2)
        for (i, label) in pairs(labels)
            c = coeffs[label]
            expected = (c.constant + Float64(s1) * c.s1 + Float64(s2) * c.s2) / denominator
            @test isapprox(q[i], expected; atol = 1e-12)
        end
    end

    pairwise = pairwise_percentages_4c(p4, labels)
    @test length(pairwise) == 12
    @test isapprox(pairwise[(:A, :B)], 100 * 6 / 9; atol = 1e-12)
    @test isapprox(pairwise[(:B, :A)], 100 * 3 / 9; atol = 1e-12)
    @test isapprox(pairwise[(:A, :C)], 100 * 7 / 9; atol = 1e-12)
    @test isapprox(pairwise[(:C, :A)], 100 * 2 / 9; atol = 1e-12)
    @test isapprox(pairwise[(:B, :C)], 100 * 7 / 9; atol = 1e-12)
    @test isapprox(pairwise[(:C, :B)], 100 * 2 / 9; atol = 1e-12)
    @test all(isapprox(pairwise[(a, :D)], 100.0; atol = 1e-12) for a in (:A, :B, :C))
    @test all(isapprox(pairwise[(:D, a)], 0.0; atol = 1e-12) for a in (:A, :B, :C))
    for a in labels, b in labels
        a == b && continue
        @test isapprox(pairwise[(a, b)] + pairwise[(b, a)], 100.0; atol = 1e-12)
    end

    @test condorcet_winner_4c(p4, labels) == :A
    @test condorcet_loser_4c(p4, labels) == :D

    tie_profile = _profile_with_rankings([
        ([1, 2, 3, 4], 1),
        ([2, 1, 3, 4], 1),
    ])
    @test condorcet_winner_4c(tie_profile, labels) === nothing
    @test_throws ArgumentError position_share_coefficients_4c(zeros(24), labels)
    @test_throws ArgumentError pairwise_percentages_4c(p4, [:A, :B, :C, :C])
end

@testset "EJPE Bolsonaro published reference fixture" begin
    reference_path = _fixture_path("ejpe_bolsonaro_reference.toml")
    @test isfile(reference_path)
    reference = TOML.parsefile(reference_path)

    @test reference["candidate_order"] == ["Alckmin", "Bolsonaro", "Ciro", "Haddad"]
    @test reference["activation"]["status"] == "missing_local_data"
    @test reference["activation"]["profile_fixture_present"] == false
    @test occursin("DataFolha", reference["activation"]["required_profile_fixture"])
    @test occursin("Do not fabricate", reference["activation"]["activation_note"])

    expected_coefficients = Dict(
        "Alckmin" => (0.05, 0.51, 0.36),
        "Bolsonaro" => (0.49, 0.04, 0.10),
        "Ciro" => (0.13, 0.33, 0.44),
        "Haddad" => (0.31, 0.10, 0.08),
    )
    coefficients = reference["position_share_coefficients"]
    for (candidate, expected) in expected_coefficients
        observed = coefficients[candidate]
        @test isapprox(observed["constant"], expected[1]; atol = 1e-12)
        @test isapprox(observed["s1"], expected[2]; atol = 1e-12)
        @test isapprox(observed["s2"], expected[3]; atol = 1e-12)
    end
    for key in ("constant", "s1", "s2")
        rounded_slot_sum = sum(coefficients[candidate][key] for candidate in reference["candidate_order"])
        @test isapprox(rounded_slot_sum, 0.98; atol = 1e-12)
    end

    pairwise_rows = reference["pairwise_percentages"]
    @test length(pairwise_rows) == 6
    pairwise = Dict(
        (row["left"], row["right"]) => (row["left_over_right"], row["right_over_left"])
        for row in pairwise_rows
    )
    @test isapprox(pairwise[("Alckmin", "Bolsonaro")][1], 43.57; atol = 1e-12)
    @test isapprox(pairwise[("Bolsonaro", "Haddad")][1], 100.00; atol = 1e-12)
    @test isapprox(pairwise[("Bolsonaro", "Haddad")][2], 0.00; atol = 1e-12)
    @test all(isapprox(left + right, 100.0; atol = 0.02) for (left, right) in values(pairwise))

    @test reference["expected"]["borda_ranking"] == ["Bolsonaro", "Alckmin", "Ciro", "Haddad"]
    @test reference["expected"]["condorcet_winner"] == "Bolsonaro"
    @test reference["expected"]["condorcet_loser"] == "Haddad"
    @test_skip reference["activation"]["profile_fixture_present"] == true
end

@testset "4-candidate public profile validation" begin
    signed = [-1.0; 2.0; ones(22)]
    labels = [:A, :B, :C, :D]

    signed_raw = standard_vote_matrix(0, 0) * signed

    @test_throws ArgumentError get_4c_w_s(signed, 0, 0)
    @test isapprox(get_4c_w_s(signed, 0, 0; allow_signed = true), signed_raw)
    @test_throws ArgumentError raw_score_tally_4c(signed, 0, 0)
    @test isapprox(raw_score_tally_4c(signed, 0, 0; allow_signed = true), signed_raw)
    @test_throws ArgumentError score_tally_per_rule_mass_4c(signed, 0, 0)
    @test isapprox(score_tally_per_rule_mass_4c(signed, 0, 0; allow_signed = true), signed_raw)
    @test_throws ArgumentError candidate_score_share_4c(signed, 0, 0)
    @test_throws ArgumentError q_s_4candidates(signed, 0, 0)
    @test_throws ArgumentError plurality_4c_q_s(signed)
    @test_throws ArgumentError procedure_hull_q_image_vertices_4c(signed)
    @test_throws ArgumentError procedure_hull_q_image_point_4c(signed, 2 / 3, 1 / 3)
    @test_throws ArgumentError procedure_hull_q_image_4c(signed)
    @test_throws ArgumentError candidate_share_hull_vertices_4c(signed)
    @test_throws ArgumentError candidate_share_hull_point_4c(signed, 2 / 3, 1 / 3)
    @test_throws ArgumentError positional_comparison_region_masks(
        signed,
        labels;
        comparisons = [(:A, :B)],
        resolution = 3,
    )
    @test_throws ArgumentError positional_comparison_region_exact(signed, labels; comparison = (:A, :B))
    @test_throws ArgumentError positional_comparison_region_exact_table(signed, labels; comparisons = [(:A, :B)])
    @test_throws ArgumentError plot_profile_tetrahedron_freqs(signed)
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
    @test all(isapprox.(procedure_hull_q_image_barycentric_4c(2 / 3, 1 / 3), (1 / 6, 1 / 3, 1 / 2)))
    @test all(isapprox.(candidate_share_hull_barycentric_4c(2 / 3, 1 / 3), procedure_hull_q_image_barycentric_4c(2 / 3, 1 / 3)))
    @test all(isapprox.(procedure_hull_barycentric_4c(2 / 3, 1 / 3; convention = :candidate_share), (1 / 6, 1 / 3, 1 / 2)))
    @test_throws ArgumentError procedure_hull_barycentric_4c(0.2, 0.3)
    @test_throws ArgumentError procedure_hull_barycentric_4c(1.2, 0.1)
    @test_throws ArgumentError procedure_hull_barycentric_4c(0.5, 0.25; convention = :unknown)
    @test_throws ArgumentError procedure_hull_vertices_4c(ones(23))

    for p4 in vcat(deterministic_profiles, random_profiles)
        saari_vertices = procedure_hull_vertices_4c(p4)
        q_image_vertices = procedure_hull_q_image_vertices_4c(p4)
        q_vertices = candidate_share_hull_vertices_4c(p4)
        @test isapprox(saari_vertices.vote_for_one, get_4c_w_s(p4, 0, 0))
        @test isapprox(saari_vertices.vote_for_two, get_4c_w_s(p4, 1, 0))
        @test isapprox(saari_vertices.vote_for_three, get_4c_w_s(p4, 1, 1))
        @test isapprox(q_image_vertices.vote_for_one, plurality_4c_q_s(p4))
        @test isapprox(q_image_vertices.vote_for_two, vote_for_two_4c_q_s(p4))
        @test isapprox(q_image_vertices.vote_for_three, antiplurality_4c_q_s(p4))
        @test isapprox(q_vertices.vote_for_one, q_image_vertices.vote_for_one)
        @test isapprox(q_vertices.vote_for_two, q_image_vertices.vote_for_two)
        @test isapprox(q_vertices.vote_for_three, q_image_vertices.vote_for_three)

        for (s1, s2) in scoring_parameters
            lambdas = procedure_hull_barycentric_4c(s1, s2)
            @test all(x -> x >= -1e-12, lambdas)
            @test isapprox(sum(lambdas), 1.0; atol = 1e-12)
            raw_point = procedure_hull_point_4c(p4, s1, s2; convention = :saari)
            q_image_point = procedure_hull_q_image_point_4c(p4, s1, s2)
            @test isapprox(
                raw_point,
                get_4c_w_s(p4, s1, s2);
                atol = 1e-10,
            )
            @test isapprox(
                q_image_point,
                q_s_4candidates(p4, s1, s2);
                atol = 1e-10,
            )
            @test isapprox(sum(q_image_point), 1.0; atol = 1e-12)
            @test isapprox(
                candidate_share_hull_point_4c(p4, s1, s2),
                q_image_point;
                atol = 1e-10,
            )
        end

        scaled_p4 = 3.5 .* p4
        raw_reference = procedure_hull_point_4c(p4, 0.75, 0.25; convention = :saari)
        scaled_raw_reference = procedure_hull_point_4c(scaled_p4, 0.75, 0.25; convention = :saari)
        @test isapprox(scaled_raw_reference, 3.5 .* raw_reference; atol = 1e-10)
        @test !isapprox(scaled_raw_reference, raw_reference; atol = 1e-10)
        @test isapprox(
            procedure_hull_q_image_point_4c(scaled_p4, 0.75, 0.25),
            procedure_hull_q_image_point_4c(p4, 0.75, 0.25);
            atol = 1e-12,
        )

        borda_raw = borda_procedure_hull_point_4c(p4)
        @test isapprox(
            borda_raw,
            (saari_vertices.vote_for_one .+ saari_vertices.vote_for_two .+ saari_vertices.vote_for_three) ./ 3;
            atol = 1e-10,
        )
    end

    q_image = procedure_hull_q_image_4c(collect(1.0:24.0); labels = (:A, :B, :C, :D))
    @test q_image.convention == :q_image
    @test q_image.labels == (:A, :B, :C, :D)
    @test isapprox(q_image.borda_point, borda_4c_q_s(collect(1.0:24.0)))
    @test all(isapprox.(q_image.borda_barycentric, (1 / 6, 1 / 3, 1 / 2)))
    @test all(isapprox.(q_image.saari_borda_barycentric, (1 / 3, 1 / 3, 1 / 3)))
    @test _same_point(q_image.borda_cartesian, barycentric_to_cartesian(q_image.borda_point))

    hull = procedure_hull_4c(collect(1.0:24.0); labels = (:A, :B, :C, :D))
    @test hull.convention == :saari
    @test hull.labels == (:A, :B, :C, :D)
    @test isapprox(hull.borda_point, get_4c_w_s(collect(1.0:24.0), 2 / 3, 1 / 3))
    @test isapprox(hull.borda_q, borda_4c_q_s(collect(1.0:24.0)))
    @test all(isapprox.(hull.borda_barycentric, (1 / 3, 1 / 3, 1 / 3)))
    @test all(isapprox.(hull.candidate_share_borda_barycentric, (1 / 6, 1 / 3, 1 / 2)))
    @test isapprox(hull.q_image.borda_point, hull.borda_q)
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

    expected_pairwise = [
        "A >= B",
        "A >= C",
        "A >= D",
        "B >= C",
        "B >= D",
        "C >= D",
    ]
    default_data = positional_comparison_region_masks(p4, labels; resolution = 3)
    @test length(default_data.comparisons) == 6
    @test [spec.label for spec in default_data.comparisons] == expected_pairwise
    @test length(default_data.masks) == 6
    @test all(length(mask) == length(default_data.s1) for mask in default_data.masks)

    paper_labels = [:Alckmin, :Bolsonaro, :Ciro, :Haddad]
    paper_default_data = positional_comparison_region_masks(p4, paper_labels; resolution = 3)
    @test [spec.label for spec in paper_default_data.comparisons] == [
        "Alckmin >= Bolsonaro",
        "Alckmin >= Ciro",
        "Alckmin >= Haddad",
        "Bolsonaro >= Ciro",
        "Bolsonaro >= Haddad",
        "Ciro >= Haddad",
    ]

    ejpe_specs = ejpe_bolsonaro_comparison_specs()
    @test ejpe_specs == [
        ("Bolsonaro", Symbol(">="), "Ciro"),
        ("Ciro", Symbol(">="), "Haddad"),
        ("Alckmin", Symbol(">"), "Bolsonaro"),
    ]
    ejpe_data = positional_comparison_region_masks(
        p4,
        paper_labels;
        comparisons = ejpe_specs,
        resolution = 3,
    )
    @test [spec.label for spec in ejpe_data.comparisons] == [
        "Bolsonaro >= Ciro",
        "Ciro >= Haddad",
        "Alckmin > Bolsonaro",
    ]

    table = positional_comparison_region_table(p4, labels; comparisons = comparisons, resolution = 5)
    @test length(table) == 2
    @test [row.comparison for row in table] == ["A >= B", "C > D"]
    @test all(row.grid_count == length(data.s1) for row in table)
    @test table[1].true_count == count(data.masks[1])
    @test isapprox(table[1].grid_proportion, count(data.masks[1]) / length(data.s1))
    @test table[1].parameter_space_proportion == table[1].grid_proportion

    default_table = positional_comparison_region_table(p4, labels; resolution = 5)
    @test length(default_table) == 6
    @test [row.comparison for row in default_table] == expected_pairwise
end

@testset "Exact positional comparison regions" begin
    labels = [:A, :B, :C, :D]
    triangle = [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0)]

    @test isapprox(polygon_area_2d(triangle), 1 // 2; atol = 1e-12)

    whole_triangle = clip_polygon_halfplane_2d(triangle, 0, 1, -1)
    @test isapprox(polygon_area_2d(whole_triangle), 1 // 2; atol = 1e-12)

    right_half = clip_polygon_halfplane_2d(triangle, -1 // 2, 1, 0)
    @test isapprox(polygon_area_2d(right_half), 3 // 8; atol = 1e-12)
    @test all(point -> point[1] >= 0.5 - 1e-12, right_half)

    tied_profile = ones(24)
    tied_ge = positional_comparison_region_exact(tied_profile, labels; comparison = (:A, ">=", :B))
    tied_gt = positional_comparison_region_exact(tied_profile, labels; comparison = (:A, ">", :B))
    tied_eq = positional_comparison_region_exact(tied_profile, labels; comparison = (:A, "==", :B))
    @test tied_ge.comparison == "A >= B"
    @test isapprox(tied_ge.parameter_space_proportion, 1.0; atol = 1e-12)
    @test tied_gt.region_kind == :strict_identically_false
    @test isapprox(tied_gt.parameter_space_proportion, 0.0; atol = 1e-12)
    @test tied_eq.region_kind == :identically_true
    @test isapprox(tied_eq.parameter_space_proportion, 1.0; atol = 1e-12)

    always_a = _profile_with_rankings([
        ([1, 3, 4, 2], 1),
    ])
    always_coeffs = positional_score_affine_coefficients_4c(always_a, labels, (:A, ">=", :B))
    @test always_coeffs.c0 == 1.0
    @test always_coeffs.c1 == 0.0
    @test always_coeffs.c2 == 0.0
    always_region = positional_comparison_region_exact(always_a, labels; comparison = (:A, ">=", :B))
    never_region = positional_comparison_region_exact(always_a, labels; comparison = (:A, "<=", :B))
    @test isapprox(always_region.parameter_space_proportion, 1.0; atol = 1e-12)
    @test isapprox(never_region.parameter_space_proportion, 0.0; atol = 1e-12)

    threshold_profile = _profile_with_rankings([
        ([2, 1, 3, 4], 1),
        ([1, 2, 3, 4], 1 // 2),
        ([3, 1, 4, 2], 1 // 2),
    ])
    threshold_coeffs = positional_score_affine_coefficients_4c(threshold_profile, labels, (:A, ">=", :B))
    @test isapprox(threshold_coeffs.c0, -1 // 2; atol = 1e-12)
    @test isapprox(threshold_coeffs.c1, 1; atol = 1e-12)
    @test isapprox(threshold_coeffs.c2, 0; atol = 1e-12)

    threshold_region = positional_comparison_region_exact(threshold_profile, labels; comparison = (:A, ">=", :B))
    @test isapprox(threshold_region.area, 3 // 8; atol = 1e-12)
    @test isapprox(threshold_region.parameter_space_proportion, 3 // 4; atol = 1e-12)
    @test all(point -> point[1] >= 0.5 - 1e-12, threshold_region.polygon)

    equality_boundary = positional_comparison_region_exact(threshold_profile, labels; comparison = (:A, "==", :B))
    @test equality_boundary.region_kind == :zero_area_boundary
    @test equality_boundary.area == 0.0
    @test length(equality_boundary.boundary) == 2
    @test all(point -> isapprox(point[1], 0.5; atol = 1e-12), equality_boundary.boundary)

    exact_table = positional_comparison_region_exact_table(
        threshold_profile,
        labels;
        comparisons = [(:A, ">=", :B), (:A, "<", :B)],
    )
    @test [row.comparison for row in exact_table] == ["A >= B", "A < B"]
    @test isapprox(exact_table[1].parameter_space_proportion, 3 // 4; atol = 1e-12)
    @test isapprox(exact_table[2].parameter_space_proportion, 1 // 4; atol = 1e-12)

    exact_default_table = positional_comparison_region_exact_table(threshold_profile, labels)
    @test length(exact_default_table) == 6
    @test [row.comparison for row in exact_default_table] == [
        "A >= B",
        "A >= C",
        "A >= D",
        "B >= C",
        "B >= D",
        "C >= D",
    ]

    grid_table = positional_comparison_region_table(
        threshold_profile,
        labels;
        comparisons = [(:A, ">=", :B)],
        resolution = 401,
    )
    @test isapprox(grid_table[1].grid_proportion, threshold_region.parameter_space_proportion; atol = 0.01)
end

@testset "Saari source-derived decomposition" begin
    @test check_basis_identity()
    t_fixture = _read_int_matrix_fixture(_fixture_path("saari_theorem14_t_int.csv"))
    @test t_fixture == VotingGeometry._T_INT
    @test transformation_matrix() == t_fixture ./ 24.0

    @test length(COMPONENT_METADATA) == length(COMPONENT_LABELS)
    @test [meta.label for meta in COMPONENT_METADATA] == collect(COMPONENT_LABELS)
    @test all(meta -> haskey(COMPONENT_GROUPS, meta.group), COMPONENT_METADATA)
    @test all(meta -> meta.label in COMPONENT_GROUPS[meta.group], COMPONENT_METADATA)
    @test all(meta -> meta.group isa Symbol, COMPONENT_METADATA)
    @test all(meta -> meta.description isa String && !isempty(meta.description), COMPONENT_METADATA)
    @test all(meta -> meta.source isa String && !isempty(meta.source), COMPONENT_METADATA)

    subset_metadata = COMPONENT_METADATA[4:11]
    @test [meta.label for meta in subset_metadata] == collect(COMPONENT_GROUPS[:subset_departures])
    @test [meta.source_index for meta in subset_metadata] == collect(1:8)
    @test all(meta -> occursin("pending verification", meta.description), subset_metadata)
    @test component_metadata(:departure_subset_1).source_index == 1

    dec_compat = decompose_profile(collect(1.0:24.0))
    for label in COMPONENT_LABELS
        @test isapprox(
            reconstruct(dec_compat; labels = label),
            dec_compat.components[:, _component_index(label)];
            atol = 1e-12,
        )
    end
    @test isapprox(
        reconstruct(dec_compat; labels = COMPONENT_GROUPS[:subset_departures]),
        group_component(dec_compat, :subset_departures);
        atol = 1e-12,
    )

    basis_profiles = Float64.(basis_matrix())
    source_basis = source_component_basis_matrix_4c()
    @test size(source_basis) == (24, 13)
    for candidate_id in 1:3
        @test source_basis[:, candidate_id] == basic_profile_differential_4c(candidate_id)
    end
    for (j, order) in pairs(CONDORCET_GENERATORS_4C)
        @test source_basis[:, 3 + j] == condorcet_profile_differential_4c(order)
    end
    @test source_basis[:, 7] == kernel_profile_4c()
    for (j, edge) in pairs(DOUBLE_REVERSAL_EDGE_ORDER_4C)
        @test source_basis[:, 7 + j] == double_reversal_differential_4c(edge)
    end
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
    @test all(_zeroish(get_4c_w_s(eq34, s1, s2; allow_signed = true)) for (s1, s2) in ((0, 0), (1, 0), (1, 1), (2 / 3, 1 / 3)))
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
            @test _zeroish(get_4c_w_s(component, s1, s2; allow_signed = true))
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
        @test _zeroish(get_4c_w_s(component, 2 / 3, 1 / 3; allow_signed = true))
        @test !_zeroish(get_4c_w_s(component, 0, 0; allow_signed = true)) || !_zeroish(get_4c_w_s(component, 1, 0; allow_signed = true))
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
    @test _axis_title_string(ax6) == "Diagnostic grid comparison regions over Saari parameter space"
    ax6_ejpe = plot_positional_comparison_regions(
        ones(24),
        [:Alckmin, :Bolsonaro, :Ciro, :Haddad];
        comparisons = ejpe_bolsonaro_comparison_specs(),
        resolution = 7,
    )
    @test ax6_ejpe !== nothing
    ax6b = plot_positional_comparison_regions_exact(
        ones(24),
        [:Alckmin, :Bolsonaro, :Ciro, :Haddad],
    )
    @test ax6b !== nothing
    @test _axis_title_string(ax6b) == "Exact half-plane clipped regions over Saari parameter space"
    ax6b_ejpe = plot_positional_comparison_regions_exact(
        ones(24),
        [:Alckmin, :Bolsonaro, :Ciro, :Haddad];
        comparisons = ejpe_bolsonaro_comparison_specs(),
    )
    @test ax6b_ejpe !== nothing

    dec = decompose_profile(collect(1.0:24.0))
    label_summary = component_summary(dec; by = :label)
    group_summary = component_summary(dec; by = :group)
    description_summary = component_summary(dec; by = :label, label_style = :description)
    group_description_summary = component_summary(dec; by = :group, label_style = :description)
    @test length(label_summary) == length(COMPONENT_LABELS)
    @test length(group_summary) == length(COMPONENT_GROUPS)
    @test label_summary[1].label_or_group == COMPONENT_LABELS[1]
    @test label_summary[1].label == COMPONENT_LABELS[1]
    @test label_summary[1].description == component_metadata(COMPONENT_LABELS[1]).description
    @test description_summary[1].label_or_group == component_metadata(COMPONENT_LABELS[1]).description
    @test description_summary[4].source_index == 1
    @test occursin("pending verification", description_summary[4].label_or_group)
    @test group_summary[1].coefficient === nothing
    @test group_description_summary[1].group == :departure_differentials
    @test occursin("departure", group_description_summary[1].label_or_group)
    l1_group_summary = component_summary(dec; by = :group, norm = :l1)
    @test l1_group_summary[1].norm_value == l1_group_summary[1].l1_norm
    @test_throws ArgumentError component_summary(dec; by = :unknown)
    @test_throws ArgumentError component_summary(dec; label_style = :unknown)

    ax7 = plot_decomposition_coefficients(dec)
    @test ax7 !== nothing
    ax7b = plot_decomposition_coefficients(dec; label_style = :description)
    @test ax7b !== nothing
    ax8 = plot_decomposition_coefficients(dec; by = :group)
    @test ax8 !== nothing
    ax9 = plot_signed_profile_tetrahedron(group_component(dec, :condorcet))
    @test ax9 !== nothing
    fig1 = plot_decomposition_component_tetrahedra(dec; groups = (:kernel, :condorcet))
    @test fig1 !== nothing
    fig2 = plot_decomposition_reconstruction_check(dec)
    @test fig2 !== nothing
    ax10 = plot_procedure_hull_q_image_4c(collect(1.0:24.0))
    @test ax10 !== nothing
    @test _axis_title_string(ax10) == "q-normalized image of the 4-candidate Saari procedure hull"
    ax10b = plot_procedure_hull_4c(collect(1.0:24.0); convention = :candidate_share)
    @test ax10b !== nothing
    @test _axis_title_string(ax10b) == "q-normalized image of the 4-candidate Saari procedure hull"
    ax11 = plot_procedure_hull_parameter_triangle()
    @test ax11 !== nothing
    @test _axis_title_string(ax11) == "4-candidate Saari parameter space"

    VotingGeometry.PythonPlot.close("all")
end
