@testset "PreferenceMeasures weighted/unweighted regression" begin
    pool = pp.CandidatePool([:A, :B, :C])
    abc = pp.StrictRank(pool, [:A, :B, :C])
    acb = pp.StrictRank(pool, [:A, :C, :B])
    cba = pp.StrictRank(pool, [:C, :B, :A])
    bca = pp.StrictRank(pool, [:B, :C, :A])

    measures = (
        pp.ranking_proportions,
        pp.effective_observed_rankings,
        pp.effective_reversal_rankings,
        pp.effective_reversal_ranking_diagnostics,
        pp.ranking_support_diagnostics,
        p -> pp.average_normalized_distance(p, abc),
        pp.can_polarization,
        pp.total_reversal_component,
        pp.reversal_hhi,
        pp.reversal_geometric,
    )

    @testset "unweighted and unit-weighted profiles agree" begin
        profile = pp.Profile(pool, [abc, cba, acb])
        weighted = pp.WeightedProfile(pool, [abc, cba, acb], [1.0, 1.0, 1.0])

        for measure in measures
            @test measure(profile) == measure(weighted)
        end
    end

    @testset "canonical hand-built profiles" begin
        unanimous = pp.Profile(pool, [abc, abc, abc])
        @test pp.ranking_proportions(unanimous) == Dict((:A, :B, :C) => 1.0)
        @test pp.effective_observed_rankings(unanimous) == 1.0
        @test pp.effective_reversal_rankings(unanimous) == 0.0
        @test pp.effective_reversal_ranking_diagnostics(unanimous) == (
            ENRP = 0.0,
            EO = 1.0,
            reversal_to_ranking_effective_ratio = 0.0,
        )
        @test pp.ranking_support_diagnostics(unanimous).n_unique_rankings == 1
        @test pp.average_normalized_distance(unanimous, abc) == 0.0
        @test pp.can_polarization(unanimous) == 0.0
        @test pp.total_reversal_component(unanimous) == 0.0
        @test pp.reversal_hhi(unanimous) == 0.0
        @test pp.reversal_geometric(unanimous) == 0.0

        exact_reversal = pp.Profile(pool, [abc, cba])
        @test pp.ranking_proportions(exact_reversal) == Dict(
            (:A, :B, :C) => 0.5,
            (:C, :B, :A) => 0.5,
        )
        @test pp.effective_observed_rankings(exact_reversal) == 2.0
        @test pp.effective_reversal_rankings(exact_reversal) == 1.0
        @test pp.average_normalized_distance(exact_reversal, abc) == 0.5
        @test pp.can_polarization(exact_reversal) == 1.0
        @test pp.total_reversal_component(exact_reversal) == 1.0
        @test pp.reversal_hhi(exact_reversal) == 1.0
        @test pp.reversal_geometric(exact_reversal) == 1.0

        three_types = pp.Profile(pool, [abc, cba, acb])
        @test pp.ranking_proportions(three_types) == Dict(
            (:A, :B, :C) => 1 / 3,
            (:C, :B, :A) => 1 / 3,
            (:A, :C, :B) => 1 / 3,
        )
        @test pp.effective_observed_rankings(three_types) == 3.0
        @test pp.effective_reversal_rankings(three_types) == 1.0
        @test pp.total_reversal_component(three_types) == 2 / 3
        @test pp.reversal_hhi(three_types) == 1.0
        @test isapprox(pp.reversal_geometric(three_types), sqrt(2 / 3))

        two_reversal_pairs = pp.Profile(pool, [abc, cba, acb, bca])
        @test pp.effective_observed_rankings(two_reversal_pairs) == 4.0
        @test pp.effective_reversal_rankings(two_reversal_pairs) == 2.0
        @test pp.total_reversal_component(two_reversal_pairs) == 1.0
        @test pp.reversal_hhi(two_reversal_pairs) == 0.5
        @test pp.reversal_geometric(two_reversal_pairs) == sqrt(0.5)
    end

    @testset "nonuniform weighted manual expectations" begin
        weighted = pp.WeightedProfile(pool, [abc, cba, acb], [2.0, 1.0, 1.0])

        @test pp.ranking_proportions(weighted) == Dict(
            (:A, :B, :C) => 0.5,
            (:C, :B, :A) => 0.25,
            (:A, :C, :B) => 0.25,
        )
        @test pp.effective_observed_rankings(weighted) == 8 / 3
        @test pp.effective_reversal_rankings(weighted) == 1.0
        @test pp.effective_reversal_ranking_diagnostics(weighted) == (
            ENRP = 1.0,
            EO = 8 / 3,
            reversal_to_ranking_effective_ratio = 3 / 8,
        )
        support = pp.ranking_support_diagnostics(weighted)
        @test support.n_observations == 4
        @test support.n_unique_rankings == 3
        @test support.singleton_rankings == 2
        @test support.singleton_share_of_observations == 0.5
        @test pp.average_normalized_distance(weighted, abc) == 1 / 3
        @test pp.can_polarization(weighted) == 2 / 3
        @test pp.total_reversal_component(weighted) == 0.5
        @test pp.reversal_hhi(weighted) == 1.0
        @test pp.reversal_geometric(weighted) == sqrt(0.5)
    end

    @testset "empty, zero-mass, invalid-weight, and one-candidate behavior" begin
        empty = pp.Profile(pool, pp.StrictRank[])
        @test pp.ranking_proportions(empty) == Dict{Tuple,Float64}()
        @test pp.effective_reversal_rankings(empty) == 0.0
        @test pp.total_reversal_component(empty) == 0.0
        @test pp.reversal_hhi(empty) == 0.0
        @test pp.reversal_geometric(empty) == 0.0
        @test_throws ArgumentError pp.effective_observed_rankings(empty)
        @test_throws ArgumentError pp.ranking_support_diagnostics(empty)
        @test_throws ArgumentError pp.average_normalized_distance(empty, abc)
        @test_throws ArgumentError pp.can_polarization(empty)

        zero_weight = pp.WeightedProfile(pool, [abc, cba, acb], [0.0, 0.0, 0.0])
        @test pp.validate(zero_weight)
        @test pp.ranking_proportions(zero_weight) == Dict{Tuple,Float64}()
        @test pp.effective_reversal_rankings(zero_weight) == 0.0
        @test pp.total_reversal_component(zero_weight) == 0.0
        @test pp.reversal_hhi(zero_weight) == 0.0
        @test pp.reversal_geometric(zero_weight) == 0.0
        @test_throws ArgumentError pp.effective_observed_rankings(zero_weight)
        @test_throws ArgumentError pp.ranking_support_diagnostics(zero_weight)
        @test_throws ArgumentError pp.average_normalized_distance(zero_weight, abc)
        @test_throws ArgumentError pp.can_polarization(zero_weight)

        invalid_weight = pp.WeightedProfile(pool, [abc, cba], [-1.0, 1.0])
        @test_throws ArgumentError pp.validate(invalid_weight)
        @test pp.ranking_proportions(invalid_weight) == Dict{Tuple,Float64}()
        @test pp.total_reversal_component(invalid_weight) == 0.0
        @test_throws ArgumentError pp.effective_observed_rankings(invalid_weight)
        @test_throws ArgumentError pp.average_normalized_distance(invalid_weight, abc)
        @test_throws ArgumentError pp.can_polarization(invalid_weight)

        one_pool = pp.CandidatePool([:A])
        only_a = pp.StrictRank(one_pool, [:A])
        one_candidate = pp.Profile(one_pool, [only_a])
        one_candidate_weighted = pp.WeightedProfile(one_pool, [only_a], [1.0])
        @test pp.ranking_proportions(one_candidate) == Dict((:A,) => 1.0)
        @test pp.ranking_proportions(one_candidate_weighted) == Dict((:A,) => 1.0)
        @test pp.effective_observed_rankings(one_candidate) == 1.0
        @test pp.effective_observed_rankings(one_candidate_weighted) == 1.0
        @test pp.total_reversal_component(one_candidate) == 0.0
        @test pp.total_reversal_component(one_candidate_weighted) == 0.0
        @test_throws ArgumentError pp.average_normalized_distance(one_candidate, only_a)
        @test_throws ArgumentError pp.average_normalized_distance(one_candidate_weighted, only_a)
        @test_throws ArgumentError pp.can_polarization(one_candidate)
        @test_throws ArgumentError pp.can_polarization(one_candidate_weighted)
    end
end
