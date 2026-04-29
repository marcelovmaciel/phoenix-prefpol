@testset "single-peaked endpoint test" begin
    axis = [:A, :B, :C, :D]
    @test pp.is_single_peaked([:C, :B, :D, :A], axis)
    @test !pp.is_single_peaked([:C, :A, :D, :B], axis)
    @test_throws ArgumentError pp.is_single_peaked([:C, :A, :D, :D], axis)
    @test_throws ArgumentError pp.is_single_peaked([:C, :A, :D], axis)
end

@testset "axes up to reversal counts" begin
    @test length(pp.axes_up_to_reversal([:A])) == 1
    @test length(pp.axes_up_to_reversal([:A, :B, :C])) == 3
    @test length(pp.axes_up_to_reversal([:A, :B, :C, :D])) == 12
    @test length(pp.axes_up_to_reversal([:A, :B, :C, :D, :E])) == 60
end

@testset "single-peaked ranking generation" begin
    for m in 1:5
        axis = collect(1:m)
        @test length(pp.single_peaked_rankings(axis)) == 2^(m - 1)
    end

    axis = [:A, :B, :C, :D]
    expected = Set([
        [:D, :C, :B, :A],
        [:C, :D, :B, :A],
        [:C, :B, :D, :A],
        [:B, :C, :D, :A],
        [:C, :B, :A, :D],
        [:B, :C, :A, :D],
        [:B, :A, :C, :D],
        [:A, :B, :C, :D],
    ])
    @test Set(pp.single_peaked_rankings(axis)) == expected
end

@testset "single-peaked L0 and L1 measures use proportions" begin
    pool = pp.CandidatePool([:A, :B, :C, :D])
    sp1 = pp.StrictRank(pool, [:C, :B, :D, :A])
    sp2 = pp.StrictRank(pool, [:A, :B, :C, :D])
    nonsp = pp.StrictRank(pool, [:C, :A, :D, :B])
    axis = [1, 2, 3, 4]

    all_sp = pp.Profile(pool, [sp1, sp2, sp1])
    res_all = pp.single_peakedness_summary(all_sp; axes = [axis])
    @test res_all.best_L0 ≈ 0.0
    @test res_all.best_L1 ≈ 0.0

    mixed = pp.Profile(pool, [sp1, sp1, sp2, nonsp])
    res_mixed = pp.single_peakedness_summary(mixed; axes = [axis])
    @test res_mixed.best_L0 ≈ 0.25
    @test length(res_mixed.support) == 3
    @test sum(entry.proportion for entry in res_mixed.support) ≈ 1.0

    repeated = pp.Profile(pool, [sp1, sp1, nonsp, nonsp, nonsp])
    weighted = pp.WeightedProfile(pool, [sp1, nonsp], [2.0, 3.0])
    res_repeated = pp.single_peakedness_summary(repeated; axes = [axis])
    res_weighted = pp.single_peakedness_summary(weighted; axes = [axis])
    @test res_repeated.best_L0 ≈ res_weighted.best_L0
    @test res_repeated.best_L1 ≈ res_weighted.best_L1

    support = pp.profile_distribution(weighted)
    @test [entry.proportion for entry in support] ≈ [0.4, 0.6]
    @test [entry.survey_weight_sum for entry in support] == [2.0, 3.0]
end

@testset "weighted profile masses normalize before scoring" begin
    pool = pp.CandidatePool([:A, :B, :C, :D])
    r1 = pp.StrictRank(pool, [:A, :B, :C, :D])
    r2 = pp.StrictRank(pool, [:D, :C, :B, :A])
    r3 = pp.StrictRank(pool, [:C, :A, :D, :B])
    profile = pp.WeightedProfile(pool, [r1, r2, r3], [2.0, 3.0, 5.0])

    support = pp.profile_distribution(profile)
    @test [entry.proportion for entry in support] ≈ [0.2, 0.3, 0.5]

    res = pp.single_peakedness_summary(profile; axes = [[1, 2, 3, 4]])
    @test res.best_L0 ≈ 0.5
end

@testset "single-peaked Kendall distance" begin
    axis = [:A, :B, :C, :D]
    @test pp.single_peaked_distance([:C, :B, :D, :A], axis) == 0
    @test pp.single_peaked_distance([:C, :A, :B, :D], axis) == 1
end

@testset "best L0 and L1 axes are stored separately" begin
    pool = pp.CandidatePool([:A, :B, :C, :D])
    profile = pp.Profile(pool, [
        pp.StrictRank(pool, [:A, :B, :C, :D]),
        pp.StrictRank(pool, [:C, :A, :D, :B]),
    ])
    res = pp.single_peakedness_summary(profile)
    @test res.best_L0_axis_ids isa Vector{Int}
    @test res.best_L1_axis_ids isa Vector{Int}
    @test hasproperty(res, :best_L0_axis_ids)
    @test hasproperty(res, :best_L1_axis_ids)
end
