using Test
using DataFrames
using Preferences
using PreferencePlots

@testset "PreferencePlots" begin
    pool = CandidatePool([:A, :B, :C])
    profile = Profile(pool, [
        StrictRank(pool, [:A, :B, :C]),
        StrictRank(pool, [:A, :C, :B]),
        StrictRank(pool, [:B, :A, :C]),
        StrictRank(pool, [:C, :A, :B]),
    ])
    basis = voter_type_basis(pool; order=:kendall_shell, reference_order=[:A, :B, :C])
    result = majority_graph_support(profile; basis=basis)
    dir = mktempdir()

    path1 = joinpath(dir, "plurality.png")
    plot_plurality_scores(profile; output_path=path1)
    @test isfile(path1)
    @test filesize(path1) > 0

    path2 = joinpath(dir, "margins.png")
    plot_pairwise_margins(result; output_path=path2)
    @test isfile(path2)
    @test filesize(path2) > 0
end

@testset "single-peakedness artifacts" begin
    @test single_peaked_uniform_benchmark(3) ≈ 4 / 6
    @test single_peaked_uniform_benchmark(4) ≈ 8 / 24
    @test single_peaked_uniform_benchmark(5) ≈ 16 / 120
    @test canonical_axis_string("A < B < C") == canonical_axis_string("C < B < A")

    axis_summary = DataFrame(
        year = [2006, 2006, 2006, 2006, 2006, 2006],
        m = [3, 3, 3, 3, 3, 3],
        scenario_name = fill("demo", 6),
        candidate_set = fill("A|B|C", 6),
        imputer_backend = fill("none", 6),
        linearizer_policy = fill("none", 6),
        b = [1, 1, 1, 1, 2, 2],
        r = [1, 1, 1, 1, 1, 1],
        k = [1, 1, 1, 1, 1, 1],
        axis_id = [1, 2, 1, 2, 1, 2],
        axis_as_string = ["A < B < C", "C < B < A", "A < C < B", "B < C < A", "A < B < C", "A < C < B"],
        L0 = [0.25, 0.25, 0.30, 0.35, 0.50, 0.60],
        L1 = [0.10, 0.10, 0.20, 0.25, 0.30, 0.40],
        is_best_L0_axis = [true, true, true, false, true, false],
        is_best_L1_axis = [true, true, true, false, true, false],
    )
    main_values = table_single_peakedness_main_values(axis_summary)
    @test main_values.sp_mass[1] ≈ 1 - main_values.mean_L0[1]

    support_classification = DataFrame(
        year = [2006, 2006, 2006, 2006],
        m = fill(3, 4),
        scenario_name = fill("demo", 4),
        candidate_set = fill("A|B|C", 4),
        b = [1, 1, 1, 1],
        r = [1, 1, 1, 1],
        k = [1, 1, 1, 1],
        axis_id = fill(1, 4),
        unique_ranking_id = 1:4,
        proportion = [0.5, 0.25, 0.15, 0.10],
        distance_to_SP_axis = [0, 1, 1, 2],
    )
    dist = table_single_peakedness_distance_distribution(support_classification)
    @test sum(dist.mass) ≈ 1

    row_classification = DataFrame(
        year = fill(2006, 4),
        m = fill(3, 4),
        scenario_name = fill("demo", 4),
        b = fill(1, 4),
        r = fill(1, 4),
        k = fill(1, 4),
        is_single_peaked = [true, true, false, false],
        PT = ["a", "a", "b", "b"],
    )
    cov = table_single_peakedness_covariates(row_classification, [:PT])
    a = cov[cov.category .== "a", :][1, :]
    @test a.delta_pp ≈ a.sp_share_percent - a.baseline_percent

    dir = mktempdir()
    plot_sp_mass_by_m_year(axis_summary; output_path=joinpath(dir, "sp.png"))
    plot_distance_distribution(support_classification; output_path=joinpath(dir, "distance.png"))
    plot_covariate_exact_fit(row_classification; year=2006, m=3, variable=:PT,
                             output_path=joinpath(dir, "cov.png"))
    @test isfile(joinpath(dir, "sp.png"))
    @test isfile(joinpath(dir, "distance.png"))
    @test isfile(joinpath(dir, "cov.png"))
end
