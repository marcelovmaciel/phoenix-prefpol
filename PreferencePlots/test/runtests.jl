using Test
using CSV
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
    @test main_values.n_realizations[1] == 2

    support_classification = DataFrame(
        year = repeat([2006], 8),
        m = fill(3, 8),
        scenario_name = fill("demo", 8),
        candidate_set = fill("A|B|C", 8),
        imputer_backend = fill("none", 8),
        linearizer_policy = fill("none", 8),
        b = fill(1, 8),
        r = fill(1, 8),
        k = fill(1, 8),
        axis_id = [1, 1, 1, 1, 2, 2, 2, 2],
        unique_ranking_id = repeat(1:4, 2),
        proportion = [0.5, 0.25, 0.15, 0.10, 0.10, 0.20, 0.30, 0.40],
        distance_to_SP_axis = [0, 1, 1, 2, 0, 1, 2, 2],
    )
    axis_pick = DataFrame(
        year = [2006, 2006],
        m = [3, 3],
        scenario_name = fill("demo", 2),
        candidate_set = fill("A|B|C", 2),
        imputer_backend = fill("none", 2),
        linearizer_policy = fill("none", 2),
        b = [1, 1],
        r = [1, 1],
        k = [1, 1],
        axis_id = [1, 2],
        L0 = [0.50, 0.10],
        L1 = [0.20, 0.10],
        is_best_L0_axis = [false, true],
        is_best_L1_axis = [false, true],
    )
    dist = table_single_peakedness_distance_distribution(support_classification, axis_pick)
    @test sum(dist.mass) ≈ 1
    @test dist[dist.distance .== 0, :mass][1] ≈ 0.10
    @test dist[dist.distance .== 2, :mass][1] ≈ 0.70

    tied_axes = vcat(axis_pick, DataFrame(
        year = [2006],
        m = [3],
        scenario_name = ["demo"],
        candidate_set = ["A|B|C"],
        imputer_backend = ["none"],
        linearizer_policy = ["none"],
        b = [1],
        r = [1],
        k = [1],
        axis_id = [3],
        L0 = [0.10],
        L1 = [0.15],
        is_best_L0_axis = [true],
        is_best_L1_axis = [false],
    ); cols=:union)
    support_tied = vcat(support_classification, DataFrame(
        year = repeat([2006], 4),
        m = fill(3, 4),
        scenario_name = fill("demo", 4),
        candidate_set = fill("A|B|C", 4),
        imputer_backend = fill("none", 4),
        linearizer_policy = fill("none", 4),
        b = fill(1, 4),
        r = fill(1, 4),
        k = fill(1, 4),
        axis_id = fill(3, 4),
        unique_ranking_id = 1:4,
        proportion = [0.80, 0.10, 0.05, 0.05],
        distance_to_SP_axis = [0, 1, 2, 3],
    ); cols=:union)
    tied_dist = table_single_peakedness_distance_distribution(support_tied, tied_axes)
    @test tied_dist[tied_dist.distance .== 0, :mass][1] ≈ 0.10
    @test sum(tied_dist.mass) ≈ 1

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

@testset "single-peakedness cached composable outputs" begin
    repo_root = normpath(joinpath(@__DIR__, "..", ".."))
    cache_dirs = [
        joinpath(repo_root, "PrefPol", "composable_running", "output", "paper_b2", "single_peakedness_m3_m4"),
        joinpath(repo_root, "PrefPol", "composable_running", "output", "paper_b2", "single_peakedness"),
    ]
    required = [
        "single_peakedness_axis_summary.csv",
        "single_peakedness_best_axes.csv",
        "single_peakedness_support_classification.csv",
        "single_peakedness_row_classification.csv",
    ]
    if all(all(isfile(joinpath(dir, file)) for file in required) for dir in cache_dirs)
        outputs = load_single_peakedness_outputs(cache_dirs)

        main_values = table_single_peakedness_main_values(outputs.axis_summary)
        @test all(main_values.n_realizations .== 32)
        @test nrow(main_values) == 9
        @test all(main_values.uniform_benchmark .≈ [single_peaked_uniform_benchmark(Int(m)) for m in main_values.m])

        dist = table_single_peakedness_distance_distribution(outputs)
        mass_by_ym = combine(groupby(dist, [:year, :m]), :mass => sum => :mass)
        @test all(isapprox.(mass_by_ym.mass, 1.0; atol=1e-8))

        direct_dist = table_single_peakedness_distance_distribution(outputs.support_classification,
                                                                   outputs.axis_summary)
        @test nrow(direct_dist) == nrow(dist)
        @test all(direct_dist.mass .≈ dist.mass)

        pipeline = table_single_peakedness_pipeline_variation(outputs.axis_summary)
        @test Set(humanize_pipeline_label(row.imputer_backend, row.linearizer_policy) for row in eachrow(pipeline)) ==
              Set(["mice + pattern conditional", "mice + random ties",
                   "random + pattern conditional", "random + random ties"])

        cov = table_single_peakedness_covariates(outputs.row_classification,
            Dict(2006 => [:PT], 2018 => [:LulaScoreGroup, :Ideology],
                 2022 => [:PT, :Ideology, :Abortion]);
            m_values=[5])
        @test all([:year, :variable, :category, :sp_share_percent, :delta_pp, :rows] .∈ Ref(propertynames(cov)))
    else
        @info "Skipping cached composable-output smoke tests; full single-peakedness CSV cache is absent." cache_dirs
    end
end
