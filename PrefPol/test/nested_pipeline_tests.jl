using Test
using PrefPol
using DataFrames
using JLD2
using OrderedCollections: OrderedDict
using Random
using Statistics

const _NESTED_RESOLVE_DF = DataFrame(
    A = [10, 1, 1],
    B = [1, 10, 1],
    C = [1, 1, 10],
    D = [96, 96, 1],
    peso = [10.0, 5.0, 1.0],
    grp = ["x", "y", "x"],
)

const _NESTED_RUN_DF = DataFrame(
    A = [10, 10, 9, 9, 10, 2, 2, 3, 3, 2],
    B = [8, 8, 7, 7, 8, 6, 6, 7, 7, 6],
    C = [6, 6, 5, 5, 6, 10, 10, 9, 9, 10],
    grp = ["x", "x", "x", "x", "x", "y", "y", "y", "y", "y"],
    peso = fill(1.0, 10),
)

@eval PrefPol begin
    function __nested_candidate_loader__(path; candidates)
        return deepcopy($(_NESTED_RESOLVE_DF))
    end

    function __nested_run_loader__(path; candidates)
        return deepcopy($(_NESTED_RUN_DF))
    end
end

ranking_dict(order::Vector{Symbol}) = Dict(candidate => rank for (rank, candidate) in enumerate(order))

function _legacy_raw_group_coherence(bundle, demo::Symbol)
    group_values = PrefPol._metadata_column(bundle, demo)
    grouped = PrefPol._group_row_indices(group_values)
    total_n = length(group_values)
    total_n > 0 || error("test fixture produced an empty grouping")

    c_raw = 0.0

    for (_, idxs) in grouped
        subbundle = PrefPol.subset_annotated_profile(bundle, idxs)
        profile = PrefPol.strict_profile(subbundle)
        result = PrefPol.Preferences.consensus_kendall(
            profile;
            cache = PrefPol.Preferences.GLOBAL_LINEAR_ORDER_CACHE,
        )
        c_raw += (1.0 - result.avg_normalized_distance) * (length(idxs) / total_n)
    end

    return c_raw
end

@testset "tree variance decomposition follows the explicit BRK identity" begin
    leaf_table = DataFrame(
        measure = fill(:M, 8),
        grouping = fill(missing, 8),
        b = repeat(1:2; inner = 4),
        r = repeat(1:2; inner = 2, outer = 2),
        k = repeat(1:2; outer = 4),
        value = [
            -3.5, -2.5, 0.5, 1.5,
            -1.5, -0.5, 2.5, 3.5,
        ],
    )

    table = PrefPol.tree_variance_decomposition_table(leaf_table)
    row = table[1, :]

    @test row.measure == :M
    @test ismissing(row.grouping)
    @test isapprox(row.estimate, 0.0; atol = 1e-12)
    @test isapprox(row.bootstrap_variance, 1.0; atol = 1e-12)
    @test isapprox(row.imputation_variance, 4.0; atol = 1e-12)
    @test isapprox(row.linearization_variance, 0.25; atol = 1e-12)
    @test isapprox(row.total_variance, 5.25; atol = 1e-12)
    @test isapprox(
        row.total_variance,
        row.bootstrap_variance + row.imputation_variance + row.linearization_variance;
        atol = 1e-12,
    )
    @test isapprox(row.bootstrap_share, 1 / 5.25; atol = 1e-12)
    @test isapprox(row.imputation_share, 4 / 5.25; atol = 1e-12)
    @test isapprox(row.linearization_share, 0.25 / 5.25; atol = 1e-12)
end

@testset "tree variance decomposition returns zero shares when total variance is zero" begin
    leaf_table = DataFrame(
        measure = fill(:flat, 8),
        grouping = fill(missing, 8),
        b = repeat(1:2; inner = 4),
        r = repeat(1:2; inner = 2, outer = 2),
        k = repeat(1:2; outer = 4),
        value = fill(3.0, 8),
    )

    row = PrefPol.tree_variance_decomposition_table(leaf_table)[1, :]
    @test row.total_variance == 0.0
    @test row.bootstrap_share == 0.0
    @test row.imputation_share == 0.0
    @test row.linearization_share == 0.0
end

@testset "nested pipeline resolves candidate set before stochastic stages" begin
    cfg = PrefPol.ElectionConfig(
        2022,
        "__nested_candidate_loader__",
        "/tmp/unused",
        4,
        [3],
        2,
        3,
        123,
        ["A", "B", "C", "D"],
        ["grp"],
        [PrefPol.Scenario("front", ["D"])],
    )
    wave = PrefPol.SurveyWaveConfig(cfg; wave_id = "candidate-wave")

    active = PrefPol.resolve_active_candidate_set(wave; scenario_name = "front", m = 3)
    @test active == ["D", "A", "B"]
    @test_throws ArgumentError PrefPol.resolve_active_candidate_set(
        wave;
        scenario_name = "front",
        m = 5,
    )

    spec = PrefPol.build_pipeline_spec(
        wave;
        scenario_name = "front",
        m = 3,
        groupings = [:grp],
        measures = [:Psi],
        B = 1,
        R = 1,
        K = 1,
        imputer_backend = :zero,
    )
    @test spec.active_candidates == ["D", "A", "B"]
end

@testset "nested grouped measure normalization accepts S and S_old" begin
    @test PrefPol._normalize_measure_list([:S, :S_old, :S, "Psi"]) == [:S, :S_old, :Psi]

    cfg = PrefPol.ElectionConfig(
        2022,
        "__nested_run_loader__",
        "/tmp/unused",
        3,
        [3],
        3,
        3,
        123,
        ["A", "B", "C"],
        ["grp"],
        [PrefPol.Scenario("all", String[])],
    )
    wave = PrefPol.SurveyWaveConfig(cfg; wave_id = "measure-wave")

    spec = PrefPol.build_pipeline_spec(
        wave;
        active_candidates = ["A", "B", "C"],
        groupings = [:grp],
        measures = [:S, :S_old],
        B = 1,
        R = 1,
        K = 1,
        imputer_backend = :zero,
    )
    @test spec.measures == [:S, :S_old]

    @test_throws ArgumentError PrefPol.build_pipeline_spec(
        wave;
        active_candidates = ["A", "B", "C"],
        groupings = Symbol[],
        measures = [:S_old],
        B = 1,
        R = 1,
        K = 1,
        imputer_backend = :zero,
    )
end

@testset "nested pipeline run preserves BRK lineage and zeroes inactive variance layers" begin
    cfg = PrefPol.ElectionConfig(
        2022,
        "__nested_run_loader__",
        "/tmp/unused",
        3,
        [3],
        3,
        3,
        123,
        ["A", "B", "C"],
        ["grp"],
        [PrefPol.Scenario("all", String[])],
    )
    wave = PrefPol.SurveyWaveConfig(cfg; wave_id = "run-wave")

    mktempdir() do dir
        pipeline = PrefPol.NestedStochasticPipeline([wave]; cache_root = dir)
            spec = PrefPol.build_pipeline_spec(
                wave;
                active_candidates = ["A", "B", "C"],
                groupings = [:grp],
                measures = [:Psi, :R, :HHI, :RHHI, :C, :D, :D_median, :O, :O_smoothed, :Sep, :G, :Gsep, :S, :S_old],
                B = 3,
                R = 2,
                K = 2,
                imputer_backend = :zero,
            consensus_tie_policy = :average,
        )

        result = PrefPol.run_pipeline(pipeline, spec; force = true)

        @test result.spec.wave_id == spec.wave_id
        @test result.spec.active_candidates == spec.active_candidates
        @test nrow(result.measure_cube) == spec.B * spec.R * spec.K * length(spec.measures)
        @test all(isfile, result.stage_manifest.path)
        @test nrow(result.stage_manifest) == 2 + spec.B + spec.B * spec.R + 2 * spec.B * spec.R * spec.K
        @test Set(result.measure_cube.measure) == Set(spec.measures)
        @test all(isfinite, result.pooled_summaries.estimate)
        summary_cols = propertynames(result.pooled_summaries)
        @test :bootstrap_variance in summary_cols
        @test :imputation_variance in summary_cols
        @test :linearization_variance in summary_cols
        @test :bootstrap_share in summary_cols
        @test :imputation_share in summary_cols
        @test :linearization_share in summary_cols
        @test all(result.pooled_summaries.bootstrap_variance .≈ result.pooled_summaries.V_res)
        @test all(result.pooled_summaries.imputation_variance .≈ result.pooled_summaries.V_imp)
        @test all(result.pooled_summaries.linearization_variance .≈ result.pooled_summaries.V_lin)
        @test all(
            isapprox.(
                result.pooled_summaries.total_variance,
                result.pooled_summaries.bootstrap_variance .+
                result.pooled_summaries.imputation_variance .+
                result.pooled_summaries.linearization_variance;
                atol = 1e-12,
                rtol = 1e-10,
            ),
        )
        @test all(result.pooled_summaries.V_imp .≈ 0.0)
        @test all(result.pooled_summaries.V_lin .≈ 0.0)

        csv_path = joinpath(dir, "variance_decomposition.csv")
        saved_path = PrefPol.save_pipeline_variance_decomposition_csv(csv_path, result)
        @test saved_path == csv_path
        @test isfile(csv_path)
        csv_text = read(csv_path, String)
        @test occursin("bootstrap_variance", csv_text)
        @test occursin("linearization_share", csv_text)
    end
end

@testset "nested pipeline exposes consensus tie policy for group measures" begin
    df = DataFrame(
        profile = [
            Dict(:A => 1, :B => 2, :C => 3),
            Dict(:B => 1, :C => 2, :A => 3),
            Dict(:C => 1, :A => 2, :B => 3),
            Dict(:A => 1, :B => 2, :C => 3),
            Dict(:A => 1, :C => 2, :B => 3),
            Dict(:C => 1, :B => 2, :A => 3),
        ],
        grp = ["x", "x", "x", "y", "y", "y"],
    )
    metadata!(df, "candidates", [:A, :B, :C])
    metadata!(df, "profile_kind", "linearized")

    bundle = PrefPol.dataframe_to_annotated_profile(df)
    avg = PrefPol.compute_group_measure_details(bundle, :grp; tie_policy = :average)
    hash = PrefPol.compute_group_measure_details(bundle, :grp; tie_policy = :hash)
    interval = PrefPol.compute_group_measure_details(bundle, :grp; tie_policy = :interval)
    raw_C = _legacy_raw_group_coherence(bundle, :grp)

    @test avg.diagnostics.tied_groups >= 1
    @test isapprox(avg.C, (2 * raw_C) - 1)
    @test isapprox(hash.C, avg.C)
    @test isapprox(interval.C, avg.C)
    @test 0.0 <= avg.C <= 1.0
    @test interval.D_lo <= interval.D <= interval.D_hi
    @test avg.D_median == hash.D_median
    @test hash.D_median == interval.D_median
    @test interval.D_median_lo == interval.D_median
    @test interval.D_median == interval.D_median_hi
    @test 0.0 <= avg.D_median <= 1.0
    @test 0.0 <= avg.O <= 1.0
    @test 0.0 <= avg.O_smoothed <= 1.0
    @test avg.O_smoothed == hash.O_smoothed
    @test hash.O_smoothed == interval.O_smoothed
    @test interval.O_smoothed_lo == interval.O_smoothed
    @test interval.O_smoothed == interval.O_smoothed_hi
    @test 0.0 <= avg.Sep <= 1.0
    @test isapprox(avg.S, PrefPol.Preferences.overall_sstar_from_CD(avg.C, avg.D))
    @test avg.S_old == hash.S_old
    @test hash.S_old == interval.S_old
    @test interval.S_lo <= interval.S <= interval.S_hi
    @test isapprox(interval.S_lo, PrefPol.Preferences.overall_sstar_from_CD(interval.C, interval.D_lo))
    @test isapprox(interval.S_hi, PrefPol.Preferences.overall_sstar_from_CD(interval.C, interval.D_hi))
    @test interval.S_old_lo == interval.S_old
    @test interval.S_old == interval.S_old_hi
    @test isapprox(avg.Gsep, sqrt(max(avg.C * avg.Sep, 0.0)))
    @test isapprox(avg.G, sqrt(max(avg.C * avg.D, 0.0)))
    @test interval.G_lo <= interval.G <= interval.G_hi
    @test isapprox(interval.G_lo, sqrt(max(interval.C * interval.D_lo, 0.0)))
    @test isapprox(interval.G_hi, sqrt(max(interval.C * interval.D_hi, 0.0)))
    @test interval.D_lo <= hash.D <= interval.D_hi
end

@testset "group measures agree across tie policies on tie-free slices" begin
    df = DataFrame(
        profile = [
            Dict(:A => 1, :B => 2, :C => 3),
            Dict(:A => 1, :B => 2, :C => 3),
            Dict(:A => 1, :B => 2, :C => 3),
            Dict(:C => 1, :B => 2, :A => 3),
            Dict(:C => 1, :B => 2, :A => 3),
            Dict(:C => 1, :B => 2, :A => 3),
        ],
        grp = ["x", "x", "x", "y", "y", "y"],
    )
    metadata!(df, "candidates", [:A, :B, :C])
    metadata!(df, "profile_kind", "linearized")

    bundle = PrefPol.dataframe_to_annotated_profile(df)
    avg = PrefPol.compute_group_measure_details(bundle, :grp; tie_policy = :average)
    hash = PrefPol.compute_group_measure_details(bundle, :grp; tie_policy = :hash)
    interval = PrefPol.compute_group_measure_details(bundle, :grp; tie_policy = :interval)

    @test avg.diagnostics.tied_groups == 0
    @test hash.diagnostics.tied_groups == 0
    @test interval.diagnostics.tied_groups == 0
    @test avg.C == 1.0
    @test avg.C == hash.C
    @test hash.C == interval.C
    @test avg.D == hash.D
    @test hash.D == interval.D
    @test avg.D_median == hash.D_median
    @test hash.D_median == interval.D_median
    @test avg.O == hash.O
    @test hash.O == interval.O
    @test avg.O_smoothed == hash.O_smoothed
    @test hash.O_smoothed == interval.O_smoothed
    @test avg.Sep == hash.Sep
    @test hash.Sep == interval.Sep
    @test avg.S == hash.S
    @test hash.S == interval.S
    @test avg.S_old == hash.S_old
    @test hash.S_old == interval.S_old
    @test avg.G == hash.G
    @test hash.G == interval.G
    @test avg.Gsep == hash.Gsep
    @test hash.Gsep == interval.Gsep
    @test interval.S_lo == interval.S
    @test interval.S == interval.S_hi
    @test interval.S_old_lo == interval.S_old
    @test interval.S_old == interval.S_old_hi
    @test interval.D_lo == interval.D
    @test interval.D == interval.D_hi
    @test interval.D_median_lo == interval.D_median
    @test interval.D_median == interval.D_median_hi
    @test interval.O_smoothed_lo == interval.O_smoothed
    @test interval.O_smoothed == interval.O_smoothed_hi
    @test interval.G_lo == interval.G
    @test interval.G == interval.G_hi
    @test interval.Gsep_lo == interval.Gsep
    @test interval.Gsep == interval.Gsep_hi
end

@testset "cleaned S is derived directly from grouped C and D" begin
    abc = ranking_dict([:A, :B, :C])
    cba = ranking_dict([:C, :B, :A])

    identical_df = vcat(
        DataFrame(grp = :A, profile = vcat(fill(abc, 50), fill(cba, 50))),
        DataFrame(grp = :B, profile = vcat(fill(abc, 50), fill(cba, 50))),
    )
    metadata!(identical_df, "candidates", [:A, :B, :C])
    metadata!(identical_df, "profile_kind", "linearized")
    identical_bundle = PrefPol.dataframe_to_annotated_profile(identical_df)
    identical_details = PrefPol.compute_group_measure_details(identical_bundle, :grp; tie_policy = :average)

    @test isapprox(
        identical_details.S,
        PrefPol.Preferences.overall_sstar_from_CD(identical_details.C, identical_details.D);
        atol = 1e-12,
    )
    @test isapprox(
        identical_details.S_lo,
        PrefPol.Preferences.overall_sstar_from_CD(identical_details.C, identical_details.D_lo);
        atol = 1e-12,
    )
    @test isapprox(
        identical_details.S_hi,
        PrefPol.Preferences.overall_sstar_from_CD(identical_details.C, identical_details.D_hi);
        atol = 1e-12,
    )
    @test isfinite(identical_details.S)
    @test isapprox(identical_details.S_old, 0.0; atol = 0.01)
end

@testset "legacy support-separation S_old survives the rename" begin
    abc = ranking_dict([:A, :B, :C])
    acb = ranking_dict([:A, :C, :B])
    cba = ranking_dict([:C, :B, :A])

    positive_df = vcat(
        DataFrame(grp = :A, profile = fill(abc, 3)),
        DataFrame(grp = :B, profile = fill(cba, 3)),
    )
    metadata!(positive_df, "candidates", [:A, :B, :C])
    metadata!(positive_df, "profile_kind", "linearized")
    positive_bundle = PrefPol.dataframe_to_annotated_profile(positive_df)

    positive_avg = PrefPol.compute_group_measure_details(positive_bundle, :grp; tie_policy = :average)
    positive_hash = PrefPol.compute_group_measure_details(positive_bundle, :grp; tie_policy = :hash)
    positive_interval = PrefPol.compute_group_measure_details(positive_bundle, :grp; tie_policy = :interval)

    @test isapprox(positive_avg.S_old, 1.0; atol = 1e-12)
    @test positive_avg.S_old == positive_hash.S_old
    @test positive_hash.S_old == positive_interval.S_old
    @test positive_avg.S_old_lo == positive_avg.S_old
    @test positive_avg.S_old == positive_avg.S_old_hi

    identical_df = vcat(
        DataFrame(grp = :A, profile = vcat(fill(abc, 50), fill(cba, 50))),
        DataFrame(grp = :B, profile = vcat(fill(abc, 50), fill(cba, 50))),
    )
    metadata!(identical_df, "candidates", [:A, :B, :C])
    metadata!(identical_df, "profile_kind", "linearized")
    identical_bundle = PrefPol.dataframe_to_annotated_profile(identical_df)
    identical_details = PrefPol.compute_group_measure_details(identical_bundle, :grp; tie_policy = :average)

    @test isapprox(identical_details.S_old, 0.0; atol = 0.01)

    negative_df = vcat(
        DataFrame(grp = :A, profile = [abc, cba]),
        DataFrame(grp = :B, profile = [abc, cba]),
    )
    metadata!(negative_df, "candidates", [:A, :B, :C])
    metadata!(negative_df, "profile_kind", "linearized")
    negative_bundle = PrefPol.dataframe_to_annotated_profile(negative_df)
    negative_details = PrefPol.compute_group_measure_details(negative_bundle, :grp; tie_policy = :average)

    @test negative_details.S_old < 0.0
    @test !isapprox(negative_details.S, negative_details.S_old; atol = 1e-12)

    relabeled_df = vcat(
        DataFrame(grp = :Z, profile = [cba, cba]),
        DataFrame(grp = :X, profile = [abc, abc]),
        DataFrame(grp = :Y, profile = [acb, acb]),
    )
    metadata!(relabeled_df, "candidates", [:A, :B, :C])
    metadata!(relabeled_df, "profile_kind", "linearized")
    relabeled_bundle = PrefPol.dataframe_to_annotated_profile(relabeled_df)

    original_df = vcat(
        DataFrame(grp = :A, profile = [abc, abc]),
        DataFrame(grp = :B, profile = [cba, cba]),
        DataFrame(grp = :C, profile = [acb, acb]),
    )
    metadata!(original_df, "candidates", [:A, :B, :C])
    metadata!(original_df, "profile_kind", "linearized")
    original_bundle = PrefPol.dataframe_to_annotated_profile(original_df)

    @test isapprox(
        PrefPol.compute_group_measure_details(original_bundle, :grp; tie_policy = :average).S_old,
        PrefPol.compute_group_measure_details(relabeled_bundle, :grp; tie_policy = :average).S_old;
        atol = 1e-12,
    )
end

@testset "legacy support-separation S_old handles singleton groups conservatively" begin
    abc = ranking_dict([:A, :B, :C])
    cba = ranking_dict([:C, :B, :A])

    surviving_pair_df = vcat(
        DataFrame(grp = :A, profile = [abc]),
        DataFrame(grp = :B, profile = [abc, abc]),
        DataFrame(grp = :C, profile = [cba, cba]),
    )
    metadata!(surviving_pair_df, "candidates", [:A, :B, :C])
    metadata!(surviving_pair_df, "profile_kind", "linearized")
    surviving_pair_bundle = PrefPol.dataframe_to_annotated_profile(surviving_pair_df)

    @test isapprox(
        PrefPol.compute_group_measure_details(surviving_pair_bundle, :grp; tie_policy = :average).S_old,
        1.0;
        atol = 1e-12,
    )

    no_valid_pair_df = vcat(
        DataFrame(grp = :A, profile = [abc]),
        DataFrame(grp = :B, profile = [cba, cba]),
    )
    metadata!(no_valid_pair_df, "candidates", [:A, :B, :C])
    metadata!(no_valid_pair_df, "profile_kind", "linearized")
    no_valid_pair_bundle = PrefPol.dataframe_to_annotated_profile(no_valid_pair_df)

    @test isnan(PrefPol.compute_group_measure_details(no_valid_pair_bundle, :grp; tie_policy = :average).S_old)
end

@testset "nested batch reporting tables preserve stored measure outputs" begin
    cfg = PrefPol.ElectionConfig(
        2022,
        "__nested_run_loader__",
        "/tmp/unused",
        3,
        [3],
        3,
        3,
        123,
        ["A", "B", "C"],
        ["grp"],
        [PrefPol.Scenario("all", String[])],
    )
    wave = PrefPol.SurveyWaveConfig(cfg; wave_id = "batch-wave")

    mktempdir() do dir
        pipeline = PrefPol.NestedStochasticPipeline([wave]; cache_root = dir)

        grouped_spec = PrefPol.build_pipeline_spec(
            wave;
            active_candidates = ["A", "B", "C"],
            groupings = [:grp],
            measures = [:Psi, :R, :HHI, :RHHI, :C, :D, :G],
            B = 3,
            R = 2,
            K = 2,
            imputer_backend = :zero,
            consensus_tie_policy = :average,
        )

        global_spec = PrefPol.build_pipeline_spec(
            wave;
            active_candidates = ["A", "C"],
            groupings = Symbol[],
            measures = [:Psi, :R],
            B = 2,
            R = 1,
            K = 1,
            imputer_backend = :zero,
        )

        runner = PrefPol.BatchRunner(pipeline)
        batch = PrefPol.StudyBatchSpec([grouped_spec, global_spec])
        results = PrefPol.run_batch(runner, batch; force = true)

        @test length(results) == 2

        grouped_result = results[1]
        loaded = PrefPol.load_pipeline_result(pipeline, grouped_spec)
        @test isequal(loaded.pooled_summaries, grouped_result.pooled_summaries)

        measure_table = PrefPol.pipeline_measure_table(results)
        summary_table = PrefPol.pipeline_summary_table(results)
        panel_table = PrefPol.pipeline_panel_table(results)

        @test nrow(measure_table) == sum(nrow(result.measure_cube) for result in results)
        @test nrow(summary_table) == sum(nrow(result.pooled_summaries) for result in results)
        @test nrow(panel_table) == nrow(summary_table)
        @test collect(unique(summary_table.batch_index)) == [1, 2]
        @test collect(unique(panel_table.batch_index)) == [1, 2]

        g_draws = Float64.(grouped_result.measure_cube[
            (grouped_result.measure_cube.measure .== :G) .&
            coalesce.(grouped_result.measure_cube.grouping .== :grp, false),
            :value,
        ])
        c_draws = Float64.(grouped_result.measure_cube[
            (grouped_result.measure_cube.measure .== :C) .&
            coalesce.(grouped_result.measure_cube.grouping .== :grp, false),
            :value,
        ])
        d_draws = Float64.(grouped_result.measure_cube[
            (grouped_result.measure_cube.measure .== :D) .&
            coalesce.(grouped_result.measure_cube.grouping .== :grp, false),
            :value,
        ])

        g_panel = panel_table[
            (panel_table.batch_index .== 1) .&
            (panel_table.measure .== :G) .&
            coalesce.(panel_table.grouping .== :grp, false),
            :,
        ][1, :]
        g_summary = summary_table[
            (summary_table.batch_index .== 1) .&
            (summary_table.measure .== :G) .&
            coalesce.(summary_table.grouping .== :grp, false),
            :,
        ][1, :]

        @test isapprox(g_panel.estimate, mean(g_draws))
        @test isapprox(g_panel.q50, quantile(g_draws, 0.50))
        @test isapprox(g_panel.q95, quantile(g_draws, 0.95))
        @test isapprox(g_panel.estimate, g_summary.estimate)
    end
end

@testset "cached grouped S can be backfilled from grouped C and D" begin
    cfg = PrefPol.ElectionConfig(
        2022,
        "__nested_run_loader__",
        "/tmp/unused",
        3,
        [3],
        3,
        3,
        123,
        ["A", "B", "C"],
        ["grp"],
        [PrefPol.Scenario("all", String[])],
    )
    wave = PrefPol.SurveyWaveConfig(cfg; wave_id = "augment-s-wave")

    mktempdir() do dir
        pipeline = PrefPol.NestedStochasticPipeline([wave]; cache_root = dir)
        spec = PrefPol.build_pipeline_spec(
            wave;
            active_candidates = ["A", "B", "C"],
            groupings = [:grp],
            measures = [:Psi, :C, :D, :G],
            B = 2,
            R = 2,
            K = 2,
            imputer_backend = :zero,
            consensus_tie_policy = :average,
        )

        result = PrefPol.run_pipeline(pipeline, spec; force = true)
        updated = PrefPol.augment_pipeline_result_with_grouped_s(result)

        @test !(:S in result.spec.measures)
        @test :S in updated.spec.measures
        @test count(==(:S), updated.measure_cube.measure) ==
              spec.B * spec.R * spec.K * length(spec.groupings)
        @test nrow(updated.measure_cube) ==
              nrow(result.measure_cube) + spec.B * spec.R * spec.K * length(spec.groupings)

        c_rows = select(
            updated.measure_cube[updated.measure_cube.measure .== :C, :],
            :b, :r, :k, :grouping, :value,
        )
        rename!(c_rows, :value => :c_value)
        d_rows = select(
            updated.measure_cube[updated.measure_cube.measure .== :D, :],
            :b, :r, :k, :grouping, :value, :value_lo, :value_hi,
        )
        rename!(d_rows, :value => :d_value, :value_lo => :d_lo, :value_hi => :d_hi)
        s_rows = select(
            updated.measure_cube[updated.measure_cube.measure .== :S, :],
            :b, :r, :k, :grouping, :value, :value_lo, :value_hi,
        )
        rename!(s_rows, :value => :s_value, :value_lo => :s_lo, :value_hi => :s_hi)

        joined = innerjoin(c_rows, d_rows; on = [:b, :r, :k, :grouping])
        joined = innerjoin(joined, s_rows; on = [:b, :r, :k, :grouping])
        @test nrow(joined) == spec.B * spec.R * spec.K * length(spec.groupings)

        for row in eachrow(joined)
            @test isapprox(
                row.s_value,
                PrefPol.Preferences.overall_sstar_from_CD(row.c_value, row.d_value);
                atol = 1e-12,
            )
            @test isapprox(
                row.s_lo,
                PrefPol.Preferences.overall_sstar_from_CD(row.c_value, row.d_lo);
                atol = 1e-12,
            )
            @test isapprox(
                row.s_hi,
                PrefPol.Preferences.overall_sstar_from_CD(row.c_value, row.d_hi);
                atol = 1e-12,
            )
        end

        @test any(updated.pooled_summaries.measure .== :S)
        @test any(summary.measure_id == :S for summary in updated.decomposition.summaries)

        saved_path = joinpath(dir, "augmented_result.jld2")
        PrefPol.save_pipeline_result(saved_path, updated)
        loaded = PrefPol.load_pipeline_result(saved_path)
        @test :S in loaded.spec.measures
        @test isequal(loaded.measure_cube, updated.measure_cube)
        @test isequal(loaded.pooled_summaries, updated.pooled_summaries)
    end
end

@testset "cached grouped S augmentation fails on malformed grouped C/D keys" begin
    cfg = PrefPol.ElectionConfig(
        2022,
        "__nested_run_loader__",
        "/tmp/unused",
        3,
        [3],
        3,
        3,
        123,
        ["A", "B", "C"],
        ["grp"],
        [PrefPol.Scenario("all", String[])],
    )
    wave = PrefPol.SurveyWaveConfig(cfg; wave_id = "augment-s-bad-wave")

    mktempdir() do dir
        pipeline = PrefPol.NestedStochasticPipeline([wave]; cache_root = dir)
        spec = PrefPol.build_pipeline_spec(
            wave;
            active_candidates = ["A", "B", "C"],
            groupings = [:grp],
            measures = [:C, :D, :G],
            B = 1,
            R = 1,
            K = 1,
            imputer_backend = :zero,
            consensus_tie_policy = :average,
        )

        result = PrefPol.run_pipeline(pipeline, spec; force = true)
        dup_c = result.measure_cube[result.measure_cube.measure .== :C, :][1:1, :]
        bad_cube = vcat(result.measure_cube, dup_c; cols = :setequal)
        bad_result = PrefPol.PipelineResult(
            result.spec,
            result.cache_dir,
            copy(result.stage_manifest),
            bad_cube,
            result.pooled_summaries,
            result.decomposition,
            copy(result.audit_log),
        )

        @test_throws ArgumentError PrefPol.augment_pipeline_result_with_grouped_s(bad_result)
    end
end

@testset "nested batch metadata decorates reporting tables" begin
    cfg = PrefPol.ElectionConfig(
        2022,
        "__nested_run_loader__",
        "/tmp/unused",
        3,
        [2, 3],
        3,
        3,
        123,
        ["A", "B", "C"],
        ["grp"],
        [PrefPol.Scenario("all", String[])],
    )
    wave = PrefPol.SurveyWaveConfig(cfg; wave_id = "meta-wave")

    mktempdir() do dir
        pipeline = PrefPol.NestedStochasticPipeline([wave]; cache_root = dir)
        spec_m2 = PrefPol.build_pipeline_spec(
            wave;
            active_candidates = ["A", "C"],
            groupings = [:grp],
            measures = [:Psi, :C, :D, :G],
            B = 2,
            R = 1,
            K = 1,
            imputer_backend = :zero,
        )
        spec_m3 = PrefPol.build_pipeline_spec(
            wave;
            active_candidates = ["A", "B", "C"],
            groupings = [:grp],
            measures = [:Psi, :C, :D, :G],
            B = 2,
            R = 1,
            K = 1,
            imputer_backend = :zero,
        )

        batch = PrefPol.StudyBatchSpec([
            PrefPol.StudyBatchItem(spec_m2; year = 2022, scenario_name = "all", m = 2, candidate_label = "Candidates: A, C"),
            PrefPol.StudyBatchItem(spec_m3; year = 2022, scenario_name = "all", m = 3, candidate_label = "Candidates: A, B, C"),
        ])
        results = PrefPol.run_batch(PrefPol.BatchRunner(pipeline), batch; force = true)
        panel = PrefPol.pipeline_panel_table(results)
        summary = PrefPol.pipeline_summary_table(results)

        @test length(results) == 2
        @test results[1].spec.active_candidates == spec_m2.active_candidates
        @test results[2].spec.active_candidates == spec_m3.active_candidates
        @test :year in propertynames(panel)
        @test :scenario_name in propertynames(panel)
        @test :m in propertynames(panel)
        @test :candidate_label in propertynames(panel)
        @test :batch_index in propertynames(panel)
        @test :batch_index in propertynames(summary)
        @test Set(panel.m) == Set([2, 3])
        @test Set(summary.scenario_name) == Set(["all"])
        @test Set(summary.year) == Set([2022])
    end
end

@testset "nested batch allows identical specs and preserves item order" begin
    cfg = PrefPol.ElectionConfig(
        2022,
        "__nested_run_loader__",
        "/tmp/unused",
        3,
        [3],
        3,
        3,
        123,
        ["A", "B", "C"],
        ["grp"],
        [PrefPol.Scenario("all", String[])],
    )
    wave = PrefPol.SurveyWaveConfig(cfg; wave_id = "duplicate-wave")

    mktempdir() do dir
        pipeline = PrefPol.NestedStochasticPipeline([wave]; cache_root = dir)
        spec = PrefPol.build_pipeline_spec(
            wave;
            active_candidates = ["A", "B", "C"],
            groupings = [:grp],
            measures = [:Psi, :C, :D, :G],
            B = 2,
            R = 1,
            K = 1,
            imputer_backend = :zero,
        )

        batch = PrefPol.StudyBatchSpec([
            PrefPol.StudyBatchItem(spec; run_label = "first"),
            PrefPol.StudyBatchItem(spec; run_label = "second"),
        ])
        results = PrefPol.run_batch(PrefPol.BatchRunner(pipeline), batch; force = true)
        summary = PrefPol.pipeline_summary_table(results)

        @test length(results) == 2
        @test results[1].spec.active_candidates == spec.active_candidates
        @test results[2].spec.active_candidates == spec.active_candidates
        @test isequal(results[1].measure_cube, results[2].measure_cube)
        @test collect(unique(summary.batch_index)) == [1, 2]
        @test Set(summary.run_label) == Set(["first", "second"])
        @test all(summary[summary.batch_index .== 1, :run_label] .== "first")
        @test all(summary[summary.batch_index .== 2, :run_label] .== "second")
    end
end

@testset "nested compact artifact codec remains smaller than serialized strict bundles" begin
    # Keep the fixture large enough that JLD2 container overhead does not
    # dominate the encoded artifact size comparison.
    df = DataFrame(
        A = fill(10, 1000),
        B = fill(8, 1000),
        C = fill(6, 1000),
        grp = fill("x", 1000),
    )

    weak_df = PrefPol.profile_dataframe(df; score_cols = [:A, :B, :C], demo_cols = [:grp], kind = :weak)
    metadata!(weak_df, "candidates", [:A, :B, :C])
    metadata!(weak_df, "profile_kind", "weak")
    weak_bundle = PrefPol.dataframe_to_annotated_profile(weak_df; ballot_kind = :weak)
    strict_bundle = PrefPol.linearize_annotated_profile(weak_bundle; rng = MersenneTwister(1))
    artifact = PrefPol.compact_profile_artifact_dataframe(strict_bundle)

    mktempdir() do dir
        compact_path = joinpath(dir, "compact.jld2")
        bundle_path = joinpath(dir, "bundle.jld2")
        JLD2.@save compact_path artifact
        JLD2.@save bundle_path strict_bundle
        @test stat(compact_path).size < stat(bundle_path).size
    end
end

@testset "nested linearization artifacts are persisted in compact encoded form" begin
    cfg = PrefPol.ElectionConfig(
        2022,
        "__nested_run_loader__",
        "/tmp/unused",
        3,
        [3],
        3,
        3,
        123,
        ["A", "B", "C"],
        ["grp"],
        [PrefPol.Scenario("all", String[])],
    )
    wave = PrefPol.SurveyWaveConfig(cfg; wave_id = "artifact-wave")

    mktempdir() do dir
        pipeline = PrefPol.NestedStochasticPipeline([wave]; cache_root = dir)
        spec = PrefPol.build_pipeline_spec(
            wave;
            active_candidates = ["A", "B", "C"],
            groupings = [:grp],
            measures = [:Psi, :C, :D, :G],
            B = 1,
            R = 1,
            K = 1,
            imputer_backend = :zero,
        )
        result = PrefPol.run_pipeline(pipeline, spec; force = true)
        linearized_path = result.stage_manifest[result.stage_manifest.stage .== :linearized, :path][1]
        artifact = JLD2.load(linearized_path, "artifact")

        @test artifact isa DataFrame
        @test metadata(artifact, "profile_encoding") == "rank_vector_v1"
        @test !(eltype(artifact.profile) <: AbstractDict)
    end
end

@testset "nested measure stage rejects weak profile slices" begin
    weak_df = PrefPol.profile_dataframe(
        DataFrame(
            A = [10, 10, 6],
            B = [10, 8, 6],
            C = [7, 6, 10],
            grp = ["x", "x", "y"],
        );
        score_cols = [:A, :B, :C],
        demo_cols = [:grp],
        kind = :weak,
    )
    metadata!(weak_df, "candidates", [:A, :B, :C])
    metadata!(weak_df, "profile_kind", "weak")
    weak_bundle = PrefPol.dataframe_to_annotated_profile(weak_df; ballot_kind = :weak)
    artifact = PrefPol.compact_profile_artifact_dataframe(weak_bundle)

    mktempdir() do dir
        weak_path = joinpath(dir, "weak_profile.jld2")
        JLD2.@save weak_path artifact

        weak_profiles = OrderedDict(
            "all" => OrderedDict(
                3 => PrefPol.ProfilesSlice(
                    2022,
                    "all",
                    3,
                    [:A, :B, :C],
                    Dict(:zero => [weak_path]),
                ),
            ),
        )

        @test_throws ArgumentError PrefPol.apply_measures_for_year(weak_profiles)
    end
end

@testset "nested weak-order guard rejects incomplete weak profiles" begin
    bad_df = DataFrame(
        profile = [Dict(:A => 1, :B => 1), Dict(:A => 1, :C => 1)],
        grp = ["x", "y"],
    )
    metadata!(bad_df, "candidates", [:A, :B, :C])
    metadata!(bad_df, "profile_kind", "weak")
    bad_bundle = PrefPol.dataframe_to_annotated_profile(bad_df)

    @test_throws ArgumentError PrefPol._assert_complete_weak_orders(
        bad_bundle;
        context = "test-bad-weak-order",
    )
end

@testset "nested stage seeds are reproducible across reruns" begin
    cfg = PrefPol.ElectionConfig(
        2022,
        "__nested_run_loader__",
        "/tmp/unused",
        3,
        [3],
        3,
        3,
        777,
        ["A", "B", "C"],
        ["grp"],
        [PrefPol.Scenario("all", String[])],
    )
    wave = PrefPol.SurveyWaveConfig(cfg; wave_id = "seed-wave")

    spec = PrefPol.build_pipeline_spec(
        wave;
        active_candidates = ["A", "B", "C"],
        groupings = [:grp],
        measures = [:Psi, :R, :HHI, :RHHI, :C, :D, :G],
        B = 2,
        R = 2,
        K = 2,
        imputer_backend = :zero,
    )

    mktempdir() do dir1
        mktempdir() do dir2
            result1 = PrefPol.run_pipeline(
                PrefPol.NestedStochasticPipeline([wave]; cache_root = dir1),
                spec;
                force = true,
            )
            result2 = PrefPol.run_pipeline(
                PrefPol.NestedStochasticPipeline([wave]; cache_root = dir2),
                spec;
                force = true,
            )

            @test isequal(result1.measure_cube, result2.measure_cube)
            @test isequal(select(result1.stage_manifest, Not(:path)), select(result2.stage_manifest, Not(:path)))
        end
    end
end

@testset "nested panel selectors and plot prep preserve stored grouped measures" begin
    cfg = PrefPol.ElectionConfig(
        2022,
        "__nested_run_loader__",
        "/tmp/unused",
        3,
        [2, 3],
        3,
        3,
        123,
        ["A", "B", "C"],
        ["grp"],
        [PrefPol.Scenario("all", String[])],
    )
    wave = PrefPol.SurveyWaveConfig(cfg; wave_id = "plot-wave")

    mktempdir() do dir
        pipeline = PrefPol.NestedStochasticPipeline([wave]; cache_root = dir)
        items = PrefPol.StudyBatchItem[]

        for (m, active_candidates, label) in (
            (2, ["A", "C"], "Candidates: A, C"),
            (3, ["A", "B", "C"], "Candidates: A, B, C"),
        )
            spec = PrefPol.build_pipeline_spec(
                wave;
                active_candidates = active_candidates,
                groupings = [:grp],
                measures = [:Psi, :C, :D, :D_median, :O, :O_smoothed, :Sep, :G, :Gsep, :S, :S_old],
                B = 3,
                R = 2,
                K = 1,
                imputer_backend = :zero,
                consensus_tie_policy = :average,
            )
            push!(items, PrefPol.StudyBatchItem(
                spec;
                year = 2022,
                scenario_name = "all",
                m = m,
                candidate_label = label,
            ))
        end

        results = PrefPol.run_batch(PrefPol.BatchRunner(pipeline), PrefPol.StudyBatchSpec(items); force = true)
        panel = PrefPol.pipeline_panel_table(results)
        scenario_rows = PrefPol.select_pipeline_panel_rows(
            panel;
            year = 2022,
            scenario_name = "all",
            imputer_backend = :zero,
            measures = [:Psi],
            include_grouped = false,
        )
        group_rows = PrefPol.select_pipeline_panel_rows(
            panel;
            year = 2022,
            scenario_name = "all",
            imputer_backend = :zero,
            measures = [:C, :D, :D_median, :O, :O_smoothed, :Sep, :G, :Gsep, :S, :S_old],
            groupings = [:grp],
            include_grouped = true,
        )
        scenario_data = PrefPol.pipeline_scenario_plot_data(
            results;
            year = 2022,
            scenario_name = "all",
            imputer_backend = :zero,
            measures = [:Psi],
        )
        heatmap_data = PrefPol.pipeline_group_heatmap_values(
            results;
            year = 2022,
            scenario_name = "all",
            imputer_backend = :zero,
            measures = [:C, :D, :D_median, :O, :O_smoothed, :Sep, :G, :Gsep, :S, :S_old],
            groupings = [:grp],
            statistic = :median,
        )

        @test Set(scenario_rows.n_candidates) == Set([2, 3])
        @test all(ismissing, scenario_rows.grouping)
        @test Set(Symbol.(skipmissing(group_rows.grouping))) == Set([:grp])
        @test :D_median in Set(Symbol.(group_rows.measure))
        @test :O in Set(Symbol.(group_rows.measure))
        @test :O_smoothed in Set(Symbol.(group_rows.measure))
        @test :Sep in Set(Symbol.(group_rows.measure))
        @test :Gsep in Set(Symbol.(group_rows.measure))
        @test :S in Set(Symbol.(group_rows.measure))
        @test :S_old in Set(Symbol.(group_rows.measure))
        @test scenario_data.m_values == [2, 3]
        @test scenario_data.candidate_label == "Candidates: A, B, C"

        g_row = group_rows[(group_rows.measure .== :G) .& coalesce.(group_rows.grouping .== :grp, false), :][1, :]
        @test isapprox(heatmap_data.matrices[:G][1, 1], g_row.q50)
        dmedian_row = group_rows[(group_rows.measure .== :D_median) .& coalesce.(group_rows.grouping .== :grp, false), :][1, :]
        @test isapprox(heatmap_data.matrices[:D_median][1, 1], dmedian_row.q50)
        o_row = group_rows[(group_rows.measure .== :O) .& coalesce.(group_rows.grouping .== :grp, false), :][1, :]
        @test isapprox(heatmap_data.matrices[:O][1, 1], o_row.q50)
        osmoothed_row = group_rows[(group_rows.measure .== :O_smoothed) .& coalesce.(group_rows.grouping .== :grp, false), :][1, :]
        @test isapprox(heatmap_data.matrices[:O_smoothed][1, 1], osmoothed_row.q50)
        sep_row = group_rows[(group_rows.measure .== :Sep) .& coalesce.(group_rows.grouping .== :grp, false), :][1, :]
        @test isapprox(heatmap_data.matrices[:Sep][1, 1], sep_row.q50)
        gsep_row = group_rows[(group_rows.measure .== :Gsep) .& coalesce.(group_rows.grouping .== :grp, false), :][1, :]
        @test isapprox(heatmap_data.matrices[:Gsep][1, 1], gsep_row.q50)
        s_row = group_rows[(group_rows.measure .== :S) .& coalesce.(group_rows.grouping .== :grp, false), :][1, :]
        @test isapprox(heatmap_data.matrices[:S][1, 1], s_row.q50)
        sold_row = group_rows[(group_rows.measure .== :S_old) .& coalesce.(group_rows.grouping .== :grp, false), :][1, :]
        @test isapprox(heatmap_data.matrices[:S_old][1, 1], sold_row.q50)
        @test_throws ArgumentError PrefPol.pipeline_group_heatmap_values(
            results;
            year = 2022,
            scenario_name = "all",
            imputer_backend = :zero,
            measures = [:C, :HHI],
            groupings = [:grp],
            statistic = :median,
        )
    end
end

@testset "legacy year-level pipeline APIs hard-error" begin
    cfg = PrefPol.ElectionConfig(
        2022,
        "__nested_run_loader__",
        "/tmp/unused",
        3,
        [2, 3],
        2,
        3,
        123,
        ["A", "B", "C"],
        ["grp"],
        [PrefPol.Scenario("all", String[])],
    )
    f3_entry = (cfg = cfg, data = DataFrame[])
    imps_entry = (data = OrderedDict{Symbol,Vector{DataFrame}}(),)
    imputed_year = PrefPol.ImputedYear(2022, Dict(:zero => String[]))
    weak_profiles = OrderedDict("all" => OrderedDict(3 => nothing))

    mktempdir() do dir
        @test_throws ArgumentError PrefPol.save_or_load_candidate_sets_for_year(
            cfg;
            dir = dir,
            overwrite = true,
            verbose = false,
        )
        @test_throws ArgumentError PrefPol.save_bootstrap(cfg; dir = dir, overwrite = true, quiet = true)
        @test_throws ArgumentError PrefPol.save_all_bootstraps(; years = 2022, cfgdir = dir, overwrite = true)
        @test_throws ArgumentError PrefPol.load_all_bootstraps(; years = 2022, dir = dir, quiet = true)
        @test_throws ArgumentError PrefPol.impute_bootstrap_to_files(joinpath(dir, "boot_2022.jld2"))
        @test_throws ArgumentError PrefPol.impute_all_bootstraps(; years = 2022, base_dir = dir, imp_dir = dir)
        @test_throws ArgumentError PrefPol._impute_year_to_files(DataFrame[], cfg; imp_dir = dir, overwrite = true)
        @test_throws ArgumentError PrefPol.impute_from_f3(OrderedDict{Int,NamedTuple}())
        @test_throws ArgumentError PrefPol.load_imputed_bootstrap(2022; dir = dir, quiet = true)
        @test_throws ArgumentError PrefPol.load_imputed_year(2022; dir = dir)
        @test_throws ArgumentError PrefPol.generate_profiles_for_year(
            2022,
            f3_entry,
            imps_entry;
            candidate_sets = OrderedDict("all" => ["A", "B", "C"]),
        )
        @test_throws ArgumentError PrefPol.load_profiles_index(2022; dir = dir)
        @test_throws ArgumentError PrefPol.load_linearized_profiles_index(2022; dir = dir)
        @test_throws ArgumentError PrefPol.generate_profiles_for_year_streamed_from_index(
            2022,
            f3_entry,
            imputed_year;
            candidate_sets = OrderedDict("all" => ["A", "B", "C"]),
            out_dir = dir,
            overwrite = true,
        )
        @test_throws ArgumentError PrefPol.linearize_profiles_for_year_streamed_from_index(
            2022,
            f3_entry,
            weak_profiles;
            out_dir = dir,
            overwrite = true,
        )
        @test_throws ArgumentError PrefPol.save_or_load_profiles_for_year(
            2022,
            OrderedDict{Int,NamedTuple}(),
            OrderedDict{Int,NamedTuple}();
            dir = dir,
            overwrite = true,
            verbose = false,
        )
        @test_throws ArgumentError PrefPol.save_or_load_measures_for_year(
            2022,
            weak_profiles;
            dir = dir,
            overwrite = true,
            verbose = false,
        )
        @test_throws ArgumentError PrefPol.save_or_load_group_metrics_for_year(
            2022,
            weak_profiles,
            f3_entry;
            dir = dir,
            overwrite = true,
            verbose = false,
        )
        @test_throws ArgumentError PrefPol.plot_scenario_year(2022, "all", Dict{Int,Any}(), Dict{Int,Any}())
        @test_throws ArgumentError PrefPol.plot_group_demographics_lines(
            Dict{Int,Any}(),
            Dict{Int,Any}(),
            2022,
            "all",
        )
        @test_throws ArgumentError PrefPol.plot_group_demographics_heatmap(
            Dict{Int,Any}(),
            Dict{Int,Any}(),
            2022,
            "all",
        )
        @test_throws ArgumentError PrefPol.save_plot(
            nothing,
            2022,
            "all",
            cfg;
            variant = "mice",
            dir = dir,
        )
    end
end
