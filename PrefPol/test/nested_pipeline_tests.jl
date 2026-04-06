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
            measures = [:Psi, :R, :HHI, :RHHI, :C, :D, :G],
            B = 3,
            R = 2,
            K = 2,
            imputer_backend = :zero,
            consensus_tie_policy = :average,
        )

        result = PrefPol.run_pipeline(pipeline, spec; force = true)

        @test result.spec_hash == PrefPol.pipeline_spec_hash(spec)
        @test nrow(result.measure_cube) == spec.B * spec.R * spec.K * length(spec.measures)
        @test all(isfile, result.stage_manifest.path)
        @test nrow(result.stage_manifest) == 2 + spec.B + spec.B * spec.R + 2 * spec.B * spec.R * spec.K
        @test Set(result.measure_cube.measure) == Set(spec.measures)
        @test all(isfinite, result.pooled_summaries.estimate)
        @test all(result.pooled_summaries.V_imp .≈ 0.0)
        @test all(result.pooled_summaries.V_lin .≈ 0.0)
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
    @test avg.G == hash.G
    @test hash.G == interval.G
    @test interval.D_lo == interval.D
    @test interval.D == interval.D_hi
    @test interval.G_lo == interval.G
    @test interval.G == interval.G_hi
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

        grouped_hash = PrefPol.pipeline_spec_hash(grouped_spec)
        grouped_result = results[grouped_hash]
        loaded = PrefPol.load_pipeline_result(pipeline, grouped_hash)
        @test loaded.spec_hash == grouped_result.spec_hash
        @test isequal(loaded.pooled_summaries, grouped_result.pooled_summaries)

        measure_table = PrefPol.pipeline_measure_table(results)
        summary_table = PrefPol.pipeline_summary_table(results)
        panel_table = PrefPol.pipeline_panel_table(results)

        @test nrow(measure_table) == sum(nrow(result.measure_cube) for result in values(results))
        @test nrow(summary_table) == sum(nrow(result.pooled_summaries) for result in values(results))
        @test nrow(panel_table) == nrow(summary_table)
        @test Set(summary_table.spec_hash) == Set(keys(results))
        @test Set(panel_table.spec_hash) == Set(keys(results))

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
            (panel_table.spec_hash .== grouped_hash) .&
            (panel_table.measure .== :G) .&
            coalesce.(panel_table.grouping .== :grp, false),
            :,
        ][1, :]
        g_summary = summary_table[
            (summary_table.spec_hash .== grouped_hash) .&
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
        @test haskey(results, PrefPol.pipeline_spec_hash(spec_m2))
        @test haskey(results, PrefPol.pipeline_spec_hash(spec_m3))
        @test :year in propertynames(panel)
        @test :scenario_name in propertynames(panel)
        @test :m in propertynames(panel)
        @test :candidate_label in propertynames(panel)
        @test Set(panel.m) == Set([2, 3])
        @test Set(summary.scenario_name) == Set(["all"])
        @test Set(summary.year) == Set([2022])
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
                measures = [:Psi, :C, :D, :G],
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
            measures = [:C, :D, :G],
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
            measures = [:C, :D, :G],
            groupings = [:grp],
            statistic = :median,
        )

        @test Set(scenario_rows.n_candidates) == Set([2, 3])
        @test all(ismissing, scenario_rows.grouping)
        @test Set(Symbol.(skipmissing(group_rows.grouping))) == Set([:grp])
        @test scenario_data.m_values == [2, 3]
        @test scenario_data.candidate_label == "Candidates: A, B, C"

        g_row = group_rows[(group_rows.measure .== :G) .& coalesce.(group_rows.grouping .== :grp, false), :][1, :]
        @test isapprox(heatmap_data.matrices[:G][1, 1], g_row.q50)
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
