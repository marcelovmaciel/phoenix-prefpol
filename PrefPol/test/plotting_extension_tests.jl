using Test
using PrefPol
using CairoMakie
using DataFrames
import PrefPol as pp

const _NESTED_PLOT_DF = DataFrame(
    A = [10, 10, 9, 9, 10, 2, 2, 3, 3, 2],
    B = [8, 8, 7, 7, 8, 6, 6, 7, 7, 6],
    C = [6, 6, 5, 5, 6, 10, 10, 9, 9, 10],
    grp = ["x", "x", "x", "x", "x", "y", "y", "y", "y", "y"],
    peso = fill(1.0, 10),
)

@eval PrefPol begin
    function __nested_plot_loader__(path; candidates)
        return deepcopy($(_NESTED_PLOT_DF))
    end
end

function _plotting_test_results()
    cfg = PrefPol.ElectionConfig(
        2022,
        "__nested_plot_loader__",
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
    wave = PrefPol.SurveyWaveConfig(cfg; wave_id = "plot-ext-wave")

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
                measures = [:Psi, :R, :HHI, :RHHI, :C, :D, :G, :S],
                B = 2,
                R = 1,
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

        batch = PrefPol.StudyBatchSpec(items)
        return PrefPol.run_batch(PrefPol.BatchRunner(pipeline), batch; force = true)
    end
end

@testset "plotting extension smoke" begin
    @test Base.get_extension(PrefPol, :PrefPolPlottingExt) !== nothing

    results = _plotting_test_results()

    fig_lines = PrefPol.plot_pipeline_scenario(
        results;
        year = 2022,
        scenario_name = "all",
        imputer_backend = :zero,
        plot_kind = :lines,
    )
    fig_dot = PrefPol.plot_pipeline_scenario(
        results;
        year = 2022,
        scenario_name = "all",
        imputer_backend = :zero,
        plot_kind = :dotwhisker,
        connect_lines = true,
    )
    fig_group_lines = PrefPol.plot_pipeline_group_lines(
        results;
        year = 2022,
        scenario_name = "all",
        imputer_backend = :zero,
        groupings = [:grp],
        measures = [:C, :D, :G],
        maxcols = 2,
    )
    fig_heatmap = PrefPol.plot_pipeline_group_heatmap(
        results;
        year = 2022,
        scenario_name = "all",
        imputer_backend = :zero,
        groupings = [:grp],
        measures = [:C, :D, :G],
        statistic = :median,
        colormap = pp.Makie.Reverse(:RdBu),
        show_values = true,
        fixed_colorrange = true,
    )
    fig_signed_heatmap = PrefPol.plot_pipeline_group_heatmap(
        results;
        year = 2022,
        scenario_name = "all",
        imputer_backend = :zero,
        groupings = [:grp],
        measures = [:S],
        statistic = :median,
        colormap = pp.Makie.Reverse(:RdBu),
        show_values = true,
        fixed_colorrange_limits = (-1.0, 1.0),
        colorbar_label = "median signed support-separation contrast",
    )

    @test pp.Makie === CairoMakie.Makie
    @test fig_lines isa pp.Makie.Figure
    @test fig_dot isa pp.Makie.Figure
    @test fig_group_lines isa pp.Makie.Figure
    @test fig_heatmap isa pp.Makie.Figure
    @test fig_signed_heatmap isa pp.Makie.Figure

    mktempdir() do dir
        manual_save = joinpath(dir, "plotting_manual_save.png")
        pp.save(manual_save, fig_lines; px_per_unit = 2)
        saved_lines = PrefPol.save_pipeline_plot(fig_lines, "plotting_lines"; dir = dir)
        saved_heatmap = PrefPol.save_pipeline_plot(fig_heatmap, "plotting_heatmap"; dir = dir)
        saved_signed_heatmap = PrefPol.save_pipeline_plot(fig_signed_heatmap, "plotting_signed_heatmap"; dir = dir)

        @test isfile(manual_save)
        @test isfile(saved_lines)
        @test isfile(saved_heatmap)
        @test isfile(saved_signed_heatmap)
        @test endswith(saved_lines, ".png")
        @test endswith(saved_heatmap, ".png")
        @test endswith(saved_signed_heatmap, ".png")
    end
end
