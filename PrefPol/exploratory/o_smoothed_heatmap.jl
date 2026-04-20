"""
Plot the grouped scalar `1 - O_smoothed`.

`O_smoothed` is the smoothed analogue of the grouped overall overlap metric. It
is computed by replacing exact group-profile overlap with radius-1 Kendall-local
overlap between the empirical group distributions over complete linear orders.

This plot shows `1 - O_smoothed` with:

- rows = grouping variables / demographic partitions
- columns = number of alternatives `m`
- cells = the grouped scalar `1 - O_smoothed` for that partition and `m`

Low values mean stronger local overlap between groups within that partition.
High values mean less local overlap, even after radius-1 smoothing.
"""

include(joinpath(@__DIR__, "grouped_overlap_heatmap_2x2.jl"))

using Printf
using TextWrap
using .GroupedOverlapHeatmap2x2
import PrefPol as pp

const M = pp.Makie

const YEAR = 2022
const SCENARIO_NAME = "lula_bolsonaro_ciro_marina_tebet"
const IMPUTER_BACKEND = :mice
const GROUPINGS = nothing
const CONSENSUS_TIE_POLICY = :average
const FORCE_PIPELINE = false
const R = parse(Int, get(ENV, "PREFPOL_O_SMOOTHED_R", "2"))
const K = parse(Int, get(ENV, "PREFPOL_O_SMOOTHED_K", "2"))

const SCRIPT_STEM = "o_smoothed_heatmap"
const OUTPUT_ROOT = joinpath(pp.project_root, "exploratory", "output", SCRIPT_STEM)
const CACHE_ROOT = joinpath(pp.project_root, "exploratory", "_tmp", SCRIPT_STEM)

mkpath(OUTPUT_ROOT)
mkpath(CACHE_ROOT)

function o_smoothed_heatmap_values(results;
                                   year::Int,
                                   scenario_name::AbstractString,
                                   imputer_backend::Symbol = :mice,
                                   groupings = nothing,
                                   statistic::Symbol = :median)
    return pp.pipeline_group_heatmap_values(
        results;
        year = year,
        scenario_name = scenario_name,
        imputer_backend = imputer_backend,
        measures = [:O_smoothed],
        groupings = groupings,
        statistic = statistic,
    )
end

function plot_o_smoothed_heatmap(data;
                                 year::Int,
                                 scenario_name::AbstractString,
                                 imputer_backend::Symbol = :mice,
                                 colorrange = (0.0f0, 1.0f0))
    rows = data.rows
    m_values_int = Int.(data.m_values)
    xs_m = Float32.(m_values_int)
    group_labels = string.(data.grouping_values)
    z = Float32.(1.0 .- data.matrices[:O_smoothed])
    valid_values = Float32.(filter(!isnan, vec(z)))
    data_min, data_max = isempty(valid_values) ? (0.0f0, 1.0f0) : extrema(valid_values)
    title_txt = GroupedOverlapHeatmap2x2._plot_title(
        rows;
        year = year,
        scenario_name = scenario_name,
        imputer_backend = imputer_backend,
    )

    fig = M.Figure(resolution = (max(520, 10 * length(title_txt) + 60), 420))
    M.rowgap!(fig.layout, 24)
    M.colgap!(fig.layout, 24)

    fig[1, 1:2] = M.Label(fig, title_txt; fontsize = 20, halign = :left)
    fig[2, 1:2] = M.Label(
        fig,
        join(TextWrap.wrap(data.candidate_label; width = 72));
        fontsize = 14,
        halign = :left,
    )

    ax = M.Axis(
        fig[3, 1];
        title = "1 - O_smoothed",
        xlabel = "number of alternatives",
        ylabel = "grouping",
        xticks = (xs_m, string.(m_values_int)),
        yticks = (1:length(data.grouping_values), group_labels),
    )

    hm = M.heatmap!(
        ax,
        xs_m,
        1:length(data.grouping_values),
        z;
        colormap = M.Reverse(:RdBu),
        colorrange = colorrange,
    )
    GroupedOverlapHeatmap2x2.annotate_heatmap_values!(ax, xs_m, z)

    cbgrid = M.GridLayout()
    fig[3, 2] = cbgrid
    M.Label(cbgrid[1, 1]; text = "max found = $(round(data_max; digits = 3))", halign = :center)
    M.Colorbar(cbgrid[2, 1], hm; label = "median 1 - O_smoothed")
    M.Label(cbgrid[3, 1]; text = "min found = $(round(data_min; digits = 3))", halign = :center)
    M.rowsize!(cbgrid, 1, M.Auto(0.15))
    M.rowsize!(cbgrid, 3, M.Auto(0.15))
    M.colsize!(fig.layout, 2, M.Relative(0.2))

    M.resize_to_layout!(fig)
    return fig
end

function run_o_smoothed_heatmap(year::Int,
                                scenario_name::AbstractString;
                                imputer_backend::Symbol = :mice,
                                groupings = nothing,
                                statistic::Symbol = :median,
                                consensus_tie_policy::Symbol = :average,
                                force_pipeline::Bool = false,
                                B = nothing,
                                R::Int = parse(Int, get(ENV, "PREFPOL_O_SMOOTHED_R", "2")),
                                K::Int = parse(Int, get(ENV, "PREFPOL_O_SMOOTHED_K", "2")),
                                output_root = OUTPUT_ROOT,
                                cache_root = CACHE_ROOT)
    mkpath(output_root)
    mkpath(cache_root)

    cfg, wave = load_target_wave(year)
    batch_groupings = selected_groupings(cfg, groupings)
    heatmap_groupings = groupings === nothing ? nothing : batch_groupings
    resolved_B = B === nothing ? cfg.n_bootstrap : Int(B)

    pipeline = pp.NestedStochasticPipeline([wave]; cache_root = cache_root)
    runner = pp.BatchRunner(pipeline)
    batch = build_target_batch(
        cfg,
        wave;
        scenario_name = scenario_name,
        groupings = batch_groupings,
        measures = [:O_smoothed],
        B = resolved_B,
        R = R,
        K = K,
        imputer_backend = imputer_backend,
        consensus_tie_policy = consensus_tie_policy,
    )
    results = pp.run_batch(runner, batch; force = force_pipeline)
    heatmap_data = o_smoothed_heatmap_values(
        results;
        year = year,
        scenario_name = scenario_name,
        imputer_backend = imputer_backend,
        groupings = heatmap_groupings,
        statistic = statistic,
    )
    fig = plot_o_smoothed_heatmap(
        heatmap_data;
        year = year,
        scenario_name = scenario_name,
        imputer_backend = imputer_backend,
    )

    file_stub = string(year, "_", scenario_name, "_", Symbol(imputer_backend))
    path = joinpath(output_root, "o_smoothed_heatmap_" * file_stub * ".png")
    GroupedOverlapHeatmap2x2.save_figure(path, fig)

    return (
        cfg = cfg,
        wave = wave,
        results = results,
        heatmap_data = heatmap_data,
        figure = fig,
        figure_path = path,
        groupings = batch_groupings,
        selected_groupings = heatmap_groupings,
    )
end

function main(; year::Int = YEAR,
              scenario_name::AbstractString = SCENARIO_NAME,
              imputer_backend::Symbol = IMPUTER_BACKEND,
              groupings = GROUPINGS,
              statistic::Symbol = :median,
              consensus_tie_policy::Symbol = CONSENSUS_TIE_POLICY,
              force_pipeline::Bool = FORCE_PIPELINE,
              B = nothing,
              R::Int = R,
              K::Int = K)
    return run_o_smoothed_heatmap(
        year,
        scenario_name;
        imputer_backend = imputer_backend,
        groupings = groupings,
        statistic = statistic,
        consensus_tie_policy = consensus_tie_policy,
        force_pipeline = force_pipeline,
        B = B,
        R = R,
        K = K,
    )
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
