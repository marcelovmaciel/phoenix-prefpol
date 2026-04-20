"""
Compare the current grouped divergence `D` against the consensus-only alternative `D_median`.

`D` averages distances from individuals in group `i` to the consensus of group `j`,
so it can change when within-group dispersion changes even if the estimated group
consensuses stay fixed. `D_median` instead uses only the estimated group consensus
rankings and computes a weighted average of normalized Kendall distances between
group consensuses, with unordered-pair weights proportional to `n_i * n_j`.

This script runs the existing nested pipeline for one year/scenario/backend target and
writes three heatmaps to `PrefPol/exploratory/output/d_median_heatmap_comparison/`:

- the current `D`
- the new `D_median`
- the difference `D_median - D`
"""

using CairoMakie
using PrefPol
using Printf
using TextWrap
import PrefPol as pp

const M = pp.Makie

const YEAR = 2022
const SCENARIO_NAME = "lula_bolsonaro_ciro_marina_tebet"
const IMPUTER_BACKEND = :mice
const GROUPINGS = nothing
const CONSENSUS_TIE_POLICY = :average
const FORCE_PIPELINE = false
const R = parse(Int, get(ENV, "PREFPOL_D_CLEAN_R", "2"))
const K = parse(Int, get(ENV, "PREFPOL_D_CLEAN_K", "2"))

const SCRIPT_STEM = "d_median_heatmap_comparison"
const OUTPUT_ROOT = joinpath(pp.project_root, "exploratory", "output", SCRIPT_STEM)
const CACHE_ROOT = joinpath(pp.project_root, "exploratory", "_tmp", SCRIPT_STEM)

mkpath(OUTPUT_ROOT)
mkpath(CACHE_ROOT)

function load_target_wave(year::Int)
    cfgdir = joinpath(pp.project_root, "config")

    for path in sort(filter(p -> endswith(p, ".toml"), readdir(cfgdir; join = true)))
        cfg = pp.load_election_cfg(path)
        cfg.year == year || continue
        return cfg, pp.SurveyWaveConfig(cfg; wave_id = string(cfg.year))
    end

    error("No election config found for year $year.")
end

function selected_groupings(cfg)
    return GROUPINGS === nothing ? Symbol.(cfg.demographics) : Symbol.(collect(GROUPINGS))
end

function build_target_batch(cfg, wave)
    items = pp.StudyBatchItem[]
    groupings = selected_groupings(cfg)

    for m in cfg.m_values_range
        spec = pp.build_pipeline_spec(
            wave;
            scenario_name = SCENARIO_NAME,
            m = m,
            groupings = groupings,
            measures = [:C, :D, :D_median, :G],
            B = cfg.n_bootstrap,
            R = R,
            K = K,
            imputer_backend = IMPUTER_BACKEND,
            consensus_tie_policy = CONSENSUS_TIE_POLICY,
        )
        push!(items, pp.StudyBatchItem(
            spec;
            year = YEAR,
            scenario_name = SCENARIO_NAME,
            m = m,
            candidate_label = pp.describe_candidate_set(spec.active_candidates),
        ))
    end

    return pp.StudyBatchSpec(items)
end

function annotate_heatmap_values!(ax, xs_m, z::AbstractMatrix)
    n_m, n_groups = size(z)
    xs = repeat(xs_m, inner = n_groups)
    ys = repeat(collect(1:n_groups), outer = n_m)
    labels = [@sprintf("%.3f", z[i, j]) for i in 1:n_m for j in 1:n_groups]
    M.text!(ax, xs, ys; text = labels, align = (:center, :center), color = :black, fontsize = 8)
    return ax
end

function plot_difference_heatmap(diff_matrix::AbstractMatrix, heatmap_data)
    rows = heatmap_data.rows
    m_values_int = Int.(heatmap_data.m_values)
    xs_m = Float32.(m_values_int)
    grouping_values = heatmap_data.grouping_values
    group_labels = string.(grouping_values)
    candidate_label = heatmap_data.candidate_label

    valid_values = collect(filter(!isnan, vec(Float64.(diff_matrix))))
    max_abs = isempty(valid_values) ? 1.0 : max(maximum(abs.(valid_values)), 1e-6)
    colorrange = (-Float32(max_abs), Float32(max_abs))

    title_txt = string(
        "Year ",
        YEAR,
        " • scenario = ",
        SCENARIO_NAME,
        " • backend = ",
        Symbol(IMPUTER_BACKEND),
        " • B = ",
        rows[1, :B],
        ", R = ",
        rows[1, :R],
        ", K = ",
        rows[1, :K],
        " • draws = ",
        rows[1, :n_draws],
    )

    fig = M.Figure(resolution = (max(360, 10 * length(title_txt) + 60), 360))
    M.rowgap!(fig.layout, 24)
    M.colgap!(fig.layout, 24)

    fig[1, 1] = M.Label(fig, title_txt; fontsize = 20, halign = :left)
    fig[2, 1] = M.Label(
        fig,
        join(TextWrap.wrap(candidate_label; width = 60));
        fontsize = 14,
        halign = :left,
    )

    ax = M.Axis(
        fig[3, 1];
        title = "D_median - D",
        xlabel = "number of alternatives",
        ylabel = "grouping",
        xticks = (xs_m, string.(m_values_int)),
        yticks = (1:length(grouping_values), group_labels),
    )

    z = Float32.(diff_matrix)
    hm = M.heatmap!(
        ax,
        xs_m,
        1:length(grouping_values),
        z;
        colormap = M.Reverse(:RdBu),
        colorrange = colorrange,
    )
    annotate_heatmap_values!(ax, xs_m, z)
    M.Colorbar(fig[3, 2], hm; label = "median difference")

    M.resize_to_layout!(fig)
    return fig
end

function save_figure(path::AbstractString, fig)
    mkpath(dirname(path))
    pp.save(path, fig; px_per_unit = 4)
    println("saved ", path)
    return path
end

cfg, wave = load_target_wave(YEAR)
pipeline = pp.NestedStochasticPipeline([wave]; cache_root = CACHE_ROOT)
runner = pp.BatchRunner(pipeline)
batch = build_target_batch(cfg, wave)
results = pp.run_batch(runner, batch; force = FORCE_PIPELINE)

groupings = GROUPINGS === nothing ? nothing : selected_groupings(cfg)

fig_d = pp.plot_pipeline_group_heatmap(
    results;
    year = YEAR,
    scenario_name = SCENARIO_NAME,
    imputer_backend = IMPUTER_BACKEND,
    measures = [:D],
    groupings = groupings,
    statistic = :median,
    maxcols = 1,
    colormap = M.Reverse(:RdBu),
    fixed_colorrange = true,
    show_values = true,
    simplified_labels = false,
    clist_size = 60,
)

fig_d_median = pp.plot_pipeline_group_heatmap(
    results;
    year = YEAR,
    scenario_name = SCENARIO_NAME,
    imputer_backend = IMPUTER_BACKEND,
    measures = [:D_median],
    groupings = groupings,
    statistic = :median,
    maxcols = 1,
    colormap = M.Reverse(:RdBu),
    fixed_colorrange = true,
    show_values = true,
    simplified_labels = false,
    clist_size = 60,
)

d_data = pp.pipeline_group_heatmap_values(
    results;
    year = YEAR,
    scenario_name = SCENARIO_NAME,
    imputer_backend = IMPUTER_BACKEND,
    measures = [:D],
    groupings = groupings,
    statistic = :median,
)

d_median_data = pp.pipeline_group_heatmap_values(
    results;
    year = YEAR,
    scenario_name = SCENARIO_NAME,
    imputer_backend = IMPUTER_BACKEND,
    measures = [:D_median],
    groupings = groupings,
    statistic = :median,
)

d_data.m_values == d_median_data.m_values || error("D and D_median m grids do not match.")
d_data.grouping_values == d_median_data.grouping_values || error("D and D_median grouping grids do not match.")

diff_matrix = d_median_data.matrices[:D_median] .- d_data.matrices[:D]
fig_diff = plot_difference_heatmap(diff_matrix, d_median_data)

file_stub = string(YEAR, "_", SCENARIO_NAME, "_", Symbol(IMPUTER_BACKEND))
d_path = joinpath(OUTPUT_ROOT, "d_current_heatmap_" * file_stub * ".png")
d_median_path = joinpath(OUTPUT_ROOT, "d_median_heatmap_" * file_stub * ".png")
diff_path = joinpath(OUTPUT_ROOT, "d_median_minus_d_heatmap_" * file_stub * ".png")

save_figure(d_path, fig_d)
save_figure(d_median_path, fig_d_median)
save_figure(diff_path, fig_diff)
