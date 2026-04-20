"""
Visualize the grouped median-distance composite `G_median = sqrt(C * D_median)`.

This script runs the existing nested pipeline for one year/scenario/backend
target, computes `G_median` from the raw grouped draws of `C` and `D_median`,
and writes one grouped heatmap to `PrefPol/exploratory/output/g_median_heatmap/`.

`G_median` uses the current coherence component `C` together with the
consensus-only median-distance component `D_median`. It does not use the legacy grouped composite
based on `D`.
"""

using CairoMakie
using DataFrames
using PrefPol
using Printf
using Statistics
using TextWrap
import PrefPol as pp

const M = pp.Makie

const YEAR = 2022
const SCENARIO_NAME = "lula_bolsonaro_ciro_marina_tebet"
const IMPUTER_BACKEND = :mice
const GROUPINGS = nothing
const CONSENSUS_TIE_POLICY = :average
const FORCE_PIPELINE = false
const R = parse(Int, get(ENV, "PREFPOL_G_CLEAN_HEATMAP_R", "2"))
const K = parse(Int, get(ENV, "PREFPOL_G_CLEAN_HEATMAP_K", "2"))

const SCRIPT_STEM = "g_median_heatmap"
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
            measures = [:C, :D_median],
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

function save_figure(path::AbstractString, fig)
    mkpath(dirname(path))
    pp.save(path, fig; px_per_unit = 4)
    println("saved ", path)
    return path
end

function _filter_group_measure_rows(df::AbstractDataFrame, measure::Symbol; groupings = nothing)
    backend_name = String(Symbol(IMPUTER_BACKEND))
    rows = df[
        (df.year .== YEAR) .&
        (String.(df.scenario_name) .== SCENARIO_NAME) .&
        (String.(df.imputer_backend) .== backend_name) .&
        (df.measure .== measure) .&
        (.!ismissing.(df.grouping)),
        :,
    ]

    if groupings !== nothing
        wanted = Set(Symbol.(groupings))
        rows = rows[[Symbol(row.grouping) in wanted for row in eachrow(rows)], :]
    end

    isempty(rows) && error(
        "No grouped `$measure` rows matched year=$YEAR, scenario=$SCENARIO_NAME, " *
        "backend=$(Symbol(IMPUTER_BACKEND)).",
    )
    return rows
end

function _assert_unique_draw_rows(df::AbstractDataFrame, keys::Vector{Symbol}, measure::Symbol)
    dupes = combine(groupby(df, keys), nrow => :n_rows)
    any(dupes.n_rows .!= 1) && error(
        "Expected exactly one grouped `$measure` row per draw key $(keys), but found duplicates.",
    )
    return df
end

_q05(x) = quantile(x, 0.05)
_q25(x) = quantile(x, 0.25)
_q75(x) = quantile(x, 0.75)
_q95(x) = quantile(x, 0.95)

function compute_g_median_heatmap_inputs(results; groupings = nothing)
    measure_table = pp.pipeline_measure_table(results)
    panel_table = pp.pipeline_panel_table(results)
    panel_rows = pp.select_pipeline_panel_rows(
        panel_table;
        year = YEAR,
        scenario_name = SCENARIO_NAME,
        imputer_backend = IMPUTER_BACKEND,
        measures = [:C],
        groupings = groupings,
        include_grouped = true,
    )

    isempty(panel_rows) && error(
        "No grouped panel rows matched year=$YEAR, scenario=$SCENARIO_NAME, " *
        "backend=$(Symbol(IMPUTER_BACKEND)).",
    )

    c_rows = _filter_group_measure_rows(measure_table, :C; groupings = groupings)
    dmedian_rows = _filter_group_measure_rows(measure_table, :D_median; groupings = groupings)

    join_keys = [:spec_hash, :b, :r, :k, :grouping]
    _assert_unique_draw_rows(c_rows, join_keys, :C)
    _assert_unique_draw_rows(dmedian_rows, join_keys, :D_median)

    c_draws = select(
        c_rows,
        :spec_hash, :b, :r, :k, :grouping, :m,
        :value => :C_value,
    )
    dmedian_draws = select(
        dmedian_rows,
        :spec_hash, :b, :r, :k, :grouping,
        :value => :D_median_value,
    )

    joined = innerjoin(c_draws, dmedian_draws; on = join_keys)
    nrow(joined) == nrow(c_draws) || error(
        "Could not align grouped `C` and `D_median` draws one-to-one for G_median.",
    )
    hasproperty(joined, :D_median_value) || error(
        "Joined grouped draws are missing `D_median_value`; `D_median` is required for G_median.",
    )

    any(isnan, joined.C_value) && error("Encountered NaN in grouped `C` draws required for G_median.")
    any(isnan, joined.D_median_value) && error("Encountered NaN in grouped `D_median` draws required for G_median.")

    products = joined.C_value .* joined.D_median_value
    any((!isnan(x)) && (x < 0.0) for x in products) && error(
        "Encountered negative `C * D_median` while computing G_median.",
    )
    g_median_values = sqrt.(products)
    g_median_draws = select(joined, :m, :grouping)
    g_median_draws[!, :g_median_value] = g_median_values

    grouped_draws = combine(
        groupby(g_median_draws, [:m, :grouping]),
        :g_median_value => mean => :mean_value,
        :g_median_value => _q05 => :q05,
        :g_median_value => _q25 => :q25,
        :g_median_value => median => :q50,
        :g_median_value => _q75 => :q75,
        :g_median_value => _q95 => :q95,
        :g_median_value => minimum => :min_value,
        :g_median_value => maximum => :max_value,
        :g_median_value => length => :n_draws,
    )

    m_values = sort(unique(Int.(grouped_draws.m)))
    grouping_values = collect(unique(Symbol.(skipmissing(panel_rows.grouping))))
    m_lookup = Dict(m => idx for (idx, m) in enumerate(m_values))
    grouping_lookup = Dict(group => idx for (idx, group) in enumerate(grouping_values))
    matrix = fill(NaN, length(m_values), length(grouping_values))

    for row in eachrow(grouped_draws)
        matrix[m_lookup[Int(row.m)], grouping_lookup[Symbol(row.grouping)]] = Float64(row.q50)
    end

    title_txt = string(
        "Year ",
        YEAR,
        " • scenario = ",
        SCENARIO_NAME,
        " • backend = ",
        Symbol(IMPUTER_BACKEND),
        " • B = ",
        panel_rows[1, :B],
        ", R = ",
        panel_rows[1, :R],
        ", K = ",
        panel_rows[1, :K],
        " • draws = ",
        grouped_draws[1, :n_draws],
    )

    return (
        title_txt = title_txt,
        candidate_label = pp.pipeline_candidate_label(panel_rows),
        m_values = m_values,
        grouping_values = grouping_values,
        matrix = matrix,
    )
end

function annotate_heatmap_values!(ax, xs_m, z::AbstractMatrix)
    n_m, n_groups = size(z)
    xs = repeat(xs_m, inner = n_groups)
    ys = repeat(collect(1:n_groups), outer = n_m)
    labels = [isnan(z[i, j]) ? "NA" : @sprintf("%.3f", z[i, j]) for i in 1:n_m for j in 1:n_groups]
    M.text!(ax, xs, ys; text = labels, align = (:center, :center), color = :black, fontsize = 8)
    return ax
end

function plot_g_median_heatmap(data)
    m_values_int = Int.(data.m_values)
    xs_m = Float32.(m_values_int)
    group_labels = string.(data.grouping_values)

    fig = M.Figure(resolution = (max(360, 10 * length(data.title_txt) + 60), 360))
    M.rowgap!(fig.layout, 24)
    M.colgap!(fig.layout, 24)

    fig[1, 1] = M.Label(fig, data.title_txt; fontsize = 20, halign = :left)
    fig[2, 1] = M.Label(
        fig,
        join(TextWrap.wrap(data.candidate_label; width = 60));
        fontsize = 14,
        halign = :left,
    )

    ax = M.Axis(
        fig[3, 1];
        title = "G_median = sqrt(C * D_median)",
        xlabel = "number of alternatives",
        ylabel = "grouping",
        xticks = (xs_m, string.(m_values_int)),
        yticks = (1:length(data.grouping_values), group_labels),
    )

    z = Float32.(data.matrix)
    hm = M.heatmap!(
        ax,
        xs_m,
        1:length(data.grouping_values),
        z;
        colormap = M.Reverse(:RdBu),
        colorrange = (0.0f0, 1.0f0),
    )
    annotate_heatmap_values!(ax, xs_m, z)
    M.Colorbar(fig[3, 2], hm; label = "median grouped composite based on D_median")

    M.resize_to_layout!(fig)
    return fig
end

cfg, wave = load_target_wave(YEAR)
pipeline = pp.NestedStochasticPipeline([wave]; cache_root = CACHE_ROOT)
runner = pp.BatchRunner(pipeline)
batch = build_target_batch(cfg, wave)
results = pp.run_batch(runner, batch; force = FORCE_PIPELINE)

groupings = GROUPINGS === nothing ? nothing : selected_groupings(cfg)
heatmap_inputs = compute_g_median_heatmap_inputs(results; groupings = groupings)
fig_g_median = plot_g_median_heatmap(heatmap_inputs)

file_stub = string(YEAR, "_", SCENARIO_NAME, "_", Symbol(IMPUTER_BACKEND))
g_median_path = joinpath(OUTPUT_ROOT, "g_median_heatmap_" * file_stub * ".png")

save_figure(g_median_path, fig_g_median)
