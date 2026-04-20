module GroupedOverlapHeatmap2x2

using Pkg

const PACKAGE_ROOT = normpath(joinpath(@__DIR__, ".."))
const TEST_ENV = joinpath(PACKAGE_ROOT, "test")

Pkg.activate(TEST_ENV)
PACKAGE_ROOT in LOAD_PATH || pushfirst!(LOAD_PATH, PACKAGE_ROOT)

using CairoMakie
using PrefPol
using Printf
using TextWrap
import PrefPol as pp

const M = pp.Makie

const GROUPED_OVERLAP_PANEL_ORDER = [:C, :D_median, :O, :Gsep]
const GROUPED_OVERLAP_PANEL_LABELS = Dict(
    :C => "C",
    :D_median => "D",
    :O => "O",
    :Gsep => "G",
)

function load_target_wave(year::Int)
    cfgdir = joinpath(pp.project_root, "config")

    for path in sort(filter(p -> endswith(p, ".toml"), readdir(cfgdir; join = true)))
        cfg = pp.load_election_cfg(path)
        cfg.year == year || continue
        return cfg, pp.SurveyWaveConfig(cfg; wave_id = string(cfg.year))
    end

    error("No election config found for year $year.")
end

function selected_groupings(cfg, groupings)
    return groupings === nothing ? Symbol.(cfg.demographics) : Symbol.(collect(groupings))
end

function build_target_batch(cfg,
                            wave;
                            scenario_name,
                            groupings,
                            measures = GROUPED_OVERLAP_PANEL_ORDER,
                            B::Int = cfg.n_bootstrap,
                            R::Int = 2,
                            K::Int = 2,
                            imputer_backend::Symbol = :mice,
                            consensus_tie_policy::Symbol = :average)
    items = pp.StudyBatchItem[]

    for m in cfg.m_values_range
        spec = pp.build_pipeline_spec(
            wave;
            scenario_name = scenario_name,
            m = m,
            groupings = groupings,
            measures = measures,
            B = B,
            R = R,
            K = K,
            imputer_backend = imputer_backend,
            consensus_tie_policy = consensus_tie_policy,
        )
        push!(items, pp.StudyBatchItem(
            spec;
            year = cfg.year,
            scenario_name = scenario_name,
            m = m,
            candidate_label = pp.describe_candidate_set(spec.active_candidates),
        ))
    end

    return pp.StudyBatchSpec(items)
end

function _plot_title(rows;
                     year::Int,
                     scenario_name::AbstractString,
                     imputer_backend::Symbol)
    return string(
        "Year ",
        year,
        " • scenario = ",
        scenario_name,
        " • backend = ",
        imputer_backend,
        " • B = ",
        rows[1, :B],
        ", R = ",
        rows[1, :R],
        ", K = ",
        rows[1, :K],
        " • draws = ",
        rows[1, :n_draws],
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

function grouped_overlap_heatmap_values(results;
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
        measures = GROUPED_OVERLAP_PANEL_ORDER,
        groupings = groupings,
        statistic = statistic,
    )
end

function plot_grouped_overlap_heatmap(data;
                                      year::Int,
                                      scenario_name::AbstractString,
                                      imputer_backend::Symbol = :mice,
                                      colorrange = (0.0f0, 1.0f0))
    rows = data.rows
    m_values_int = Int.(data.m_values)
    xs_m = Float32.(m_values_int)
    group_labels = string.(data.grouping_values)
    allvals = Float32[]
    for measure in GROUPED_OVERLAP_PANEL_ORDER
        append!(allvals, Float32.(filter(!isnan, vec(data.matrices[measure]))))
    end
    data_min, data_max = isempty(allvals) ? (0.0f0, 1.0f0) : extrema(allvals)
    title_txt = _plot_title(
        rows;
        year = year,
        scenario_name = scenario_name,
        imputer_backend = imputer_backend,
    )

    fig = M.Figure(resolution = (max(720, 10 * length(title_txt) + 60), 720))
    M.rowgap!(fig.layout, 24)
    M.colgap!(fig.layout, 24)

    fig[1, 1:2] = M.Label(fig, title_txt; fontsize = 20, halign = :left)
    fig[2, 1:2] = M.Label(
        fig,
        join(TextWrap.wrap(data.candidate_label; width = 72));
        fontsize = 14,
        halign = :left,
    )

    header_rows = 2
    hm_ref = nothing

    for (idx, measure) in enumerate(GROUPED_OVERLAP_PANEL_ORDER)
        r, c = fldmod1(idx, 2)
        ax = M.Axis(
            fig[r + header_rows, c];
            title = GROUPED_OVERLAP_PANEL_LABELS[measure],
            xlabel = "number of alternatives",
            ylabel = c == 1 ? "grouping" : "",
            xticks = (xs_m, string.(m_values_int)),
            yticks = (1:length(data.grouping_values), group_labels),
        )

        z = Float32.(data.matrices[measure])
        hm = M.heatmap!(
            ax,
            xs_m,
            1:length(data.grouping_values),
            z;
            colormap = M.Reverse(:RdBu),
            colorrange = colorrange,
        )
        annotate_heatmap_values!(ax, xs_m, z)
        hm_ref === nothing && (hm_ref = hm)
    end

    if hm_ref !== nothing
        cbgrid = M.GridLayout()
        fig[header_rows + 1:header_rows + 2, 3] = cbgrid
        M.Label(cbgrid[1, 1]; text = "max found = $(round(data_max; digits = 3))", halign = :center)
        M.Colorbar(cbgrid[2, 1], hm_ref; label = "median grouped measure")
        M.Label(cbgrid[3, 1]; text = "min found = $(round(data_min; digits = 3))", halign = :center)
        M.rowsize!(cbgrid, 1, M.Auto(0.15))
        M.rowsize!(cbgrid, 3, M.Auto(0.15))
        M.colsize!(fig.layout, 3, M.Relative(0.25))
    end

    M.resize_to_layout!(fig)
    return fig
end

function save_figure(path::AbstractString, fig)
    mkpath(dirname(path))
    pp.save(path, fig; px_per_unit = 4)
    println("saved ", path)
    return path
end

function run_grouped_overlap_heatmap(year::Int,
                                     scenario_name::AbstractString;
                                     imputer_backend::Symbol = :mice,
                                     groupings = nothing,
                                     statistic::Symbol = :median,
                                     consensus_tie_policy::Symbol = :average,
                                     force_pipeline::Bool = false,
                                     B = nothing,
                                     R::Int = parse(Int, get(ENV, "PREFPOL_GROUPED_OVERLAP_R", "2")),
                                     K::Int = parse(Int, get(ENV, "PREFPOL_GROUPED_OVERLAP_K", "2")),
                                     output_root = joinpath(
                                         pp.project_root,
                                         "exploratory",
                                         "output",
                                         "grouped_overlap_heatmap_2x2",
                                     ),
                                     cache_root = joinpath(
                                         pp.project_root,
                                         "exploratory",
                                         "_tmp",
                                         "grouped_overlap_heatmap_2x2",
                                     ))
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
        B = resolved_B,
        R = R,
        K = K,
        imputer_backend = imputer_backend,
        consensus_tie_policy = consensus_tie_policy,
    )
    results = pp.run_batch(runner, batch; force = force_pipeline)
    heatmap_data = grouped_overlap_heatmap_values(
        results;
        year = year,
        scenario_name = scenario_name,
        imputer_backend = imputer_backend,
        groupings = heatmap_groupings,
        statistic = statistic,
    )
    fig = plot_grouped_overlap_heatmap(
        heatmap_data;
        year = year,
        scenario_name = scenario_name,
        imputer_backend = imputer_backend,
    )

    file_stub = string(year, "_", scenario_name, "_", Symbol(imputer_backend))
    path = joinpath(output_root, "grouped_overlap_heatmap_2x2_" * file_stub * ".png")
    save_figure(path, fig)

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

function main(; year::Int = 2022,
              scenario_name::AbstractString = "lula_bolsonaro",
              imputer_backend::Symbol = :mice,
              groupings = nothing,
              statistic::Symbol = :median,
              consensus_tie_policy::Symbol = :average,
              force_pipeline::Bool = false,
              B = nothing,
              R::Int = parse(Int, get(ENV, "PREFPOL_GROUPED_OVERLAP_R", "2")),
              K::Int = parse(Int, get(ENV, "PREFPOL_GROUPED_OVERLAP_K", "2")))
    return run_grouped_overlap_heatmap(
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

export GROUPED_OVERLAP_PANEL_ORDER,
       load_target_wave,
       selected_groupings,
       build_target_batch,
       grouped_overlap_heatmap_values,
       plot_grouped_overlap_heatmap,
       run_grouped_overlap_heatmap,
       main

end

if abspath(PROGRAM_FILE) == @__FILE__
    GroupedOverlapHeatmap2x2.main()
end
