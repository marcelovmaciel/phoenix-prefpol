"""
Visualize the grouped statistic `S`, a signed support-separation contrast.

`S` compares between-group separation in ranking space against average internal
group spread using the full empirical group distributions, not group consensuses.

- positive: groups are farther from each other than their average internal spread
- zero: no extra support separation beyond internal spread
- negative: within-group spread dominates between-group separation

This script runs the existing nested pipeline for one year/scenario/backend target
and writes one heatmap to `PrefPol/exploratory/output/s_heatmap/`.
"""

using Pkg

const PACKAGE_ROOT = normpath(joinpath(@__DIR__, ".."))
const TEST_ENV = joinpath(PACKAGE_ROOT, "test")

Pkg.activate(TEST_ENV)
PACKAGE_ROOT in LOAD_PATH || pushfirst!(LOAD_PATH, PACKAGE_ROOT)

using CairoMakie
using PrefPol
import PrefPol as pp

const M = pp.Makie

const YEAR = 2022
const SCENARIO_NAME = "lula_bolsonaro_ciro_marina_tebet"
const IMPUTER_BACKEND = :mice
const GROUPINGS = nothing
const CONSENSUS_TIE_POLICY = :average
const FORCE_PIPELINE = false
const R = parse(Int, get(ENV, "PREFPOL_S_HEATMAP_R", "2"))
const K = parse(Int, get(ENV, "PREFPOL_S_HEATMAP_K", "2"))

const SCRIPT_STEM = "s_heatmap"
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
            measures = [:S],
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

cfg, wave = load_target_wave(YEAR)
pipeline = pp.NestedStochasticPipeline([wave]; cache_root = CACHE_ROOT)
runner = pp.BatchRunner(pipeline)
batch = build_target_batch(cfg, wave)
results = pp.run_batch(runner, batch; force = FORCE_PIPELINE)

groupings = GROUPINGS === nothing ? nothing : selected_groupings(cfg)

fig_s = pp.plot_pipeline_group_heatmap(
    results;
    year = YEAR,
    scenario_name = SCENARIO_NAME,
    imputer_backend = IMPUTER_BACKEND,
    measures = [:S],
    groupings = groupings,
    statistic = :median,
    maxcols = 1,
    colormap = M.Reverse(:RdBu),
    fixed_colorrange_limits = (-1.0, 1.0),
    show_values = true,
    colorbar_label = "median signed support-separation contrast",
    simplified_labels = false,
    clist_size = 60,
)

file_stub = string(YEAR, "_", SCENARIO_NAME, "_", Symbol(IMPUTER_BACKEND))
s_path = joinpath(OUTPUT_ROOT, "s_heatmap_" * file_stub * ".png")

save_figure(s_path, fig_s)
