using Revise 
using Pkg

const PACKAGE_ROOT = normpath(joinpath(@__DIR__, ".."))
const TEST_ENV = joinpath(PACKAGE_ROOT, "test")

# Use the test environment so CairoMakie is available without changing the
# main package environment, but keep the package root in LOAD_PATH so `PrefPol`
# resolves to this checkout.
Pkg.activate(TEST_ENV)
Pkg.instantiate()
PACKAGE_ROOT in LOAD_PATH || pushfirst!(LOAD_PATH, PACKAGE_ROOT)

using CairoMakie
using DataFrames
using PrefPol
import PrefPol as pp

const M = pp.Makie

const CFG_PATH = joinpath(pp.project_root, "config", "2022.toml")
const CACHE_ROOT = joinpath(pp.project_root, "exploratory", "_tmp", "nested_pipeline_demo_2022")
const IMG_DIR = joinpath(pp.project_root, "exploratory", "imgs", "nested_pipeline_demo_2022")
const B = 2
const R = 2
const K = 2
const IMPUTER_BACKEND = :mice
const TIE_POLICY = :average

const FULL_MEASURES = [:Psi, :R, :HHI, :RHHI, :C, :D, :G]
const GROUP_LINE_GROUPINGS = ["Ideology", "PT", "Abortion", "Religion", "Sex", "Income"]
const SCENARIO_NAME = "lula_bolsonaro_ciro_marina_tebet"

mkpath(CACHE_ROOT)
mkpath(IMG_DIR)

cfg = pp.load_election_cfg(CFG_PATH)
wave = pp.SurveyWaveConfig(cfg; wave_id = string(cfg.year))
raw_df = pp.load_wave_data(wave)
scenario_name = SCENARIO_NAME

println("="^80)
println("Nested stochastic pipeline demo for year $(cfg.year)")
println("="^80)
println("Config path: ", CFG_PATH)
println("Data file:   ", cfg.data_file)
println("Scenario:    ", scenario_name)
println("Candidates:  ", length(cfg.candidates))
println("Demographics:", " ", join(cfg.demographics, ", "))
println("Raw rows:    ", nrow(raw_df))
println("B/R/K:       ", (B, R, K))
println("Backend:     ", IMPUTER_BACKEND)
println("Cache root:  ", CACHE_ROOT)
println("Image dir:   ", IMG_DIR)
println()

println("First five rows of the observed 2022 data:")
show(stdout, MIME"text/plain"(), first(raw_df, 5))
println("\n")

function build_demo_batch(wave::pp.SurveyWaveConfig,
                          cfg::pp.ElectionConfig,
                          scenario_name::AbstractString)
    items = pp.StudyBatchItem[]

    for m in cfg.m_values_range
        spec = pp.build_pipeline_spec(
            wave;
            scenario_name = scenario_name,
            m = m,
            groupings = Symbol.(cfg.demographics),
            measures = FULL_MEASURES,
            B = B,
            R = R,
            K = K,
            imputer_backend = IMPUTER_BACKEND,
            consensus_tie_policy = TIE_POLICY,
        )

        push!(items, pp.StudyBatchItem(
            spec;
            year = cfg.year,
            scenario_name = String(scenario_name),
            m = m,
            candidate_label = pp.describe_candidate_set(spec.active_candidates),
        ))
    end

    return pp.StudyBatchSpec(items)
end

pipeline = pp.NestedStochasticPipeline([wave]; cache_root = CACHE_ROOT)
runner = pp.BatchRunner(pipeline)
batch = build_demo_batch(wave, cfg, scenario_name)

results = pp.run_batch(runner, batch; force = true)

measure_table = pp.pipeline_measure_table(results)
summary_table = pp.pipeline_summary_table(results)
panel_table = pp.pipeline_panel_table(results)

measure_preview = select(
    sort(measure_table, [:m, :b, :r, :k, :measure, :grouping]),
    :year, :scenario_name, :m, :b, :r, :k, :measure, :grouping, :value, :value_lo, :value_hi,
)[1:min(20, nrow(measure_table)), :]

summary_preview = select(
    sort(summary_table, [:m, :measure, :grouping]),
    :year, :scenario_name, :m, :measure, :grouping,
    :estimate, :V_res, :V_imp, :V_lin, :total_sd,
)


filter(:measure => in(["HHI", "Psi", "R", "RHHI"]), summary_preview)

panel_preview = select(
    sort(panel_table, [:m, :measure, :grouping]),
    :year, :scenario_name, :m, :measure, :grouping,
    :n_draws, :q05, :q25, :q50, :q75, :q95, :estimate, :total_sd,
)

println("="^80)
println("Measure table preview")
println("="^80)
show(stdout, MIME"text/plain"(), measure_preview)
println("\n")

println("="^80)
println("Summary table")
println("="^80)
show(stdout, MIME"text/plain"(), summary_preview)
println("\n")

println("="^80)
println("Panel table")
println("="^80)
show(stdout, MIME"text/plain"(), panel_preview)
println("\n")

scenario_plot = pp.plot_pipeline_scenario(
    results;
    year = cfg.year,
    scenario_name = scenario_name,
    imputer_backend = IMPUTER_BACKEND,
    plot_kind = :dotwhisker,
    connect_lines = true,
)

group_lines_plot = pp.plot_pipeline_group_lines(
    results;
    year = cfg.year,
    scenario_name = scenario_name,
    imputer_backend = IMPUTER_BACKEND,
    groupings = GROUP_LINE_GROUPINGS,
    measures = [:C, :D, :G],
    maxcols = 3,
)

group_heatmap_plot = pp.plot_pipeline_group_heatmap(
    results;
    year = cfg.year,
    scenario_name = scenario_name,
    imputer_backend = IMPUTER_BACKEND,
    measures = [:C, :D, :G],
    statistic = :median,
    maxcols = 3,
    colormap = M.Reverse(:RdBu),
    fixed_colorrange = true,
    show_values = true,
    simplified_labels = true,
)

display(scenario_plot)

display(group_lines_plot)

display(group_heatmap_plot)

scenario_plot_path = pp.save_pipeline_plot(
    scenario_plot,
    "2022_nested_demo_scenario";
    dir = IMG_DIR,
)

group_lines_plot_path = pp.save_pipeline_plot(
    group_lines_plot,
    "2022_nested_demo_group_lines";
    dir = IMG_DIR,
)

group_heatmap_plot_path = pp.save_pipeline_plot(
    group_heatmap_plot,
    "2022_nested_demo_group_heatmap";
    dir = IMG_DIR,
)

println("="^80)
println("Saved plots")
println("="^80)
println(scenario_plot_path)
println(group_lines_plot_path)
println(group_heatmap_plot_path)
println()
println("Objects left in scope:")
println("  cfg, wave, raw_df, pipeline, runner, batch, results")
println("  measure_table, summary_table, panel_table")
println("  scenario_plot, group_lines_plot, group_heatmap_plot")
