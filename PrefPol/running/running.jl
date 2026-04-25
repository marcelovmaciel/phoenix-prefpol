# Run with:
#   julia +1.11.9 --startup-file=no --project=PrefPol/running/plotting_env -e 'using Pkg; Pkg.instantiate()'
#   julia +1.11.9 --startup-file=no --project=PrefPol/running/plotting_env PrefPol/running/running.jl

include(joinpath(@__DIR__, "plotting_setup.jl"))
ensure_prefpol_plotting_environment!()

using Revise
using CairoMakie
using PrefPol
import PrefPol as pp

const _PLOT_EXT = ensure_prefpol_plotting_extension!(pp)
const M = CairoMakie.Makie
const CONFIG_DIR = joinpath("PrefPol", "config")
const CACHE_ROOT = joinpath(pp.project_root, "intermediate_data", "nested_pipeline", "operational")
const IMG_DIR = joinpath(pp.project_root, "imgs", "nested_pipeline")
const OPERATIONAL_R = parse(Int, get(ENV, "PREFPOL_NESTED_R", "2"))
const OPERATIONAL_K = parse(Int, get(ENV, "PREFPOL_NESTED_K", "2"))
const OPERATIONAL_FORCE = lowercase(get(ENV, "PREFPOL_NESTED_FORCE", "false")) in ("1", "true", "yes")
const OPERATIONAL_TIE_POLICY = :average
# Include `:O` and cleaned `:S` because the replaced grouped summary slot is
# now the shared-scale `C | 1 - O | S` triplet panel.
const FULL_MEASURES = [:Psi, :R, :HHI, :RHHI, :C, :D, :O, :S, :G]
const SCENARIO_2006_MAIN = "main_2006"
const SCENARIO_2018_MAIN = "main_2018"
const SCENARIO_2022_MAIN = "main_2022"
const SCENARIO_2022_DIAGNOSTIC = "no_forcing"

const SCENARIO_PLOTS = [
    (year = 2006, scenario_name = SCENARIO_2006_MAIN, backends = [:mice, :zero]),
    (year = 2018, scenario_name = SCENARIO_2018_MAIN, backends = [:mice]),
    (year = 2022, scenario_name = SCENARIO_2022_MAIN, backends = [:mice, :zero]),
    (year = 2022, scenario_name = SCENARIO_2022_DIAGNOSTIC, backends = [:mice]),
]

const GROUP_LINE_PLOTS = [
    (year = 2018, scenario_name = SCENARIO_2018_MAIN, imputer_backend = :mice,
     groupings = ["Sex", "Religion", "Race", "Age", "Education", "Income", "Ideology", "LulaScoreGroup"], stem = "2018_main_group_lines"),
    (year = 2022, scenario_name = SCENARIO_2022_MAIN, imputer_backend = :mice,
     groupings = ["Ideology", "PT", "Abortion", "Religion", "Sex", "Income"], stem = "2022_main_group_main"),
    (year = 2022, scenario_name = SCENARIO_2022_MAIN, imputer_backend = :mice,
     groupings = ["Race", "Age", "Education"], stem = "2022_main_group_other"),
    (year = 2006, scenario_name = SCENARIO_2006_MAIN, imputer_backend = :mice,
     groupings = nothing, stem = "2006_main_group_lines"),
]

const GROUP_TRIPLET_PANEL_PLOTS = [
    (year = 2018, scenario_name = SCENARIO_2018_MAIN, imputer_backend = :mice,
     groupings = ["Sex", "Religion", "Race", "Age", "Education", "Income", "Ideology", "LulaScoreGroup"], stem = "2018_main_group_triplet_panel_C_1mO_S"),
    (year = 2022, scenario_name = SCENARIO_2022_MAIN, imputer_backend = :mice,
     groupings = nothing, stem = "2022_main_group_triplet_panel_C_1mO_S"),
    (year = 2006, scenario_name = SCENARIO_2006_MAIN, imputer_backend = :mice,
     groupings = nothing, stem = "2006_main_group_triplet_panel_C_1mO_S"),
]

function load_operational_configs(cfgdir::AbstractString = CONFIG_DIR)
    cfgs = Dict{Int,pp.ElectionConfig}()
    waves = pp.SurveyWaveConfig[]

    for path in sort(filter(p -> endswith(p, ".toml"), readdir(cfgdir; join = true)))
        cfg = pp.load_election_cfg(path)
        cfgs[cfg.year] = cfg
        push!(waves, pp.SurveyWaveConfig(cfg; wave_id = string(cfg.year)))
    end

    wave_by_year = Dict(wave.year => wave for wave in waves)
    return cfgs, waves, wave_by_year
end

function operational_targets()
    targets = Set{Tuple{Int,String,Symbol}}()

    for entry in SCENARIO_PLOTS
        for backend in entry.backends
            push!(targets, (entry.year, entry.scenario_name, backend))
        end
    end

    for entry in GROUP_LINE_PLOTS
        push!(targets, (entry.year, entry.scenario_name, entry.imputer_backend))
    end

    for entry in GROUP_TRIPLET_PANEL_PLOTS
        push!(targets, (entry.year, entry.scenario_name, entry.imputer_backend))
    end

    return sort!(collect(targets); by = x -> (x[1], x[2], string(x[3])))
end

function build_operational_batch(cfgs, wave_by_year)
    items = pp.StudyBatchItem[]

    for (year, scenario_name, backend) in operational_targets()
        cfg = cfgs[year]
        wave = wave_by_year[year]

        for m in cfg.m_values_range
            spec = pp.build_pipeline_spec(
                wave;
                scenario_name = scenario_name,
                m = m,
                groupings = Symbol.(cfg.demographics),
                measures = FULL_MEASURES,
                B = cfg.n_bootstrap,
                R = OPERATIONAL_R,
                K = OPERATIONAL_K,
                imputer_backend = backend,
                consensus_tie_policy = OPERATIONAL_TIE_POLICY,
            )
            push!(items, pp.StudyBatchItem(
                spec;
                year = year,
                scenario_name = scenario_name,
                m = m,
                candidate_label = pp.describe_candidate_set(spec.active_candidates),
            ))
        end
    end

    return pp.StudyBatchSpec(items)
end

function render_scenario_plots(results)
    for entry in SCENARIO_PLOTS
        for backend in entry.backends
            stem = "$(entry.year)_$(entry.scenario_name)_$(backend)"
            fig_lines = pp.plot_pipeline_scenario(
                results;
                year = entry.year,
                scenario_name = entry.scenario_name,
                imputer_backend = backend,
                plot_kind = :lines,
            )
            pp.save_pipeline_plot(fig_lines, stem; dir = IMG_DIR)

            fig_dot = pp.plot_pipeline_scenario(
                results;
                year = entry.year,
                scenario_name = entry.scenario_name,
                imputer_backend = backend,
                plot_kind = :dotwhisker,
                connect_lines = true,
            )
            pp.save_pipeline_plot(fig_dot, stem * "_dot"; dir = IMG_DIR)
        end
    end
end

function render_group_line_plots(results)
    for entry in GROUP_LINE_PLOTS
        fig = pp.plot_pipeline_group_lines(
            results;
            year = entry.year,
            scenario_name = entry.scenario_name,
            imputer_backend = entry.imputer_backend,
            groupings = entry.groupings,
            measures = [:C, :D, :G],
            maxcols = 3,
            clist_size = 60,
        )
        pp.save_pipeline_plot(fig, entry.stem; dir = IMG_DIR)
    end
end

function render_group_triplet_panels(results)
    for entry in GROUP_TRIPLET_PANEL_PLOTS
        # Replace the old grouped summary heatmap slot rather than adding a new
        # grouped figure alongside the other heatmap outputs.
        fig = _PLOT_EXT.plot_pipeline_group_triplet_panel(
            results;
            year = entry.year,
            scenario_name = entry.scenario_name,
            imputer_backend = entry.imputer_backend,
            groupings = entry.groupings,
            statistic = :median,
            colormap = M.Reverse(:RdBu),
            show_values = true,
            colorbar_label = "median grouped value",
            simplified_labels = true,
            clist_size = 60,
        )
        pp.save_pipeline_plot(fig, entry.stem; dir = IMG_DIR)
    end
end

cfgs, waves, wave_by_year = load_operational_configs()
pipeline = pp.NestedStochasticPipeline(waves; cache_root = CACHE_ROOT)
runner = pp.BatchRunner(pipeline)
batch = build_operational_batch(cfgs, wave_by_year)
results = pp.run_batch(runner, batch; force = OPERATIONAL_FORCE)

panel_table = pp.pipeline_panel_table(results)
summary_table = pp.pipeline_summary_table(results)

render_scenario_plots(results)
render_group_line_plots(results)
render_group_triplet_panels(results)
