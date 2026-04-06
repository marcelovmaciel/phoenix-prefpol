using Revise
using CairoMakie
using PrefPol
import PrefPol as pp

const CONFIG_DIR = joinpath("PrefPol", "config")
const CACHE_ROOT = joinpath(pp.project_root, "intermediate_data", "nested_pipeline", "operational")
const IMG_DIR = joinpath(pp.project_root, "imgs", "nested_pipeline")
const OPERATIONAL_R = parse(Int, get(ENV, "PREFPOL_NESTED_R", "2"))
const OPERATIONAL_K = parse(Int, get(ENV, "PREFPOL_NESTED_K", "2"))
const OPERATIONAL_FORCE = lowercase(get(ENV, "PREFPOL_NESTED_FORCE", "false")) in ("1", "true", "yes")
const OPERATIONAL_TIE_POLICY = :average
const FULL_MEASURES = [:Psi, :R, :HHI, :RHHI, :C, :D, :G]

const SCENARIO_PLOTS = [
    (year = 2006, scenario_name = "lula_alckmin", backends = [:mice, :zero]),
    (year = 2018, scenario_name = "main_four", backends = [:mice]),
    (year = 2018, scenario_name = "no_forcing", backends = [:mice]),
    (year = 2018, scenario_name = "lula_bolsonaro", backends = [:mice]),
    (year = 2022, scenario_name = "lula_bolsonaro", backends = [:mice, :zero]),
]

const GROUP_LINE_PLOTS = [
    (year = 2018, scenario_name = "main_four", imputer_backend = :mice,
     groupings = ["Income", "Ideology"], stem = "2018_main_four_group_main"),
    (year = 2018, scenario_name = "main_four", imputer_backend = :mice,
     groupings = ["Sex", "Religion", "Race", "Age", "Education"], stem = "2018_main_four_group_other"),
    (year = 2022, scenario_name = "lula_bolsonaro", imputer_backend = :mice,
     groupings = ["Ideology", "PT", "Abortion", "Religion", "Sex", "Income"], stem = "2022_lula_bolsonaro_group_main"),
    (year = 2022, scenario_name = "lula_bolsonaro", imputer_backend = :mice,
     groupings = ["Race", "Age", "Education"], stem = "2022_lula_bolsonaro_group_other"),
    (year = 2006, scenario_name = "lula_alckmin", imputer_backend = :mice,
     groupings = nothing, stem = "2006_lula_alckmin_group_lines"),
]

const GROUP_HEATMAP_PLOTS = [
    (year = 2018, scenario_name = "main_four", imputer_backend = :mice,
     measures = [:C, :D, :G], groupings = nothing, stem = "2018_main_four_group_heatmap"),
    (year = 2022, scenario_name = "lula_bolsonaro", imputer_backend = :mice,
     measures = [:C, :D, :G], groupings = nothing, stem = "2022_lula_bolsonaro_group_heatmap"),
    (year = 2006, scenario_name = "lula_alckmin", imputer_backend = :mice,
     measures = [:C, :D, :G], groupings = nothing, stem = "2006_lula_alckmin_group_heatmap"),
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

    for entry in GROUP_HEATMAP_PLOTS
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

function render_group_heatmaps(results)
    for entry in GROUP_HEATMAP_PLOTS
        fig = pp.plot_pipeline_group_heatmap(
            results;
            year = entry.year,
            scenario_name = entry.scenario_name,
            imputer_backend = entry.imputer_backend,
            measures = entry.measures,
            groupings = entry.groupings,
            statistic = :median,
            maxcols = 3,
            colormap = CairoMakie.Makie.Reverse(:RdBu),
            fixed_colorrange = true,
            show_values = true,
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
render_group_heatmaps(results)
