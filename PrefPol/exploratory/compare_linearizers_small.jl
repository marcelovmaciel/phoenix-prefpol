using Pkg

const PACKAGE_ROOT = normpath(joinpath(@__DIR__, ".."))
const TEST_ENV = joinpath(PACKAGE_ROOT, "test")

Pkg.activate(TEST_ENV)
PACKAGE_ROOT in LOAD_PATH || pushfirst!(LOAD_PATH, PACKAGE_ROOT)

using CairoMakie
using DataFrames
using OrderedCollections: OrderedDict
using PrefPol
import PrefPol as pp

const M = pp.Makie

const CONFIG_DIR = joinpath(pp.project_root, "config")
const CACHE_ROOT = joinpath(
    pp.project_root,
    "intermediate_data",
    "nested_pipeline",
    "exploratory_compare_linearizers_small_runnerlike",
)
const IMG_DIR = joinpath(pp.project_root, "running", "imgs")

const B_SMALL = 2
const R_SMALL = 2
const K_SMALL = 2
const FORCE_RUN = false

const CONSENSUS_TIE_POLICY = :average
const FULL_MEASURES = [:Psi, :R, :HHI, :RHHI, :C, :D, :G]
const GROUP_HEATMAP_MEASURES = [:C, :D, :G]
const IMPUTER_BACKENDS = [:zero, :mice]
const LINEARIZER_POLICIES = [:random_ties, :pattern_conditional]

const TARGET_SCENARIOS = [
    (year = 2018, scenario_name = "main_four"),
    (year = 2022, scenario_name = "lula_bolsonaro"),
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

function resolve_target_m_values(cfg::pp.ElectionConfig,
                                 wave::pp.SurveyWaveConfig,
                                 scenario_name::AbstractString)
    valid_m_values = Int[]

    for m in cfg.m_values_range
        try
            pp.resolve_active_candidate_set(wave; scenario_name = scenario_name, m = m)
            push!(valid_m_values, m)
        catch err
            println(
                "Skipping invalid m=",
                m,
                " for ",
                cfg.year,
                " / ",
                scenario_name,
                " (",
                sprint(showerror, err),
                ")",
            )
        end
    end

    isempty(valid_m_values) && error(
        "No valid m values were found for $(cfg.year) / $(scenario_name).",
    )

    return valid_m_values
end

function resolved_targets(cfgs, wave_by_year)
    targets = NamedTuple[]

    for target in TARGET_SCENARIOS
        cfg = cfgs[target.year]
        wave = wave_by_year[target.year]
        m_values = resolve_target_m_values(cfg, wave, target.scenario_name)
        push!(targets, (
            year = target.year,
            scenario_name = target.scenario_name,
            m_values = m_values,
            max_m = maximum(m_values),
        ))
    end

    return targets
end

function build_small_batch(targets, cfgs, wave_by_year)
    items = pp.StudyBatchItem[]

    for target in targets
        cfg = cfgs[target.year]
        wave = wave_by_year[target.year]

        for backend in IMPUTER_BACKENDS
            for policy in LINEARIZER_POLICIES
                for m in target.m_values
                    spec = pp.build_pipeline_spec(
                        wave;
                        scenario_name = target.scenario_name,
                        m = m,
                        groupings = Symbol.(cfg.demographics),
                        measures = FULL_MEASURES,
                        B = B_SMALL,
                        R = R_SMALL,
                        K = K_SMALL,
                        imputer_backend = backend,
                        consensus_tie_policy = CONSENSUS_TIE_POLICY,
                        linearizer_policy = policy,
                    )

                    push!(items, pp.StudyBatchItem(
                        spec;
                        year = target.year,
                        scenario_name = target.scenario_name,
                        m = m,
                        candidate_label = pp.describe_candidate_set(spec.active_candidates),
                    ))
                end
            end
        end
    end

    return pp.StudyBatchSpec(items)
end

function subset_results_by_policy(results::pp.BatchRunResult, policy::Symbol)
    items = pp.StudyBatchItem[]
    subset_results = OrderedDict{String,pp.PipelineResult}()
    subset_meta = Dict{String,NamedTuple}()

    for item in results.batch.items
        item.spec.linearizer_policy === policy || continue
        spec_hash = pp.pipeline_spec_hash(item.spec)
        push!(items, item)
        subset_results[spec_hash] = results.results[spec_hash]
        subset_meta[spec_hash] = results.metadata_by_hash[spec_hash]
    end

    isempty(items) && error("No batch items matched linearizer policy $(policy).")

    return pp.BatchRunResult(pp.StudyBatchSpec(items), subset_results, subset_meta)
end

function print_run_summary(targets)
    println("="^80)
    println("Small linearizer comparison diagnostic")
    println("="^80)
    println("Output directory:      ", IMG_DIR)
    println("Cache root:            ", CACHE_ROOT)
    println("B/R/K:                 ", B_SMALL, "/", R_SMALL, "/", K_SMALL)
    println("Force run:             ", FORCE_RUN)
    println("Linearizer policies:   ", join(string.(LINEARIZER_POLICIES), ", "))
    println("Imputation backends:   ", join(string.(IMPUTER_BACKENDS), ", "))
    println("Selected targets:")

    for target in targets
        println(
            "  - ",
            target.year,
            " ",
            target.scenario_name,
            " m=",
            first(target.m_values),
            ":",
            target.max_m,
            " (",
            join(target.m_values, ","),
            ")",
        )
    end

    println()
end

function print_m2_sanity_rows(rows::DataFrame;
                              year::Int,
                              scenario_name::AbstractString,
                              imputer_backend::Symbol,
                              linearizer_policy::Symbol)
    println(
        "m=2 sanity rows | year=",
        year,
        " scenario=",
        scenario_name,
        " backend=",
        imputer_backend,
        " policy=",
        linearizer_policy,
    )
    show(
        stdout,
        MIME"text/plain"(),
        rows;
        allrows = true,
        allcols = true,
    )
    println("\n")
end

function assert_m2_psi_equals_r(policy_panel::DataFrame;
                                year::Int,
                                scenario_name::AbstractString,
                                imputer_backend::Symbol,
                                linearizer_policy::Symbol)
    rows = pp.select_pipeline_panel_rows(
        policy_panel;
        year = year,
        scenario_name = scenario_name,
        m = 2,
        imputer_backend = imputer_backend,
        measures = [:Psi, :R],
        include_grouped = false,
    )

    nrow(rows) == 2 || error(
        "Expected exactly two scenario rows for m=2 / $(year) / $(scenario_name) / $(imputer_backend) / $(linearizer_policy), found $(nrow(rows)).",
    )

    rows = sort(rows, :measure)
    print_m2_sanity_rows(
        select(
            rows,
            :year,
            :scenario_name,
            :imputer_backend,
            :linearizer_policy,
            :m,
            :measure,
            :estimate,
            :mean_value,
            :q05,
            :q25,
            :q50,
            :q75,
            :q95,
            :min_value,
            :max_value,
        );
        year = year,
        scenario_name = scenario_name,
        imputer_backend = imputer_backend,
        linearizer_policy = linearizer_policy,
    )

    psi_row = rows[rows.measure .== :Psi, :]
    r_row = rows[rows.measure .== :R, :]
    nrow(psi_row) == 1 || error("Missing Psi row in m=2 sanity check.")
    nrow(r_row) == 1 || error("Missing R row in m=2 sanity check.")

    check_cols = [
        :estimate,
        :mean_value,
        :q05,
        :q25,
        :q50,
        :q75,
        :q95,
        :min_value,
        :max_value,
    ]
    tolerance = 1.0e-10
    mismatched = String[]

    for col in check_cols
        delta = abs(Float64(psi_row[1, col]) - Float64(r_row[1, col]))
        delta <= tolerance || push!(mismatched, "$(col)=Δ$(delta)")
    end

    isempty(mismatched) || error(
        "m=2 sanity check failed for $(year) / $(scenario_name) / $(imputer_backend) / $(linearizer_policy): " *
        join(mismatched, ", "),
    )
end

function enforce_quarter_yticks!(fig,
                                 result_or_results;
                                 year::Int,
                                 scenario_name::AbstractString,
                                 imputer_backend::Symbol)
    data = pp.pipeline_scenario_plot_data(
        result_or_results;
        year = year,
        scenario_name = scenario_name,
        imputer_backend = imputer_backend,
    )
    rows = data.rows
    values = Float64.(vcat(rows.q05, rows.q95, rows.estimate))
    ymin, ymax = extrema(values)
    step = 0.1
    tick_lo = step * floor(ymin / step)
    tick_hi = step * ceil(ymax / step)

    if tick_lo == tick_hi
        tick_lo -= step
        tick_hi += step
    end

    ticks = collect(tick_lo:step:tick_hi)
    ax = only([obj for obj in fig.content if obj isa Axis])
    ax.yticks[] = (ticks, string.(round.(ticks; digits = 1)))
    ylims!(ax, tick_lo, tick_hi)
    return fig
end

function render_scenario_plots(results_by_policy::Dict{Symbol,pp.BatchRunResult}, targets)
    saved_paths = String[]

    for target in targets
        for backend in IMPUTER_BACKENDS
            for policy in LINEARIZER_POLICIES
                stem = "$(target.year)_$(target.scenario_name)_$(backend)_$(policy)"
                fig = pp.plot_pipeline_scenario(
                    results_by_policy[policy];
                    year = target.year,
                    scenario_name = target.scenario_name,
                    imputer_backend = backend,
                    plot_kind = :dotwhisker,
                    connect_lines = true,
                )
                enforce_quarter_yticks!(
                    fig,
                    results_by_policy[policy];
                    year = target.year,
                    scenario_name = target.scenario_name,
                    imputer_backend = backend,
                )
                push!(saved_paths, pp.save_pipeline_plot(fig, stem * "_dot"; dir = IMG_DIR))
            end
        end
    end

    return saved_paths
end

function render_group_heatmaps(results_by_policy::Dict{Symbol,pp.BatchRunResult}, targets)
    saved_paths = String[]

    for target in targets
        for backend in IMPUTER_BACKENDS
            for policy in LINEARIZER_POLICIES
                stem = "$(target.year)_$(target.scenario_name)_$(backend)_$(policy)"
                fig = pp.plot_pipeline_group_heatmap(
                    results_by_policy[policy];
                    year = target.year,
                    scenario_name = target.scenario_name,
                    imputer_backend = backend,
                    measures = GROUP_HEATMAP_MEASURES,
                    statistic = :median,
                    groupings = nothing,
                    maxcols = 3,
                    colormap = M.Reverse(:RdBu),
                    fixed_colorrange = true,
                    show_values = true,
                    simplified_labels = true,
                    clist_size = 60,
                )
                push!(
                    saved_paths,
                    pp.save_pipeline_plot(fig, stem * "_group_heatmap"; dir = IMG_DIR),
                )
            end
        end
    end

    return saved_paths
end

function print_saved_paths(paths)
    println("="^80)
    println("Saved images")
    println("="^80)

    for path in paths
        println(path)
    end

    println()
end

mkpath(CACHE_ROOT)
mkpath(IMG_DIR)

cfgs, waves, wave_by_year = load_operational_configs()
targets = resolved_targets(cfgs, wave_by_year)
print_run_summary(targets)

pipeline = pp.NestedStochasticPipeline(waves; cache_root = CACHE_ROOT)
runner = pp.BatchRunner(pipeline)
batch = build_small_batch(targets, cfgs, wave_by_year)
results = pp.run_batch(runner, batch; force = FORCE_RUN)

results_by_policy = Dict{Symbol,pp.BatchRunResult}()
panels_by_policy = Dict{Symbol,DataFrame}()

for policy in LINEARIZER_POLICIES
    policy_results = subset_results_by_policy(results, policy)
    results_by_policy[policy] = policy_results
    panels_by_policy[policy] = pp.pipeline_panel_table(policy_results)
end

for policy in LINEARIZER_POLICIES
    policy_panel = panels_by_policy[policy]
    for target in targets
        for backend in IMPUTER_BACKENDS
            assert_m2_psi_equals_r(
                policy_panel;
                year = target.year,
                scenario_name = target.scenario_name,
                imputer_backend = backend,
                linearizer_policy = policy,
            )
        end
    end
end

saved_paths = vcat(
    render_scenario_plots(results_by_policy, targets),
    render_group_heatmaps(results_by_policy, targets),
)

for path in saved_paths
    isfile(path) || error("Expected saved plot was not found at $(path).")
end

print_saved_paths(saved_paths)
