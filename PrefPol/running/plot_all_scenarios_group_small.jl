"""
    plot_all_scenarios_group_small.jl

Load the saved nested-analysis outputs produced by
`running/run_all_scenarios_small.jl`, then generate the canonical grouped
plots and grouped heatmaps for every saved wave/scenario using the exported
nested plotting/data helpers.

This script does not rerun the analysis pipeline. It reads the saved manifest at
`running/output/all_scenarios_small/run_manifest.csv`, loads the cached
`PipelineResult`s referenced there, and writes plots/tables under
`running/output/all_scenarios_small/group/`.

Run later with:

    julia +1.11 --startup-file=no --project=PrefPol PrefPol/running/plot_all_scenarios_group_small.jl
"""

include(joinpath(@__DIR__, "plot_all_scenarios_global_small.jl"))

const GROUP_OUTPUT_ROOT = joinpath(SMALL_OUTPUT_ROOT, "group")
const M = pp.Makie

# These match the current canonical grouped plotting workflow:
# line plots use the exported defaults, while the heatmaps use the intended
# 2x2 panel set `C / D / 1 - O / G`.
const CANONICAL_GROUP_LINE_MEASURES = [:C, :D, :G]
const CANONICAL_GROUP_HEATMAP_MEASURES = [:C, :D_median, :O, :Gsep]
const CANONICAL_GROUP_HEATMAP_LABELS = Dict(
    :C => "C",
    :D_median => "D",
    :O => "1 - O",
    :Gsep => "G",
)
const O_SMOOTHED_MEASURES = [:O_smoothed]
const O_SMOOTHED_LABELS = Dict(:O_smoothed => "1 - O_smoothed")
const ALL_GROUP_MEASURES = [:C, :D, :D_median, :O, :O_smoothed, :Sep, :G, :Gsep, :S]

function group_output_dir(target)
    return joinpath(
        GROUP_OUTPUT_ROOT,
        sanitize_path_component(target.wave_id),
        sanitize_path_component(target.scenario_name),
    )
end

function _measure_mask(df::AbstractDataFrame, measures)
    wanted = Set(Symbol.(collect(measures)))
    return [Symbol(row.measure) in wanted for row in eachrow(df)]
end

function filter_group_rows(df::AbstractDataFrame; measures = ALL_GROUP_MEASURES)
    mask = _measure_mask(df, measures) .& .!ismissing.(df.grouping)
    return DataFrame(df[mask, :])
end

function prepare_group_plot_table(rows::AbstractDataFrame;
                                  value_column::Symbol = :estimate,
                                  complement_measures = Symbol[],
                                  measure_labels = nothing)
    out = DataFrame(rows)
    complemented = Set(Symbol.(collect(complement_measures)))

    if !(:m in propertynames(out)) && (:n_candidates in propertynames(out))
        out[!, :m] = Int.(out.n_candidates)
    end

    if value_column in propertynames(out)
        values = Float64.(out[!, value_column])
        out[!, :value] = [
            Symbol(row.measure) in complemented ? 1.0 - values[idx] : values[idx]
            for (idx, row) in enumerate(eachrow(out))
        ]
    elseif !(:value in propertynames(out)) && (:estimate in propertynames(out))
        out[!, :value] = Float64.(out.estimate)
    end

    if measure_labels !== nothing
        out[!, :display_measure] = [
            get(measure_labels, Symbol(row.measure), String(row.measure))
            for row in eachrow(out)
        ]
    end

    return sorted_table(out)
end

function combo_stem(combo)
    return string(
        sanitize_path_component(String(combo.imputer_backend)),
        "_",
        sanitize_path_component(String(combo.linearizer_policy)),
    )
end

function write_group_scenario_outputs!(results::pp.BatchRunResult,
                                       manifest::DataFrame,
                                       target)
    dir = group_output_dir(target)
    mkpath(dir)

    scenario_results = subset_batch_results(
        results;
        wave_id = target.wave_id,
        scenario_name = target.scenario_name,
    )

    decomposition_table = filter_group_rows(combined_decomposition_table(scenario_results))
    save_csv(joinpath(dir, "decomposition_table.csv"), sorted_table(decomposition_table))

    for combo in backend_combinations(manifest, target)
        combo_results = subset_batch_results(
            scenario_results;
            imputer_backend = combo.imputer_backend,
            linearizer_policy = combo.linearizer_policy,
        )
        stem = combo_stem(combo)

        line_data = pp.pipeline_group_plot_data(
            combo_results;
            wave_id = target.wave_id,
            scenario_name = target.scenario_name,
            imputer_backend = combo.imputer_backend,
            measures = CANONICAL_GROUP_LINE_MEASURES,
        )
        save_csv(
            joinpath(dir, "group_lines_table_" * stem * ".csv"),
            prepare_group_plot_table(line_data.rows; value_column = :estimate),
        )

        heatmap_data = pp.pipeline_group_heatmap_values(
            combo_results;
            wave_id = target.wave_id,
            scenario_name = target.scenario_name,
            imputer_backend = combo.imputer_backend,
            measures = CANONICAL_GROUP_HEATMAP_MEASURES,
            statistic = :median,
        )
        save_csv(
            joinpath(dir, "group_heatmap_table_" * stem * ".csv"),
            prepare_group_plot_table(
                heatmap_data.rows;
                value_column = :q50,
                complement_measures = [:O],
                measure_labels = CANONICAL_GROUP_HEATMAP_LABELS,
            ),
        )

        o_smoothed_data = pp.pipeline_group_heatmap_values(
            combo_results;
            wave_id = target.wave_id,
            scenario_name = target.scenario_name,
            imputer_backend = combo.imputer_backend,
            measures = O_SMOOTHED_MEASURES,
            statistic = :median,
        )
        save_csv(
            joinpath(dir, "o_smoothed_heatmap_table_" * stem * ".csv"),
            prepare_group_plot_table(
                o_smoothed_data.rows;
                value_column = :q50,
                complement_measures = [:O_smoothed],
                measure_labels = O_SMOOTHED_LABELS,
            ),
        )

        # The exported grouped plotting helpers filter on imputer backend only,
        # so we subset to one explicit linearizer branch here rather than
        # reimplement the grouped figures.
        fig_lines = pp.plot_pipeline_group_lines(
            combo_results;
            wave_id = target.wave_id,
            scenario_name = target.scenario_name,
            imputer_backend = combo.imputer_backend,
            measures = CANONICAL_GROUP_LINE_MEASURES,
            maxcols = 3,
            clist_size = 60,
        )
        pp.save_pipeline_plot(fig_lines, "group_lines_" * stem; dir = dir)

        fig_heatmap = pp.plot_pipeline_group_heatmap(
            combo_results;
            wave_id = target.wave_id,
            scenario_name = target.scenario_name,
            imputer_backend = combo.imputer_backend,
            statistic = :median,
            colormap = M.Reverse(:RdBu),
            fixed_colorrange = true,
            show_values = true,
            colorbar_label = "median grouped measure",
            simplified_labels = true,
            clist_size = 60,
        )
        pp.save_pipeline_plot(fig_heatmap, "group_heatmap_" * stem; dir = dir)

        fig_o_smoothed = pp.plot_pipeline_group_heatmap(
            combo_results;
            wave_id = target.wave_id,
            scenario_name = target.scenario_name,
            imputer_backend = combo.imputer_backend,
            measures = O_SMOOTHED_MEASURES,
            statistic = :median,
            complement_measures = [:O_smoothed],
            measure_labels = O_SMOOTHED_LABELS,
            maxcols = 1,
            colormap = M.Reverse(:RdBu),
            fixed_colorrange = true,
            show_values = true,
            colorbar_label = "median 1 - O_smoothed",
            simplified_labels = true,
            clist_size = 60,
        )
        pp.save_pipeline_plot(fig_o_smoothed, "o_smoothed_heatmap_" * stem; dir = dir)
    end

    return nothing
end

function main_group()
    manifest = load_small_run_manifest()
    results = load_saved_small_results(manifest)

    for target in scenario_targets(manifest)
        write_group_scenario_outputs!(results, manifest, target)
    end

    return nothing
end

if abspath(PROGRAM_FILE) == @__FILE__
    main_group()
end
