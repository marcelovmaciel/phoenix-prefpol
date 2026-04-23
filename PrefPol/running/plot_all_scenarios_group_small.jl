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

    julia +1.11.9 --startup-file=no --project=PrefPol/running/plotting_env -e 'using Pkg; Pkg.instantiate()'
    julia +1.11.9 --startup-file=no --project=PrefPol/running/plotting_env PrefPol/running/plot_all_scenarios_group_small.jl
"""

include(joinpath(@__DIR__, "plot_all_scenarios_global_small.jl"))

const GROUP_OUTPUT_ROOT = joinpath(SMALL_OUTPUT_ROOT, "group")
const M = CairoMakie.Makie

# These match the current grouped plotting workflow:
# line plots use the exported defaults, while the old compact grouped summary
# heatmap slot is replaced here by the dedicated `C / 1 - O / S` triplet panel.
const CANONICAL_GROUP_LINE_MEASURES = [:C, :D, :G]
const GROUP_TRIPLET_PANEL_MEASURES = [:C, :O, :S]
const GROUP_TRIPLET_PANEL_LABELS = Dict(
    :C => "C",
    :O => "1 - O",
    :S => "S",
)
const GROUP_TRIPLET_PANEL_BASENAME = "group_triplet_panel_C_1mO_S"
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

function _complement_group_plot_columns!(out::DataFrame, complemented::Set{Symbol})
    isempty(complemented) && return Set{Symbol}()

    transformed = Set{Symbol}()
    measure_mask = [Symbol(row.measure) in complemented for row in eachrow(out)]
    any(measure_mask) || return transformed

    direct_cols = (:estimate, :mean_value)
    for col in direct_cols
        col in propertynames(out) || continue
        vals = Float64.(out[!, col])
        vals[measure_mask] .= 1.0 .- vals[measure_mask]
        out[!, col] = vals
        push!(transformed, col)
    end

    quantile_cols = (:q05, :q25, :q50, :q75, :q95)
    if all(col -> col in propertynames(out), quantile_cols)
        q05 = Float64.(out.q05)
        q25 = Float64.(out.q25)
        q50 = Float64.(out.q50)
        q75 = Float64.(out.q75)
        q95 = Float64.(out.q95)

        q05[measure_mask] .= 1.0 .- q95[measure_mask]
        q25[measure_mask] .= 1.0 .- q75[measure_mask]
        q50[measure_mask] .= 1.0 .- q50[measure_mask]
        q75[measure_mask] .= 1.0 .- Float64.(out.q25)[measure_mask]
        q95[measure_mask] .= 1.0 .- Float64.(out.q05)[measure_mask]

        out[!, :q05] = q05
        out[!, :q25] = q25
        out[!, :q50] = q50
        out[!, :q75] = q75
        out[!, :q95] = q95
        union!(transformed, quantile_cols)
    end

    if :min_value in propertynames(out) && :max_value in propertynames(out)
        min_vals = Float64.(out.min_value)
        max_vals = Float64.(out.max_value)
        min_vals[measure_mask] .= 1.0 .- Float64.(out.max_value)[measure_mask]
        max_vals[measure_mask] .= 1.0 .- Float64.(out.min_value)[measure_mask]
        out[!, :min_value] = min_vals
        out[!, :max_value] = max_vals
        push!(transformed, :min_value)
        push!(transformed, :max_value)
    end

    if :value_lo_min in propertynames(out) && :value_hi_max in propertynames(out)
        lo_vals = Float64.(out.value_lo_min)
        hi_vals = Float64.(out.value_hi_max)
        lo_vals[measure_mask] .= 1.0 .- Float64.(out.value_hi_max)[measure_mask]
        hi_vals[measure_mask] .= 1.0 .- Float64.(out.value_lo_min)[measure_mask]
        out[!, :value_lo_min] = lo_vals
        out[!, :value_hi_max] = hi_vals
        push!(transformed, :value_lo_min)
        push!(transformed, :value_hi_max)
    end

    return transformed
end

function prepare_group_plot_table(rows::AbstractDataFrame;
                                  value_column::Symbol = :estimate,
                                  complement_measures = Symbol[],
                                  measure_labels = nothing)
    out = DataFrame(rows)
    complemented = Set(Symbol.(collect(complement_measures)))
    transformed_cols = _complement_group_plot_columns!(out, complemented)

    if !(:m in propertynames(out)) && (:n_candidates in propertynames(out))
        out[!, :m] = Int.(out.n_candidates)
    end

    if value_column in propertynames(out)
        values = Float64.(out[!, value_column])
        if value_column in transformed_cols
            out[!, :value] = values
        else
            out[!, :value] = [
                Symbol(row.measure) in complemented ? 1.0 - values[idx] : values[idx]
                for (idx, row) in enumerate(eachrow(out))
            ]
        end
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
            measures = GROUP_TRIPLET_PANEL_MEASURES,
            statistic = :median,
        )
        save_csv(
            joinpath(dir, GROUP_TRIPLET_PANEL_BASENAME * "_table_" * stem * ".csv"),
            prepare_group_plot_table(
                heatmap_data.rows;
                value_column = :q50,
                complement_measures = [:O],
                measure_labels = GROUP_TRIPLET_PANEL_LABELS,
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

        # Replace the old grouped summary heatmap output for this slot instead
        # of emitting an additional grouped panel.
        fig_heatmap = _PLOT_EXT.plot_pipeline_group_triplet_panel(
            combo_results;
            wave_id = target.wave_id,
            scenario_name = target.scenario_name,
            imputer_backend = combo.imputer_backend,
            statistic = :median,
            colormap = M.Reverse(:RdBu),
            show_values = true,
            colorbar_label = "median grouped value",
            simplified_labels = true,
            clist_size = 60,
        )
        pp.save_pipeline_plot(fig_heatmap, GROUP_TRIPLET_PANEL_BASENAME * "_" * stem; dir = dir)

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
