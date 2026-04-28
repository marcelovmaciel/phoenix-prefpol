const _PREFPOL_PROJECT_DIR = normpath(joinpath(@__DIR__, ".."))
if _PREFPOL_PROJECT_DIR ∉ LOAD_PATH
    pushfirst!(LOAD_PATH, _PREFPOL_PROJECT_DIR)
end

using CSV
using DataFrames
import PrefPol

function _ensure_variance_report_layer!()
    isdefined(PrefPol, :VarianceDecompositionReportSpec) && return nothing

    report_src = joinpath(PrefPol.project_root, "src", "variance_decomposition_report.jl")
    isfile(report_src) || throw(ArgumentError(
        "Variance-decomposition reporting layer is not available at `$report_src`. " *
        "Make sure your working tree includes `PrefPol/src/variance_decomposition_report.jl`.",
    ))
    Base.include(PrefPol, report_src)
    return nothing
end

# Edit these filters before running the demo.
ROOT_DECOMPOSITION_CSV = joinpath(PrefPol.project_root, "decomposition_table.csv")
RUNNING_DECOMPOSITION_CSV = joinpath(
    PrefPol.project_root,
    "running",
    "output",
    "all_scenarios_small",
    "decomposition_table.csv",
)
INPUT_CSV = isfile(ROOT_DECOMPOSITION_CSV) ? ROOT_DECOMPOSITION_CSV : RUNNING_DECOMPOSITION_CSV
OUTPUT_DIR = joinpath(@__DIR__, "output", "variance_decomposition_report_demo")
SELECTED_SELECTIONS = nothing
# Example:
# SELECTED_SELECTIONS = [
#     (year = 2006, scenario_name = "main_2006"),
#     (year = 2018, scenario_name = "main_2018"),
#     (year = 2022, scenario_name = "main_2022"),
# ]
M_VALUES = 2:5
MEASURES = :paper
GROUPINGS = nothing
# Keep `m` explicit in the primary report. Pooling over `m` is written only as
# a secondary diagnostic because the decomposition is cellwise conditional on m.
POOL_OVER_M = false
POOL_OVER_SELECTIONS = false
INCLUDE_EMPIRICAL = false

function _slug(value)
    raw = lowercase(string(value))
    return replace(raw, r"[^a-z0-9]+" => "_", r"(^_+|_+$)" => "")
end

function _selected_or_available_selections(input::AbstractDataFrame)
    SELECTED_SELECTIONS !== nothing && return collect(SELECTED_SELECTIONS)
    required = [:year, :scenario_name]
    all(col -> col in Symbol.(names(input)), required) || throw(ArgumentError(
        "SELECTED_SELECTIONS is nothing, but the input lacks `year` and `scenario_name` columns.",
    ))
    return [
        (year = row.year, scenario_name = row.scenario_name)
        for row in eachrow(unique(input[:, required]))
    ]
end

function _available_selections(table::AbstractDataFrame)
    required = [:year, :scenario_name]
    all(col -> col in Symbol.(names(table)), required) || return NamedTuple[]
    isempty(table) && return NamedTuple[]
    return [
        (year = row.year, scenario_name = row.scenario_name)
        for row in eachrow(sort(unique(table[:, required]), required))
    ]
end

function main()
    _ensure_variance_report_layer!()
    isfile(INPUT_CSV) || throw(ArgumentError(
        "Input decomposition CSV does not exist: `$INPUT_CSV`. " *
        "Edit `INPUT_CSV` at the top of this script or run the nested pipeline first.",
    ))

    mkpath(OUTPUT_DIR)

    input = CSV.read(INPUT_CSV, DataFrame)
    spec = PrefPol.VarianceDecompositionReportSpec(
        selections = SELECTED_SELECTIONS,
        m_values = M_VALUES,
        measures = MEASURES,
        groupings = GROUPINGS,
        pool_over_m = POOL_OVER_M,
        pool_over_selections = POOL_OVER_SELECTIONS,
        include_empirical = INCLUDE_EMPIRICAL,
    )

    fine, pooled = PrefPol.variance_decomposition_report(input, spec)
    pooled_diagnostic = PrefPol.variance_decomposition_pooled_table(
        fine,
        PrefPol.VarianceDecompositionReportSpec(
            selections = SELECTED_SELECTIONS,
            m_values = M_VALUES,
            measures = MEASURES,
            groupings = GROUPINGS,
            pool_over_m = true,
            pool_over_selections = POOL_OVER_SELECTIONS,
            include_empirical = INCLUDE_EMPIRICAL,
        ),
    )

    fine_path = joinpath(OUTPUT_DIR, "variance_decomposition_fine.csv")
    pooled_path = joinpath(OUTPUT_DIR, "variance_decomposition_pooled.csv")
    pooled_diagnostic_path = joinpath(OUTPUT_DIR, "variance_decomposition_pooled_over_m_diagnostic.csv")
    CSV.write(fine_path, fine)
    CSV.write(pooled_path, pooled)
    CSV.write(pooled_diagnostic_path, pooled_diagnostic)
    @info "wrote variance decomposition report tables" fine_path pooled_path pooled_diagnostic_path

    try
        @eval using CairoMakie
        boxplot_path = joinpath(OUTPUT_DIR, "variance_decomposition_pooled_boxplot_diagnostic.png")
        primary_plot_paths = String[]
        selected = SELECTED_SELECTIONS === nothing ?
                   _available_selections(fine) :
                   collect(SELECTED_SELECTIONS)
        available_set = Set(_available_selections(fine))
        isempty(selected) && throw(ArgumentError(
            "No selected year/scenario cells have report rows. Available input selections are: " *
            string(_available_selections(input)),
        ))

        for selection in selected
            if !(selection in available_set)
                @info "skipping unavailable variance decomposition selection" selection available = collect(available_set)
                continue
            end

            year_slug = _slug(selection.year)
            scenario_slug = _slug(selection.scenario_name)
            share_plot_path = joinpath(
                OUTPUT_DIR,
                "variance_decomposition_$(year_slug)_$(scenario_slug)_share_boxplots_compact.png",
            )
            variance_plot_path = joinpath(
                OUTPUT_DIR,
                "variance_decomposition_$(year_slug)_$(scenario_slug)_variance_boxplots_compact.png",
            )

            Base.invokelatest(
                PrefPol.plot_variance_decomposition_year_scenario_boxplots,
                fine;
                year = selection.year,
                scenario_name = selection.scenario_name,
                measures = MEASURES,
                groupings = GROUPINGS,
                group_pooling = :within_grouping,
                value_kind = :share,
                outfile = share_plot_path,
            )
            Base.invokelatest(
                PrefPol.plot_variance_decomposition_year_scenario_boxplots,
                fine;
                year = selection.year,
                scenario_name = selection.scenario_name,
                measures = MEASURES,
                groupings = GROUPINGS,
                group_pooling = :within_grouping,
                value_kind = :variance,
                outfile = variance_plot_path,
            )
            append!(primary_plot_paths, [share_plot_path, variance_plot_path])
        end
        Base.invokelatest(
            PrefPol.plot_variance_decomposition_boxplot,
            pooled_diagnostic;
            outfile = boxplot_path,
        )
        isempty(primary_plot_paths) && throw(ArgumentError(
            "No primary plots were written. Available report selections are: " *
            string(_available_selections(fine)),
        ))
        @info "wrote variance decomposition plots" primary_plot_paths boxplot_path
    catch err
        @error "Failed to write variance decomposition plots" err
        rethrow()
    end

    return (; fine, pooled)
end

main()
