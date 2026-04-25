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
#     (year = 2022, scenario_name = "lula_bolsonaro"),
#     (year = 2018, scenario_name = "main_four"),
# ]
M_VALUES = 2:5
MEASURES = :paper
GROUPINGS = nothing
POOL_OVER_M = true
POOL_OVER_SELECTIONS = false
INCLUDE_EMPIRICAL = false

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

    fine_path = joinpath(OUTPUT_DIR, "variance_decomposition_fine.csv")
    pooled_path = joinpath(OUTPUT_DIR, "variance_decomposition_pooled.csv")
    CSV.write(fine_path, fine)
    CSV.write(pooled_path, pooled)
    @info "wrote variance decomposition report tables" fine_path pooled_path

    try
        @eval using CairoMakie
        plot_path = joinpath(OUTPUT_DIR, "variance_decomposition_dotwhisker.png")
        boxplot_path = joinpath(OUTPUT_DIR, "variance_decomposition_boxplot.png")
        Base.invokelatest(
            PrefPol.plot_variance_decomposition_dotwhisker,
            pooled;
            outfile = plot_path,
        )
        Base.invokelatest(
            PrefPol.plot_variance_decomposition_boxplot,
            pooled;
            outfile = boxplot_path,
        )
        @info "wrote variance decomposition plots" plot_path boxplot_path
    catch err
        @info(
            "Skipping variance decomposition plot. If CairoMakie is unavailable, instantiate and run with " *
            "Run with `julia +1.11.9 --project=PrefPol/running/plotting_env " *
            "PrefPol/exploratory/variance_decomposition_report_demo.jl` after instantiating that environment.",
            err,
        )
    end

    return (; fine, pooled)
end

main()
