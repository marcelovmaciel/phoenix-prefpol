__precompile__(false)

"""
    PrefPol

Applied Brazil/ESEB replication package built on the reusable `Preferences`
formal package.

`PrefPol` owns survey configuration loading, candidate-set selection,
bootstrap/imputation/linearization orchestration, cache layout, applied measure
execution, and paper-facing tables/figures. Formal profile representations,
rankings, consensus, overlap, reversal, polarization, and related mathematical
definitions live in `Preferences`; PrefPol adapters construct those
formal objects from survey data for applied ESEB workflows.
"""
module PrefPol

using CategoricalArrays
using Combinatorics
using DataFrames
using Dates 
import Impute 
using Random
using SHA
using Statistics
using StatsBase
import  ProgressMeter as pm 
using Printf
using TextWrap
import OrderedCollections
using OrderedCollections: OrderedDict
using Logging
using PooledArrays, StaticArrays 

using JLD2, TOML

const project_root = normpath(joinpath(@__DIR__, ".."))
const _LOCAL_PREFERENCES_SRC = normpath(
    joinpath(project_root, "..", "Preferences", "src", "Preferences.jl"),
)

# Load local Preferences at module load time to avoid world-age issues from
# runtime includes under Revise/Julia 1.12.
if !isdefined(@__MODULE__, :Preferences)
    if isfile(_LOCAL_PREFERENCES_SRC)
        include(_LOCAL_PREFERENCES_SRC)
    else
        @warn "Local Preferences module not found at $(_LOCAL_PREFERENCES_SRC). " *
              "Raw profile helpers will error until it is available."
    end
end

function _load_optional_dependency!(sym::Symbol;
                                    feature::AbstractString,
                                    using_names::Bool = false)
    isdefined(@__MODULE__, sym) && return getfield(@__MODULE__, sym)

    stmt = using_names ? "using $(sym)" : "import $(sym)"
    expr = Meta.parse(stmt)
    try
        Base.eval(@__MODULE__, expr)
    catch err
        throw(ArgumentError(
            "$feature requires optional dependency `$sym`, but it could not be loaded: " *
            sprint(showerror, err),
        ))
    end

    return getfield(@__MODULE__, sym)
end

@inline _require_rcall!() = _load_optional_dependency!(
    :RCall;
    feature = "SPSS loading and R/mice imputation",
    using_names = false,
)

@inline _rcall_eval(rcall, expr) =
    Base.invokelatest(getproperty(rcall, :reval), expr)

@inline _rcall_copy(rcall, T, value) =
    Base.invokelatest(getproperty(rcall, :rcopy), T, value)

@inline _rcall_setglobal!(rcall, name::Symbol, value) =
    Base.invokelatest(setindex!, getproperty(rcall, :globalEnv), value, name)

function _plotting_extension_module()
    ext = Base.get_extension(@__MODULE__, :PrefPolPlottingExt)
    ext === nothing || return ext

    throw(ArgumentError(
        "Plotting requires the CairoMakie extension. Add CairoMakie to the active environment " *
        "and load it with `using CairoMakie` before calling PrefPol plotting helpers.",
    ))
end

@inline _call_plotting_extension(name::Symbol, args...; kwargs...) =
    getfield(_plotting_extension_module(), name)(args...; kwargs...)

include("eseb_semantics.jl")
include("preprocessing_general.jl")
include("legacy_preprocessing.jl")
include("preprocessing_specific.jl")
include("profile_adapters.jl")
include("survey_config.jl")
include("variance_decomposition.jl")
include("nested_pipeline.jl")
include("variance_decomposition_report.jl")


export project_root
export ESEB_SCORE_MISSING_CODES,
       ESEB_VALID_SCORE_MIN,
       ESEB_VALID_SCORE_MAX,
       is_eseb_missing_score,
       normalize_eseb_score,
       normalize_eseb_score_column!,
       normalize_eseb_score_columns!

export LULA_SCORE_GROUP_LEVELS,
       lula_score_group_value,
       lula_score_group_column,
       add_lula_score_group!

export load_raw_pref_data,
       build_profile,
       available_election_years,
       default_config_path

export SurveyWaveConfig,
       load_survey_wave_config,
       build_source_registry,
       resolve_active_candidate_set,
       tree_variance_decomposition_table,
       PipelineSpec,
       build_pipeline_spec,
       NestedStochasticPipeline,
       ObservedData,
       Resample,
       ImputedData,
       LinearizedProfile,
       MeasureResult,
       VarianceComponentSummary,
       VarianceDecomposition,
       PipelineResult,
       StudyBatchItem,
       StudyBatchSpec,
       BatchRunner,
       BatchRunResult,
       load_observed_data,
       compute_group_measure_details,
       pipeline_measure_table,
       pipeline_summary_table,
       pipeline_variance_decomposition_table,
       pipeline_cache_dir,
       pipeline_stage_paths,
       ensure_observed!,
       ensure_resamples!,
       ensure_imputations!,
       ensure_linearizations!,
       ensure_measures!,
       load_stage_artifact,
       rebuild_pipeline_result_from_stage_cache,
       VarianceDecompositionReportSpec,
       DEFAULT_PAPER_VARIANCE_MEASURES,
       DEFAULT_PAPER_VARIANCE_MEASURE_LABELS,
       normalize_variance_measure,
       variance_decomposition_report,
       variance_decomposition_fine_table,
       variance_decomposition_pooled_table,
       variance_decomposition_by_m_plot_table,
       variance_decomposition_year_scenario_boxplot_table,
       plot_variance_decomposition_by_m,
       plot_variance_decomposition_year_scenario_boxplots,
       plot_variance_decomposition_dotwhisker,
       plot_variance_decomposition_boxplot,
       pipeline_panel_table,
       select_pipeline_panel_rows,
       pipeline_candidate_label,
       pipeline_scenario_plot_data,
       pipeline_group_plot_data,
       pipeline_group_heatmap_values,
       decomposition_table,
       save_pipeline_variance_decomposition_csv,
       run_pipeline,
       load_pipeline_result,
       save_pipeline_result,
       run_batch,
       plot_pipeline_scenario,
       plot_pipeline_group_lines,
       plot_pipeline_group_heatmap,
       save_pipeline_plot

end # module PrefPol
