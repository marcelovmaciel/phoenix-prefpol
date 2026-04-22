module PrefPol

using Pkg

using CategoricalArrays
using Combinatorics
using DataFrames
using Dates 
import Impute 
using Pkg
using Random
using SHA
using Statistics
using StatsBase
using CategoricalArrays
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

include("preprocessing_general.jl")
include("preprocessing_specific.jl")
include("profile_adapters.jl")
include("mallows_play.jl")
include("polarization_measures.jl")
include("pipeline.jl")
include("raw_profiles.jl")
include("variance_decomposition.jl")
include("nested_pipeline.jl")

#include("newplotting.jl")

export project_root, eseb_22, CANDIDATOS_eseb2022
export load_raw_pref_data,
       build_profile,
       S,
       S_old,
       profile_pattern_proportions,
       pretty_print_profile_patterns,
       ranked_count,
       has_ties,
       ranking_type_support,
       ranking_type_template,
       profile_ranksize_summary,
       profile_ranking_type_proportions,
       pretty_print_ranksize_summary,
       pretty_print_ranking_type_proportions

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





# dF_2022 = project_root * "/data/datafolha_vespera_2022_04780/04780/04780.SAV"
# dF_2018 = project_root * "/data/04619/04619.SAV"




# eseb_22 = project_root * "/data/04810/04810.sav"

# eseb_18  = project_root *"/data/eseb_2018/04622/04622.sav"
# eseb_06 = project_root * "/data/02489/1_02489.sav"


# CANDIDATOS_eseb2022 = [
#         "CIRO_GOMES", "BOLSONARO", "ALVARO_DIAS", "ARTHUR_LIRA", "LULA",
#         "GERALDO_ALCKMIN", "GILBERTO_KASSAB", "EDUARDO_LEITE", "BOULOS",
#         "MARINA_SILVA", "TARCISIO_DE_FREITAS", "LUCIANO_BIVAR", "SIMONE_TEBET"
#     ]
