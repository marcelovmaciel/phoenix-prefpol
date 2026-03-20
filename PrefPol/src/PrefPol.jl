module PrefPol

using Pkg

using CairoMakie
using CategoricalArrays
using Combinatorics
using DataFrames
using DataVoyager
using Dates 
import Impute 
using LaTeXStrings
using Pkg
using Random
using RCall
using Statistics
using StatsBase
using CategoricalArrays
using PrettyTables
import  ProgressMeter as pm 
using Printf
using KernelDensity
import Colors
using TextWrap
import OrderedCollections
using OrderedCollections: OrderedDict
using Logging
using PooledArrays, StaticArrays 

using JLD2, Arrow, TOML




project_root = dirname(Pkg.project().path)
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

include("preprocessing_general.jl")
include("preprocessing_specific.jl")
include("polarization_measures.jl")
include("mallows_play.jl")
include("pipeline.jl")
include("raw_profiles.jl")

#include("newplotting.jl")

export project_root, eseb_22, CANDIDATOS_eseb2022
export load_raw_pref_data,
       build_profile,
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
