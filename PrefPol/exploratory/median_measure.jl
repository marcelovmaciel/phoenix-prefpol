using PrefPol
using JLD2
using Statistics
using DataFrames

const PROJECT_ROOT = PrefPol.project_root
const GLOBAL_MEASURE_DIR = joinpath(PROJECT_ROOT, "intermediate_data", "global_measures")

normalize_scenario_name(s::AbstractString) = replace(lowercase(String(s)), "-" => "_")

function load_measures_year(year::Integer; dir::AbstractString = GLOBAL_MEASURE_DIR)
    path = joinpath(dir, "measures_$(year).jld2")
    isfile(path) || error("No global measures file for $year at $path")
    measures = nothing
    @load path measures
    return measures
end

function median_enrp(year::Integer;
                     scenario::AbstractString,
                     variant::Union{Symbol,AbstractString} = :mice,
                     m::Integer)

    scen = normalize_scenario_name(scenario)
    var  = Symbol(variant)
    m >= 2 || error("m must be >= 2, got m=$m")

    measures = load_measures_year(year)
    haskey(measures, scen) || error("Scenario '$scen' not found for year $year")
    haskey(measures[scen], m) || error("m=$m not found for $year/$scen")

    # With exactly two alternatives there is a single reversal pair,
    # so the effective number is always 1.
    m == 2 && return 1.0

    haskey(measures[scen][m], :calc_reversal_HHI) || error("HHI measure not found for $year/$scen/m=$m")
    haskey(measures[scen][m][:calc_reversal_HHI], var) || error("Variant '$var' not found for $year/$scen/m=$m")

    median_hhi = median(measures[scen][m][:calc_reversal_HHI][var])
    return 1 / median_hhi
end

function enrp_table(rows)
    df = DataFrame(year = Int[], scenario = String[], m = Int[], variant = String[], enrp = Float64[])
    for r in rows
        year = r.year
        scenario = r.scenario
        m = r.m
        variant = get(r, :variant, :mice)
        enrp = median_enrp(year; scenario = scenario, m = m, variant = variant)
        push!(df, (
            year = year,
            scenario = normalize_scenario_name(scenario),
            m = m,
            variant = string(variant),
            enrp = enrp
        ))
    end
    return df
end

rows = [
    (year = 2006, scenario = "lula-alckmin",   m = 4, variant = :mice),
    (year = 2018, scenario = "main-four",      m = 4, variant = :mice),
    (year = 2022, scenario = "lula-bolsonaro", m = 4, variant = :mice),
]

table = enrp_table(rows)

table
