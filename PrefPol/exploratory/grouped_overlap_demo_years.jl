include(joinpath(@__DIR__, "grouped_overlap_heatmap_2x2.jl"))

using Printf
using Statistics
using .GroupedOverlapHeatmap2x2
import PrefPol as pp

const DEFAULT_SCENARIOS = Dict(
    2006 => "lula_alckmin",
    2018 => "main_four",
    2022 => "lula_bolsonaro",
)

const SUMMARY_MEASURES = [:C, :D_median, :O, :Sep, :Gsep]

const SCRIPT_STEM = "grouped_overlap_demo_years"
const OUTPUT_ROOT = joinpath(pp.project_root, "exploratory", "output", SCRIPT_STEM)
const CACHE_ROOT = joinpath(pp.project_root, "exploratory", "_tmp", SCRIPT_STEM)

mkpath(OUTPUT_ROOT)
mkpath(CACHE_ROOT)

function _measure_values(matrix::AbstractMatrix)
    return collect(filter(!isnan, vec(Float64.(matrix))))
end

function _measure_summary_line(label::AbstractString, matrix::AbstractMatrix)
    values = _measure_values(matrix)
    isempty(values) && return @sprintf("%-5s median=NA range=[NA, NA]", label)
    return @sprintf(
        "%-5s median=%.3f range=[%.3f, %.3f]",
        label,
        median(values),
        minimum(values),
        maximum(values),
    )
end

function year_summary_lines(summary_data;
                            year::Int,
                            scenario_name::AbstractString,
                            imputer_backend::Symbol)
    rows = summary_data.rows
    return [
        string(
            "Year ",
            year,
            " • scenario = ",
            scenario_name,
            " • backend = ",
            imputer_backend,
            " • B = ",
            rows[1, :B],
            ", R = ",
            rows[1, :R],
            ", K = ",
            rows[1, :K],
        ),
        _measure_summary_line("C", summary_data.matrices[:C]),
        _measure_summary_line("D", summary_data.matrices[:D_median]),
        _measure_summary_line("O", summary_data.matrices[:O]),
        _measure_summary_line("Sep", summary_data.matrices[:Sep]),
        _measure_summary_line("Gsep", summary_data.matrices[:Gsep]),
    ]
end

function run_demo_year(year::Int;
                       scenario_name::AbstractString = DEFAULT_SCENARIOS[year],
                       imputer_backend::Symbol = :mice,
                       groupings = nothing,
                       force_pipeline::Bool = false,
                       B = nothing,
                       R::Int = parse(Int, get(ENV, "PREFPOL_GROUPED_OVERLAP_R", "2")),
                       K::Int = parse(Int, get(ENV, "PREFPOL_GROUPED_OVERLAP_K", "2")))
    year_cache_root = joinpath(CACHE_ROOT, string(year))
    run = GroupedOverlapHeatmap2x2.run_grouped_overlap_heatmap(
        year,
        scenario_name;
        imputer_backend = imputer_backend,
        groupings = groupings,
        measures = SUMMARY_MEASURES,
        force_pipeline = force_pipeline,
        B = B,
        R = R,
        K = K,
        output_root = OUTPUT_ROOT,
        cache_root = year_cache_root,
    )

    summary_data = pp.pipeline_group_heatmap_values(
        run.results;
        year = year,
        scenario_name = scenario_name,
        imputer_backend = imputer_backend,
        measures = SUMMARY_MEASURES,
        groupings = run.selected_groupings,
        statistic = :median,
    )

    lines = year_summary_lines(
        summary_data;
        year = year,
        scenario_name = scenario_name,
        imputer_backend = imputer_backend,
    )
    println(join(lines, "\n"))
    println("figure ", run.figure_path)

    return merge(
        run,
        (
            summary_data = summary_data,
            summary_lines = lines,
        ),
    )
end

function save_summary(path::AbstractString, lines::Vector{String})
    mkpath(dirname(path))
    open(path, "w") do io
        for line in lines
            println(io, line)
        end
    end
    println("saved ", path)
    return path
end

function main(; years = [2006, 2018, 2022],
              scenario_map = DEFAULT_SCENARIOS,
              imputer_backend::Symbol = :mice,
              groupings = nothing,
              force_pipeline::Bool = false,
              B = nothing,
              R::Int = parse(Int, get(ENV, "PREFPOL_GROUPED_OVERLAP_R", "2")),
              K::Int = parse(Int, get(ENV, "PREFPOL_GROUPED_OVERLAP_K", "2")))
    summary_lines = String[]
    runs = Dict{Int,Any}()

    for year in years
        haskey(scenario_map, year) || error("No default scenario configured for year $year.")
        run = run_demo_year(
            year;
            scenario_name = scenario_map[year],
            imputer_backend = imputer_backend,
            groupings = groupings,
            force_pipeline = force_pipeline,
            B = B,
            R = R,
            K = K,
        )
        runs[year] = run
        append!(summary_lines, run.summary_lines)
        push!(summary_lines, "figure " * run.figure_path)
        push!(summary_lines, "")
    end

    if !isempty(summary_lines) && isempty(last(summary_lines))
        pop!(summary_lines)
    end

    summary_path = joinpath(OUTPUT_ROOT, "summary.txt")
    save_summary(summary_path, summary_lines)

    return (runs = runs, summary_path = summary_path)
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end

main()
