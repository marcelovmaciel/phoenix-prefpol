"""
    plot_effective_rankings_evolution.jl

Read the compact effective-ranking evolution tables and generate a 1x2 plot:
median EO by m on the left, median ENRP by m on the right, with one connected
dot series per year.

Run after `effective_rankings_evolution_tables.jl`:

    julia +1.11.9 --startup-file=no --project=PrefPol/running/plotting_env \
        PrefPol/running/plot_effective_rankings_evolution.jl
"""

include(joinpath(@__DIR__, "plotting_setup.jl"))
ensure_prefpol_plotting_environment!()

using CairoMakie
using CSV
using DataFrames

const SMALL_OUTPUT_ROOT = normpath(joinpath(@__DIR__, "output", "all_scenarios_small"))
const INPUT_DIR = joinpath(SMALL_OUTPUT_ROOT, "effective_rankings_evolution")
const OUTPUT_DIR = INPUT_DIR
const OUTPUT_PNG = joinpath(OUTPUT_DIR, "effective_rankings_evolution_1x2.png")
const OUTPUT_SVG = joinpath(OUTPUT_DIR, "effective_rankings_evolution_1x2.svg")

function load_evolution_tables(input_dir::AbstractString = INPUT_DIR)
    isdir(input_dir) || error(
        "Input directory not found: $(input_dir). Run " *
        "`PrefPol/running/effective_rankings_evolution_tables.jl` first.",
    )

    files = [
        joinpath(input_dir, file)
        for file in readdir(input_dir)
        if startswith(file, "effective_rankings_evolution_") &&
           endswith(file, ".csv")
    ]
    isempty(files) && error(
        "No effective-ranking evolution CSVs found in $(input_dir). Run " *
        "`PrefPol/running/effective_rankings_evolution_tables.jl` first.",
    )

    tables = DataFrame[]
    for file in sort(files)
        df = CSV.read(file, DataFrame)
        if !in(:ENRP_median, propertynames(df)) && :ER_median in propertynames(df)
            rename!(df, :ER_median => :ENRP_median)
        end
        required = [:year, :m, :EO_median, :ENRP_median]
        missing_cols = setdiff(required, propertynames(df))
        isempty(missing_cols) || error("$(file) is missing required columns $(missing_cols).")
        push!(tables, df)
    end

    return sort(vcat(tables...), [:year, :m])
end

function regular_ticks(values, step::Real)
    ymax = maximum(skipmissing(values))
    upper = step * ceil(ymax / step)
    return 0:step:upper
end

function plot_evolution(df::DataFrame)
    years = sort(unique(Int.(df.year)))
    colors = Dict(
        year => color for (year, color) in zip(
            years,
            (RGBf(0.13, 0.37, 0.66), RGBf(0.82, 0.33, 0.16), RGBf(0.20, 0.55, 0.28)),
        )
    )

    fig = Figure(size = (1050, 430), fontsize = 14)
    ax_eo = Axis(
        fig[1, 1],
        xlabel = "m",
        ylabel = "median EO",
        title = "Effective number of rankings",
        xticks = 2:5,
        yticks = regular_ticks(df.EO_median, 5),
    )
    ax_er = Axis(
        fig[1, 2],
        xlabel = "m",
        ylabel = "median ENRP",
        title = "Effective reversal pairs",
        xticks = 2:5,
        yticks = regular_ticks(df.ENRP_median, 2.5),
    )

    for year in years
        rows = df[df.year .== year, :]
        sort!(rows, :m)
        color = colors[year]
        label = string(year)

        lines!(ax_eo, rows.m, rows.EO_median; color, linewidth = 2.5, label)
        scatter!(ax_eo, rows.m, rows.EO_median; color, markersize = 9)

        lines!(ax_er, rows.m, rows.ENRP_median; color, linewidth = 2.5, label)
        scatter!(ax_er, rows.m, rows.ENRP_median; color, markersize = 9)
    end

    axislegend(ax_eo, "year"; position = :lt, framevisible = false)
    linkxaxes!(ax_eo, ax_er)
    return fig
end

function main()
    df = load_evolution_tables()
    fig = plot_evolution(df)
    mkpath(OUTPUT_DIR)
    save(OUTPUT_PNG, fig)
    save(OUTPUT_SVG, fig)
    println("Saved effective-ranking evolution plot: ", OUTPUT_PNG)
    println("Saved effective-ranking evolution plot: ", OUTPUT_SVG)
    return nothing
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
