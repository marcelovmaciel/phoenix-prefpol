#= function plot_pipeline_scenario(
    scenario_measures::Dict{Int,<:AbstractDict},
    candidates::Vector{String};
    variants    ::Vector{String} = ["zero","random","mice"],
    palette     ::Vector         = Makie.wong_colors(),
    boxwidth    ::Real           = 0.18,
    figsize     ::Tuple          = (1000, 900)
)::Figure

    years = sort(collect(keys(scenario_measures)))
    n     = length(years)

    # prepare the candidate‐list subtitle once
    cand_lbl = describe_candidate_set(candidates)

    # collect each year‐figure
    figs = Vector{Figure}(undef, n)

    for (i, yr) in enumerate(years)
        measures_over_m = scenario_measures[yr]

        # call your existing Makie helper
        fig = boxplot_alt_by_variant(
                  measures_over_m;
                  variants = variants,
                  palette  = palette,
                  boxwidth = boxwidth,
                  figsize  = figsize
              )

        # now overwrite the two‐line header at fig[0,1:2]
        #   line 1: Year = …
        #   line 2: Candidates: …
        first_m, last_m = first(sort(collect(keys(measures_over_m)))), last(sort(collect(keys(measures_over_m))))
        n_bootstrap     = length(first(values(first(values(measures_over_m))))[Symbol(first(variants))])

        header = GridLayout(tellwidth = true)
        fig[0, 1] = header
        Label(header[1, 1];
              text     = "Year = $(yr)   •   Number of alternatives m = $(first_m)…$(last_m)   •   B = $(n_bootstrap)",
              fontsize = 18,
              halign   = :left)
        Label(header[2, 1];
              text     = cand_lbl,
              fontsize = 14,
              halign   = :left)

        figs[i] = fig
    end

    # tile them horizontally
    return hcat(figs...; tellwidth = true)
end



function plot_scenario_over_years(
    all_measures,
    f3          ,
    scenario_name ::String,
    variant       ::String;
    palette       = Makie.wong_colors(),
    figsize       ::Tuple{<:Integer,<:Integer} = (400, 350)
)

    years = sort([yr for yr in keys(all_measures) if haskey(all_measures[yr], scenario_name)])
   
    figs = Vector{Makie.Figure}(undef, length(years))
    for (i, yr) in enumerate(years)
        # 1) get the measures map m → measure → variant → [values]
        measures_over_m = all_measures[yr][scenario_name]

        # 2) find the candidate list for that scenario in this year
        scen_objs = filter(s->s.name == scenario_name, f3[yr].cfg.scenarios)
        @assert length(scen_objs)==1 "Year $yr has $(length(scen_objs)) scenarios named $scenario_name"
        cands = scen_objs[1].candidates

        # 3) pretty‐print it
        candidate_label = describe_candidate_set(cands)

        # 4) draw one small figure for this year & variant
        figs[i] = lines_alt_by_variant(
            measures_over_m;
            variants        = [variant],
            palette         = palette,
            figsize         = figsize,
            candidate_label = candidate_label,
            year            = yr
        )
    end

    # 5) tile them horizontally
    return hcat(figs...)
end

"""
    plot_scenario_year(
      year::Int,
      scenario::String,
      f3       ::Dict{Int,NamedTuple},
      all_meas ::Dict{Int,Dict{String,Any}};
      variant      = "mice",
      palette      = Makie.wong_colors(),
      figsize      = (500,400)
    ) -> Figure

Draw the global‐measures plot for one year × scenario × variant, exactly in
the style of your example.

- `f3[year].data` is the raw bootstraps.  
- `all_meas[year][scenario]` is the `m ⇒ (measure ⇒ (variant ⇒ Vector{Float64}))` map.  

The function will:
1.  Load the precomputed year-level candidate list for the scenario,  
2.  Build the two-line header (Year…, Candidates…),  
3.  Call `lines_alt_by_variant` for that one plot, and  
4.  Return the single-panel Figure.
"""
function plot_scenario_year(
    year::Int,
    scenario::String,
    f3       ,
    all_meas ;
    variant      ::String  = "mice",
    palette      ::Vector  = Makie.wong_colors(),
    figsize      ::Tuple   = (500,400)
)

    # —───────────────────────────────────────────────────────────────────────────
    # 1) look up inputs & do basic checks
    haskey(f3, year)              || error("No bootstrap found for year $year")
    haskey(all_meas, year)        || error("No measures found for year $year")
    year_meas = all_meas[year]

    haskey(year_meas, scenario)   || error("Scenario “$scenario” not found in year $year")
    meas_map  = year_meas[scenario]     # Dict{Int,Dict{Symbol,Dict{String,Vector{Float64}}}}

    cfg       = f3[year].cfg
    scen_obj  = _lookup_scenario(cfg, scenario)

    # —───────────────────────────────────────────────────────────────────────────
    # 2) load the precomputed candidate set
    full_list = _full_candidate_list(cfg, scen_obj)
    candidate_label = describe_candidate_set(full_list)

    # —───────────────────────────────────────────────────────────────────────────
    # 3) delegate to your existing Makie helper
    fig = lines_alt_by_variant(
        meas_map;
        variants        = [variant],
        palette         = palette,
        figsize         = figsize,
        year            = year,
        candidate_label = candidate_label,
    )

    return fig
end =#
