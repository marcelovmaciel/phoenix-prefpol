using Revise
using PrefPol
import PrefPol as pp

# Rerun pipeline for 2018. If you meant a different year, change it here.
year = 2018
scenario = "main_four"

# Toggle these if you want to force recomputation.
overwrite_profiles = false
overwrite_measures = false
overwrite_group_metrics = false 

# ------------------------------------------------------------------
# Bootstraps (load) + imputation indices
# --h----------------------------------------------------------------
#bootstrap_index1 = pp.save_all_bootstraps(years = [2018], cfgdir = "../config")   # year ⇒ (data, cfg, path)

bootstrap_index = pp.load_all_bootstraps(years = [year])


#pp.impute_from_f3(bootstrap_index; overwrite = false)


imputed_year = pp.load_imputed_year(year)

# ------------------------------------------------------------------
# Generate streamed weak profiles for the year
# ------------------------------------------------------------------
weak_profiles_year = pp.generate_profiles_for_year_streamed_from_index(
    year, bootstrap_index[year], imputed_year; overwrite = overwrite_profiles)

pp.linearize_profiles_for_year_streamed_from_index(
    year, bootstrap_index[year], weak_profiles_year; overwrite = overwrite_profiles)
linearized_profiles_year = pp.load_linearized_profiles_index(year)

# ------------------------------------------------------------------
# Global measures (per year, from loaded linearized profiles)
# ------------------------------------------------------------------
measures_year = pp.save_or_load_measures_for_year(
    year, linearized_profiles_year; overwrite = overwrite_measures, verbose = true)

# ------------------------------------------------------------------
# Group metrics (per year)
# ------------------------------------------------------------------
group_metrics_year = pp.save_or_load_group_metrics_for_year(
    year, linearized_profiles_year, bootstrap_index[year];
    overwrite = overwrite_group_metrics, verbose = true, two_pass = true)

# TODO: PAREI AQUI 

# ------------------------------------------------------------------
# Group-demographics plots — lines and heatmap (as in running/)
# ------------------------------------------------------------------
cfg = bootstrap_index[year].cfg

# fig_dem_main = pp.plot_group_demographics_lines(
#     Dict(year => group_metrics_year), bootstrap_index, year, scenario;
#     variants = [:mice], maxcols = 2, clist_size = 60,
#     demographics = ["Income", "Ideology"])

# fig_dem_other = pp.plot_group_demographics_lines(
#     Dict(year => group_metrics_year), bootstrap_index, year, scenario;
#     variants = [:mice], maxcols = 3, clist_size = 60,
#     demographics = setdiff(bootstrap_index[year].cfg.demographics, ["Income", "Ideology"]))

fig_dem_hm = pp.plot_group_demographics_heatmap(
    Dict(year => group_metrics_year), bootstrap_index, year, scenario;
    variants = [:mice],
    measures = [:C, :D, :G],
    maxcols = 3,
    clist_size = 60,
    colormaps = :RdBu |> pp.Makie.Reverse,
    fixed_colorrange = true, show_values = true, simplified_labels = true)

# ------------------------------------------------------------------
# Save figures
# ------------------------------------------------------------------
# pp.save_plot(fig_dem_main,  year, "$(scenario)_group_main",  cfg; variant = "mice")
# pp.save_plot(fig_dem_other, year, "$(scenario)_group_therest", cfg; variant = "mice")
# pp.save_plot(fig_dem_hm,    year, "$(scenario)_group_hm",    cfg; variant = "mice")


plot_measures_2018 = Dict(year => measures_2018)


fig_2018_main_four_dot     = pp.plot_scenario_year(year, "main_four",      bootstrap_index, plot_measures_2018; variant = "mice", plot_kind=:dotwhisker, connect_lines = true)
fig_2018_no_forcing_dot    = pp.plot_scenario_year(year, "no_forcing",     bootstrap_index, plot_measures_2018; variant = "mice", plot_kind=:dotwhisker, connect_lines = true)
fig_2018_lula_bolsonaro_dot = pp.plot_scenario_year(year, "lula_bolsonaro", bootstrap_index, plot_measures_2018; variant = "mice", plot_kind=:dotwhisker, connect_lines = true)



cfg_2018 = bootstrap_index[year].cfg


pp.save_plot(fig_2018_main_four_dot,      year, "main_four_dot",     cfg_2018; variant = "mice")
pp.save_plot(fig_2018_no_forcing_dot,     year, "no_forcing_dot",    cfg_2018; variant = "mice")
pp.save_plot(fig_2018_lula_bolsonaro_dot, year, "lula_bolsonaro_dot", cfg_2018; variant = "mice")
