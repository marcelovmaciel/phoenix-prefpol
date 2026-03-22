using Revise
using PrefPol
import PrefPol as pp

# ------------------------------------------------------------------
# Bootstraps: save (idempotent) and load
# ------------------------------------------------------------------


# saved_bootstrap_paths = pp.save_all_bootstraps()

#  saved_bootstrap_paths = nothing

bootstrap_index = pp.load_all_bootstraps()   # year ⇒ (data, cfg, path)

# ------------------------------------------------------------------
# Imputation indices (idempotent unless overwrite=true)
# ------------------------------------------------------------------
imputed_index_paths = pp.impute_from_f3(bootstrap_index; overwrite = false)


cfg_2006 = bootstrap_index[2006].cfg
cfg_2022 = bootstrap_index[2022].cfg
cfg_2018 = bootstrap_index[2018].cfg

imputed_index_paths = nothing

# ------------------------------------------------------------------
# Load per‑year imputation indices
# ------------------------------------------------------------------

imputed_year_2006 = pp.load_imputed_year(2006)


imputed_year_2018 = pp.load_imputed_year(2018)

imputed_year_2022 = pp.load_imputed_year(2022)

# ------------------------------------------------------------------
# Generate streamed weak profiles per year
# ------------------------------------------------------------------
weak_profiles_2006 = pp.generate_profiles_for_year_streamed_from_index(
                    2006, bootstrap_index[2006], imputed_year_2006; overwrite = false)

weak_profiles_2018 = pp.generate_profiles_for_year_streamed_from_index(
                    2018, bootstrap_index[2018], imputed_year_2018; overwrite = false)





weak_profiles_2022 = pp.generate_profiles_for_year_streamed_from_index(
                    2022, bootstrap_index[2022], imputed_year_2022; overwrite = false)

# ------------------------------------------------------------------
# Linearize saved profiles and reload the linearized index
# ------------------------------------------------------------------
pp.linearize_profiles_for_year_streamed_from_index(
    2006, bootstrap_index[2006], weak_profiles_2006; overwrite = false)
pp.linearize_profiles_for_year_streamed_from_index(
    2018, bootstrap_index[2018], weak_profiles_2018; overwrite = false)
pp.linearize_profiles_for_year_streamed_from_index(
    2022, bootstrap_index[2022], weak_profiles_2022; overwrite = false)

linearized_profiles_2006 = pp.load_linearized_profiles_index(2006)
linearized_profiles_2018 = pp.load_linearized_profiles_index(2018)
linearized_profiles_2022 = pp.load_linearized_profiles_index(2022)

# ------------------------------------------------------------------
# Global measures (per year, from loaded linearized profiles)
# ------------------------------------------------------------------
measures_2006 = pp.save_or_load_measures_for_year(2006, linearized_profiles_2006;
                    overwrite = false,   # set true to rebuild
                    verbose   = true)    # progress / info logs

measures_2018 = pp.save_or_load_measures_for_year(2018, linearized_profiles_2018;
                    overwrite = false,   # set true to rebuild
                    verbose   = true)    # progress / info logs

measures_2022 = pp.save_or_load_measures_for_year(2022, linearized_profiles_2022;
                    overwrite = false,   # set true to rebuild
                    verbose   = true)    # progress / info logs

# ------------------------------------------------------------------
# Group metrics (per year)
# ------------------------------------------------------------------
group_metrics_2006 = pp.save_or_load_group_metrics_for_year(
                        2006, linearized_profiles_2006, bootstrap_index[2006];
                        overwrite = false, verbose = true, two_pass = true)

group_metrics_2018 = pp.save_or_load_group_metrics_for_year(
                        2018, linearized_profiles_2018, bootstrap_index[2018];
                        overwrite = false, verbose = true, two_pass = true)

group_metrics_2022 = pp.save_or_load_group_metrics_for_year(
                        2022, linearized_profiles_2022, bootstrap_index[2022];
                        overwrite = false, verbose = true, two_pass = true)


# ------------------------------------------------------------------
# Scenario plots — 2006 (Lula vs Alckmin)
# ------------------------------------------------------------------
plot_measures_2006 = Dict(2006 => measures_2006)

fig_2006_mice   = pp.plot_scenario_year(2006, "lula_alckmin", bootstrap_index, plot_measures_2006; variant = "mice", connect_lines = true)

fig_2006_random = pp.plot_scenario_year(2006, "lula_alckmin", bootstrap_index, plot_measures_2006; variant = "random", connect_lines = true)


fig_2006_zero   = pp.plot_scenario_year(2006, "lula_alckmin", bootstrap_index, plot_measures_2006; variant = "zero", connect_lines = true)


fig_2006_mice_dot = pp.plot_scenario_year(2006, "lula_alckmin", bootstrap_index, plot_measures_2006;
    variant="mice", plot_kind=:dotwhisker, connect_lines = true)

fig_2006_random_dot = pp.plot_scenario_year(2006, "lula_alckmin", bootstrap_index, plot_measures_2006; variant = "random",  plot_kind=:dotwhisker, connect_lines = true)


fig_2006_zero_dot   = pp.plot_scenario_year(2006, "lula_alckmin", bootstrap_index, plot_measures_2006; variant = "zero",  plot_kind=:dotwhisker, connect_lines = true)





pp.save_plot(fig_2006_mice,   2006, "lula_alckmin", cfg_2006; variant = "mice")
pp.save_plot(fig_2006_random, 2006, "lula_alckmin", cfg_2006; variant = "random")
pp.save_plot(fig_2006_zero,   2006, "lula_alckmin", cfg_2006; variant = "zero")


pp.save_plot(fig_2006_mice_dot,   2006, "lula_alckmin_dot", cfg_2006; variant = "mice")
pp.save_plot(fig_2006_random_dot, 2006, "lula_alckmin_dot", cfg_2006; variant = "random")
pp.save_plot(fig_2006_zero_dot,   2006, "lula_alckmin_dot", cfg_2006; variant = "zero")

# ------------------------------------------------------------------
# Scenario plots — 2018 (three scenarios)
# ------------------------------------------------------------------
plot_measures_2018 = Dict(2018 => measures_2018)

fig_2018_main_four     = pp.plot_scenario_year(2018, "main_four",      bootstrap_index, plot_measures_2018; variant = "mice", connect_lines = true)
fig_2018_no_forcing    = pp.plot_scenario_year(2018, "no_forcing",     bootstrap_index, plot_measures_2018; variant = "mice", connect_lines = true)
fig_2018_lula_bolsonaro = pp.plot_scenario_year(2018, "lula_bolsonaro", bootstrap_index, plot_measures_2018; variant = "mice", connect_lines = true)


fig_2018_main_four_dot     = pp.plot_scenario_year(2018, "main_four",      bootstrap_index, plot_measures_2018; variant = "mice", plot_kind=:dotwhisker, connect_lines = true)
fig_2018_no_forcing_dot    = pp.plot_scenario_year(2018, "no_forcing",     bootstrap_index, plot_measures_2018; variant = "mice", plot_kind=:dotwhisker, connect_lines = true)
fig_2018_lula_bolsonaro_dot = pp.plot_scenario_year(2018, "lula_bolsonaro", bootstrap_index, plot_measures_2018; variant = "mice", plot_kind=:dotwhisker, connect_lines = true)



cfg_2018 = bootstrap_index[2018].cfg
pp.save_plot(fig_2018_main_four,      2018, "main_four",     cfg_2018; variant = "mice")
pp.save_plot(fig_2018_no_forcing,     2018, "no_forcing",    cfg_2018; variant = "mice")
pp.save_plot(fig_2018_lula_bolsonaro, 2018, "lula_bolsonaro", cfg_2018; variant = "mice")


pp.save_plot(fig_2018_main_four_dot,      2018, "main_four_dot",     cfg_2018; variant = "mice")
pp.save_plot(fig_2018_no_forcing_dot,     2018, "no_forcing_dot",    cfg_2018; variant = "mice")
pp.save_plot(fig_2018_lula_bolsonaro_dot, 2018, "lula_bolsonaro_dot", cfg_2018; variant = "mice")


# ------------------------------------------------------------------
# Scenario plots — 2022 (Lula vs Bolsonaro)
# ------------------------------------------------------------------
plot_measures_2022 = Dict(2022 => measures_2022)

fig_2022_mice   = pp.plot_scenario_year(2022, "lula_bolsonaro", bootstrap_index, plot_measures_2022; variant = "mice", connect_lines = true)


fig_2022_random = pp.plot_scenario_year(2022, "lula_bolsonaro", bootstrap_index, plot_measures_2022; variant = "random", connect_lines = true)

fig_2022_zero   = pp.plot_scenario_year(2022, "lula_bolsonaro", bootstrap_index, plot_measures_2022; variant = "zero", connect_lines = true)


fig_2022_mice_dot   = pp.plot_scenario_year(2022, "lula_bolsonaro",
                                            bootstrap_index,
                                            plot_measures_2022;
                                            variant = "mice",
                                            plot_kind=:dotwhisker,
                                            connect_lines = true)


fig_2022_random_dot = pp.plot_scenario_year(2022, "lula_bolsonaro",
                                            bootstrap_index,
                                            plot_measures_2022;
                                            variant = "random",
                                            plot_kind=:dotwhisker,
                                            connect_lines = true)

fig_2022_zero_dot   = pp.plot_scenario_year(2022, "lula_bolsonaro",
                                            bootstrap_index,
                                            plot_measures_2022;
                                            variant = "zero",
                                            plot_kind=:dotwhisker,
                                            connect_lines = true)



pp.save_plot(fig_2022_mice,   2022, "lula_bolsonaro", cfg_2022; variant = "mice")
pp.save_plot(fig_2022_random, 2022, "lula_bolsonaro", cfg_2022; variant = "random")
pp.save_plot(fig_2022_zero,   2022, "lula_bolsonaro", cfg_2022; variant = "zero")

pp.save_plot(fig_2022_mice_dot,   2022, "lula_bolsonaro_dot", cfg_2022; variant = "mice")
pp.save_plot(fig_2022_random_dot, 2022, "lula_bolsonaro_dot", cfg_2022; variant = "random")
pp.save_plot(fig_2022_zero_dot,   2022, "lula_bolsonaro_dot", cfg_2022; variant = "zero")



# ------------------------------------------------------------------
# Group-demographics panels — 2018
# ------------------------------------------------------------------



fig_2018_main_four_dem_main = pp.plot_group_demographics_lines(
    Dict(2018 => group_metrics_2018), bootstrap_index, 2018, "main_four";
    variants = [:mice], maxcols = 2, clist_size = 60,
    demographics = ["Income", "Ideology"], ytick_step = 0.05) 

fig_2018_main_four_dem_other = pp.plot_group_demographics_lines(
    Dict(2018 => group_metrics_2018), bootstrap_index, 2018, "main_four";
    variants = [:mice], maxcols = 3, clist_size = 60,
    demographics = setdiff(bootstrap_index[2018].cfg.demographics, ["Income", "Ideology"]))

fig_2018_main_four_dem_hm = pp.plot_group_demographics_heatmap(
    Dict(2018 => group_metrics_2018), bootstrap_index, 2018, "main_four";
    variants = [:mice],
    measures = [:C, :D, :G],
    maxcols = 3,
    clist_size = 60,
    colormaps = :RdBu |> pp.Makie.Reverse,
    fixed_colorrange = true, show_values = true,  simplified_labels = true)


pp.save_plot(fig_2018_main_four_dem_main, 2018, "main_four_group_main", cfg_2018; variant = "mice")
pp.save_plot(fig_2018_main_four_dem_other, 2018, "main_four_group_therest", cfg_2018; variant = "mice")
pp.save_plot(fig_2018_main_four_dem_hm, 2018, "main_four_group_hm", cfg_2018; variant = "mice")


# ------------------------------------------------------------------
# Group-demographics panels — 2022 (split main vs others)
# ------------------------------------------------------------------
dems_main_2022 = ["Ideology", "PT", "Abortion", "Religion", "Sex", "Income"]

fig_2022_lula_bolsonaro_dem_main = pp.plot_group_demographics_lines(
    Dict(2022 => group_metrics_2022), bootstrap_index, 2022, "lula_bolsonaro";
    variants = [:mice], maxcols = 3, clist_size = 60, demographics = dems_main_2022)

pp.save_plot(fig_2022_lula_bolsonaro_dem_main, 2022, "lula_bolsonaro_main", bootstrap_index[2022].cfg; variant = "mice")


fig_2022_lula_bolsonaro_dem_other = pp.plot_group_demographics_lines(
    Dict(2022 => group_metrics_2022), bootstrap_index, 2022, "lula_bolsonaro";
    variants = [:mice], maxcols = 3, clist_size = 60,
    demographics = setdiff(bootstrap_index[2022].cfg.demographics, dems_main_2022))

pp.save_plot(fig_2022_lula_bolsonaro_dem_other, 2022, "lula_bolsonaro_others", bootstrap_index[2022].cfg; variant = "mice")

fig_2022_lula_bolsonaro_dem_hm = pp.plot_group_demographics_heatmap(
    Dict(2022 => group_metrics_2022), bootstrap_index, 2022, "lula_bolsonaro";
    variants = [:mice],
    measures = [:C, :D, :G],
    maxcols = 3,
    clist_size = 60,
    colormaps = :RdBu |> pp.Makie.Reverse,
    fixed_colorrange = true, show_values =  true )

pp.save_plot(fig_2022_lula_bolsonaro_dem_other_hm, 2022, "lula_bolsonaro_dem_hm", bootstrap_index[2022].cfg; variant = "mice")
# ------------------------------------------------------------------
# Group-demographics panels — 2006 (all demographics)
# ------------------------------------------------------------------
fig_2006_lula_alckmin_group = pp.plot_group_demographics_lines(
    Dict(2006 => group_metrics_2006), bootstrap_index, 2006, "lula_alckmin";
    variants = [:mice], maxcols = 3, clist_size = 60)

pp.save_plot(fig_2006_lula_alckmin_group, 2006, "lula_alckmin_group", cfg_2006; variant = "mice")




fig_2006_lula_alckmin_group_hm = pp.plot_group_demographics_heatmap(
    Dict(2006 => group_metrics_2006), bootstrap_index, 2006, "lula_alckmin";
    variants = [:mice],
    measures = [:C, :D, :G],
    maxcols = 3,
    clist_size = 60,
    colormaps = :RdBu |> pp.Makie.Reverse,
    fixed_colorrange = true, show_values = true,  simplified_labels = true
) 



pp.save_plot(fig_2006_lula_alckmin_group_hm, 2006, "lula_alckmin_group_hm", cfg_2006; variant = "mice")



fig_2006_lula_alckmin_group_hm = group_metrics_2006 = imputed_year_2006 = profiles_20006 = nothing


#colormaps = pp.Makie.Reverse(:RdBu), fixed_colorrange = true)





# Checking:
