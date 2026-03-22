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
# # Global measures (per year)
# # ------------------------------------------------------------------
# measures_2006 = pp.save_or_load_measures_for_year(2006, linearized_profiles_2006;
#                     overwrite = false,   # set true to rebuild
#                     verbose   = true)    # progress / info logs



# measures_2018 = pp.save_or_load_measures_for_year(2018, linearized_profiles_2018;
#                     overwrite = false,   # set true to rebuild
#                     verbose   = true)    # progress / info logs

# measures_2022 = pp.save_or_load_measures_for_year(2022, linearized_profiles_2022;
#                     overwrite = false,   # set true to rebuild
#                     verbose   = true)    # progress / info logs


# ------------------------------------------------------------------
# Group metrics (per year)
# ------------------------------------------------------------------


group_metrics_2006 = pp.save_or_load_group_metrics_for_year(
                        2006, linearized_profiles_2006, bootstrap_index[2006];
                        overwrite = false, verbose = true, two_pass = true)



fig_group_hm1 = pp.plot_group_demographics_heatmap(
    Dict(2006 => group_metrics_2006), bootstrap_index, 2006, "lula_alckmin";
    variants = [:mice],
    measures = [:C, :D, :G],
    maxcols = 3,
    clist_size = 60,
    show_values = true,
    fixed_colorrange = true,
    simplified_labels = true,
    modified_G = false,
    modified_C = true,
    colormaps = :RdBu |> pp.Makie.Reverse,
)


    # colormaps = pp.Makie.Reverse(:RdBu),
    # fixed_colorrange = true,


pp.save_plot(fig_group_hm1, 2006,
             "lula_alckmin_group_hm_Cstar",
             cfg_2006; variant = "mice")


group_metrics_2018 = pp.save_or_load_group_metrics_for_year(
                        2018, linearized_profiles_2018, bootstrap_index[2018];
                        overwrite = false, verbose = true, two_pass = true)

fig_group_hm2 = pp.plot_group_demographics_heatmap(
    Dict(2018 => group_metrics_2018), bootstrap_index, 2018, "main_four";
    variants = [:mice],
    measures = [:C, :D, :G],
    maxcols = 3,
    clist_size = 60,
    show_values = true,
    fixed_colorrange = true,
    simplified_labels = true,
    modified_G = false,
    modified_C = true,
    colormaps = :RdBu |> pp.Makie.Reverse,
)

pp.save_plot(fig_group_hm2, 2018, "main_four_group_hm_Cstar", cfg_2018; variant = "mice")

group_metrics_2022 = pp.save_or_load_group_metrics_for_year(
                        2022, linearized_profiles_2022, bootstrap_index[2022];
                        overwrite = false, verbose = true, two_pass = true)

fig_group_hm3 = pp.plot_group_demographics_heatmap(
    Dict(2022 => group_metrics_2022), bootstrap_index, 2022, "lula_bolsonaro";
    variants = [:mice],
    measures = [:C, :D, :G],
    maxcols = 3,
    clist_size = 60,
    show_values = true,
    fixed_colorrange = true,
    simplified_labels = true,
    modified_G = false,
    modified_C = true,
    colormaps = :RdBu |> pp.Makie.Reverse,
)

pp.save_plot(fig_group_hm3, 2022, "lula_bolsonaro_group_hm_Cstar", cfg_2022; variant = "mice")
