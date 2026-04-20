using Revise
using Pkg

const PACKAGE_ROOT = normpath(joinpath(@__DIR__, ".."))
const TEST_ENV = joinpath(PACKAGE_ROOT, "test")

Pkg.activate(TEST_ENV)
PACKAGE_ROOT in LOAD_PATH || pushfirst!(LOAD_PATH, PACKAGE_ROOT)

using CairoMakie
using PrefPol
import PrefPol as pp

const M = pp.Makie

year = 2006
scenario = "lula_alckmin"

overwrite_imputation = false
overwrite_profiles = false
overwrite_measures = false
overwrite_group_metrics = false

# ------------------------------------------------------------------
# Bootstraps (load) + imputation index
# ------------------------------------------------------------------
bootstrap_index = pp.load_all_bootstraps(years = [year])
#pp.impute_from_f3(bootstrap_index; overwrite = overwrite_imputation)

imputed_year = pp.load_imputed_year(year)

# ------------------------------------------------------------------
# Weak profiles, linearized profiles, measures, and group metrics
# ------------------------------------------------------------------
weak_profiles_year = pp.generate_profiles_for_year_streamed_from_index(
    year, bootstrap_index[year], imputed_year; overwrite = overwrite_profiles)

pp.linearize_profiles_for_year_streamed_from_index(
    year, bootstrap_index[year], weak_profiles_year; overwrite = overwrite_profiles)
linearized_profiles_year = pp.load_linearized_profiles_index(year)

measures_year = pp.save_or_load_measures_for_year(
    year, linearized_profiles_year; overwrite = overwrite_measures, verbose = true)

group_metrics_year = pp.save_or_load_group_metrics_for_year(
    year, linearized_profiles_year, bootstrap_index[year];
    overwrite = overwrite_group_metrics, verbose = true, two_pass = true)




# ------------------------------------------------------------------
# Scenario dot-whisker plots (mice, random, zero)
# ------------------------------------------------------------------
cfg = bootstrap_index[year][:cfg]

plot_measures = Dict(year=> measures_year)

fig_mice_dot = pp.plot_scenario_year(
    year, scenario, bootstrap_index, plot_measures;
    variant = "mice", plot_kind = :dotwhisker, connect_lines = true)

fig_random_dot = pp.plot_scenario_year(
    year, scenario, bootstrap_index, plot_measures;
    variant = "random", plot_kind = :dotwhisker, connect_lines = true)

fig_zero_dot = pp.plot_scenario_year(
    year, scenario, bootstrap_index, plot_measures;
    variant = "zero", plot_kind = :dotwhisker, connect_lines = true)

pp.save_plot(fig_mice_dot, year, "$(scenario)_dot", cfg; variant = "mice")
pp.save_plot(fig_random_dot, year, "$(scenario)_dot", cfg; variant = "random")
pp.save_plot(fig_zero_dot, year, "$(scenario)_dot", cfg; variant = "zero")

# ------------------------------------------------------------------
# Group-demographics plots (all demographics)
# ------------------------------------------------------------------



fig_group_hm = pp.plot_group_demographics_heatmap(
    Dict(year => group_metrics_year), bootstrap_index, year, scenario;
    variants = [:mice],
    measures = [:C, :D, :G],
    maxcols = 3,
    clist_size = 60,
    colormaps = :RdBu |> M.Reverse,
    fixed_colorrange = true,
    show_values = true,
    simplified_labels = true)

pp.save_plot(fig_group_hm, year, "$(scenario)_group_hm", cfg; variant = "mice")
