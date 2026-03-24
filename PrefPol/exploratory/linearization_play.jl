using Revise
using PrefPol
using DataFrames
using OrderedCollections: OrderedDict
import PrefPol as pp

# ------------------------------------------------------------------
# Choose a year/scenario to inspect
# ------------------------------------------------------------------
year = 2022
scenario = "lula_bolsonaro"

# Set `m` to a concrete Int to force a specific candidate count.
# Leave as `nothing` to use the first available `m` for the scenario.
m = nothing

variant = :mice
rep = 1

# Build a tiny exploratory pipeline instead of depending on the full
# bootstrap/imputation artifacts for the whole project.
bootstrap_replicates = 3

overwrite_bootstrap = false
overwrite_imputation = false
overwrite_candidate_sets = false
overwrite_weak_profiles = false  
overwrite_linearized_profiles = false

play_root = joinpath(pp.project_root, "exploratory", "_tmp", "linearization_play", string(year))
bootstrap_dir = joinpath(play_root, "bootstraps")
imputation_dir = joinpath(play_root, "imputed")
candidate_dir = joinpath(play_root, "candidate_sets")
weak_dir = joinpath(play_root, "weak_profiles")
linearized_dir = joinpath(play_root, "linearized_profiles")

for dir in (play_root, bootstrap_dir, imputation_dir, candidate_dir, weak_dir, linearized_dir)
    mkpath(dir)
end

cfg_path = joinpath(pp.project_root, "config", "$(year).toml")
base_cfg = pp.load_election_cfg(cfg_path)

play_cfg = pp.ElectionConfig(
    base_cfg.year,
    base_cfg.data_loader,
    base_cfg.data_file,
    base_cfg.max_candidates,
    base_cfg.m_values_range,
    bootstrap_replicates,
    base_cfg.n_alternatives,
    base_cfg.rng_seed,
    base_cfg.candidates,
    base_cfg.demographics,
    base_cfg.scenarios,
)

println("play root:             ", play_root)
println("bootstrap replicates:  ", bootstrap_replicates)
println()

# ------------------------------------------------------------------
# Stage 0: build/load a tiny bootstrap + imputation index
# ------------------------------------------------------------------
boot = pp.save_bootstrap(
    play_cfg;
    dir = bootstrap_dir,
    overwrite = overwrite_bootstrap,
    quiet = false,
)

bootstrap_index = OrderedDict(
    year => (data = boot.data, cfg = play_cfg, path = boot.path),
)

pp.impute_from_f3(
    bootstrap_index;
    years = [year],
    imp_dir = imputation_dir,
    overwrite = overwrite_imputation,
)

imputed_year = pp.load_imputed_year(year; dir = imputation_dir)
candidate_sets = pp.save_or_load_candidate_sets_for_year(
    play_cfg;
    dir = candidate_dir,
    overwrite = overwrite_candidate_sets,
    verbose = false,
)

# ------------------------------------------------------------------
# Stage 1: build and save weak profiles
# ------------------------------------------------------------------
weak_profiles_year = pp.generate_profiles_for_year_streamed_from_index(
    year,
    bootstrap_index[year],
    imputed_year;
    candidate_sets = candidate_sets,
    out_dir = weak_dir,
    overwrite = overwrite_weak_profiles,
)

# ------------------------------------------------------------------
# Stage 2: linearize saved weak profiles into a separate artifact
# ------------------------------------------------------------------
pp.linearize_profiles_for_year_streamed_from_index(
    year,
    bootstrap_index[year],
    weak_profiles_year;
    out_dir = linearized_dir,
    overwrite = overwrite_linearized_profiles,
)

# ------------------------------------------------------------------
# Stage 3: load the linearized artifact
# ------------------------------------------------------------------
linearized_profiles_year = pp.load_linearized_profiles_index(year; dir = linearized_dir)

available_scenarios = collect(keys(weak_profiles_year))
scenario in available_scenarios || error("Unknown scenario `$scenario`. Available: $(available_scenarios)")

available_ms = sort(collect(keys(weak_profiles_year[scenario])))
chosen_m = isnothing(m) ? first(available_ms) : m
chosen_m in available_ms || error("Unknown m=$chosen_m. Available for `$scenario`: $(available_ms)")

weak_slice = weak_profiles_year[scenario][chosen_m]
linearized_slice = linearized_profiles_year[scenario][chosen_m]

haskey(weak_slice.paths, variant) || error("Unknown variant `$variant`. Available: $(collect(keys(weak_slice.paths)))")
length(weak_slice.paths[variant]) >= rep || error("rep=$rep exceeds available replicates for `$variant`.")

weak_bundle = weak_slice[variant, rep]
linearized_bundle = linearized_slice[variant, rep]

weak_artifact = pp._load_profile_artifact(weak_slice.paths[variant][rep])
linearized_artifact = pp._load_profile_artifact(linearized_slice.paths[variant][rep])

weak_rankings = pp.profile_to_ranking_dicts(weak_bundle.profile)
linearized_rankings = pp.profile_to_ranking_dicts(linearized_bundle.profile)

has_tie(ranking) = length(unique(values(ranking))) < length(ranking)
row_to_show = something(findfirst(has_tie, weak_rankings), 1)

println("year = $year")
println("scenario = $scenario")
println("m = $chosen_m")
println("variant = $variant")
println("rep = $rep")
println()
println("weak profile file:       ", weak_slice.paths[variant][rep])
println("linearized profile file: ", linearized_slice.paths[variant][rep])
println()
println("weak profile kind:       ", metadata(weak_artifact, "profile_kind"))
println("linearized profile kind: ", metadata(linearized_artifact, "profile_kind"))
println("candidates:              ", collect(pp.Preferences.candidates(linearized_bundle.profile.pool)))
println()
println("inspecting row $row_to_show (first tied row if available)")
println("weak profile row:")
println(weak_rankings[row_to_show])
println()
println("linearized profile row:")
println(linearized_rankings[row_to_show])

# ------------------------------------------------------------------
# Optional next step: measures are applied only after loading the
# linearized artifact, not during linearization itself.
# ------------------------------------------------------------------
# one_rep_measures = pp.apply_all_measures_to_bts(Dict(variant => [linearized_bundle]))
# measures_year = pp.save_or_load_measures_for_year(year, linearized_profiles_year;
#     overwrite = false, verbose = true)
