using PrefPol

import PrefPol as pp

Base.include(PrefPol, "../src/unused_helpers.jl")

year = 2018
iy  = PrefPol.load_imputed_year(year)

bootstrap_index = pp.load_all_bootstraps(years = [year])

cfg = bootstrap_index[year].cfg  # ysame cfg you pass into group metrics

bad = PrefPol.audit_profiles_year(year)

bad = filter(b -> b.scenario != "no_forcing", bad)

PrefPol.repair_bad_profiles!(year, bad, iy, cfg)





using PrefPol
import PrefPol as pp 
Base.include(PrefPol, "../src/unused_helpers.jl")



year = 2018
iy  = PrefPol.load_imputed_year(year)
bootstrap_index = pp.load_all_bootstraps(years = [year])

cfg = bootstrap_index[year].cfg

b = (problem = :read_error,
     file = "../intermediate_data/profiles_data/prof_2018_lula_bolsonaro_m2_rep370_zero.jld2",
     scenario = "lula_bolsonaro",
     m = 2,
     variant = :zero,
     rep = 370,
     err = pp.JLD2.InvalidDataException("Invalid Object header signature"))


idx = PrefPol.load_profiles_index(year)

ps  = idx[b.scenario][b.m]

PrefPol.rebuild_profile_file!(ps, iy, cfg, b.variant, b.rep)
