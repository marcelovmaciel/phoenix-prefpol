using Revise
import PrefPol as pp
using DataFrames

# Quick REPL helper for inspecting candidate-score missingness on the raw survey
# datasets used by PrefPol before bootstrap/imputation. We route through the
# existing year-specific loaders so the hard-coded candidate recodes are
# respected. Those loaders collapse candidate nonresponse to survey code `99.0`,
# so the missingness table must count that code as missing.
const Preferences = pp.Preferences




years = [2006, 2018, 2022]
missing_codes = (99.0,)

for year in years
    println("\n" * "="^72)
    println("Year $year")
    println("="^72)

    cfg = pp.load_election_cfg(joinpath(pp.project_root, "config", "$year.toml"))
    raw = pp.load_raw_pref_data(year; candidate_set = cfg.candidates)

    println("Pre-imputation survey data: $(raw.cfg.data_file)")
    println("nrow = $(nrow(raw.df))")
    println("candidate_cols = $(raw.candidate_cols)")
    println("candidate nonresponse codes treated as missing = $(collect(missing_codes))")

    tbl_unweighted = Preferences.candidate_missingness_table(
        raw.df,
        raw.candidate_cols;
        weighted = false,
        missing_codes = missing_codes,
    )
    display(tbl_unweighted)
end
