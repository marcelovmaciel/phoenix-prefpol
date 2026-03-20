using Revise
import PrefPol as pp

# Quick REPL helper for inspecting candidate-score missingness on raw survey
# datasets. The reusable utility lives in Preferences; this script just wires it
# to PrefPol's raw pre-imputation loaders and TOML-driven candidate selection.
# These loaders preserve candidate nonresponse as survey code `99.0`, so the
# missingness table must be told to count that code as missing.
const Preferences = pp.Preferences

year = 2018
candidate_scope = :all_configured  # or :scenario
scenario_name = "main_four"
missing_codes = (99.0,)

cfg = pp.load_election_cfg(joinpath(pp.project_root, "config", "$year.toml"))

raw = if candidate_scope == :all_configured
    pp.load_raw_pref_data(year; candidate_set = cfg.candidates)
elseif candidate_scope == :scenario
    pp.load_raw_pref_data(year; scenario_name = scenario_name)
else
    error("Unknown candidate_scope = $candidate_scope. Use :all_configured or :scenario.")
end

df = raw.df
candidate_cols = raw.candidate_cols
weight_col = raw.weight_col

println("Pre-imputation survey data: $(raw.cfg.data_file)")
println("nrow = $(nrow(df))")
println("candidate_scope = $(candidate_scope)")
println("candidate_cols = $(candidate_cols)")
println("candidate nonresponse codes treated as missing = $(collect(missing_codes))")

tbl_unweighted = Preferences.candidate_missingness_table(df, candidate_cols;
                                                         weighted = false,
                                                         missing_codes = missing_codes)
display(tbl_unweighted)

if weight_col !== nothing
    tbl_weighted = Preferences.candidate_missingness_table(
        df,
        candidate_cols;
        weighted = true,
        weight_col = weight_col,
        missing_codes = missing_codes,
    )
    display(tbl_weighted)
else
    println("No weight column detected for year $year.")
end
