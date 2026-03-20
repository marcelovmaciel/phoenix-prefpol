using Revise
import PrefPol as pp




cfg = pp.load_election_cfg("../config/2018.toml")
df  = pp.load_election_data(cfg)


df.Q18 |> unique 

cfg

df_test = pp.load_spss_file(cfg.data_file)

df_test.Q18 |> unique 

names(df)
# make sure PT is present


# impute candidate scores (keeps PT + other demos)





# Pipeline‑consistent candidate ordering (pick a scenario if you want)
scen = cfg.scenarios[3]  # e.g., "no_forcing"
full_list = pp.compute_candidate_set(df;
    candidate_cols = cfg.candidates,
    m = cfg.max_candidates,
    force_include = scen.candidates)

cand3 = first(full_list, 3)
cand4 = first(full_list, 4)

demos

imp = pp.imputation_variants(df, cfg.candidates, cfg.demographics;
                             most_known_candidates = cand4)


df_prof = pp.profile_dataframe(imp.mice;
        score_cols = Symbol.(cand4),
        demo_cols  = Symbol.(cfg.demographics))


C, D = pp.compute_group_metrics(df_prof, :PT)



sqrt(C * D)
