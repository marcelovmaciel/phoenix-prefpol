# From Scores to Profiles

The tabular profile layer converts empirical score columns into formal weak
rankings. Higher numeric scores imply more preferred alternatives. Equal scores
become ties when `allow_ties=true`. Missing scores become unranked when
`allow_incomplete=true`.

Rows with all candidates unranked are skipped by default. If
`all_unranked_as_indifferent=true`, they are instead converted into a fully
indifferent weak ranking. In weighted profiles, weights are stored as mass
attached to ballots; they are not replicated rows.

```julia
using DataFrames, Preferences

df = DataFrame(
    a = [10, 5, 96, missing],
    b = [7, 5, 3, 97],
    c = [0, 99, 8, missing],
    weight = [1.0, 2.0, 1.5, 0.5],
)

sentinel_missing = Set([96, 97, 98, 99])
score_norm(x) = ismissing(x) || x in sentinel_missing ? missing : normalize_numeric_score(x)

p = build_profile_from_scores(
    df,
    ["a", "b", "c"],
    [:a, :b, :c];
    weighted = true,
    weight_col = :weight,
    allow_ties = true,
    allow_incomplete = true,
    score_normalizer = score_norm,
)

profile_build_meta(p).kept_rows
```

Use missingness tables to inspect the measurement layer before constructing the
profile:

```julia
candidate_missingness_table(
    df,
    ["a", "b", "c"];
    weighted = true,
    weight_col = :weight,
    missing_codes = sentinel_missing,
)
```

## API

- `humanize_candidate_name`
- `canonical_candidate_key`
- `candidate_display_symbols`
- `guess_weight_col`
- `resolve_candidate_cols_from_set`
- `candidate_missingness_table`
- `normalize_numeric_score`
- `normalize_nonnegative_weight`
- `row_to_weak_rank_from_scores`
- `build_profile_from_scores`
- `profile_build_meta`

