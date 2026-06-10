# Annotated Profiles

```@meta
CurrentModule = PreferenceProfiles
```

`AnnotatedProfile` bundles a formal profile with row-aligned metadata. The
formal profile is what ranking, consensus, and diagnostic functions use.
Metadata is for grouping, subsetting, and reporting. Subsetting an annotated
profile must preserve ballot-metadata alignment; if weights are present, the
selected weights stay attached to the selected ballots.

```julia
using PreferenceProfiles

pool = CandidatePool([:a, :b, :c])
p = Profile(pool, [
    StrictRank(pool, [:a, :b, :c]),
    StrictRank(pool, [:a, :c, :b]),
    StrictRank(pool, [:c, :b, :a]),
])

bundle = annotated_profile(p, (group = [:g1, :g1, :g2],))
sub = subset_annotated_profile(bundle, [1, 2])

(nballots(sub.profile), sub.metadata.group)
```

DataFrame encoders keep a portable rank representation together with candidate
metadata:

```julia
df = annotated_profile_to_dataframe(bundle)
decoded = dataframe_to_annotated_profile(df)
```

## API

```@docs
AnnotatedProfile
annotated_profile
dataframe_to_annotated_profile
annotated_profile_to_dataframe
profile_to_ranking_dicts
linearize_annotated_profile
subset_annotated_profile
strict_profile
overall_divergences
overall_overlaps
overall_overlaps_smoothed
overall_divergences_median
overall_separations
compute_group_metrics
```

