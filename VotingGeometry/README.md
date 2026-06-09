# VotingGeometry.jl

WIP note: the four-candidate Saari geometry and decomposition notes in this
package are still being tested. 


`VotingGeometry.jl` is a Saari-style voting geometry package for the
`phoenix-prefpol` monorepo. It uses `Preferences.jl` for formal candidate
pools, rankings, profiles, weighted profiles, and profile linearization. It
owns the Saari canonical ranking bases, profile-vector projections, positional
scoring, simplex geometry, Saari decomposition, and PythonPlot visualizations.

Plotting currently lives directly in this package and uses `PythonPlot.jl` as a
normal dependency.

## Walkthrough Notebook

A longer Jupyter walkthrough lives at `examples/saari_geometry_walkthrough.ipynb`. It covers canonical orders, strict and weighted profile projection through `Preferences.jl`, 3-candidate Saari triangles, 4-candidate positional methods, tetrahedron plots, candidate restriction, and the 24-dimensional decomposition.

Run it from the repository root:

```bash
julia +1.11.9 --project=VotingGeometry -e 'using Pkg; Pkg.instantiate()'
julia +1.11.9 -e 'using Pkg; Pkg.add("IJulia"); using IJulia; installkernel("Julia 1.11 VotingGeometry", "--project=" * abspath("VotingGeometry"))'
jupyter lab VotingGeometry/examples/saari_geometry_walkthrough.ipynb
```

The `IJulia`/`installkernel` command is a one-time setup. In Jupyter, select the `Julia 1.11 VotingGeometry` kernel. If you already use VS Code notebooks, open the same `.ipynb` file and select that Julia kernel there.

If `jupyter lab` fails with `Jupyter command jupyter-lab not found`, only `jupyter-core` is installed. Install a notebook frontend first. With Conda, prefer:

```bash
conda install -c conda-forge jupyterlab
```

Or with pip:

```bash
python3 -m pip install --user jupyterlab
```

Then rerun:

```bash
jupyter lab VotingGeometry/examples/saari_geometry_walkthrough.ipynb
```

## Minimal 3-Candidate Example

```julia
using Preferences, VotingGeometry

pool = CandidatePool([:A, :B, :C])
p = Profile(pool, [
    StrictRank(pool, [:A, :B, :C]),
    StrictRank(pool, [:A, :C, :B]),
    StrictRank(pool, [:C, :A, :B]),
])

basis = SaariBasis3(pool)
v = profile_vector(p, basis)
ax = plot_saari_triangle(v)
```

## 4-Candidate Decomposition Example

```julia
using Preferences, VotingGeometry

pool = CandidatePool([:A, :B, :C, :D])
p = Profile(pool, [
    StrictRank(pool, [:A, :B, :C, :D]),
    StrictRank(pool, [:B, :A, :C, :D]),
    StrictRank(pool, [:D, :C, :B, :A]),
])

basis = SaariBasis4(pool)
v = profile_vector(p, basis)
dec = decompose_profile(v)

condorcet_component = group_component(dec, :condorcet)
reconstructed_profile = reconstruct(dec)
```


## Four-Candidate Geometry Objects

`VotingGeometry` keeps two Saari objects separate.

The candidate-tally tetrahedron is the closed simplex of normalized candidate
score/tally shares, `x_A + x_B + x_C + x_D = 1`. Use
`plot_candidate_tally_tetrahedron` for this object. When a Saari procedure hull
is drawn in this tetrahedron, the display coordinates are candidate score shares in the q-image; the
hull itself is still defined by Saari's raw vote-for-k tallies.

The opened representation tetrahedron is the profile/ranking-region object from
Saari's representation simplex. It is opened by cutting along edges from `D`; each
face omits the candidate that is bottom-ranked on that face, and ranking regions
are interpreted by distance to candidate vertices. Use
`plot_opened_representation_tetrahedron`, `plot_profile_on_opened_tetrahedron`,
and `plot_signed_profile_tetrahedron` for profile frequencies or signed profile
differentials.

## Four-Candidate Procedure Hull

For four candidates, positional scoring vectors have the form `(1, s1, s2, 0)`
with `0 <= s2 <= s1 <= 1`. Saari's procedure hull uses the score-vector identity

```julia
(1, s1, s2, 0) ==
    (1 - s1) * (1, 0, 0, 0) +
    (s1 - s2) * (1, 1, 0, 0) +
    s2 * (1, 1, 1, 0)
```

So `procedure_hull_barycentric_4c(s1, s2)` defaults to Saari coordinates
`(1 - s1, s1 - s2, s2)`. Borda `(1, 2/3, 1/3, 0)` is the barycenter
`(1/3, 1/3, 1/3)`, and the raw Borda tally is the average of the three
vote-for-k tallies.

`get_4c_w_s` and `raw_score_tally_4c` return raw score tallies. For
count profiles, those tallies sum to `sum(p4) * (1 + s1 + s2)`. The explicit
`score_tally_per_rule_mass_4c` helper preserves the older normalization that
divides only by `1 + s1 + s2`; for count profiles it sums to `sum(p4)` and is
not a candidate score share.

`candidate_score_share_4c` and its compatibility name `q_s_4candidates` return
true candidate score shares by dividing by `sum(p4) * (1 + s1 + s2)`. They are
invariant to multiplying an actual profile by a positive scalar:

```julia
procedure_hull_point_4c(p4, 2/3, 1/3)       # Saari raw Borda tally
score_tally_per_rule_mass_4c(p4, 2/3, 1/3)  # old per-rule-mass tally
q_s_4candidates(p4, 2/3, 1/3)               # Borda candidate score shares
```

For a fixed profile and `(s1, s2)`, these normalizations divide all candidates
by positive scalars, so they do not change candidate score comparisons at that
rule. They should not be used to decide whether Borda is the procedure-hull
barycenter; that statement belongs to Saari's raw score-vector hull.


## Four-Candidate Comparison Regions

`positional_comparison_region_masks`, `positional_comparison_region_table`,
`positional_comparison_region_exact_table`, and the corresponding plot helpers
default to generic comparisons. With `comparisons=nothing`, they use every
unordered pairwise weak comparison in the supplied label order: `label_i >=
label_j` for `i < j`. For labels `[:A, :B, :C, :D]`, the default comparisons are
`A >= B`, `A >= C`, `A >= D`, `B >= C`, `B >= D`, and `C >= D`.

`plot_positional_comparison_regions` draws exact half-plane-clipped
polygons by default, and `positional_comparison_region_exact_table` reports
exact proportions from polygon area over the Saari parameter triangle
`0 <= s2 <= s1 <= 1`. The old finite-grid visualization remains available as
`plot_positional_comparison_regions_grid`, while
`positional_comparison_region_table` reports approximate diagnostic grid
proportions.

Pass explicit comparisons for paper-specific claims. The EJPE Bolsonaro 2018
examples remain available under an explicit helper:

```julia
plot_positional_comparison_regions(
    p4,
    [:Alckmin, :Bolsonaro, :Ciro, :Haddad];
    comparisons = ejpe_bolsonaro_comparison_specs(),
)
```

## Decomposition Summaries and Plots

`CANONICAL_4C_IDS` follows Saari's Figure 8 voter-type order for the Theorem 14
matrix: type 1 is `A>B>C>D` and type 18 is `B>C>D>A`. The matrix inverse is
validated against source-derived constructors for:

- `K4`, the all-ranking neutral profile direction;
- six double-reversal differentials spanning `UK4` with `K4`;
- Basic profile differentials `B^4_A`, `B^4_B`, `B^4_C`, with `B^4_D` determined by their sum;
- Condorcet profile differentials generated by `ABCD`, `ABDC`, and `ACBD`;
- departure/positional deviations that affect non-Borda positional procedures but have zero pairwise and full-set Borda tallies.

Basic and Borda profiles are related but not identical for four candidates. Basic
profiles are the conflict-free components: normalized positional tallies over all
subsets agree in Saari's sense, and pairwise margins satisfy additive transitivity.
Condorcet components are orthogonal to Basic and double-reversal components;
they have zero full four-candidate positional tallies but nonzero cyclic pairwise
effects. Departure components have zero pairwise margins and zero full-set Borda
tallies while changing non-Borda positional outcomes. `COMPONENT_METADATA` records
source descriptions for each component. The eight subset departure components
keep canonical `:departure_subset_i` labels until their exact Saari subset and
candidate-role mapping is verified from a source fixture.

```julia
dec = decompose_profile(p4)
label_rows = component_summary(dec; by = :label)
description_rows = component_summary(dec; by = :label, label_style = :description)
group_rows = component_summary(dec; by = :group)

ax = plot_decomposition_coefficients(dec)
fig = plot_decomposition_component_tetrahedra(dec; groups = (:kernel, :condorcet))
check_fig = plot_decomposition_reconstruction_check(dec)
```

Decomposition components can be signed, so use
`plot_signed_profile_tetrahedron` or the decomposition plot helpers for them.
`plot_profile_tetrahedron_freqs` remains for nonnegative profile frequencies.
