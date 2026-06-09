# VotingGeometry.jl

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
is drawn in this tetrahedron, only the display coordinates are normalized into
candidate shares; the hull itself is still defined by Saari's raw vote-for-k
tallies.

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

The older `q_s_4candidates` helper normalizes each score tally by total score
`1 + s1 + s2`. This is a score-share display helper, not Saari's procedure-hull
coordinate system:

```julia
procedure_hull_point_4c(p4, 2/3, 1/3)  # Saari raw Borda tally
q_s_4candidates(p4, 2/3, 1/3)          # normalized Borda score shares
```

Because this normalization divides all candidates by the same positive scalar
for fixed `(s1, s2)`, it does not change candidate score comparisons at that
rule. It should not be used to decide whether Borda is the procedure-hull
barycenter; that statement belongs to Saari's raw score-vector hull.

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
tallies while changing non-Borda positional outcomes.

```julia
dec = decompose_profile(p4)
label_rows = component_summary(dec; by = :label)
group_rows = component_summary(dec; by = :group)

ax = plot_decomposition_coefficients(dec)
fig = plot_decomposition_component_tetrahedra(dec; groups = (:kernel, :condorcet))
check_fig = plot_decomposition_reconstruction_check(dec)
```

Decomposition components can be signed, so use
`plot_signed_profile_tetrahedron` or the decomposition plot helpers for them.
`plot_profile_tetrahedron_freqs` remains for nonnegative profile frequencies.

See `docs/saari_implementation_notes.md` for the source audit and page/section
references used for this implementation pass. A compact script version is
available at `examples/procedure_hull_and_decomposition.jl`.
