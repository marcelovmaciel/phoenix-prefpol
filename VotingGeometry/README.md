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
