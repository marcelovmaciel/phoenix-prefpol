# PreferenceProfiles.jl Documentation

This directory contains local HTML documentation for the `PreferenceProfiles` Julia
package in this monorepo.

From the repository root, instantiate and build the docs with Julia 1.11.9:

```bash
ROOT=$(pwd)
(cd /tmp && julia +1.11.9 --project="$ROOT/PreferenceProfiles/docs" -e 'using Pkg; Pkg.instantiate()')
(cd /tmp && julia +1.11.9 --project="$ROOT/PreferenceProfiles/docs" "$ROOT/PreferenceProfiles/docs/make.jl")
```

Starting Julia outside the repository tree avoids shadowing Julia's internal
`PreferenceProfiles` dependency with the local package directory during dependency
resolution.

To browse the generated site locally, serve the build directory:

```bash
cd PreferenceProfiles/docs/build
python3 -m http.server --bind localhost 8000
```

Then open `http://localhost:8000`.
