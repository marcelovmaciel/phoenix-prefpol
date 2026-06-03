# Preferences.jl Documentation

This directory contains local HTML documentation for the `Preferences` Julia
package in this monorepo.

From the repository root, instantiate and build the docs with:

```bash
ROOT=$(pwd)
(cd /tmp && julia --project="$ROOT/Preferences/docs" -e 'using Pkg; Pkg.instantiate()')
(cd /tmp && julia --project="$ROOT/Preferences/docs" "$ROOT/Preferences/docs/make.jl")
```

Starting Julia outside the repository tree avoids shadowing Julia's internal
`Preferences` dependency with the local package directory during dependency
resolution. Use a Julia 1.11.9 executable for this package; with juliaup, that can be
spelled as `julia +1.11.9`.

To browse the generated site locally, serve the build directory:

```bash
cd Preferences/docs/build
python3 -m http.server --bind localhost 8000
```

Then open `http://localhost:8000`.
