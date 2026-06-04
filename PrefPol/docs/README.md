# PrefPol.jl Documentation

This directory contains local HTML documentation for the applied `PrefPol` Julia
package in this monorepo.

The publication-facing replication command is:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/run_all_paper.jl \
  --config PrefPol/config/publication.toml
```

`PrefPol/config/publication.toml` is the clean manuscript-facing entry point;
extended and diagnostic configs are secondary to article reproduction.

From the repository root, instantiate and build the docs with a Julia 1.11.9
executable:

```bash
ROOT=$(pwd)
(cd /tmp && julia --project="$ROOT/PrefPol/docs" -e 'using Pkg; Pkg.instantiate()')
(cd /tmp && julia --project="$ROOT/PrefPol/docs" "$ROOT/PrefPol/docs/make.jl")
```

Starting Julia outside the repository tree avoids shadowing Julia's internal
`Preferences` dependency with the local `Preferences/` package directory during
dependency resolution.

To browse the generated site locally, serve the build directory:

```bash
cd PrefPol/docs/build
python3 -m http.server --bind localhost 8001
```

Then open `http://localhost:8001`.
