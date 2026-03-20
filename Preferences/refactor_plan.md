# Refactor Plan

Goal: improve structure and extensibility for complete/incomplete preferences with a clear static-sized fast path.

## Task 1: Replace StrictRank shim with explicit accessors
- Subtask 1.1: Define `perm(x)` and `ranks(x)` as the public accessors
  - Subtask 1.1.1: Add `perm(::StrictRank)` and `ranks(::StrictRank)` in `src/PreferenceBallot.jl`
  - Subtask 1.1.2: Add `ranks(::WeakRank)` to standardize downstream usage
- Subtask 1.2: Remove the `Base.getproperty` shim
  - Subtask 1.2.1: Delete the compatibility block in `src/Preferences.jl`
  - Subtask 1.2.2: Update any internal calls that relied on `x.ranks`
- Subtask 1.3: Fix display to use accessors
  - Subtask 1.3.1: Update `Base.show(::StrictRank)` in `src/PreferenceDisplay.jl` to call `perm(x)`
  - Subtask 1.3.2: Add a small note in docs/examples that `perm`/`ranks` are the supported APIs

## Task 2: Parametric static/dynamic storage types
- Subtask 2.1: Generalize `CandidatePool` storage
  - Subtask 2.1.1: Change to `CandidatePool{N,Storage}` with `names::Storage`
  - Subtask 2.1.2: Preserve `SVector` for `N <= MAX_STATIC_N` and `Vector` otherwise
- Subtask 2.2: Parametrize ballots for static sizing
  - Subtask 2.2.1: Define `StrictRank{N,Storage}` and `WeakRank{N}` in `src/PreferenceBallot.jl`
  - Subtask 2.2.2: Make constructors infer `N` from `CandidatePool`
- Subtask 2.3: Keep a clear dynamic fallback
  - Subtask 2.3.1: Provide `StrictRankDyn` and `WeakRankDyn` type aliases
  - Subtask 2.3.2: Add simple `@inline` constructors to keep call sites clean

## Task 3: Clarify pairwise type naming and interface
- Subtask 3.1: Introduce a unified interface
  - Subtask 3.1.1: Add `abstract type AbstractPairwise end` in a dedicated file
  - Subtask 3.1.2: Provide `score`, `isdefined`, and `dense` as the shared API surface
- Subtask 3.2: Rename dense and triangular pairwise types
  - Subtask 3.2.1: Rename `Pairwise` to `PairwiseDense`
  - Subtask 3.2.2: Rename `PairwiseBallot*` to `PairwiseTriangular*`
- Subtask 3.3: Maintain compatibility for existing names
  - Subtask 3.3.1: Add deprecation aliases in `src/Compat.jl`
  - Subtask 3.3.2: Update exports in `src/Preferences.jl`

## Task 4: Tighten module layering for policies
- Subtask 4.1: Move the policy type
  - Subtask 4.1.1: Move `abstract type ExtensionPolicy end` to `src/PreferencePolicy.jl`
  - Subtask 4.1.2: Update imports and references in pairwise builders
- Subtask 4.2: Localize policy protocol docs
  - Subtask 4.2.1: Document `compare_maybe` signature in `src/PreferencePolicy.jl`
  - Subtask 4.2.2: Remove redundant comments in `src/PreferenceBallot.jl`
- Subtask 4.3: Ensure policy dependencies are one-way
  - Subtask 4.3.1: Make `PreferenceBallot` depend on `PreferencePolicy` for policy types only
  - Subtask 4.3.2: Keep `PreferencePolicy` free of ballot-specific code

## Task 5: Add a minimal preference traits interface
- Subtask 5.1: Create a traits file
  - Subtask 5.1.1: Add `src/PreferenceTraits.jl`
  - Subtask 5.1.2: Define `is_complete`, `is_strict`, `is_weak_order`, `is_transitive`
- Subtask 5.2: Implement traits for core ballot types
  - Subtask 5.2.1: Provide trait methods for `StrictRank` and `WeakRank`
  - Subtask 5.2.2: Add traits for pairwise types where sensible
- Subtask 5.3: Use traits in algorithms
  - Subtask 5.3.1: Route strict-only algorithms through `is_strict`
  - Subtask 5.3.2: Route complete-only algorithms through `is_complete`

## Task 6: Isolate display dependencies behind an extension
- Subtask 6.1: Split core vs. display
  - Subtask 6.1.1: Keep basic `Base.show` in `src/PreferenceDisplay.jl`
  - Subtask 6.1.2: Move `PrettyTables` and `Crayons` usage to an extension file
- Subtask 6.2: Add extension layout
  - Subtask 6.2.1: Create `ext/PreferencesPrettyTablesExt.jl` for rich tables
  - Subtask 6.2.2: Guard extension load via `Project.toml` extras if needed
- Subtask 6.3: Update exports and documentation
  - Subtask 6.3.1: Export rich display helpers only when extension is available
  - Subtask 6.3.2: Note optional dependency usage in `README` or module docs
