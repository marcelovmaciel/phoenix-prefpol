
# ─────────────────────────────────────────────────────────────────────────────
# Scenario inside a TOML file
# ─────────────────────────────────────────────────────────────────────────────
"""
    Scenario

A named candidate subset used to **force-include** certain candidates when
constructing profiles. Scenarios come from the election TOML and are used in
`generate_profiles_for_year*` to pin the candidate set logic for each plot/run.

# Fields
- `name::String`: Scenario identifier (e.g., "ideology", "front_runners").
- `candidates::Vector{String}`: Ordered list of candidate *ids/names* that must
  be present (and ordered first when trimming to `m` alternatives).
"""
struct Scenario
    name       :: String
    candidates :: Vector{String}
end

# ─────────────────────────────────────────────────────────────────────────────
# FULL election specification (= everything in the TOML)
# ─────────────────────────────────────────────────────────────────────────────
"""
    ElectionConfig

Full configuration parsed from the TOML for a given election year.

This struct travels alongside bootstrap data throughout the pipeline and encodes
how to load raw data, which candidates/demographics to keep, bootstrap settings,
and the list of `Scenario`s to generate.

# Fields
- `year::Int`: Election year.
- `data_loader::String`: Name of a loader function in the current module.
- `data_file::String`: Absolute path to the raw data file.
- `max_candidates::Int`: Upper bound for candidate set discovery.
- `m_values_range::Vector{Int}`: Values of `m` (number of alternatives) to plot.
- `n_bootstrap::Int`: Number of bootstrap replicates (default; can be overridden).
- `n_alternatives::Int`: Number of alternatives expected by some loaders.
- `rng_seed::Int`: Seed for reproducibility.
- `candidates::Vector{String}`: All candidate columns to consider.
- `demographics::Vector{String}`: Demographic columns used for grouping.
- `scenarios::Vector{Scenario}`: List of forced scenarios from the TOML.
"""
struct ElectionConfig
    year            :: Int
    data_loader     :: String
    data_file       :: String
    max_candidates  :: Int
    m_values_range  :: Vector{Int}

    n_bootstrap     :: Int          # default; can be overridden later
    n_alternatives  :: Int
    rng_seed        :: Int

    candidates      :: Vector{String}
    demographics    :: Vector{String}
    scenarios       :: Vector{Scenario}    # list of Scenario structs
end

function _validate_candidate_count(m::Integer,
                                   max_candidates::Integer;
                                   context::AbstractString,
                                   min_allowed::Integer = 1)
    mm = Int(m)
    maxc = Int(max_candidates)

    maxc >= min_allowed || throw(ArgumentError(
        "$context configured max_candidates=$maxc, but at least $min_allowed candidates are required.",
    ))
    mm >= min_allowed || throw(ArgumentError(
        "$context requires m >= $min_allowed, got $mm.",
    ))
    mm <= maxc || throw(ArgumentError(
        "$context requested m=$mm, but max_candidates=$maxc.",
    ))

    return mm
end

function _validate_election_cfg(cfg::ElectionConfig)
    cfg.max_candidates >= 2 || throw(ArgumentError(
        "Election year $(cfg.year) must set max_candidates >= 2; got $(cfg.max_candidates).",
    ))

    cfg.n_alternatives == cfg.max_candidates || throw(ArgumentError(
        "Election year $(cfg.year) must set n_alternatives == max_candidates to keep the default candidate count unambiguous; " *
        "got n_alternatives=$(cfg.n_alternatives), max_candidates=$(cfg.max_candidates).",
    ))

    !isempty(cfg.m_values_range) || throw(ArgumentError(
        "Election year $(cfg.year) must provide a nonempty m_values_range.",
    ))
    cfg.m_values_range == sort(unique(cfg.m_values_range)) || throw(ArgumentError(
        "Election year $(cfg.year) must provide a sorted, unique m_values_range; got $(cfg.m_values_range).",
    ))
    invalid_m_values = [m for m in cfg.m_values_range if m < 2 || m > cfg.max_candidates]
    isempty(invalid_m_values) || throw(ArgumentError(
        "Election year $(cfg.year) has m_values_range values outside 2:max_candidates ($(cfg.max_candidates)): $(invalid_m_values).",
    ))

    length(cfg.candidates) >= cfg.max_candidates || throw(ArgumentError(
        "Election year $(cfg.year) declares max_candidates=$(cfg.max_candidates), but only $(length(cfg.candidates)) candidates are listed.",
    ))
    length(unique(cfg.candidates)) == length(cfg.candidates) || throw(ArgumentError(
        "Election year $(cfg.year) contains duplicate candidate names.",
    ))

    scenario_names = [sc.name for sc in cfg.scenarios]
    length(unique(scenario_names)) == length(scenario_names) || throw(ArgumentError(
        "Election year $(cfg.year) contains duplicate scenario names: $(scenario_names).",
    ))

    universe = Set(cfg.candidates)
    for sc in cfg.scenarios
        unique_candidates = unique(sc.candidates)
        length(unique_candidates) == length(sc.candidates) || throw(ArgumentError(
            "Scenario `$(sc.name)` for year $(cfg.year) contains duplicate forced candidates.",
        ))
        length(sc.candidates) <= cfg.max_candidates || throw(ArgumentError(
            "Scenario `$(sc.name)` for year $(cfg.year) forces $(length(sc.candidates)) candidates, exceeding max_candidates=$(cfg.max_candidates).",
        ))
        missing_candidates = setdiff(sc.candidates, universe)
        isempty(missing_candidates) || throw(ArgumentError(
            "Scenario `$(sc.name)` for year $(cfg.year) references unknown candidates $(missing_candidates).",
        ))
    end

    return cfg
end

"""
    load_election_cfg(path) -> ElectionConfig

Parse a TOML file describing an election and return an `ElectionConfig`.

- Resolves `data_file` relative to the **project root** unless already absolute.
- Converts `forced_scenarios` into a vector of `Scenario` objects.

This is the canonical entry point for configuration used by bootstrap and
profile generation.
"""
function load_election_cfg(path::AbstractString)::ElectionConfig
    t = TOML.parsefile(path)
    rawfile = isabspath(t["data_file"]) ? t["data_file"] :
              joinpath(project_root, t["data_file"])

    scen_vec = [Scenario(s["name"], Vector{String}(s["candidates"]))
                for s in t["forced_scenarios"]]

    cfg = ElectionConfig(
        t["year"], t["data_loader"], rawfile,
        t["max_candidates"], Vector{Int}(t["m_values_range"]),
        t["n_bootstrap"], t["n_alternatives"],
        t["rng_seed"],
        Vector{String}(t["candidates"]),
        Vector{String}(t["demographics"]),
        scen_vec,
    )

    return _validate_election_cfg(cfg)
end

import Base: show

const IND = "    "                       # 4-space indent

# ────────────────────────── helpers ──────────────────────────
"""
    _pp(io, key, val, lvl)

Pretty-print a single `key = value` line with indentation level `lvl`.
Internal helper used by the custom `show` methods below.
"""
_pp(io, key, val, lvl) = println(io, repeat(IND, lvl), key, " = ", val)

"""
    _pp_vec(io, key, vec, lvl; max=8)

Pretty-print up to the first `max` elements of `vec` as `key = [...]`.
Appends an elision marker with total length when the vector is longer.
"""
function _pp_vec(io, key, vec, lvl; max=8)
    head = first(vec, max)
    tail = length(vec) > max ? " … ("*string(length(vec))*")" : ""
    println(io, repeat(IND, lvl), key, " = ", head, tail)
end

# ────────────────────────── Scenario ─────────────────────────
"""
    show(io, ::MIME"text/plain", s::Scenario)

Pretty printer for `Scenario`, shown as `Scenario("name", [c1, c2, …])`.
"""
function show(io::IO, ::MIME"text/plain", s::Scenario; kwargs...)
    print(io, "Scenario(\"", s.name, "\", ", s.candidates, ")")
end

# ─────────────────────── ElectionConfig ──────────────────────
"""
    show(io, ::MIME"text/plain", ec::ElectionConfig)

Multi-line pretty printer for `ElectionConfig`, including a compact preview of
candidate and scenario lists.
"""
function show(io::IO, ::MIME"text/plain", ec::ElectionConfig; kwargs...)
    println(io, "ElectionConfig(")
    _pp(io, "year",             ec.year, 1)
    _pp(io, "data_loader",      ec.data_loader, 1)
    _pp(io, "data_file",        ec.data_file, 1)
    _pp(io, "max_candidates",   ec.max_candidates, 1)
    _pp(io, "m_values_range",   ec.m_values_range, 1)
    _pp(io, "n_bootstrap(def)", ec.n_bootstrap, 1)
    _pp(io, "n_alternatives",   ec.n_alternatives, 1)
    _pp(io, "rng_seed(def)",    ec.rng_seed, 1)
    _pp_vec(io, "candidates",   ec.candidates, 1)
    _pp(io, "demographics",     ec.demographics, 1)
    println(io, IND, "scenarios = [")
    for sc in ec.scenarios
        println(io, repeat(IND,2), sc)           # uses Scenario show
    end
    println(io, IND, "]")
    print(io, ")")
end

"""
    load_election_data(cfg) -> DataFrame

Resolve and call the configured `data_loader` in the **current module**,
returning a `DataFrame` with candidate and demographic columns. The loader is
looked up dynamically by name (`cfg.data_loader`) and called as

```julia
loader_fun(cfg.data_file; candidates = cfg.candidates)
```
"""
function load_election_data(cfg::ElectionConfig)
    loader_sym = Symbol(cfg.data_loader)

    # resolve in the current module’s namespace
    if !isdefined(@__MODULE__, loader_sym)
        throw(ArgumentError("data_loader ‘$(cfg.data_loader)’ not found in module $(nameof(@__MODULE__))"))
    end

    loader_fun = getfield(@__MODULE__, loader_sym)
    return loader_fun(cfg.data_file; candidates = cfg.candidates)
end

"""
    weighted_bootstrap(cfg::ElectionConfig) -> Dict{Symbol,Vector{DataFrame}}

Load raw election data via `load_election_data(cfg)`, slice to the configured
candidate and demographic columns, and run weighted bootstrapping using the
`peso` column as replicate weights. Returns a dictionary of bootstrap
replicates per imputation variant **only if** the underlying `weighted_bootstrap`
method dispatches on `(df, weights, B)`; here we simply delegate.
"""
function weighted_bootstrap(ecfg::ElectionConfig)
    df = load_election_data(ecfg)
    weights = df.peso
    B = ecfg.n_bootstrap
    # slice df to only candidates and demographics from ecfg
    candidates = ecfg.candidates
    demographics = ecfg.demographics
    df = select(df, candidates..., demographics...)
    bts = weighted_bootstrap(df, weights, B)
    return bts
end

const INT_DIR = normpath(joinpath(project_root, "intermediate_data"))
mkpath(INT_DIR)

const _LEGACY_PIPELINE_WARNED_APIS = Set{Symbol}()

function _warn_legacy_pipeline_api(api::Symbol, replacement::AbstractString)
    if !(api in _LEGACY_PIPELINE_WARNED_APIS)
        push!(_LEGACY_PIPELINE_WARNED_APIS, api)
        @warn string(
            api,
            " uses the legacy year-level pipeline and cache layout. Prefer ",
            replacement,
            " for fixed-spec nested runs.",
        )
    end

    return nothing
end

function _error_legacy_pipeline_api(api::Symbol, replacement::AbstractString)
    throw(ArgumentError(string(
        api,
        " belongs to the retired year-level pipeline and has been disabled. Use ",
        replacement,
        " instead.",
    )))
end

const CANDIDATE_SET_DIR = joinpath(INT_DIR, "candidate_sets")
mkpath(CANDIDATE_SET_DIR)

function _candidate_weight_col(df::DataFrame)
    for nm in (:peso, :weight, :weights)
        hasproperty(df, nm) && return nm
    end
    throw(ArgumentError("Could not find a survey weight column. Expected one of :peso, :weight or :weights."))
end

function _lookup_scenario(cfg::ElectionConfig, scenario_name)
    sname = String(scenario_name)
    for sc in cfg.scenarios
        sc.name == sname && return sc
    end
    available = [sc.name for sc in cfg.scenarios]
    throw(ArgumentError(
        "Unknown scenario `$sname` for year $(cfg.year). Available scenarios: $(available).",
    ))
end

"""
    compute_global_candidate_list(cfg; scenario=nothing, m=cfg.max_candidates, data=nothing)

Compute the deterministic year-level candidate ordering from the raw weighted
survey table. When `scenario` is provided, its forced candidates are pinned to
the front of the list before filling the remaining slots by weighted
missingness.
"""
function compute_global_candidate_list(cfg::ElectionConfig;
                                       scenario = nothing,
                                       m::Int = cfg.max_candidates,
                                       data = nothing)
    resolved_m = _validate_candidate_count(
        m,
        cfg.max_candidates;
        context = "Candidate-set resolution for year $(cfg.year)",
        min_allowed = 1,
    )
    df = data === nothing ? load_election_data(cfg) : data

    force_include = if scenario === nothing
        String[]
    elseif scenario isa Scenario
        Vector{String}(scenario.candidates)
    else
        Vector{String}(_lookup_scenario(cfg, scenario).candidates)
    end

    weight_col = _candidate_weight_col(df)
    return compute_global_candidate_set(df;
                                        candidate_cols = cfg.candidates,
                                        m = resolved_m,
                                        force_include = force_include,
                                        weights = df[!, weight_col])
end

"""
    compute_global_candidate_sets(cfg; data=nothing)

Precompute the year-level ordered candidate list for every configured scenario.
"""
function compute_global_candidate_sets(cfg::ElectionConfig; data = nothing)
    df = data === nothing ? load_election_data(cfg) : data
    out = OrderedDict{String,Vector{String}}()

    for scen in cfg.scenarios
        out[scen.name] = compute_global_candidate_list(cfg;
                                                       scenario = scen,
                                                       m = cfg.max_candidates,
                                                       data = df)
    end

    return out
end

"""
    save_or_load_candidate_sets_for_year(cfg; dir=CANDIDATE_SET_DIR, overwrite=false, verbose=true, data=nothing)

Persist the deterministic year-level candidate ordering for each scenario and
reuse it across downstream profile and plotting stages.
"""
function save_or_load_candidate_sets_for_year(cfg::ElectionConfig;
                                              dir::AbstractString = CANDIDATE_SET_DIR,
                                              overwrite::Bool = false,
                                              verbose::Bool = true,
                                              data = nothing)
    _error_legacy_pipeline_api(
        :save_or_load_candidate_sets_for_year,
        "`resolve_active_candidate_set`, `SurveyWaveConfig`, and `build_pipeline_spec`",
    )

    path = joinpath(dir, "candidate_sets_$(cfg.year).jld2")

    if isfile(path) && !overwrite
        verbose && @warn "Candidate sets for $(cfg.year) already exist at $path; loading cache."
        candidate_sets = nothing
        @load path candidate_sets
        return candidate_sets
    end

    verbose && @info "Computing global candidate sets for year $(cfg.year)…"
    candidate_sets = compute_global_candidate_sets(cfg; data = data)
    @save path candidate_sets
    verbose && @info "Saved candidate sets for year $(cfg.year) → $path"
    return candidate_sets
end

function _resolve_year_candidate_sets(cfg::ElectionConfig; candidate_sets = nothing)
    return candidate_sets === nothing ?
           save_or_load_candidate_sets_for_year(cfg; verbose = false) :
           candidate_sets
end

function _full_candidate_list(candidate_sets, scen::Scenario)
    haskey(candidate_sets, scen.name) || throw(ArgumentError(
        "Missing candidate list for scenario `$(scen.name)` in year-level cache.",
    ))
    return Vector{String}(candidate_sets[scen.name])
end

function _trim_candidate_list(full_list::Vector{String}, m::Int;
                              year::Int,
                              scenario::AbstractString)
    if length(full_list) < m
        @warn "Year $year, scenario $scenario: only $(length(full_list)) candidates available; requested $m."
        return Symbol.(full_list)
    end
    return Symbol.(first(full_list, m))
end


"""
    save_bootstrap(cfg; dir = INT_DIR, overwrite = false, quiet = false)
        → NamedTuple{(:path,:data,:cached), ...}

Ensure a weighted-bootstrap exists for `cfg`:

  • If `dir/boot_YEAR.jld2` is missing *or* `overwrite=true`, build the
    bootstrap with `weighted_bootstrap(cfg)` and write it to disk.

  • Otherwise reuse the cached file, **loading the replicates** so that
    `.data` is never `nothing`.

Returned fields
---------------
| field   | meaning                                   |
|---------|-------------------------------------------|
| `path`  | full path to the `.jld2` file             |
| `data`  | the `reps` object (always in memory)      |
| `cached`| `true` if we reused an existing file      |
"""
function save_bootstrap(cfg::ElectionConfig;
                        dir::AbstractString = INT_DIR,
                        overwrite::Bool = false,
                        quiet::Bool = false)
    _error_legacy_pipeline_api(
        :save_bootstrap,
        "`SurveyWaveConfig`, `NestedStochasticPipeline`, and `run_pipeline`",
    )

    path = joinpath(dir, "boot_$(cfg.year).jld2")

    # ------------------ cache hit ------------------
    if !overwrite && isfile(path)
        !quiet && @warn "Reusing cached bootstrap at $(path); loading into memory"
        reps = nothing
        @load path reps                           # brings `reps` back
        return (path = path, data = reps, cached = true)
    end

    # ------------------ (re)build ------------------
    reps = weighted_bootstrap(cfg)                # heavy call
    @save path reps cfg
    !quiet && @info "Saved bootstrap for year $(cfg.year) → $(path)"
    return (path = path, data = reps, cached = false)
end



"""
    save_all_bootstraps(; years = nothing,
                            cfgdir = "config",
                            overwrite = false) -> Dict{Int,String}

Iterate over every `*.toml` in `cfgdir`; for each year that matches
`years` (or *all* years if `years === nothing`) build & save the bootstrap.

Returns a dictionary `year ⇒ saved_filepath`.
"""
function save_all_bootstraps(; years = nothing,
                             cfgdir::AbstractString = "config",
                             overwrite::Bool = false)
    _error_legacy_pipeline_api(
        :save_all_bootstraps,
        "`SurveyWaveConfig`, `StudyBatchSpec`, and `run_batch`",
    )

    # discover configs on disk
    toml_files = filter(p -> endswith(p, ".toml"), readdir(cfgdir; join=true))
    isempty(toml_files) && error("No TOML files found in $(cfgdir)")

    wanted = years === nothing        ? nothing           :
             isa(years, Integer)      ? Set([years])      :
             Set(years)

    saved = Dict{Int,String}()

    for f in sort(toml_files)
        cfg = load_election_cfg(f)
        (wanted !== nothing && !(cfg.year in wanted)) && continue
        @info "Processing year $(cfg.year) from $(f)"
        saved[cfg.year] = save_bootstrap(cfg; overwrite).path
    end
    return saved
end

"""
    load_all_bootstraps(; years = nothing,
                           dir   = INT_DIR,
                           quiet = false)
        → OrderedDict{Int,NamedTuple}

Read every `boot_YYYY.jld2` in `dir` (or just the chosen `years`)
and return them in a year-sorted `OrderedDict`.

Each value is a `NamedTuple` with

| field   | meaning                     |
|---------|-----------------------------|
| `data`  | the bootstrap replicates    |
| `cfg`   | the `ElectionConfig` object |
| `path`  | full path to the file       |
"""
function load_all_bootstraps(; years   = nothing,
                             dir::AbstractString = INT_DIR,
                             quiet::Bool = false)
    _error_legacy_pipeline_api(
        :load_all_bootstraps,
        "`SurveyWaveConfig`, `NestedStochasticPipeline`, and `load_pipeline_result`",
    )

    paths = filter(p -> occursin(r"boot_\d+\.jld2$", p),
                   readdir(dir; join = true))

    isempty(paths) && error("No bootstrap files found in $(dir)")

    selected = years === nothing       ? nothing :
               isa(years,Integer)      ? Set([years]) :
               Set(years)

    out = OrderedCollections.OrderedDict{Int,NamedTuple}()

    for f in sort(paths)                       # alphabetical = chronological
        yr = parse(Int, match(r"boot_(\d{4})\.jld2", basename(f)).captures[1])
        (selected !== nothing && !(yr in selected)) && continue

        reps = cfg = nothing
        @load f reps cfg

        !quiet && @info "Loaded bootstrap $(yr)  ←  $(f)"

        out[yr] = (data = reps, cfg = cfg, path = f)
    end
    return out
end

"""
    ImputedYear

Small index object that records, for one `year`, where each imputed replicate
is stored on disk for each variant.

# Fields
- `year::Int`
- `paths::Dict{Symbol,Vector{String}}`: e.g., `:mice => ["imp_2022_rep1_mice.jld2", …]`
"""
struct ImputedYear
    year::Int
    # Dict(:zero => [path1, path2, …], :random => …, :mice => …)
    paths::Dict{Symbol,Vector{String}}
end

"""
    getrep(iy::ImputedYear, variant::Symbol, i::Int) -> DataFrame

Load the *i*-th replicate of `variant` for that year.
"""
function getrep(iy::ImputedYear, variant::Symbol, i::Int)
    p = iy.paths[variant][i]
    df = nothing; @load p df        # `df` is how we store it below
    return df
end

Base.getindex(iy::ImputedYear, variant::Symbol, i::Int) = getrep(iy, variant, i)

function _select_available_imputation_variants(available, variants; context::AbstractString = "available data")
    requested = normalize_imputation_variants(variants)
    available_set = Set(Symbol(var) for var in available)
    selected = Tuple(var for var in requested if var in available_set)

    isempty(selected) || return selected
    throw(ArgumentError(
        "Requested imputation variants $(collect(requested)) are unavailable in $(context). " *
        "Available variants: $(collect(Symbol(var) for var in available)).",
    ))
end

function _index_covers_variants(index::ImputedYear, variants, nboot::Int)
    for var in variants
        haskey(index.paths, var) || return false
        paths = index.paths[var]
        length(paths) == nboot || return false
        all(isfile, paths) || return false
    end
    return true
end

const IMP_DATA_DIR = joinpath(INT_DIR, "imputed_data"); mkpath(IMP_DATA_DIR)

"""
    impute_bootstrap_to_files(path_boot;
                              imp_dir=IMP_DATA_DIR,
                              overwrite=false,
                              most_known_candidates=String[],
                              variants=DEFAULT_PIPELINE_IMPUTATION_VARIANTS) -> String

Run the requested imputation variants for every bootstrap replicate stored in
`path_boot::String`, save each imputed DataFrame to JLD2, and write a compact
`ImputedYear` index (`index_YEAR.jld2`). Returns the index file path.
"""
function impute_bootstrap_to_files(path_boot::String;
                                   imp_dir::AbstractString = IMP_DATA_DIR,
                                   overwrite::Bool         = false,
                                   most_known_candidates   = String[],
                                   variants                = DEFAULT_PIPELINE_IMPUTATION_VARIANTS)
    _error_legacy_pipeline_api(
        :impute_bootstrap_to_files,
        "`run_pipeline` with explicit `R` and `imputer_backend` settings",
    )

    reps = cfg = nothing
    @load path_boot reps cfg                 # same vars saved by save_bootstrap
    year = cfg.year
    var_syms = normalize_imputation_variants(variants)

    # ---------------- per-variant path collectors -----------------
    paths_dict = Dict(var => Vector{String}(undef, length(reps)) for var in var_syms)

    for (i, df_raw) in enumerate(reps)
        imp = imputation_variants(df_raw, cfg.candidates, cfg.demographics;
                                  most_known_candidates = most_known_candidates,
                                  variants = var_syms)

        for var in var_syms
            file = joinpath(imp_dir,
                    "imp_$(year)_rep$(i)_$(String(var)).jld2")
            if !overwrite && isfile(file)
                @warn "reusing $(file)"
            else
                df = imp[var]                                # DataFrame
                @save file df
            end
            paths_dict[var][i] = file
        end

        # ---------- free memory for this replicate ----------
        imp = nothing
        df_raw = nothing
        GC.gc()
    end

    # tiny index object
    index = ImputedYear(year, paths_dict)
    ind_file = joinpath(imp_dir, "index_$(year).jld2")
    @save ind_file index

    @info "Finished imputation for $(year) → $(ind_file)"
    return ind_file
end

"""
    impute_all_bootstraps(; years=nothing, base_dir=INT_DIR, imp_dir=IMP_DATA_DIR,
                           overwrite=false, most_known_candidates=String[],
                           variants=DEFAULT_PIPELINE_IMPUTATION_VARIANTS)
        -> OrderedDict{Int,String}

Batch version of `impute_bootstrap_to_files` over all `boot_YYYY.jld2` found in
`base_dir`, optionally filtered by `years`. Returns `year => index_path`.
"""
function impute_all_bootstraps(; years = nothing,
                               base_dir = INT_DIR,
                               imp_dir  = IMP_DATA_DIR,
                               overwrite = false,
                               most_known_candidates = String[],
                               variants = DEFAULT_PIPELINE_IMPUTATION_VARIANTS)
    _error_legacy_pipeline_api(
        :impute_all_bootstraps,
        "`StudyBatchSpec` plus `run_batch` over explicit `PipelineSpec`s",
    )

    rx = r"boot_(\d{4})\.jld2$"
    files = filter(p -> occursin(rx, basename(p)), readdir(base_dir; join=true))
    isempty(files) && error("No bootstrap files found in $(base_dir)")

    wanted = years === nothing ? nothing :
             isa(years,Integer) ? Set([years]) : Set(years)

    worklist = Tuple{Int,String}[]
    for p in files
        yr = parse(Int, match(rx, basename(p)).captures[1])
        (wanted !== nothing && yr ∉ wanted) && continue
        push!(worklist, (yr, p))
    end
    sort!(worklist; by = first)

    prog = pm.Progress(length(worklist); desc = "Imputing bootstraps", barlen = 30)
    out  = OrderedDict{Int,String}()

    for (yr, p) in worklist
        @info "Imputing year $yr …"
        out[yr] = impute_bootstrap_to_files(p;
                     imp_dir = imp_dir,
                     overwrite = overwrite,
                     most_known_candidates = most_known_candidates,
                     variants = variants)
        GC.gc()
        pm.next!(prog)
    end
    return out
end

"""
    _impute_year_to_files(reps, cfg; imp_dir=IMP_DATA_DIR, overwrite=false,
                          most_known_candidates=String[],
                          variants=DEFAULT_PIPELINE_IMPUTATION_VARIANTS) -> String

Internal worker that mirrors `impute_bootstrap_to_files`, but receives
in‑memory bootstrap `reps::Vector{DataFrame}` and an `ElectionConfig` instead
of a `boot_YYYY.jld2` file. Returns the written index path.
"""
function _impute_year_to_files(reps::Vector{DataFrame},
                               cfg::ElectionConfig;
                               imp_dir::AbstractString = IMP_DATA_DIR,
                               overwrite::Bool = false,
                               most_known_candidates = String[],
                               variants = DEFAULT_PIPELINE_IMPUTATION_VARIANTS)
    _error_legacy_pipeline_api(
        :_impute_year_to_files,
        "`run_pipeline` with explicit `R` and `imputer_backend` settings",
    )

    year      = cfg.year
    idxfile   = joinpath(imp_dir, "index_$(year).jld2")
    variant_syms = normalize_imputation_variants(variants)
    nboot     = length(reps)

    # ────────────────────────────────────────────────────────────────────
    # 1. Fast path: cached index exists
    # ────────────────────────────────────────────────────────────────────
    if !overwrite && isfile(idxfile)
        index = JLD2.load(idxfile, "index")
        if index isa ImputedYear && _index_covers_variants(index, variant_syms, nboot)
            @info "Reusing cached imputed index for year $year → $(idxfile)"
            return idxfile
        end
        @info "Cached imputed index for year $year does not cover requested variants; rebuilding."
    end

    # helper that lists all expected replicate files
    expected_path(var, i) = joinpath(
        imp_dir, "imp_$(year)_rep$(i)_$(String(var)).jld2")

    # ────────────────────────────────────────────────────────────────────
    # 2.  Could we build the index without recomputation?
    # ────────────────────────────────────────────────────────────────────
    if !overwrite
        all_exist = true
        for i in 1:nboot, var in variant_syms
            isfile(expected_path(var, i)) || (all_exist = false; break)
        end
        if all_exist
            paths = Dict(var => [expected_path(var, i) for i in 1:nboot]
                         for var in variant_syms)
            index = ImputedYear(year, paths)
            @save idxfile index
            @info "Rebuilt index for year $year without re-imputation."
            return idxfile
        end
    end

    # ────────────────────────────────────────────────────────────────────
    # 3.  Run full imputation (some files missing or overwrite=true)
    # ────────────────────────────────────────────────────────────────────
    @info "Running imputation for year $year …"
    paths = Dict(var => Vector{String}(undef, nboot) for var in variant_syms)

    for i in 1:nboot
        df_raw = reps[i]
        imp    = imputation_variants(df_raw,
                                     cfg.candidates,
                                     cfg.demographics;
                                     most_known_candidates = most_known_candidates,
                                     variants = variant_syms)

        for var in variant_syms
            file = expected_path(var, i)
            if overwrite || !isfile(file)
                df = imp[var]
                @save file df
            end
            paths[var][i] = file
        end

        imp    = df_raw = nothing        # local cleanup
        GC.gc()
    end

    index = ImputedYear(year, paths)
    @save idxfile index
    @info "Saved imputed index for year $year → $(idxfile)"
    return idxfile
end

# ---------------------------------------------------------------------
# 2 · top-level driver starting from your in-memory `f3`
# ---------------------------------------------------------------------
"""
    impute_from_f3(f3; years=nothing, imp_dir=IMP_DATA_DIR, overwrite=false,
                   most_known_candidates=String[],
                   variants=DEFAULT_PIPELINE_IMPUTATION_VARIANTS) -> OrderedDict{Int,String}

Top‑level driver when you already have the `f3` object in memory. For each
requested `year`, runs `_impute_year_to_files` and returns `year => index_path`.
"""
function impute_from_f3(f3::OrderedDict;
                        years = nothing,
                        imp_dir::AbstractString = IMP_DATA_DIR,
                        overwrite::Bool = false,
                        most_known_candidates = String[],
                        variants = DEFAULT_PIPELINE_IMPUTATION_VARIANTS)
    _error_legacy_pipeline_api(
        :impute_from_f3,
        "`StudyBatchSpec` plus `run_batch` over explicit `PipelineSpec`s",
    )

    wanted = years === nothing        ? sort(collect(keys(f3))) :
             isa(years,Integer)       ? [years]                 :
             sort(collect(years))

    prog = pm.Progress(length(wanted); desc = "Imputing bootstraps", barlen = 30)
    out  = OrderedDict{Int,String}()

    for yr in wanted
        entry = f3[yr]                       # (data = reps, cfg = cfg, path = …)
        reps  = entry.data
        cfg   = entry.cfg

        @info "Imputing year $yr …"
        out[yr] = _impute_year_to_files(reps, cfg;
                                        imp_dir    = imp_dir,
                                        overwrite  = overwrite,
                                        most_known_candidates = most_known_candidates,
                                        variants = variants)

        GC.gc()                             # reclaim before next year
        pm.next!(prog)
    end
    return out
end

const IMP_PREFIX  = "boot_imp_"   # change here if you rename files
const IMP_DIR     = INT_DIR       # default directory to look in

# ————————————————————————————————————————————————————————————————
# 1.  Single-year loader
# ————————————————————————————————————————————————————————————————
"""
    load_imputed_bootstrap(year;
                           dir   = IMP_DIR,
                           quiet = false)  -> NamedTuple

Load `dir/boot_imp_YEAR.jld2` and return the stored NamedTuple
`(data = Dict, cfg = ElectionConfig, path = String)`.
"""
function load_imputed_bootstrap(year::Integer;
                                dir::AbstractString = IMP_DIR,
                                quiet::Bool = false)
    _error_legacy_pipeline_api(
        :load_imputed_bootstrap,
        "`NestedStochasticPipeline`, `run_pipeline`, and `load_pipeline_result`",
    )

    path = joinpath(dir, "$(IMP_PREFIX)$(year).jld2")
    isfile(path) || error("File not found: $(path)")

    imp = nothing
    @load path imp
    !quiet && @info "Loaded imputed bootstrap for year $(year) ← $(path)"
    return imp
end

"""
    load_imputed_year(year; dir=IMP_DATA_DIR) -> ImputedYear

Load the `index_YEAR.jld2` produced by imputation and return the `ImputedYear`
object for convenient access to per‑variant replicate paths.
"""
function load_imputed_year(year::Int;
                           dir::AbstractString = IMP_DATA_DIR)::ImputedYear
    _error_legacy_pipeline_api(
        :load_imputed_year,
        "`NestedStochasticPipeline`, `run_pipeline`, and `load_pipeline_result`",
    )
    idxfile = joinpath(dir, "index_$(year).jld2")
    isfile(idxfile) || error("index file not found: $(idxfile)")
    return JLD2.load(idxfile, "index")    # returns ImputedYear struct
end

# TODO: later, write a variant that takes just imp and config
# and loads f3 from disk, cakculate the sets, cleans it from disk, and proceeds

"""
    generate_profiles_for_year(year, f3_entry, imps_entry) -> OrderedDict

For each `Scenario` and each `m ∈ cfg.m_values_range`, build a weak-profile
`DataFrame` for every imputation variant and replicate:
- load the precomputed year-level candidate ordering for the scenario,
- build `profile_dataframe(...; kind = :weak)` and attach candidate metadata.

Returns a nested `OrderedDict`:
`scenario ⇒ m ⇒ (variant ⇒ Vector{AnnotatedProfile})`.
"""
function generate_profiles_for_year(year::Int,
                                    f3_entry::NamedTuple,
                                    imps_entry::NamedTuple;
                                    candidate_sets = nothing,
                                    variants = DEFAULT_PIPELINE_IMPUTATION_VARIANTS)
    _error_legacy_pipeline_api(
        :generate_profiles_for_year,
        "`run_pipeline` and `pipeline_panel_table` on `PipelineResult`s",
    )

    cfg            = f3_entry.cfg
    variants_dict  = imps_entry.data
    variant_syms   = _select_available_imputation_variants(keys(variants_dict), variants;
                                                           context = "imputed profiles for year $year")
    m_values       = cfg.m_values_range
    year_csets     = _resolve_year_candidate_sets(cfg; candidate_sets = candidate_sets)

    result = OrderedDict{String,OrderedDict{Int,OrderedDict{Symbol,Vector{AnnotatedProfile}}}}()

    for scen in cfg.scenarios
        full_list = _full_candidate_list(year_csets, scen)

        m_map = OrderedDict{Int,OrderedDict{Symbol,Vector{AnnotatedProfile}}}()

        for m in m_values
            trimmed  = _trim_candidate_list(full_list, m; year = year, scenario = scen.name)
            var_map  = OrderedDict{Symbol,Vector{AnnotatedProfile}}()

            for variant in variant_syms
                reps_imp = variants_dict[variant]
                profiles = Vector{AnnotatedProfile}(undef, length(reps_imp))

                for (i, df_imp) in enumerate(reps_imp)
                    df = profile_dataframe(
                             df_imp;
                             score_cols = trimmed,
                             demo_cols  = cfg.demographics,
                             kind       = :weak)
                    metadata!(df, "candidates", Symbol.(trimmed))
                    metadata!(df, "profile_kind", "weak")

                    profiles[i] = dataframe_to_annotated_profile(df; ballot_kind = :weak)
                end
                var_map[variant] = profiles
            end
            m_map[m] = var_map
        end
        result[scen.name] = m_map
    end
    return result
end

# ─────────────────────────────────────────────────────────────────────────────
# directories / tiny types
# ─────────────────────────────────────────────────────────────────────────────
const PROFILES_DATA_DIR = joinpath(INT_DIR, "profiles_data")
mkpath(PROFILES_DATA_DIR)

const LINEARIZED_PROFILES_DATA_DIR = joinpath(INT_DIR, "linearized_profiles_data")
mkpath(LINEARIZED_PROFILES_DATA_DIR)

"""
    ProfilesSlice

Lightweight handle to per‑replicate profile files for a fixed `(year, scenario, m)`.

# Fields
- `year::Int`
- `scenario::String`
- `m::Int`
- `cand_list::Vector{Symbol}`: ordered candidate list used for later linearization
- `paths::Dict{Symbol,Vector{String}}`: `variant ⇒ replicate paths`
"""
struct ProfilesSlice
    year::Int
    scenario::String
    m::Int
    cand_list::Vector{Symbol}              # ordered for (en/de)coding
    paths::Dict{Symbol,Vector{String}}     # variant ⇒ file paths
end

"""
    LinearizedProfilesSlice

Lightweight handle to per-replicate linearized profile files for a fixed
`(year, scenario, m)`.
"""
struct LinearizedProfilesSlice
    year::Int
    scenario::String
    m::Int
    cand_list::Vector{Symbol}
    paths::Dict{Symbol,Vector{String}}
end

function _load_profile_artifact(path::AbstractString)
    payload = JLD2.load(path)

    if payload isa AbstractDict
        for key in ("bundle", "df", "artifact")
            haskey(payload, key) && return payload[key]
        end
        length(payload) == 1 && return first(values(payload))
    end

    return payload
end

function _coerce_loaded_profile_artifact(artifact, kind::Symbol)
    if artifact isa AnnotatedProfile
        return artifact
    elseif artifact isa DataFrame
        if kind === :strict &&
           _metadata_profile_encoding(artifact) != _PROFILE_ENCODING_RANK_VECTOR_V1 &&
           !(eltype(artifact.profile) <: AbstractDict)
            decode_profile_column!(artifact)
        end
        return dataframe_to_annotated_profile(artifact; ballot_kind = kind)
    end

    throw(ArgumentError(
        "Unsupported cached profile artifact type $(typeof(artifact)); expected AnnotatedProfile or DataFrame.",
    ))
end

Base.getindex(ps::ProfilesSlice, var::Symbol, i::Int) =
    _coerce_loaded_profile_artifact(_load_profile_artifact(ps.paths[var][i]), :weak)

Base.getindex(ps::LinearizedProfilesSlice, var::Symbol, i::Int) =
    _coerce_loaded_profile_artifact(_load_profile_artifact(ps.paths[var][i]), :strict)

function load_profiles_index(year::Int;
                             dir::AbstractString = PROFILES_DATA_DIR)
    _error_legacy_pipeline_api(
        :load_profiles_index,
        "`NestedStochasticPipeline`, `run_pipeline`, and `load_pipeline_result`",
    )
    idxfile = joinpath(dir, "profiles_index_$(year).jld2")
    isfile(idxfile) || error("profiles index not found: $(idxfile)")
    return JLD2.load(idxfile, "result")
end

function load_linearized_profiles_index(year::Int;
                                        dir::AbstractString = LINEARIZED_PROFILES_DATA_DIR)
    _error_legacy_pipeline_api(
        :load_linearized_profiles_index,
        "`NestedStochasticPipeline`, `run_pipeline`, and `load_pipeline_result`",
    )
    idxfile = joinpath(dir, "linearized_profiles_index_$(year).jld2")
    isfile(idxfile) || error("linearized profiles index not found: $(idxfile)")
    return JLD2.load(idxfile, "result")
end

"""
    generate_profiles_for_year_streamed_from_index(year, f3_entry, iy;
                                                   out_dir=PROFILES_DATA_DIR,
                                                   variants=DEFAULT_PIPELINE_IMPUTATION_VARIANTS,
                                                   overwrite=false)

Streaming variant of `generate_profiles_for_year` that reads imputed replicates
on demand from an `ImputedYear` index, writing each weak-profile `DataFrame` to
disk and returning an index:
`scenario ⇒ m ⇒ ProfilesSlice`.
"""
function generate_profiles_for_year_streamed_from_index(
            year::Int,
            f3_entry::NamedTuple,
            iy::ImputedYear;
            candidate_sets = nothing,
            out_dir::AbstractString = PROFILES_DATA_DIR,
            variants = DEFAULT_PIPELINE_IMPUTATION_VARIANTS,
            overwrite::Bool         = false)
    _error_legacy_pipeline_api(
        :generate_profiles_for_year_streamed_from_index,
        "`run_pipeline` and `pipeline_panel_table` on `PipelineResult`s",
    )

    mkpath(out_dir)
    cfg            = f3_entry.cfg
    m_values       = cfg.m_values_range
    variant_syms   = _select_available_imputation_variants(keys(iy.paths), variants;
                                                           context = "imputed index for year $year")
    n_by_var       = Dict(v => length(iy.paths[v]) for v in variant_syms)
    year_csets     = _resolve_year_candidate_sets(cfg; candidate_sets = candidate_sets)

    result = OrderedDict{String,OrderedDict{Int,ProfilesSlice}}()

    for scen in cfg.scenarios
        full_cset = _full_candidate_list(year_csets, scen)

        scen_map = OrderedDict{Int,ProfilesSlice}()

        for m in m_values
            cand_syms  = _trim_candidate_list(full_cset, m; year = year, scenario = scen.name)
            paths_prof = Dict(v => Vector{String}(undef, n_by_var[v]) for v in variant_syms)

            rep_counter = 0      # throttle GC

            for var in variant_syms
                n_rep = n_by_var[var]

                for i in 1:n_rep
                    fprof = joinpath(out_dir,
                             "prof_$(year)_$(scen.name)_m$(m)_rep$(i)_" *
                             "$(String(var)).jld2")

                    # -------- fast‑skip if file already present --------------
                    if !overwrite && isfile(fprof)
                        paths_prof[var][i] = fprof
                        @debug "exists, skipping $(basename(fprof))"
                        continue
                    end

                    # -------- otherwise build & save -------------------------
                    df_imp = iy[var, i]

                    df = profile_dataframe(df_imp;
                            score_cols = cand_syms,
                            demo_cols  = cfg.demographics,
                            kind       = :weak)
                    metadata!(df, "candidates", cand_syms)
                    metadata!(df, "profile_kind", "weak")
                    bundle = dataframe_to_annotated_profile(df; ballot_kind = :weak)
                    artifact = compact_profile_artifact_dataframe(bundle)

                    JLD2.@save fprof artifact
                    @info "writing $(basename(fprof))"
                    paths_prof[var][i] = fprof

                    df = bundle = artifact = df_imp = nothing
                    rep_counter += 1
                    rep_counter % 10 == 0 && GC.gc()
                end
            end

            slice = ProfilesSlice(year, scen.name, m, cand_syms, paths_prof)
            scen_map[m] = slice
        end
        result[scen.name] = scen_map
    end

    idxfile = joinpath(out_dir, "profiles_index_$(year).jld2")
    JLD2.@save idxfile result
    @info "Weak profiles for $year written; index at $(idxfile)"
    return result
end

function _next_linearization_rng!(seed_stream::AbstractRNG)
    return MersenneTwister(rand(seed_stream, UInt32))
end

function _assert_complete_weak_orders(bundle::AnnotatedProfile;
                                      context::AbstractString)
    profile = bundle.profile

    if !Preferences.is_weak_order(profile) || !Preferences.is_complete(profile)
        throw(ArgumentError(
            "$context expected complete weak orders before linearization. " *
            "This pipeline stage does not define incomplete-ballot completion semantics.",
        ))
    end

    return bundle
end

"""
    linearize_profiles_for_year_streamed_from_index(year, f3_entry, profiles_year;
                                                    out_dir=LINEARIZED_PROFILES_DATA_DIR,
                                                    overwrite=false)

Read weak profile files from `profiles_year`, linearize them in a dedicated
pass, write the linearized profile bundles to disk, and return a parallel
index:
`scenario ⇒ m ⇒ LinearizedProfilesSlice`.

RNG note: this stage seeds a deterministic `seed_stream = MersenneTwister(cfg.rng_seed)`
once per year, then draws one child `UInt32` seed per `(scenario, m, variant, replicate)`
in traversal order. Re-running this stage with the same cached weak profiles, config,
and `cfg.rng_seed` reproduces the same tie-breaking sequence.
"""
function linearize_profiles_for_year_streamed_from_index(
            year::Int,
            f3_entry::NamedTuple,
            profiles_year::OrderedDict{String,<:Any};
            out_dir::AbstractString = LINEARIZED_PROFILES_DATA_DIR,
            overwrite::Bool         = false)
    _error_legacy_pipeline_api(
        :linearize_profiles_for_year_streamed_from_index,
        "`run_pipeline` and `load_pipeline_result`",
    )

    mkpath(out_dir)
    cfg         = f3_entry.cfg
    seed_stream = MersenneTwister(cfg.rng_seed)
    result = OrderedDict{String,OrderedDict{Int,LinearizedProfilesSlice}}()

    for (scen, m_map) in profiles_year
        scen_map = OrderedDict{Int,LinearizedProfilesSlice}()

        for (m, slice) in m_map
            slice isa ProfilesSlice || throw(ArgumentError(
                "linearize_profiles_for_year_streamed_from_index expects ProfilesSlice inputs; got $(typeof(slice)) for scenario=$scen, m=$m.",
            ))

            variants   = collect(keys(slice.paths))
            paths_prof = Dict(v => Vector{String}(undef, length(slice.paths[v])) for v in variants)
            rep_counter = 0

            for var in variants
                n_rep = length(slice.paths[var])

                for i in 1:n_rep
                    rng = _next_linearization_rng!(seed_stream)
                    flin = joinpath(out_dir,
                                    "linprof_$(year)_$(scen)_m$(m)_rep$(i)_$(String(var)).jld2")

                    if !overwrite && isfile(flin)
                        paths_prof[var][i] = flin
                        @debug "exists, skipping $(basename(flin))"
                        continue
                    end

                    bundle = slice[var, i]
                    _assert_complete_weak_orders(
                        bundle;
                        context = "year=$year scenario=$scen m=$m variant=$(String(var)) replicate=$i",
                    )
                    strict_bundle = linearize_annotated_profile(bundle; rng = rng)
                    artifact = compact_profile_artifact_dataframe(strict_bundle)

                    JLD2.@save flin artifact
                    @info "writing $(basename(flin))"
                    paths_prof[var][i] = flin

                    bundle = strict_bundle = artifact = nothing
                    rep_counter += 1
                    rep_counter % 10 == 0 && GC.gc()
                end
            end

            scen_map[m] = LinearizedProfilesSlice(
                year,
                scen,
                m,
                copy(slice.cand_list),
                paths_prof,
            )
        end
        result[scen] = scen_map
    end

    idxfile = joinpath(out_dir, "linearized_profiles_index_$(year).jld2")
    JLD2.@save idxfile result
    @info "Linearized profiles for $year written; index at $(idxfile)"
    return result
end

const PROFILE_FILE = joinpath(INT_DIR, "all_profiles.jld2")

const PROFILE_DIR = joinpath(INT_DIR, "profiles")
mkpath(PROFILE_DIR)   # ensure it exists

# ────────────────────────────────────────────────────────────────────────────────
"""
    save_or_load_profiles_for_year(year, f3, imps;
                                  dir       = PROFILE_DIR,
                                  overwrite = false,
                                  verbose   = true)

For the given `year`, if `dir/profiles_YEAR.jld2` exists and `overwrite=false`,
issues a warning and loads it.  Otherwise:

  • Calls `generate_profiles_for_year(year, f3[year], imps[year])`
  • Saves the result as `profiles` in `dir/profiles_YEAR.jld2`
  • Returns the `profiles` object.
"""
function save_or_load_profiles_for_year(year::Int,
                                        f3,
                                        imps;
                                        candidate_sets = nothing,
                                        dir::AbstractString = PROFILE_DIR,
                                        overwrite::Bool     = false,
                                        verbose::Bool       = true,
                                        variants            = DEFAULT_PIPELINE_IMPUTATION_VARIANTS)
    _error_legacy_pipeline_api(
        :save_or_load_profiles_for_year,
        "`run_pipeline` and `load_pipeline_result`",
    )

    mkpath(dir)
    path = joinpath(dir, "profiles_$(year).jld2")

    if isfile(path) && !overwrite
        verbose && @warn "Profiles for $year already exist at $path; loading cache."
        profiles = nothing
        @load path profiles
        return profiles
    end

    verbose && @info "Generating weak profiles for year $year…"
    profiles = generate_profiles_for_year(year, f3[year], imps[year];
                                          candidate_sets = candidate_sets,
                                          variants = variants)
    @save path profiles
    verbose && @info "Saved weak profiles for year $year → $path"
    return profiles
end

"Make a Dict<measure,<variant,Vector>> skeleton with empty vectors."
function init_accumulator(var_syms, measure_syms)
    accum = Dict{Symbol,Dict{Symbol,Vector{Float64}}}()
    for meas in measure_syms
        inner = Dict{Symbol,Vector{Float64}}()
        for var in var_syms
            inner[var] = Float64[]          # will push! into it
        end
        accum[meas] = inner
    end
    return accum
end

"Append values from `meas_one_rep` (1‑replicate output) into `accum`."
@inline function update_accumulator!(accum, meas_one_rep)
    for (meas, vdict) in meas_one_rep           # meas ⇒ variant ⇒ Vector(1)
        inner = accum[meas]
        for (var, vec1) in vdict
            push!(inner[var], vec1[1])          # vec1 has length 1
        end
    end
    return
end

function _require_linearized_slice(slice, scen, m)
    slice isa LinearizedProfilesSlice && return
    throw(ArgumentError(
        "Downstream measurement for scenario=$scen, m=$m expects LinearizedProfilesSlice inputs. " *
        "Run `linearize_profiles_for_year_streamed_from_index` and load the resulting index before applying measures.",
    ))
end

"""
    apply_measures_for_year(profiles_year) -> OrderedDict

For each scenario and each `m`, apply the global polarization measures to all
replicates and accumulate distributions per variant.

Returns: `scenario ⇒ m ⇒ (measure ⇒ variant ⇒ Vector{Float64})`.
"""
function apply_measures_for_year(
    profiles_year::OrderedDict{String,<:Any}
)::OrderedDict{String,OrderedDict{Int,Dict{Symbol,Dict{Symbol,Vector{Float64}}}}}

    out = OrderedDict{String,OrderedDict{Int,Dict{Symbol,Dict{Symbol,Vector{Float64}}}}}()

    for (scen, m_map) in profiles_year
        scen_out = OrderedDict{Int,Dict{Symbol,Dict{Symbol,Vector{Float64}}}}()

        for (m, slice) in m_map
            _require_linearized_slice(slice, scen, m)

            variants   = collect(keys(slice.paths))
            n_rep_max  = maximum(length(slice.paths[v]) for v in variants)

            #####  first replicate: discover measure names  #####
            var_map1 = Dict(var => [slice[var, 1]] for var in variants)
            meas1     = apply_all_measures_to_bts(var_map1)
            meas_syms = collect(keys(meas1))

            accum = init_accumulator(variants, meas_syms)
            update_accumulator!(accum, meas1)

            #####  progress bar  #####
            prog = pm.Progress(n_rep_max - 1;  desc = "[$scen|m=$m]", barlen = 30)

            #####  remaining replicates  #####
            rep_counter = 1
            for i in 2:n_rep_max
                var_map = Dict{Symbol,Vector{AnnotatedProfile}}()

                for var in variants
                    length(slice.paths[var]) < i && continue
                    bundle = slice[var, i]
                    var_map[var] = [bundle]
                end
                isempty(var_map) && continue

                meas_i = apply_all_measures_to_bts(var_map)
                update_accumulator!(accum, meas_i)

                pm.next!(prog)                           # advance bar
                rep_counter += 1
                rep_counter % 10 == 0 && GC.gc()
            end
            pm.finish!(prog)

            scen_out[m] = accum
            GC.gc()
        end
        out[scen] = scen_out
    end
    return out
end

const GLOBAL_MEASURE_DIR = joinpath(INT_DIR, "global_measures")
mkpath(GLOBAL_MEASURE_DIR)   # ensure the directory exists

"""
    save_or_load_measures_for_year(year, linearized_profiles_year;
                                   dir       = GLOBAL_MEASURE_DIR,
                                   overwrite = false,
                                   verbose   = true)

For a single `year`:

- If `dir/measures_YEAR.jld2` exists and `overwrite == false`, emits a warning and loads `measures` from disk.
- Otherwise, runs `apply_measures_for_year(linearized_profiles_year)`, saves the result under the name `measures`, and returns it.
"""
function save_or_load_measures_for_year(year,
                                        linearized_profiles_year;
                                        dir::AbstractString = GLOBAL_MEASURE_DIR,
                                        overwrite::Bool     = false,
                                        verbose::Bool       = true)
    _error_legacy_pipeline_api(
        :save_or_load_measures_for_year,
        "`run_pipeline` plus `pipeline_summary_table`/`pipeline_panel_table`",
    )

    mkpath(dir)
    path = joinpath(dir, "measures_$(year).jld2")

    if isfile(path) && !overwrite
        verbose && @warn "Global measures for $year already cached at $path; loading."
        measures = nothing
        @load path measures
        return measures
    end

    verbose && @info "Computing global measures for year $year…"
    measures = apply_measures_for_year(linearized_profiles_year)
    @save path measures
    verbose && @info "Saved global measures for year $year → $path"
    return measures
end

const GROUP_DIR = joinpath(INT_DIR, "group_metrics"); mkpath(GROUP_DIR)

"""
    update_accum!(accum::Dict, res::Dict, variants)

Internal helper: append one‑replicate results `res` into aggregate `accum` for
each `variant`. Creates missing variant vectors on the fly.
"""
function update_accum!(accum::Dict, res::Dict, variants)
    for (met, vdict) in res
        inner = get!(accum, met) do
            # first time we see this metric → create inner dict with empty vectors
            Dict(var => Float64[] for var in variants)
        end
        for (var, vec1) in vdict          # vec1 length == 1
            push!(get!(inner, var, Float64[]), vec1[1])   # create variant slot if absent
        end
    end
end

# ─────────────────── streaming apply_group_metrics_for_year ───────────────────
"""
    apply_group_metrics_for_year_streaming(linearized_profiles_year, cfg) -> OrderedDict

Stream over all replicates to compute group metrics (`C`, `D`, optionally `G`)
for every scenario, `m`, and demographic in `cfg.demographics`, using loaded
linearized profile slices.

Returns: `scenario ⇒ m ⇒ dem ⇒ metric ⇒ variant ⇒ Vector`.
"""
function apply_group_metrics_for_year_streaming(
        profiles_year::OrderedDict{String,<:Any},
        cfg)

    out = OrderedDict()
    year = cfg.year

    for (scen, m_map) in profiles_year
        scen_out = OrderedDict()

        for (m, slice) in m_map
            _require_linearized_slice(slice, scen, m)
            get_linear_order_catalog(Tuple(slice.cand_list))
            variants   = collect(keys(slice.paths))
            n_rep_max  = maximum(length(slice.paths[v]) for v in variants)

            dem_out = OrderedDict()

            for dem in cfg.demographics
                dem_sym = dem isa Symbol ? dem : Symbol(dem)
                @info "  → scenario=$scen, m=$m, dem=$dem_sym"

                accum = Dict{Symbol,Dict{Symbol,Vector{Float64}}}()
                prog  = pm.Progress(n_rep_max; desc="[$scen|m=$m|$dem_sym]", barlen=28)

                for i in 1:n_rep_max
                    var_map = Dict{Symbol,Vector{AnnotatedProfile}}()
                    for var in variants
                        length(slice.paths[var]) < i && continue
                        bundle = slice[var, i]
                        var_map[var] = [bundle]
                    end
                    isempty(var_map) && (pm.next!(prog); continue)

                    res = bootstrap_group_metrics(
                        var_map,
                        dem_sym;
                        tie_break_context = (
                            year = year,
                            scenario = String(scen),
                            m = Int(m),
                        ),
                    )
                    update_accum!(accum, res, variants)
                    pm.next!(prog)
                end
                pm.finish!(prog)
                dem_out[dem_sym] = accum
                GC.gc()
            end
            scen_out[m] = dem_out
        end
        out[scen] = scen_out
    end
    return out          # scenario ⇒ m ⇒ dem ⇒ metric ⇒ variant ⇒ Vector
end

# directory layout for per‑DataFrame caches
"""
    _perdf(dir, year, scen, m, dem, rep) -> String

Compute a canonical cache path for metrics of a single DataFrame (replicate).
Layout: `dir/per_df/{year}/{scen}/m{m}/{dem}/rep{rep}.jld2`.
"""
_perdf(dir, year, scen, m, dem, rep) =
    joinpath(dir, "per_df", string(year), string(scen), "m$m", string(dem),
             "rep$rep.jld2")

"Ensure the parent directory of `path` exists."
_mkparent(path) = mkpath(dirname(path))

function _atomic_jldsave(path::AbstractString, entries::Pair...)
    _mkparent(path)
    tmp = path * ".tmp"
    isfile(tmp) && rm(tmp; force = true)
    JLD2.jldopen(tmp, "w"; iotype = IOStream) do f
        for (name, value) in entries
            f[String(name)] = value
        end
    end
    mv(tmp, path; force = true)
end

function _atomic_save(path::AbstractString, res)
    return _atomic_jldsave(path, "res" => res)
end

function _safe_load_res(path::AbstractString)
    try
        return JLD2.jldopen(path, "r"; iotype = IOStream) do f
            f["res"]
        end
    catch err
        @warn "Failed to load cache; recomputing" cache_path = path err
        return nothing
    end
end

"""
    compute_and_cache_group_metrics_per_df!(year, linearized_profiles_year, cfg;
                                            dir=GROUP_DIR, overwrite=false,
                                            verbose=true)

Pass 1 of the two‑pass pipeline: compute group metrics for each replicate and
**write per‑DataFrame caches**. Safe to re‑run: respects `overwrite=false`.
"""
function compute_and_cache_group_metrics_per_df!(
        year::Int,
        profiles_year::OrderedDict{String,<:Any},
        cfg;
        dir::AbstractString = GROUP_DIR,
        overwrite::Bool     = false,
        verbose::Bool       = true)

    for (scen, m_map) in profiles_year
        for (m, slice) in m_map
            _require_linearized_slice(slice, scen, m)
            get_linear_order_catalog(Tuple(slice.cand_list))
            variants   = collect(keys(slice.paths))
            n_rep_max  = maximum(length(slice.paths[v]) for v in variants)

            for dem in cfg.demographics
                dem_sym = Symbol(dem)
                pbar = pm.Progress(n_rep_max;
                                desc="[$year|$scen|m=$m|$dem_sym]",
                                barlen=28)

                for rep in 1:n_rep_max
                    cache_path = _perdf(dir, year, scen, m, dem_sym, rep)
                    if isfile(cache_path) && !overwrite
                        pm.next!(pbar); continue
                    end

                    var_map = Dict{Symbol,Vector{AnnotatedProfile}}()
                    for var in variants
                        length(slice.paths[var]) < rep && continue
                        bundle = slice[var, rep]                 # load
                        var_map[var] = [bundle]
                    end
                    isempty(var_map) && (pm.next!(pbar); continue)

                    res = bootstrap_group_metrics(
                        var_map,
                        dem_sym;
                        tie_break_context = (
                            year = year,
                            scenario = String(scen),
                            m = Int(m),
                        ),
                    )

                    _atomic_save(cache_path, res)
                    pm.next!(pbar)
                end
                pm.finish!(pbar); GC.gc()
            end
        end
    end
end

# ────────────────── pass 2: aggregate caches ──────────────────
"""
    accumulate_cached_group_metrics_for_year!(year, linearized_profiles_year, cfg;
                                              dir=GROUP_DIR, verbose=true)

Pass 2 of the two‑pass pipeline: aggregate previously cached per‑DataFrame
metrics into distributions per `(scenario, m, demographic, metric, variant)`.
"""
function accumulate_cached_group_metrics_for_year!(
        year::Int,
        profiles_year::OrderedDict{String,<:Any},
        cfg;
        dir::AbstractString = GROUP_DIR,
        verbose::Bool       = true)

    out = OrderedDict()

    for (scen, m_map) in profiles_year
        scen_out = OrderedDict()
        for (m, slice) in m_map
            _require_linearized_slice(slice, scen, m)
            get_linear_order_catalog(Tuple(slice.cand_list))
            variants   = collect(keys(slice.paths))
            n_rep_max  = maximum(length(slice.paths[v]) for v in variants)

            dem_out = OrderedDict()
            for dem in cfg.demographics
                dem_sym = Symbol(dem)
                verbose && @info "  → aggregating $scen, m=$m, dem=$dem_sym"

                accum = Dict{Symbol,Dict{Symbol,Vector{Float64}}}()
                pbar  = pm.Progress(n_rep_max;
                                 desc="[$scen|m=$m|$dem_sym]", barlen=28)

                for rep in 1:n_rep_max
                    cache_path = _perdf(dir, year, scen, m, dem_sym, rep)
                    res = nothing
                    if isfile(cache_path)
                        res = _safe_load_res(cache_path)
                    end

                    if res === nothing
                        var_map = Dict{Symbol,Vector{AnnotatedProfile}}()
                        for var in variants
                            length(slice.paths[var]) < rep && continue
                            bundle = slice[var, rep]
                            var_map[var] = [bundle]
                        end
                        isempty(var_map) && error("Missing data for $cache_path")

                        res = bootstrap_group_metrics(
                            var_map,
                            dem_sym;
                            tie_break_context = (
                                year = year,
                                scenario = String(scen),
                                m = Int(m),
                            ),
                        )
                        _atomic_save(cache_path, res)
                    end

                    update_accum!(accum, res, variants)
                    pm.next!(pbar)
                end
                pm.finish!(pbar)
                dem_out[dem_sym] = accum
            end
            scen_out[m] = dem_out
        end
        out[scen] = scen_out
    end
    return out           # scenario ⇒ m ⇒ dem ⇒ metric ⇒ variant ⇒ Vector
end

# ────────────────── public API (drop‑in) ──────────────────
"""
    save_or_load_group_metrics_for_year(year, linearized_profiles_year, f3_entry;
                                        dir=GROUP_DIR, overwrite=false,
                                        two_pass=false, verbose=true)

Run the group‑metrics pipeline for one `year`. If `two_pass=true`, compute and
cache per‑DataFrame results first and then aggregate; otherwise stream
computation in a single pass. Always writes `group_metrics_YEAR.jld2` and
returns the aggregated object.
"""
function save_or_load_group_metrics_for_year(year::Int,
                                             profiles_year,
                                             f3_entry;
                                             dir::AbstractString = GROUP_DIR,
                                             overwrite::Bool     = false,
                                             two_pass::Bool      = false,
                                             verbose::Bool       = true)
    _error_legacy_pipeline_api(
        :save_or_load_group_metrics_for_year,
        "`run_pipeline` plus `pipeline_measure_table`/`pipeline_panel_table`",
    )

    mkpath(dir)
    final_path = joinpath(dir, "group_metrics_$(year).jld2")

    if isfile(final_path) && !overwrite && !two_pass
        verbose && @warn "Group metrics for $year already cached; loading."
        metrics = nothing; @load final_path metrics; return metrics
    end

    if two_pass
        verbose && @info "Pass 1: computing & caching per‑DataFrame metrics…"
        compute_and_cache_group_metrics_per_df!(year, profiles_year, f3_entry.cfg;
                                                dir=dir, overwrite=overwrite,
                                                verbose=verbose)

        verbose && @info "Pass 2: aggregating cached metrics…"
        metrics = accumulate_cached_group_metrics_for_year!(year, profiles_year,
                                                            f3_entry.cfg;
                                                            dir=dir,
                                                            verbose=verbose)
    else
        verbose && @info "Computing group metrics for year $year (one‑pass)…"
        metrics = apply_group_metrics_for_year_streaming(profiles_year,
                                                         f3_entry.cfg)
    end

    @save final_path metrics
    verbose && @info "Saved aggregated metrics for $year → $final_path"
    return metrics
end



function describe_candidate_set(candidates::AbstractVector{<:AbstractString})
    pretty_names = [join([uppercasefirst(lowercase(w)) for w in split(String(name), "_")], " ")
                    for name in candidates]
    return "Candidates: " * join(pretty_names, ", ")
end

function lines_alt_by_variant(measures_over_m::AbstractDict;
                              variants   = DEFAULT_PIPELINE_IMPUTATION_VARIANTS,
                              palette    = nothing,
                              figsize    = (1000, 900),
                              candidate_label::String = "", year)
    return _call_plotting_extension(
        :lines_alt_by_variant,
        measures_over_m;
        variants = variants,
        palette = palette,
        figsize = figsize,
        candidate_label = candidate_label,
        year = year,
    )
end

function dotwhisker_alt_by_variant(measures_over_m::AbstractDict;
                                   variants   = DEFAULT_PIPELINE_IMPUTATION_VARIANTS,
                                   palette    = nothing,
                                   figsize    = (1000, 900),
                                   candidate_label::String = "", year,
                                   whiskerwidth_outer = 10,
                                   whiskerwidth_inner = 6,
                                   linewidth_outer = 1.5,
                                   linewidth_inner = 3.0,
                                   dot_size = 8,
                                   dodge = 0.18,
                                   connect_lines::Bool = false,
                                   connect_linewidth = 1.5)
    return _call_plotting_extension(
        :dotwhisker_alt_by_variant,
        measures_over_m;
        variants = variants,
        palette = palette,
        figsize = figsize,
        candidate_label = candidate_label,
        year = year,
        whiskerwidth_outer = whiskerwidth_outer,
        whiskerwidth_inner = whiskerwidth_inner,
        linewidth_outer = linewidth_outer,
        linewidth_inner = linewidth_inner,
        dot_size = dot_size,
        dodge = dodge,
        connect_lines = connect_lines,
        connect_linewidth = connect_linewidth,
    )
end



"""
    plot_scenario_year(year, scenario, f3, all_meas;
                       variant=\"mice\", palette, figsize,
                       plot_kind=:lines, connect_lines=false) -> Figure

Convenience wrapper to produce a single‑scenario plot for `year` and
`scenario` using either `lines_alt_by_variant` or
`dotwhisker_alt_by_variant`. Loads the precomputed year-level candidate list
used by `generate_profiles_for_year` and passes a human-readable label to the
plotting helper.
"""
function plot_scenario_year(
    year,
    scenario,
    f3,
    all_meas;
    variant = "mice",
    palette = nothing,
    figsize = (500,400),
    plot_kind::Symbol = :lines,
    connect_lines::Bool = false,
)
    _error_legacy_pipeline_api(
        :plot_scenario_year,
        "`plot_pipeline_scenario` on `PipelineResult` or `BatchRunResult` outputs",
    )
    return _call_plotting_extension(
        :plot_scenario_year,
        year,
        scenario,
        f3,
        all_meas;
        variant = variant,
        palette = palette,
        figsize = figsize,
        plot_kind = plot_kind,
        connect_lines = connect_lines,
    )
end

# ──────────────────────────────────────────────────────────────────
# helper: replicate the candidate-set logic from generate_profiles…
# ──────────────────────────────────────────────────────────────────
"""
    _full_candidate_list(cfg, scen_obj; candidate_sets=nothing) -> Vector{String}

Load the precomputed candidate list used by `generate_profiles_for_year`.
"""
function _full_candidate_list(cfg::ElectionConfig, scen_obj::Scenario; candidate_sets = nothing)
    year_csets = _resolve_year_candidate_sets(cfg; candidate_sets = candidate_sets)
    return _full_candidate_list(year_csets, scen_obj)
end

"""
    plot_group_demographics_lines(all_gm, f3, year, scenario;
                                  variants=DEFAULT_PIPELINE_IMPUTATION_VARIANTS,
                                  measures=[:C,:D,:G], maxcols=3,
                                  n_yticks=5, ytick_step=nothing,
                                  palette=Makie.wong_colors(),
                                  clist_size=60, demographics=f3[year].cfg.demographics)
        -> Figure

One panel per demographic; **x = number of alternatives (m)**.
For every *(measure, variant)* pair the panel shows

* a translucent band between Q25 and Q75
* a line for the mean.

The title and candidate list match the original
`plot_group_demographics`, but the layout and styling come from
`lines_group_measures_over_m`.
"""
function plot_group_demographics_lines(
        all_gm,
        f3,
        year::Int,
        scenario::String;
        variants      = DEFAULT_PIPELINE_IMPUTATION_VARIANTS,
        measures      = [:C, :D, :G],
        maxcols::Int  = 3,
        n_yticks::Int = 5,
        ytick_step    = nothing,
        palette       = nothing, clist_size = 60,
        demographics = nothing
)
    _error_legacy_pipeline_api(
        :plot_group_demographics_lines,
        "`plot_pipeline_group_lines` on `PipelineResult` or `BatchRunResult` outputs",
    )
    return _call_plotting_extension(
        :plot_group_demographics_lines,
        all_gm,
        f3,
        year,
        scenario;
        variants = variants,
        measures = measures,
        maxcols = maxcols,
        n_yticks = n_yticks,
        ytick_step = ytick_step,
        palette = palette,
        clist_size = clist_size,
        demographics = demographics,
    )
end

"""
    plot_group_demographics_heatmap(all_gm, f3, year, scenario;
                                    variants=DEFAULT_PIPELINE_IMPUTATION_VARIANTS,
                                    measures=[:C,:D,:G],
                                    modified_C=false,
                                    modified_G=false,
                                    groupings=f3[year].cfg.demographics,
                                    maxcols=3, colormap=:viridis, colormaps=nothing,
                                    fixed_colorrange=false,
                                    show_values=false,
                                    simplified_labels=false,
                                    clist_size=60) -> Figure

Heatmap view of group metrics. Each panel is a *(measure, variant)* pair,
with **x = number of alternatives (m)** and **y = grouping**. Cells show
the median across bootstrap draws. Set `show_values=true` to annotate
each cell with its median. Set `simplified_labels=true` to show the y-axis
label only on the first measure panel and the x-axis label only on the middle
measure panel (to reduce repetition).
Set `modified_C=true` to transform `C` as `2*C - 1`; `G` will then be computed
using this transformed `C`.
Set `modified_G=true` to keep `C` as plotted while computing `G` as
`sqrt((2*C - 1) * D)`.
"""
function plot_group_demographics_heatmap(
        all_gm,
        f3,
        year::Int,
        scenario::String;
        variants      = DEFAULT_PIPELINE_IMPUTATION_VARIANTS,
        measures      = [:C, :D, :G],
        modified_C::Bool = false,
        modified_G::Bool = false,
        groupings     = nothing,
        maxcols::Int  = 3,
        colormap      = :viridis,
        colormaps     = nothing,
        fixed_colorrange::Bool = false,
        show_values::Bool = false,
        simplified_labels::Bool = false,
        clist_size    = 60
)
    _error_legacy_pipeline_api(
        :plot_group_demographics_heatmap,
        "`plot_pipeline_group_heatmap` on `PipelineResult` or `BatchRunResult` outputs",
    )
    return _call_plotting_extension(
        :plot_group_demographics_heatmap,
        all_gm,
        f3,
        year,
        scenario;
        variants = variants,
        measures = measures,
        modified_C = modified_C,
        modified_G = modified_G,
        groupings = groupings,
        maxcols = maxcols,
        colormap = colormap,
        colormaps = colormaps,
        fixed_colorrange = fixed_colorrange,
        show_values = show_values,
        simplified_labels = simplified_labels,
        clist_size = clist_size,
    )
end

"""
    save_plot(fig, year, scenario, cfg; variant, dir = "imgs", ext = ".png")

Save `fig` under `dir/`, creating the directory if needed.
The file name pattern is:

    {year}_{scenario}_{variant}_B{n_bootstrap}_M{max_m}_{yyyymmdd-HHMMSS}{ext}
"""
function save_plot(fig, year::Int, scenario::AbstractString, cfg;
                   variant::AbstractString,
                   dir::AbstractString = "imgs",
                   ext::AbstractString = ".png")
    _error_legacy_pipeline_api(
        :save_plot,
        "`save_pipeline_plot` with nested batch/spec-aware stems",
    )
    return _call_plotting_extension(
        :save_plot,
        fig,
        year,
        scenario,
        cfg;
        variant = variant,
        dir = dir,
        ext = ext,
    )
end
