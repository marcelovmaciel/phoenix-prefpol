"""
    Scenario

A named TOML scenario that force-includes an ordered prefix of candidates when
PrefPol resolves active candidate sets.
"""
struct Scenario
    name::String
    candidates::Vector{String}
end

"""
    ElectionConfig

Full configuration parsed from a PrefPol year TOML file.

It records the raw-data loader, candidate universe, demographic columns,
candidate-count settings, default stochastic settings, and scenario definitions
used by raw-profile helpers and the nested pipeline.
"""
struct ElectionConfig
    year::Int
    data_loader::String
    data_file::String
    max_candidates::Int
    m_values_range::Vector{Int}
    n_bootstrap::Int
    n_alternatives::Int
    rng_seed::Int
    candidates::Vector{String}
    demographics::Vector{String}
    scenarios::Vector{Scenario}
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

@inline _infer_m(cfg::ElectionConfig) =
    cfg.n_alternatives > 0 ? cfg.n_alternatives : cfg.max_candidates

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

        if cfg.year == 2018 && sc.name == "main_2018" && "Lula" in sc.candidates
            throw(ArgumentError(
                "Scenario `main_2018` must not include `Lula` as an active candidate; " *
                "use `LulaScoreGroup` as the respondent partition instead.",
            ))
        end
    end

    return cfg
end

"""
    load_election_cfg(path) -> ElectionConfig

Parse a PrefPol year TOML file and return an `ElectionConfig`. Relative
`data_file` paths are resolved under `project_root`; `forced_scenarios` entries
are converted to `Scenario` values.
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

const _SURVEY_CONFIG_INDENT = "    "

_survey_config_pp(io, key, val, lvl) =
    println(io, repeat(_SURVEY_CONFIG_INDENT, lvl), key, " = ", val)

function _survey_config_pp_vec(io, key, vec, lvl; max = 8)
    head = first(vec, max)
    tail = length(vec) > max ? " ... (" * string(length(vec)) * ")" : ""
    println(io, repeat(_SURVEY_CONFIG_INDENT, lvl), key, " = ", head, tail)
end

function show(io::IO, ::MIME"text/plain", s::Scenario; kwargs...)
    print(io, "Scenario(\"", s.name, "\", ", s.candidates, ")")
end

function show(io::IO, ::MIME"text/plain", ec::ElectionConfig; kwargs...)
    println(io, "ElectionConfig(")
    _survey_config_pp(io, "year", ec.year, 1)
    _survey_config_pp(io, "data_loader", ec.data_loader, 1)
    _survey_config_pp(io, "data_file", ec.data_file, 1)
    _survey_config_pp(io, "max_candidates", ec.max_candidates, 1)
    _survey_config_pp(io, "m_values_range", ec.m_values_range, 1)
    _survey_config_pp(io, "n_bootstrap(def)", ec.n_bootstrap, 1)
    _survey_config_pp(io, "n_alternatives", ec.n_alternatives, 1)
    _survey_config_pp(io, "rng_seed(def)", ec.rng_seed, 1)
    _survey_config_pp_vec(io, "candidates", ec.candidates, 1)
    _survey_config_pp(io, "demographics", ec.demographics, 1)
    println(io, _SURVEY_CONFIG_INDENT, "scenarios = [")
    for sc in ec.scenarios
        println(io, repeat(_SURVEY_CONFIG_INDENT, 2), sc)
    end
    println(io, _SURVEY_CONFIG_INDENT, "]")
    print(io, ")")
end

"""
    load_election_data(cfg) -> DataFrame

Resolve and call the configured data loader in the `PrefPol` module, passing
`cfg.data_file` and `candidates = cfg.candidates`.
"""
function load_election_data(cfg::ElectionConfig)
    loader_sym = Symbol(cfg.data_loader)

    isdefined(@__MODULE__, loader_sym) || throw(ArgumentError(
        "data_loader `$(cfg.data_loader)` not found in module $(nameof(@__MODULE__)).",
    ))

    loader_fun = getfield(@__MODULE__, loader_sym)
    return loader_fun(cfg.data_file; candidates = cfg.candidates)
end

function _candidate_weight_col(df::DataFrame)
    for nm in (:peso, :weight, :weights)
        hasproperty(df, nm) && return nm
    end
    throw(ArgumentError(
        "Could not find a survey weight column. Expected one of :peso, :weight or :weights.",
    ))
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

function _candidate_order(cfg::ElectionConfig;
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
    return compute_global_candidate_set(
        df;
        candidate_cols = cfg.candidates,
        m = resolved_m,
        force_include = force_include,
        weights = df[!, weight_col],
    )
end
