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
candidate-count settings, default stochastic settings, and scenario definitions.
The same parsed object feeds survey-wave configuration, raw survey profile
construction, and the nested pipeline candidate-selection rules.
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


"""
    SurveyWaveConfig

Applied configuration for one Brazil/ESEB survey wave in the PrefPol
replication workflow.

The object is derived from a year TOML parsed by `load_election_cfg` and records
how PrefPol should load the raw survey table, which candidate columns form the
candidate universe, which demographic columns are available for grouped
measures, and which scenario-specific candidates must be forced into an active
candidate set. Raw-profile builders and the nested pipeline resolve candidates
through the same `SurveyWaveConfig` rules. Formal profile and measure
definitions live in `Preferences`.
"""
struct SurveyWaveConfig
    wave_id::String
    year::Int
    data_loader::String
    data_file::String
    candidate_universe::Vector{String}
    demographic_cols::Vector{String}
    scenario_candidates::Dict{String,Vector{String}}
    max_candidates::Int
    default_m::Int
    default_seed::Int
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

@inline _infer_m(wcfg::SurveyWaveConfig) = wcfg.default_m

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

"""
    SurveyWaveConfig(cfg::ElectionConfig; wave_id=string(cfg.year))

Convert a year TOML configuration into the survey-wave configuration used by
the raw-profile helpers and reproducible BRK pipeline.
"""
SurveyWaveConfig(cfg::ElectionConfig; wave_id::AbstractString = string(cfg.year)) =
    SurveyWaveConfig(
        String(wave_id),
        cfg.year,
        cfg.data_loader,
        cfg.data_file,
        Vector{String}(cfg.candidates),
        Vector{String}(cfg.demographics),
        Dict(sc.name => Vector{String}(sc.candidates) for sc in cfg.scenarios),
        cfg.max_candidates,
        _infer_m(cfg),
        cfg.rng_seed,
    )

"""
    load_survey_wave_config(path; wave_id=nothing) -> SurveyWaveConfig

Load a PrefPol year configuration TOML and return the corresponding
`SurveyWaveConfig`.

This is the application-wave configuration entry point for the Brazil/ESEB
workflow. `data_file` paths are resolved by `load_election_cfg`; no survey data
or cache artifact is read here.
"""
function load_survey_wave_config(path::AbstractString; wave_id = nothing)
    cfg = load_election_cfg(path)
    resolved_wave_id = wave_id === nothing ? string(cfg.year) : String(wave_id)
    return SurveyWaveConfig(cfg; wave_id = resolved_wave_id)
end

function load_wave_data(wcfg::SurveyWaveConfig)
    loader_sym = Symbol(wcfg.data_loader)

    if !isdefined(@__MODULE__, loader_sym)
        throw(ArgumentError(
            "data_loader `$(wcfg.data_loader)` not found in module $(nameof(@__MODULE__)).",
        ))
    end

    loader_fun = getfield(@__MODULE__, loader_sym)
    return loader_fun(wcfg.data_file; candidates = wcfg.candidate_universe)
end

function _scenario_force_include(wcfg::SurveyWaveConfig, scenario_name)
    sname = String(scenario_name)
    haskey(wcfg.scenario_candidates, sname) || throw(ArgumentError(
        "Unknown scenario `$sname` for wave $(wcfg.wave_id). Available scenarios: $(collect(keys(wcfg.scenario_candidates))).",
    ))
    return Vector{String}(wcfg.scenario_candidates[sname])
end

function _scenario_from_wave(wcfg::SurveyWaveConfig, scenario_name)
    return Scenario(String(scenario_name), _scenario_force_include(wcfg, scenario_name))
end

function _election_cfg(wcfg::SurveyWaveConfig)
    scenarios = [
        Scenario(name, Vector{String}(candidates))
        for (name, candidates) in wcfg.scenario_candidates
    ]
    return ElectionConfig(
        wcfg.year,
        wcfg.data_loader,
        wcfg.data_file,
        wcfg.max_candidates,
        collect(2:wcfg.max_candidates),
        1,
        wcfg.default_m,
        wcfg.default_seed,
        Vector{String}(wcfg.candidate_universe),
        Vector{String}(wcfg.demographic_cols),
        scenarios,
    )
end

function _candidate_order(wcfg::SurveyWaveConfig;
                          scenario = nothing,
                          m::Int = wcfg.default_m,
                          data = nothing)
    resolved_m = _validate_candidate_count(
        m,
        wcfg.max_candidates;
        context = "Candidate-set resolution for wave $(wcfg.wave_id)",
        min_allowed = 1,
    )
    df = data === nothing ? load_wave_data(wcfg) : data

    force_include = if scenario === nothing
        String[]
    elseif scenario isa Scenario
        Vector{String}(scenario.candidates)
    else
        _scenario_force_include(wcfg, scenario)
    end

    weight_col = _candidate_weight_col(df)
    return compute_global_candidate_set(
        df;
        candidate_cols = wcfg.candidate_universe,
        m = resolved_m,
        force_include = force_include,
        weights = df[!, weight_col],
    )
end

function _ensure_preferences_module()
    if !isdefined(@__MODULE__, :Preferences)
        throw(ArgumentError(
            "Missing Preferences module. Expected local source at " *
            "`$(_LOCAL_PREFERENCES_SRC)`. Restart Julia so PrefPol can " *
            "load it at module initialization.",
        ))
    end

    prefs = getfield(@__MODULE__, :Preferences)
    required = (
        :CandidatePool, :WeakRank, :Profile, :WeightedProfile,
        :build_profile_from_scores,
        :candidate_display_symbols,
        :guess_weight_col,
        :resolve_candidate_cols_from_set,
        :profile_pattern_proportions,
        :ranked_count,
        :has_ties,
        :ranking_type_support,
        :ranking_type_template,
        :profile_ranksize_summary,
        :profile_ranking_type_proportions,
        :pretty_print_profile_patterns,
        :pretty_print_ranksize_summary,
        :pretty_print_ranking_type_proportions,
    )
    for sym in required
        isdefined(prefs, sym) || throw(ArgumentError(
            "Incompatible Preferences module: missing `$sym`.",
        ))
    end
    return prefs
end

@inline _prefs() = _ensure_preferences_module()

const RAW_PROFILE_SUPPORTED_YEARS = Set([2006, 2018, 2022])
const _RAW_PROFILE_CFG_DIR = joinpath(project_root, "config")

function _load_raw_profile_cfg(year::Int; config_path=nothing)
    year in RAW_PROFILE_SUPPORTED_YEARS || throw(ArgumentError(
        "Unsupported year $year. Supported years: 2006, 2018, 2022.",
    ))

    cfg_path = config_path === nothing ? joinpath(_RAW_PROFILE_CFG_DIR, "$year.toml") :
               String(config_path)
    isfile(cfg_path) || throw(ArgumentError("Config file not found: `$cfg_path`."))

    cfg = load_election_cfg(cfg_path)
    cfg.year == year || throw(ArgumentError(
        "Config year $(cfg.year) does not match requested year $year.",
    ))
    return cfg, cfg_path
end

@inline _humanize_candidate_name(name::AbstractString) = _prefs().humanize_candidate_name(name)
@inline _canonical_candidate_key(x) = _prefs().canonical_candidate_key(x)

function _candidate_display_symbols(candidate_cols::Vector{String})
    return _prefs().candidate_display_symbols(candidate_cols)
end

function _guess_weight_col(df::DataFrame)
    col = _prefs().guess_weight_col(df; preferred = (:peso, :weight, :weights))
    col === nothing && throw(ArgumentError(
        "Could not find a survey weight column. Expected one of :peso, :weight or :weights.",
    ))
    return col
end

function _resolve_candidate_cols_from_set(df::DataFrame,
                                          universe_cols::Vector{String},
                                          candidate_set)
    return _prefs().resolve_candidate_cols_from_set(df, universe_cols, candidate_set)
end

_candidate_universe(cfg::ElectionConfig) = Vector{String}(cfg.candidates)
_candidate_universe(wcfg::SurveyWaveConfig) = Vector{String}(wcfg.candidate_universe)
_config_year(cfg::ElectionConfig) = cfg.year
_config_year(wcfg::SurveyWaveConfig) = wcfg.year
_config_max_candidates(cfg::ElectionConfig) = cfg.max_candidates
_config_max_candidates(wcfg::SurveyWaveConfig) = wcfg.max_candidates
_config_context(cfg::ElectionConfig) = "year $(cfg.year)"
_config_context(wcfg::SurveyWaveConfig) = "wave $(wcfg.wave_id)"

function _normalize_k_m(; k = nothing, m = nothing, default_m::Integer,
                        context::AbstractString)
    if k !== nothing && m !== nothing && Int(k) != Int(m)
        throw(ArgumentError(
            "$context received conflicting `k=$(Int(k))` and `m=$(Int(m))`.",
        ))
    end
    return Int(m !== nothing ? m : k !== nothing ? k : default_m)
end

function _resolve_candidate_cols(df::DataFrame, cfg;
                                 candidate_set = nothing,
                                 active_candidates = nothing,
                                 scenario_name = nothing,
                                 m = nothing,
                                 k = nothing,
                                 min_allowed::Integer = 1)
    if candidate_set !== nothing && active_candidates !== nothing
        throw(ArgumentError("Pass only one of `candidate_set` or `active_candidates`."))
    end
    exact_set = active_candidates !== nothing ? active_candidates : candidate_set
    if exact_set !== nothing && scenario_name !== nothing
        throw(ArgumentError("Pass either an exact candidate set or `scenario_name`, not both."))
    end
    if exact_set !== nothing && (k !== nothing || m !== nothing)
        throw(ArgumentError(
            "When an exact candidate set is provided, do not pass `k` or `m`; its size defines the candidate count.",
        ))
    end

    universe = _candidate_universe(cfg)
    context = _config_context(cfg)

    if exact_set !== nothing
        candidate_cols = _resolve_candidate_cols_from_set(df, universe, exact_set)
        _validate_candidate_count(
            length(candidate_cols),
            _config_max_candidates(cfg);
            context = "Exact candidate-set request for $context",
            min_allowed = min_allowed,
        )
        return candidate_cols
    end

    mm = _validate_candidate_count(
        _normalize_k_m(
            k = k,
            m = m,
            default_m = _infer_m(cfg),
            context = "Candidate-set request for $context",
        ),
        _config_max_candidates(cfg);
        context = "Candidate-set request for $context",
        min_allowed = min_allowed,
    )

    scenario = scenario_name === nothing ? nothing : (
        cfg isa ElectionConfig ? _lookup_scenario(cfg, scenario_name) :
        _scenario_from_wave(cfg, scenario_name)
    )
    candidate_cols = _candidate_order(cfg; scenario = scenario, m = mm, data = df)
    return length(candidate_cols) < mm ? candidate_cols : first(candidate_cols, mm)
end

"""
    resolve_active_candidate_set(wcfg; active_candidates=nothing,
                                 scenario_name=nothing, m=nothing,
                                 data=nothing) -> Vector{String}

Resolve the ordered candidate set used by one Brazil/ESEB pipeline spec.

Pass `active_candidates` to use an exact set. Otherwise PrefPol computes a
weighted-missingness ordering from the raw survey data, optionally force-including
the candidates declared by `scenario_name`, and trims to `m` alternatives. Raw
profile construction calls the same resolver, so scenario forcing and candidate
counts agree with the nested pipeline. This function may load the raw survey
table unless `data` is provided; it does not write cache.
"""
function resolve_active_candidate_set(wcfg::SurveyWaveConfig;
                                      active_candidates = nothing,
                                      scenario_name = nothing,
                                      m = nothing,
                                      data = nothing)
    df = data === nothing ? load_wave_data(wcfg) : data
    return _resolve_candidate_cols(
        df,
        wcfg;
        active_candidates = active_candidates,
        scenario_name = scenario_name,
        m = m,
        min_allowed = 2,
    )
end

@inline function _normalize_score(v)
    v === missing && return missing

    x = if v isa Real
        Float64(v)
    elseif v isa AbstractString
        parsed = tryparse(Float64, strip(v))
        parsed === nothing && return missing
        parsed
    else
        return missing
    end

    isfinite(x) || return missing
    x in (96.0, 97.0, 98.0, 99.0) && return missing
    (0.0 <= x <= 10.0) || return missing
    return x
end

function _build_profile_with_candidates(df::DataFrame,
                                        candidate_cols::Vector{String},
                                        candidate_syms::Vector{Symbol};
                                        weighted::Bool = false,
                                        allow_ties::Bool = true,
                                        allow_incomplete::Bool = true,
                                        all_unranked_as_indifferent::Bool = false,
                                        weight_col = nothing)
    prefs = _prefs()
    return prefs.build_profile_from_scores(
        df,
        candidate_cols,
        candidate_syms;
        weighted = weighted,
        allow_ties = allow_ties,
        allow_incomplete = allow_incomplete,
        all_unranked_as_indifferent = all_unranked_as_indifferent,
        weight_col = weight_col,
        score_normalizer = _normalize_score,
        empty_profile_error_message =
            "No valid preference rows remained after filtering. " *
            "Rules: keep scores in [0,10], treat 96-99 as unranked, " *
            "drop all-unranked rows, and (if allow_incomplete=false) drop partial rows.",
    )
end

function _raw_pref_tuple(cfg, df::DataFrame, candidate_cols::Vector{String};
                         scenario_name = nothing)
    isempty(candidate_cols) && throw(ArgumentError(
        "Candidate set inference failed for year $(_config_year(cfg)).",
    ))

    missing_cols = setdiff(candidate_cols, names(df))
    isempty(missing_cols) || throw(ArgumentError(
        "Missing candidate mapping/columns in data for year $(_config_year(cfg)): $(missing_cols).",
    ))

    candidate_syms = _candidate_display_symbols(candidate_cols)
    weight_col = _guess_weight_col(df)

    return (
        year = _config_year(cfg),
        cfg = cfg isa ElectionConfig ? cfg : _election_cfg(cfg),
        wave_config = cfg isa SurveyWaveConfig ? cfg : SurveyWaveConfig(cfg),
        df = df,
        m = length(candidate_cols),
        candidate_cols = candidate_cols,
        candidate_labels = String.(candidate_syms),
        weight_col = weight_col,
        scenario_name = scenario_name === nothing ? nothing : String(scenario_name),
    )
end

"""
    load_raw_pref_data(year::Int;
                       config_path=nothing,
                       candidate_set=nothing,
                       scenario_name=nothing,
                       k=nothing)

Load raw survey data for `year` (2006, 2018, 2022) through the same TOML,
survey-loader, and candidate-resolution layer used by `SurveyWaveConfig` and
the nested pipeline.

This is pre-imputation data, but not necessarily untouched SPSS input: loader
recodes may already have collapsed survey nonresponse into sentinel values such
as `99.0` rather than Julia `missing`.

Returns a named tuple with raw rows, parsed config, selected candidate columns,
display labels, detected survey-weight column, and the resolved number of
alternatives. `k` is a backward-compatible alias for `m` in raw-profile calls.
"""
function load_raw_pref_data(year::Int;
                            config_path = nothing,
                            candidate_set = nothing,
                            scenario_name = nothing,
                            k = nothing,
                            m = nothing)
    cfg, _ = _load_raw_profile_cfg(year; config_path = config_path)
    return load_raw_pref_data(
        cfg;
        candidate_set = candidate_set,
        scenario_name = scenario_name,
        k = k,
        m = m,
    )
end

function load_raw_pref_data(cfg::ElectionConfig;
                            active_candidates = nothing,
                            candidate_set = nothing,
                            scenario_name = nothing,
                            k = nothing,
                            m = nothing,
                            data = nothing)
    df = data === nothing ? load_election_data(cfg) : data
    candidate_cols = _resolve_candidate_cols(
        df,
        cfg;
        active_candidates = active_candidates,
        candidate_set = candidate_set,
        scenario_name = scenario_name,
        k = k,
        m = m,
        min_allowed = 1,
    )
    return _raw_pref_tuple(cfg, df, candidate_cols; scenario_name = scenario_name)
end

function load_raw_pref_data(wcfg::SurveyWaveConfig;
                            active_candidates = nothing,
                            candidate_set = nothing,
                            scenario_name = nothing,
                            k = nothing,
                            m = nothing,
                            data = nothing)
    df = data === nothing ? load_wave_data(wcfg) : data
    candidate_cols = _resolve_candidate_cols(
        df,
        wcfg;
        active_candidates = active_candidates,
        candidate_set = candidate_set,
        scenario_name = scenario_name,
        k = k,
        m = m,
        min_allowed = 1,
    )
    return _raw_pref_tuple(wcfg, df, candidate_cols; scenario_name = scenario_name)
end

"""
    build_profile(df, year::Int; ...)
    build_profile(df, cfg::ElectionConfig; ...)
    build_profile(df, wcfg::SurveyWaveConfig; ...)

Build a `Preferences.Profile` or `Preferences.WeightedProfile` from raw survey
rows using weak rankings and the centralized PrefPol candidate-resolution
rules.

Candidate scores keep the Brazil/ESEB convention: 96-99, nonfinite values, and
scores outside `[0,10]` are unranked. Ties are preserved when
`allow_ties=true`; unranked candidates are preserved as missing when
`allow_incomplete=true`. Rows are skipped if all candidates are unranked, and
additionally skipped when `allow_incomplete=false` for partial responses. With
`weighted=true`, rows with missing or invalid weights are skipped.
"""
function build_profile(df::DataFrame, year::Int;
                       weighted::Bool = false,
                       allow_ties::Bool = true,
                       allow_incomplete::Bool = true,
                       all_unranked_as_indifferent::Bool = false,
                       candidate_set = nothing,
                       active_candidates = nothing,
                       scenario_name = nothing,
                       k = nothing,
                       m = nothing,
                       config_path = nothing)
    cfg, _ = _load_raw_profile_cfg(year; config_path = config_path)
    return build_profile(
        df,
        cfg;
        weighted = weighted,
        allow_ties = allow_ties,
        allow_incomplete = allow_incomplete,
        all_unranked_as_indifferent = all_unranked_as_indifferent,
        candidate_set = candidate_set,
        active_candidates = active_candidates,
        scenario_name = scenario_name,
        k = k,
        m = m,
    )
end

function build_profile(df::DataFrame, cfg::ElectionConfig;
                       weighted::Bool = false,
                       allow_ties::Bool = true,
                       allow_incomplete::Bool = true,
                       all_unranked_as_indifferent::Bool = false,
                       active_candidates = nothing,
                       candidate_set = nothing,
                       scenario_name = nothing,
                       k = nothing,
                       m = nothing)
    raw = load_raw_pref_data(
        cfg;
        active_candidates = active_candidates,
        candidate_set = candidate_set,
        scenario_name = scenario_name,
        k = k,
        m = m,
        data = df,
    )
    weight_col = weighted ? raw.weight_col : nothing
    return _build_profile_with_candidates(
        df,
        Vector{String}(raw.candidate_cols),
        Symbol.(String.(raw.candidate_labels));
        weighted = weighted,
        allow_ties = allow_ties,
        allow_incomplete = allow_incomplete,
        all_unranked_as_indifferent = all_unranked_as_indifferent,
        weight_col = weight_col,
    )
end

function build_profile(df::DataFrame, wcfg::SurveyWaveConfig;
                       weighted::Bool = false,
                       allow_ties::Bool = true,
                       allow_incomplete::Bool = true,
                       all_unranked_as_indifferent::Bool = false,
                       active_candidates = nothing,
                       candidate_set = nothing,
                       scenario_name = nothing,
                       k = nothing,
                       m = nothing)
    raw = load_raw_pref_data(
        wcfg;
        active_candidates = active_candidates,
        candidate_set = candidate_set,
        scenario_name = scenario_name,
        k = k,
        m = m,
        data = df,
    )
    weight_col = weighted ? raw.weight_col : nothing
    return _build_profile_with_candidates(
        df,
        Vector{String}(raw.candidate_cols),
        Symbol.(String.(raw.candidate_labels));
        weighted = weighted,
        allow_ties = allow_ties,
        allow_incomplete = allow_incomplete,
        all_unranked_as_indifferent = all_unranked_as_indifferent,
        weight_col = weight_col,
    )
end

"""
    build_profile(raw::NamedTuple; kwargs...) -> Preferences.Profile

Build a formal `Preferences.Profile` or `Preferences.WeightedProfile` from a
`load_raw_pref_data` result. This method reuses the stored raw DataFrame,
candidate columns, labels, and weight-column hint when available; scenario-based
selection falls back to the centralized survey config resolver.
"""
function build_profile(raw::NamedTuple;
                       weighted::Bool = false,
                       allow_ties::Bool = true,
                       allow_incomplete::Bool = true,
                       all_unranked_as_indifferent::Bool = false,
                       candidate_set = nothing,
                       active_candidates = nothing,
                       scenario_name = nothing,
                       k = nothing,
                       m = nothing)
    hasproperty(raw, :df) || throw(ArgumentError(
        "NamedTuple input must contain a `df` field.",
    ))

    if scenario_name !== nothing
        cfg = hasproperty(raw, :wave_config) ? raw.wave_config :
              hasproperty(raw, :cfg) ? raw.cfg :
              hasproperty(raw, :year) ? first(_load_raw_profile_cfg(Int(raw.year))) :
              throw(ArgumentError("When `scenario_name` is used, input must contain `year`, `cfg`, or `wave_config`."))
        return build_profile(
            raw.df,
            cfg;
            weighted = weighted,
            allow_ties = allow_ties,
            allow_incomplete = allow_incomplete,
            all_unranked_as_indifferent = all_unranked_as_indifferent,
            candidate_set = candidate_set,
            active_candidates = active_candidates,
            scenario_name = scenario_name,
            k = k,
            m = m,
        )
    end

    if hasproperty(raw, :candidate_cols)
        if candidate_set !== nothing && active_candidates !== nothing
            throw(ArgumentError("Pass only one of `candidate_set` or `active_candidates`."))
        end
        base_cols = Vector{String}(raw.candidate_cols)
        exact_set = active_candidates !== nothing ? active_candidates : candidate_set
        if exact_set !== nothing && (k !== nothing || m !== nothing)
            throw(ArgumentError(
                "When an exact candidate set is provided, do not pass `k` or `m`; its size defines the candidate count.",
            ))
        end
        chosen_set = if exact_set !== nothing
            exact_set
        elseif k !== nothing || m !== nothing
            kk = _normalize_k_m(
                k = k,
                m = m,
                default_m = length(base_cols),
                context = "Raw NamedTuple profile build",
            )
            kk >= 1 || throw(ArgumentError("Invalid candidate count $kk; must be >= 1."))
            kk <= length(base_cols) || throw(ArgumentError(
                "Requested candidate count $kk exceeds available candidates ($(length(base_cols))) in `raw`.",
            ))
            base_cols[1:kk]
        else
            nothing
        end
        candidate_cols = _resolve_candidate_cols_from_set(raw.df, base_cols, chosen_set)
        candidate_syms = hasproperty(raw, :candidate_labels) && chosen_set === nothing ?
                         Symbol.(String.(raw.candidate_labels)) :
                         _candidate_display_symbols(candidate_cols)
        weight_col = weighted ?
                     (hasproperty(raw, :weight_col) ? raw.weight_col : _guess_weight_col(raw.df)) :
                     nothing

        return _build_profile_with_candidates(
            raw.df,
            candidate_cols,
            candidate_syms;
            weighted = weighted,
            allow_ties = allow_ties,
            allow_incomplete = allow_incomplete,
            all_unranked_as_indifferent = all_unranked_as_indifferent,
            weight_col = weight_col,
        )
    end

    cfg = hasproperty(raw, :wave_config) ? raw.wave_config :
          hasproperty(raw, :cfg) ? raw.cfg :
          hasproperty(raw, :year) ? first(_load_raw_profile_cfg(Int(raw.year))) :
          throw(ArgumentError("NamedTuple input must contain `year`, `cfg`, or `wave_config`."))

    return build_profile(
        raw.df,
        cfg;
        weighted = weighted,
        allow_ties = allow_ties,
        allow_incomplete = allow_incomplete,
        all_unranked_as_indifferent = all_unranked_as_indifferent,
        candidate_set = candidate_set,
        active_candidates = active_candidates,
        scenario_name = scenario_name,
        k = k,
        m = m,
    )
end

