const RAW_PROFILE_SUPPORTED_YEARS = Set([2006, 2018, 2022])
const _RAW_PROFILE_CFG_DIR = joinpath(project_root, "config")

"""
    _ensure_preferences_module()

Validate that local `Preferences` is loaded and exposes the generic tabular
profile-building utilities required by this file.
"""
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

"""
    _load_raw_profile_cfg(year; config_path=nothing) -> (cfg, path)

Load the election TOML for `year` using the existing PrefPol config parser.
Only 2006, 2018 and 2022 are supported by this raw-profile helper.
"""
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

@inline _infer_m(cfg) = cfg.n_alternatives > 0 ? cfg.n_alternatives : cfg.max_candidates

# Generic candidate-label helpers live in Preferences.
@inline _humanize_candidate_name(name::AbstractString) = _prefs().humanize_candidate_name(name)
@inline _canonical_candidate_key(x) = _prefs().canonical_candidate_key(x)

function _candidate_display_symbols(candidate_cols::Vector{String})
    return _prefs().candidate_display_symbols(candidate_cols)
end

function _guess_weight_col(df::DataFrame)
    return _prefs().guess_weight_col(df; preferred = (:peso, :weight, :weights))
end

function _resolve_candidate_cols_from_set(df::DataFrame,
                                          universe_cols::Vector{String},
                                          candidate_set)
    return _prefs().resolve_candidate_cols_from_set(df, universe_cols, candidate_set)
end

function _scenario_for_year(cfg, scenario_name)::Scenario
    sname = String(scenario_name)
    for sc in cfg.scenarios
        sc.name == sname && return sc
    end
    available = [sc.name for sc in cfg.scenarios]
    throw(ArgumentError(
        "Unknown scenario `$sname` for year $(cfg.year). Available scenarios: $(available).",
    ))
end

function _resolve_candidate_cols(df::DataFrame, cfg;
                                 candidate_set = nothing,
                                 scenario_name = nothing,
                                 m = nothing)
    (candidate_set !== nothing && scenario_name !== nothing) && throw(ArgumentError(
        "Pass only one of `candidate_set` or `scenario_name`.",
    ))

    configured_universe = Vector{String}(cfg.candidates)

    if scenario_name !== nothing
        sc = _scenario_for_year(cfg, scenario_name)
        mm = m === nothing ? _infer_m(cfg) : Int(m)
        n_forced = length(unique(sc.candidates))
        (n_forced > mm) && throw(ArgumentError(
            "Requested k/m = $mm is smaller than forced candidates in scenario " *
            "`$(sc.name)` ($n_forced). Increase k/m or choose another scenario.",
        ))
        return compute_candidate_set(df;
                                     candidate_cols = configured_universe,
                                     m = mm,
                                     force_include = Vector{String}(sc.candidates))
    end

    if candidate_set !== nothing
        return _resolve_candidate_cols_from_set(df, configured_universe, candidate_set)
    end

    mm = m === nothing ? _infer_m(cfg) : Int(m)
    return compute_candidate_set(df;
                                 candidate_cols = configured_universe,
                                 m = mm)
end

# PrefPol keeps ESEB score conventions local and injects this into Preferences.
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

"""
    load_raw_pref_data(year::Int;
                       config_path=nothing,
                       candidate_set=nothing,
                       scenario_name=nothing,
                       k=nothing)

Load raw survey data for `year` (2006, 2018, 2022) through the existing
PrefPol TOML + loader pipeline, before bootstrap/imputation.

This is pre-imputation data, but not necessarily untouched SPSS input: loader
recodes may already have collapsed survey nonresponse into sentinel values such
as `99.0` rather than Julia `missing`.

Returns a named tuple with:
- `df`: raw loaded DataFrame
- `cfg`: parsed `ElectionConfig`
- `m`: number of alternatives inferred from TOML (`n_alternatives` fallback `max_candidates`)
  or `length(candidate_set)` when `candidate_set` is provided
- `candidate_cols`: selected candidate columns used to build profiles
- `candidate_labels`: human-readable labels for those candidates
- `weight_col`: detected survey-weight column (`nothing` if absent)

Selection rules:
- `candidate_set`: use exactly this subset (`k = length(candidate_set)`).
- `scenario_name`: use TOML `forced_scenarios` behavior (force-include scenario
  candidates and fill up to year `m` via `compute_candidate_set`).
- `k`: optional override for how many candidates to keep when using
  `scenario_name` (or default selection with no scenario).
"""
function load_raw_pref_data(year::Int;
                            config_path = nothing,
                            candidate_set = nothing,
                            scenario_name = nothing,
                            k = nothing)
    cfg, cfg_path = _load_raw_profile_cfg(year; config_path = config_path)
    df = load_election_data(cfg)

    (candidate_set !== nothing && scenario_name !== nothing) && throw(ArgumentError(
        "Pass only one of `candidate_set` or `scenario_name`.",
    ))
    (candidate_set !== nothing && k !== nothing) && throw(ArgumentError(
        "When `candidate_set` is provided, do not pass `k`; its size defines k.",
    ))

    m = if candidate_set !== nothing
        length(collect(candidate_set))
    elseif k !== nothing
        Int(k)
    else
        _infer_m(cfg)
    end
    m >= 1 || throw(ArgumentError("Invalid m=$m for year $year in `$cfg_path`."))

    candidate_cols = _resolve_candidate_cols(df, cfg;
                                             candidate_set = candidate_set,
                                             scenario_name = scenario_name,
                                             m = m)
    isempty(candidate_cols) && throw(ArgumentError(
        "Candidate set inference failed for year $year.",
    ))

    missing_cols = setdiff(candidate_cols, names(df))
    isempty(missing_cols) || throw(ArgumentError(
        "Missing candidate mapping/columns in data for year $year: $(missing_cols).",
    ))

    candidate_syms = _candidate_display_symbols(candidate_cols)
    weight_col = _guess_weight_col(df)

    return (
        year = year,
        cfg = cfg,
        df = df,
        m = m,
        candidate_cols = candidate_cols,
        candidate_labels = String.(candidate_syms),
        weight_col = weight_col,
        scenario_name = scenario_name === nothing ? nothing : String(scenario_name),
    )
end

"""
    build_profile(df, year::Int;
                  weighted::Bool=false,
                  allow_ties::Bool=true,
                  allow_incomplete::Bool=true,
                  all_unranked_as_indifferent::Bool=false,
                  candidate_set=nothing,
                  scenario_name=nothing,
                  k=nothing,
                  config_path=nothing)

Build a `Preferences.Profile` or `Preferences.WeightedProfile` from raw survey
rows using weak rankings:
- ties are preserved when `allow_ties=true`;
- unranked candidates are preserved as missing when `allow_incomplete=true`.

Rows are skipped if all candidates are unranked, and additionally skipped when
`allow_incomplete=false` for partial responses. With `weighted=true`, rows with
missing/invalid weights are skipped.

If `all_unranked_as_indifferent=true`, rows with no ranked candidates are kept
as an all-indifferent weak ballot (all candidates tied).

Candidate selection:
- `candidate_set`: exact subset.
- `scenario_name`: scenario force-include + fill to year `m`.
- `k`: optional override for the number of candidates when using
  `scenario_name` (or default selection).
"""
function build_profile(df::DataFrame, year::Int;
                       weighted::Bool = false,
                       allow_ties::Bool = true,
                       allow_incomplete::Bool = true,
                       all_unranked_as_indifferent::Bool = false,
                       candidate_set = nothing,
                       scenario_name = nothing,
                       k = nothing,
                       config_path = nothing)
    cfg, _ = _load_raw_profile_cfg(year; config_path = config_path)
    (candidate_set !== nothing && scenario_name !== nothing) && throw(ArgumentError(
        "Pass only one of `candidate_set` or `scenario_name`.",
    ))
    (candidate_set !== nothing && k !== nothing) && throw(ArgumentError(
        "When `candidate_set` is provided, do not pass `k`; its size defines k.",
    ))
    m = if candidate_set !== nothing
        length(collect(candidate_set))
    elseif k !== nothing
        Int(k)
    else
        _infer_m(cfg)
    end
    m >= 1 || throw(ArgumentError("Invalid k/m = $m for year $year."))

    candidate_cols = _resolve_candidate_cols(df, cfg;
                                             candidate_set = candidate_set,
                                             scenario_name = scenario_name,
                                             m = m)
    isempty(candidate_cols) && throw(ArgumentError(
        "Candidate set inference failed for year $year.",
    ))

    missing_cols = setdiff(candidate_cols, names(df))
    isempty(missing_cols) || throw(ArgumentError(
        "Missing candidate mapping/columns in data for year $year: $(missing_cols).",
    ))

    candidate_syms = _candidate_display_symbols(candidate_cols)
    weight_col = weighted ? _guess_weight_col(df) : nothing

    return _build_profile_with_candidates(
        df,
        candidate_cols,
        candidate_syms;
        weighted = weighted,
        allow_ties = allow_ties,
        allow_incomplete = allow_incomplete,
        all_unranked_as_indifferent = all_unranked_as_indifferent,
        weight_col = weight_col,
    )
end

function build_profile(raw::NamedTuple;
                       weighted::Bool = false,
                       allow_ties::Bool = true,
                       allow_incomplete::Bool = true,
                       all_unranked_as_indifferent::Bool = false,
                       candidate_set = nothing,
                       scenario_name = nothing,
                       k = nothing)
    hasproperty(raw, :df) || throw(ArgumentError(
        "NamedTuple input must contain a `df` field.",
    ))

    (candidate_set !== nothing && scenario_name !== nothing) && throw(ArgumentError(
        "Pass only one of `candidate_set` or `scenario_name`.",
    ))
    (candidate_set !== nothing && k !== nothing) && throw(ArgumentError(
        "When `candidate_set` is provided, do not pass `k`; its size defines k.",
    ))

    if scenario_name !== nothing
        year = hasproperty(raw, :year) ? Int(raw.year) :
               hasproperty(raw, :cfg) ? Int(raw.cfg.year) :
               throw(ArgumentError("When `scenario_name` is used, input must contain `year` or `cfg`."))
        return build_profile(raw.df, year;
                             weighted = weighted,
                             allow_ties = allow_ties,
                             allow_incomplete = allow_incomplete,
                             all_unranked_as_indifferent = all_unranked_as_indifferent,
                             scenario_name = scenario_name,
                             k = k)
    end

    if hasproperty(raw, :candidate_cols)
        base_cols = Vector{String}(raw.candidate_cols)
        chosen_set = if candidate_set !== nothing
            candidate_set
        elseif k !== nothing
            kk = Int(k)
            kk >= 1 || throw(ArgumentError("Invalid k = $kk; must be >= 1."))
            kk <= length(base_cols) || throw(ArgumentError(
                "Requested k = $kk exceeds available candidates ($(length(base_cols))) in `raw`.",
            ))
            base_cols[1:kk]
        else
            nothing
        end
        candidate_cols = _resolve_candidate_cols_from_set(raw.df, base_cols, chosen_set)
        candidate_syms = hasproperty(raw, :candidate_labels) ?
            Symbol.(String.(raw.candidate_labels)) :
            _candidate_display_symbols(candidate_cols)
        if chosen_set !== nothing
            candidate_syms = _candidate_display_symbols(candidate_cols)
        end
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

    year = hasproperty(raw, :year) ? Int(raw.year) :
           hasproperty(raw, :cfg) ? Int(raw.cfg.year) :
           throw(ArgumentError("NamedTuple input must contain `year` or `cfg`."))

    return build_profile(raw.df, year;
                         weighted = weighted,
                         allow_ties = allow_ties,
                         allow_incomplete = allow_incomplete,
                         all_unranked_as_indifferent = all_unranked_as_indifferent,
                         candidate_set = candidate_set,
                         scenario_name = scenario_name,
                         k = k)
end

# Generic pattern summarization/formatting now lives in Preferences.
function profile_pattern_proportions(profile; weighted::Bool = true,
                                     candidate_set = nothing)
    return _prefs().profile_pattern_proportions(
        profile;
        weighted = weighted,
        candidate_set = candidate_set,
    )
end

ranked_count(pattern::AbstractString) = _prefs().ranked_count(pattern)
ranked_count(blocks::AbstractVector{<:Integer}) = _prefs().ranked_count(blocks)

has_ties(pattern::AbstractString) = _prefs().has_ties(pattern)
has_ties(blocks::AbstractVector{<:Integer}) = _prefs().has_ties(blocks)

ranking_type_support(r::Int) = _prefs().ranking_type_support(r)
ranking_type_template(blocks::AbstractVector{<:Integer}) = _prefs().ranking_type_template(blocks)

function profile_ranksize_summary(profile; k::Int, weighted::Bool = true,
                                  include_zero_rank::Bool = true)
    return _prefs().profile_ranksize_summary(
        profile;
        k = k,
        weighted = weighted,
        include_zero_rank = include_zero_rank,
    )
end

function profile_ranking_type_proportions(profile; k::Int, weighted::Bool = true,
                                          include_zero_rank::Bool = true)
    return _prefs().profile_ranking_type_proportions(
        profile;
        k = k,
        weighted = weighted,
        include_zero_rank = include_zero_rank,
    )
end

function pretty_print_ranksize_summary(summary; digits::Int = 4, io::IO = stdout)
    return _prefs().pretty_print_ranksize_summary(summary; digits = digits, io = io)
end

function pretty_print_ranking_type_proportions(type_tbl; digits::Int = 4, io::IO = stdout)
    return _prefs().pretty_print_ranking_type_proportions(type_tbl; digits = digits, io = io)
end

function pretty_print_profile_patterns(tbl;
                                       digits::Int = 4,
                                       io::IO = stdout,
                                       others_threshold = nothing,
                                       others_label::AbstractString = "Others")
    return _prefs().pretty_print_profile_patterns(
        tbl;
        digits = digits,
        io = io,
        others_threshold = others_threshold,
        others_label = others_label,
    )
end
