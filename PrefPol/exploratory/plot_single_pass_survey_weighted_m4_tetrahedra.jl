# %% 0. Environment and imports
ENV["MPLBACKEND"] = get(ENV, "MPLBACKEND", "Agg")

using CSV
using DataFrames
using Logging
using Random
using StaticArrays
using Statistics
using TOML

using PrefPol
using PreferenceProfiles
using PythonPlot
using VotingGeometry

const REPO_ROOT = normpath(joinpath(@__DIR__, "..", ".."))
const OUTPUT_ROOT = joinpath(
    @__DIR__,
    "output",
    "single_pass_survey_weighted_m4_tetrahedra",
)

@assert VotingGeometry.PreferenceProfiles === PreferenceProfiles

# %% 1. Repository paths and input CSVs
# The requested audit schema uses the column name `source_csv`. The repository's
# standard respondent-level sources for this analysis are the configured SPSS
# files below, loaded through PrefPol's standard year-specific loaders.
const CONFIG_RELATIVE_PATHS = Dict(
    2006 => joinpath("PrefPol", "config", "2006.toml"),
    2018 => joinpath("PrefPol", "config", "2018.toml"),
    2022 => joinpath("PrefPol", "config", "2022.toml"),
)

const STANDARD_SOURCE_RELATIVE_PATHS = Dict(
    2006 => joinpath("PrefPol", "data", "02489", "1_02489.sav"),
    2018 => joinpath("PrefPol", "data", "eseb_2018", "04622", "04622.sav"),
    2022 => joinpath("PrefPol", "data", "04810", "04810.sav"),
)

repo_path(relpath::AbstractString) = normpath(joinpath(REPO_ROOT, relpath))
config_path(year::Integer) = repo_path(CONFIG_RELATIVE_PATHS[Int(year)])
standard_source_path(year::Integer) = repo_path(STANDARD_SOURCE_RELATIVE_PATHS[Int(year)])

# %% 2. Main scenario candidate sets
const YEAR_ORDER = [2006, 2018, 2022]

const YEAR_SPECS = Dict(
    2006 => (
        scenario_name = "main_2006",
        candidates = ["Lula", "Geraldo_Alckmin", "Heloísa_Helena", "José_Serra"],
        title = "2006 survey-weighted profile, m = 4",
        figure = "opened_tetrahedron_single_pass_survey_weighted_m4_2006.png",
    ),
    2018 => (
        scenario_name = "main_2018",
        candidates = ["Fernando_Haddad", "Jair_Bolsonaro", "Ciro_Gomes", "Geraldo_Alckmin"],
        title = "2018 survey-weighted profile, m = 4",
        figure = "opened_tetrahedron_single_pass_survey_weighted_m4_2018.png",
    ),
    2022 => (
        scenario_name = "main_2022",
        candidates = ["LULA", "BOLSONARO", "CIRO_GOMES", "SIMONE_TEBET"],
        title = "2022 survey-weighted profile, m = 4",
        figure = "opened_tetrahedron_single_pass_survey_weighted_m4_2022.png",
    ),
)

# %% 3. Seeds and output paths
const MICE_SEEDS = Dict(
    2006 => 200601,
    2018 => 201801,
    2022 => 202201,
)

const LINEARIZATION_SEEDS = Dict(
    2006 => 200641,
    2018 => 201841,
    2022 => 202241,
)

const RANKING_AUDIT_CSV = joinpath(OUTPUT_ROOT, "survey_weighted_m4_ranking_proportions.csv")
const METADATA_CSV = joinpath(OUTPUT_ROOT, "single_pass_survey_weighted_m4_metadata.csv")

# %% 4. Load and validate one survey
function _scenario_prefix(wcfg, scenario_name::AbstractString, m::Integer)
    haskey(wcfg.scenario_candidates, scenario_name) ||
        throw(ArgumentError("Scenario `$scenario_name` is not configured for year $(wcfg.year)."))
    configured = wcfg.scenario_candidates[scenario_name]
    length(configured) >= m || throw(ArgumentError(
        "Scenario `$scenario_name` for year $(wcfg.year) has only $(length(configured)) candidates.",
    ))
    return first(configured, Int(m))
end

function _resolve_candidate_columns(df::DataFrame, wcfg, expected_candidates::Vector{String})
    candidate_columns = PreferenceProfiles.resolve_candidate_cols_from_set(
        df,
        wcfg.candidate_universe,
        expected_candidates,
    )
    candidate_columns == expected_candidates || throw(ArgumentError(
        "Resolved candidate columns $(candidate_columns) do not exactly match expected configured order $(expected_candidates).",
    ))
    return candidate_columns
end

function _resolve_weight_column(df::DataFrame)
    weight_col = PreferenceProfiles.guess_weight_col(df; preferred = (:peso, :weight, :weights))
    weight_col === nothing && throw(ArgumentError(
        "Could not find a survey weight column. Expected one of :peso, :weight or :weights.",
    ))
    return weight_col
end

function _weight_validation(weight_values)
    valid_mask = falses(length(weight_values))
    valid_weights = Float64[]
    sizehint!(valid_weights, length(weight_values))

    missing_weight_rows = 0
    nonnumeric_weight_rows = 0
    nonfinite_weight_rows = 0
    negative_weight_rows = 0
    zero_weight_rows = 0

    for (i, raw) in pairs(weight_values)
        if raw === missing
            missing_weight_rows += 1
            continue
        elseif !(raw isa Real)
            nonnumeric_weight_rows += 1
            continue
        end

        w = Float64(raw)
        if !isfinite(w)
            nonfinite_weight_rows += 1
        elseif w < 0.0
            negative_weight_rows += 1
        else
            valid_mask[i] = true
            push!(valid_weights, w)
            w == 0.0 && (zero_weight_rows += 1)
        end
    end

    invalid_weight_rows = missing_weight_rows + nonnumeric_weight_rows +
                          nonfinite_weight_rows + negative_weight_rows
    total_valid_weight = sum(valid_weights)
    total_valid_weight > 0.0 || throw(ArgumentError(
        "Total retained survey weight must be strictly positive after explicit weight validation.",
    ))

    return (
        valid_mask = valid_mask,
        valid_weights = valid_weights,
        missing_weight_rows = missing_weight_rows,
        nonnumeric_weight_rows = nonnumeric_weight_rows,
        nonfinite_weight_rows = nonfinite_weight_rows,
        negative_weight_rows = negative_weight_rows,
        zero_weight_rows = zero_weight_rows,
        invalid_weight_rows = invalid_weight_rows,
        total_valid_weight = total_valid_weight,
    )
end

function load_and_validate_one_survey(year::Integer)
    y = Int(year)
    spec = YEAR_SPECS[y]
    cfg_path = config_path(y)
    source_path = standard_source_path(y)
    isfile(cfg_path) || throw(ArgumentError("Config file not found: $cfg_path"))
    isfile(source_path) || throw(ArgumentError("Configured respondent-level source not found: $source_path"))

    wcfg = PrefPol.load_survey_wave_config(cfg_path; wave_id = string(y))
    @assert normpath(wcfg.data_file) == source_path
    scenario_candidates = _scenario_prefix(wcfg, spec.scenario_name, 4)
    scenario_candidates == spec.candidates || throw(ArgumentError(
        "Configured first four candidates for $(spec.scenario_name) are $(scenario_candidates), expected $(spec.candidates).",
    ))

    source_df = PrefPol.load_wave_data(wcfg)
    candidate_columns = _resolve_candidate_columns(source_df, wcfg, spec.candidates)
    weight_col = _resolve_weight_column(source_df)
    weight_summary = _weight_validation(source_df[!, weight_col])

    retained_table = source_df[weight_summary.valid_mask, :]
    survey_weight_vector = copy(weight_summary.valid_weights)
    retained_row_ids = findall(weight_summary.valid_mask)
    @assert nrow(retained_table) == length(survey_weight_vector)
    @assert length(retained_row_ids) == length(survey_weight_vector)
    @assert all(isfinite, survey_weight_vector)
    @assert all(w -> w >= 0.0, survey_weight_vector)
    @assert sum(survey_weight_vector) ≈ weight_summary.total_valid_weight

    @info "Resolved survey source before MICE" year = y source_path candidate_columns weight_col rows = nrow(source_df)
    @info "Survey weight validation" year = y missing = weight_summary.missing_weight_rows nonnumeric = weight_summary.nonnumeric_weight_rows nonfinite = weight_summary.nonfinite_weight_rows negative = weight_summary.negative_weight_rows zero = weight_summary.zero_weight_rows total_valid_weight = weight_summary.total_valid_weight retained_rows = nrow(retained_table)

    return (
        year = y,
        scenario_name = spec.scenario_name,
        title = spec.title,
        figure = spec.figure,
        config_path = cfg_path,
        wave_config = wcfg,
        source_path = source_path,
        source_df = source_df,
        retained_table = retained_table,
        retained_row_ids = retained_row_ids,
        candidate_columns = candidate_columns,
        weight_col = weight_col,
        survey_weight_vector = survey_weight_vector,
        weight_summary = weight_summary,
    )
end

# %% 5. Prepare one MICE input
function prepare_one_mice_input(state)
    prepared_mice_table = PrefPol.prepare_scores_for_imputation_categorical(
        state.retained_table,
        state.candidate_columns,
    )
    names(prepared_mice_table) == state.candidate_columns || throw(ArgumentError(
        "Prepared MICE table columns $(names(prepared_mice_table)) do not match active score columns $(state.candidate_columns).",
    ))
    @assert nrow(prepared_mice_table) == nrow(state.retained_table)
    @assert nrow(prepared_mice_table) == length(state.survey_weight_vector)
    @info "Prepared categorical MICE input" year = state.year columns = names(prepared_mice_table) rows = nrow(prepared_mice_table)
    return merge(state, (; prepared_mice_table = prepared_mice_table))
end

# %% 6. Run one MICE imputation
function _completed_score_is_valid(x)
    normalized = PrefPol.normalize_eseb_score(x)
    normalized === missing && return false
    return PrefPol.ESEB_VALID_SCORE_MIN <= normalized <= PrefPol.ESEB_VALID_SCORE_MAX
end

function _logged_event_count(report)
    report.loggedEvents === nothing && return 0
    return nrow(report.loggedEvents)
end

function _normalized_r_seed(seed::Integer)
    return Base.isdefined(PrefPol, :_normalize_r_seed) ? PrefPol._normalize_r_seed(seed) : missing
end

function run_one_mice_imputation(state; mice_seed::Integer)
    weights_before = copy(state.survey_weight_vector)
    mice_report = PrefPol.r_impute_mice_report(state.prepared_mice_table; seed = mice_seed)
    completed_score_table = DataFrame(mice_report.completed)

    @assert nrow(completed_score_table) == nrow(state.prepared_mice_table)
    @assert state.survey_weight_vector == weights_before
    for col in state.candidate_columns
        col in names(completed_score_table) || throw(ArgumentError(
            "MICE output for year $(state.year) is missing active candidate column `$col`.",
        ))
        if any(ismissing, completed_score_table[!, col])
            missing_rows = findall(ismissing, completed_score_table[!, col])
            throw(ArgumentError(
                "MICE left missing values in year $(state.year), column `$col`, rows $(first(missing_rows, min(length(missing_rows), 10))).",
            ))
        end
        all(_completed_score_is_valid, completed_score_table[!, col]) || throw(ArgumentError(
            "MICE completed scores for year $(state.year), column `$col`, are outside the ESEB score domain.",
        ))
    end

    @info "Completed one MICE imputation" year = state.year mice_seed normalized_r_seed = _normalized_r_seed(mice_seed) logged_events = _logged_event_count(mice_report) dropped_predictors = mice_report.dropped_predictors
    return merge(
        state,
        (;
            mice_seed = Int(mice_seed),
            normalized_r_seed = _normalized_r_seed(mice_seed),
            mice_report = mice_report,
            completed_score_table = completed_score_table,
        ),
    )
end

# %% 7. Construct the weighted weak profile
function _has_tie(ballot)
    present = collect(skipmissing(PreferenceProfiles.ranks(ballot)))
    return length(unique(present)) < length(present)
end

function construct_weighted_weak_profile(state)
    completed_table = DataFrame(state.completed_score_table[:, state.candidate_columns])
    completed_table[!, state.weight_col] = copy(state.survey_weight_vector)
    @assert nrow(completed_table) == length(state.survey_weight_vector)
    @assert completed_table[!, state.weight_col] == state.survey_weight_vector

    candidate_symbols = PreferenceProfiles.candidate_display_symbols(state.candidate_columns)
    weighted_weak_profile = PreferenceProfiles.build_profile_from_scores(
        completed_table,
        state.candidate_columns,
        candidate_symbols;
        weighted = true,
        weight_col = state.weight_col,
        allow_ties = true,
        allow_incomplete = false,
        score_normalizer = PrefPol.normalize_eseb_score,
    )

    @assert weighted_weak_profile isa PreferenceProfiles.WeightedProfile{<:PreferenceProfiles.WeakRank}
    @assert PreferenceProfiles.validate(weighted_weak_profile)
    @assert PreferenceProfiles.nballots(weighted_weak_profile) == nrow(completed_table)
    @assert PreferenceProfiles.weights(weighted_weak_profile) == state.survey_weight_vector
    @assert PreferenceProfiles.total_weight(weighted_weak_profile) ≈ sum(state.survey_weight_vector)
    @assert PreferenceProfiles.candidates(weighted_weak_profile.pool) == candidate_symbols
    @assert all(PreferenceProfiles.is_complete, weighted_weak_profile.ballots)
    @assert all(PreferenceProfiles.is_weak_order, weighted_weak_profile.ballots)

    tied_ballot_count = count(_has_tie, weighted_weak_profile.ballots)
    @info "Constructed weighted weak profile" year = state.year profile_type = typeof(weighted_weak_profile) ballots = PreferenceProfiles.nballots(weighted_weak_profile) total_weight = PreferenceProfiles.total_weight(weighted_weak_profile) tied_ballots = tied_ballot_count
    return merge(
        state,
        (;
            completed_table_with_weights = completed_table,
            candidate_symbols = candidate_symbols,
            weighted_weak_profile = weighted_weak_profile,
            tied_weak_ballot_count = tied_ballot_count,
        ),
    )
end

# %% 8. Run one weighted linearization
function run_one_weighted_linearization(state; linearization_seed::Integer)
    weights_before = copy(PreferenceProfiles.weights(state.weighted_weak_profile))
    pool_before = PreferenceProfiles.candidates(state.weighted_weak_profile.pool)
    total_before = PreferenceProfiles.total_weight(state.weighted_weak_profile)

    linearizer = PreferenceProfiles.PatternConditionalLinearizer(
        state.weighted_weak_profile;
        alpha = 0.5,
        fallback = :uniform,
    )
    rng = MersenneTwister(linearization_seed)
    weighted_strict_profile = PreferenceProfiles.linearize(
        state.weighted_weak_profile;
        tie_break = linearizer,
        rng = rng,
        incomplete_policy = :error,
    )

    @assert weighted_strict_profile isa PreferenceProfiles.WeightedProfile{<:PreferenceProfiles.StrictRank}
    @assert PreferenceProfiles.validate(weighted_strict_profile)
    @assert PreferenceProfiles.nballots(weighted_strict_profile) == PreferenceProfiles.nballots(state.weighted_weak_profile)
    @assert PreferenceProfiles.candidates(weighted_strict_profile.pool) == pool_before
    @assert PreferenceProfiles.weights(weighted_strict_profile) == weights_before
    @assert PreferenceProfiles.total_weight(weighted_strict_profile) ≈ total_before
    @assert all(PreferenceProfiles.is_complete, weighted_strict_profile.ballots)
    @assert all(PreferenceProfiles.is_strict, weighted_strict_profile.ballots)

    @info "Constructed weighted strict profile" year = state.year profile_type = typeof(weighted_strict_profile) linearization_seed total_weight = PreferenceProfiles.total_weight(weighted_strict_profile)
    return merge(
        state,
        (;
            linearization_seed = Int(linearization_seed),
            linearizer = linearizer,
            weighted_strict_profile = weighted_strict_profile,
        ),
    )
end

# %% 9. Calculate the 24 survey-weighted proportions
function _manual_weighted_totals(profile, basis)
    totals = zeros(Float64, length(basis.permutations))
    for (ballot, weight) in zip(profile.ballots, PreferenceProfiles.weights(profile))
        idx = basis.index[SVector{4,Int}(PreferenceProfiles.perm(ballot))]
        totals[idx] += Float64(weight)
    end
    return totals
end

function calculate_24_survey_weighted_proportions(state)
    basis = VotingGeometry.SaariBasis4(state.weighted_strict_profile.pool)
    survey_weighted_totals = VotingGeometry.profile_vector(
        state.weighted_strict_profile,
        basis;
        normalize = false,
    )
    survey_weighted_proportions = VotingGeometry.profile_vector(
        state.weighted_strict_profile,
        basis;
        normalize = true,
    )

    total_retained_weight = sum(state.survey_weight_vector)
    @assert length(survey_weighted_totals) == 24
    @assert length(survey_weighted_proportions) == 24
    @assert all(isfinite, survey_weighted_totals)
    @assert all(isfinite, survey_weighted_proportions)
    @assert all(x -> x >= 0.0, survey_weighted_totals)
    @assert all(x -> x >= 0.0, survey_weighted_proportions)
    @assert isapprox(sum(survey_weighted_totals), total_retained_weight; atol = 1e-8, rtol = 1e-10)
    @assert isapprox(sum(survey_weighted_proportions), 1.0; atol = 1e-10, rtol = 1e-10)

    manual_totals = _manual_weighted_totals(state.weighted_strict_profile, basis)
    manual_proportions = manual_totals ./ sum(manual_totals)
    @assert isapprox(manual_totals, survey_weighted_totals; atol = 1e-8, rtol = 1e-10)
    @assert isapprox(manual_proportions, survey_weighted_proportions; atol = 1e-10, rtol = 1e-10)

    ranking_labels = VotingGeometry.ranking_labels(basis; sep = " > ")
    @info "Calculated weighted ranking-type distribution" year = state.year total_weight = sum(survey_weighted_totals) proportion_sum = sum(survey_weighted_proportions)
    return merge(
        state,
        (;
            basis = basis,
            ranking_labels = ranking_labels,
            survey_weighted_totals = survey_weighted_totals,
            survey_weighted_proportions = survey_weighted_proportions,
            manual_survey_weighted_totals = manual_totals,
            manual_survey_weighted_proportions = manual_proportions,
            manual_matches_votinggeometry = true,
        ),
    )
end

# %% 10. Plot the opened tetrahedron
function _plot_labels(profile)
    return Tuple(String.(PreferenceProfiles.candidates(profile.pool)))
end

function plot_opened_tetrahedron(state)
    mkpath(OUTPUT_ROOT)
    fig_path = joinpath(OUTPUT_ROOT, state.figure)
    ax = VotingGeometry.plot_profile_tetrahedron_proportions(
        state.survey_weighted_proportions;
        labels = _plot_labels(state.weighted_strict_profile),
        normalize = false,
        plot_percentages = true,
        digits = 2,
        textsize = 8,
        title = state.title,
    )
    ax.figure.savefig(String(fig_path); dpi = 220, bbox_inches = "tight")
    @info "Wrote opened tetrahedron figure" year = state.year fig_path
    return merge(state, (; tetrahedron_axis = ax, figure_path = fig_path))
end

# %% 11. Write audit outputs
function _method_map_string(meth::AbstractDict)
    return join(["$k=$v" for (k, v) in sort(collect(meth); by = first)], ";")
end

function _dropped_predictors_string(x)
    return join(String.(collect(x)), ";")
end

function ranking_audit_table(results)
    rows = NamedTuple[]
    for state in results
        for idx in 1:24
            push!(rows, (
                year = state.year,
                scenario_name = state.scenario_name,
                m = 4,
                rank_index = idx,
                ranking = state.ranking_labels[idx],
                survey_weighted_total = state.survey_weighted_totals[idx],
                survey_weighted_proportion = state.survey_weighted_proportions[idx],
                survey_weighted_percent = 100 * state.survey_weighted_proportions[idx],
                candidate_1 = state.candidate_columns[1],
                candidate_2 = state.candidate_columns[2],
                candidate_3 = state.candidate_columns[3],
                candidate_4 = state.candidate_columns[4],
                source_csv = state.source_path,
                source_weight_column = String(state.weight_col),
                mice_seed = state.mice_seed,
                linearization_seed = state.linearization_seed,
                imputer = "mice",
                linearizer = "pattern_conditional(alpha=0.5,fallback=uniform)",
            ))
        end
    end
    return DataFrame(rows)
end

function metadata_table(results)
    rows = NamedTuple[]
    for state in results
        ws = state.weight_summary
        notes = join([
            "configured_sav_source",
            "normalized_r_seed=$(state.normalized_r_seed)",
            "nonnumeric_weight_rows=$(ws.nonnumeric_weight_rows)",
            "tied_weak_ballots=$(state.tied_weak_ballot_count)",
        ], ";")
        push!(rows, (
            year = state.year,
            scenario_name = state.scenario_name,
            m = 4,
            candidate_set = join(state.candidate_columns, "|"),
            source_csv = state.source_path,
            source_weight_column = String(state.weight_col),
            input_rows = nrow(state.source_df),
            retained_rows = length(state.survey_weight_vector),
            zero_weight_rows = ws.zero_weight_rows,
            invalid_weight_rows = ws.invalid_weight_rows,
            total_input_valid_weight = ws.total_valid_weight,
            total_retained_weight = sum(state.survey_weight_vector),
            mice_seed = state.mice_seed,
            mice_method_map = _method_map_string(state.mice_report.meth),
            mice_logged_event_count = _logged_event_count(state.mice_report),
            mice_dropped_predictors = _dropped_predictors_string(state.mice_report.dropped_predictors),
            linearization_seed = state.linearization_seed,
            linearizer = "pattern_conditional(alpha=0.5,fallback=uniform)",
            weak_profile_type = string(typeof(state.weighted_weak_profile)),
            strict_profile_type = string(typeof(state.weighted_strict_profile)),
            proportion_sum = sum(state.survey_weighted_proportions),
            notes = notes,
        ))
    end
    return DataFrame(rows)
end

function write_audit_outputs(results)
    mkpath(OUTPUT_ROOT)
    ranking_df = ranking_audit_table(results)
    metadata_df = metadata_table(results)
    CSV.write(RANKING_AUDIT_CSV, ranking_df)
    CSV.write(METADATA_CSV, metadata_df)
    @info "Wrote aggregate audit outputs" ranking_csv = RANKING_AUDIT_CSV metadata_csv = METADATA_CSV
    return (ranking_table = ranking_df, metadata_table = metadata_df)
end

# %% 12. Run all three years
function analyze_one_year(year::Integer)
    y = Int(year)
    state = load_and_validate_one_survey(y)
    state = prepare_one_mice_input(state)
    state = run_one_mice_imputation(state; mice_seed = MICE_SEEDS[y])
    state = construct_weighted_weak_profile(state)
    state = run_one_weighted_linearization(state; linearization_seed = LINEARIZATION_SEEDS[y])
    state = calculate_24_survey_weighted_proportions(state)
    state = plot_opened_tetrahedron(state)
    return state
end

function run_all_years(years = YEAR_ORDER)
    results = [analyze_one_year(year) for year in years]
    audit_outputs = write_audit_outputs(results)
    PythonPlot.close("all")
    return (by_year = Dict(state.year => state for state in results), audit_outputs = audit_outputs)
end

if abspath(PROGRAM_FILE) == @__FILE__
    results = run_all_years()
end
