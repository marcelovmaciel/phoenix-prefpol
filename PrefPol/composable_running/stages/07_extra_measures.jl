#!/usr/bin/env julia

const PREFPOL_PROJECT_DIR = normpath(joinpath(@__DIR__, "..", ".."))
PREFPOL_PROJECT_DIR in LOAD_PATH || pushfirst!(LOAD_PATH, PREFPOL_PROJECT_DIR)

include(joinpath(@__DIR__, "04_measures.jl"))

using JLD2
using SHA
using Statistics

const prefs = pp.Preferences
const DEFAULT_EXTRA_M_VALUES = collect(2:5)

function parse_extra_measure_args(args)
    if any(arg -> arg in ("--help", "-h"), args)
        println("""
        Usage:
          julia +1.11.9 --project=PrefPol PrefPol/composable_running/stages/07_extra_measures.jl [--config PATH] [--year YEAR] [--scenario NAME] [--m VALUE_OR_RANGE] [--backend NAME] [--linearizer NAME] [--force] [--dry-run] [--smoke-test]

        Phase 8:
          Reads cached PipelineResult linearized artifacts and writes ranking
          support diagnostics, effective-count diagnostics, and
          extra_measure_manifest.csv.
        """)
        exit(0)
    end
    return parse_args(args)
end

function extra_measure_settings(cfg, opts)
    run_cfg = get(cfg, "run", Dict{String,Any}())
    extra_cfg = get(cfg, "extra_measures", Dict{String,Any}())
    output_root = resolve_path(String(config_value(extra_cfg, "output_root",
                                 config_value(run_cfg, "output_root", DEFAULT_OUTPUT_ROOT))))
    m_values = parse_m_values(opts["m"])
    m_values === nothing && (m_values = Int.(config_value(extra_cfg, "m_values", DEFAULT_EXTRA_M_VALUES)))

    return (
        output_root = output_root,
        run_manifest = resolve_path(String(config_value(extra_cfg, "run_manifest",
                               joinpath(output_root, "manifests", "run_manifest.csv")))),
        extra_measure_manifest = resolve_path(String(config_value(extra_cfg, "extra_measure_manifest",
                                        joinpath(output_root, "manifests", "extra_measure_manifest.csv")))),
        output_dir = resolve_path(String(config_value(extra_cfg, "output_dir",
                             joinpath(output_root, "extra_measures")))),
        analysis_role = String(config_value(extra_cfg, "analysis_role", "main")),
        m_values = Set(Int.(m_values)),
        backend_filter = opts["backend"] === nothing ? nothing : Set([String(opts["backend"])]),
        linearizer_filter = opts["linearizer"] === nothing ? nothing : Set([String(opts["linearizer"])]),
        year_filter = opts["year"] === nothing ? nothing : String(opts["year"]),
        scenario_filter = opts["scenario"] === nothing ? nothing : String(opts["scenario"]),
        force = Bool(opts["force"]) || Bool(config_value(extra_cfg, "force", false)),
        dry_run = Bool(opts["dry-run"]) || Bool(config_value(extra_cfg, "dry_run", false)),
    )
end

function stage_is(row, name::Symbol)
    return Symbol(row.stage) === name
end

function linearized_stage_rows(result::pp.PipelineResult)
    rows = result.stage_manifest[[stage_is(row, :linearized) for row in eachrow(result.stage_manifest)], :]
    isempty(rows) && error("PipelineResult at $(result.cache_dir) has no cached linearized profile artifacts.")
    return rows
end

function strict_profile_from_linearized(path::AbstractString)
    isfile(path) || error("Cached linearized profile artifact not found: $(path)")
    artifact = JLD2.load(path, "artifact")
    artifact isa AbstractDataFrame || error(
        "Expected cached linearized artifact $(path) to be a DataFrame; got $(typeof(artifact)).",
    )
    return pp.dataframe_to_annotated_profile(artifact; ballot_kind = :strict).profile
end

function ranking_masses(profile)
    masses = Dict{Tuple,Float64}()
    order = Tuple[]
    weights = hasproperty(profile, :weights) ? Float64.(profile.weights) :
              ones(Float64, prefs.nballots(profile))

    for (ballot, weight) in zip(profile.ballots, weights)
        sig = prefs.ranking_signature(ballot, profile.pool)
        haskey(masses, sig) || push!(order, sig)
        masses[sig] = get(masses, sig, 0.0) + weight
    end

    return masses, order, sum(weights)
end

function effective_number_stats(profile)
    masses, order, total = ranking_masses(profile)
    total > 0 || error("Linearized profile has zero ranking mass.")

    ranking_probs = [mass / total for mass in values(masses)]
    HHI_rankings = sum(p^2 for p in ranking_probs)
    EO = 1.0 / HHI_rankings

    paired, _ = prefs.reversal_pairs(order)
    reversal_values = Float64[2.0 * min(masses[pair[1]], masses[pair[3]]) / total for pair in paired]
    reversal_total = sum(reversal_values)
    HHI_reversal = reversal_total == 0.0 ? missing :
                   sum((value / reversal_total)^2 for value in reversal_values)
    ENRP = HHI_reversal === missing ? missing : 1.0 / HHI_reversal
    m = length(profile.pool)

    return (
        ENRP = ENRP,
        EO = EO,
        reversal_to_ranking_effective_ratio = EO == 0.0 || ENRP === missing ? missing : ENRP / EO,
        HHI_reversal = HHI_reversal,
        HHI_rankings = HHI_rankings,
        n_rankings_observed = length(masses),
        n_reversal_pairs_observed = count(>(0.0), reversal_values),
        max_rankings_possible = factorial(m),
        max_reversal_pairs_possible = div(factorial(m), 2),
    )
end

function base_draw_metadata(row, stage)
    return (
        batch_index = Int(row.batch_index),
        analysis_role = string(row.analysis_role),
        wave_id = string(row.wave_id),
        year = Int(row.year),
        scenario_name = string(row.scenario_name),
        m = Int(row.m),
        active_candidates = string(row.active_candidates),
        imputer_backend = string(row.imputer_backend),
        linearizer_policy = string(row.linearizer_policy),
        B = Int(row.B),
        R = Int(row.R),
        K = Int(row.K),
        b = Int(stage.b),
        r = Int(stage.r),
        k = Int(stage.k),
        scenario_dir = :scenario_dir in propertynames(row) ? string(row.scenario_dir) : "",
        cache_dir = string(row.cache_dir),
        linearized_path = string(stage.path),
    )
end

function selected_manifest_rows(manifest::DataFrame, settings)
    rows = manifest
    :status in propertynames(rows) && (rows = rows[string.(rows.status) .== "success", :])
    rows = rows[
        (string.(rows.analysis_role) .== settings.analysis_role) .&
        in.(Int.(rows.m), Ref(settings.m_values)),
        :,
    ]
    settings.year_filter !== nothing && (rows = rows[string.(rows.wave_id) .== settings.year_filter, :])
    settings.scenario_filter !== nothing && (rows = rows[string.(rows.scenario_name) .== settings.scenario_filter, :])
    settings.backend_filter !== nothing && (rows = rows[in.(string.(rows.imputer_backend), Ref(settings.backend_filter)), :])
    settings.linearizer_filter !== nothing && (rows = rows[in.(string.(rows.linearizer_policy), Ref(settings.linearizer_filter)), :])
    isempty(rows) && error("No run manifest rows matched extra-measure filters.")
    return sort(rows, [:year, :scenario_name, :m, :imputer_backend, :linearizer_policy])
end

function parse_manifest_columns!(manifest::DataFrame)
    for col in (:year, :m, :B, :R, :K, :n_candidates, :batch_index)
        col in propertynames(manifest) || continue
        manifest[!, col] = Int.(manifest[!, col])
    end
    return manifest
end

function read_manifest(path::AbstractString)
    isfile(path) || error("Required manifest not found: $(path)")
    df = CSV.read(path, DataFrame)
    isempty(df) && error("Manifest is empty: $(path)")
    return df
end

manifest_hash(path::AbstractString) = bytes2hex(SHA.sha256(read(path)))

function validate_ranking_support_row(row)
    possible = factorial(Int(row.m))
    row.possible_rankings == possible || error("Expected $(possible) possible rankings for m=$(row.m).")
    for col in (
        :unique_share_of_possible, :unique_share_of_observations,
        :singleton_share_of_unique, :singleton_share_of_observations,
        :max_ranking_mass, :effective_share_of_possible, :support_saturation,
    )
        value = Float64(row[col])
        (0.0 <= value <= 1.0) || error("Diagnostic proportion $(col)=$(value) is outside [0, 1].")
    end
    return true
end

function build_draw_tables(manifest::DataFrame)
    ranking_rows = NamedTuple[]
    effective_rows = NamedTuple[]

    for row in eachrow(manifest)
        result = pp.load_pipeline_result(String(row.result_path))
        for stage in eachrow(linearized_stage_rows(result))
            profile = strict_profile_from_linearized(String(stage.path))
            base = base_draw_metadata(row, stage)

            ranking = merge(base, prefs.ranking_support_diagnostics(profile; m = Int(row.m)),
                            (diagnostic = "ranking_support",))
            validate_ranking_support_row(ranking)
            push!(ranking_rows, ranking)

            push!(effective_rows, merge(base, effective_number_stats(profile)))
        end
    end

    isempty(ranking_rows) && error("No ranking-support diagnostic rows were produced.")
    isempty(effective_rows) && error("No effective-count diagnostic rows were produced.")
    return DataFrame(ranking_rows), DataFrame(effective_rows)
end

function summarize_quantiles(draws::DataFrame, group_cols, metric_cols)
    rows = NamedTuple[]
    for subdf in groupby(draws, group_cols)
        base = NamedTuple(name => subdf[1, name] for name in group_cols)
        stats = (n_draws = nrow(subdf),)
        for metric in metric_cols
            values = collect(skipmissing(subdf[!, metric]))
            values_f = Float64.(values)
            stats = merge(stats, NamedTuple{(
                Symbol(metric, "_mean"), Symbol(metric, "_median"),
                Symbol(metric, "_q25"), Symbol(metric, "_q75"),
                Symbol(metric, "_q05"), Symbol(metric, "_q95"),
                Symbol(metric, "_missing"),
            )}(isempty(values_f) ? (
                missing, missing, missing, missing, missing, missing, nrow(subdf)
            ) : (
                mean(values_f), median(values_f), quantile(values_f, 0.25),
                quantile(values_f, 0.75), quantile(values_f, 0.05),
                quantile(values_f, 0.95), nrow(subdf) - length(values_f)
            )))
        end
        push!(rows, merge(base, stats))
    end
    return sort(DataFrame(rows), group_cols)
end

function summarize_ranking_support(draws::DataFrame)
    metric_cols = [
        :n_observations, :possible_rankings, :n_unique_rankings,
        :unique_share_of_possible, :unique_share_of_observations,
        :singleton_rankings, :singleton_share_of_unique,
        :singleton_observation_count, :singleton_share_of_observations,
        :max_ranking_mass, :EO, :effective_share_of_possible,
        :support_saturation, :sparsity_pressure,
    ]
    group_cols = [
        :analysis_role, :wave_id, :year, :scenario_name,
        :imputer_backend, :linearizer_policy, :m, :active_candidates, :diagnostic,
    ]
    rows = NamedTuple[]
    for subdf in groupby(draws, group_cols)
        base = NamedTuple(name => subdf[1, name] for name in group_cols)
        for metric in metric_cols
            values = Float64.(subdf[!, metric])
            push!(rows, merge(base, (
                metric = String(metric),
                n_draws = length(values),
                median = quantile(values, 0.50),
                q25 = quantile(values, 0.25),
                q75 = quantile(values, 0.75),
                q05 = quantile(values, 0.05),
                q95 = quantile(values, 0.95),
            )))
        end
    end
    return sort(DataFrame(rows), vcat(group_cols, [:metric]))
end

function summarize_effective_counts(draws::DataFrame)
    group_cols = [
        :analysis_role, :wave_id, :year, :scenario_name,
        :imputer_backend, :linearizer_policy, :m, :active_candidates,
        :B, :R, :K,
    ]
    summary = summarize_quantiles(
        draws,
        group_cols,
        [:ENRP, :EO, :reversal_to_ranking_effective_ratio, :HHI_reversal, :HHI_rankings],
    )
    means = combine(groupby(draws, group_cols),
        :n_rankings_observed => (x -> mean(Float64.(x))) => :n_rankings_observed_mean,
        :n_reversal_pairs_observed => (x -> mean(Float64.(x))) => :n_reversal_pairs_observed_mean,
        :max_rankings_possible => first => :max_rankings_possible,
        :max_reversal_pairs_possible => first => :max_reversal_pairs_possible,
    )
    return sort(innerjoin(summary, means; on = group_cols), group_cols)
end

function manifest_rows(output_paths, input_path)
    source_hash = isfile(input_path) ? manifest_hash(input_path) : ""
    return DataFrame([(
        stage = "extra_measures",
        artifact_id = splitext(basename(path))[1],
        input_path = input_path,
        output_path = path,
        format = lowercase(splitext(path)[2][2:end]),
        source_manifest_hash = source_hash,
        status = "success",
        error = "",
        timestamp = Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS"),
    ) for path in output_paths])
end

function main(args = ARGS)
    opts = parse_extra_measure_args(args)
    cfg = load_orchestration_config(opts["config"])
    settings = extra_measure_settings(cfg, opts)

    println("Extra measure stage plan:")
    println("  run_manifest=", settings.run_manifest)
    println("  output_dir=", settings.output_dir)
    println("  analysis_role=", settings.analysis_role, " m=", join(sort(collect(settings.m_values)), ","))
    println("  force=", settings.force, " dry_run=", settings.dry_run)
    settings.dry_run && return nothing

    manifest = parse_manifest_columns!(read_manifest(settings.run_manifest))
    rows = selected_manifest_rows(manifest, settings)
    ranking_draws, effective_draws = build_draw_tables(rows)
    ranking_summary = summarize_ranking_support(ranking_draws)
    effective_summary = summarize_effective_counts(effective_draws)

    ranking_dir = joinpath(settings.output_dir, "ranking_support")
    effective_dir = joinpath(settings.output_dir, "effective_counts")
    paths = [
        joinpath(ranking_dir, "ranking_support_draws.csv"),
        joinpath(ranking_dir, "ranking_support_summary.csv"),
        joinpath(effective_dir, "effective_counts_draws.csv"),
        joinpath(effective_dir, "effective_counts_summary.csv"),
    ]
    write_csv(paths[1], sorted_table(ranking_draws))
    write_csv(paths[2], sorted_table(ranking_summary))
    write_csv(paths[3], sorted_table(effective_draws))
    write_csv(paths[4], sorted_table(effective_summary))
    write_csv(settings.extra_measure_manifest, manifest_rows(paths, settings.run_manifest))

    println("Wrote extra measures under ", settings.output_dir)
    println("Wrote extra measure manifest to ", settings.extra_measure_manifest)
    return nothing
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
