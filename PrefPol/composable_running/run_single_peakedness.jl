#!/usr/bin/env julia

# Run from the repository root, for example:
#
#   julia +1.11.9 --project=PrefPol \
#     PrefPol/composable_running/run_single_peakedness.jl \
#     --config PrefPol/config/single_peakedness.toml
#
# The script reads the existing wave/orchestration conventions, ensures the
# selected linearized BRK artifacts exist, calls Preferences.single_peakedness_summary,
# and writes axis, best-axis, support, and row-classification CSVs under the
# configured single_peakedness.output_dir. Row classifications use profile_row_id;
# if upstream respondent IDs are later preserved in linearized artifacts, they
# can be joined or emitted from the metadata columns handled below.

const PREFPOL_PROJECT_DIR = normpath(joinpath(@__DIR__, ".."))
PREFPOL_PROJECT_DIR in LOAD_PATH || pushfirst!(LOAD_PATH, PREFPOL_PROJECT_DIR)

include(joinpath(@__DIR__, "stages", "04_measures.jl"))

using CSV
using DataFrames
using Dates
using JLD2
using SHA

const prefs = pp.Preferences

function parse_single_peakedness_args(args)
    opts = parse_args(String[])
    opts["b"] = nothing
    opts["r"] = nothing
    opts["k"] = nothing

    i = 1
    while i <= length(args)
        arg = args[i]
        if arg in ("--force", "--dry-run", "--smoke-test")
            opts[arg[3:end]] = true
        elseif arg in ("--config", "--year", "--scenario", "--m", "--backend", "--linearizer", "--b", "--r", "--k")
            i == length(args) && error("$(arg) requires a value.")
            opts[arg[3:end]] = args[i + 1]
            i += 1
        elseif arg in ("--help", "-h")
            println("""
            Usage:
              julia +1.11.9 --project=PrefPol PrefPol/composable_running/run_single_peakedness.jl [--config PATH] [--year YEAR] [--scenario NAME] [--m VALUE_OR_RANGE] [--backend NAME] [--linearizer NAME] [--b LIST] [--r LIST] [--k LIST] [--force] [--dry-run] [--smoke-test]

            Computes Preferences single-peakedness L0 and normalized L1 for
            selected linearized BRK profiles and writes CSV outputs.
            """)
            exit(0)
        else
            error("Unknown argument: $(arg)")
        end
        i += 1
    end
    return opts
end

function parse_int_list(raw)
    raw === nothing && return nothing
    text = strip(String(raw))
    isempty(text) && return Int[]
    return [parse(Int, strip(part)) for part in split(text, ",") if !isempty(strip(part))]
end

function single_peakedness_settings(cfg, opts)
    run_cfg = get(cfg, "run", Dict{String,Any}())
    sp_cfg = get(cfg, "single_peakedness", Dict{String,Any}())
    output_root = resolve_path(String(config_value(run_cfg, "output_root", DEFAULT_OUTPUT_ROOT)))

    b = parse_int_list(opts["b"])
    r = parse_int_list(opts["r"])
    k = parse_int_list(opts["k"])
    b === nothing && (b = Int.(config_value(sp_cfg, "bootstrap_reps", [1])))
    r === nothing && (r = Int.(config_value(sp_cfg, "imputation_reps", [1])))
    k === nothing && (k = Int.(config_value(sp_cfg, "linearization_reps", [1])))

    source = Symbol(String(config_value(sp_cfg, "proportion_source", "resampled_profile")))
    source in (:resampled_profile, :survey_weight, :unweighted_rows) || error(
        "single_peakedness.proportion_source must be resampled_profile, survey_weight, or unweighted_rows.",
    )

    return (
        output_dir = resolve_path(String(config_value(sp_cfg, "output_dir",
                             joinpath(output_root, "single_peakedness")))),
        bootstrap_reps = b,
        imputation_reps = r,
        linearization_reps = k,
        proportion_source = source,
        force = Bool(opts["force"]) || Bool(config_value(sp_cfg, "force", false)),
        dry_run = Bool(opts["dry-run"]) || Bool(config_value(sp_cfg, "dry_run", false)),
    )
end

function selected_single_peakedness_targets(cfg, opts)
    targets = selected_targets(cfg, opts)
    for target in targets
        any(==(5), target.m_values) || @warn(
            "Single-peakedness target m_values=$(target.m_values); main intended target is m=5.",
        )
    end
    return targets
end

function max_or_one(xs)
    isempty(xs) && error("Replicate selection cannot be empty.")
    return maximum(xs)
end

function build_single_peakedness_batch(targets, wave_by_id, settings, run_cfg)
    adjusted = (
        B = max(settings.B, max_or_one(run_cfg.bootstrap_reps)),
        R = max(settings.R, max_or_one(run_cfg.imputation_reps)),
        K = max(settings.K, max_or_one(run_cfg.linearization_reps)),
        force = settings.force,
        dry_run = settings.dry_run,
        output_root = settings.output_root,
        cache_root = settings.cache_root,
        consensus_tie_policy = settings.consensus_tie_policy,
        imputer_backends = settings.imputer_backends,
        linearizer_policies = settings.linearizer_policies,
        measures = settings.measures,
    )
    return build_batch(targets, wave_by_id, adjusted)
end

function linearized_artifact_bundle(path::AbstractString)
    isfile(path) || error("Linearized artifact not found: $(path)")
    artifact = JLD2.load(path, "artifact")
    artifact isa AbstractDataFrame || error(
        "Expected linearized artifact $(path) to be a DataFrame; got $(typeof(artifact)).",
    )
    return pp.dataframe_to_annotated_profile(artifact; ballot_kind = :strict), DataFrame(artifact)
end

function first_present_column(df::AbstractDataFrame, candidates)
    for col in candidates
        sym = Symbol(col)
        sym in propertynames(df) && return sym
    end
    return nothing
end

function profile_for_source(bundle, artifact_df::DataFrame, source::Symbol)
    if source === :survey_weight
        weight_col = first_present_column(artifact_df, (:survey_weight, :weight, :weights, :peso))
        if weight_col === nothing
            @warn "No survey-weight column found in linearized artifact; falling back to unweighted row proportions." proportion_source = source
            return bundle.profile, :unweighted_rows
        end
        return prefs.WeightedProfile(bundle.profile, Float64.(artifact_df[!, weight_col])), :survey_weight
    elseif source === :unweighted_rows
        return bundle.profile, :unweighted_rows
    else
        return bundle.profile, :resampled_profile
    end
end

ranking_key(ballot) = Tuple(prefs.to_perm(ballot))
axis_string(axis) = join(String.(axis), " < ")
ranking_string(ranking) = join(String.(ranking), " > ")

function support_id_map(result)
    return Dict(Tuple(Symbol.(entry.ranking)) => entry.unique_ranking_id for entry in result.support)
end

function base_metadata(item, stage_row)
    spec = item.spec
    meta = item.metadata
    return (
        year = Int(meta.year),
        scenario_name = string(meta.scenario_name),
        variant = string(meta.scenario_name),
        candidate_set_name = hasproperty(meta, :candidate_label) ? string(meta.candidate_label) : "",
        candidate_set = join(spec.active_candidates, "|"),
        m = Int(meta.m),
        imputer_backend = String(spec.imputer_backend),
        linearizer_policy = String(spec.linearizer_policy),
        b = Int(stage_row.b),
        r = Int(stage_row.r),
        k = Int(stage_row.k),
    )
end

function axis_rows(result, base, proportion_source::Symbol)
    rows = NamedTuple[]
    best_L0 = Set(result.best_L0_axis_ids)
    best_L1 = Set(result.best_L1_axis_ids)
    total_support = length(result.support)
    for summary in result.axis_summaries
        push!(rows, merge(base, (
            proportion_source = String(proportion_source),
            axis_id = summary.axis_id,
            axis_as_string = axis_string(summary.axis),
            L0 = summary.L0,
            L1 = summary.L1,
            non_single_peaked_support_count = length(summary.non_single_peaked_support_ids),
            non_single_peaked_mass = summary.non_single_peaked_mass,
            total_support_count = total_support,
            total_mass = summary.total_mass,
            is_best_L0_axis = summary.axis_id in best_L0,
            is_best_L1_axis = summary.axis_id in best_L1,
        )))
    end
    return rows
end

function best_axis_rows(result, base, proportion_source::Symbol)
    rows = NamedTuple[]
    summaries = Dict(summary.axis_id => summary for summary in result.axis_summaries)
    for axis_id in result.best_L0_axis_ids
        summary = summaries[axis_id]
        push!(rows, merge(base, (
            proportion_source = String(proportion_source),
            measure = "L0",
            value = result.best_L0,
            axis_id = axis_id,
            axis_as_string = axis_string(summary.axis),
            n_tied_best_axes = length(result.best_L0_axis_ids),
        )))
    end
    for axis_id in result.best_L1_axis_ids
        summary = summaries[axis_id]
        push!(rows, merge(base, (
            proportion_source = String(proportion_source),
            measure = "L1",
            value = result.best_L1,
            axis_id = axis_id,
            axis_as_string = axis_string(summary.axis),
            n_tied_best_axes = length(result.best_L1_axis_ids),
        )))
    end
    return rows
end

function support_rows(result, base, proportion_source::Symbol)
    rows = NamedTuple[]
    for cls in result.support_classifications
        push!(rows, merge(base, (
            proportion_source = String(proportion_source),
            axis_id = cls.axis_id,
            unique_ranking_id = cls.unique_ranking_id,
            ranking_as_string = ranking_string(cls.ranking),
            proportion = cls.proportion,
            raw_count = cls.raw_count === nothing ? missing : cls.raw_count,
            survey_weight_sum = cls.survey_weight_sum === nothing ? missing : cls.survey_weight_sum,
            is_single_peaked = cls.is_single_peaked,
            distance_to_SP_axis = cls.distance,
        )))
    end
    return rows
end

function respondent_id_column(df::DataFrame)
    return first_present_column(df, (:respondent_id, :id, :ID, :case_id, :interview_id))
end

function artifact_metadata_columns(df::DataFrame)
    return [name for name in propertynames(df) if name !== :profile]
end

function artifact_row_metadata(df::DataFrame, row_id::Int)
    cols = artifact_metadata_columns(df)
    return (; (col => df[row_id, col] for col in cols)...)
end

function row_classification_rows(result, bundle, artifact_df::DataFrame, base, proportion_source::Symbol)
    rows = NamedTuple[]
    best_L0_axes = Set(result.best_L0_axis_ids)
    classifications = [cls for cls in result.support_classifications if cls.axis_id in best_L0_axes]
    class_by_axis_rank = Dict((cls.axis_id, Tuple(cls.ranking)) => cls for cls in classifications)
    rid_col = respondent_id_column(artifact_df)
    rid_col === nothing && @warn(
        "No respondent ID column found in linearized artifact; row classification will use profile_row_id and preserved demographic metadata columns.",
    )

    id_by_ranking = support_id_map(result)
    for (row_id, ballot) in enumerate(bundle.profile.ballots)
        ranking = [bundle.profile.pool[i] for i in prefs.to_perm(ballot)]
        key = Tuple(ranking)
        unique_id = id_by_ranking[key]
        row_meta = artifact_row_metadata(artifact_df, row_id)
        for axis_id in result.best_L0_axis_ids
            cls = class_by_axis_rank[(axis_id, key)]
            push!(rows, merge(base, (
                proportion_source = String(proportion_source),
                axis_id = axis_id,
                respondent_id = rid_col === nothing ? missing : artifact_df[row_id, rid_col],
                profile_row_id = row_id,
                unique_ranking_id = unique_id,
                ranking_as_string = ranking_string(ranking),
                is_single_peaked = cls.is_single_peaked,
            ), row_meta))
        end
    end
    return rows
end

function selected_stage_rows(pipeline, item, sp_settings)
    paths = pp.pipeline_stage_paths(pipeline, item.spec)
    rows = paths[Symbol.(paths.stage) .== :linearized, :]
    rows = rows[
        in.(Int.(rows.b), Ref(Set(sp_settings.bootstrap_reps))) .&
        in.(Int.(rows.r), Ref(Set(sp_settings.imputation_reps))) .&
        in.(Int.(rows.k), Ref(Set(sp_settings.linearization_reps))),
        :,
    ]
    isempty(rows) && error("No linearized stage rows matched selected BRK values.")
    return sort(rows, [:b, :r, :k])
end

function write_nonempty_csv(path::AbstractString, rows)
    if isempty(rows)
        df = DataFrame()
    else
        cols = Symbol[]
        seen = Set{Symbol}()
        for row in rows
            for col in keys(row)
                col in seen && continue
                push!(cols, col)
                push!(seen, col)
            end
        end
        df = DataFrame([
            col => [haskey(row, col) ? getproperty(row, col) : missing for row in rows]
            for col in cols
        ])
    end
    mkpath(dirname(path))
    CSV.write(path, df)
    return path
end

function main(args = ARGS)
    opts = parse_single_peakedness_args(args)
    cfg = load_orchestration_config(opts["config"] === nothing ? joinpath(DEFAULT_CONFIG_DIR, "single_peakedness.toml") : opts["config"])
    run_cfg = run_settings(cfg, opts)
    sp_cfg = single_peakedness_settings(cfg, opts)
    targets = selected_single_peakedness_targets(cfg, opts)
    waves, registry, wave_by_id = load_waves()
    batch = build_single_peakedness_batch(targets, wave_by_id, run_cfg, sp_cfg)

    println("Single-peakedness plan:")
    println("  output_dir=", sp_cfg.output_dir)
    println("  cache_root=", run_cfg.cache_root)
    println("  BRK=", sp_cfg.bootstrap_reps, "/", sp_cfg.imputation_reps, "/", sp_cfg.linearization_reps)
    println("  proportion_source=", sp_cfg.proportion_source)
    for (idx, item) in enumerate(batch.items)
        println("  [", idx, "] wave=", item.spec.wave_id,
                " scenario=", item.metadata.scenario_name,
                " m=", item.metadata.m,
                " backend=", item.spec.imputer_backend,
                " linearizer=", item.spec.linearizer_policy)
    end
    sp_cfg.dry_run && return nothing

    pipeline = pp.NestedStochasticPipeline(registry; cache_root = run_cfg.cache_root)
    axis_all = NamedTuple[]
    best_all = NamedTuple[]
    support_all = NamedTuple[]
    row_all = NamedTuple[]

    for item in batch.items
        pp.ensure_linearizations!(pipeline, item.spec; force = sp_cfg.force)
        for stage in eachrow(selected_stage_rows(pipeline, item, sp_cfg))
            bundle, artifact_df = linearized_artifact_bundle(String(stage.path))
            profile, actual_source = profile_for_source(bundle, artifact_df, sp_cfg.proportion_source)
            result = prefs.single_peakedness_summary(
                profile;
                proportion_source = actual_source,
                classify_axes = :best,
            )
            base = base_metadata(item, stage)
            append!(axis_all, axis_rows(result, base, actual_source))
            append!(best_all, best_axis_rows(result, base, actual_source))
            append!(support_all, support_rows(result, base, actual_source))
            append!(row_all, row_classification_rows(result, bundle, artifact_df, base, actual_source))
        end
    end

    write_nonempty_csv(joinpath(sp_cfg.output_dir, "single_peakedness_axis_summary.csv"), axis_all)
    write_nonempty_csv(joinpath(sp_cfg.output_dir, "single_peakedness_best_axes.csv"), best_all)
    write_nonempty_csv(joinpath(sp_cfg.output_dir, "single_peakedness_support_classification.csv"), support_all)
    write_nonempty_csv(joinpath(sp_cfg.output_dir, "single_peakedness_row_classification.csv"), row_all)

    println("Wrote single-peakedness CSVs under ", sp_cfg.output_dir)
    return nothing
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
