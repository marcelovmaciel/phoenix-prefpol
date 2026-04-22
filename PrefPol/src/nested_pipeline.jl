const NESTED_PIPELINE_SCHEMA_VERSION = 1
const NESTED_PIPELINE_CODE_VERSION = "nested-pipeline-v4-ordered-batch"
const DEFAULT_NESTED_PIPELINE_CACHE_ROOT = normpath(
    joinpath(project_root, "intermediate_data", "nested_pipeline"),
)
mkpath(DEFAULT_NESTED_PIPELINE_CACHE_ROOT)

const SUPPORTED_NESTED_IMPUTERS = (:zero, :random, :mice)
const SUPPORTED_LINEARIZER_POLICIES = (:random_ties, :pattern_conditional)
const SUPPORTED_CONSENSUS_TIE_POLICIES = (:average, :hash, :interval)
const SUPPORTED_GLOBAL_NESTED_MEASURES = (:Psi, :R, :HHI, :RHHI)
const SUPPORTED_GROUPED_NESTED_MEASURES = (
    :C, :D, :D_median, :O, :O_smoothed, :Sep, :G, :Gsep, :S, :S_old,
)

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

function load_survey_wave_config(path::AbstractString; wave_id = nothing)
    cfg = load_election_cfg(path)
    resolved_wave_id = wave_id === nothing ? string(cfg.year) : String(wave_id)
    return SurveyWaveConfig(cfg; wave_id = resolved_wave_id)
end

function build_source_registry(waves)
    registry = Dict{String,SurveyWaveConfig}()

    for wave in waves
        registry[wave.wave_id] = wave
    end

    return registry
end

function _lookup_wave_config(pipeline, wave_id::AbstractString)
    haskey(pipeline.source_registry, wave_id) || throw(ArgumentError(
        "Unknown wave_id `$wave_id`. Register the corresponding SurveyWaveConfig first.",
    ))
    return pipeline.source_registry[wave_id]
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

function _normalize_measure(measure)
    sym = Symbol(measure)

    if sym === Symbol("Ψ") || sym === :Psi
        return :Psi
    elseif sym in SUPPORTED_GLOBAL_NESTED_MEASURES || sym in SUPPORTED_GROUPED_NESTED_MEASURES
        return sym
    end

    supported = join(
        (String(measure_id)
         for measure_id in (SUPPORTED_GLOBAL_NESTED_MEASURES..., SUPPORTED_GROUPED_NESTED_MEASURES...)),
        ", ",
    )
    throw(ArgumentError(
        "Unsupported measure `$measure`. Supported measures: $supported.",
    ))
end

function _normalize_measure_list(measures)
    raw = measures isa Symbol || measures isa AbstractString ? (measures,) : Tuple(measures)
    isempty(raw) && throw(ArgumentError("Provide at least one measure."))
    return unique!([_normalize_measure(measure) for measure in raw])
end

function _normalize_groupings(groupings)
    return Symbol.(collect(groupings))
end

struct PipelineSpec
    wave_id::String
    active_candidates::Vector{String}
    groupings::Vector{Symbol}
    measures::Vector{Symbol}
    B::Int
    R::Int
    K::Int
    resample_policy::Symbol
    imputer_backend::Symbol
    imputer_options::NamedTuple
    linearizer_policy::Symbol
    consensus_tie_policy::Symbol
    seed_namespace::String
    schema_version::Int
    code_version::String
end

function PipelineSpec(wave_id::AbstractString,
                      active_candidates;
                      groupings = Symbol[],
                      measures = nothing,
                      B::Int = 100,
                      R::Int = 1,
                      K::Int = 1,
                      resample_policy::Symbol = :weighted_bootstrap,
                      imputer_backend::Symbol = :mice,
                      imputer_options::NamedTuple = (;),
                      linearizer_policy::Symbol = :random_ties,
                      consensus_tie_policy::Symbol = :average,
                      seed_namespace::AbstractString = "prefpol-nested-pipeline",
                      schema_version::Int = NESTED_PIPELINE_SCHEMA_VERSION,
                      code_version::AbstractString = NESTED_PIPELINE_CODE_VERSION)
    candidates = unique!(String.(collect(active_candidates)))
    length(candidates) >= 2 || throw(ArgumentError(
        "PipelineSpec requires at least two active candidates.",
    ))

    grouping_syms = _normalize_groupings(groupings)
    measure_syms = measures === nothing ?
                   (isempty(grouping_syms) ? [:Psi, :R, :HHI, :RHHI] :
                                             [:Psi, :R, :HHI, :RHHI, :C, :D, :G]) :
                   _normalize_measure_list(measures)

    grouped_measure_names = join(String.(SUPPORTED_GROUPED_NESTED_MEASURES), ", ")
    any(measure in SUPPORTED_GROUPED_NESTED_MEASURES for measure in measure_syms) &&
        isempty(grouping_syms) &&
        throw(ArgumentError(
            "Measures $grouped_measure_names require at least one grouping column.",
        ))

    B >= 1 || throw(ArgumentError("B must be at least 1."))
    R >= 1 || throw(ArgumentError("R must be at least 1."))
    K >= 1 || throw(ArgumentError("K must be at least 1."))

    resample_policy === :weighted_bootstrap || throw(ArgumentError(
        "Unsupported resample_policy `$resample_policy`. Only :weighted_bootstrap is currently implemented.",
    ))
    imputer_backend in SUPPORTED_NESTED_IMPUTERS || throw(ArgumentError(
        "Unsupported imputer_backend `$imputer_backend`. Supported backends: $(SUPPORTED_NESTED_IMPUTERS).",
    ))
    linearizer_policy in SUPPORTED_LINEARIZER_POLICIES || throw(ArgumentError(
        "Unsupported linearizer_policy `$linearizer_policy`. Supported policies: $(SUPPORTED_LINEARIZER_POLICIES).",
    ))
    consensus_tie_policy in SUPPORTED_CONSENSUS_TIE_POLICIES || throw(ArgumentError(
        "Unsupported consensus_tie_policy `$consensus_tie_policy`. Supported policies: $(SUPPORTED_CONSENSUS_TIE_POLICIES).",
    ))

    return PipelineSpec(
        String(wave_id),
        candidates,
        grouping_syms,
        measure_syms,
        B,
        R,
        K,
        resample_policy,
        imputer_backend,
        imputer_options,
        linearizer_policy,
        consensus_tie_policy,
        String(seed_namespace),
        schema_version,
        String(code_version),
    )
end

function resolve_active_candidate_set(wcfg::SurveyWaveConfig;
                                      active_candidates = nothing,
                                      scenario_name = nothing,
                                      m = nothing,
                                      data = nothing)
    (active_candidates !== nothing && scenario_name !== nothing) && throw(ArgumentError(
        "Pass either `active_candidates` or `scenario_name`, not both.",
    ))

    if active_candidates !== nothing
        candidates = unique!(String.(collect(active_candidates)))
        length(candidates) >= 2 || throw(ArgumentError(
            "Resolved active candidate set must contain at least two candidates.",
        ))
        length(candidates) <= wcfg.max_candidates || throw(ArgumentError(
            "Resolved active candidate set for wave $(wcfg.wave_id) has $(length(candidates)) candidates, " *
            "but max_candidates=$(wcfg.max_candidates).",
        ))
        missing_candidates = setdiff(candidates, wcfg.candidate_universe)
        isempty(missing_candidates) || throw(ArgumentError(
            "Resolved active candidate set for wave $(wcfg.wave_id) contains unknown candidates $(missing_candidates).",
        ))
        return candidates
    end

    mm = _validate_candidate_count(
        m === nothing ? wcfg.default_m : Int(m),
        wcfg.max_candidates;
        context = "resolve_active_candidate_set for wave $(wcfg.wave_id)",
        min_allowed = 2,
    )

    df = data === nothing ? load_wave_data(wcfg) : data
    weight_col = _candidate_weight_col(df)
    force_include = scenario_name === nothing ? String[] :
                    _scenario_force_include(wcfg, scenario_name)

    return compute_global_candidate_set(
        df;
        candidate_cols = wcfg.candidate_universe,
        m = mm,
        force_include = force_include,
        weights = df[!, weight_col],
    )
end

function build_pipeline_spec(wcfg::SurveyWaveConfig;
                             active_candidates = nothing,
                             scenario_name = nothing,
                             m = nothing,
                             groupings = nothing,
                             kwargs...)
    resolved_groupings = groupings === nothing ?
                         Symbol.(wcfg.demographic_cols) :
                         _normalize_groupings(groupings)
    resolved_candidates = resolve_active_candidate_set(
        wcfg;
        active_candidates = active_candidates,
        scenario_name = scenario_name,
        m = m,
    )
    return PipelineSpec(
        wcfg.wave_id,
        resolved_candidates;
        groupings = resolved_groupings,
        kwargs...,
    )
end

_stable_serialize(x::Nothing) = "nothing"
_stable_serialize(x::Bool) = x ? "true" : "false"
_stable_serialize(x::Integer) = "int($(x))"
_stable_serialize(x::AbstractFloat) = @sprintf("float(%.17g)", Float64(x))
_stable_serialize(x::Symbol) = "sym($(String(x)))"
_stable_serialize(x::AbstractString) = "str($(replace(String(x), "|" => "\\|")))"

function _stable_serialize(xs::Tuple)
    return "tuple(" * join((_stable_serialize(x) for x in xs), "|") * ")"
end

function _stable_serialize(xs::NamedTuple)
    parts = String[]

    for name in propertynames(xs)
        push!(parts, String(name) * "=" * _stable_serialize(getfield(xs, name)))
    end

    return "named(" * join(parts, "|") * ")"
end

function _stable_serialize(xs::AbstractVector)
    return "vec(" * join((_stable_serialize(x) for x in xs), "|") * ")"
end

function _stable_serialize(spec::PipelineSpec)
    return _stable_serialize((
        wave_id = spec.wave_id,
        active_candidates = spec.active_candidates,
        groupings = String.(spec.groupings),
        measures = String.(spec.measures),
        B = spec.B,
        R = spec.R,
        K = spec.K,
        resample_policy = spec.resample_policy,
        imputer_backend = spec.imputer_backend,
        imputer_options = spec.imputer_options,
        linearizer_policy = spec.linearizer_policy,
        consensus_tie_policy = spec.consensus_tie_policy,
        seed_namespace = spec.seed_namespace,
        schema_version = spec.schema_version,
        code_version = spec.code_version,
    ))
end

_stable_serialize(x) = repr(x)

_pipeline_cache_key(spec::PipelineSpec) =
    bytes2hex(SHA.sha256(codeunits(_stable_serialize(spec))))

function _seed_int(seed::UInt64)
    modulus = UInt64(typemax(Int) - 1)
    return Int(mod(seed, modulus) + 1)
end

function _digest_seed(payload::AbstractString)
    digest = SHA.sha256(codeunits(payload))
    seed = zero(UInt64)

    for byte in digest[1:8]
        seed = (seed << 8) | UInt64(byte)
    end

    return seed == 0 ? UInt64(1) : seed
end

function _stage_seed(spec::PipelineSpec,
                     stage::Symbol;
                     b::Int = 0,
                     r::Int = 0,
                     k::Int = 0)
    return _digest_seed(_stable_serialize((
        namespace = spec.seed_namespace,
        spec = spec,
        stage = String(stage),
        b = b,
        r = r,
        k = k,
    )))
end

_rng_from_seed(seed::UInt64) = MersenneTwister(_seed_int(seed))

function resolve_pipeline_cache_root(root = nothing)
    if root === nothing
        mkpath(DEFAULT_NESTED_PIPELINE_CACHE_ROOT)
        return DEFAULT_NESTED_PIPELINE_CACHE_ROOT
    end

    candidate = String(root)
    resolved = isabspath(candidate) ? normpath(candidate) :
               normpath(joinpath(project_root, candidate))
    mkpath(resolved)
    return resolved
end

struct NestedStochasticPipeline
    source_registry::Dict{String,SurveyWaveConfig}
    cache_root::String
end

function NestedStochasticPipeline(source_registry::Dict{String,SurveyWaveConfig};
                                  cache_root = nothing)
    return NestedStochasticPipeline(
        source_registry,
        resolve_pipeline_cache_root(cache_root),
    )
end

NestedStochasticPipeline(waves; cache_root = nothing) =
    NestedStochasticPipeline(build_source_registry(waves); cache_root = cache_root)

struct ObservedData
    wave_id::String
    active_candidates::Vector{String}
    scores::DataFrame
    weights::Vector{Float64}
    groupings::Vector{Symbol}
    provenance::NamedTuple
end

struct Resample
    b::Int
    indices::Vector{Int}
    multiplicities::Vector{Int}
    table::DataFrame
    seed::UInt64
end

struct ImputedData
    b::Int
    r::Int
    table::DataFrame
    backend::Symbol
    options::NamedTuple
    seed::UInt64
    report::NamedTuple
end

struct LinearizedProfile
    b::Int
    r::Int
    k::Int
    bundle::AnnotatedProfile
    candidate_tuple::Tuple{Vararg{Symbol}}
    policy::Symbol
    seed::UInt64
end

struct MeasureResult
    b::Int
    r::Int
    k::Int
    measure_id::Symbol
    grouping::Union{Nothing,Symbol}
    value::Float64
    lower::Union{Nothing,Float64}
    upper::Union{Nothing,Float64}
    diagnostics::NamedTuple
end

# `V_res` is kept as a legacy field name for backward compatibility with
# existing scripts; in the explicit tree decomposition it is the bootstrap
# component `Var_b(E[M | b])`.
struct VarianceComponentSummary
    measure_id::Symbol
    grouping::Union{Nothing,Symbol}
    estimate::Float64
    V_res::Float64
    V_imp::Float64
    V_lin::Float64
    total_variance::Float64
    empirical_variance::Float64
end

struct VarianceDecomposition
    summaries::Vector{VarianceComponentSummary}
end

struct PipelineResult
    spec::PipelineSpec
    cache_dir::String
    stage_manifest::DataFrame
    measure_cube::DataFrame
    pooled_summaries::DataFrame
    decomposition::VarianceDecomposition
    audit_log::DataFrame
end

struct StudyBatchItem
    spec::PipelineSpec
    metadata::NamedTuple
end

StudyBatchItem(spec::PipelineSpec; metadata...) =
    StudyBatchItem(spec, (; metadata...))

# Batch items are an ordered job list. Identical PipelineSpecs are legal and
# remain distinct by position in `items`.
struct StudyBatchSpec
    items::Vector{StudyBatchItem}
end

StudyBatchSpec(items::AbstractVector{<:StudyBatchItem}) =
    StudyBatchSpec(collect(items))

StudyBatchSpec(specs::AbstractVector{<:PipelineSpec}) =
    StudyBatchSpec([StudyBatchItem(spec) for spec in specs])

struct BatchRunner
    pipeline::NestedStochasticPipeline
end

struct BatchRunResult
    batch::StudyBatchSpec
    results::Vector{PipelineResult}
    metadata::Vector{NamedTuple}
end

function BatchRunResult(batch::StudyBatchSpec,
                        results::AbstractVector{<:PipelineResult},
                        metadata::AbstractVector{<:NamedTuple})
    length(results) == length(batch.items) || throw(ArgumentError(
        "BatchRunResult results must align 1:1 with StudyBatchSpec.items.",
    ))
    length(metadata) == length(batch.items) || throw(ArgumentError(
        "BatchRunResult metadata must align 1:1 with StudyBatchSpec.items.",
    ))
    return BatchRunResult(batch, collect(results), collect(metadata))
end

Base.length(results::BatchRunResult) = length(results.results)
Base.haskey(results::BatchRunResult, idx::Integer) = checkbounds(Bool, results.results, idx)
Base.getindex(results::BatchRunResult, idx::Integer) = results.results[idx]
Base.firstindex(results::BatchRunResult) = firstindex(results.results)
Base.lastindex(results::BatchRunResult) = lastindex(results.results)
Base.keys(results::BatchRunResult) = eachindex(results.results)
Base.values(results::BatchRunResult) = results.results
Base.pairs(results::BatchRunResult) = pairs(results.results)
Base.iterate(results::BatchRunResult, state...) = iterate(results.results, state...)

function load_observed_data(pipeline::NestedStochasticPipeline, spec::PipelineSpec)
    wcfg = _lookup_wave_config(pipeline, spec.wave_id)
    raw = load_wave_data(wcfg)

    missing_candidates = setdiff(spec.active_candidates, names(raw))
    isempty(missing_candidates) || throw(ArgumentError(
        "Observed data for wave $(spec.wave_id) is missing active candidates $(missing_candidates).",
    ))

    missing_groupings = setdiff(String.(spec.groupings), names(raw))
    isempty(missing_groupings) || throw(ArgumentError(
        "Observed data for wave $(spec.wave_id) is missing grouping columns $(missing_groupings).",
    ))

    weight_col = _candidate_weight_col(raw)
    requested_cols = vcat(spec.active_candidates, String.(spec.groupings))
    scores = select(raw, requested_cols)
    weights = _normalize_weight_vector(raw[!, weight_col])

    return ObservedData(
        spec.wave_id,
        copy(spec.active_candidates),
        scores,
        weights,
        copy(spec.groupings),
        (
            year = wcfg.year,
            data_loader = wcfg.data_loader,
            data_file = wcfg.data_file,
            weight_col = String(weight_col),
        ),
    )
end

function _multiplicities(indices::Vector{Int}, n::Int)
    counts = zeros(Int, n)

    for idx in indices
        counts[idx] += 1
    end

    return counts
end

function _draw_resample(observed::ObservedData,
                        spec::PipelineSpec,
                        b::Int)
    seed = _stage_seed(spec, :resample; b = b)
    rng = _rng_from_seed(seed)
    n = nrow(observed.scores)
    idxs = sample(rng, 1:n, Weights(observed.weights), n; replace = true)

    return Resample(
        b,
        idxs,
        _multiplicities(idxs, n),
        observed.scores[idxs, :],
        seed,
    )
end

function _score_and_group_tables(df::DataFrame,
                                 candidate_cols::Vector{String},
                                 grouping_cols::Vector{Symbol})
    scores = select(df, candidate_cols)
    groups = isempty(grouping_cols) ? DataFrame() : select(df, String.(grouping_cols))
    return scores, groups
end

function _impute_resample(resample::Resample,
                          spec::PipelineSpec,
                          r::Int)
    seed = _stage_seed(spec, :imputation; b = resample.b, r = r)
    scores, groups = _score_and_group_tables(
        resample.table,
        spec.active_candidates,
        spec.groupings,
    )

    if spec.imputer_backend === :zero
        prepared = prepare_scores_for_imputation_int(scores, spec.active_candidates)
        completed_scores = Impute.replace(prepared, values = 0)
        report = (backend = :zero, deterministic = true)
    elseif spec.imputer_backend === :random
        prepared = prepare_scores_for_imputation_categorical(scores, spec.active_candidates)
        completed_scores = Impute.impute(
            prepared,
            Impute.SRS(; rng = _rng_from_seed(seed)),
        )
        report = (backend = :random, deterministic = false)
    elseif spec.imputer_backend === :mice
        prepared = prepare_scores_for_imputation_categorical(scores, spec.active_candidates)
        mice_report = r_impute_mice_report(prepared; seed = _seed_int(seed))
        completed_scores = mice_report.completed
        report = (
            backend = :mice,
            meth = mice_report.meth,
            logged_events = mice_report.loggedEvents,
            dropped_predictors = mice_report.dropped_predictors,
        )
    else
        error("Unhandled imputer backend $(spec.imputer_backend).")
    end

    table = isempty(spec.groupings) ? completed_scores : hcat(completed_scores, groups)

    return ImputedData(
        resample.b,
        r,
        table,
        spec.imputer_backend,
        spec.imputer_options,
        seed,
        report,
    )
end

function _weak_profile_bundle(imputed::ImputedData, spec::PipelineSpec)
    candidate_syms = Symbol.(spec.active_candidates)
    df = profile_dataframe(
        imputed.table;
        score_cols = candidate_syms,
        demo_cols = collect(spec.groupings),
        kind = :weak,
    )
    metadata!(df, "candidates", candidate_syms)
    metadata!(df, "profile_kind", "weak")
    return dataframe_to_annotated_profile(df; ballot_kind = :weak)
end

function _resolve_nested_linearizer(weak_bundle::AnnotatedProfile,
                                    spec::PipelineSpec)
    if spec.linearizer_policy === :random_ties
        return :random
    elseif spec.linearizer_policy === :pattern_conditional
        return Preferences.PatternConditionalLinearizer(
            weak_bundle.profile;
            alpha = 0.5,
            fallback = :uniform,
        )
    end

    throw(ArgumentError(
        "Unsupported linearizer_policy `$(spec.linearizer_policy)`. Supported policies: $(SUPPORTED_LINEARIZER_POLICIES).",
    ))
end

function _linearize_imputed(imputed::ImputedData,
                            spec::PipelineSpec,
                            k::Int)
    seed = _stage_seed(spec, :linearization;
                       b = imputed.b,
                       r = imputed.r,
                       k = k)
    weak_bundle = _weak_profile_bundle(imputed, spec)
    _assert_complete_weak_orders(
        weak_bundle;
        context = "wave=$(spec.wave_id) b=$(imputed.b) r=$(imputed.r) k=$k",
    )
    rng = _rng_from_seed(seed)
    tie_break = _resolve_nested_linearizer(weak_bundle, spec)
    strict_bundle = linearize_annotated_profile(
        weak_bundle;
        rng = rng,
        tie_break = tie_break,
    )

    return LinearizedProfile(
        imputed.b,
        imputed.r,
        k,
        strict_bundle,
        tuple(Symbol.(spec.active_candidates)...),
        spec.linearizer_policy,
        seed,
    )
end

function _metadata_column(bundle::AnnotatedProfile, key::Symbol)
    metadata_obj = bundle.metadata

    if metadata_obj isa NamedTuple
        hasproperty(metadata_obj, key) || throw(ArgumentError(
            "AnnotatedProfile metadata is missing grouping column `$key`.",
        ))
        return getproperty(metadata_obj, key)
    elseif metadata_obj isa AbstractDataFrame
        hasproperty(metadata_obj, key) || throw(ArgumentError(
            "AnnotatedProfile metadata is missing grouping column `$key`.",
        ))
        return metadata_obj[!, key]
    end

    throw(ArgumentError("Unsupported AnnotatedProfile metadata type $(typeof(metadata_obj))."))
end

function _group_row_indices(values)
    grouped = OrderedDict{Any,Vector{Int}}()

    for (idx, value) in enumerate(values)
        push!(get!(grouped, value, Int[]), idx)
    end

    return grouped
end

function _merge_tie_break_context(base_key, extension::NamedTuple)
    if base_key === nothing
        return extension
    elseif base_key isa NamedTuple
        return merge(base_key, extension)
    end

    return (; context = base_key, extension...)
end

function _consensus_ballots_for_result(result, pool::Preferences.CandidatePool, tie_policy::Symbol)
    if tie_policy === :hash
        return [Preferences.StrictRank(pool, collect(Int.(result.consensus_perm)))]
    end

    return [
        Preferences.StrictRank(pool, collect(Int.(perm)))
        for perm in result.all_minimizers
    ]
end

@inline _normalize_group_coherence(raw_C::Real) = (2.0 * Float64(raw_C)) - 1.0
@inline _normalized_kendall_distance(ballot_i, ballot_j, norm_factor::Real) =
    Preferences.kendall_tau_distance(ballot_i, ballot_j) / norm_factor

function _pairwise_consensus_distance_stats(result_i,
                                            pool_i::Preferences.CandidatePool,
                                            result_j,
                                            pool_j::Preferences.CandidatePool,
                                            tie_policy::Symbol)
    ballots_i = _consensus_ballots_for_result(result_i, pool_i, tie_policy)
    ballots_j = _consensus_ballots_for_result(result_j, pool_j, tie_policy)
    norm_factor = binomial(length(pool_i), 2)
    norm_factor > 0 || throw(ArgumentError("At least two candidates are required."))

    distances = Float64[]
    sizehint!(distances, length(ballots_i) * length(ballots_j))

    for ballot_i in ballots_i, ballot_j in ballots_j
        push!(distances, _normalized_kendall_distance(ballot_i, ballot_j, norm_factor))
    end

    point = tie_policy === :hash ? only(distances) : mean(distances)
    return (point = point, lo = minimum(distances), hi = maximum(distances))
end

function compute_group_measure_details(bundle::AnnotatedProfile,
                                       demo::Symbol;
                                       tie_policy::Symbol = :average,
                                       cache = Preferences.GLOBAL_LINEAR_ORDER_CACHE,
                                       tie_break_context = nothing)
    tie_policy in SUPPORTED_CONSENSUS_TIE_POLICIES || throw(ArgumentError(
        "Unsupported tie_policy `$tie_policy`.",
    ))

    group_values = _metadata_column(bundle, demo)
    grouped = _group_row_indices(group_values)
    isempty(grouped) && throw(ArgumentError("Grouping column `$demo` is empty."))

    total_n = length(group_values)
    total_n > 0 || throw(ArgumentError("Cannot compute group metrics for an empty profile."))

    props = OrderedDict{Any,Float64}()
    group_sizes = OrderedDict{Any,Float64}()
    group_profiles = OrderedDict{Any,Any}()
    consensus_results = OrderedDict{Any,Any}()
    C_raw = 0.0

    for (group, idxs) in grouped
        subbundle = subset_annotated_profile(bundle, idxs)
        profile = strict_profile(subbundle)
        tie_key = _merge_tie_break_context(
            tie_break_context,
            (demographic = String(demo), group = string(group)),
        )
        result = Preferences.consensus_kendall(
            profile;
            cache = cache,
            tie_break_key = tie_key,
        )
        group_sizes[group] = length(idxs)
        props[group] = group_sizes[group] / total_n
        group_profiles[group] = profile
        consensus_results[group] = result
        C_raw += (1.0 - result.avg_normalized_distance) * props[group]
    end

    support_separation_S_old = Preferences.overall_support_separation_old(
        group_profiles,
        group_sizes,
    )
    support_separation_S_old_lo = support_separation_S_old
    support_separation_S_old_hi = support_separation_S_old

    if length(grouped) <= 1
        D = 0.0
        D_lo = 0.0
        D_hi = 0.0
    else
        point_sum = 0.0
        lo_sum = 0.0
        hi_sum = 0.0

        for target in keys(consensus_results)
            result = consensus_results[target]
            ballots = _consensus_ballots_for_result(result, group_profiles[target].pool, tie_policy)
            contributions = Float64[]

            for ballot in ballots
                contribution = 0.0
                for source in keys(group_profiles)
                    source == target && continue
                    contribution += props[source] * Preferences.average_normalized_distance(
                        group_profiles[source],
                        ballot,
                    )
                end
                push!(contributions, contribution)
            end

            point_sum += tie_policy === :hash ? only(contributions) : mean(contributions)
            lo_sum += minimum(contributions)
            hi_sum += maximum(contributions)
        end

        denom = length(grouped) - 1
        D = point_sum / denom
        D_lo = lo_sum / denom
        D_hi = hi_sum / denom
    end

    D_median = Preferences.overall_divergence_median(group_profiles, consensus_results)
    D_median_lo = D_median
    D_median_hi = D_median
    O = Preferences.overall_overlap(group_profiles)
    O_lo = O
    O_hi = O
    O_smoothed = Preferences.overall_overlap_smoothed(group_profiles)
    O_smoothed_lo = O_smoothed
    O_smoothed_hi = O_smoothed
    Sep = Preferences.overall_separation(group_profiles, consensus_results)
    Sep_lo = Sep
    Sep_hi = Sep

    C = _normalize_group_coherence(C_raw)
    cleaned_S = Preferences.overall_sstar_from_CD(C, D)
    cleaned_S_lo = Preferences.overall_sstar_from_CD(C, D_lo)
    cleaned_S_hi = Preferences.overall_sstar_from_CD(C, D_hi)
    G = sqrt(max(C * D, 0.0))
    G_lo = sqrt(max(C * D_lo, 0.0))
    G_hi = sqrt(max(C * D_hi, 0.0))
    Gsep = Preferences.grouped_gsep(C, Sep)
    Gsep_lo = Gsep
    Gsep_hi = Gsep

    n_tied_groups = count(result -> result.is_tied_minimizer, values(consensus_results))
    max_minimizers = isempty(consensus_results) ? 0 :
                     maximum(result.n_minimizers for result in values(consensus_results))

    diagnostics = (
        tie_policy = tie_policy,
        n_groups = length(grouped),
        tied_groups = n_tied_groups,
        max_minimizers = max_minimizers,
        group_minimizers = Dict(
            string(group) => result.n_minimizers
            for (group, result) in consensus_results
        ),
    )

    return (
        C = C,
        D = D,
        D_lo = D_lo,
        D_hi = D_hi,
        D_median = D_median,
        D_median_lo = D_median_lo,
        D_median_hi = D_median_hi,
        O = O,
        O_lo = O_lo,
        O_hi = O_hi,
        O_smoothed = O_smoothed,
        O_smoothed_lo = O_smoothed_lo,
        O_smoothed_hi = O_smoothed_hi,
        Sep = Sep,
        Sep_lo = Sep_lo,
        Sep_hi = Sep_hi,
        G = G,
        G_lo = G_lo,
        G_hi = G_hi,
        Gsep = Gsep,
        Gsep_lo = Gsep_lo,
        Gsep_hi = Gsep_hi,
        S = cleaned_S,
        S_lo = cleaned_S_lo,
        S_hi = cleaned_S_hi,
        S_old = support_separation_S_old,
        S_old_lo = support_separation_S_old_lo,
        S_old_hi = support_separation_S_old_hi,
        diagnostics = diagnostics,
    )
end

function _global_measure_value(measure::Symbol, bundle::AnnotatedProfile)
    if measure === :Psi
        return Ψ(bundle)
    elseif measure === :R
        return calc_total_reversal_component(bundle)
    elseif measure === :HHI
        return calc_reversal_HHI(bundle)
    elseif measure === :RHHI
        return fast_reversal_geometric(bundle)
    end

    throw(ArgumentError("Unsupported global measure `$measure`."))
end

function _measure_results_for_profile(profile::LinearizedProfile,
                                      spec::PipelineSpec)
    results = MeasureResult[]
    audits = NamedTuple[]

    for measure in spec.measures
        measure in SUPPORTED_GROUPED_NESTED_MEASURES && continue
        value = _global_measure_value(measure, profile.bundle)
        push!(results, MeasureResult(
            profile.b,
            profile.r,
            profile.k,
            measure,
            nothing,
            Float64(value),
            nothing,
            nothing,
            (measure_class = :global,),
        ))
    end

    if any(measure in SUPPORTED_GROUPED_NESTED_MEASURES for measure in spec.measures)
        for grouping in spec.groupings
            details = compute_group_measure_details(
                profile.bundle,
                grouping;
                tie_policy = spec.consensus_tie_policy,
                tie_break_context = (
                    seed_namespace = spec.seed_namespace,
                    spec_payload = _stable_serialize(spec),
                    b = profile.b,
                    r = profile.r,
                    k = profile.k,
                ),
            )

            details.diagnostics.tied_groups > 0 && push!(audits, (
                stage = :measure,
                b = profile.b,
                r = profile.r,
                k = profile.k,
                message = "Grouping $(grouping) had $(details.diagnostics.tied_groups) tied consensus groups under policy $(spec.consensus_tie_policy).",
            ))

            if :C in spec.measures
                push!(results, MeasureResult(
                    profile.b,
                    profile.r,
                    profile.k,
                    :C,
                    grouping,
                    details.C,
                    details.C,
                    details.C,
                    details.diagnostics,
                ))
            end

            if :D in spec.measures
                push!(results, MeasureResult(
                    profile.b,
                    profile.r,
                    profile.k,
                    :D,
                    grouping,
                    details.D,
                    details.D_lo,
                    details.D_hi,
                    details.diagnostics,
                ))
            end

            if :D_median in spec.measures
                push!(results, MeasureResult(
                    profile.b,
                    profile.r,
                    profile.k,
                    :D_median,
                    grouping,
                    details.D_median,
                    details.D_median_lo,
                    details.D_median_hi,
                    details.diagnostics,
                ))
            end

            if :O in spec.measures
                push!(results, MeasureResult(
                    profile.b,
                    profile.r,
                    profile.k,
                    :O,
                    grouping,
                    details.O,
                    details.O_lo,
                    details.O_hi,
                    details.diagnostics,
                ))
            end

            if :O_smoothed in spec.measures
                push!(results, MeasureResult(
                    profile.b,
                    profile.r,
                    profile.k,
                    :O_smoothed,
                    grouping,
                    details.O_smoothed,
                    details.O_smoothed_lo,
                    details.O_smoothed_hi,
                    details.diagnostics,
                ))
            end

            if :Sep in spec.measures
                push!(results, MeasureResult(
                    profile.b,
                    profile.r,
                    profile.k,
                    :Sep,
                    grouping,
                    details.Sep,
                    details.Sep_lo,
                    details.Sep_hi,
                    details.diagnostics,
                ))
            end

            if :S in spec.measures
                push!(results, MeasureResult(
                    profile.b,
                    profile.r,
                    profile.k,
                    :S,
                    grouping,
                    details.S,
                    details.S_lo,
                    details.S_hi,
                    details.diagnostics,
                ))
            end

            if :S_old in spec.measures
                push!(results, MeasureResult(
                    profile.b,
                    profile.r,
                    profile.k,
                    :S_old,
                    grouping,
                    details.S_old,
                    details.S_old_lo,
                    details.S_old_hi,
                    details.diagnostics,
                ))
            end

            if :Gsep in spec.measures
                push!(results, MeasureResult(
                    profile.b,
                    profile.r,
                    profile.k,
                    :Gsep,
                    grouping,
                    details.Gsep,
                    details.Gsep_lo,
                    details.Gsep_hi,
                    details.diagnostics,
                ))
            end

            if :G in spec.measures
                push!(results, MeasureResult(
                    profile.b,
                    profile.r,
                    profile.k,
                    :G,
                    grouping,
                    details.G,
                    details.G_lo,
                    details.G_hi,
                    details.diagnostics,
                ))
            end
        end
    end

    return results, audits
end

function _measure_results_dataframe(results::Vector{MeasureResult})
    rows = NamedTuple[]

    for result in results
        push!(rows, (
            b = result.b,
            r = result.r,
            k = result.k,
            measure = result.measure_id,
            grouping = something(result.grouping, missing),
            value = result.value,
            value_lo = something(result.lower, missing),
            value_hi = something(result.upper, missing),
            diagnostics = result.diagnostics,
        ))
    end

    return DataFrame(rows)
end

function _grouped_s_measure_rows(measure_cube::AbstractDataFrame)
    required_cols = [:b, :r, :k, :measure, :grouping, :value, :value_lo, :value_hi, :diagnostics]
    missing_cols = setdiff(required_cols, propertynames(measure_cube))
    isempty(missing_cols) || throw(ArgumentError(
        "measure_cube is missing required columns for grouped S derivation: $(missing_cols).",
    ))

    grouped_cd = measure_cube[
        ((measure_cube.measure .== :C) .| (measure_cube.measure .== :D)) .&
        .!ismissing.(measure_cube.grouping),
        required_cols,
    ]
    isempty(grouped_cd) && return DataFrame(measure_cube[1:0, :])

    grouped_c = grouped_cd[grouped_cd.measure .== :C, :]
    grouped_d = grouped_cd[grouped_cd.measure .== :D, :]
    (isempty(grouped_c) || isempty(grouped_d)) && return DataFrame(measure_cube[1:0, :])

    key_cols = [:b, :r, :k, :grouping]
    counts = combine(groupby(grouped_cd, vcat(key_cols, :measure)), nrow => :nrows)
    bad_counts = counts[counts.nrows .!= 1, :]
    isempty(bad_counts) || throw(ArgumentError(
        "Cannot derive grouped S because grouped C/D rows are not unique per (b, r, k, grouping, measure).",
    ))

    c_rows = select(grouped_c, :b, :r, :k, :grouping, :value, :value_lo, :value_hi, :diagnostics)
    rename!(c_rows, :value => :c_value, :value_lo => :c_value_lo, :value_hi => :c_value_hi, :diagnostics => :c_diagnostics)
    d_rows = select(grouped_d, :b, :r, :k, :grouping, :value, :value_lo, :value_hi, :diagnostics)
    rename!(d_rows, :value => :d_value, :value_lo => :d_value_lo, :value_hi => :d_value_hi, :diagnostics => :d_diagnostics)

    joined = innerjoin(c_rows, d_rows; on = key_cols)
    nrow(joined) == nrow(grouped_c) == nrow(grouped_d) || throw(ArgumentError(
        "Cannot derive grouped S because grouped C and D draw keys do not align exactly.",
    ))

    rows = NamedTuple[]
    for row in eachrow(joined)
        c_value = Float64(row.c_value)
        d_value = Float64(row.d_value)
        value_lo = ismissing(row.d_value_lo) ? missing :
                   Preferences.overall_sstar_from_CD(c_value, Float64(row.d_value_lo))
        value_hi = ismissing(row.d_value_hi) ? missing :
                   Preferences.overall_sstar_from_CD(c_value, Float64(row.d_value_hi))

        push!(rows, (
            b = Int(row.b),
            r = Int(row.r),
            k = Int(row.k),
            measure = :S,
            grouping = row.grouping,
            value = Preferences.overall_sstar_from_CD(c_value, d_value),
            value_lo = value_lo,
            value_hi = value_hi,
            diagnostics = row.c_diagnostics,
        ))
    end

    return DataFrame(rows)
end

function _with_grouped_s_measure_list(measures::AbstractVector{<:Symbol})
    :S in measures && return collect(measures)
    updated = collect(measures)
    push!(updated, :S)
    return updated
end

function augment_pipeline_result_with_grouped_s(result::PipelineResult;
                                                audit_message::Union{Nothing,AbstractString} = nothing)
    any(result.measure_cube.measure .== :S) && throw(ArgumentError(
        "PipelineResult already contains grouped S rows.",
    ))
    :S in result.spec.measures && throw(ArgumentError(
        "PipelineResult spec already lists :S even though the measure cube does not.",
    ))

    s_rows = _grouped_s_measure_rows(result.measure_cube)
    isempty(s_rows) && throw(ArgumentError(
        "PipelineResult does not contain grouped C and D rows required to derive grouped S.",
    ))

    augmented_cube = vcat(result.measure_cube, s_rows; cols = :setequal)
    decomposition = compute_variance_decomposition(augmented_cube)
    pooled_summaries = decomposition_table(decomposition)

    spec = result.spec
    updated_spec = PipelineSpec(
        spec.wave_id,
        copy(spec.active_candidates),
        copy(spec.groupings),
        _with_grouped_s_measure_list(spec.measures),
        spec.B,
        spec.R,
        spec.K,
        spec.resample_policy,
        spec.imputer_backend,
        spec.imputer_options,
        spec.linearizer_policy,
        spec.consensus_tie_policy,
        spec.seed_namespace,
        spec.schema_version,
        spec.code_version,
    )

    message = something(
        audit_message,
        "Appended $(nrow(s_rows)) grouped S rows from cached grouped C and D, and recomputed pooled summaries and variance decomposition.",
    )
    audit_entry = DataFrame(
        stage = Symbol[:result],
        b = [0],
        r = [0],
        k = [0],
        message = [message],
    )
    updated_audit = vcat(copy(result.audit_log), audit_entry; cols = :setequal)

    return PipelineResult(
        updated_spec,
        result.cache_dir,
        copy(result.stage_manifest),
        augmented_cube,
        pooled_summaries,
        decomposition,
        updated_audit,
    )
end

function compute_variance_decomposition(measure_cube::DataFrame)
    summaries = VarianceComponentSummary[]
    summary_df = tree_variance_decomposition_table(
        measure_cube;
        id_cols = [:measure, :grouping],
        bootstrap_col = :b,
        imputation_col = :r,
        linearization_col = :k,
        value_col = :value,
    )

    for row in eachrow(summary_df)
        grouping_value = row.grouping
        push!(summaries, VarianceComponentSummary(
            Symbol(row.measure),
            ismissing(grouping_value) ? nothing : Symbol(grouping_value),
            Float64(row.estimate),
            Float64(row.bootstrap_variance),
            Float64(row.imputation_variance),
            Float64(row.linearization_variance),
            Float64(row.total_variance),
            Float64(row.empirical_variance),
        ))
    end

    return VarianceDecomposition(summaries)
end

function decomposition_table(decomposition::VarianceDecomposition)
    rows = NamedTuple[]

    for summary in decomposition.summaries
        bootstrap_variance = summary.V_res
        imputation_variance = summary.V_imp
        linearization_variance = summary.V_lin

        push!(rows, (
            measure = summary.measure_id,
            grouping = something(summary.grouping, missing),
            estimate = summary.estimate,
            total_variance = summary.total_variance,
            bootstrap_variance = bootstrap_variance,
            imputation_variance = imputation_variance,
            linearization_variance = linearization_variance,
            bootstrap_share = _safe_variance_share(bootstrap_variance, summary.total_variance),
            imputation_share = _safe_variance_share(imputation_variance, summary.total_variance),
            linearization_share = _safe_variance_share(linearization_variance, summary.total_variance),
            total_sd = sqrt(max(summary.total_variance, 0.0)),
            empirical_variance = summary.empirical_variance,
            V_res = bootstrap_variance,
            V_imp = imputation_variance,
            V_lin = linearization_variance,
        ))
    end

    return DataFrame(rows)
end

function _stage_path(cache_dir::AbstractString,
                     stage::Symbol;
                     b::Int = 0,
                     r::Int = 0,
                     k::Int = 0)
    if stage === :spec
        return joinpath(cache_dir, "spec.jld2")
    elseif stage === :observed
        return joinpath(cache_dir, "observed.jld2")
    elseif stage === :resample
        return joinpath(cache_dir, "resample", @sprintf("b%04d.jld2", b))
    elseif stage === :imputed
        return joinpath(cache_dir, "imputed", @sprintf("b%04d_r%04d.jld2", b, r))
    elseif stage === :linearized
        return joinpath(cache_dir, "linearized", @sprintf("b%04d_r%04d_k%04d.jld2", b, r, k))
    elseif stage === :measure
        return joinpath(cache_dir, "measure", @sprintf("b%04d_r%04d_k%04d.jld2", b, r, k))
    end

    throw(ArgumentError("Unsupported stage `$stage`."))
end

function _save_artifact(path::AbstractString, artifact)
    mkpath(dirname(path))
    _atomic_jldsave(path, "artifact" => artifact)
    return bytes2hex(SHA.sha256(read(path)))
end

function _save_stage_artifact(cache_dir::AbstractString,
                              stage::Symbol,
                              artifact;
                              b::Int = 0,
                              r::Int = 0,
                              k::Int = 0)
    path = _stage_path(cache_dir, stage; b = b, r = r, k = k)
    artifact_hash = _save_artifact(path, artifact)
    return path, artifact_hash
end

function _manifest_dataframe(rows)
    return isempty(rows) ? DataFrame(
        stage = Symbol[],
        b = Int[],
        r = Int[],
        k = Int[],
        seed = UInt64[],
        path = String[],
        artifact_hash = String[],
        artifact_kind = String[],
    ) : DataFrame(rows)
end

function _audit_dataframe(rows)
    return isempty(rows) ? DataFrame(
        stage = Symbol[],
        b = Int[],
        r = Int[],
        k = Int[],
        message = String[],
    ) : DataFrame(rows)
end

function pipeline_result_path(pipeline::NestedStochasticPipeline, spec::PipelineSpec)
    return joinpath(pipeline.cache_root, _pipeline_cache_key(spec), "result.jld2")
end

function load_pipeline_result(path::AbstractString)
    return JLD2.load(path, "result")
end

function save_pipeline_result(path::AbstractString,
                              result::PipelineResult;
                              overwrite::Bool = false)
    isfile(path) && !overwrite && throw(ArgumentError(
        "Refusing to overwrite existing PipelineResult at `$path` without overwrite=true.",
    ))
    _atomic_jldsave(path, "result" => result)
    return path
end

function load_pipeline_result(pipeline::NestedStochasticPipeline, spec::PipelineSpec)
    return load_pipeline_result(pipeline_result_path(pipeline, spec))
end

function _metadata_value(metadata::NamedTuple, key::Symbol, default = missing)
    return hasproperty(metadata, key) ? getproperty(metadata, key) : default
end

function run_pipeline(pipeline::NestedStochasticPipeline,
                      spec::PipelineSpec;
                      force::Bool = false,
                      progress::Bool = true)
    cache_dir = joinpath(pipeline.cache_root, _pipeline_cache_key(spec))
    mkpath(cache_dir)

    result_path = pipeline_result_path(pipeline, spec)
    if isfile(result_path) && !force
        return load_pipeline_result(result_path)
    end

    stage_rows = NamedTuple[]
    audit_rows = NamedTuple[
        (
            stage = :spec,
            b = 0,
            r = 0,
            k = 0,
            message = "Running wave $(spec.wave_id) with $(length(spec.active_candidates)) candidates, B=$(spec.B), R=$(spec.R), K=$(spec.K).",
        ),
    ]

    spec_path, spec_artifact_hash = _save_stage_artifact(cache_dir, :spec, (spec = spec,))
    push!(stage_rows, (
        stage = :spec,
        b = 0,
        r = 0,
        k = 0,
        seed = UInt64(0),
        path = spec_path,
        artifact_hash = spec_artifact_hash,
        artifact_kind = "PipelineSpec",
    ))

    observed = load_observed_data(pipeline, spec)
    observed_path, observed_hash = _save_stage_artifact(cache_dir, :observed, observed)
    push!(stage_rows, (
        stage = :observed,
        b = 0,
        r = 0,
        k = 0,
        seed = UInt64(0),
        path = observed_path,
        artifact_hash = observed_hash,
        artifact_kind = "ObservedData",
    ))

    all_results = MeasureResult[]
    leaf_progress = pm.Progress(
        spec.B * spec.R * spec.K;
        desc = "Nested BRK $(spec.wave_id)",
        dt = 1.0,
        enabled = progress,
    )

    for b in 1:spec.B
        resample = _draw_resample(observed, spec, b)
        resample_path, resample_hash = _save_stage_artifact(
            cache_dir,
            :resample,
            (
                indices = resample.indices,
                multiplicities = resample.multiplicities,
                seed = resample.seed,
            );
            b = b,
        )
        push!(stage_rows, (
            stage = :resample,
            b = b,
            r = 0,
            k = 0,
            seed = resample.seed,
            path = resample_path,
            artifact_hash = resample_hash,
            artifact_kind = "Resample",
        ))

        for r in 1:spec.R
            imputed = _impute_resample(resample, spec, r)
            imputed_path, imputed_hash = _save_stage_artifact(
                cache_dir,
                :imputed,
                (
                    table = imputed.table,
                    report = imputed.report,
                    seed = imputed.seed,
                    backend = imputed.backend,
                );
                b = b,
                r = r,
            )
            push!(stage_rows, (
                stage = :imputed,
                b = b,
                r = r,
                k = 0,
                seed = imputed.seed,
                path = imputed_path,
                artifact_hash = imputed_hash,
                artifact_kind = "ImputedData",
            ))

            for k in 1:spec.K
                linearized = _linearize_imputed(imputed, spec, k)
                linearized_artifact = compact_profile_artifact_dataframe(linearized.bundle)
                linearized_path, linearized_hash = _save_stage_artifact(
                    cache_dir,
                    :linearized,
                    linearized_artifact;
                    b = b,
                    r = r,
                    k = k,
                )
                push!(stage_rows, (
                    stage = :linearized,
                    b = b,
                    r = r,
                    k = k,
                    seed = linearized.seed,
                    path = linearized_path,
                    artifact_hash = linearized_hash,
                    artifact_kind = "LinearizedProfile",
                ))

                stage_results, stage_audits = _measure_results_for_profile(linearized, spec)
                append!(all_results, stage_results)
                append!(audit_rows, stage_audits)

                measure_df = _measure_results_dataframe(stage_results)
                measure_path, measure_hash = _save_stage_artifact(
                    cache_dir,
                    :measure,
                    measure_df;
                    b = b,
                    r = r,
                    k = k,
                )
                push!(stage_rows, (
                    stage = :measure,
                    b = b,
                    r = r,
                    k = k,
                    seed = UInt64(0),
                    path = measure_path,
                    artifact_hash = measure_hash,
                    artifact_kind = "MeasureResult",
                ))
                pm.next!(
                    leaf_progress;
                    showvalues = [
                        (:wave_id, spec.wave_id),
                        (:b, b),
                        (:r, r),
                        (:k, k),
                        (:imputer_backend, spec.imputer_backend),
                        (:linearizer_policy, spec.linearizer_policy),
                    ],
                )
            end
        end
    end

    measure_cube = _measure_results_dataframe(all_results)
    decomposition = compute_variance_decomposition(measure_cube)
    pooled_summaries = decomposition_table(decomposition)
    stage_manifest = _manifest_dataframe(stage_rows)
    audit_log = _audit_dataframe(audit_rows)

    result = PipelineResult(
        spec,
        cache_dir,
        stage_manifest,
        measure_cube,
        pooled_summaries,
        decomposition,
        audit_log,
    )

    save_pipeline_result(result_path, result; overwrite = true)
    pm.finish!(leaf_progress)
    return result
end

run(pipeline::NestedStochasticPipeline, spec::PipelineSpec;
    force::Bool = false,
    progress::Bool = true) =
    run_pipeline(pipeline, spec; force = force, progress = progress)

function run_batch(runner::BatchRunner,
                   batch::StudyBatchSpec;
                   force::Bool = false,
                   progress::Bool = true)
    results = PipelineResult[]
    metadata = NamedTuple[]
    batch_progress = pm.Progress(
        length(batch.items);
        desc = "Nested batch",
        dt = 1.0,
        enabled = progress,
    )

    for item in batch.items
        result = run_pipeline(runner.pipeline, item.spec; force = force, progress = false)
        push!(results, result)
        push!(metadata, item.metadata)
        pm.next!(
            batch_progress;
            showvalues = [
                (:wave_id, item.spec.wave_id),
                (:scenario_name, _metadata_value(item.metadata, :scenario_name)),
                (:m, _metadata_value(item.metadata, :m)),
                (:imputer_backend, item.spec.imputer_backend),
                (:linearizer_policy, item.spec.linearizer_policy),
            ],
        )
    end

    pm.finish!(batch_progress)
    return BatchRunResult(batch, results, metadata)
end

function _result_collection(result::PipelineResult)
    return (result,)
end

function _result_collection(results::AbstractVector{<:PipelineResult})
    return Tuple(results)
end

function _result_collection(results::AbstractDict{<:Any,<:PipelineResult})
    return Tuple(values(results))
end

function _result_collection(results::BatchRunResult)
    return Tuple(results.results)
end

function _result_spec_metadata(result::PipelineResult)
    spec = result.spec
    return (
        wave_id = spec.wave_id,
        active_candidates_key = join(spec.active_candidates, "|"),
        n_candidates = length(spec.active_candidates),
        groupings_key = join(String.(spec.groupings), "|"),
        measures_key = join(String.(spec.measures), "|"),
        B = spec.B,
        R = spec.R,
        K = spec.K,
        resample_policy = String(spec.resample_policy),
        imputer_backend = String(spec.imputer_backend),
        linearizer_policy = String(spec.linearizer_policy),
        consensus_tie_policy = String(spec.consensus_tie_policy),
        seed_namespace = spec.seed_namespace,
        schema_version = spec.schema_version,
        code_version = spec.code_version,
        cache_dir = result.cache_dir,
    )
end

function _decorate_result_table(df::DataFrame,
                               result::PipelineResult,
                               extra_meta::NamedTuple = (;))
    out = copy(df)
    meta = merge(_result_spec_metadata(result), extra_meta)

    for name in propertynames(meta)
        out[!, name] = fill(getproperty(meta, name), nrow(out))
    end

    return out
end

function _merge_result_tables(results, builder)
    tables = [builder(result) for result in _result_collection(results)]
    isempty(tables) && return DataFrame()
    return vcat(tables...; cols = :union)
end

function _merge_batch_result_tables(results::BatchRunResult, builder)
    tables = DataFrame[]

    for (idx, result) in enumerate(results.results)
        meta = merge(results.metadata[idx], (batch_index = idx,))
        push!(tables, builder(result, meta))
    end

    isempty(tables) && return DataFrame()
    return vcat(tables...; cols = :union)
end

"""
    pipeline_measure_table(result_or_results) -> DataFrame

Return the normalized raw measure cube augmented with fixed-spec metadata.
When passed a collection of `PipelineResult`s, rows are stacked with one row per
`(b, r, k, measure, grouping)` observation. `BatchRunResult` tables also carry
`batch_index` so duplicate specs remain distinct by position.
"""
function pipeline_measure_table(result::PipelineResult)
    return _decorate_result_table(result.measure_cube, result)
end

function pipeline_measure_table(result::PipelineResult, extra_meta::NamedTuple)
    return _decorate_result_table(result.measure_cube, result, extra_meta)
end

pipeline_measure_table(results::AbstractVector{<:PipelineResult}) =
    _merge_result_tables(results, pipeline_measure_table)

pipeline_measure_table(results::AbstractDict{<:Any,<:PipelineResult}) =
    _merge_result_tables(results, pipeline_measure_table)

pipeline_measure_table(results::BatchRunResult) =
    _merge_batch_result_tables(results, pipeline_measure_table)

"""
    pipeline_summary_table(result_or_results) -> DataFrame

Return pooled estimates and variance components augmented with fixed-spec
metadata. One row is produced per `(measure, grouping)` summary. The explicit
tree decomposition is reported in `bootstrap_variance`,
`imputation_variance`, and `linearization_variance`; legacy aliases
`V_res`, `V_imp`, and `V_lin` are retained for compatibility.
"""
function pipeline_summary_table(result::PipelineResult)
    return _decorate_result_table(result.pooled_summaries, result)
end

function pipeline_summary_table(result::PipelineResult, extra_meta::NamedTuple)
    return _decorate_result_table(result.pooled_summaries, result, extra_meta)
end

pipeline_summary_table(results::AbstractVector{<:PipelineResult}) =
    _merge_result_tables(results, pipeline_summary_table)

pipeline_summary_table(results::AbstractDict{<:Any,<:PipelineResult}) =
    _merge_result_tables(results, pipeline_summary_table)

pipeline_summary_table(results::BatchRunResult) =
    _merge_batch_result_tables(results, pipeline_summary_table)

"""
    pipeline_variance_decomposition_table(result_or_results) -> DataFrame

Return the tidy variance decomposition table for the nested
`bootstrap -> imputation -> linearization` pipeline tree.
"""
pipeline_variance_decomposition_table(result::PipelineResult) =
    pipeline_summary_table(result)

pipeline_variance_decomposition_table(result::PipelineResult, extra_meta::NamedTuple) =
    pipeline_summary_table(result, extra_meta)

pipeline_variance_decomposition_table(results::AbstractVector{<:PipelineResult}) =
    pipeline_summary_table(results)

pipeline_variance_decomposition_table(results::AbstractDict{<:Any,<:PipelineResult}) =
    pipeline_summary_table(results)

pipeline_variance_decomposition_table(results::BatchRunResult) =
    pipeline_summary_table(results)

function save_pipeline_variance_decomposition_csv(path::AbstractString, result_or_results)
    table = pipeline_variance_decomposition_table(result_or_results)
    return _write_table_csv(path, table)
end

"""
    pipeline_panel_table(result_or_results) -> DataFrame

Summarize the raw measure cube for plotting/reporting. The returned rows are
grouped by `(measure, grouping)` and include BRK-draw quantiles together with
the pooled estimate and variance decomposition. `G` is taken directly from the
stored measure rows and is never re-derived from `C` and `D` at reporting time.
"""
function pipeline_panel_table(result::PipelineResult)
    summary_lookup = Dict{Tuple{Symbol,Any},NamedTuple}()

    for row in eachrow(result.pooled_summaries)
        summary_lookup[(Symbol(row.measure), row.grouping)] = (
            estimate = Float64(row.estimate),
            bootstrap_variance = Float64(row.bootstrap_variance),
            imputation_variance = Float64(row.imputation_variance),
            linearization_variance = Float64(row.linearization_variance),
            bootstrap_share = Float64(row.bootstrap_share),
            imputation_share = Float64(row.imputation_share),
            linearization_share = Float64(row.linearization_share),
            V_res = Float64(row.V_res),
            V_imp = Float64(row.V_imp),
            V_lin = Float64(row.V_lin),
            total_variance = Float64(row.total_variance),
            total_sd = Float64(row.total_sd),
            empirical_variance = Float64(row.empirical_variance),
        )
    end

    rows = NamedTuple[]

    for subdf in groupby(result.measure_cube, [:measure, :grouping])
        measure = Symbol(subdf[1, :measure])
        grouping = subdf[1, :grouping]
        values = Float64.(subdf.value)
        summary = summary_lookup[(measure, grouping)]
        los = collect(skipmissing(subdf.value_lo))
        his = collect(skipmissing(subdf.value_hi))

        push!(rows, (
            measure = measure,
            grouping = grouping,
            n_draws = length(values),
            mean_value = mean(values),
            q05 = quantile(values, 0.05),
            q25 = quantile(values, 0.25),
            q50 = quantile(values, 0.50),
            q75 = quantile(values, 0.75),
            q95 = quantile(values, 0.95),
            min_value = minimum(values),
            max_value = maximum(values),
            value_lo_min = isempty(los) ? missing : minimum(Float64.(los)),
            value_hi_max = isempty(his) ? missing : maximum(Float64.(his)),
            estimate = summary.estimate,
            bootstrap_variance = summary.bootstrap_variance,
            imputation_variance = summary.imputation_variance,
            linearization_variance = summary.linearization_variance,
            bootstrap_share = summary.bootstrap_share,
            imputation_share = summary.imputation_share,
            linearization_share = summary.linearization_share,
            V_res = summary.V_res,
            V_imp = summary.V_imp,
            V_lin = summary.V_lin,
            total_variance = summary.total_variance,
            total_sd = summary.total_sd,
            empirical_variance = summary.empirical_variance,
        ))
    end

    panel = DataFrame(rows)
    return _decorate_result_table(panel, result)
end

function pipeline_panel_table(result::PipelineResult, extra_meta::NamedTuple)
    summary_lookup = Dict{Tuple{Symbol,Any},NamedTuple}()

    for row in eachrow(result.pooled_summaries)
        summary_lookup[(Symbol(row.measure), row.grouping)] = (
            estimate = Float64(row.estimate),
            bootstrap_variance = Float64(row.bootstrap_variance),
            imputation_variance = Float64(row.imputation_variance),
            linearization_variance = Float64(row.linearization_variance),
            bootstrap_share = Float64(row.bootstrap_share),
            imputation_share = Float64(row.imputation_share),
            linearization_share = Float64(row.linearization_share),
            V_res = Float64(row.V_res),
            V_imp = Float64(row.V_imp),
            V_lin = Float64(row.V_lin),
            total_variance = Float64(row.total_variance),
            total_sd = Float64(row.total_sd),
            empirical_variance = Float64(row.empirical_variance),
        )
    end

    rows = NamedTuple[]

    for subdf in groupby(result.measure_cube, [:measure, :grouping])
        measure = Symbol(subdf[1, :measure])
        grouping = subdf[1, :grouping]
        values = Float64.(subdf.value)
        summary = summary_lookup[(measure, grouping)]
        los = collect(skipmissing(subdf.value_lo))
        his = collect(skipmissing(subdf.value_hi))

        push!(rows, (
            measure = measure,
            grouping = grouping,
            n_draws = length(values),
            mean_value = mean(values),
            q05 = quantile(values, 0.05),
            q25 = quantile(values, 0.25),
            q50 = quantile(values, 0.50),
            q75 = quantile(values, 0.75),
            q95 = quantile(values, 0.95),
            min_value = minimum(values),
            max_value = maximum(values),
            value_lo_min = isempty(los) ? missing : minimum(Float64.(los)),
            value_hi_max = isempty(his) ? missing : maximum(Float64.(his)),
            estimate = summary.estimate,
            bootstrap_variance = summary.bootstrap_variance,
            imputation_variance = summary.imputation_variance,
            linearization_variance = summary.linearization_variance,
            bootstrap_share = summary.bootstrap_share,
            imputation_share = summary.imputation_share,
            linearization_share = summary.linearization_share,
            V_res = summary.V_res,
            V_imp = summary.V_imp,
            V_lin = summary.V_lin,
            total_variance = summary.total_variance,
            total_sd = summary.total_sd,
            empirical_variance = summary.empirical_variance,
        ))
    end

    panel = DataFrame(rows)
    return _decorate_result_table(panel, result, extra_meta)
end

pipeline_panel_table(results::AbstractVector{<:PipelineResult}) =
    _merge_result_tables(results, pipeline_panel_table)

pipeline_panel_table(results::AbstractDict{<:Any,<:PipelineResult}) =
    _merge_result_tables(results, pipeline_panel_table)

pipeline_panel_table(results::BatchRunResult) =
    _merge_batch_result_tables(results, pipeline_panel_table)

function _as_panel_table(result_or_results)
    if result_or_results isa AbstractDataFrame
        return DataFrame(result_or_results)
    end

    return pipeline_panel_table(result_or_results)
end

function _require_panel_column(df::AbstractDataFrame,
                               column::Symbol,
                               context::AbstractString)
    hasproperty(df, column) || throw(ArgumentError(
        "$context requires column `$column`, but it is missing from the panel table.",
    ))
    return column
end

function _panel_measure_set(measures)
    raw = measures isa Symbol || measures isa AbstractString ? (measures,) : Tuple(measures)
    return Set(_normalize_measure(measure) for measure in raw)
end

function _panel_grouping_set(groupings)
    raw = groupings isa Symbol || groupings isa AbstractString ? (groupings,) : Tuple(groupings)
    return Set(Symbol(grouping) for grouping in raw)
end

function _panel_measure_rows(df::AbstractDataFrame, measures)
    wanted = _panel_measure_set(measures)
    return df[[Symbol(row.measure) in wanted for row in eachrow(df)], :]
end

function _panel_grouping_rows(df::AbstractDataFrame, groupings)
    wanted = _panel_grouping_set(groupings)
    return df[
        [(!ismissing(row.grouping)) && (Symbol(row.grouping) in wanted) for row in eachrow(df)],
        :,
    ]
end

function _panel_m_column(df::AbstractDataFrame)
    if hasproperty(df, :m)
        return :m
    end

    _require_panel_column(df, :n_candidates, "Pipeline panel selection")
    return :n_candidates
end

function select_pipeline_panel_rows(result_or_results;
                                    year = nothing,
                                    wave_id = nothing,
                                    scenario_name = nothing,
                                    m = nothing,
                                    imputer_backend = nothing,
                                    measures = nothing,
                                    groupings = nothing,
                                    include_grouped::Union{Nothing,Bool} = nothing)
    panel = _as_panel_table(result_or_results)

    if year !== nothing
        if hasproperty(panel, :year)
            panel = panel[panel.year .== Int(year), :]
        elseif hasproperty(panel, :wave_id)
            panel = panel[String.(panel.wave_id) .== string(year), :]
        else
            throw(ArgumentError(
                "Filtering by `year` requires either a batch metadata column `:year` or `:wave_id`.",
            ))
        end
    end

    if wave_id !== nothing
        _require_panel_column(panel, :wave_id, "Filtering by wave_id")
        panel = panel[String.(panel.wave_id) .== String(wave_id), :]
    end

    if scenario_name !== nothing
        _require_panel_column(panel, :scenario_name, "Filtering by scenario_name")
        panel = panel[String.(panel.scenario_name) .== String(scenario_name), :]
    end

    if m !== nothing
        mcol = _panel_m_column(panel)
        panel = panel[panel[!, mcol] .== Int(m), :]
    end

    if imputer_backend !== nothing
        _require_panel_column(panel, :imputer_backend, "Filtering by imputer_backend")
        backend_name = String(Symbol(imputer_backend))
        panel = panel[String.(panel.imputer_backend) .== backend_name, :]
    end

    measures === nothing || (panel = _panel_measure_rows(panel, measures))
    groupings === nothing || (panel = _panel_grouping_rows(panel, groupings))

    if include_grouped !== nothing
        grouped_mask = .!ismissing.(panel.grouping)
        panel = include_grouped ? panel[grouped_mask, :] : panel[.!grouped_mask, :]
    end

    isempty(panel) && return panel

    sort_cols = Symbol[]
    mcol = _panel_m_column(panel)
    push!(sort_cols, mcol)
    hasproperty(panel, :measure) && push!(sort_cols, :measure)
    hasproperty(panel, :grouping) && push!(sort_cols, :grouping)
    return sort(panel, sort_cols)
end

function pipeline_candidate_label(rows::AbstractDataFrame)
    isempty(rows) && return ""

    if hasproperty(rows, :candidate_label)
        label_values = collect(skipmissing(rows.candidate_label))

        if !isempty(label_values)
            mcol = _panel_m_column(rows)
            idx = argmax(rows[!, mcol])
            label = rows[idx, :candidate_label]
            !ismissing(label) && return String(label)

            labels = unique(String.(label_values))
            !isempty(labels) && return first(labels)
        end
    end

    _require_panel_column(rows, :active_candidates_key, "Resolving candidate label")
    mcol = _panel_m_column(rows)
    idx = argmax(rows[!, mcol])
    return describe_candidate_set(split(String(rows[idx, :active_candidates_key]), '|'))
end

function pipeline_scenario_plot_data(result_or_results;
                                     year = nothing,
                                     wave_id = nothing,
                                     scenario_name = nothing,
                                     imputer_backend = nothing,
                                     measures = [:Psi, :R, :HHI, :RHHI])
    rows = select_pipeline_panel_rows(
        result_or_results;
        year = year,
        wave_id = wave_id,
        scenario_name = scenario_name,
        imputer_backend = imputer_backend,
        measures = measures,
        include_grouped = false,
    )
    isempty(rows) && throw(ArgumentError("No scenario rows matched the requested pipeline selection."))

    return (
        rows = rows,
        m_values = collect(unique(rows[!, _panel_m_column(rows)])),
        candidate_label = pipeline_candidate_label(rows),
    )
end

function pipeline_group_plot_data(result_or_results;
                                  year = nothing,
                                  wave_id = nothing,
                                  scenario_name = nothing,
                                  imputer_backend = nothing,
                                  measures = [:C, :D, :G],
                                  groupings = nothing)
    rows = select_pipeline_panel_rows(
        result_or_results;
        year = year,
        wave_id = wave_id,
        scenario_name = scenario_name,
        imputer_backend = imputer_backend,
        measures = measures,
        groupings = groupings,
        include_grouped = true,
    )
    isempty(rows) && throw(ArgumentError("No grouped rows matched the requested pipeline selection."))

    return (
        rows = rows,
        m_values = collect(unique(rows[!, _panel_m_column(rows)])),
        grouping_values = collect(unique(Symbol.(skipmissing(rows.grouping)))),
        candidate_label = pipeline_candidate_label(rows),
    )
end

function _panel_stat_column(statistic::Symbol)
    if statistic === :median
        return :q50
    elseif statistic === :mean
        return :mean_value
    elseif statistic in (:estimate, :q05, :q25, :q50, :q75, :q95, :min_value, :max_value)
        return statistic
    end

    throw(ArgumentError(
        "Unsupported panel statistic `$statistic`. Supported statistics: estimate, mean, median, q05, q25, q50, q75, q95, min_value, max_value.",
    ))
end

function pipeline_group_heatmap_values(result_or_results;
                                       year = nothing,
                                       wave_id = nothing,
                                       scenario_name = nothing,
                                       imputer_backend = nothing,
                                       measures = [:C, :D, :G],
                                       groupings = nothing,
                                       statistic::Symbol = :median)
    data = pipeline_group_plot_data(
        result_or_results;
        year = year,
        wave_id = wave_id,
        scenario_name = scenario_name,
        imputer_backend = imputer_backend,
        measures = measures,
        groupings = groupings,
    )
    rows = data.rows
    m_values = data.m_values
    grouping_values = data.grouping_values
    stat_col = _panel_stat_column(statistic)
    wanted_measures = sort!(collect(_panel_measure_set(measures)))
    matrices = Dict{Symbol,Matrix{Float64}}()
    m_lookup = Dict(Int(m) => idx for (idx, m) in enumerate(m_values))
    grouping_lookup = Dict(group => idx for (idx, group) in enumerate(grouping_values))

    for measure in wanted_measures
        subdf = rows[rows.measure .== measure, :]
        isempty(subdf) && throw(ArgumentError(
            "No grouped rows matched measure `$measure` for the requested pipeline selection. " *
            "This usually means the measure was not computed in the underlying batch result.",
        ))
        z = fill(NaN, length(m_values), length(grouping_values))

        for row in eachrow(subdf)
            z[m_lookup[Int(row[_panel_m_column(rows)])], grouping_lookup[Symbol(row.grouping)]] =
                Float64(row[stat_col])
        end

        matrices[measure] = z
    end

    return merge(data, (matrices = matrices, statistic = stat_col))
end

function plot_pipeline_scenario(result_or_results; kwargs...)
    return _call_plotting_extension(:plot_pipeline_scenario, result_or_results; kwargs...)
end

function plot_pipeline_group_lines(result_or_results; kwargs...)
    return _call_plotting_extension(:plot_pipeline_group_lines, result_or_results; kwargs...)
end

function plot_pipeline_group_heatmap(result_or_results; kwargs...)
    return _call_plotting_extension(:plot_pipeline_group_heatmap, result_or_results; kwargs...)
end

function save_pipeline_plot(fig, stem::AbstractString; kwargs...)
    return _call_plotting_extension(:save_pipeline_plot, fig, stem; kwargs...)
end
