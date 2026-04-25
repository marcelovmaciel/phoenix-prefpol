const DEFAULT_VARIANCE_DECOMPOSITION_ESTIMATOR = :existing_nested_moments
const DEFAULT_PAPER_VARIANCE_MEASURES = [:Psi, :R, :HHI, :RHHI, :C, :D, :Sep, :S]
const DEFAULT_PAPER_VARIANCE_MEASURE_LABELS = Dict(
    :Psi => "Ψ",
    :R => "R",
    :HHI => "HHI",
    :RHHI => "RHHI",
    :C => "C",
    :D => "D",
    :Sep => "1-O",
    :S => "S",
)

const _VARIANCE_REPORT_COMPONENTS = (
    (:bootstrap, :bootstrap_variance),
    (:imputation, :imputation_variance),
    (:linearization, :linearization_variance),
    (:total, :total_variance),
)
const _VARIANCE_REPORT_DECOMPOSITION_COLUMNS = [
    :bootstrap_variance,
    :imputation_variance,
    :linearization_variance,
    :total_variance,
]

struct VarianceDecompositionReportSpec
    selections::Any
    m_values::Any
    measures::Any
    groupings::Any
    pool_over_m::Bool
    pool_over_selections::Bool
    include_empirical::Bool
    estimator::Symbol
end

function VarianceDecompositionReportSpec(; selections = nothing,
                                         m_values = nothing,
                                         measures = nothing,
                                         groupings = nothing,
                                         pool_over_m::Bool = true,
                                         pool_over_selections::Bool = false,
                                         include_empirical::Bool = false,
                                         estimator::Symbol = DEFAULT_VARIANCE_DECOMPOSITION_ESTIMATOR)
    estimator === DEFAULT_VARIANCE_DECOMPOSITION_ESTIMATOR || throw(ArgumentError(
        "Unsupported variance decomposition estimator `$estimator`. " *
        "Only `:$(DEFAULT_VARIANCE_DECOMPOSITION_ESTIMATOR)` is implemented.",
    ))
    return VarianceDecompositionReportSpec(
        selections,
        m_values,
        measures,
        groupings,
        pool_over_m,
        pool_over_selections,
        include_empirical,
        estimator,
    )
end

function normalize_variance_measure(measure)
    sym = Symbol(measure)
    sym === Symbol("Ψ") && return :Psi
    sym === :D_consensus && return :D
    sym === :D_clean && return :D
    sym === Symbol("1-O") && return :Sep
    sym === Symbol("1 - O") && return :Sep
    sym === :O_complement && return :Sep
    sym === :G_median && return :G_median
    return _normalize_measure(sym)
end

_as_vector(x::Symbol) = [x]
_as_vector(x::AbstractString) = [x]
_as_vector(x::NamedTuple) = [x]
_as_vector(x::Number) = [x]
_as_vector(x) = collect(x)

function _variance_report_measure_list(measures, available::AbstractVector{<:Symbol})
    available_set = Set(available)

    if measures === nothing || measures === :paper
        return [measure for measure in DEFAULT_PAPER_VARIANCE_MEASURES if measure in available_set]
    elseif measures === :all
        order = [
            DEFAULT_PAPER_VARIANCE_MEASURES...,
            :D_median,
            :G,
            :G_median,
            :O,
            :O_smoothed,
            :Gsep,
            :S_old,
        ]
        ordered = [measure for measure in order if measure in available_set]
        extras = sort(setdiff(available, ordered); by = string)
        return [ordered...; extras...]
    end

    normalized = unique!([normalize_variance_measure(measure) for measure in _as_vector(measures)])
    missing_measures = setdiff(normalized, available)
    isempty(missing_measures) || throw(ArgumentError(
        "Requested variance decomposition measures $(missing_measures) are not present in the input table.",
    ))
    return normalized
end

_variance_measure_label(measure::Symbol) =
    get(DEFAULT_PAPER_VARIANCE_MEASURE_LABELS, measure, String(measure))

function _has_all_columns(df::AbstractDataFrame, cols)
    names_set = Set(Symbol.(names(df)))
    return all(col -> col in names_set, cols)
end

function _canonicalize_decomposition_columns!(df::DataFrame)
    if !(:bootstrap_variance in Symbol.(names(df))) && :V_res in Symbol.(names(df))
        df[!, :bootstrap_variance] = df.V_res
    end
    if !(:imputation_variance in Symbol.(names(df))) && :V_imp in Symbol.(names(df))
        df[!, :imputation_variance] = df.V_imp
    end
    if !(:linearization_variance in Symbol.(names(df))) && :V_lin in Symbol.(names(df))
        df[!, :linearization_variance] = df.V_lin
    end
    return df
end

function _ensure_derived_variance_measures(df::DataFrame)
    :measure in Symbol.(names(df)) || return df
    measures = [Symbol(measure) for measure in df.measure]
    :Sep in measures && return df
    :O in measures || return df

    derived = copy(df[measures .== :O, :])
    derived[!, :measure] = fill(:Sep, nrow(derived))

    # The paper reports 1-O. For the variance components, translating O by
    # `1 - O` leaves every component unchanged; only the point estimate changes.
    if :estimate in Symbol.(names(derived))
        derived[!, :estimate] = 1 .- Float64.(derived.estimate)
    end

    return vcat(df, derived; cols = :union)
end

_is_decomposition_table(df::AbstractDataFrame) =
    _has_all_columns(_canonicalize_decomposition_columns!(DataFrame(df)), [:measure, _VARIANCE_REPORT_DECOMPOSITION_COLUMNS...])

_is_leaf_table(df::AbstractDataFrame) = _has_all_columns(df, [:b, :r, :k, :value])

function _branch_counts_table(df::DataFrame, id_cols::Vector{Symbol})
    grouped = isempty(id_cols) ? [df] : groupby(df, id_cols)
    rows = NamedTuple[]

    for subdf in grouped
        id_values = (; (name => subdf[1, name] for name in id_cols)...)
        push!(rows, merge(id_values, (
            B = length(unique(subdf.b)),
            R = length(unique(subdf.r)),
            K = length(unique(subdf.k)),
        )))
    end

    return DataFrame(rows)
end

function _variance_decomposition_input_table(input)
    if input isa AbstractDataFrame
        df = DataFrame(input)
        _canonicalize_decomposition_columns!(df)
        if _has_all_columns(df, [:measure, _VARIANCE_REPORT_DECOMPOSITION_COLUMNS...])
            return _ensure_derived_variance_measures(df)
        end

        if _is_leaf_table(df)
            skip_cols = Set([:b, :r, :k, :value, :value_lo, :value_hi, :lower, :upper, :diagnostics])
            id_cols = [Symbol(name) for name in names(df) if !(Symbol(name) in skip_cols)]
            out = tree_variance_decomposition_table(
                df;
                id_cols = id_cols,
                bootstrap_col = :b,
                imputation_col = :r,
                linearization_col = :k,
                value_col = :value,
            )
            counts = _branch_counts_table(df, id_cols)
            out = isempty(id_cols) ? hcat(out, counts[:, [:B, :R, :K]]) :
                  leftjoin(out, counts; on = id_cols, matchmissing = :equal)
            return _ensure_derived_variance_measures(out)
        end

        throw(ArgumentError(
            "Variance decomposition report input DataFrame must either contain decomposition " *
            "columns $(String.(_VARIANCE_REPORT_DECOMPOSITION_COLUMNS)) or leaf columns `b`, `r`, `k`, and `value`.",
        ))
    end

    return _ensure_derived_variance_measures(pipeline_variance_decomposition_table(input))
end

function _selection_matches(row, selection::NamedTuple)
    for name in propertynames(selection)
        hasproperty(row, name) || return false
        expected = getproperty(selection, name)
        actual = getproperty(row, name)
        ismissing(actual) && return false
        actual == expected && continue
        string(actual) == string(expected) || return false
    end
    return true
end

function _variance_report_filter!(df::DataFrame, spec::VarianceDecompositionReportSpec)
    if spec.m_values !== nothing && :m in Symbol.(names(df))
        wanted_m = Set(Int.(collect(_as_vector(spec.m_values))))
        filter!(:m => value -> !ismissing(value) && Int(value) in wanted_m, df)
    end

    if spec.selections !== nothing
        selection_list = [NamedTuple(selection) for selection in _as_vector(spec.selections)]
        filter!(row -> any(_selection_matches(row, selection) for selection in selection_list), df)
    end

    isempty(df) && return df, Symbol[]

    df[!, :measure] = [normalize_variance_measure(value) for value in df[!, :measure]]
    measures = _variance_report_measure_list(spec.measures, unique(Symbol.(df.measure)))
    measure_order = Dict(measure => idx for (idx, measure) in enumerate(measures))
    filter!(:measure => measure -> haskey(measure_order, Symbol(measure)), df)

    if spec.groupings !== nothing && :grouping in Symbol.(names(df))
        wanted_groupings = Set(Symbol.(collect(_as_vector(spec.groupings))))
        filter!(:grouping => value -> ismissing(value) || Symbol(value) in wanted_groupings, df)
    end

    sort!(df, [order(:measure, by = measure -> get(measure_order, Symbol(measure), typemax(Int)))])
    return df, measures
end

function _count_or_missing(row, name::Symbol)
    hasproperty(row, name) && !ismissing(getproperty(row, name)) && return Int(getproperty(row, name))
    return missing
end

function _tree_dimension_notes(B, R, K)
    notes = String[]
    !ismissing(B) && B == 1 && push!(notes, "bootstrap component is zero because B=1")
    !ismissing(R) && R == 1 && push!(notes, "imputation component is zero because R=1")
    !ismissing(K) && K == 1 && push!(notes, "linearization component is zero because K=1")
    return join(notes, "; ")
end

function _source_result(row)
    hasproperty(row, :batch_index) && !ismissing(row.batch_index) && return string("batch:", row.batch_index)
    hasproperty(row, :cache_dir) && !ismissing(row.cache_dir) && return string(row.cache_dir)
    return missing
end

_get_or_missing(row, name::Symbol) = hasproperty(row, name) ? getproperty(row, name) : missing

function _fine_row(row, component::Symbol, value, measure::Symbol, spec::VarianceDecompositionReportSpec)
    B = _count_or_missing(row, :B)
    R = _count_or_missing(row, :R)
    K = _count_or_missing(row, :K)
    grouping = _get_or_missing(row, :grouping)

    return (
        year = _get_or_missing(row, :year),
        wave_id = _get_or_missing(row, :wave_id),
        scenario_name = _get_or_missing(row, :scenario_name),
        m = _get_or_missing(row, :m),
        grouping = grouping,
        grouping_label = ismissing(grouping) ? missing : string(grouping),
        measure = measure,
        measure_label = _variance_measure_label(measure),
        component = component,
        value = Float64(value),
        estimator = spec.estimator,
        B = B,
        R = R,
        K = K,
        imputer_backend = _get_or_missing(row, :imputer_backend),
        linearizer_policy = _get_or_missing(row, :linearizer_policy),
        consensus_tie_policy = _get_or_missing(row, :consensus_tie_policy),
        source_result = _source_result(row),
        notes = _tree_dimension_notes(B, R, K),
    )
end

function variance_decomposition_fine_table(input,
                                           spec::VarianceDecompositionReportSpec = VarianceDecompositionReportSpec())
    base = _variance_decomposition_input_table(input)
    filtered, _ = _variance_report_filter!(base, spec)
    rows = NamedTuple[]

    for row in eachrow(filtered)
        measure = Symbol(row.measure)

        for (component, col) in _VARIANCE_REPORT_COMPONENTS
            push!(rows, _fine_row(row, component, row[col], measure, spec))
        end

        if spec.include_empirical
            hasproperty(row, :empirical_variance) || throw(ArgumentError(
                "`include_empirical=true` requires an `empirical_variance` column.",
            ))
            push!(rows, _fine_row(row, :empirical, row.empirical_variance, measure, spec))
        end
    end

    return DataFrame(rows)
end

function _pooled_group_cols(fine::AbstractDataFrame, spec::VarianceDecompositionReportSpec)
    cols = Symbol[
        :grouping,
        :measure,
        :measure_label,
        :component,
        :estimator,
        :imputer_backend,
        :linearizer_policy,
        :consensus_tie_policy,
    ]

    !spec.pool_over_m && push!(cols, :m)
    if !spec.pool_over_selections
        append!(cols, [:year, :wave_id, :scenario_name])
    end

    available = Set(Symbol.(names(fine)))
    return [col for col in cols if col in available]
end

function _selection_label(subdf::AbstractDataFrame, spec::VarianceDecompositionReportSpec)
    spec.pool_over_selections && return "pooled selections"
    parts = String[]
    for name in (:year, :wave_id, :scenario_name)
        name in Symbol.(names(subdf)) || continue
        value = subdf[1, name]
        ismissing(value) || push!(parts, string(name, "=", value))
    end
    return isempty(parts) ? "all selections" : join(parts, " | ")
end

function _m_pool_label(subdf::AbstractDataFrame, spec::VarianceDecompositionReportSpec)
    :m in Symbol.(names(subdf)) || return "all m"
    spec.pool_over_m || return string("m=", subdf[1, :m])
    vals = sort(unique(skipmissing(subdf.m)))
    isempty(vals) && return "all m"
    return length(vals) == 1 ? string("m=", first(vals)) : string("m=", first(vals), ":", last(vals))
end

function _joined_notes(values)
    vals = unique([String(value) for value in skipmissing(values) if !isempty(String(value))])
    return join(vals, "; ")
end

function variance_decomposition_pooled_table(fine::AbstractDataFrame,
                                             spec::VarianceDecompositionReportSpec = VarianceDecompositionReportSpec())
    isempty(fine) && return DataFrame()
    group_cols = _pooled_group_cols(fine, spec)
    rows = NamedTuple[]

    for subdf in groupby(DataFrame(fine), group_cols)
        vals = Float64.(subdf.value)
        row_values = (; (col => subdf[1, col] for col in group_cols)...)
        push!(rows, merge(row_values, (
            selection_label = _selection_label(subdf, spec),
            m_pool_label = _m_pool_label(subdf, spec),
            grouping_label = :grouping_label in Symbol.(names(subdf)) ? subdf[1, :grouping_label] : missing,
            q25 = quantile(vals, 0.25),
            median = median(vals),
            q75 = quantile(vals, 0.75),
            mean = mean(vals),
            n_cells_pooled = nrow(subdf),
            notes = :notes in Symbol.(names(subdf)) ? _joined_notes(subdf.notes) : "",
        )))
    end

    return DataFrame(rows)
end

function variance_decomposition_report(input,
                                       spec::VarianceDecompositionReportSpec = VarianceDecompositionReportSpec())
    fine = variance_decomposition_fine_table(input, spec)
    pooled = variance_decomposition_pooled_table(fine, spec)
    return fine, pooled
end

function plot_variance_decomposition_dotwhisker(pooled_table; kwargs...)
    return _call_plotting_extension(:plot_variance_decomposition_dotwhisker, pooled_table; kwargs...)
end

function plot_variance_decomposition_boxplot(pooled_table; kwargs...)
    return _call_plotting_extension(:plot_variance_decomposition_boxplot, pooled_table; kwargs...)
end
