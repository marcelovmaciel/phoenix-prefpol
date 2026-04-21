@inline function _population_variance(values)
    vals = Float64.(collect(values))
    length(vals) <= 1 && return 0.0
    return max(var(vals; corrected = false), 0.0)
end

@inline function _safe_variance_share(component::Real, total::Real)
    total64 = Float64(total)
    iszero(total64) && return 0.0
    return Float64(component) / total64
end

function _decomposition_context(subdf::AbstractDataFrame, id_cols::Vector{Symbol})
    isempty(id_cols) && return "all rows"
    return join(["$(name)=$(subdf[1, name])" for name in id_cols], ", ")
end

function _require_decomposition_columns(df::AbstractDataFrame,
                                        required::Vector{Symbol},
                                        context::AbstractString)
    missing_cols = setdiff(required, Symbol.(names(df)))
    isempty(missing_cols) || throw(ArgumentError(
        "Variance decomposition for $(context) is missing required columns $(missing_cols).",
    ))
    return nothing
end

function _assert_rectangular_tree(subdf::AbstractDataFrame;
                                  bootstrap_col::Symbol,
                                  imputation_col::Symbol,
                                  linearization_col::Symbol,
                                  context::AbstractString)
    leaf_keys = select(subdf, bootstrap_col, imputation_col, linearization_col)
    nrow(unique(leaf_keys)) == nrow(leaf_keys) || throw(ArgumentError(
        "Variance decomposition for $(context) found duplicate leaf rows for " *
        "($(bootstrap_col), $(imputation_col), $(linearization_col)).",
    ))

    imputation_counts = combine(
        groupby(select(subdf, bootstrap_col, imputation_col), bootstrap_col),
        imputation_col => (x -> length(unique(x))) => :n_imputation,
    )
    length(unique(imputation_counts.n_imputation)) <= 1 || throw(ArgumentError(
        "Variance decomposition for $(context) requires the realized tree to have the same number " *
        "of imputation branches within each bootstrap branch.",
    ))

    linearization_counts = combine(
        groupby(select(subdf, bootstrap_col, imputation_col, linearization_col),
                [bootstrap_col, imputation_col]),
        linearization_col => (x -> length(unique(x))) => :n_linearization,
    )
    length(unique(linearization_counts.n_linearization)) <= 1 || throw(ArgumentError(
        "Variance decomposition for $(context) requires the realized tree to have the same number " *
        "of linearization branches within each bootstrap/imputation branch.",
    ))

    return nothing
end

"""
    tree_variance_decomposition_table(df; kwargs...) -> DataFrame

Compute the explicit `bootstrap -> imputation -> linearization` variance
decomposition for scalar leaf-level outputs stored in `df`.

The decomposition follows the realized pipeline tree exactly:
- `bootstrap_variance = Var_b(E[M | b])`
- `imputation_variance = E_b(Var_i(E[M | b, i]))`
- `linearization_variance = E_b(E_i(Var_l(M | b, i)))`

`total_variance` is the population variance across all realized leaves with
equal weight, so the returned table satisfies
`total_variance ≈ bootstrap_variance + imputation_variance + linearization_variance`
up to floating-point error.
"""
function tree_variance_decomposition_table(df::AbstractDataFrame;
                                           id_cols = [:measure, :grouping],
                                           bootstrap_col::Symbol = :b,
                                           imputation_col::Symbol = :r,
                                           linearization_col::Symbol = :k,
                                           value_col::Symbol = :value,
                                           check::Bool = true,
                                           atol::Real = 1e-12,
                                           rtol::Real = 1e-10)
    id_syms = Symbol.(collect(id_cols))
    required = unique!([id_syms...,
                        bootstrap_col,
                        imputation_col,
                        linearization_col,
                        value_col])
    _require_decomposition_columns(df, required, "input table")

    grouped = isempty(id_syms) ? [DataFrame(df)] : groupby(DataFrame(df), id_syms)
    rows = NamedTuple[]

    for subdf in grouped
        context = _decomposition_context(subdf, id_syms)
        _assert_rectangular_tree(
            subdf;
            bootstrap_col = bootstrap_col,
            imputation_col = imputation_col,
            linearization_col = linearization_col,
            context = context,
        )

        any(!isfinite, Float64.(subdf[!, value_col])) && throw(ArgumentError(
            "Variance decomposition for $(context) requires finite `$(value_col)` values.",
        ))

        # The pipeline tree is bootstrap -> imputation -> linearization. We first
        # collapse the deepest branch to E[M | b, i], then the middle branch to
        # E[M | b], and finally take population variances at the appropriate level.
        mean_bi = combine(
            groupby(subdf, [bootstrap_col, imputation_col]),
            value_col => mean => :mean_bi,
        )
        mean_b = combine(groupby(mean_bi, bootstrap_col), :mean_bi => mean => :mean_b)

        estimate = mean(Float64.(subdf[!, value_col]))
        total_variance = _population_variance(subdf[!, value_col])
        bootstrap_variance = _population_variance(mean_b.mean_b)
        imputation_variance = mean(
            _population_variance(group.mean_bi)
            for group in groupby(mean_bi, bootstrap_col)
        )
        linearization_variance = mean(
            _population_variance(group[!, value_col])
            for group in groupby(subdf, [bootstrap_col, imputation_col])
        )
        component_sum = bootstrap_variance + imputation_variance + linearization_variance

        if check && !isapprox(total_variance, component_sum; atol = atol, rtol = rtol)
            throw(ArgumentError(
                "Tree variance decomposition failed for $(context): " *
                "total_variance=$(total_variance), " *
                "bootstrap_variance=$(bootstrap_variance), " *
                "imputation_variance=$(imputation_variance), " *
                "linearization_variance=$(linearization_variance), " *
                "difference=$(total_variance - component_sum).",
            ))
        end

        id_values = (; (name => subdf[1, name] for name in id_syms)...)
        push!(rows, merge(id_values, (
            estimate = estimate,
            total_variance = total_variance,
            bootstrap_variance = bootstrap_variance,
            imputation_variance = imputation_variance,
            linearization_variance = linearization_variance,
            bootstrap_share = _safe_variance_share(bootstrap_variance, total_variance),
            imputation_share = _safe_variance_share(imputation_variance, total_variance),
            linearization_share = _safe_variance_share(linearization_variance, total_variance),
            empirical_variance = total_variance,
        )))
    end

    return DataFrame(rows)
end

function _csv_escape(value)
    raw = value === missing ? "" : string(value)
    return "\"" * replace(raw, "\"" => "\"\"") * "\""
end

function _write_table_csv(path::AbstractString, df::AbstractDataFrame)
    mkpath(dirname(path))
    open(path, "w") do io
        println(io, join((_csv_escape(name) for name in names(df)), ","))

        for row in eachrow(df)
            println(io, join((_csv_escape(row[name]) for name in names(df)), ","))
        end
    end

    return path
end
