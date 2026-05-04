const SINGLE_PEAKEDNESS_OUTPUT_FILES = (
    axis_summary = "single_peakedness_axis_summary.csv",
    best_axes = "single_peakedness_best_axes.csv",
    support_classification = "single_peakedness_support_classification.csv",
    row_classification = "single_peakedness_row_classification.csv",
)

const SINGLE_PEAKEDNESS_REALIZATION_CANDIDATES = [
    :year, :m, :scenario_name, :candidate_set, :candidate_set_name,
    :imputer_backend, :linearizer_policy, :b, :r, :k,
]

function _path_value(input, key::Symbol)
    if input isa NamedTuple
        return getproperty(input, key)
    elseif input isa AbstractDict
        haskey(input, key) && return input[key]
        haskey(input, String(key)) && return input[String(key)]
    end
    throw(ArgumentError("explicit single-peakedness input is missing key $(key)"))
end

function _load_single_peakedness_dir(dir::AbstractString)
    dfs = Dict{Symbol,DataFrame}()
    for key in keys(SINGLE_PEAKEDNESS_OUTPUT_FILES)
        file = getproperty(SINGLE_PEAKEDNESS_OUTPUT_FILES, key)
        path = joinpath(dir, file)
        isfile(path) || throw(ArgumentError("missing required single-peakedness file: $(path)"))
        dfs[key] = DataFrame(CSV.File(path))
    end
    return (; (key => dfs[key] for key in keys(SINGLE_PEAKEDNESS_OUTPUT_FILES))...)
end

function load_single_peakedness_outputs(input)
    if input isa AbstractString
        return _load_single_peakedness_dir(input)
    elseif input isa AbstractVector
        isempty(input) && throw(ArgumentError("single-peakedness input directory vector is empty"))
        parts = [_load_single_peakedness_dir(String(dir)) for dir in input]
        return (;
            axis_summary = vcat((p.axis_summary for p in parts)...; cols=:union),
            best_axes = vcat((p.best_axes for p in parts)...; cols=:union),
            support_classification = vcat((p.support_classification for p in parts)...; cols=:union),
            row_classification = vcat((p.row_classification for p in parts)...; cols=:union),
        )
    elseif input isa NamedTuple || input isa AbstractDict
        return (;
            axis_summary = DataFrame(CSV.File(String(_path_value(input, :axis_summary)))),
            best_axes = DataFrame(CSV.File(String(_path_value(input, :best_axes)))),
            support_classification = DataFrame(CSV.File(String(_path_value(input, :support_classification)))),
            row_classification = DataFrame(CSV.File(String(_path_value(input, :row_classification)))),
        )
    end
    throw(ArgumentError("single-peakedness input must be a directory, vector of directories, NamedTuple, or Dict"))
end

function require_columns(df, cols; context="")
    wanted = Symbol.(cols)
    missing_cols = [col for col in wanted if col ∉ propertynames(df)]
    if !isempty(missing_cols)
        prefix = isempty(context) ? "table" : context
        throw(ArgumentError("$(prefix) is missing required column(s): $(join(string.(missing_cols), ", "))"))
    end
    return nothing
end

function _filter_in!(mask, df, col::Symbol, values)
    values === nothing && return mask
    col ∈ propertynames(df) || return mask
    allowed = Set(values isa AbstractSet ? collect(values) : collect(values))
    return mask .& in.(df[!, col], Ref(allowed))
end

function filter_single_peakedness(df; years=nothing, m_values=nothing, scenario_names=nothing,
                                  imputer_backends=nothing, linearizer_policies=nothing,
                                  b=nothing, r=nothing, k=nothing)
    mask = trues(nrow(df))
    mask = _filter_in!(mask, df, :year, years)
    mask = _filter_in!(mask, df, :m, m_values)
    mask = _filter_in!(mask, df, :scenario_name, scenario_names)
    mask = _filter_in!(mask, df, :imputer_backend, imputer_backends)
    mask = _filter_in!(mask, df, :linearizer_policy, linearizer_policies)
    mask = _filter_in!(mask, df, :b, b)
    mask = _filter_in!(mask, df, :r, r)
    mask = _filter_in!(mask, df, :k, k)
    return df[mask, :]
end

single_peaked_uniform_benchmark(m::Integer) = 2.0^(m - 1) / factorial(m)

function canonical_axis_string(axis_as_string::AbstractString)
    parts = strip.(split(axis_as_string, "<"))
    forward = join(parts, " < ")
    backward = join(reverse(parts), " < ")
    return forward <= backward ? forward : backward
end

realization_key_cols(df; include_axis=false) = begin
    cols = [col for col in SINGLE_PEAKEDNESS_REALIZATION_CANDIDATES if col ∈ propertynames(df)]
    include_axis && :axis_id ∈ propertynames(df) && push!(cols, :axis_id)
    cols
end

realization_cols(df) = realization_key_cols(df)

function _common_realization_key_cols(dfs::DataFrame...; include_axis=false)
    cols = Symbol[]
    for col in SINGLE_PEAKEDNESS_REALIZATION_CANDIDATES
        all(col ∈ propertynames(df) for df in dfs) && push!(cols, col)
    end
    include_axis && all(:axis_id ∈ propertynames(df) for df in dfs) && push!(cols, :axis_id)
    return cols
end

function _write_markdown(path::AbstractString, df::DataFrame)
    mkpath(dirname(path))
    open(path, "w") do io
        cols = propertynames(df)
        println(io, "| ", join(string.(cols), " | "), " |")
        println(io, "| ", join(fill("---", length(cols)), " | "), " |")
        for row in eachrow(df)
            println(io, "| ", join(string.(row[col] for col in cols), " | "), " |")
        end
    end
    return path
end

_latex_cell(x) = replace(string(x), "\\" => "\\textbackslash{}", "_" => "\\_", "%" => "\\%")

function _write_latex(path::AbstractString, df::DataFrame)
    mkpath(dirname(path))
    open(path, "w") do io
        cols = propertynames(df)
        println(io, "\\begin{tabular}{", repeat("l", length(cols)), "}")
        println(io, join(_latex_cell.(cols), " & "), " \\\\")
        println(io, "\\hline")
        for row in eachrow(df)
            println(io, join((_latex_cell(row[col]) for col in cols), " & "), " \\\\")
        end
        println(io, "\\end{tabular}")
    end
    return path
end

function _maybe_write_table(df::DataFrame; output_path=nothing, csv_path=nothing,
                            markdown_path=nothing, latex_path=nothing)
    csv_path === nothing && output_path !== nothing && (csv_path = output_path)
    if csv_path !== nothing
        mkpath(dirname(String(csv_path)))
        CSV.write(String(csv_path), df)
    end
    markdown_path !== nothing && _write_markdown(String(markdown_path), df)
    latex_path !== nothing && _write_latex(String(latex_path), df)
    return df
end

function _deduplicate_best_axis_ties(df::DataFrame)
    cols = realization_key_cols(df)
    isempty(cols) && (cols = [:year, :m])
    :axis_id ∈ propertynames(df) || return unique(df, cols)
    keep = combine(groupby(df, cols), :axis_id => minimum => :axis_id)
    chosen = semijoin(df, keep; on=unique(vcat(cols, [:axis_id])))
    return unique(chosen, cols)
end

function _best_rows(df::DataFrame, flag::Symbol, value::Symbol)
    require_columns(df, [:year, :m, :axis_id, flag, value]; context="axis_summary")
    sub = df[df[!, flag] .== true, :]
    sub = _deduplicate_best_axis_ties(sub)
    cols = realization_key_cols(sub)
    isempty(cols) && (cols = [:year, :m])
    return select(sub, unique(vcat(cols, [:axis_id, value])))
end

function _deduplicate_axis_ties(df::DataFrame)
    :axis_id ∈ propertynames(df) || return df
    cols = realization_key_cols(df)
    isempty(cols) && return df
    keep = combine(groupby(df, cols), :axis_id => minimum => :axis_id)
    return semijoin(df, keep; on=unique(vcat(cols, [:axis_id])))
end

function _best_l0_axes_from_axis_summary(axis_summary::DataFrame)
    require_columns(axis_summary, [:year, :m, :axis_id, :is_best_L0_axis]; context="axis_summary")
    best = axis_summary[axis_summary.is_best_L0_axis .== true, :]
    return _deduplicate_best_axis_ties(best)
end

function _best_l0_axes_from_best_axes(best_axes::DataFrame)
    require_columns(best_axes, [:year, :m, :axis_id, :measure]; context="best_axes")
    best = best_axes[string.(best_axes.measure) .== "L0", :]
    return _deduplicate_best_axis_ties(best)
end

function _select_best_l0_support(support_classification::DataFrame, best_axes_source::DataFrame;
                                 source=:axis_summary)
    support = support_classification
    best = source === :best_axes ? _best_l0_axes_from_best_axes(best_axes_source) :
           _best_l0_axes_from_axis_summary(best_axes_source)
    keycols = _common_realization_key_cols(support, best)
    isempty(keycols) && (keycols = [:year, :m])
    require_columns(support, unique(vcat(keycols, [:axis_id])); context="support_classification")
    best_key = unique(vcat(keycols, [:axis_id]))
    best = unique(select(best, best_key))
    return innerjoin(support, best; on=best_key)
end

function table_single_peakedness_main_values(axis_summary::DataFrame; output_path=nothing,
                                             csv_path=nothing, markdown_path=nothing,
                                             latex_path=nothing, kwargs...)
    df = filter_single_peakedness(axis_summary; kwargs...)
    l0 = _best_rows(df, :is_best_L0_axis, :L0)
    l1 = _best_rows(df, :is_best_L1_axis, :L1)
    g0 = combine(groupby(l0, [:year, :m]), :L0 => mean => :mean_L0, nrow => :n_realizations)
    g1 = combine(groupby(l1, [:year, :m]), :L1 => mean => :mean_L1)
    out = outerjoin(g0, g1; on=[:year, :m])
    out.sp_mass = 1 .- out.mean_L0
    out.raw_swaps = [binomial(Int(row.m), 2) * row.mean_L1 for row in eachrow(out)]
    out.uniform_benchmark = [single_peaked_uniform_benchmark(Int(m)) for m in out.m]
    out.ratio_to_uniform = out.sp_mass ./ out.uniform_benchmark
    select!(out, [:year, :m, :mean_L0, :sp_mass, :mean_L1, :raw_swaps,
                  :uniform_benchmark, :ratio_to_uniform, :n_realizations])
    sort!(out, [:year, :m])
    return _maybe_write_table(out; output_path, csv_path, markdown_path, latex_path)
end

function table_single_peakedness_distance_distribution(support_classification::DataFrame;
                                                       axis_ids=nothing,
                                                       output_path=nothing,
                                                       csv_path=nothing,
                                                       markdown_path=nothing,
                                                       latex_path=nothing,
                                                       kwargs...)
    df = filter_single_peakedness(support_classification; kwargs...)
    require_columns(df, [:year, :m, :proportion, :distance_to_SP_axis]; context="support_classification")
    axis_ids !== nothing && :axis_id ∈ propertynames(df) && (df = df[in.(df.axis_id, Ref(Set(axis_ids))), :])
    :is_best_L0_axis ∈ propertynames(df) && (df = df[df.is_best_L0_axis .== true, :])
    axis_ids === nothing && (df = _deduplicate_axis_ties(df))
    cols = realization_key_cols(df)
    isempty(cols) && (cols = [:year, :m])
    per_realization = combine(groupby(df, unique(vcat(cols, [:distance_to_SP_axis]))),
                              :proportion => sum => :realization_mass)
    out = combine(groupby(per_realization, [:year, :m, :distance_to_SP_axis]),
                  :realization_mass => mean => :mass)
    rename!(out, :distance_to_SP_axis => :distance)
    out.mass_percent = 100 .* out.mass
    sort!(out, [:year, :m, :distance])
    return _maybe_write_table(out; output_path, csv_path, markdown_path, latex_path)
end

function table_single_peakedness_distance_distribution(support_classification::DataFrame,
                                                       axis_summary::DataFrame;
                                                       output_path=nothing,
                                                       csv_path=nothing,
                                                       markdown_path=nothing,
                                                       latex_path=nothing,
                                                       best_axes_source=:axis_summary,
                                                       kwargs...)
    support = filter_single_peakedness(support_classification; kwargs...)
    axes = filter_single_peakedness(axis_summary; kwargs...)
    df = _select_best_l0_support(support, axes; source=best_axes_source)
    out = table_single_peakedness_distance_distribution(df; output_path=nothing, kwargs...)
    return _maybe_write_table(out; output_path, csv_path, markdown_path, latex_path)
end

function table_single_peakedness_distance_distribution(outputs::NamedTuple; kwargs...)
    if :axis_summary ∈ keys(outputs)
        return table_single_peakedness_distance_distribution(outputs.support_classification,
                                                            outputs.axis_summary; kwargs...)
    elseif :best_axes ∈ keys(outputs)
        return table_single_peakedness_distance_distribution(outputs.support_classification,
                                                            outputs.best_axes;
                                                            best_axes_source=:best_axes,
                                                            kwargs...)
    end
    throw(ArgumentError("outputs must contain axis_summary or best_axes for distance distribution"))
end

function table_single_peakedness_modal_axes(best_axes::DataFrame; output_path=nothing,
                                            csv_path=nothing, markdown_path=nothing,
                                            latex_path=nothing, kwargs...)
    df = filter_single_peakedness(best_axes; kwargs...)
    require_columns(df, [:year, :m, :measure, :axis_as_string]; context="best_axes")
    df = transform(df, :axis_as_string => ByRow(canonical_axis_string) => :canonical_axis)
    grouped = combine(groupby(df, [:year, :m, :measure, :canonical_axis]), nrow => :frequency)
    den = combine(groupby(df, [:year, :m, :measure]), nrow => :denominator)
    grouped = leftjoin(grouped, den; on=[:year, :m, :measure])
    grouped.share = grouped.frequency ./ grouped.denominator
    sort!(grouped, [:year, :m, :measure, order(:frequency, rev=true), :canonical_axis])
    out = combine(groupby(grouped, [:year, :m, :measure]), first)
    rename!(out, :canonical_axis => :modal_axis)
    select!(out, [:year, :m, :measure, :modal_axis, :frequency, :denominator, :share])
    return _maybe_write_table(out; output_path, csv_path, markdown_path, latex_path)
end

function _axis_gaps(df::DataFrame, value_col::Symbol, output_col::Symbol)
    cols = realization_cols(df)
    isempty(cols) && (cols = [:year, :m])
    rows = NamedTuple[]
    for sub in groupby(df, cols)
        vals = sort(Float64.(collect(skipmissing(sub[!, value_col]))))
        gap = length(vals) >= 2 ? vals[2] - vals[1] : missing
        push!(rows, merge((; (col => sub[1, col] for col in cols)...), (; output_col => gap)))
    end
    return DataFrame(rows)
end

function table_single_peakedness_axis_gaps(axis_summary::DataFrame; output_path=nothing,
                                           csv_path=nothing, markdown_path=nothing,
                                           latex_path=nothing, kwargs...)
    df = filter_single_peakedness(axis_summary; kwargs...)
    require_columns(df, [:year, :m, :L0, :L1]; context="axis_summary")
    l0 = _axis_gaps(df, :L0, :L0_gap)
    l1 = _axis_gaps(df, :L1, :L1_gap)
    joined = outerjoin(l0, l1; on=realization_cols(df))
    out = combine(groupby(joined, [:year, :m]),
                  :L0_gap => (x -> mean(skipmissing(x))) => :mean_L0_gap,
                  :L1_gap => (x -> mean(skipmissing(x))) => :mean_L1_gap,
                  nrow => :n_realizations)
    sort!(out, [:year, :m])
    return _maybe_write_table(out; output_path, csv_path, markdown_path, latex_path)
end

function table_single_peakedness_pipeline_variation(axis_summary::DataFrame; output_path=nothing,
                                                    csv_path=nothing, markdown_path=nothing,
                                                    latex_path=nothing, kwargs...)
    df = filter_single_peakedness(axis_summary; kwargs...)
    l0 = _best_rows(df, :is_best_L0_axis, :L0)
    l1 = _best_rows(df, :is_best_L1_axis, :L1)
    oncols = realization_key_cols(df)
    l0 = select(l0, unique(vcat(oncols, [:L0])))
    l1 = select(l1, unique(vcat(oncols, [:L1])))
    joined = outerjoin(l0, l1; on=oncols)
    require_columns(joined, [:imputer_backend, :linearizer_policy, :m, :L0, :L1]; context="axis_summary")
    out = combine(groupby(joined, [:imputer_backend, :linearizer_policy, :m]),
                  :L0 => (x -> mean(1 .- x)) => :mean_sp_mass,
                  :L0 => mean => :mean_L0,
                  :L1 => mean => :mean_L1,
                  nrow => :n_realizations)
    sort!(out, [:m, :imputer_backend, :linearizer_policy])
    return _maybe_write_table(out; output_path, csv_path, markdown_path, latex_path)
end

humanize_pipeline_label(imputer_backend, linearizer_policy) =
    string(imputer_backend, " + ", replace(string(linearizer_policy), "_" => " "))

function _weighted_mean_bool(y, w)
    weights = Float64.(w)
    total = sum(skipmissing(weights))
    total == 0 && return missing
    return sum((Bool(v) ? 1.0 : 0.0) * Float64(wi) for (v, wi) in zip(y, weights) if !ismissing(v) && !ismissing(wi)) / total
end

function _share_bool(y)
    vals = collect(skipmissing(y))
    isempty(vals) && return missing
    return mean(Bool.(vals))
end

function _covariate_variables(row_classification::DataFrame, variables, year)
    if variables isa AbstractDict
        vals = get(variables, year, get(variables, string(year), Symbol[]))
        return Symbol.(vals)
    end
    return Symbol.(variables)
end

function _row_classification_unique_rows(df::AbstractDataFrame)
    if :axis_id ∈ propertynames(df)
        id_cols = [col for col in [:profile_row_id, :respondent_id, :unique_ranking_id] if col ∈ propertynames(df)]
        if !isempty(id_cols)
            keep_cols = unique(vcat(realization_key_cols(df), id_cols))
            return unique(df, keep_cols)
        end
        return _deduplicate_axis_ties(df)
    end
    return df
end

function _share_table_for_group(df::AbstractDataFrame, variable::Symbol, weight_sym, aggregation::Symbol)
    keycols = realization_key_cols(df)
    if aggregation == :pooled_rows || isempty(keycols)
        baseline = weight_sym === nothing ? _share_bool(df.is_single_peaked) :
                   _weighted_mean_bool(df.is_single_peaked, df[!, weight_sym])
        rows = NamedTuple[]
        for sub in groupby(df, variable)
            share = weight_sym === nothing ? _share_bool(sub.is_single_peaked) :
                    _weighted_mean_bool(sub.is_single_peaked, sub[!, weight_sym])
            push!(rows, (
                category = string(sub[1, variable]),
                sp_share_percent = 100 * share,
                baseline_percent = 100 * baseline,
                delta_pp = 100 * (share - baseline),
                rows = nrow(sub),
            ))
        end
        return rows
    elseif aggregation == :realization_mean
        baseline_by_realization = combine(groupby(df, keycols),
            :is_single_peaked => _share_bool => :baseline_share,
            nrow => :realization_rows)
        shares = combine(groupby(df, unique(vcat(keycols, [variable]))),
            :is_single_peaked => _share_bool => :category_share,
            nrow => :category_rows)
        shares.category = string.(shares[!, variable])
        den = combine(groupby(shares, :category), :category_rows => sum => :rows)
        out = leftjoin(
            combine(groupby(shares, :category),
                :category_share => (x -> 100 * mean(skipmissing(x))) => :sp_share_percent),
            den; on=:category,
        )
        baseline = 100 * mean(skipmissing(baseline_by_realization.baseline_share))
        out.baseline_percent = fill(baseline, nrow(out))
        out.delta_pp = out.sp_share_percent .- out.baseline_percent
        return [NamedTuple(row) for row in eachrow(out)]
    end
    throw(ArgumentError("aggregation must be :pooled_rows or :realization_mean"))
end

function table_single_peakedness_covariates(row_classification::DataFrame, variables; weight_col=nothing,
                                            aggregation=:pooled_rows,
                                            output_path=nothing, csv_path=nothing,
                                            markdown_path=nothing, latex_path=nothing,
                                            kwargs...)
    df = filter_single_peakedness(row_classification; kwargs...)
    require_columns(df, [:year, :m, :is_single_peaked]; context="row_classification")
    df = _row_classification_unique_rows(df)
    weight_sym = weight_col === nothing ? nothing : Symbol(weight_col)
    rows = NamedTuple[]
    for ym in groupby(df, [:year, :m])
        for variable in _covariate_variables(df, variables, ym.year[1])
            variable ∈ propertynames(ym) || continue
            for row in _share_table_for_group(ym, variable, weight_sym, Symbol(aggregation))
                push!(rows, (
                    year = ym.year[1],
                    m = ym.m[1],
                    variable = String(variable),
                    category = row.category,
                    sp_share_percent = row.sp_share_percent,
                    baseline_percent = row.baseline_percent,
                    delta_pp = row.delta_pp,
                    rows = row.rows,
                ))
            end
        end
    end
    out = DataFrame(rows)
    !isempty(out) && sort!(out, [:year, :m, :variable, :category])
    return _maybe_write_table(out; output_path, csv_path, markdown_path, latex_path)
end

function _apply_single_peakedness_style!(; dpi=150)
    rc = PythonPlot.matplotlib.rcParams
    rc["font.family"] = "serif"
    rc["axes.titlesize"] = "large"
    rc["axes.labelsize"] = "large"
    rc["xtick.labelsize"] = 10
    rc["ytick.labelsize"] = 10
    rc["legend.fontsize"] = 10
    rc["figure.dpi"] = dpi
    rc["savefig.dpi"] = dpi
    rc["figure.facecolor"] = "white"
    rc["axes.facecolor"] = "white"
    rc["axes.grid"] = true
    rc["grid.alpha"] = 0.3
    return nothing
end

function _format_percent_axis!(ax)
    ax.yaxis.set_major_formatter(PythonPlot.matplotlib.ticker.PercentFormatter(1.0))
    return ax
end

_percent_axis!(ax) = _format_percent_axis!(ax)

function _set_integer_xticks!(ax, values)
    vals = sort(unique(Int.(values)))
    ax.set_xticks(vals)
    ax.set_xticklabels(string.(vals))
    return ax
end

function _finalize_figure!(fig; output_path=nothing, dpi=150, bottom_legend=false)
    if output_path !== nothing
        path = String(output_path)
        mkpath(dirname(path))
        bottom_legend || fig.tight_layout()
        if bottom_legend
            fig.savefig(path, dpi=dpi, bbox_inches="tight")
        else
            fig.savefig(path, dpi=dpi)
        end
        PythonPlot.pyplot.close(fig)
    end
    return fig
end

function _line_by_year(table::DataFrame, xcol::Symbol, ycol::Symbol; ylabel, xlabel, title,
                       figsize=(8.5, 5.0), dpi=150, style=true,
                       benchmark=false, reference_one=false,
                       legend_loc="upper left")
    style && _apply_single_peakedness_style!(; dpi)
    fig, ax = subplots(figsize=figsize, dpi=dpi)
    for year in sort(unique(table.year))
        sub = sort(table[table.year .== year, :], xcol)
        ax.plot(sub[!, xcol], sub[!, ycol], marker="o", label=string(year))
    end
    if benchmark
        ms = sort(unique(Int.(table[!, xcol])))
        ax.plot(ms, [single_peaked_uniform_benchmark(m) for m in ms],
                linestyle="--", color="red", marker="s", label="Uniform-domain benchmark")
    end
    reference_one && ax.axhline(1.0, linestyle="--", color="black", linewidth=1)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    _set_integer_xticks!(ax, table[!, xcol])
    ax.legend(loc=legend_loc)
    return fig, ax, table
end

function plot_sp_mass_by_m_year(axis_summary::DataFrame; output_path=nothing,
                                figsize=(8.5, 5.0),
                                title="Exact single-peaked mass declines with candidate-set size",
                                dpi=150, style=true, kwargs...)
    table = table_single_peakedness_main_values(axis_summary; kwargs...)
    fig, ax, out = _line_by_year(table, :m, :sp_mass;
        ylabel="Single-peaked mass on best axis (1 - L0)",
        xlabel="Candidate-set size m",
        title, figsize, dpi, style, benchmark=true, legend_loc="upper right")
    _format_percent_axis!(ax)
    _finalize_figure!(fig; output_path, dpi)
    return fig, ax, out
end

function plot_ratio_uniform_by_m_year(axis_summary::DataFrame; output_path=nothing,
                                      figsize=(8.5, 5.0),
                                      title="Best-axis fit is increasingly above the uniform baseline",
                                      dpi=150, style=true, kwargs...)
    table = table_single_peakedness_main_values(axis_summary; kwargs...)
    fig, ax, out = _line_by_year(table, :m, :ratio_to_uniform;
        ylabel="Best-axis SP mass / uniform benchmark",
        xlabel="Candidate-set size m",
        title, figsize, dpi, style, reference_one=true, legend_loc="upper left")
    _finalize_figure!(fig; output_path, dpi)
    return fig, ax, out
end

function plot_l1_by_m_year(axis_summary::DataFrame; output_path=nothing,
                           figsize=(8.5, 5.0),
                           title="Average repair cost remains moderate despite high exact failure",
                           dpi=150, style=true, kwargs...)
    table = table_single_peakedness_main_values(axis_summary; kwargs...)
    fig, ax, out = _line_by_year(table, :m, :mean_L1;
        ylabel="Unconditional normalized Kendall repair cost (L1)",
        xlabel="Candidate-set size m",
        title, figsize, dpi, style, legend_loc="upper left")
    _finalize_figure!(fig; output_path, dpi)
    return fig, ax, out
end

function _distance_label(d, bins)
    bins === :tail || return string(Int(round(d)))
    return d >= 4 ? "4+" : string(Int(round(d)))
end

_subplot_axis(axes, i::Integer) = axes[0, i - 1]

function plot_distance_distribution(table_or_support; axis_summary=nothing, output_path=nothing,
                                    distance_bins=nothing, figsize=(10, 5),
                                    title="Distance to the best L0 single-peaked domain",
                                    dpi=150, style=true, kwargs...)
    table = if table_or_support isa NamedTuple
        table_single_peakedness_distance_distribution(table_or_support; kwargs...)
    elseif axis_summary !== nothing
        table_single_peakedness_distance_distribution(table_or_support, axis_summary; kwargs...)
    else
        table_single_peakedness_distance_distribution(table_or_support; kwargs...)
    end
    table.distance_label = [_distance_label(d, distance_bins) for d in table.distance]
    plot_table = combine(groupby(table, [:year, :m, :distance_label]), :mass => sum => :mass)
    years = sort(unique(plot_table.year))
    style && _apply_single_peakedness_style!(; dpi)
    fig, axes = subplots(1, length(years), figsize=figsize, dpi=dpi, squeeze=false, sharey=true)
    labels = sort(unique(plot_table.distance_label); by=x -> parse(Int, replace(x, "+" => "")))
    fig.suptitle(title)
    for (i, year) in enumerate(years)
        ax = _subplot_axis(axes, i)
        sub = plot_table[plot_table.year .== year, :]
        ms = sort(unique(sub.m))
        bottoms = zeros(length(ms))
        xpos = collect(0:(length(ms)-1))
        for label in labels
            vals = [sum(sub[(sub.m .== m) .& (sub.distance_label .== label), :mass]) for m in ms]
            ax.bar(xpos, vals, bottom=bottoms, label=label)
            bottoms .+= vals
        end
        ax.set_xticks(xpos, string.(ms))
        ax.set_xlabel("m")
        ax.set_title(string(year))
        _format_percent_axis!(ax)
        i == 1 && ax.set_ylabel("Profile mass")
    end
    handles, legend_labels = _subplot_axis(axes, length(years)).get_legend_handles_labels()
    fig.legend(handles, legend_labels, title="Distance", loc="lower center",
               bbox_to_anchor=(0.5, -0.02), ncol=max(1, length(labels)))
    fig.subplots_adjust(bottom=0.24, top=0.82, wspace=0.12)
    _finalize_figure!(fig; output_path, dpi, bottom_legend=true)
    return fig, axes, table
end

function plot_distance_distribution(support_classification::DataFrame, axis_summary::DataFrame; kwargs...)
    return plot_distance_distribution(support_classification; axis_summary, kwargs...)
end

function plot_pipeline_effects(axis_summary::DataFrame; output_path=nothing,
                               figsize=(9, 5),
                               title="Linearization and imputation affect the level, not the main pattern",
                               dpi=150, style=true, kwargs...)
    table = table_single_peakedness_pipeline_variation(axis_summary; kwargs...)
    table.pipeline = [humanize_pipeline_label(row.imputer_backend, row.linearizer_policy) for row in eachrow(table)]
    ms = sort(unique(table.m))
    preferred = ["mice + pattern conditional", "mice + random ties",
                 "random + pattern conditional", "random + random ties"]
    groups = [g for g in preferred if g in Set(table.pipeline)]
    append!(groups, sort(setdiff(unique(table.pipeline), groups)))
    width = 0.8 / max(1, length(groups))
    style && _apply_single_peakedness_style!(; dpi)
    fig, ax = subplots(figsize=figsize, dpi=dpi)
    base = collect(0:(length(ms)-1))
    for (j, group) in enumerate(groups)
        vals = [sum(table[(table.m .== m) .& (table.pipeline .== group), :mean_sp_mass]) for m in ms]
        ax.bar(base .+ (j - 1) * width, vals, width * 0.95, label=group)
    end
    ax.set_xticks(base .+ width * (length(groups) - 1) / 2, string.(ms))
    ax.set_xlabel("Candidate-set size m")
    ax.set_ylabel("Mean exact single-peaked mass")
    ax.set_title(title)
    _format_percent_axis!(ax)
    ax.legend(loc="upper right")
    _finalize_figure!(fig; output_path, dpi)
    return fig, ax, table
end

function plot_ideology_by_year_m(row_classification::DataFrame; output_path=nothing,
                                 ideology_col=:Ideology,
                                 aggregation=:pooled_rows,
                                 figsize=(11, 5),
                                 title="Ideology and exact single-peakedness across m",
                                 dpi=150, style=true, kwargs...)
    table = table_single_peakedness_covariates(row_classification, [ideology_col];
                                              aggregation, kwargs...)
    years = sort(unique(table.year))
    style && _apply_single_peakedness_style!(; dpi)
    fig, axes = subplots(1, length(years), figsize=figsize, dpi=dpi, squeeze=false, sharey=true)
    fig.suptitle(title)
    for (i, year) in enumerate(years)
        ax = _subplot_axis(axes, i)
        sub = table[table.year .== year, :]
        for m in sort(unique(sub.m))
            sm = sort(sub[sub.m .== m, :], :category)
            ax.plot(sm.category, sm.sp_share_percent ./ 100, marker="o", label="m=$(m)")
        end
        ax.set_title(string(year))
        ax.set_xlabel(ideology_col == :Ideology ? "Ideology code" : String(ideology_col))
        _format_percent_axis!(ax)
        i == 1 && ax.set_ylabel("Single-peaked share")
        ax.tick_params(axis="x", rotation=0)
    end
    _subplot_axis(axes, length(years)).legend(loc="lower left")
    fig.subplots_adjust(top=0.82, wspace=0.12)
    _finalize_figure!(fig; output_path, dpi)
    return fig, axes, table
end

function plot_covariate_exact_fit(row_classification::DataFrame; year, m, variable,
                                  output_path=nothing, title=nothing,
                                  category_order=nothing, category_labels=nothing,
                                  aggregation=:pooled_rows,
                                  figsize=(8.5, 5), dpi=150, style=true,
                                  kwargs...)
    var = Symbol(variable)
    table = table_single_peakedness_covariates(row_classification, [var];
                                               years=[year], m_values=[m],
                                               aggregation, kwargs...)
    isempty(table) && throw(ArgumentError("no row-classification data found for year=$(year), m=$(m), variable=$(variable)"))
    if category_order !== nothing
        ordermap = Dict(string(v) => i for (i, v) in enumerate(category_order))
        sort!(table, [:category]; by=x -> get(ordermap, string(x), typemax(Int)))
    else
        sort!(table, :category)
    end
    labels = string.(table.category)
    if category_labels !== nothing
        labelmap = Dict(string(k) => string(v) for (k, v) in pairs(category_labels))
            labels = [get(labelmap, label, label) for label in labels]
    end
    style && _apply_single_peakedness_style!(; dpi)
    fig, ax = subplots(figsize=figsize, dpi=dpi)
    ax.bar(labels, table.sp_share_percent ./ 100)
    ax.axhline(first(table.baseline_percent) / 100, linestyle="--", color="black",
               linewidth=1, label="Year-m baseline")
    ax.set_ylabel("Single-peaked share")
    ax.set_xlabel(String(var))
    title !== nothing && ax.set_title(title)
    ax.legend(loc="upper right")
    ax.tick_params(axis="x", rotation=25)
    _format_percent_axis!(ax)
    _finalize_figure!(fig; output_path, dpi)
    return fig, ax, table
end
