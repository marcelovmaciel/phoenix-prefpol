module PreferenceGraphPlots

using DataFrames
using Preferences
using Printf
using PythonPlot
using Statistics

export plot_plurality_scores, plot_pairwise_margins, plot_shell_masses
export plot_edge_overlap_heatmap, plot_support_matrix, plot_type_anchoring
export plot_type_breakers, plot_group_contributions
export plot_candidate_position_by_current_first, plot_plurality_swing_values

function _savefig(fig, output_path)
    path = String(output_path)
    mkpath(dirname(path))
    fig.tight_layout()
    fig.savefig(path)
    PythonPlot.pyplot.close(fig)
    return fig
end

_edge_label(w, l) = string(w, ">", l)

function _plurality_table(table_or_profile)
    if table_or_profile isa DataFrame
        return table_or_profile
    end
    return Preferences.plurality_scores_table(table_or_profile)
end

function _edges_table(table_or_result)
    if table_or_result isa DataFrame
        return table_or_result
    end
    return Preferences.majority_edges_table(table_or_result)
end

function _column_or_error(df::DataFrame, name::Symbol)
    name in propertynames(df) || throw(ArgumentError("table must contain column :$name"))
    return df[!, name]
end

function plot_plurality_scores(table_or_profile; output_path, title="Plurality scores")
    table = _plurality_table(table_or_profile)
    labels = string.(_column_or_error(table, :candidate))
    values = :first_place_share in propertynames(table) ? table.first_place_share : table.first_place_count
    ylabel = :first_place_share in propertynames(table) ? "First-place share" : "First-place count"

    fig, ax = subplots(figsize=(7, 4))
    ax.bar(labels, values)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.tick_params(axis="x", rotation=25)
    return _savefig(fig, output_path)
end

function plot_pairwise_margins(edges_table_or_result; output_path, title="Pairwise majority margins")
    table = _edges_table(edges_table_or_result)
    labels = if :edge in propertynames(table)
        string.(table.edge)
    else
        [_edge_label(row.winner, row.loser) for row in eachrow(table)]
    end
    values = :normalized_margin in propertynames(table) ? table.normalized_margin : table.margin_mass
    ylabel = :normalized_margin in propertynames(table) ? "Normalized margin" : "Margin mass"

    fig, ax = subplots(figsize=(8, 4))
    ax.bar(labels, values)
    !isempty(values) && ax.axhline(mean(skipmissing(values)), linestyle="--", linewidth=1, color="black")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.tick_params(axis="x", rotation=25)
    return _savefig(fig, output_path)
end

function plot_shell_masses(voter_type_table; output_path, title="Mass by Kendall shell")
    :shell in propertynames(voter_type_table) || throw(ArgumentError("voter_type_table must contain a :shell column"))
    mass_col = :proportion in propertynames(voter_type_table) ? :proportion : :mass
    shell_df = combine(groupby(voter_type_table, :shell), mass_col => sum => :mass)
    sort!(shell_df, :shell)

    fig, ax = subplots(figsize=(7, 4))
    ax.bar(string.(shell_df.shell), shell_df.mass)
    ax.set_xlabel("Kendall shell")
    ax.set_ylabel(String(mass_col))
    ax.set_title(title)
    return _savefig(fig, output_path)
end

function plot_edge_overlap_heatmap(edge_overlap_table; output_path, value=:jaccard)
    value in propertynames(edge_overlap_table) || throw(ArgumentError("edge_overlap_table must contain column :$value"))
    labels = unique(vcat(edge_overlap_table.edge_i_label, edge_overlap_table.edge_j_label))
    idx = Dict(label => i for (i, label) in enumerate(labels))
    mat = fill(NaN, length(labels), length(labels))
    for row in eachrow(edge_overlap_table)
        mat[idx[row.edge_i_label], idx[row.edge_j_label]] = Float64(row[value])
    end

    fig, ax = subplots(figsize=(6, 5))
    im = ax.imshow(mat, aspect="auto")
    fig.colorbar(im, ax=ax, label=String(value))
    ax.set_xticks(0:(length(labels)-1), labels, rotation=35, ha="right")
    ax.set_yticks(0:(length(labels)-1), labels)
    ax.set_title("Edge overlap")
    return _savefig(fig, output_path)
end

function plot_support_matrix(edge_support_table; output_path)
    types = sort(unique(edge_support_table.type_index))
    edges = sort(unique(edge_support_table.edge_index))
    type_idx = Dict(t => i for (i, t) in enumerate(types))
    edge_idx = Dict(e => i for (i, e) in enumerate(edges))
    mat = zeros(Float64, length(types), length(edges))
    labels = fill("", length(edges))
    for row in eachrow(edge_support_table)
        mat[type_idx[row.type_index], edge_idx[row.edge_index]] = row.supports ? 1.0 : 0.0
        labels[edge_idx[row.edge_index]] = _edge_label(row.winner, row.loser)
    end

    fig, ax = subplots(figsize=(max(6, 0.6 * length(edges)), max(5, 0.18 * length(types))))
    im = ax.imshow(mat, aspect="auto", interpolation="nearest")
    fig.colorbar(im, ax=ax, label="supports edge")
    ax.set_xlabel("Majority edge")
    ax.set_ylabel("Voter type index")
    ax.set_xticks(0:(length(edges)-1), labels, rotation=35, ha="right")
    ax.set_title("Support matrix")
    return _savefig(fig, output_path)
end

function plot_type_anchoring(voter_type_table; output_path, top_n=24)
    col = :anchoring in propertynames(voter_type_table) ? :anchoring : :normalized_anchoring
    df = sort(voter_type_table, col, rev=true)[1:min(top_n, nrow(voter_type_table)), :]
    labels = string.(df.type_index, ": ", df.ranking)

    fig, ax = subplots(figsize=(8, max(4, 0.28 * nrow(df))))
    ax.barh(labels, df[!, col])
    ax.invert_yaxis()
    ax.set_xlabel(String(col))
    ax.set_title("Type anchoring")
    return _savefig(fig, output_path)
end

function plot_type_breakers(type_breaker_table; output_path, edge=nothing, top_n=10)
    df = type_breaker_table
    if edge !== nothing
        edge_string = String(edge)
        df = filter(row -> _edge_label(row.winner, row.loser) == edge_string || string(row.edge_index) == edge_string, df)
    end
    isempty(df) && throw(ArgumentError("no type breaker rows to plot"))
    df = sort(df, :breaking_score, rev=true)[1:min(top_n, nrow(df)), :]
    labels = string.(_edge_label.(df.winner, df.loser), " | ", df.type_index, ": ", df.ranking)

    fig, ax = subplots(figsize=(9, max(4, 0.35 * nrow(df))))
    ax.barh(labels, df.breaking_score)
    ax.invert_yaxis()
    ax.set_xlabel("Breaking score")
    ax.set_title("Type breakers")
    return _savefig(fig, output_path)
end

function plot_group_contributions(group_edge_power_table; output_path, value=:group_margin_contribution)
    value in propertynames(group_edge_power_table) || throw(ArgumentError("group_edge_power_table must contain column :$value"))
    groups = unique(group_edge_power_table.group)
    edges = unique(group_edge_power_table.edge_index)
    gidx = Dict(g => i for (i, g) in enumerate(groups))
    eidx = Dict(e => i for (i, e) in enumerate(edges))
    mat = zeros(Float64, length(groups), length(edges))
    labels = fill("", length(edges))
    for row in eachrow(group_edge_power_table)
        mat[gidx[row.group], eidx[row.edge_index]] = Float64(row[value])
        labels[eidx[row.edge_index]] = _edge_label(row.winner, row.loser)
    end

    fig, ax = subplots(figsize=(8, max(3, 0.35 * length(groups) + 1.5)))
    im = ax.imshow(mat, aspect="auto")
    fig.colorbar(im, ax=ax, label=String(value))
    ax.set_yticks(0:(length(groups)-1), string.(groups))
    ax.set_xticks(0:(length(edges)-1), labels, rotation=35, ha="right")
    ax.set_title("Group contributions")
    return _savefig(fig, output_path)
end

function plot_candidate_position_by_current_first(position_table; output_path, target_label=nothing)
    firsts = unique(position_table.current_first)
    positions = sort(unique(position_table.target_position))
    fidx = Dict(f => i for (i, f) in enumerate(firsts))
    width = 0.8 / max(1, length(positions))
    fig, ax = subplots(figsize=(8, 4))
    base = collect(0:(length(firsts)-1))
    for (j, pos) in enumerate(positions)
        values = zeros(Float64, length(firsts))
        for row in eachrow(filter(:target_position => ==(pos), position_table))
            values[fidx[row.current_first]] = Float64(row.mass)
        end
        ax.bar(base .+ (j - 1) * width, values, width, label="position $pos")
    end
    ax.set_xticks(base .+ width * (length(positions) - 1) / 2, string.(firsts))
    ax.set_ylabel("Mass")
    title = target_label === nothing ? "Candidate position by current first" : "$(target_label) position by current first"
    ax.set_title(title)
    ax.legend()
    return _savefig(fig, output_path)
end

function plot_plurality_swing_values(swing_table; output_path)
    fig, ax = subplots(figsize=(7, 4))
    ax.bar(string.(swing_table.current_first), swing_table.plurality_swing_value)
    ax.set_xlabel("Current first")
    ax.set_ylabel("Plurality swing value")
    ax.set_title("Plurality swing values")
    return _savefig(fig, output_path)
end

end
