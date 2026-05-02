#!/usr/bin/env julia

const SCRIPT_DIR = @__DIR__
const REPO_ROOT = normpath(joinpath(SCRIPT_DIR, "..", ".."))
push!(LOAD_PATH, joinpath(REPO_ROOT, "Preferences"))

using CSV
using DataFrames
using Dates
using Preferences
using Printf
using Statistics
using TOML

# PythonPlot uses CondaPkg, which inspects active environments on LOAD_PATH.
# Keep the old PrefPol Manifest out of that resolver and load the plotting
# package through its own local environment.
empty!(LOAD_PATH)
push!(LOAD_PATH, joinpath(REPO_ROOT, "PreferenceGraphPlots"))
push!(LOAD_PATH, joinpath(REPO_ROOT, "Preferences"))
push!(LOAD_PATH, "@stdlib")
using PreferenceGraphPlots

const CANDIDATES = [
    (column=:rank_LULA, label=:Lu, display="Lula"),
    (column=:rank_BOLSONARO, label=:Bo, display="Bolsonaro"),
    (column=:rank_CIRO_GOMES, label=:Ci, display="Ciro Gomes"),
    (column=:rank_SIMONE_TEBET, label=:Te, display="Simone Tebet"),
]

const REFERENCE_ORDER = [:Lu, :Te, :Ci, :Bo]
const TARGET = :Lu
const OPPONENT = :Bo
const CURRENT_FIRST_CANDIDATES = [:Te, :Ci, :Bo]
const PARTITIONS = [:Sex, :Religion, :Race, :Ideology, :PT, :Abortion, :Age, :Education, :Income]

const DEFAULT_INPUT = joinpath(SCRIPT_DIR, "input", "m04_mice_pattern_conditional_2022_augmented_linearized_profile.csv")
const BUNDLE_INPUT = joinpath(REPO_ROOT, "majority_graph_support_2022_report_bundle",
                              "majority_graph_support_report", "input",
                              "m04_mice_pattern_conditional_2022_augmented_linearized_profile.csv")
const DEFAULT_OUTPUT = joinpath(SCRIPT_DIR, "output", "majority_graph_support_2022")

function parse_args(args)
    opts = Dict{String,String}()
    i = 1
    while i <= length(args)
        arg = args[i]
        if arg in ("--input", "-i")
            i += 1
            i <= length(args) || error("--input requires a path")
            opts["input"] = args[i]
        elseif arg in ("--output", "-o")
            i += 1
            i <= length(args) || error("--output requires a path")
            opts["output"] = args[i]
        elseif arg == "--no-validate-known"
            opts["validate_known"] = "false"
        else
            error("Unknown argument: $arg")
        end
        i += 1
    end
    input = get(opts, "input", isfile(DEFAULT_INPUT) ? DEFAULT_INPUT : BUNDLE_INPUT)
    output = get(opts, "output", DEFAULT_OUTPUT)
    validate_known = get(opts, "validate_known", "true") == "true"
    return (; input, output, validate_known)
end

function build_profile(df::DataFrame)
    pool = CandidatePool([c.label for c in CANDIDATES])
    ballots = StrictRank[]
    for row in eachrow(df)
        ranked = Vector{Tuple{Int,Symbol}}()
        for c in CANDIDATES
            push!(ranked, (Int(row[c.column]), c.label))
        end
        sort!(ranked; by=first)
        push!(ballots, StrictRank(pool, [label for (_, label) in ranked]))
    end
    return Profile(pool, ballots)
end

function write_table(path, df::DataFrame)
    mkpath(dirname(path))
    CSV.write(path, df)
end

function write_plot_pair(f, basename; kwargs...)
    f(; output_path=basename * ".png", kwargs...)
    f(; output_path=basename * ".pdf", kwargs...)
end

function safe_partition_labels(df::DataFrame, partition::Symbol)
    return [ismissing(x) ? "NA" : string(x) for x in df[!, partition]]
end

function add_edge_label!(df::DataFrame)
    if (:winner in propertynames(df)) && (:loser in propertynames(df))
        df.edge = string.(df.winner, ">", df.loser)
    end
    return df
end

function psi_from_edges(edges::DataFrame)
    return 1.0 - mean(edges.normalized_margin)
end

function table_total_row(swing::DataFrame)
    baseline = nrow(swing) == 0 ? 0.0 : swing.target_opponent_margin_before[1]
    total_swing = sum(swing.plurality_swing_value)
    return DataFrame(current_first_id=missing, current_first=:TOTAL,
                     one_swap_mass=sum(swing.one_swap_mass),
                     per_voter_swing=missing,
                     plurality_swing_value=total_swing,
                     target_opponent_margin_before=baseline,
                     target_opponent_margin_after_if_pool_switches=baseline + total_swing)
end

function assert_close(name, actual, expected; atol=1e-6)
    isapprox(Float64(actual), Float64(expected); atol=atol) || error("Validation failed for $name: got $actual, expected $expected")
end

function validate_known_values(profile, tables)
    nballots(profile) == 2001 || return false

    plurality = tables["plurality_scores"]
    expected_plurality = Dict(:Lu=>823.0, :Bo=>794.0, :Te=>241.0, :Ci=>143.0)
    for (candidate, expected) in expected_plurality
        actual = plurality[plurality.candidate .== candidate, :first_place_count][1]
        assert_close("plurality $candidate", actual, expected)
    end
    assert_close("plurality Lu-Bo margin", 823 - 794, 29)

    edges = tables["majority_edges"]
    lubo = edges[(edges.winner .== :Lu) .& (edges.loser .== :Bo), :]
    nrow(lubo) == 1 || error("Validation failed: Lu>Bo edge not found")
    assert_close("Lu>Bo support", lubo.support_mass[1], 1116)
    assert_close("Lu>Bo opposition", lubo.opposition_mass[1], 885)
    assert_close("Lu>Bo margin", lubo.margin_mass[1], 231)

    edge_pairs = Set(zip(edges.winner, edges.loser))
    for pair in [(:Lu, :Te), (:Lu, :Ci), (:Lu, :Bo), (:Te, :Ci), (:Te, :Bo), (:Ci, :Bo)]
        pair in edge_pairs || error("Validation failed: expected majority edge $(pair[1])>$(pair[2])")
    end
    assert_close("Psi", psi_from_edges(edges), 0.922; atol=0.001)

    one_swap = tables["one_swap_target"]
    expected_one_swap = Dict(:Te=>138.0, :Ci=>47.0, :Bo=>69.0)
    for (candidate, expected) in expected_one_swap
        actual = one_swap[one_swap.current_first .== candidate, :mass][1]
        assert_close("one-swap $candidate -> Lu", actual, expected)
    end

    swing = tables["plurality_swing_values"]
    expected_swing = Dict(:Te=>138.0, :Ci=>47.0, :Bo=>138.0)
    for (candidate, expected) in expected_swing
        actual = swing[swing.current_first .== candidate, :plurality_swing_value][1]
        assert_close("swing $candidate -> Lu", actual, expected)
    end
    assert_close("total swing", sum(swing.plurality_swing_value), 323)
    assert_close("counterfactual Lu-Bo margin", swing.target_opponent_margin_before[1] + sum(swing.plurality_swing_value), 352)
    return true
end

function latex_escape(x)
    s = string(x)
    replacements = [
        "\\" => "\\textbackslash{}", "_" => "\\_", "%" => "\\%", "&" => "\\&",
        "#" => "\\#", raw"$" => "\\\$", "{" => "\\{", "}" => "\\}",
        "~" => "\\textasciitilde{}", "^" => "\\textasciicircum{}",
    ]
    for (k, v) in replacements
        s = replace(s, k => v)
    end
    return s
end

latex_label(x) = replace(string(x), r"[^A-Za-z0-9:.-]" => "-")

function latex_value(x)
    if x === missing || (x isa AbstractFloat && isnan(x))
        return ""
    elseif x isa Integer
        return string(x)
    elseif x isa AbstractFloat
        return isapprox(x, round(x); atol=1e-8) ? string(Int(round(x))) : @sprintf("%.3f", x)
    else
        return latex_escape(x)
    end
end

function table_columns(df::DataFrame, specs)
    cols = Symbol[]
    heads = String[]
    for spec in specs
        col, head = spec
        col in propertynames(df) || continue
        push!(cols, col)
        push!(heads, head)
    end
    return cols, heads
end

function latex_table(df::DataFrame; caption="", label="", columns=nothing, headers=nothing,
                     digits=3, longtable=false, fontsize="scriptsize",
                     landscape=false, resize=false, maxrows=nrow(df))
    cols = columns === nothing ? propertynames(df) : Symbol.(columns)
    heads = headers === nothing ? string.(cols) : String.(headers)
    length(cols) == length(heads) || throw(ArgumentError("columns and headers must have the same length"))
    rows = first(df, min(maxrows, nrow(df)))
    lines = String[]
    landscape && push!(lines, "\\begin{landscape}")
    if longtable
        push!(lines, "{\\" * fontsize)
        push!(lines, "\\begin{longtable}{" * "l" ^ length(cols) * "}")
        !isempty(caption) && push!(lines, "\\caption{$(latex_escape(caption))}$(isempty(label) ? "" : "\\label{$(latex_label(label))}")\\\\")
    else
        push!(lines, "\\begin{table}[H]")
        push!(lines, "\\centering")
        push!(lines, "\\" * fontsize)
        resize && push!(lines, "\\resizebox{\\linewidth}{!}{%")
        push!(lines, "\\begin{tabular}{" * "l" ^ length(cols) * "}")
    end
    push!(lines, "\\toprule")
    push!(lines, join(latex_escape.(heads), " & ") * " \\\\")
    push!(lines, "\\midrule")
    for row in eachrow(rows)
        push!(lines, join((latex_value(row[c]) for c in cols), " & ") * " \\\\")
    end
    push!(lines, "\\bottomrule")
    if longtable
        push!(lines, "\\end{longtable}")
        push!(lines, "}")
    else
        push!(lines, "\\end{tabular}")
        resize && push!(lines, "}")
        !isempty(caption) && push!(lines, "\\caption{$(latex_escape(caption))}")
        !isempty(label) && push!(lines, "\\label{$(latex_label(label))}")
        nrow(df) > maxrows && push!(lines, "\\par\\smallskip Showing $(maxrows) of $(nrow(df)) rows.")
        push!(lines, "\\end{table}")
    end
    landscape && push!(lines, "\\end{landscape}")
    return join(lines, "\n")
end

function figure_tex(filename, caption; width="0.78\\linewidth", label="")
    return """
\\begin{figure}[H]
\\centering
\\includegraphics[width=$width]{../figures/$filename}
\\caption{$(latex_escape(caption))}
$(isempty(label) ? "" : "\\label{$(latex_label(label))}")
\\end{figure}
"""
end

edge_string(row) = string(row.winner, ">", row.loser)

function edge_rows(df::DataFrame, pairs::Vector{Tuple{Symbol,Symbol}})
    mask = falses(nrow(df))
    for (w, l) in pairs
        mask .|= (df.winner .== w) .& (df.loser .== l)
    end
    return df[mask, :]
end

function unordered_edge_overlap(overlap::DataFrame)
    df = filter(row -> row.edge_i < row.edge_j, overlap)
    return sort(df, :jaccard, rev=true)
end

function top_type_anchors(voter_types::DataFrame; n=12)
    return first(sort(voter_types, :anchoring, rev=true), min(n, nrow(voter_types)))
end

function group_table_sections(tables, group_partitions, stem; caption_prefix, selected=group_partitions)
    parts = String[]
    for partition in selected
        key = "$(stem)_$(partition)"
        haskey(tables, key) || continue
        table = tables[key]
        cols, heads = if startswith(stem, "group_edge_power")
            table_columns(table, [
                (:group, "group"), (:edge, "edge"), (:group_mass, "group share"),
                (:group_support, "support"), (:group_margin_contribution, "margin contrib."),
                (:group_support_share_within_edge, "edge support share"),
                (:raw_group_breaking_score, "raw breaker")
            ])
        elseif startswith(stem, "group_target_switch")
            table_columns(table, [
                (:group, "group"), (:current_first, "current first"),
                (:current_first_mass, "current first count"),
                (:target_second_mass, "Lu second"),
                (:target_second_share_within_current_first, "Lu second rate"),
                (:per_voter_swing, "per voter swing"),
                (:plurality_swing_value, "swing value"),
                (:group_share_of_pool, "pool share")
            ])
        else
            table_columns(table, [(c, string(c)) for c in propertynames(table)])
        end
        push!(parts, latex_table(table; caption="$(caption_prefix): $(partition)", label="tab:$(stem)_$(caption_prefix)_$(partition)",
                                 columns=cols, headers=heads, longtable=true, fontsize="scriptsize", landscape=true))
    end
    return join(parts, "\n\n")
end

function generate_report(report_path, output_dir, input_path, tables, group_partitions)
    mkpath(dirname(report_path))
    edges = tables["majority_edges"]
    plurality = tables["plurality_scores"]
    decomp = tables["pairwise_vs_plurality"]
    swing_with_total = vcat(tables["plurality_swing_values"], table_total_row(tables["plurality_swing_values"]); cols=:union)
    psi = psi_from_edges(edges)
    lu = plurality[plurality.candidate .== :Lu, :first_place_count][1]
    bo = plurality[plurality.candidate .== :Bo, :first_place_count][1]
    lubo = edges[(edges.winner .== :Lu) .& (edges.loser .== :Bo), :]
    lute = edges[(edges.winner .== :Lu) .& (edges.loser .== :Te), :]
    ci_surplus = decomp[decomp.current_first .== :Ci, :pairwise_contribution][1]
    te_surplus = decomp[decomp.current_first .== :Te, :pairwise_contribution][1]
    plurality_margin = lu - bo
    edge_overlap_pairs = unordered_edge_overlap(tables["edge_overlap"])
    anchor_top = top_type_anchors(tables["voter_types"]; n=12)
    breaker_lute = first(sort(edge_rows(tables["type_breakers"], [(:Lu, :Te)]), :breaking_score, rev=true), 8)
    breaker_lubo = first(sort(edge_rows(tables["type_breakers"], [(:Lu, :Bo)]), :breaking_score, rev=true), 8)
    breaker_teci = first(sort(edge_rows(tables["type_breakers"], [(:Te, :Ci)]), :breaking_score, rev=true), 8)
    breaker_selected = vcat(breaker_lute, breaker_lubo, breaker_teci)
    exact_switch = sort(tables["exact_type_switch"], [:current_first, :target_position, :type_index])
    main_group_partitions = [p for p in [:Ideology, :PT, :Sex, :Education, :Income] if p in group_partitions]
    switch_group_partitions = [p for p in [:Ideology, :PT, :Sex, :Religion, :Education, :Income, :Abortion] if p in group_partitions]

    plurality_cols, plurality_heads = table_columns(plurality, [
        (:candidate, "candidate"), (:first_place_count, "count"), (:first_place_share, "share")
    ])
    edge_cols, edge_heads = table_columns(edges, [
        (:edge, "edge"), (:winner, "winner"), (:loser, "loser"),
        (:support_mass, "support"), (:opposition_mass, "opposition"),
        (:margin_mass, "margin"), (:normalized_margin, "norm. margin"),
        (:integer_flip_count, "flip count")
    ])
    decomp_cols, decomp_heads = table_columns(decomp, [
        (:current_first, "current first"), (:target_over_opponent_mass, "Lu>Bo"),
        (:opponent_over_target_mass, "Bo>Lu"), (:pairwise_contribution, "pairwise contrib."),
        (:plurality_contribution, "plurality contrib.")
    ])
    voter_cols, voter_heads = table_columns(tables["voter_types"], [
        (:type_index, "type"), (:ranking, "ranking"), (:shell, "distance"),
        (:mass, "count"), (:proportion, "share"), (:coverage, "K"), (:anchoring, "anchoring")
    ])
    core_cols, core_heads = table_columns(tables["core"], [
        (:k, "k"), (:core_mass, "core share")
    ])
    overlap_cols, overlap_heads = table_columns(edge_overlap_pairs, [
        (:edge_i_label, "edge i"), (:edge_j_label, "edge j"), (:overlap, "overlap"),
        (:jaccard, "jaccard"), (:conditional_i_given_j, "Pr(i|j)"),
        (:conditional_j_given_i, "Pr(j|i)")
    ])
    breaker_cols, breaker_heads = table_columns(breaker_selected, [
        (:edge, "edge"), (:type_index, "type"), (:ranking, "ranking"),
        (:type_mass, "count"), (:type_proportion, "share"),
        (:boundary_distance, "distance"), (:amenability, "amenability"),
        (:raw_breaking_score, "raw score"), (:breaking_score, "breaker score")
    ])
    coalition_cols, coalition_heads = table_columns(tables["minimal_breaking_coalition"], [
        (:edge, "edge"), (:threshold, "threshold"), (:rank_in_coalition, "rank"),
        (:type_index, "type"), (:ranking, "ranking"), (:type_mass, "count"),
        (:type_proportion, "share"), (:boundary_distance, "distance"),
        (:cumulative_mass, "cumulative"), (:flips_edge, "flips edge")
    ])
    position_cols, position_heads = table_columns(tables["candidate_position_by_current_first"], [
        (:current_first, "current first"), (:target_position, "Lu position"),
        (:mass, "count"), (:share_within_current_first, "share within current first")
    ])
    one_swap_cols, one_swap_heads = table_columns(tables["one_swap_target"], [
        (:current_first, "current first"), (:mass, "Lu second"), (:share_within_current_first, "share")
    ])
    swing_cols, swing_heads = table_columns(swing_with_total, [
        (:current_first, "current first"), (:one_swap_mass, "Lu second"),
        (:per_voter_swing, "per voter swing"), (:plurality_swing_value, "swing value"),
        (:target_opponent_margin_after_if_pool_switches, "counterfactual margin")
    ])
    exact_cols, exact_heads = table_columns(exact_switch, [
        (:type_index, "type"), (:ranking, "ranking"), (:current_first, "current first"),
        (:target_position, "Lu position"), (:mass, "count"), (:share, "share")
    ])

    tex = """
\\documentclass[11pt]{article}
\\usepackage[margin=0.85in]{geometry}
\\usepackage{booktabs,longtable,array,graphicx,float,caption,pdflscape}
\\usepackage{amsmath,amssymb}
\\usepackage[T1]{fontenc}
\\usepackage[utf8]{inputenc}
\\usepackage{lmodern,microtype,hyperref}
\\hypersetup{colorlinks=true,linkcolor=black,urlcolor=black}
\\setlength{\\parindent}{0pt}
\\setlength{\\parskip}{0.6em}
\\begin{document}

\\title{Majority-Graph Support for the 2022 Linearized Profile}
\\author{Julia replication workflow}
\\date{$(latex_escape(Dates.format(now(), dateformat"yyyy-mm-dd HH:MM")))}
\\maketitle
\\tableofcontents
\\newpage

\\section{Introduction and scope}

This report is generated by Julia from the composable package workflow. It analyzes one linearized profile, not an uncertainty-pooled bootstrap, imputation, or linearization distribution. The input CSV was:
\\begin{quote}\\texttt{$(latex_escape(input_path))}\\end{quote}

The analysis uses candidates Lu = Lula, Bo = Bolsonaro, Ci = Ciro Gomes, and Te = Simone Tebet. The support basis is the full strict-ranking basis over these four candidates, ordered by Kendall shell around Lu > Te > Ci > Bo. The plurality-switch target is Lu and the opponent is Bo. Switch pools are restricted to current first choices Te, Ci, and Bo. The group partitions analyzed are: $(join(latex_escape.(group_partitions), ", ")).

\\section{Plurality and pairwise majority}

For pairwise margins, the polarization statistic is
\\[
\\Psi = 1 - \\frac{1}{\\binom{m}{2}}\\sum_{\\{a,b\\}}\\frac{|n_{ab}-n_{ba}|}{n},
\\]
so \\(\\Psi\\) is one minus the average normalized absolute pairwise margin. Here \\(\\Psi \\approx $(@sprintf("%.3f", psi))\\).

Lula has $(Int(round(lu))) first-place rankings and Bolsonaro has $(Int(round(bo))), so the plurality Lu-Bo margin is $(Int(round(plurality_margin))). In contrast, the pairwise Lu>Bo edge has support $(Int(round(lubo.support_mass[1]))), opposition $(Int(round(lubo.opposition_mass[1]))), and margin $(Int(round(lubo.margin_mass[1]))). The majority order is Lu > Te > Ci > Bo. Non-Lu/non-Bo first-choice voters explain the difference between the near plurality tie and the larger pairwise Lu>Bo margin.

$(latex_table(plurality; caption="Plurality scores", label="tab:plurality", columns=plurality_cols, headers=plurality_heads))

$(latex_table(edges; caption="Pairwise majority edge table", label="tab:edges", columns=edge_cols, headers=edge_heads, resize=true))

$(figure_tex("plurality_scores.png", "Plurality scores."; width="0.72\\linewidth"))

$(figure_tex("pairwise_margins.png", "Pairwise majority margins."; width="0.78\\linewidth"))

\\section{Why pairwise and plurality differ}

The decomposition below groups the target-opponent comparison by current first choice. The identity is
\\[
231 = 29 + $(Int(round(ci_surplus))) + $(Int(round(te_surplus))),
\\]
or equivalently pairwise Lu>Bo margin = plurality Lu-Bo margin + Ci-first surplus + Te-first surplus. This is the central reason pairwise and plurality diagnose different structures.

$(latex_table(decomp; caption="Pairwise-vs-plurality decomposition by current first choice", label="tab:decomp", columns=decomp_cols, headers=decomp_heads, resize=true))

\\section{Voter-type basis and majority-graph support}

The basis is the full \\(4!\\) strict-ranking basis, ordered by Kendall shell around Lu > Te > Ci > Bo. Zero-mass types are retained and type indices are stable.

$(latex_table(tables["voter_types"]; caption="Full voter-type basis", label="tab:voter-types", columns=voter_cols, headers=voter_heads, longtable=true, fontsize="scriptsize"))

$(figure_tex("shell_masses.png", "Mass by Kendall shell."; width="0.74\\linewidth"))

$(figure_tex("support_matrix.png", "Type by edge support matrix. Type-index order is the Kendall-shell basis order."; width="0.92\\linewidth"))

\\section{Support cores}

The strict core \\(\\gamma_6\\) is the mass supporting all six majority edges. The relaxed cores \\(\\gamma_5\\) and \\(\\gamma_4\\) show how much mass supports at least five or at least four edges. The majority graph is therefore sustained by overlapping partial coalitions rather than only by voters who support the whole graph.

$(latex_table(tables["core"]; caption="Support cores by coverage threshold", label="tab:cores", columns=core_cols, headers=core_heads))

\\section{Edge coalition overlap}

Anti-Bo edges overlap strongly, while the internal ordering among Lu, Te, and Ci is less coalitionally unified. In particular, Lu>Te with Te>Ci has low overlap compared with the anti-Bo edge pairs.

$(latex_table(edge_overlap_pairs; caption="Unordered edge coalition overlaps", label="tab:edge-overlap", columns=overlap_cols, headers=overlap_heads, longtable=true, fontsize="scriptsize"))

$(figure_tex("edge_overlap_heatmap.png", "Jaccard overlap between majority-edge coalitions."; width="0.72\\linewidth"))

\\section{Type anchoring}

The main anchor is Lu > Te > Ci > Bo. Shell-one variants also matter. Large reverse or countergraph types help explain the high value of \\(\\Psi\\): the graph is transitive, but much of the profile lies far from the reference order.

$(latex_table(anchor_top; caption="Top type anchors", label="tab:type-anchors", columns=voter_cols, headers=voter_heads, resize=true))

$(figure_tex("type_anchoring_top.png", "Top type anchoring scores."; width="0.82\\linewidth"))

\\section{Type breakers and edge amenability}

The fragile edge is Lu>Te: its margin is $(Int(round(lute.margin_mass[1]))) and only $(Int(round(lute.integer_flip_count[1]))) voters are needed to flip it. Some peripheral types are locally amenable even if they are not graph anchors. Amenability differs from raw mass because it discounts types farther from reversing a given edge.

$(latex_table(breaker_selected; caption="Top type breakers for Lu>Te, Lu>Bo, and Te>Ci", label="tab:type-breakers-selected", columns=breaker_cols, headers=breaker_heads, longtable=true, fontsize="scriptsize", landscape=true))

$(figure_tex("type_breakers_LuTe.png", "Type breakers for Lu>Te."; width="0.86\\linewidth"))

$(figure_tex("type_breakers_LuBo.png", "Type breakers for Lu>Bo."; width="0.86\\linewidth"))

\\section{Minimal breaking coalitions}

The table reports the first cumulative coalition, ordered by amenability, that exceeds each edge's flip threshold. For Lu>Te, the threshold is tiny, so a very small coalition can reverse the edge even though the majority graph as a whole remains broadly supported.

$(latex_table(tables["minimal_breaking_coalition"]; caption="Minimal breaking coalitions ordered by amenability", label="tab:minimal-breaking", columns=coalition_cols, headers=coalition_heads, longtable=true, fontsize="scriptsize", landscape=true))

\\section{Plurality switch analysis}

For a voter whose current first choice is \\(f\\) and whose second choice is Lu, switching Lu to first changes the Lu-Bo plurality margin by 2 if \\(f=\\)Bo and by 1 otherwise. Broad expansion and high-leverage swing mobilization point to different regions of the ranking profile: Te-first Lu-second is the best broad target; Bo-first Lu-second is smaller but twice as valuable per voter; generic Bolsonaro conversion is too crude because most Bo-first voters rank Lu last.

$(latex_table(tables["candidate_position_by_current_first"]; caption="Lu position by current first choice", label="tab:lu-position", columns=position_cols, headers=position_heads))

$(latex_table(tables["one_swap_target"]; caption="One-swap Lu target pools", label="tab:one-swap", columns=one_swap_cols, headers=one_swap_heads))

$(latex_table(swing_with_total; caption="Plurality swing values for Lu against Bo", label="tab:swing", columns=swing_cols, headers=swing_heads))

$(latex_table(exact_switch; caption="Exact type switch table for non-Lu first voters", label="tab:exact-switch", columns=exact_cols, headers=exact_heads, longtable=true, fontsize="scriptsize"))

$(figure_tex("lula_position_by_current_first.png", "Lula position by current first choice."; width="0.74\\linewidth"))

$(figure_tex("plurality_swing_values.png", "Plurality swing values."; width="0.70\\linewidth"))

\\section{Group contributions}

The majority graph is not produced by all partitions moving in the same direction. Ideology and PT produce strong pro/anti graph splits. Some partitions are cross-pressured by edge. Lu>Te is the critical fragile edge because group contributions strongly cancel.

$(join([figure_tex("group_contributions_$(p).png", "Group contribution heatmap for $(p)."; width="0.82\\linewidth") for p in group_partitions], "\n"))

$(group_table_sections(tables, group_partitions, "group_edge_power"; caption_prefix="Group edge power", selected=main_group_partitions))

\\section{Group Lula-switch amenability}

The tables below decompose Te/Ci/Bo-first voters who rank Lu second. They identify which groups contribute the largest raw mass to Lu-second pools, which groups have high conditional Lu-second rates, and where the Bo>Lu pool is concentrated.

$(join([figure_tex("group_lula_switch_$(p).png", "Group Lula-switch heatmap for $(p)."; width="0.78\\linewidth") for p in switch_group_partitions], "\n"))

$(group_table_sections(tables, group_partitions, "group_target_switch"; caption_prefix="Group Lula-switch table", selected=switch_group_partitions))

\\section{Strategic conclusion}

Plurality and pairwise majority diagnose different structures. Lula's safest expansion path runs through Te-first voters who already rank Lu second. The most efficient direct margin path runs through the smaller Bo-first Lu-second subgroup, because each voter moved both subtracts from Bo and adds to Lu. Ci-first Lu-second voters are smaller in this profile. Generic Bolsonaro conversion is not the correct category: most Bo-first voters rank Lu last, while the relevant Bo-first pool is specifically Bo-first Lu-second voters.

The one-swap pools are Te -> Lu = 138, Ci -> Lu = 47, and Bo -> Lu = 69. Their plurality swing values are 138, 47, and 138, for a total swing value of 323 and a counterfactual Lu-Bo plurality margin of 352. These are descriptive quantities from one linearized profile. Bootstrap, imputation, and linearization intervals are needed before treating the patterns as inferential.

\\appendix
\\section{Appendix: all group contribution tables}

$(group_table_sections(tables, group_partitions, "group_edge_power"; caption_prefix="Group edge power appendix"))

\\section{Appendix: all group switch tables}

$(group_table_sections(tables, group_partitions, "group_target_switch"; caption_prefix="Group switch appendix"))

\\section{Appendix: full edge support table}

$(latex_table(tables["edge_support"]; caption="Full edge support table", label="tab:edge-support-full", longtable=true, fontsize="tiny", landscape=true))

\\section{Appendix: full type breaker table}

$(latex_table(tables["type_breakers"]; caption="Full type breaker table", label="tab:type-breakers-full", longtable=true, fontsize="tiny", landscape=true))

\\section{Appendix: group breaker tables}

$(group_table_sections(tables, group_partitions, "group_breakers"; caption_prefix="Group breakers appendix"))

\\end{document}
"""
    write(report_path, tex)
end

function compile_latex(report_path)
    dir = dirname(report_path)
    file = basename(report_path)
    if Sys.which("latexmk") !== nothing
        cd(dir) do
            run(pipeline(`latexmk -pdf -interaction=nonstopmode $file`; stdout=devnull, stderr=devnull))
        end
        println("Compiled LaTeX report with latexmk.")
    elseif Sys.which("tectonic") !== nothing
        cd(dir) do
            run(pipeline(`tectonic $file`; stdout=devnull, stderr=devnull))
        end
        println("Compiled LaTeX report with tectonic.")
    else
        println("No latexmk or tectonic found; saved .tex report without compiling.")
    end
end

function main(args=ARGS)
    opts = parse_args(args)
    isfile(opts.input) || error("Input CSV not found: $(opts.input)")
    tables_dir = joinpath(opts.output, "tables")
    figures_dir = joinpath(opts.output, "figures")
    report_dir = joinpath(opts.output, "report")
    mkpath(tables_dir)
    mkpath(figures_dir)
    mkpath(report_dir)

    df = CSV.read(opts.input, DataFrame)
    profile = build_profile(df)
    basis = voter_type_basis(profile.pool; order=:kendall_shell, reference_order=REFERENCE_ORDER)
    result = majority_graph_support(profile; basis=basis)

    tables = Dict{String,DataFrame}()
    tables["majority_edges"] = add_edge_label!(majority_edges_table(result))
    tables["voter_types"] = voter_type_table(result)
    tables["edge_support"] = edge_support_table(result)
    tables["edge_overlap"] = edge_overlap_table(result)
    tables["core"] = core_table(result)
    tables["type_breakers"] = add_edge_label!(type_breaker_table(result))
    tables["minimal_breaking_coalition"] = add_edge_label!(minimal_breaking_coalition_table(result; by=:amenability))
    tables["plurality_scores"] = plurality_scores_table(profile; basis=basis)
    tables["pairwise_vs_plurality"] = pairwise_vs_plurality_decomposition_table(profile, TARGET, OPPONENT; basis=basis)
    tables["candidate_position_by_current_first"] = candidate_position_by_current_first_table(profile, TARGET; basis=basis)
    tables["one_swap_target"] = one_swap_target_table(profile, TARGET; current_first_candidates=CURRENT_FIRST_CANDIDATES, basis=basis)
    tables["plurality_swing_values"] = plurality_swing_value_table(profile, TARGET, OPPONENT; current_first_candidates=CURRENT_FIRST_CANDIDATES, basis=basis)
    tables["exact_type_switch"] = exact_type_switch_table(profile, TARGET; current_first_candidates=CURRENT_FIRST_CANDIDATES, basis=basis)

    group_partitions = Symbol[]
    for partition in PARTITIONS
        partition in propertynames(df) || continue
        push!(group_partitions, partition)
        labels = safe_partition_labels(df, partition)
        gresult = group_majority_graph_support(profile, labels; basis=basis)
        tables["group_edge_power_$(partition)"] = add_edge_label!(group_edge_power_table(gresult))
        tables["group_breakers_$(partition)"] = add_edge_label!(group_breaker_table(gresult))
        tables["group_anchor_$(partition)"] = group_anchor_table(gresult)
        tables["group_target_switch_$(partition)"] = group_target_switch_table(profile, labels, TARGET, OPPONENT;
                                                                               current_first_candidates=CURRENT_FIRST_CANDIDATES,
                                                                               basis=basis)
    end

    tables["pairwise_edges"] = tables["majority_edges"]
    tables["pairwise_vs_plurality_decomposition"] = tables["pairwise_vs_plurality"]
    tables["voter_type_basis"] = tables["voter_types"]
    tables["support_cores"] = tables["core"]
    tables["type_anchors_top"] = top_type_anchors(tables["voter_types"]; n=12)
    tables["type_breakers_all"] = tables["type_breakers"]
    tables["type_breakers_LuTe"] = edge_rows(tables["type_breakers"], [(:Lu, :Te)])
    tables["type_breakers_LuBo"] = edge_rows(tables["type_breakers"], [(:Lu, :Bo)])
    tables["minimal_breaking_coalitions"] = tables["minimal_breaking_coalition"]
    tables["candidate_position_by_current_first_Lu"] = tables["candidate_position_by_current_first"]
    tables["one_swap_Lu"] = tables["one_swap_target"]
    tables["plurality_swing_values_Lu_Bo"] = tables["plurality_swing_values"]
    tables["exact_type_switch_Lu"] = tables["exact_type_switch"]
    for partition in group_partitions
        tables["group_contributions_$(partition)"] = tables["group_edge_power_$(partition)"]
        tables["group_lula_switch_$(partition)"] = tables["group_target_switch_$(partition)"]
    end

    if opts.validate_known
        did_validate = validate_known_values(profile, tables)
        did_validate && println("Known 2022 validation checks passed.")
    end

    for (name, table) in sort(collect(tables); by=first)
        write_table(joinpath(tables_dir, "$name.csv"), table)
    end

    plot_pairs = [
        (path, fun) for (path, fun) in [
            ("plurality_scores", p -> plot_plurality_scores(tables["plurality_scores"]; output_path=p)),
            ("pairwise_margins", p -> plot_pairwise_margins(tables["majority_edges"]; output_path=p)),
            ("shell_masses", p -> plot_shell_masses(tables["voter_types"]; output_path=p)),
            ("edge_overlap_heatmap", p -> plot_edge_overlap_heatmap(tables["edge_overlap"]; output_path=p)),
            ("type_anchoring", p -> plot_type_anchoring(tables["voter_types"]; output_path=p)),
            ("type_anchoring_top", p -> plot_type_anchoring(tables["type_anchors_top"]; output_path=p, top_n=12)),
            ("support_matrix", p -> plot_support_matrix(tables["edge_support"]; output_path=p)),
            ("lula_position_by_current_first", p -> plot_candidate_position_by_current_first(tables["candidate_position_by_current_first"]; output_path=p, target_label="Lula")),
            ("plurality_swing_values", p -> plot_plurality_swing_values(tables["plurality_swing_values"]; output_path=p)),
        ]
    ]
    for (stem, fun) in plot_pairs
        fun(joinpath(figures_dir, "$stem.png"))
        fun(joinpath(figures_dir, "$stem.pdf"))
    end
    plot_type_breakers(tables["type_breakers"]; output_path=joinpath(figures_dir, "type_breakers.png"))
    plot_type_breakers(tables["type_breakers"]; output_path=joinpath(figures_dir, "type_breakers.pdf"))
    for edge in ["Lu>Te", "Lu>Bo"]
        stem = edge == "Lu>Te" ? "type_breakers_LuTe" : "type_breakers_LuBo"
        plot_type_breakers(tables["type_breakers"]; output_path=joinpath(figures_dir, "$stem.png"), edge=edge, top_n=10)
        plot_type_breakers(tables["type_breakers"]; output_path=joinpath(figures_dir, "$stem.pdf"), edge=edge, top_n=10)
    end

    for partition in group_partitions
        table = tables["group_edge_power_$(partition)"]
        plot_group_contributions(table; output_path=joinpath(figures_dir, "group_contributions_$(partition).png"))
        plot_group_contributions(table; output_path=joinpath(figures_dir, "group_contributions_$(partition).pdf"))
        switch_key = "group_target_switch_$(partition)"
        if haskey(tables, switch_key)
            plot_group_target_switch(tables[switch_key]; output_path=joinpath(figures_dir, "group_lula_switch_$(partition).png"),
                                     title="Lula switch by $(partition)")
            plot_group_target_switch(tables[switch_key]; output_path=joinpath(figures_dir, "group_lula_switch_$(partition).pdf"),
                                     title="Lula switch by $(partition)")
        end
    end

    manifest = Dict(
        "input_csv" => abspath(opts.input),
        "output_dir" => abspath(opts.output),
        "generated_at" => Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS"),
        "candidate_labels" => Dict(string(c.label) => c.display for c in CANDIDATES),
        "reference_order" => string.(REFERENCE_ORDER),
        "target" => string(TARGET),
        "opponent" => string(OPPONENT),
        "partitions" => string.(group_partitions),
    )
    open(joinpath(opts.output, "manifest.toml"), "w") do io
        TOML.print(io, manifest)
    end

    report_path = joinpath(report_dir, "majority_graph_support_2022.tex")
    generate_report(report_path, opts.output, opts.input, tables, group_partitions)
    compile_latex(report_path)

    println("Wrote outputs to $(opts.output)")
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
