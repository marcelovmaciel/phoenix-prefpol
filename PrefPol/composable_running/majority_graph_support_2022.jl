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
    replacements = Dict("\\"=>"\\textbackslash{}", "_"=>"\\_", "%"=>"\\%", "&"=>"\\&",
                        "#"=>"\\#", raw"$"=>"\\\$", "{"=>"\\{", "}"=>"\\}",
                        "~"=>"\\textasciitilde{}", "^"=>"\\textasciicircum{}")
    for (k, v) in replacements
        s = replace(s, k => v)
    end
    return s
end

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

function latex_table(df::DataFrame; maxrows=20)
    cols = propertynames(df)
    rows = first(df, min(maxrows, nrow(df)))
    lines = String[]
    push!(lines, "\\begin{center}\\small")
    push!(lines, "\\begin{tabular}{" * "l" ^ length(cols) * "}")
    push!(lines, "\\toprule")
    push!(lines, join(latex_escape.(cols), " & ") * " \\\\")
    push!(lines, "\\midrule")
    for row in eachrow(rows)
        push!(lines, join((latex_value(row[c]) for c in cols), " & ") * " \\\\")
    end
    push!(lines, "\\bottomrule")
    push!(lines, "\\end{tabular}")
    nrow(df) > maxrows && push!(lines, "\\par\\smallskip Showing $(maxrows) of $(nrow(df)) rows.")
    push!(lines, "\\end{center}")
    return join(lines, "\n")
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

    tex = """
\\documentclass[11pt]{article}
\\usepackage[margin=0.85in]{geometry}
\\usepackage{booktabs,graphicx,float}
\\usepackage{amsmath,amssymb}
\\usepackage[T1]{fontenc}
\\usepackage[utf8]{inputenc}
\\setlength{\\parindent}{0pt}
\\setlength{\\parskip}{0.6em}
\\begin{document}

\\title{Majority-Graph Support for the 2022 Linearized Profile}
\\author{Julia replication workflow}
\\date{$(latex_escape(Dates.format(now(), dateformat"yyyy-mm-dd HH:MM")))}
\\maketitle

This report is generated by Julia from the composable package workflow. It analyzes one linearized profile, not bootstrap uncertainty. The input CSV was:
\\begin{quote}\\texttt{$(latex_escape(input_path))}\\end{quote}

For pairwise margins, the corrected definition is
\\[
\\Psi = 1 - \\frac{1}{\\binom{m}{2}}\\sum_{\\{a,b\\}}\\frac{|n_{ab}-n_{ba}|}{n},
\\]
so \\(\\Psi\\) is one minus the average normalized absolute pairwise margin. Here \\(\\Psi \\approx $(@sprintf("%.3f", psi))\\).

\\section*{Plurality And Pairwise Majority}

Lula has $(Int(round(lu))) first-place rankings and Bolsonaro has $(Int(round(bo))), so the plurality Lu-Bo margin is $(Int(round(lu - bo))). In contrast, the pairwise Lu>Bo edge has support $(Int(round(lubo.support_mass[1]))), opposition $(Int(round(lubo.opposition_mass[1]))), and margin $(Int(round(lubo.margin_mass[1]))). The majority order is Lu > Te > Ci > Bo.

$(latex_table(plurality))

$(latex_table(edges))

\\begin{figure}[H]
\\centering
\\includegraphics[width=0.74\\linewidth]{../figures/plurality_scores.png}
\\caption{Plurality scores.}
\\end{figure}

\\begin{figure}[H]
\\centering
\\includegraphics[width=0.80\\linewidth]{../figures/pairwise_margins.png}
\\caption{Pairwise majority margins.}
\\end{figure}

\\section*{Why Pairwise And Plurality Differ}

The decomposition below groups the target-opponent comparison by current first choice. The non-Lu/non-Bo first-choice surplus explains why the Lu>Bo pairwise margin is much larger than the Lu-Bo plurality margin.

$(latex_table(decomp))

\\section*{Voter-Type Basis And Majority-Graph Support}

The voter-type basis is the full \\(m!\\) strict-ranking basis, ordered by Kendall shell around Lu > Te > Ci > Bo. Zero-mass types are retained and type indices are stable.

\\begin{figure}[H]
\\centering
\\includegraphics[width=0.74\\linewidth]{../figures/shell_masses.png}
\\caption{Mass by Kendall shell.}
\\end{figure}

\\begin{figure}[H]
\\centering
\\includegraphics[width=0.92\\linewidth]{../figures/support_matrix.png}
\\caption{Type by edge support matrix.}
\\end{figure}

\\section*{Plurality Swing Values}

For a voter whose current first choice is \\(f\\) and whose second choice is Lu, switching Lu to first changes the Lu-Bo plurality margin by 2 if \\(f=\\)Bo and by 1 otherwise.

$(latex_table(swing_with_total))

\\begin{figure}[H]
\\centering
\\includegraphics[width=0.74\\linewidth]{../figures/lula_position_by_current_first.png}
\\caption{Lula position by current first choice.}
\\end{figure}

\\begin{figure}[H]
\\centering
\\includegraphics[width=0.70\\linewidth]{../figures/plurality_swing_values.png}
\\caption{Plurality swing values.}
\\end{figure}

\\section*{Group Contributions}

Group tables and figures were generated for: $(join(latex_escape.(group_partitions), ", ")).

\\section*{TODO}

The same quantities should be pooled across bootstrap, imputation, and linearization replicates to obtain uncertainty intervals. This report is intentionally limited to one linearized profile.

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
    tables["type_breakers"] = type_breaker_table(result)
    tables["minimal_breaking_coalition"] = minimal_breaking_coalition_table(result)
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
        tables["group_breakers_$(partition)"] = group_breaker_table(gresult)
        tables["group_anchor_$(partition)"] = group_anchor_table(gresult)
        tables["group_target_switch_$(partition)"] = group_target_switch_table(profile, labels, TARGET, OPPONENT;
                                                                               current_first_candidates=CURRENT_FIRST_CANDIDATES,
                                                                               basis=basis)
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

    for partition in group_partitions
        table = tables["group_edge_power_$(partition)"]
        plot_group_contributions(table; output_path=joinpath(figures_dir, "group_contributions_$(partition).png"))
        plot_group_contributions(table; output_path=joinpath(figures_dir, "group_contributions_$(partition).pdf"))
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
