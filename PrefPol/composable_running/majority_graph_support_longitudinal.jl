#!/usr/bin/env julia

const SCRIPT_DIR = @__DIR__
const REPO_ROOT = normpath(joinpath(SCRIPT_DIR, "..", ".."))

using CSV
using DataFrames
using Dates
using Printf
using Statistics
using TOML

function parse_args(args)
    opts = Dict("input-root" => joinpath(SCRIPT_DIR, "output"),
                "years" => "2006,2018,2022")
    i = 1
    while i <= length(args)
        a = args[i]
        if a in ("--input-root", "--years")
            i += 1
            i <= length(args) || error("$a requires a value")
            opts[a[3:end]] = args[i]
        else
            error("Unknown argument: $a")
        end
        i += 1
    end
    return (input_root = isabspath(opts["input-root"]) ? opts["input-root"] : joinpath(REPO_ROOT, opts["input-root"]),
            years = parse.(Int, split(opts["years"], ",")))
end

function req(path)
    isfile(path) || error("Missing required longitudinal input table: $path")
    return CSV.read(path, DataFrame)
end

function manifest(path)
    isfile(path) || error("Missing required year report manifest: $path")
    return TOML.parsefile(path)
end

edge_label(row) = string(row.winner, ">", row.loser)
compact(x) = replace(string(x), " -> " => ">", " > " => ">")
same_name(x, y) = string(x) == string(y)

function first_or_missing(df, col)
    nrow(df) == 0 || !(col in propertynames(df)) ? missing : df[1, col]
end

function core_at(df, k, col)
    rows = df[Int.(df.k) .== Int(k), :]
    nrow(rows) == 0 ? missing : rows[1, col]
end

function role_share(df, role_col, role)
    role_col in propertynames(df) || return missing
    rows = df[string.(df[!, role_col]) .== role, :]
    nrow(rows) == 0 || !(:share in propertynames(rows)) ? missing : rows.share[1]
end

function top_types_for_core(df, k)
    rows = df[Int.(df.k) .== Int(k), :]
    nrow(rows) == 0 || !(:above_threshold_rankings in propertynames(rows)) ? missing : rows.above_threshold_rankings[1]
end

function minimal_flip_summary(df, edge)
    rows = df[compact.(df.edge) .== edge, :]
    nrow(rows) == 0 && return (missing, missing, missing)
    flip = rows[rows.flips_edge, :]
    nrow(flip) == 0 && return (missing, missing, missing)
    row = first(flip, 1)[1, :]
    top = join(string.(first(rows.ranking, min(4, nrow(rows)))), "; ")
    return (row.rank_in_coalition, row.cumulative_mass, top)
end

function group_extreme(tables, group_partitions, edge; value=:group_margin_contribution, rev=true)
    rows = DataFrame[]
    for p in group_partitions
        key = "group_edge_power_$(p)"
        haskey(tables, key) || continue
        df = tables[key]
        :edge in propertynames(df) || continue
        sub = df[compact.(df.edge) .== edge, :]
        isempty(sub) || push!(rows, transform(sub, [] => ByRow(() -> string(p)) => :partition))
    end
    isempty(rows) && return missing
    allrows = vcat(rows...; cols=:union)
    value in propertynames(allrows) || return missing
    sorted = sort(allrows, value, rev=rev)
    row = sorted[1, :]
    return "$(row.partition):$(row.group) ($(round(row[value]; digits=3)))"
end

function switch_extreme(tables, group_partitions; value=:plurality_swing_value)
    rows = DataFrame[]
    for p in group_partitions
        key = "group_target_switch_$(p)"
        haskey(tables, key) || continue
        df = tables[key]
        isempty(df) || push!(rows, transform(df, [] => ByRow(() -> string(p)) => :partition))
    end
    isempty(rows) && return missing
    allrows = vcat(rows...; cols=:union)
    value in propertynames(allrows) || return missing
    row = sort(allrows, value, rev=true)[1, :]
    return "$(row.partition):$(row.group)/$(row.current_first) ($(round(row[value]; digits=3)))"
end

function year_row(input_root, year)
    dir = joinpath(input_root, "majority_graph_support_$(year)")
    man = manifest(joinpath(dir, "manifest.toml"))
    tdir = joinpath(dir, "tables")
    tables = Dict{String,DataFrame}()
    for name in ["plurality_scores", "majority_edges", "core", "reverse_core_effective_composition",
                 "support_core_effective_composition", "edge_overlap", "edge_effective_composition",
                 "role_mass_summary", "primary_role_mass_summary", "countergraph_summary",
                 "minimal_breaking_coalition", "plurality_swing_values"]
        tables[name] = req(joinpath(tdir, "$name.csv"))
    end
    group_partitions = Symbol.(get(man, "partitions", String[]))
    for p in group_partitions
        for stem in ("group_edge_power", "group_target_switch", "group_breakers")
            path = joinpath(tdir, "$(stem)_$(p).csv")
            isfile(path) && (tables["$(stem)_$(p)"] = CSV.read(path, DataFrame))
        end
    end

    active = join([c["label"] for c in man["active_candidates"]], "|")
    target = string(man["target"])
    opponent = string(man["opponent"])
    plurality = tables["plurality_scores"]
    target_plurality_rows = plurality[same_name.(plurality.candidate, target), :]
    opp_plurality_rows = plurality[same_name.(plurality.candidate, opponent), :]
    nrow(target_plurality_rows) == 1 ||
        error("Target plurality row missing for $year: target=$target candidates=$(join(string.(plurality.candidate), ","))")
    nrow(opp_plurality_rows) == 1 ||
        error("Opponent plurality row missing for $year: opponent=$opponent candidates=$(join(string.(plurality.candidate), ","))")
    target_plurality = target_plurality_rows.first_place_count[1]
    opp_plurality = opp_plurality_rows.first_place_count[1]
    edges = tables["majority_edges"]
    weakest = sort(edges, :normalized_margin)[1, :]
    target_edge_rows = edges[(same_name.(edges.winner, target) .& same_name.(edges.loser, opponent)) .|
                             (same_name.(edges.winner, opponent) .& same_name.(edges.loser, target)), :]
    nrow(target_edge_rows) == 1 || error("Target-opponent edge missing for $year: $target>$opponent")
    target_edge = target_edge_rows[1, :]
    target_edge_sign = same_name(target_edge.winner, target) ? 1 : -1
    E = Int(man["E"])
    overlap = tables["edge_overlap"]
    offdiag = overlap[Int.(overlap.edge_i) .!= Int.(overlap.edge_j), :]
    edge_eff = tables["edge_effective_composition"]
    weak_eff = edge_eff[compact.(edge_eff.edge) .== compact(weakest.edge), :]
    targ_eff = edge_eff[compact.(edge_eff.edge) .== compact(target_edge.edge), :]
    support_core = tables["support_core_effective_composition"]
    reverse_core = tables["reverse_core_effective_composition"]
    minrank_w, minmass_w, mintop_w = minimal_flip_summary(tables["minimal_breaking_coalition"], compact(weakest.edge))
    minrank_t, minmass_t, mintop_t = minimal_flip_summary(tables["minimal_breaking_coalition"], compact(target_edge.edge))
    swing = tables["plurality_swing_values"]
    largest_pool = isempty(swing) ? missing : sort(swing, :one_swap_mass, rev=true)[1, :]
    highest_leverage = isempty(swing) ? missing : sort(swing, :plurality_swing_value, rev=true)[1, :]
    return (
        year=year,
        scenario=man["scenario_name"],
        m=man["m"],
        active_candidates=active,
        target=target,
        opponent=opponent,
        target_plurality_count=target_plurality,
        opponent_plurality_count=opp_plurality,
        plurality_margin=target_plurality - opp_plurality,
        target_opponent_pairwise_margin=target_edge_sign * target_edge.margin_mass,
        target_opponent_normalized_margin=target_edge_sign * target_edge.normalized_margin,
        weakest_edge=compact(weakest.edge),
        weakest_edge_normalized_margin=weakest.normalized_margin,
        weakest_edge_flip_count=weakest.integer_flip_count,
        E=E,
        gamma_support_E=core_at(tables["core"], E, :core_mass),
        gamma_support_E_minus_1=E >= 1 ? core_at(tables["core"], E-1, :core_mass) : missing,
        gamma_support_E_minus_2=E >= 2 ? core_at(tables["core"], E-2, :core_mass) : missing,
        gamma_reverse_E=core_at(reverse_core, E, :reverse_core_mass),
        gamma_reverse_E_minus_1=E >= 1 ? core_at(reverse_core, E-1, :reverse_core_mass) : missing,
        gamma_reverse_E_minus_2=E >= 2 ? core_at(reverse_core, E-2, :reverse_core_mass) : missing,
        support_core_E_neff=core_at(support_core, E, :neff),
        support_core_E_minus_1_neff=E >= 1 ? core_at(support_core, E-1, :neff) : missing,
        support_core_E_minus_2_neff=E >= 2 ? core_at(support_core, E-2, :neff) : missing,
        reverse_core_E_neff=core_at(reverse_core, E, :neff),
        reverse_core_E_minus_1_neff=E >= 1 ? core_at(reverse_core, E-1, :neff) : missing,
        reverse_core_E_minus_2_neff=E >= 2 ? core_at(reverse_core, E-2, :neff) : missing,
        support_core_E_top_effective_types=top_types_for_core(support_core, E),
        reverse_core_E_top_effective_types=top_types_for_core(reverse_core, E),
        mean_edge_jaccard_excluding_diagonal=mean(skipmissing(offdiag.jaccard)),
        min_edge_jaccard_excluding_diagonal=minimum(skipmissing(offdiag.jaccard)),
        max_edge_jaccard_excluding_diagonal=maximum(skipmissing(offdiag.jaccard)),
        weakest_edge_support_neff=first_or_missing(weak_eff, :support_neff),
        weakest_edge_opposition_neff=first_or_missing(weak_eff, :opposition_neff),
        weakest_edge_support_above_threshold_types=first_or_missing(weak_eff, :support_n_effective_above_threshold),
        weakest_edge_opposition_above_threshold_types=first_or_missing(weak_eff, :opposition_n_effective_above_threshold),
        target_opponent_support_neff=first_or_missing(targ_eff, :support_neff),
        target_opponent_opposition_neff=first_or_missing(targ_eff, :opposition_neff),
        target_opponent_support_above_threshold_types=first_or_missing(targ_eff, :support_n_effective_above_threshold),
        target_opponent_opposition_above_threshold_types=first_or_missing(targ_eff, :opposition_n_effective_above_threshold),
        edge_with_most_concentrated_support=compact(sort(edge_eff, :support_neff)[1, :edge]),
        edge_with_most_concentrated_opposition=compact(sort(edge_eff, :opposition_neff)[1, :edge]),
        edge_with_most_type_plural_support=compact(sort(edge_eff, :support_neff, rev=true)[1, :edge]),
        edge_with_most_type_plural_opposition=compact(sort(edge_eff, :opposition_neff, rev=true)[1, :edge]),
        anchor_share_nonexclusive=role_share(tables["role_mass_summary"], :role, "anchor"),
        peripheral_supporter_share_nonexclusive=role_share(tables["role_mass_summary"], :role, "peripheral_supporter"),
        mixed_share_nonexclusive=role_share(tables["role_mass_summary"], :role, "mixed"),
        countergraph_share_nonexclusive=role_share(tables["role_mass_summary"], :role, "counter_graph"),
        edge_breaker_share_nonexclusive=role_share(tables["role_mass_summary"], :role, "edge_breaker"),
        anchor_share_primary=role_share(tables["primary_role_mass_summary"], :primary_role, "anchor"),
        mixed_share_primary=role_share(tables["primary_role_mass_summary"], :primary_role, "mixed"),
        countergraph_share_primary=role_share(tables["primary_role_mass_summary"], :primary_role, "counter_graph"),
        edge_breaker_share_primary=role_share(tables["primary_role_mass_summary"], :primary_role, "edge_breaker"),
        anchor_edge_breaker_share_primary=role_share(tables["primary_role_mass_summary"], :primary_role, "anchor_edge_breaker"),
        max_countergraph_type_ranking=tables["countergraph_summary"].largest_countergraph_type_ranking[1],
        max_countergraph_type_share=tables["countergraph_summary"].largest_countergraph_type_share[1],
        weakest_edge_minimal_breaking_rank=minrank_w,
        weakest_edge_minimal_breaking_mass=minmass_w,
        weakest_edge_minimal_breaking_top_types=mintop_w,
        target_opponent_minimal_breaking_rank=minrank_t,
        target_opponent_minimal_breaking_mass=minmass_t,
        target_opponent_minimal_breaking_top_types=mintop_t,
        largest_switch_pool_by_current_first=largest_pool === missing ? missing : largest_pool.current_first,
        largest_switch_pool_count=largest_pool === missing ? missing : largest_pool.one_swap_mass,
        largest_switch_pool_rate=largest_pool === missing ? missing : missing,
        highest_leverage_switch_pool=highest_leverage === missing ? missing : highest_leverage.current_first,
        highest_leverage_swing_value=highest_leverage === missing ? missing : highest_leverage.plurality_swing_value,
        total_one_swap_target_pool=sum(swing.one_swap_mass),
        total_plurality_swing_value=sum(swing.plurality_swing_value),
        counterfactual_target_opponent_plurality_margin=(isempty(swing) ? missing : swing.target_opponent_margin_before[1] + sum(swing.plurality_swing_value)),
        strongest_group_positive_margin_contribution_on_weakest_edge=group_extreme(tables, group_partitions, compact(weakest.edge); value=:group_margin_contribution, rev=true),
        strongest_group_negative_margin_contribution_on_weakest_edge=group_extreme(tables, group_partitions, compact(weakest.edge); value=:group_margin_contribution, rev=false),
        strongest_group_breaker_on_weakest_edge=group_extreme(tables, group_partitions, compact(weakest.edge); value=:raw_group_breaking_score, rev=true),
        largest_group_switch_pool=switch_extreme(tables, group_partitions; value=:target_second_mass),
        largest_group_switch_pool_value=switch_extreme(tables, group_partitions; value=:plurality_swing_value),
    )
end

function latex_escape(x)
    s = string(x)
    for (k, v) in ["\\"=>"\\textbackslash{}", "_"=>"\\_", "%"=>"\\%", "&"=>"\\&", "#" => "\\#"]
        s = replace(s, k => v)
    end
    return s
end

function latex_table(df; caption, maxrows=20)
    rows = first(df, min(maxrows, nrow(df)))
    cols = propertynames(rows)
    lines = ["\\begin{table}[H]\\centering\\scriptsize\\resizebox{\\linewidth}{!}{%",
             "\\begin{tabular}{" * "l" ^ length(cols) * "}\\toprule",
             join(latex_escape.(cols), " & ") * " \\\\ \\midrule"]
    for row in eachrow(rows)
        push!(lines, join((latex_escape(row[c]) for c in cols), " & ") * " \\\\")
    end
    push!(lines, "\\bottomrule\\end{tabular}}")
    push!(lines, "\\caption{$(latex_escape(caption))}\\end{table}")
    return join(lines, "\n")
end

function write_report(path, table)
    mkpath(dirname(path))
    tex = """
\\documentclass[11pt]{article}
\\usepackage[margin=0.85in]{geometry}
\\usepackage{booktabs,graphicx,float,pdflscape}
\\usepackage{lmodern,microtype,hyperref}
\\setlength{\\parindent}{0pt}
\\setlength{\\parskip}{0.6em}
\\begin{document}
\\title{Longitudinal Majority-Graph Support Comparison}
\\author{Julia replication workflow}
\\date{$(Dates.format(now(), dateformat"yyyy-mm-dd HH:MM"))}
\\maketitle

\\section{Introduction}
Plurality does not reveal the ordinal architecture of electoral majorities. These Brazilian multiparty presidential-election diagnostics compare majority cohesion, overlap, fragility, concentration, and typological composition across canonical linearized profiles for 2006, 2018, and 2022.

\\section{Ordinal object}
The object is a strict-ranking profile over the active candidate set, its pairwise majority graph, voter types, edge coalitions, support cores, and reverse support cores.

\\section{Measures}
The comparison uses pairwise margin, plurality-pairwise decomposition, support cores gamma_k^+, reverse support cores gamma_k^-, effective type composition, edge overlap/Jaccard, Kendall shells, type roles, edge fragility/amenability, minimal breaking coalitions, switch pools, group switch amenability, and group edge-power decomposition. E denotes the number of majority edges; full cores are gamma_E^+ and gamma_E^-.

$(join(["\\section{$(row.year)}\nThe $(row.year) majority graph has E=$(row.E), weakest edge $(latex_escape(row.weakest_edge)), target-opponent pairwise margin $(row.target_opponent_pairwise_margin), strict support core $(row.gamma_support_E), and strict reverse core $(row.gamma_reverse_E). The largest switch pool is $(latex_escape(row.largest_switch_pool_by_current_first))." for row in eachrow(table)], "\n"))

\\section{Longitudinal comparison table}
$(latex_table(table; caption="Longitudinal majority-graph support comparison", maxrows=nrow(table)))

\\section{Comparative interpretation}
The rows compare how plurality under- or overstates pairwise target-opponent strength, whether each graph is held by a strict support core or overlapping partial coalitions, whether the reverse side is concentrated or diffuse, whether fragile edges are internal ordering edges or main opponent edges, and whether weak-edge opposition is more type-plural than support. They also compare role shifts across anchor, peripheral supporter, mixed, countergraph, and breaker types, and whether switch pools are broad, high-leverage, or group-concentrated.

\\section{Conclusion}
Brazilian multiparty elections can have similar plurality appearances but different ordinal majority architectures.
\\end{document}
"""
    write(path, tex)
end

function compile_latex(path)
    try
        if Sys.which("latexmk") !== nothing
            cd(dirname(path)) do
                run(pipeline(`latexmk -pdf -interaction=nonstopmode $(basename(path))`; stdout=devnull, stderr=devnull))
            end
        elseif Sys.which("tectonic") !== nothing
            cd(dirname(path)) do
                run(pipeline(`tectonic $(basename(path))`; stdout=devnull, stderr=devnull))
            end
        else
            println("No latexmk or tectonic found; saved .tex report without compiling.")
        end
    catch err
        @warn "LaTeX compilation failed; .tex report was written" exception=(err, catch_backtrace())
    end
end

function validate_longitudinal(table)
    nrow(table) > 0 || error("Longitudinal table has no completed year rows.")
    for col in [:active_candidates, :target, :opponent, :E, :weakest_edge,
                :gamma_support_E, :gamma_support_E_minus_1, :gamma_support_E_minus_2,
                :gamma_reverse_E, :gamma_reverse_E_minus_1, :gamma_reverse_E_minus_2]
        col in propertynames(table) || error("Longitudinal table missing column $col")
        any(ismissing, table[!, col]) && error("Longitudinal table column $col contains missing values")
    end
    return true
end

function main(args=ARGS)
    opts = parse_args(args)
    rows = [year_row(opts.input_root, year) for year in opts.years]
    table = DataFrame(rows)
    validate_longitudinal(table)
    out_dir = joinpath(opts.input_root, "majority_graph_support_longitudinal")
    mkpath(joinpath(out_dir, "tables"))
    mkpath(joinpath(out_dir, "report"))
    CSV.write(joinpath(out_dir, "tables", "longitudinal_comparison.csv"), table)
    report = joinpath(out_dir, "report", "majority_graph_support_longitudinal.tex")
    write_report(report, table)
    compile_latex(report)
    open(joinpath(out_dir, "manifest.toml"), "w") do io
        TOML.print(io, Dict("years" => opts.years,
                            "input_root" => abspath(opts.input_root),
                            "generated_at" => Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS")))
    end
    println("Wrote longitudinal majority-graph support outputs to $out_dir")
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
