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
const ROLE_REPORT_PARTITIONS = [:Ideology, :PT, :Sex, :Religion, :Education, :Income, :Abortion]
const ROLE_THRESHOLDS = MajorityGraphRoleThresholds()
const ROLE_AMENABILITY = :inverse
const ROLE_LAMBDA = 1.0
const EXPECTED_2022_WEAKEST_EDGE = "Lu>Te"

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

function write_role_table!(tables::Dict{String,DataFrame}, name::AbstractString, table::DataFrame,
                           roles_dir::AbstractString)
    tables[name] = table
    write_table(joinpath(roles_dir, "$name.csv"), table)
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

function compact_edge_label(x)
    x === missing && return missing
    return replace(string(x), " -> " => ">", " > " => ">")
end

function compact_role_edge_labels!(df::DataFrame)
    for col in (:edge, :max_breaking_edge)
        col in propertynames(df) || continue
        df[!, col] = compact_edge_label.(df[!, col])
    end
    return df
end

function selected_columns(df::DataFrame, cols)
    keep = [Symbol(c) for c in cols if Symbol(c) in propertynames(df)]
    return df[:, keep]
end

function add_partition_column(df::DataFrame, partition::Symbol)
    out = copy(df)
    out.partition = fill(string(partition), nrow(out))
    return select(out, :partition, Not(:partition))
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

function weakest_edge_label(result::MajorityGraphSupportResult)
    idx = argmin([edge.normalized_margin for edge in result.edges])
    edge = result.edges[idx]
    return string(result.pool[edge.winner], ">", result.pool[edge.loser])
end

function validate_role_outputs(tables, group_partitions)
    required = [
        "voter_type_roles",
        "role_mass_summary",
        "primary_role_mass_summary",
        "edge_type_roles",
        "weakest_edge_role_summary",
    ]
    for key in required
        haskey(tables, key) || error("Role validation failed: missing table $key")
        nrow(tables[key]) > 0 || error("Role validation failed: empty table $key")
    end
    for partition in group_partitions
        for stem in ("group_roles", "group_primary_roles", "group_role_power")
            key = "$(stem)_$(partition)"
            haskey(tables, key) || error("Role validation failed: missing table $key")
            nrow(tables[key]) > 0 || error("Role validation failed: empty table $key")
        end
    end

    roles = tables["voter_type_roles"]
    anchor = roles[(roles.ranking .== "Lu > Te > Ci > Bo") .& (roles.anchor .| occursin.("anchor", roles.primary_role)), :]
    nrow(anchor) > 0 || error("Role validation failed: Lu > Te > Ci > Bo was not anchor-related")
    counter = roles[(roles.ranking .== "Bo > Ci > Te > Lu") .& roles.counter_graph, :]
    nrow(counter) > 0 || error("Role validation failed: Bo > Ci > Te > Lu was not counter-graph")

    weakest = tables["weakest_edge_role_summary"]
    nrow(weakest) > 0 || error("Role validation failed: weakest edge summary is empty")
    selected_edge = string(weakest.edge[1])
    selected_edge == EXPECTED_2022_WEAKEST_EDGE ||
        error("Role validation failed: weakest edge was $selected_edge, expected $EXPECTED_2022_WEAKEST_EDGE")
    for partition in (:Ideology, :PT)
        key = "group_role_power_$(partition)"
        haskey(tables, key) && nrow(tables[key]) > 0 ||
            error("Role validation failed: $key is missing or empty")
    end
    return true
end

function validate_effective_type_diagnostics(result::MajorityGraphSupportResult, tables)
    edges = tables["edge_effective_types"]
    for row in eachrow(edges)
        assert_close("effective edge shares $(row.edge_index)",
                     row.support_share_total + row.opposition_share_total, 1.0)
        assert_close("support effective threshold $(row.edge)",
                     row.support_effective_threshold, 1.0 / row.support_effective_types)
        assert_close("opposition effective threshold $(row.edge)",
                     row.opposition_effective_threshold, 1.0 / row.opposition_effective_types)
    end

    edge_comp = tables["edge_effective_type_composition"]
    for sub in groupby(edge_comp, [:edge_index, :side])
        key = first(sub)
        assert_close("effective edge composition $(key.edge_index) $(key.side)",
                     sum(sub.conditional_share), 1.0)
        summary = edges[edges.edge_index .== key.edge_index, :]
        nrow(summary) == 1 || error("Validation failed: missing edge summary for edge $(key.edge_index)")
        expected_eff = key.side == "support" ?
            summary.support_effective_types[1] : summary.opposition_effective_types[1]
        expected_threshold = 1.0 / expected_eff
        assert_close("effective edge threshold $(key.edge_index) $(key.side)",
                     key.effective_threshold, expected_threshold)
        expected_count = key.side == "support" ?
            summary.support_n_above_effective_threshold[1] :
            summary.opposition_n_above_effective_threshold[1]
        actual_count = count(sub.above_effective_threshold)
        expected_count == actual_count ||
            error("Validation failed for above-threshold edge count $(key.edge_index) $(key.side): got $actual_count, expected $expected_count")
    end
    for row in eachrow(edge_comp)
        if !isnan(row.effective_threshold)
            assert_close("effective edge row weight $(row.edge_index) $(row.side) type $(row.type_index)",
                         row.effective_weight, row.conditional_share / row.effective_threshold)
            row.above_effective_threshold == (row.effective_weight > 1.0) ||
                error("Validation failed for effective edge flag $(row.edge_index) $(row.side) type $(row.type_index)")
        else
            row.above_effective_threshold == false ||
                error("Validation failed: NaN threshold row marked above effective threshold")
        end
    end

    cores = tables["core_effective_types"]
    for row in eachrow(cores)
        assert_close("effective core mass k=$(row.k)", row.core_mass, result.core_mass_by_k[row.k])
        assert_close("effective core threshold k=$(row.k)", row.effective_threshold, 1.0 / row.effective_types)
    end

    reverse_cores = tables["reverse_core_effective_types"]
    nedges = length(result.edges)
    for row in eachrow(reverse_cores)
        expected = sum(result.type_proportion[r] for r in eachindex(result.type_proportion)
                       if nedges - result.coverage[r] >= row.k)
        assert_close("effective reverse core mass k=$(row.k)", row.reverse_core_mass, expected)
        assert_close("effective reverse core threshold k=$(row.k)", row.effective_threshold, 1.0 / row.effective_types)
    end

    for key in ("core_effective_type_composition", "reverse_core_effective_type_composition")
        comp = tables[key]
        summary = key == "core_effective_type_composition" ? cores : reverse_cores
        for sub in groupby(comp, [:k, :core_kind])
            included = sub[sub.included, :]
            isempty(included) && continue
            mass = sum(included.type_proportion)
            mass == 0.0 && continue
            firstrow = first(sub)
            assert_close("effective core composition $(firstrow.core_kind) k=$(firstrow.k)",
                         sum(included.conditional_share), 1.0)
            summary_row = summary[summary.k .== firstrow.k, :]
            nrow(summary_row) == 1 || error("Validation failed: missing $(firstrow.core_kind) core summary k=$(firstrow.k)")
            assert_close("effective core composition threshold $(firstrow.core_kind) k=$(firstrow.k)",
                         firstrow.effective_threshold, 1.0 / summary_row.effective_types[1])
            actual_count = count(sub.above_effective_threshold)
            expected_count = summary_row.n_above_effective_threshold[1]
            actual_count == expected_count ||
                error("Validation failed for above-threshold $(firstrow.core_kind) core count k=$(firstrow.k): got $actual_count, expected $expected_count")
            for row in eachrow(sub)
                if row.included && !isnan(row.effective_threshold)
                    assert_close("effective core row weight $(firstrow.core_kind) k=$(row.k) type $(row.type_index)",
                                 row.effective_weight, row.conditional_share / row.effective_threshold)
                    row.above_effective_threshold == (row.effective_weight > 1.0) ||
                        error("Validation failed for effective core flag $(firstrow.core_kind) k=$(row.k) type $(row.type_index)")
                elseif !row.included
                    assert_close("non-included effective core row weight $(firstrow.core_kind) k=$(row.k) type $(row.type_index)",
                                 row.effective_weight, 0.0)
                    row.above_effective_threshold == false ||
                        error("Validation failed: non-included row marked above effective threshold")
                end
            end
        end
    end
    return true
end

function top_edge_effective_composition(df::DataFrame; n_per_side=5)
    rows = DataFrame[]
    for sub in groupby(df, [:edge_index, :side])
        sorted = sort(sub, [order(:conditional_share, rev=true), :type_index])
        push!(rows, first(sorted, min(n_per_side, nrow(sorted))))
    end
    return isempty(rows) ? DataFrame() : vcat(rows...; cols=:union)
end

function core_composition_selected(df::DataFrame; ks=(6, 5, 4))
    return sort(df[(in.(df.k, Ref(collect(ks)))) .& df.included, :],
                [:k, order(:conditional_share, rev=true), :type_index])
end

reverse_core_composition_selected(df::DataFrame; ks=(6, 5, 4)) =
    core_composition_selected(df; ks=ks)

above_edge_effective_composition(df::DataFrame) =
    sort(df[df.above_effective_threshold, :],
         [:edge_index, :side, order(:conditional_share, rev=true), :type_index])

function above_core_composition_selected(df::DataFrame; ks=(6, 5, 4))
    return sort(df[(in.(df.k, Ref(collect(ks)))) .& df.included .& df.above_effective_threshold, :],
                [:k, order(:conditional_share, rev=true), :type_index])
end

function edge_eff_row(df::DataFrame, winner::Symbol, loser::Symbol)
    rows = df[(df.winner .== winner) .& (df.loser .== loser), :]
    nrow(rows) == 1 || error("Expected exactly one effective edge row for $winner>$loser, found $(nrow(rows))")
    return rows[1, :]
end

function weak_edge_effective_type_text(tables)
    eff = tables["edge_effective_types"]
    lubo = edge_eff_row(eff, :Lu, :Bo)
    lute = edge_eff_row(eff, :Lu, :Te)
    if lute.opposition_effective_types > lubo.opposition_effective_types
        interpretation = "The contestation of Lu>Te is more type-plural than the contestation of Lu>Bo in this run. This supports the interpretation that the weak edge is not merely opposed by a single counter-graph bloc, but by a more heterogeneous collection of rankings."
    else
        interpretation = "In this run, the contestation of Lu>Te is not more type-plural than the contestation of Lu>Bo. The weak edge is therefore fragile mainly by margin, not by greater type diversity on the counter-majority side."
    end
    return """
For Lu>Bo, \\(N^+_{\\mathrm{eff}}\\) is $(@sprintf("%.3f", lubo.support_effective_types)) and \\(N^-_{\\mathrm{eff}}\\) is $(@sprintf("%.3f", lubo.opposition_effective_types)). For Lu>Te, \\(N^+_{\\mathrm{eff}}\\) is $(@sprintf("%.3f", lute.support_effective_types)) and \\(N^-_{\\mathrm{eff}}\\) is $(@sprintf("%.3f", lute.opposition_effective_types)). $(latex_escape(interpretation))
"""
end

function write_effective_report_selected_outputs!(tables, eff_dir)
    report_dir = joinpath(eff_dir, "report_selected")
    mkpath(report_dir)

    tables["report_selected/edge_effective_types_report"] =
        selected_columns(tables["edge_effective_types"], [
            :edge, :support_share_total, :support_effective_types,
            :support_effective_threshold, :support_n_above_effective_threshold,
            :opposition_share_total, :opposition_effective_types,
            :opposition_effective_threshold, :opposition_n_above_effective_threshold,
        ])
    tables["report_selected/core_effective_types_report"] =
        selected_columns(tables["core_effective_types"], [
            :k, :core_mass, :hhi, :effective_types, :effective_threshold,
            :n_above_effective_threshold, :above_effective_rankings,
        ])
    tables["report_selected/reverse_core_effective_types_report"] =
        selected_columns(tables["reverse_core_effective_types"], [
            :k, :reverse_core_mass, :hhi, :effective_types, :effective_threshold,
            :n_above_effective_threshold, :above_effective_rankings,
        ])
    tables["report_selected/edge_above_effective_threshold_types_report"] =
        selected_columns(above_edge_effective_composition(tables["edge_effective_type_composition"]), [
            :edge, :side, :type_index, :ranking, :conditional_share,
            :effective_threshold, :effective_weight,
        ])
    for key in ("report_selected/edge_effective_types_report",
                "report_selected/edge_above_effective_threshold_types_report")
        tables[key].edge = compact_edge_label.(tables[key].edge)
    end
    tables["report_selected/core_above_effective_threshold_types_k456_report"] =
        selected_columns(above_core_composition_selected(tables["core_effective_type_composition"]), [
            :k, :type_index, :ranking, :type_proportion, :coverage,
            :conditional_share, :effective_threshold, :effective_weight,
        ])
    tables["report_selected/reverse_core_above_effective_threshold_types_k456_report"] =
        selected_columns(above_core_composition_selected(tables["reverse_core_effective_type_composition"]), [
            :k, :type_index, :ranking, :type_proportion, :reverse_coverage,
            :conditional_share, :effective_threshold, :effective_weight,
        ])

    for name in [
        "edge_effective_types_report",
        "core_effective_types_report",
        "reverse_core_effective_types_report",
        "edge_above_effective_threshold_types_report",
        "core_above_effective_threshold_types_k456_report",
        "reverse_core_above_effective_threshold_types_k456_report",
    ]
        key = "report_selected/$name"
        write_table(joinpath(report_dir, "$name.csv"), tables[key])
    end
end

function run_effective_type_diagnostics!(
    tables::Dict{String,DataFrame},
    result::MajorityGraphSupportResult,
    output_dir::AbstractString,
)
    eff_dir = joinpath(output_dir, "effective_types")
    mkpath(eff_dir)

    diagnostics = effective_type_diagnostics(result)

    tables["edge_effective_types"] = diagnostics.edge_summary
    tables["edge_effective_type_composition"] = diagnostics.edge_composition
    tables["core_effective_types"] = diagnostics.core_summary
    tables["reverse_core_effective_types"] = diagnostics.reverse_core_summary
    tables["core_effective_type_composition"] = diagnostics.core_composition
    tables["reverse_core_effective_type_composition"] = diagnostics.reverse_core_composition

    write_table(joinpath(eff_dir, "edge_effective_types.csv"), diagnostics.edge_summary)
    write_table(joinpath(eff_dir, "edge_effective_type_composition.csv"), diagnostics.edge_composition)
    write_table(joinpath(eff_dir, "core_effective_types.csv"), diagnostics.core_summary)
    write_table(joinpath(eff_dir, "reverse_core_effective_types.csv"), diagnostics.reverse_core_summary)
    write_table(joinpath(eff_dir, "core_effective_type_composition.csv"), diagnostics.core_composition)
    write_table(joinpath(eff_dir, "reverse_core_effective_type_composition.csv"), diagnostics.reverse_core_composition)
    write_effective_report_selected_outputs!(tables, eff_dir)

    return diagnostics
end

function top_voter_type_roles(role_table::DataFrame; max_rows=24)
    mask = role_table.anchor .| role_table.counter_graph .| role_table.edge_breaker
    selected = role_table[mask, :]
    sorted = sort(selected, [:primary_role, order(:proportion, rev=true),
                            order(:max_breaking_score, rev=true), :type_index])
    nrow(sorted) <= max_rows && return sorted
    return first(sorted, max_rows)
end

function stack_partition_tables(tables, group_partitions, stem, cols)
    rows = DataFrame[]
    for partition in ROLE_REPORT_PARTITIONS
        partition in group_partitions || continue
        key = "$(stem)_$(partition)"
        haskey(tables, key) || continue
        push!(rows, selected_columns(add_partition_column(tables[key], partition), cols))
    end
    return isempty(rows) ? DataFrame() : vcat(rows...; cols=:union)
end

function write_role_report_selected_outputs!(tables, roles_dir, group_partitions)
    report_dir = joinpath(roles_dir, "report_selected")
    mkpath(report_dir)

    selected = top_voter_type_roles(tables["voter_type_roles"])
    write_role_table!(tables, "report_selected/top_voter_type_roles", selected, roles_dir)
    write_role_table!(tables, "report_selected/role_mass_summary_report",
                      copy(tables["role_mass_summary"]), roles_dir)
    write_role_table!(tables, "report_selected/primary_role_mass_summary_report",
                      copy(tables["primary_role_mass_summary"]), roles_dir)
    write_role_table!(tables, "report_selected/weakest_edge_breakers_report",
                      first(tables["weakest_edge_role_summary"],
                            min(10, nrow(tables["weakest_edge_role_summary"]))), roles_dir)

    power_cols = [
        :partition, :group, :group_mass, :anchoring, :conditional_anchoring,
        :edge, :edge_margin_contribution, :edge_support, :edge_support_share,
        :edge_breaking_score,
    ]
    primary_cols = [
        :partition, :group, :group_mass, :primary_role, :role_mass,
        :conditional_role_share,
    ]
    write_role_table!(tables, "report_selected/group_role_power_weakest_edge_report",
                      stack_partition_tables(tables, group_partitions, "group_role_power", power_cols),
                      roles_dir)
    write_role_table!(tables, "report_selected/group_primary_roles_report",
                      stack_partition_tables(tables, group_partitions, "group_primary_roles", primary_cols),
                      roles_dir)
end

function run_majority_graph_roles!(tables::Dict{String,DataFrame}, result::MajorityGraphSupportResult,
                                   group_results, group_partitions, roles_dir::AbstractString)
    mkpath(roles_dir)
    role_summary = graph_role_summary(result; thresholds=ROLE_THRESHOLDS,
                                      amenability=ROLE_AMENABILITY,
                                      lambda=ROLE_LAMBDA)
    role_table = compact_role_edge_labels!(role_summary.role_table)
    role_mass = role_summary.role_mass_summary
    primary_role_mass = role_summary.primary_role_mass_summary
    edge_roles = compact_role_edge_labels!(role_summary.edge_role_table)
    weakest_edge_roles = compact_role_edge_labels!(role_summary.weakest_edge_breakers)
    write_role_table!(tables, "voter_type_roles", role_table, roles_dir)
    write_role_table!(tables, "role_mass_summary", role_mass, roles_dir)
    write_role_table!(tables, "primary_role_mass_summary", primary_role_mass, roles_dir)
    write_role_table!(tables, "edge_type_roles", edge_roles, roles_dir)
    write_role_table!(tables, "weakest_edge_role_summary", weakest_edge_roles, roles_dir)

    for partition in group_partitions
        gsummary = group_graph_role_summary(group_results[partition], role_table;
                                            amenability=ROLE_AMENABILITY,
                                            lambda=ROLE_LAMBDA)
        write_role_table!(tables, "group_roles_$(partition)", gsummary.group_role_table, roles_dir)
        write_role_table!(tables, "group_primary_roles_$(partition)", gsummary.group_primary_role_table, roles_dir)
        write_role_table!(tables, "group_role_power_$(partition)",
                          compact_role_edge_labels!(gsummary.group_role_power_table), roles_dir)
    end
    write_role_report_selected_outputs!(tables, roles_dir, group_partitions)
    return role_summary
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

function ranking_list(df::DataFrame; n=4)
    nrow(df) == 0 && return "none"
    return join(latex_escape.(string.(first(df.ranking, min(n, nrow(df))))), "; ")
end

function role_interpretation_text(tables)
    roles = tables["voter_type_roles"]
    anchors = sort(roles[roles.anchor, :], [order(:proportion, rev=true), :type_index])
    counters = sort(roles[roles.counter_graph, :], [order(:proportion, rev=true), :type_index])
    weakest = tables["weakest_edge_role_summary"]
    weakest_edge = nrow(weakest) == 0 ? "NA" : string(weakest.edge[1])
    breakers = :edge_breaker_for_edge in propertynames(weakest) ?
        weakest[weakest.edge_breaker_for_edge, :] : weakest
    nrow(breakers) == 0 && (breakers = weakest)
    return """
The largest anchor type is $(ranking_list(anchors; n=1)). The largest counter-graph type is $(ranking_list(counters; n=1)). The default weakest-edge selector identifies $(latex_escape(weakest_edge)); its top local edge-breaker rows include $(ranking_list(breakers; n=4)).
"""
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
    role_selected = tables["report_selected/top_voter_type_roles"]
    weakest_edge_roles = tables["report_selected/weakest_edge_breakers_report"]
    group_role_power_report = tables["report_selected/group_role_power_weakest_edge_report"]
    group_primary_roles_report = tables["report_selected/group_primary_roles_report"]
    edge_effective_report = tables["report_selected/edge_effective_types_report"]
    core_effective_report = tables["report_selected/core_effective_types_report"]
    reverse_core_effective_report = tables["report_selected/reverse_core_effective_types_report"]
    edge_above_effective_threshold = tables["report_selected/edge_above_effective_threshold_types_report"]
    core_above_effective_threshold_k456 = tables["report_selected/core_above_effective_threshold_types_k456_report"]
    reverse_core_above_effective_threshold_k456 = tables["report_selected/reverse_core_above_effective_threshold_types_k456_report"]

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
    core_eff_cols, core_eff_heads = table_columns(core_effective_report, [
        (:k, "k"), (:core_mass, "core mass"), (:hhi, "HHI"),
        (:effective_types, "eff. types"), (:effective_threshold, "threshold"),
        (:n_above_effective_threshold, "n effective"),
        (:above_effective_rankings, "above-threshold types")
    ])
    reverse_core_eff_cols, reverse_core_eff_heads = table_columns(reverse_core_effective_report, [
        (:k, "k"), (:reverse_core_mass, "reverse core mass"), (:hhi, "HHI"),
        (:effective_types, "eff. types"), (:effective_threshold, "threshold"),
        (:n_above_effective_threshold, "n effective"),
        (:above_effective_rankings, "above-threshold types")
    ])
    edge_eff_cols, edge_eff_heads = table_columns(edge_effective_report, [
        (:edge, "edge"), (:support_share_total, "support share"),
        (:support_effective_types, "support eff. types"),
        (:support_effective_threshold, "support threshold"),
        (:support_n_above_effective_threshold, "support n effective"),
        (:opposition_share_total, "opposition share"),
        (:opposition_effective_types, "opposition eff. types"),
        (:opposition_effective_threshold, "opposition threshold"),
        (:opposition_n_above_effective_threshold, "opposition n effective")
    ])
    edge_above_eff_cols, edge_above_eff_heads = table_columns(edge_above_effective_threshold, [
        (:edge, "edge"), (:side, "side"), (:type_index, "type"),
        (:ranking, "ranking"), (:conditional_share, "conditional share"),
        (:effective_threshold, "threshold"), (:effective_weight, "effective weight")
    ])
    core_above_eff_cols, core_above_eff_heads = table_columns(core_above_effective_threshold_k456, [
        (:k, "k"), (:type_index, "type"), (:ranking, "ranking"),
        (:type_proportion, "profile share"), (:coverage, "K"),
        (:conditional_share, "conditional share"),
        (:effective_threshold, "threshold"), (:effective_weight, "effective weight")
    ])
    reverse_core_above_eff_cols, reverse_core_above_eff_heads = table_columns(reverse_core_above_effective_threshold_k456, [
        (:k, "k"), (:type_index, "type"), (:ranking, "ranking"),
        (:type_proportion, "profile share"), (:reverse_coverage, "reverse K"),
        (:conditional_share, "conditional share"),
        (:effective_threshold, "threshold"), (:effective_weight, "effective weight")
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
    role_mass_cols, role_mass_heads = table_columns(tables["role_mass_summary"], [
        (:role, "role"), (:n_types, "types"), (:mass, "mass"), (:share, "share")
    ])
    primary_role_cols, primary_role_heads = table_columns(tables["primary_role_mass_summary"], [
        (:primary_role, "primary role"), (:n_types, "types"), (:mass, "mass"), (:share, "share")
    ])
    role_selected_cols, role_selected_heads = table_columns(role_selected, [
        (:type_index, "type"), (:ranking, "ranking"), (:proportion, "share"),
        (:coverage, "K"), (:anchoring, "anchoring"),
        (:max_breaking_score, "max breaker"), (:max_breaking_edge, "max edge"),
        (:roles, "roles"), (:primary_role, "primary role")
    ])
    weakest_role_cols, weakest_role_heads = table_columns(weakest_edge_roles, [
        (:edge, "edge"), (:type_index, "type"), (:ranking, "ranking"),
        (:type_proportion, "share"), (:coverage, "K"),
        (:boundary_distance, "distance"), (:breaking_score, "breaker score"),
        (:edge_breaker_for_edge, "selected")
    ])
    group_role_power_cols, group_role_power_heads = table_columns(group_role_power_report, [
        (:partition, "partition"), (:group, "group"), (:group_mass, "group share"),
        (:anchoring, "anchoring"), (:conditional_anchoring, "cond. anchoring"),
        (:edge, "edge"), (:edge_margin_contribution, "margin contrib."),
        (:edge_support, "support"), (:edge_support_share, "support share"),
        (:edge_breaking_score, "breaker")
    ])
    group_primary_role_cols, group_primary_role_heads = table_columns(group_primary_roles_report, [
        (:partition, "partition"), (:group, "group"), (:group_mass, "group share"),
        (:primary_role, "primary role"), (:role_mass, "role mass"),
        (:conditional_role_share, "conditional share")
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

\\subsection{Effective type composition of support cores}

Mass and concentration answer different questions. The support core \\(\\gamma_k\\) measures the mass that supports at least \\(k\\) majority edges. The inverse-HHI effective type count gives the number of equally sized types that would produce the same concentration. To identify which concrete rankings carry that effective mass, I compare each conditional type share with the average effective-type share \\(1/N_{\\mathrm{eff}}\\). A type is above the effective threshold when \\(q_r > 1/N_{\\mathrm{eff}}\\), or equivalently when \\(N_{\\mathrm{eff}}q_r > 1\\). These are the rankings whose conditional mass is larger than one average effective type.

The core tables report the effective threshold and the rankings above that threshold inside each support or reverse core. This avoids reducing the core to its largest ranking and instead identifies the rankings that count as more than one average effective type.

$(latex_table(core_effective_report; caption="Effective type composition of support cores", label="tab:core-effective-types", columns=core_eff_cols, headers=core_eff_heads, resize=true))

\\subsection{Reverse support cores}

The reverse cores are the mirror image of the support cores. While \\(\\gamma_k\\) measures mass supporting at least \\(k\\) majority edges, \\(\\bar{\\gamma}_k\\) measures mass contesting at least \\(k\\) majority edges. Thus \\(\\bar{\\gamma}_6\\) is the mass that supports the full reverse of the majority graph. Effective type counts inside \\(\\bar{\\gamma}_k\\) distinguish compact counter-graph blocs from more heterogeneous opposition to the majority order.

$(latex_table(reverse_core_effective_report; caption="Effective type composition of reverse support cores", label="tab:reverse-core-effective-types", columns=reverse_core_eff_cols, headers=reverse_core_eff_heads, resize=true))

\\subsection{Effective type composition by edge}

For each majority edge \\(e\\), the support mass \\(s^+(e)\\) gives the size of the majority coalition, while \\(N^+_{\\mathrm{eff}}(e)\\) gives its effective diversity across ranking types. The opposition mass \\(s^-(e)\\) gives the size of the counter-majority coalition, while \\(N^-_{\\mathrm{eff}}(e)\\) gives its effective diversity. The edge-level table now reports both the effective number of types and the number of concrete rankings above the effective threshold on each side of the comparison. This separates three quantities: the size of the side, its concentration, and the identity of the types that effectively compose it.

$(latex_table(edge_effective_report; caption="Effective type composition by edge", label="tab:edge-effective-types", columns=edge_eff_cols, headers=edge_eff_heads, longtable=true, fontsize="scriptsize", landscape=true))

$(latex_table(edge_above_effective_threshold; caption="Above-threshold effective types by edge and side", label="tab:edge-above-effective-threshold-types", columns=edge_above_eff_cols, headers=edge_above_eff_heads, longtable=true, fontsize="tiny", landscape=true))

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

$(weak_edge_effective_type_text(tables))

$(latex_table(breaker_selected; caption="Top type breakers for Lu>Te, Lu>Bo, and Te>Ci", label="tab:type-breakers-selected", columns=breaker_cols, headers=breaker_heads, longtable=true, fontsize="scriptsize", landscape=true))

$(figure_tex("type_breakers_LuTe.png", "Type breakers for Lu>Te."; width="0.86\\linewidth"))

$(figure_tex("type_breakers_LuBo.png", "Type breakers for Lu>Bo."; width="0.86\\linewidth"))

\\section{Majority-graph role decomposition}

Voter types are classified relative to the induced majority graph. Roles are non-exclusive: anchors stabilize the graph; peripheral supporters align with it but carry less mass; edge breakers are locally close to reversing at least one edge; counter-graph types oppose much of the graph.

$(role_interpretation_text(tables))

$(latex_table(tables["role_mass_summary"]; caption="Non-exclusive majority-graph role mass summary", label="tab:role-mass-summary", columns=role_mass_cols, headers=role_mass_heads))

$(latex_table(tables["primary_role_mass_summary"]; caption="Primary majority-graph role mass summary", label="tab:primary-role-mass-summary", columns=primary_role_cols, headers=primary_role_heads))

$(latex_table(role_selected; caption="Selected voter-type roles", label="tab:selected-voter-type-roles", columns=role_selected_cols, headers=role_selected_heads, longtable=true, fontsize="scriptsize", landscape=true))

$(latex_table(weakest_edge_roles; caption="Weakest-edge role summary", label="tab:weakest-edge-role-summary", columns=weakest_role_cols, headers=weakest_role_heads, resize=true))

\\subsection{Group roles on the weakest edge}

Group power is decomposed into graph anchoring, support for the weakest edge, net contribution to that edge, and edge-breaking capacity. These columns should be read together rather than reduced to a single scalar.

$(latex_table(group_role_power_report; caption="Group role power on the weakest edge", label="tab:group-role-power-weakest-edge", columns=group_role_power_cols, headers=group_role_power_heads, longtable=true, fontsize="scriptsize", landscape=true))

The stacked group primary-role table is written as a report-ready CSV and left out of the main report because it is too long for this compact section.

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

\\section{Appendix: effective type composition tables}

$(latex_table(edge_above_effective_threshold; caption="Above-threshold effective type composition by edge and side", label="tab:edge-effective-composition-above-threshold", columns=edge_above_eff_cols, headers=edge_above_eff_heads, longtable=true, fontsize="tiny", landscape=true))

$(latex_table(core_above_effective_threshold_k456; caption="Above-threshold support core effective type composition for k=6,5,4", label="tab:core-effective-composition-above-threshold-k456", columns=core_above_eff_cols, headers=core_above_eff_heads, longtable=true, fontsize="tiny", landscape=true))

$(latex_table(reverse_core_above_effective_threshold_k456; caption="Above-threshold reverse core effective type composition for k=6,5,4", label="tab:reverse-core-effective-composition-above-threshold-k456", columns=reverse_core_above_eff_cols, headers=reverse_core_above_eff_heads, longtable=true, fontsize="tiny", landscape=true))

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
    roles_dir = joinpath(tables_dir, "roles")
    figures_dir = joinpath(opts.output, "figures")
    report_dir = joinpath(opts.output, "report")
    mkpath(tables_dir)
    mkpath(roles_dir)
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

    run_effective_type_diagnostics!(tables, result, opts.output)

    group_partitions = Symbol[]
    group_results = Dict{Symbol,GroupMajorityGraphSupportResult}()
    for partition in PARTITIONS
        partition in propertynames(df) || continue
        push!(group_partitions, partition)
        labels = safe_partition_labels(df, partition)
        gresult = group_majority_graph_support(profile, labels; basis=basis)
        group_results[partition] = gresult
        tables["group_edge_power_$(partition)"] = add_edge_label!(group_edge_power_table(gresult))
        tables["group_breakers_$(partition)"] = add_edge_label!(group_breaker_table(gresult))
        tables["group_anchor_$(partition)"] = group_anchor_table(gresult)
        tables["group_target_switch_$(partition)"] = group_target_switch_table(profile, labels, TARGET, OPPONENT;
                                                                               current_first_candidates=CURRENT_FIRST_CANDIDATES,
                                                                               basis=basis)
    end

    run_majority_graph_roles!(tables, result, group_results, group_partitions, roles_dir)

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
        did_validate_roles = validate_role_outputs(tables, group_partitions)
        did_validate_roles && println("2022 role validation checks passed.")
        did_validate_effective = validate_effective_type_diagnostics(result, tables)
        did_validate_effective && println("2022 effective-type validation checks passed.")
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
        "roles" => Dict(
            "high_mass_quantile" => ROLE_THRESHOLDS.high_mass_quantile,
            "high_coverage_slack" => ROLE_THRESHOLDS.high_coverage_slack,
            "counter_coverage_max" => ROLE_THRESHOLDS.counter_coverage_max,
            "breaker_quantile" => ROLE_THRESHOLDS.breaker_quantile,
            "fragile_edge_quantile" => ROLE_THRESHOLDS.fragile_edge_quantile,
            "amenability" => string(ROLE_AMENABILITY),
            "lambda" => ROLE_LAMBDA,
            "selected_weakest_edge" => weakest_edge_label(result),
            "tables_dir" => abspath(roles_dir),
            "tables" => [
                abspath(joinpath(roles_dir, "voter_type_roles.csv")),
                abspath(joinpath(roles_dir, "role_mass_summary.csv")),
                abspath(joinpath(roles_dir, "primary_role_mass_summary.csv")),
                abspath(joinpath(roles_dir, "edge_type_roles.csv")),
                abspath(joinpath(roles_dir, "weakest_edge_role_summary.csv")),
                abspath(joinpath(roles_dir, "report_selected", "top_voter_type_roles.csv")),
                abspath(joinpath(roles_dir, "report_selected", "group_role_power_weakest_edge_report.csv")),
                abspath(joinpath(roles_dir, "report_selected", "group_primary_roles_report.csv")),
            ],
        ),
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
