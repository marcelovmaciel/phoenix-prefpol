using Dates
using Logging
using OrderedCollections: OrderedDict
using Printf
using Random

using PrefPol

global_logger(NullLogger())

const REPORT_ROOT = joinpath("PrefPol", "reports", "consensus_tie_sensitivity")
const REPORT_FILE = joinpath("PrefPol", "reports", "consensus_tie_sensitivity.org")
const TIE_AUDIT_ROOT = joinpath("PrefPol", "reports", "consensus_tie_audit")
const CACHE_ROOT = joinpath(TIE_AUDIT_ROOT, "_cache")
const CONFIG_DIR = joinpath("PrefPol", "config")
const YEARS = (2006, 2018, 2022)

const GROUP_COLUMNS = (
    :year,
    :scenario,
    :m,
    :variant,
    :replicate,
    :demographic,
    :group,
    :group_mass,
    :n_groups,
    :total_mass,
    :n_tied_groups,
    :n_minimizers,
    :baseline_D,
    :min_D,
    :max_D,
    :delta_D,
    :candidate_tuple,
    :baseline_order,
    :min_order,
    :max_order,
)

const SLICE_COLUMNS = (
    :year,
    :scenario,
    :m,
    :variant,
    :replicate,
    :demographic,
    :n_groups,
    :total_mass,
    :n_tied_groups,
    :baseline_D,
    :max_group_delta_D,
    :sum_group_delta_D,
    :n_groups_delta_ge_1e_4,
    :n_groups_delta_ge_1e_3,
    :n_groups_delta_ge_1e_2,
    :worst_group,
    :worst_group_mass,
    :worst_group_n_minimizers,
)

function csv_escape(x)
    raw = x === nothing ? "" : string(x)
    escaped = replace(raw, "\"" => "\"\"")
    return "\"" * escaped * "\""
end

function write_csv(path::AbstractString, columns::Tuple, rows::Vector)
    mkpath(dirname(path))
    open(path, "w") do io
        println(io, join((csv_escape(col) for col in columns), ","))
        for row in rows
            println(io, join((csv_escape(getproperty(row, col)) for col in columns), ","))
        end
    end
    return path
end

function format_cell(x)
    if x isa AbstractFloat
        return @sprintf("%.6f", x)
    end
    return string(x)
end

function org_table(columns::Tuple, rows::Vector)
    rendered = Vector{Vector{String}}()
    push!(rendered, collect(String.(columns)))
    for row in rows
        push!(rendered, [format_cell(getproperty(row, col)) for col in columns])
    end

    widths = [maximum(length.(getindex.(rendered, i))) for i in eachindex(columns)]

    function fmt_row(cells)
        return "| " * join((rpad(cells[i], widths[i]) for i in eachindex(cells)), " | ") * " |"
    end

    header = fmt_row(rendered[1])
    sep = "|-" * join((repeat("-", widths[i]) for i in eachindex(widths)), "-+-") * "-|"
    lines = String[header, sep]
    for row in rendered[2:end]
        push!(lines, fmt_row(row))
    end
    return join(lines, "\n")
end

join_order(items) = join(string.(items), " > ")

function year_dirs(year::Int)
    root = joinpath(CACHE_ROOT, string(year))
    return (
        root = root,
        linearized = joinpath(root, "linearized_profiles"),
    )
end

function strict_ballot_from_perm(pool, perm)
    return PrefPol.Preferences.StrictRank(pool, collect(Int.(perm)))
end

function candidate_tuple(profile)
    return Tuple(Symbol.(PrefPol.Preferences.candidates(profile.pool)))
end

function perm_to_order(candidates, perm)
    return join_order(candidates[Int(idx)] for idx in perm)
end

function sort_rows(rows::Vector, columns::Tuple)
    return sort(rows; by = row -> ntuple(i -> getproperty(row, columns[i]), length(columns)))
end

function summarize_rows(rows::Vector, keys::Tuple, metric_key::Symbol)
    buckets = OrderedDict{NTuple{length(keys),Any},Vector{Float64}}()

    for row in rows
        key = ntuple(i -> getproperty(row, keys[i]), length(keys))
        bucket = get!(buckets, key) do
            Float64[]
        end
        push!(bucket, Float64(getproperty(row, metric_key)))
    end

    out = NamedTuple[]
    for (key, vals) in buckets
        base = NamedTuple{keys}(key)
        push!(out, merge(base, (
            count = length(vals),
            max_value = maximum(vals),
            mean_value = sum(vals) / length(vals),
            median_value = sort(vals)[cld(length(vals), 2)],
        )))
    end

    return out
end

function slice_rows(group_rows::Vector)
    grouped = OrderedDict{NTuple{6,Any},Vector{NamedTuple}}()

    for row in group_rows
        key = (row.year, row.scenario, row.m, row.variant, row.replicate, row.demographic)
        push!(get!(grouped, key, NamedTuple[]), row)
    end

    out = NamedTuple[]
    for ((year, scenario, m, variant, replicate, demographic), rows) in grouped
        deltas = [row.delta_D for row in rows]
        worst = rows[argmax(deltas)]
        push!(out, (
            year = year,
            scenario = scenario,
            m = m,
            variant = variant,
            replicate = replicate,
            demographic = demographic,
            n_groups = rows[1].n_groups,
            total_mass = rows[1].total_mass,
            n_tied_groups = rows[1].n_tied_groups,
            baseline_D = rows[1].baseline_D,
            max_group_delta_D = maximum(deltas),
            sum_group_delta_D = sum(deltas),
            n_groups_delta_ge_1e_4 = count(>=(1e-4), deltas),
            n_groups_delta_ge_1e_3 = count(>=(1e-3), deltas),
            n_groups_delta_ge_1e_2 = count(>=(1e-2), deltas),
            worst_group = worst.group,
            worst_group_mass = worst.group_mass,
            worst_group_n_minimizers = worst.n_minimizers,
        ))
    end

    return out
end

function analyze_slice(year::Int, scenario::AbstractString, m::Int, variant::AbstractString, rep::Int,
                       demographic::Symbol, bundle)
    grouped_indices = PrefPol._group_row_indices(bundle, demographic)
    length(grouped_indices) > 1 || return NamedTuple[]

    groups = collect(keys(grouped_indices))
    profiles = OrderedDict{Any,Any}()
    for group in groups
        profiles[group] = PrefPol._subset_profile(bundle.profile, grouped_indices[group])
    end

    baseline_map = Dict{Any,Any}()
    results = Dict{Any,Any}()
    for group in groups
        result = PrefPol.consensus_kendall(profiles[group]; rng = MersenneTwister(1))
        results[group] = result
        baseline_map[group] = strict_ballot_from_perm(profiles[group].pool, result.all_minimizers[1])
    end

    tied_groups = [group for group in groups if results[group].is_tied_minimizer]
    isempty(tied_groups) && return NamedTuple[]

    total_mass = Float64(sum(PrefPol.Preferences.nballots(profiles[group]) for group in groups))
    baseline_D = PrefPol.overall_divergence(profiles, baseline_map)
    out = NamedTuple[]

    for group in tied_groups
        result = results[group]
        perms = result.all_minimizers
        Ds = Float64[]

        for perm in perms
            cmap = copy(baseline_map)
            cmap[group] = strict_ballot_from_perm(profiles[group].pool, perm)
            push!(Ds, PrefPol.overall_divergence(profiles, cmap))
        end

        min_idx = argmin(Ds)
        max_idx = argmax(Ds)
        cands = result.candidates
        push!(out, (
            year = year,
            scenario = String(scenario),
            m = m,
            variant = String(variant),
            replicate = rep,
            demographic = String(demographic),
            group = string(group),
            group_mass = Float64(PrefPol.Preferences.nballots(profiles[group])),
            n_groups = length(groups),
            total_mass = total_mass,
            n_tied_groups = length(tied_groups),
            n_minimizers = result.n_minimizers,
            baseline_D = baseline_D,
            min_D = minimum(Ds),
            max_D = maximum(Ds),
            delta_D = maximum(Ds) - minimum(Ds),
            candidate_tuple = join_order(cands),
            baseline_order = perm_to_order(cands, perms[1]),
            min_order = perm_to_order(cands, perms[min_idx]),
            max_order = perm_to_order(cands, perms[max_idx]),
        ))
    end

    return out
end

function analyze_year(year::Int)
    cfg = PrefPol.load_election_cfg(joinpath(CONFIG_DIR, "$year.toml"))
    dirs = year_dirs(year)
    linearized = PrefPol.load_linearized_profiles_index(year; dir = dirs.linearized)

    rows = NamedTuple[]
    for (scenario, m_map) in linearized
        for (m, slice) in m_map
            for variant in sort!(collect(keys(slice.paths)); by = string)
                for rep in eachindex(slice.paths[variant])
                    bundle = slice[variant, rep]
                    for demographic in Symbol.(cfg.demographics)
                        append!(rows, analyze_slice(year, scenario, m, String(variant), rep, demographic, bundle))
                    end
                end
            end
        end
    end

    return rows
end

function top_rows(rows::Vector; by, n::Int = 12)
    ordered = sort(rows; by = by)
    return first(ordered, min(n, length(ordered)))
end

function build_report(group_rows::Vector, slice_summary::Vector)
    total_tied_groups = length(group_rows)
    consequential_1e3 = count(row -> row.delta_D >= 1e-3, group_rows)
    consequential_1e2 = count(row -> row.delta_D >= 1e-2, group_rows)
    max_group = group_rows[argmax([row.delta_D for row in group_rows])]
    max_slice = slice_summary[argmax([row.max_group_delta_D for row in slice_summary])]

    by_year = sort_rows(summarize_rows(group_rows, (:year,), :delta_D), (:year,))
    by_m = sort_rows(summarize_rows(group_rows, (:year, :m), :delta_D), (:year, :m))
    by_dem = sort_rows(summarize_rows(group_rows, (:year, :demographic), :delta_D), (:year, :demographic))
    top_groups = top_rows(group_rows; by = row -> (-row.delta_D, -row.group_mass, row.year, row.m, row.scenario, row.variant, row.replicate, row.demographic, row.group))
    top_slices = top_rows(slice_summary; by = row -> (-row.max_group_delta_D, -row.sum_group_delta_D, row.year, row.m, row.scenario, row.variant, row.replicate, row.demographic))

    io = IOBuffer()
    println(io, "#+title: Consensus Tie Sensitivity Audit")
    println(io, "#+date: ", Dates.format(now(), dateformat"yyyy-mm-dd HH:MM:SS"))
    println(io)
    println(io, "* Scope")
    println(io, "- Julia version: =$(VERSION)=")
    println(io, "- Source cache: =PrefPol/reports/consensus_tie_audit/_cache/=")
    println(io, "- Method: one-group-at-a-time sensitivity. For each tied group in a slice, hold every other group consensus fixed at the first minimizer in the stored consensus set, then recompute overall =D= across that group's minimizing set.")
    println(io, "- This is not the full joint range over all tied groups in a slice. It isolates which tied groups matter individually.")
    println(io, "- Production tie-breaking: =deterministic_pseudorandom_minimizer=. Production runs now choose a stable representative from each minimizing set, so tie-breaking no longer creates run-to-run drift by itself.")
    println(io)
    println(io, "* Main Findings")
    println(io, "- Tied groups scanned: =$(total_tied_groups)=")
    println(io, "- Groups with =delta_D >= 1e-3=: =$(consequential_1e3)=")
    println(io, "- Groups with =delta_D >= 1e-2=: =$(consequential_1e2)=")
    println(io, "- Largest single-group sensitivity: year=$(max_group.year), scenario=$(max_group.scenario), m=$(max_group.m), variant=$(max_group.variant), replicate=$(max_group.replicate), demographic=$(max_group.demographic), group=$(max_group.group), mass=$(format_cell(max_group.group_mass)), delta_D=$(format_cell(max_group.delta_D)), n_minimizers=$(max_group.n_minimizers).")
    println(io, "- Largest slice-level maximum sensitivity: year=$(max_slice.year), scenario=$(max_slice.scenario), m=$(max_slice.m), variant=$(max_slice.variant), replicate=$(max_slice.replicate), demographic=$(max_slice.demographic), max_group_delta_D=$(format_cell(max_slice.max_group_delta_D)).")
    println(io)
    println(io, "* Sensitivity By Year")
    println(io, org_table((:year, :count, :max_value, :mean_value, :median_value), by_year))
    println(io)
    println(io, "* Sensitivity By Year And m")
    println(io, org_table((:year, :m, :count, :max_value, :mean_value, :median_value), by_m))
    println(io)
    println(io, "* Sensitivity By Year And Demographic")
    println(io, org_table((:year, :demographic, :count, :max_value, :mean_value, :median_value), by_dem))
    println(io)
    println(io, "* Most Consequential Tied Groups")
    println(io, org_table((:year, :scenario, :m, :variant, :replicate, :demographic, :group, :group_mass, :n_minimizers, :baseline_D, :min_D, :max_D, :delta_D), top_groups))
    println(io)
    println(io, "* Most Consequential Slices")
    println(io, org_table((:year, :scenario, :m, :variant, :replicate, :demographic, :n_tied_groups, :baseline_D, :max_group_delta_D, :sum_group_delta_D, :worst_group, :worst_group_mass), top_slices))
    println(io)
    println(io, "* Artifacts")
    println(io, "- =PrefPol/reports/consensus_tie_sensitivity/group_sensitivity_all_years.csv=")
    println(io, "- =PrefPol/reports/consensus_tie_sensitivity/slice_sensitivity_summary.csv=")
    println(io, "- =PrefPol/reports/consensus_tie_sensitivity.org=")

    return String(take!(io))
end

function main()
    group_rows = NamedTuple[]
    for year in YEARS
        append!(group_rows, analyze_year(year))
    end

    slice_summary = slice_rows(group_rows)

    mkpath(REPORT_ROOT)
    write_csv(joinpath(REPORT_ROOT, "group_sensitivity_all_years.csv"), GROUP_COLUMNS, group_rows)
    write_csv(joinpath(REPORT_ROOT, "slice_sensitivity_summary.csv"), SLICE_COLUMNS, slice_summary)

    open(REPORT_FILE, "w") do io
        write(io, build_report(group_rows, slice_summary))
    end

    println("Saved report: ", REPORT_FILE)
    println("Saved CSV root: ", REPORT_ROOT)
    println("Tied groups scanned: ", length(group_rows))
end

main()
