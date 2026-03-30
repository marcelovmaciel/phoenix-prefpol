using Dates
using Logging
using OrderedCollections: OrderedDict
using Printf

using PrefPol

global_logger(NullLogger())

const REPORT_ROOT = joinpath("PrefPol", "reports", "consensus_tie_audit")
const REPORT_FILE = joinpath("PrefPol", "reports", "consensus_tie_audit.org")
const CACHE_ROOT = joinpath(REPORT_ROOT, "_cache")
const CONFIG_DIR = joinpath("PrefPol", "config")
const YEARS = (2006, 2018, 2022)
const VARIANTS = Tuple(String.(PrefPol.DEFAULT_PIPELINE_IMPUTATION_VARIANTS))

const CASE_COLUMNS = (
    :year,
    :scenario,
    :m,
    :variant,
    :replicate,
    :demographic,
    :group,
    :group_mass,
    :n_minimizers,
    :is_tied,
    :candidate_tuple,
    :chosen_order,
)

const SUMMARY_METRIC_COLUMNS = (
    :tied_consensuses,
    :total_consensuses,
    :tie_rate,
    :max_n_minimizers,
)

function csv_escape(x)
    raw = x === nothing ? "" : string(x)
    escaped = replace(raw, "\"" => "\"\"")
    return "\"" * escaped * "\""
end

function format_cell(x)
    if x isa AbstractFloat
        return @sprintf("%.6f", x)
    end
    return string(x)
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

function org_table(columns::Tuple, rows::Vector)
    labels = String.(columns)
    rendered = Vector{Vector{String}}()
    push!(rendered, collect(labels))

    for row in rows
        push!(rendered, [format_cell(getproperty(row, col)) for col in columns])
    end

    widths = [maximum(length.(getindex.(rendered, i))) for i in eachindex(labels)]

    function fmt_row(cells)
        return "| " * join((rpad(cells[i], widths[i]) for i in eachindex(cells)), " | ") * " |"
    end

    header = fmt_row(labels)
    sep = "|-" * join((repeat("-", widths[i]) for i in eachindex(widths)), "-+-") * "-|"

    lines = String[header, sep]
    for cells in rendered[2:end]
        push!(lines, fmt_row(cells))
    end

    return join(lines, "\n")
end

function join_order(items)
    return join(string.(items), " > ")
end

function consensus_case(year::Int, scen::AbstractString, m::Int, variant::AbstractString,
                        rep::Int, dem::Symbol, group, result)
    chosen = [result.candidates[Int(idx)] for idx in result.consensus_perm]
    return (
        year = year,
        scenario = String(scen),
        m = m,
        variant = String(variant),
        replicate = rep,
        demographic = String(dem),
        group = string(group),
        group_mass = Float64(result.total_mass),
        n_minimizers = Int(result.n_minimizers),
        is_tied = Bool(result.is_tied_minimizer),
        candidate_tuple = join_order(result.candidates),
        chosen_order = join_order(chosen),
    )
end

function summarize_cases(cases::Vector, keys::Tuple)
    stats = OrderedDict{NTuple{length(keys),Any},Vector{Float64}}()

    for row in cases
        key = ntuple(i -> getproperty(row, keys[i]), length(keys))
        bucket = get!(stats, key) do
            [0.0, 0.0, 0.0]
        end
        bucket[2] += 1.0
        if row.is_tied
            bucket[1] += 1.0
        end
        bucket[3] = max(bucket[3], Float64(row.n_minimizers))
    end

    out = NamedTuple[]
    for (key, bucket) in stats
        base = NamedTuple{keys}(key)
        total = Int(bucket[2])
        tied = Int(bucket[1])
        row = merge(base, (
            tied_consensuses = tied,
            total_consensuses = total,
            tie_rate = total == 0 ? 0.0 : tied / total,
            max_n_minimizers = Int(bucket[3]),
        ))
        push!(out, row)
    end

    return out
end

function sort_rows(rows::Vector, columns::Tuple)
    return sort(rows; by = row -> ntuple(i -> getproperty(row, columns[i]), length(columns)))
end

function year_dirs(year::Int)
    root = joinpath(CACHE_ROOT, string(year))
    return (
        root = root,
        candidate_sets = joinpath(root, "candidate_sets"),
        boot = joinpath(root, "boot"),
        imputed = joinpath(root, "imputed"),
        weak = joinpath(root, "weak_profiles"),
        linearized = joinpath(root, "linearized_profiles"),
    )
end

function run_year(year::Int)
    cfg = PrefPol.load_election_cfg(joinpath(CONFIG_DIR, "$year.toml"))
    dirs = year_dirs(year)
    for dir in values(dirs)
        dir isa AbstractString && mkpath(dir)
    end

    candidate_sets = PrefPol.save_or_load_candidate_sets_for_year(
        cfg;
        dir = dirs.candidate_sets,
        overwrite = false,
        verbose = false,
    )

    boot = PrefPol.save_bootstrap(
        cfg;
        dir = dirs.boot,
        overwrite = false,
        quiet = true,
    )

    PrefPol.impute_bootstrap_to_files(
        boot.path;
        imp_dir = dirs.imputed,
        overwrite = false,
        variants = PrefPol.DEFAULT_PIPELINE_IMPUTATION_VARIANTS,
    )

    iy = PrefPol.load_imputed_year(year; dir = dirs.imputed)
    f3_entry = (cfg = cfg,)

    profiles = PrefPol.generate_profiles_for_year_streamed_from_index(
        year,
        f3_entry,
        iy;
        candidate_sets = candidate_sets,
        out_dir = dirs.weak,
        overwrite = false,
        variants = PrefPol.DEFAULT_PIPELINE_IMPUTATION_VARIANTS,
    )

    linearized = PrefPol.linearize_profiles_for_year_streamed_from_index(
        year,
        f3_entry,
        profiles;
        out_dir = dirs.linearized,
        overwrite = false,
    )

    cases = NamedTuple[]

    for (scen, m_map) in linearized
        for (m, slice) in m_map
            for variant in sort!(collect(keys(slice.paths)); by = string)
                for rep in eachindex(slice.paths[variant])
                    bundle = slice[variant, rep]
                    for dem in Symbol.(cfg.demographics)
                        grouped = PrefPol._group_row_indices(bundle, dem)
                        for (group, idxs) in grouped
                            subprofile = PrefPol._subset_profile(bundle.profile, idxs)
                            result = PrefPol.consensus_kendall(subprofile)
                            push!(cases, consensus_case(year, scen, m, String(variant), rep, dem, group, result))
                        end
                    end
                end
            end
        end
    end

    return cfg, cases
end

function report_section(title::AbstractString, columns::Tuple, rows::Vector)
    body = isempty(rows) ? "No rows.\n" : org_table(columns, rows) * "\n"
    return "** " * title * "\n" * body * "\n"
end

function top_tied_cases(cases::Vector; n::Int = 12)
    tied = filter(row -> row.is_tied, cases)
    ordered = sort(
        tied;
        by = row -> (-row.n_minimizers, -row.group_mass, row.year, row.m, row.scenario, row.variant, row.replicate, row.demographic, row.group),
    )
    return first(ordered, min(n, length(ordered)))
end

function build_report(all_cases::Vector)
    overall = sort_rows(summarize_cases(all_cases, (:year,)), (:year,))
    by_variant = sort_rows(summarize_cases(all_cases, (:year, :variant)), (:year, :variant))
    by_m = sort_rows(summarize_cases(all_cases, (:year, :m)), (:year, :m))
    by_scenario = sort_rows(summarize_cases(all_cases, (:year, :scenario)), (:year, :scenario))
    by_demographic = sort_rows(summarize_cases(all_cases, (:year, :demographic)), (:year, :demographic))
    tied_only = filter(row -> row.is_tied, all_cases)
    top_cases = top_tied_cases(all_cases)

    io = IOBuffer()
    println(io, "#+title: Consensus Tie Audit")
    println(io, "#+date: ", Dates.format(now(), dateformat"yyyy-mm-dd HH:MM:SS"))
    println(io)
    println(io, "* Summary")
    println(io, "- Julia version: =$(VERSION)=")
    println(io, "- Years analyzed: =$(collect(YEARS))=")
    println(io, "- Variants analyzed: =$(collect(VARIANTS))=")
    println(io, "- Pipeline path: exact cached path (candidate-set cache, disk bootstrap, disk imputation, streamed weak profiles, streamed linearization).")
    println(io, "- Tied cases CSV: =PrefPol/reports/consensus_tie_audit/tied_cases_all_years.csv=")
    println(io, "- Summary CSV root: =PrefPol/reports/consensus_tie_audit/=")
    println(io)
    println(io, report_section("Overall By Year", (:year, :tied_consensuses, :total_consensuses, :tie_rate, :max_n_minimizers), overall))
    println(io, report_section("By Year And Variant", (:year, :variant, :tied_consensuses, :total_consensuses, :tie_rate, :max_n_minimizers), by_variant))
    println(io, report_section("By Year And m", (:year, :m, :tied_consensuses, :total_consensuses, :tie_rate, :max_n_minimizers), by_m))
    println(io, report_section("By Year And Scenario", (:year, :scenario, :tied_consensuses, :total_consensuses, :tie_rate, :max_n_minimizers), by_scenario))
    println(io, report_section("By Year And Demographic", (:year, :demographic, :tied_consensuses, :total_consensuses, :tie_rate, :max_n_minimizers), by_demographic))
    println(io, report_section("Top Tied Cases", (:year, :scenario, :m, :variant, :replicate, :demographic, :group, :group_mass, :n_minimizers, :candidate_tuple, :chosen_order), top_cases))
    println(io, "* Counts")
    println(io, "- Total consensus evaluations: =$(length(all_cases))=")
    println(io, "- Tied consensus evaluations: =$(length(tied_only))=")
    println(io, "- Overall tie rate: =",
            isempty(all_cases) ? "0.0" : @sprintf("%.6f", length(tied_only) / length(all_cases)),
            "=")
    println(io, "- Production tie-breaking: =deterministic_pseudorandom_minimizer=. The representative consensus is selected deterministically from the full minimizing set using stable tie-break metadata when supplied, or otherwise the exact active ordered candidate tuple plus the compressed ballot multiset.")
    println(io, "- Diagnostic storage: the full minimizing set remains available on =ConsensusResult.all_minimizers=. =C= is unaffected by tie choice; =D= remains tie-dependent in principle, but no longer drifts across reruns from tie-breaking alone.")

    return String(take!(io))
end

function write_outputs(all_cases::Vector)
    mkpath(REPORT_ROOT)
    tied_cases = filter(row -> row.is_tied, all_cases)

    write_csv(joinpath(REPORT_ROOT, "all_cases_all_years.csv"), CASE_COLUMNS, all_cases)
    write_csv(joinpath(REPORT_ROOT, "tied_cases_all_years.csv"), CASE_COLUMNS, tied_cases)

    for year in YEARS
        year_cases = filter(row -> row.year == year, all_cases)
        write_csv(joinpath(REPORT_ROOT, "all_cases_$year.csv"), CASE_COLUMNS, year_cases)
        write_csv(joinpath(REPORT_ROOT, "tied_cases_$year.csv"), CASE_COLUMNS, filter(row -> row.is_tied, year_cases))
    end

    write_csv(
        joinpath(REPORT_ROOT, "summary_by_year.csv"),
        (:year, SUMMARY_METRIC_COLUMNS...),
        sort_rows(summarize_cases(all_cases, (:year,)), (:year,)),
    )
    write_csv(
        joinpath(REPORT_ROOT, "summary_by_year_variant.csv"),
        (:year, :variant, SUMMARY_METRIC_COLUMNS...),
        sort_rows(summarize_cases(all_cases, (:year, :variant)), (:year, :variant)),
    )
    write_csv(
        joinpath(REPORT_ROOT, "summary_by_year_m.csv"),
        (:year, :m, SUMMARY_METRIC_COLUMNS...),
        sort_rows(summarize_cases(all_cases, (:year, :m)), (:year, :m)),
    )
    write_csv(
        joinpath(REPORT_ROOT, "summary_by_year_scenario.csv"),
        (:year, :scenario, SUMMARY_METRIC_COLUMNS...),
        sort_rows(summarize_cases(all_cases, (:year, :scenario)), (:year, :scenario)),
    )
    write_csv(
        joinpath(REPORT_ROOT, "summary_by_year_demographic.csv"),
        (:year, :demographic, SUMMARY_METRIC_COLUMNS...),
        sort_rows(summarize_cases(all_cases, (:year, :demographic)), (:year, :demographic)),
    )

    open(REPORT_FILE, "w") do io
        write(io, build_report(all_cases))
    end
end

function main()
    all_cases = NamedTuple[]
    for year in YEARS
        _, cases = run_year(year)
        append!(all_cases, cases)
    end

    write_outputs(all_cases)

    println("Saved report: ", REPORT_FILE)
    println("Saved CSV root: ", REPORT_ROOT)
    println("Total cases: ", length(all_cases))
    println("Tied cases: ", count(row -> row.is_tied, all_cases))
end

main()
