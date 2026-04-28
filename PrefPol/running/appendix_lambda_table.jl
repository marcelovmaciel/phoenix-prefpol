"""
    appendix_lambda_table.jl

Build the appendix table for the aggregate separation ratio:

    Lambda = (sum_g pi_g D_g) / (sum_g pi_g W_g)

where `W_g` is within-group dispersion, `D_g` is distance to outgroup
consensuses, and `pi_g` is the group-size share. Lambda is derived from the
same grouped C/D/S components as the paper heatmaps; it is not a primitive
measure, and the aggregate is not an average of group-level ratios.

The script reads the saved nested-pipeline cache manifest and derives Lambda
from cached C/D rows when lambda rows are not already present. It does not
rerun bootstrap, imputation, or linearization.

Run with:

    julia +1.11.9 --startup-file=no --project=PrefPol \
        PrefPol/running/appendix_lambda_table.jl
"""

using CSV
using DataFrames
using PrefPol
using Printf
using Statistics
import PrefPol as pp

const SMALL_OUTPUT_ROOT = joinpath(pp.project_root, "running", "output", "all_scenarios_small")
const MANIFEST_PATH = joinpath(SMALL_OUTPUT_ROOT, "run_manifest.csv")
const OUTPUT_DIR = joinpath(SMALL_OUTPUT_ROOT, "appendix_lambda_table")

# Matches the three main paper heatmap scenarios documented in
# `running/scenario_refactor_report.md`.
const TARGET_SPECS = [
    (wave_id = "2006", scenario_name = "main_2006"),
    (wave_id = "2018", scenario_name = "main_2018"),
    (wave_id = "2022", scenario_name = "main_2022"),
]

const TARGET_ANALYSIS_ROLE = "main"
const TARGET_IMPUTER_BACKEND = "mice"
const TARGET_LINEARIZER_POLICY = "pattern_conditional"

function target_key(row)
    return (string(row.wave_id), string(row.scenario_name))
end

function selected_manifest_rows(manifest::DataFrame)
    targets = Set((spec.wave_id, spec.scenario_name) for spec in TARGET_SPECS)
    rows = manifest[
        [target_key(row) in targets for row in eachrow(manifest)] .&
        (manifest.analysis_role .== TARGET_ANALYSIS_ROLE) .&
        (manifest.imputer_backend .== TARGET_IMPUTER_BACKEND) .&
        (manifest.linearizer_policy .== TARGET_LINEARIZER_POLICY),
        :,
    ]
    isempty(rows) && error("No manifest rows matched the appendix Lambda target specs.")
    return sort(rows, [:year, :scenario_name, :m])
end

function lambda_augmented_result(path::AbstractString)
    result = pp.load_pipeline_result(path)
    any(result.measure_cube.measure .== :lambda_sep) && return result
    return pp.augment_pipeline_result_with_lambda_sep(result; include_w = true)
end

function grouped_measure_values(result::pp.PipelineResult, measure::Symbol)
    cube = result.measure_cube
    rows = cube[(cube.measure .== measure) .& .!ismissing.(cube.grouping), :]
    isempty(rows) && error("Result $(result.cache_dir) has no grouped $(measure) rows.")
    return rows
end

function summarize_grouped_measure(result::pp.PipelineResult, measure::Symbol, value_name::Symbol)
    rows = grouped_measure_values(result, measure)
    out = combine(groupby(rows, :grouping), :value => median => value_name)
    rename!(out, :grouping => :Grouping)
    return out
end

function summarize_result(row)
    result = lambda_augmented_result(string(row.result_path))
    lambda = summarize_grouped_measure(result, :lambda_sep, :Lambda)
    W = summarize_grouped_measure(result, :W, :W)
    D = summarize_grouped_measure(result, :D, :D)
    S = any(result.measure_cube.measure .== :S) ?
        summarize_grouped_measure(result, :S, :S) :
        transform(innerjoin(W, D; on = :Grouping), [:D, :W] => ByRow(-) => :S)[:, [:Grouping, :S]]

    audit = innerjoin(innerjoin(innerjoin(lambda, W; on = :Grouping), D; on = :Grouping), S; on = :Grouping)
    audit[!, :Year] = fill(Int(row.year), nrow(audit))
    audit[!, :Scenario] = fill(string(row.scenario_name), nrow(audit))
    audit[!, :m] = fill(Int(row.m), nrow(audit))
    audit[!, :source_result_path] = fill(string(row.result_path), nrow(audit))
    select!(audit, :Year, :Scenario, :m, :Grouping, :Lambda, :W, :D, :S, :source_result_path)

    table = select(audit, :Year, :Scenario, :m, :Grouping, :Lambda)
    return table, audit
end

function format_lambda(value)
    ismissing(value) && return "NA"
    x = Float64(value)
    isnan(x) && return "NA"
    isinf(x) && return x > 0 ? "Inf" : "-Inf"
    return @sprintf("%.3f", x)
end

latex_escape(value) = replace(
    string(value),
    "\\" => "\\textbackslash{}",
    "_" => "\\_",
    "%" => "\\%",
    "&" => "\\&",
    "#" => "\\#",
)

function write_latex_table(path::AbstractString, table::DataFrame)
    open(path, "w") do io
        println(io, "\\begin{table}[!htbp]")
        println(io, "\\centering")
        println(io, "\\begin{tabular}{lllrl}")
        println(io, "\\hline")
        println(io, "Year & Scenario & m & Grouping & \$\\Lambda\$ \\\\")
        println(io, "\\hline")
        for row in eachrow(table)
            println(
                io,
                join((
                    latex_escape(row.Year),
                    latex_escape(row.Scenario),
                    latex_escape(row.m),
                    latex_escape(row.Grouping),
                    format_lambda(row.Lambda),
                ), " & "),
                " \\\\",
            )
        end
        println(io, "\\hline")
        println(io, "\\end{tabular}")
        println(io, "\\caption{Aggregate separation ratio by wave, scenario, candidate-set size, and grouping.}")
        println(io, "\\begin{flushleft}\\footnotesize")
        println(io, "\$\\Lambda\$ is the aggregate separation ratio, defined as ")
        println(io, "\$\\Lambda = (\\sum_g \\pi_g D_g)/(\\sum_g \\pi_g W_g)\$, where \$W_g\$ is within-group ")
        println(io, "dispersion, \$D_g\$ is average distance to outgroup consensuses, and \$\\pi_g\$ is the group-size share. ")
        println(io, "\$\\Lambda = 1\$ means no excess separation; \$\\Lambda = 1.5\$ means outgroup consensuses are ")
        println(io, "50 percent farther away than the ingroup consensus; \$\\Lambda = 2\$ means they are twice as far away.")
        println(io, "\\end{flushleft}")
        println(io, "\\end{table}")
    end
    return path
end

function main()
    isfile(MANIFEST_PATH) || error("Saved small-run manifest not found at $(MANIFEST_PATH).")
    manifest = CSV.read(MANIFEST_PATH, DataFrame)
    rows = selected_manifest_rows(manifest)

    tables = DataFrame[]
    audits = DataFrame[]
    for row in eachrow(rows)
        table, audit = summarize_result(row)
        push!(tables, table)
        push!(audits, audit)
    end

    table = sort(vcat(tables...), [:Year, :Scenario, :m, :Grouping])
    audit = sort(vcat(audits...), [:Year, :Scenario, :m, :Grouping])
    table_for_csv = transform(table, :Lambda => ByRow(format_lambda) => :Lambda_display)

    mkpath(OUTPUT_DIR)
    CSV.write(joinpath(OUTPUT_DIR, "appendix_lambda_table.csv"), table_for_csv)
    CSV.write(joinpath(OUTPUT_DIR, "appendix_lambda_audit.csv"), audit)
    write_latex_table(joinpath(OUTPUT_DIR, "appendix_lambda_table.tex"), table)

    println("Wrote appendix Lambda outputs to $(OUTPUT_DIR)")
    return nothing
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
