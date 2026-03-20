#!/usr/bin/env julia

using DataFrames
using CategoricalArrays
using Logging
using Random
import PrefPol
using RCall

const LOGGER = ConsoleLogger(stderr, Logging.Info)
global_logger(LOGGER)

Random.seed!(20260227)

function print_schema(df::DataFrame, label::String)
    println("\n=== Schema: $label ===")
    for nm in names(df)
        println("  ", nm, " :: ", eltype(df[!, nm]))
    end
end

function print_missing_counts(df::DataFrame, label::String)
    println("\n=== Missing Counts: $label ===")
    for nm in names(df)
        nmiss = count(ismissing, df[!, nm])
        println("  ", nm, " => ", nmiss)
    end
end

function print_categorical_ordering(df::DataFrame, label::String)
    println("\n=== Categorical Ordering: $label ===")
    for nm in names(df)
        col = df[!, nm]
        if eltype(col) <: Union{Missing, CategoricalValue}
            nonmiss = skipmissing(col)
            firstval = iterate(nonmiss)
            if firstval === nothing
                println("  ", nm, " => all missing categorical")
            else
                levs = levels(col)
                println("  ", nm, " => ordered=", isordered(col), ", levels=", levs)
            end
        end
    end
end

function build_test_df()
    n = 12

    score_num = Union{Missing, Float64}[1.0, missing, 3.5, 4.0, missing, 2.0, 5.0, missing, 1.5, 4.5, 3.0, missing]

    bin_raw = Union{Missing, String}["yes", "no", missing, "yes", "no", missing, "yes", "no", "yes", missing, "no", "yes"]
    score_bin = categorical(bin_raw; ordered=false)

    tri_raw = Union{Missing, String}["A", "B", "C", missing, "A", "B", missing, "C", "A", "B", missing, "C"]
    score_multicat = categorical(tri_raw; ordered=false)

    # Degenerate/constant column with missings.
    constant_num = Union{Missing, Int}[7, 7, 7, missing, 7, 7, 7, 7, missing, 7, 7, 7]

    # All-missing categorical column.
    all_missing_factor = categorical(Vector{Union{Missing, String}}(fill(missing, n)); ordered=false)

    # Non-numeric/non-factor type to exercise method fallback behavior.
    free_text = Union{Missing, String}["x", "x", "y", "z", missing, "x", "y", "z", "x", "y", missing, "z"]

    return DataFrame(
        score_num = score_num,
        score_bin = score_bin,
        score_multicat = score_multicat,
        constant_num = constant_num,
        all_missing_factor = all_missing_factor,
        free_text = free_text,
    )
end

function main()
    println("Julia VERSION: ", VERSION)
    println("Active project: ", Base.active_project())
    println("PrefPol module loaded: ", isdefined(Main, :PrefPol))

    println("\n=== GLOBAL_R_IMPUTATION Introspection ===")
    println("typeof(PrefPol.GLOBAL_R_IMPUTATION) = ", typeof(PrefPol.GLOBAL_R_IMPUTATION))
    println("PrefPol.GLOBAL_R_IMPUTATION isa Function = ", PrefPol.GLOBAL_R_IMPUTATION isa Function)

    df = build_test_df()

    println("\n=== Input Preview ===")
    show(stdout, MIME("text/plain"), first(df, min(8, nrow(df))))
    println()
    print_schema(df, "input")
    print_missing_counts(df, "input")
    print_categorical_ordering(df, "input")

    println("\n=== Running Single Imputation Call ===")
    RCall.reval("options(warn = 1)")

    try
        out = PrefPol.GLOBAL_R_IMPUTATION(df)
        println("\n=== Output Preview ===")
        show(stdout, MIME("text/plain"), first(out, min(8, nrow(out))))
        println()
        print_schema(out, "output")
        print_missing_counts(out, "output")
        print_categorical_ordering(out, "output")

        println("\n=== R mice Internals (Last Run) ===")
        RCall.reval("if (exists('meth')) { cat('meth:\\n'); print(meth) }")
        RCall.reval(raw"if (exists('imp'))  { cat('loggedEvents:\n'); print(imp$loggedEvents) }")
    catch err
        println("\n=== ERROR DURING IMPUTATION ===")
        showerror(stdout, err, catch_backtrace())
        println()
    end
end

main()
