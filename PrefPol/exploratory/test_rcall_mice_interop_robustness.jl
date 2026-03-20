#!/usr/bin/env julia

using Test
using DataFrames
using CategoricalArrays
import PrefPol

function build_interop_df()
    n = 12

    score_num = Union{Missing, Float64}[
        1.0, missing, 3.5, 4.0, missing, 2.0, 5.0, missing, 1.5, 4.5, 3.0, missing
    ]

    score_bin = categorical(Union{Missing, String}[
        "yes", "no", missing, "yes", "no", missing, "yes", "no", "yes", missing, "no", "yes"
    ]; ordered=false)
    levels!(score_bin, ["no", "yes"])

    score_multicat = categorical(Union{Missing, String}[
        "A", "B", "C", missing, "A", "B", missing, "C", "A", "B", missing, "C"
    ]; ordered=false)
    levels!(score_multicat, ["A", "B", "C"])

    constant_num = Union{Missing, Int}[7, 7, 7, missing, 7, 7, 7, 7, missing, 7, 7, 7]

    all_missing_factor = categorical(Vector{Union{Missing, String}}(fill(missing, n)); ordered=false)

    free_text = Union{Missing, String}[
        "x", "x", "y", "z", missing, "x", "y", "z", "x", "y", missing, "z"
    ]

    return DataFrame(
        score_num = score_num,
        score_bin = score_bin,
        score_multicat = score_multicat,
        constant_num = constant_num,
        all_missing_factor = all_missing_factor,
        free_text = free_text,
    )
end

function run_checks()
    df = build_interop_df()
    reports = NamedTuple[]
    outputs = DataFrame[]

    for i in 1:3
        out = PrefPol.GLOBAL_R_IMPUTATION(df)
        rep = PrefPol.r_impute_mice_report(df)

        @test out isa DataFrame
        @test rep.completed isa DataFrame
        @test rep.meth isa Dict{String, String}
        @test rep.loggedEvents === nothing || rep.loggedEvents isa DataFrame
        @test rep.dropped_predictors isa Vector{String}

        @test names(out) == names(df)
        @test names(rep.completed) == names(df)
        @test nrow(out) == nrow(df)
        @test nrow(rep.completed) == nrow(df)

        @test eltype(out.score_num) <: Union{Missing, Real}
        @test eltype(out.constant_num) <: Union{Missing, Real}
        @test eltype(out.score_bin) <: Union{Missing, CategoricalValue, AbstractString}
        @test eltype(out.score_multicat) <: Union{Missing, CategoricalValue, AbstractString}

        @test get(rep.meth, "constant_num", "___MISSING___") == ""
        @test get(rep.meth, "all_missing_factor", "___MISSING___") == ""
        @test "constant_num" in rep.dropped_predictors
        @test "all_missing_factor" in rep.dropped_predictors

        @test count(ismissing, rep.completed.constant_num) == count(ismissing, df.constant_num)
        @test count(ismissing, rep.completed.all_missing_factor) == count(ismissing, df.all_missing_factor)

        if rep.loggedEvents === nothing
            @test rep.loggedEvents === nothing
        else
            @test rep.loggedEvents isa DataFrame
            le = rep.loggedEvents
            if nrow(le) > 0
                @test nrow(le) > 0
            else
                txt = lowercase(sprint(show, le))
                @test occursin("constant_num", txt) || occursin("all_missing_factor", txt)
            end
        end

        push!(outputs, out)
        push!(reports, rep)
        println("Run $i: ok")
    end

    first_keys = Set(keys(reports[1].meth))
    first_dropped = Set(reports[1].dropped_predictors)
    for rep in reports
        @test Set(keys(rep.meth)) == first_keys
        @test Set(rep.dropped_predictors) == first_dropped
    end

    println("OK: RCall <-> mice interop robustness checks passed.")
end

run_checks()
