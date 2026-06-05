using Test
using DataFrames
using PrefPol

import PrefPol: compute_weighted_dont_know_her, prepare_scores_for_imputation_int

@testset "ESEB score semantics" begin
    @testset "scalar normalization" begin
        valid_cases = Any[0, 10, 5.5, "5"]
        @test normalize_eseb_score(missing) === missing
        @test normalize_eseb_score("5") == 5.0
        @test normalize_eseb_score(0) == 0.0
        @test normalize_eseb_score(10) == 10.0
        @test normalize_eseb_score(5.5) == 5.5

        for x in Any[96, 97, 98, 99, 96.0, 97.0, 98.0, 99.0, Inf, -Inf, NaN, -1, 11, "abc", "96"]
            @test normalize_eseb_score(x) === missing
        end

        agreement_cases = Any[missing, 0, 10, 5.5, 96, 99.0, Inf, NaN, -1, 11, "5", "96", "abc"]
        @test all(is_eseb_missing_score(x) == (normalize_eseb_score(x) === missing) for x in agreement_cases)
    end

    @testset "DataFrame normalization" begin
        df = DataFrame(
            A = Any[1, "5", 96, Inf, "abc"],
            B = Any[0, 10, 11, 99, missing],
            keep = 1:5,
        )

        one_col = copy(df)
        normalize_eseb_score_column!(one_col, :A)
        @test isequal(one_col.A, Union{Missing,Float64}[1.0, 5.0, missing, missing, missing])
        @test eltype(one_col.A) <: Union{Missing,Float64}
        @test one_col.keep == df.keep

        many_cols = copy(df)
        normalize_eseb_score_columns!(many_cols, [:A, :B])
        @test isequal(many_cols.A, Union{Missing,Float64}[1.0, 5.0, missing, missing, missing])
        @test isequal(many_cols.B, Union{Missing,Float64}[0.0, 10.0, missing, missing, missing])
        @test all(eltype(many_cols[!, c]) <: Union{Missing,Float64} for c in [:A, :B])
    end

    @testset "weighted missingness uses canonical semantics" begin
        df = DataFrame(
            A = Any[96, Inf, 5, missing],
            B = Any[1, 2, 3, 4],
            C = Any[NaN, 1, 2, 3],
            peso = [1.0, 2.0, 4.0, 8.0],
        )
        dkh = compute_weighted_dont_know_her(df, ["A", "B", "C"]; weights = df.peso)
        @test first.(dkh) == ["B", "C", "A"]
        @test isapprox(dkh[1][2], 0.0; atol = 1e-12)
        @test isapprox(dkh[2][2], 100 / 15; atol = 1e-12)
        @test isapprox(dkh[3][2], 1100 / 15; atol = 1e-12)

        ordinary = DataFrame(
            A = [96, 1, 1, 1],
            B = [1, 96, 1, 1],
            C = [96, 1, 1, 1],
            peso = [10.0, 1.0, missing, -2.0],
        )
        ordinary_dkh = compute_weighted_dont_know_her(ordinary, ["A", "B", "C"]; weights = ordinary.peso)
        @test ordinary_dkh[1][1] == "B"
        @test isapprox(ordinary_dkh[1][2], 100 / 11; atol = 1e-12)
        @test ordinary_dkh[2][1] == "A"
        @test isapprox(ordinary_dkh[2][2], 1000 / 11; atol = 1e-12)
        @test ordinary_dkh[3][1] == "C"
        @test isapprox(ordinary_dkh[3][2], 1000 / 11; atol = 1e-12)
    end

    @testset "candidate normalization leaves grouping columns untouched" begin
        df = DataFrame(
            A = Any[0, 5, 96, 11],
            B = Any[10, 97, -1, 3],
            Ideology = [-1, 0, 1, 99],
            PT = [-1, 0, 1, 99],
            Religion = [95, 96, 97, 99],
            Sex = [1, 2, 1, 2],
            Race = [1, 2, 3, 99],
            Abortion = [1, 2, 99, 1],
            LulaScoreGroup = ["low_lula", "medium_lula", "high_lula", missing],
            Age = [1, 2, 3, 4],
            Income = [1, 2, 99, 10],
            Education = [1, 2, 3, 4],
        )
        grouping_cols = [:Ideology, :PT, :Religion, :Sex, :Race, :Abortion, :LulaScoreGroup, :Age, :Income, :Education]
        original_groups = Dict(col => copy(df[!, col]) for col in grouping_cols)

        normalize_eseb_score_columns!(df, [:A, :B])

        @test isequal(df.A, Union{Missing,Float64}[0.0, 5.0, missing, missing])
        @test isequal(df.B, Union{Missing,Float64}[10.0, missing, missing, 3.0])
        @test all(isequal(df[!, col], original_groups[col]) for col in grouping_cols)
    end

    @testset "imputation integer preparation normalizes first" begin
        df = DataFrame(
            A = Any[0, "5", 10.0, "96", "abc", 11, Inf, -1, missing],
            B = Any[1, 2, 3, 97, 98.0, 99, -Inf, NaN, "10"],
            extra = collect('a':'i'),
        )

        prepared = prepare_scores_for_imputation_int(df, ["A", "B"]; extra_cols = ["extra"])
        @test eltype(prepared.A) <: Union{Missing,Int}
        @test eltype(prepared.B) <: Union{Missing,Int}
        @test isequal(prepared.A, Union{Missing,Int}[0, 5, 10, missing, missing, missing, missing, missing, missing])
        @test isequal(prepared.B, Union{Missing,Int}[1, 2, 3, missing, missing, missing, missing, missing, 10])
        @test prepared.extra == df.extra

        with_bad = DataFrame(A = [1, 2], Bad = [Dict(:x => 1), Dict(:x => 2)])
        @test_logs (:warn, r"skipping non-normalizable") begin
            skipped = prepare_scores_for_imputation_int(with_bad, ["A", "Bad"])
            @test names(skipped) == ["A"]
            @test isequal(skipped.A, Union{Missing,Int}[1, 2])
        end
    end
end
