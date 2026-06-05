using Test
using DataFrames
using CategoricalArrays
import PrefPol

unwrap_or_missing(x) = ismissing(x) ? missing : unwrap(x)

import PrefPol: build_numbered_symbols, build_letter_symbols,
    trichotomize_ideology_value, trichotomize_ideology_column!,
    binarize_thermometer_value, binarize_thermometer_column!,
    categorical_from_column!, normalize_candidate_score_columns!,
    lula_score_group_value, LULA_SCORE_GROUP_LEVELS,
    _prepare_e2022_df!, _prepare_e2006_df!, _prepare_e2018_df!

@testset "year preprocessing recoder helpers" begin
    @test build_numbered_symbols("Q17_", 3) == [:Q17_1, :Q17_2, :Q17_3]
    @test build_numbered_symbols("Q16", 3; minwidth = 2) == [:Q1601, :Q1602, :Q1603]
    @test build_letter_symbols("eseb16", ['a', 'b', 'c']) == [:eseb16a, :eseb16b, :eseb16c]

    @test trichotomize_ideology_value(missing) === missing
    @test trichotomize_ideology_value(0) == -1
    @test trichotomize_ideology_value(3) == -1
    @test trichotomize_ideology_value(4) == 0
    @test trichotomize_ideology_value(6) == 0
    @test trichotomize_ideology_value(7) == 1
    @test trichotomize_ideology_value(10) == 1
    @test trichotomize_ideology_value(11) == 11
    @test trichotomize_ideology_value(99) == 99

    ideology = DataFrame(src = Any[0, 3, 4, 6, 7, 10, 95, missing])
    trichotomize_ideology_column!(ideology, :src, :Ideology; missing_codes = (95,))
    @test isequal(unwrap_or_missing.(ideology.Ideology), Union{Missing,Int}[-1, -1, 0, 0, 1, 1, 99, missing])

    @test binarize_thermometer_value(missing) === missing
    @test binarize_thermometer_value(0) == 0.0
    @test binarize_thermometer_value(3) == 0.0
    @test binarize_thermometer_value(4) == 0.0
    @test binarize_thermometer_value(5) == 1.0
    @test binarize_thermometer_value(6) == 1.0
    @test binarize_thermometer_value(7) == 1.0
    @test binarize_thermometer_value(10) == 1.0
    @test binarize_thermometer_value(11) == 99.0
    @test binarize_thermometer_value(96; missing_codes = (96,)) == 99.0

    thermometer = DataFrame(src = Any[0, 4, 5, 10, 11, 96, missing])
    binarize_thermometer_column!(thermometer, :src, :PT; missing_codes = (96,))
    @test isequal(thermometer.PT, Union{Missing,Float64}[0.0, 0.0, 1.0, 1.0, 99.0, 99.0, missing])

    score_df = DataFrame(A = Any[0, 10, 96, 11, missing], B = Any[3, 97, 4, -1, "5"])
    normalize_candidate_score_columns!(score_df, [:A, :B])
    @test isequal(score_df.A, Union{Missing,Float64}[0.0, 10.0, missing, missing, missing])
    @test isequal(score_df.B, Union{Missing,Float64}[3.0, missing, 4.0, missing, 5.0])

    categorical_df = DataFrame(src = [2, 1, 2])
    categorical_from_column!(categorical_df, :target, :src; ordered = true, levels = [1, 2])
    @test isordered(categorical_df.target)
    @test levels(categorical_df.target) == [1, 2]

    @test lula_score_group_value(0) == "low_lula"
    @test lula_score_group_value(3) == "low_lula"
    @test lula_score_group_value(4) == "medium_lula"
    @test lula_score_group_value(6) == "medium_lula"
    @test lula_score_group_value(7) == "high_lula"
    @test lula_score_group_value(10) == "high_lula"
    @test lula_score_group_value(96) === missing
    @test lula_score_group_value(11) === missing
end

@testset "synthetic 2022 preprocessing" begin
    candidates = ["A", "B", "C"]
    df = DataFrame(
        Q17_1 = Any[0, 96, 11, missing],
        Q17_2 = Any[3, 4, 97, "10"],
        Q17_3 = Any[98, 7, -1, 5],
        D10 = Any[99.0, 100.0, 96.0, 98.0],
        D02 = [1, 2, 1, 2],
        D12a = Any[1.0, 97.0, 98.0, 3.0],
        Q19 = Any[0.0, 4.0, 7.0, 95.0],
        Q18_5 = Any[4.0, 5.0, 96.0, 11.0],
        Q31_7 = Any[1.0, 97.0, 98.0, 2.0],
        D01A_FX_ID = [1, 2, 3, 4],
        D09a_FX_RENDAF = [1, 2, 3, 4],
        D03 = [1, 2, 3, 4],
    )

    out = _prepare_e2022_df!(df, candidates)
    @test all(in(names(out)).(candidates))
    @test isequal(out.A, Union{Missing,Float64}[0.0, missing, missing, missing])
    @test isequal(out.B, Union{Missing,Float64}[3.0, 4.0, missing, 10.0])
    @test isequal(out.C, Union{Missing,Float64}[missing, 7.0, missing, 5.0])
    @test unwrap_or_missing.(out.Religion) == [95.0, 95.0, 97.0, 99.0]
    @test unwrap_or_missing.(out.Sex) == [1, 2, 1, 2]
    @test unwrap_or_missing.(out.Race) == [1.0, 99.0, 99.0, 3.0]
    @test unwrap_or_missing.(out.Ideology) == [-1, 0, 1, 99]
    @test out.PT == [0.0, 1.0, 99.0, 99.0]
    @test out.Abortion == [1.0, 99.0, 99.0, 2.0]
    @test unwrap_or_missing.(out.Age) == [1, 2, 3, 4]
    @test unwrap_or_missing.(out.Income) == [1, 2, 3, 4]
    @test unwrap_or_missing.(out.Education) == [1, 2, 3, 4]
end

@testset "synthetic 2006 preprocessing" begin
    candidates = ["A", "B", "C", "D", "E", "F"]
    df = DataFrame(
        eseb16a = Any[0, 11, 77, 96, missing],
        eseb16b = Any[10, 5, 99, 3, 4],
        eseb16c = Any[66, 7, 8, 9, 10],
        eseb16d = Any[1, 2, 3, 4, 5],
        eseb16e = Any[6, 7, 8, 9, 10],
        eseb16f = Any[96, 97, 98, 99, 0],
        peso_1 = [1.0, 2.0, 3.0, 4.0, 5.0],
        SEXO = [1, 2, 1, 2, 1],
        eseb15a = Any[4.0, 5.0, 11.0, 77.0, missing],
        eseb19 = Any[0.0, 4.0, 7.0, 66.0, 77.0],
        FX_IDADE = [1, 2, 3, 4, 5],
        instru = [1, 2, 3, 4, 5],
        renda1 = Any[1, 2, missing, 4, 5],
    )

    out = _prepare_e2006_df!(df, candidates)
    @test all(in(names(out)).(candidates))
    @test isequal(out.A, Union{Missing,Float64}[0.0, missing, missing, missing, missing])
    @test isequal(out.C, Union{Missing,Float64}[missing, 7.0, 8.0, 9.0, 10.0])
    @test isequal(out.F, Union{Missing,Float64}[missing, missing, missing, missing, 0.0])
    @test out.peso == out.peso_1
    @test unwrap_or_missing.(out.Sex) == [1, 2, 1, 2, 1]
    @test isequal(out.PT, Union{Missing,Float64}[0.0, 1.0, 99.0, 99.0, missing])
    @test unwrap_or_missing.(out.Ideology) == [-1, 0, 1, 99, 99]
    @test unwrap_or_missing.(out.Age) == [1, 2, 3, 4, 5]
    @test unwrap_or_missing.(out.Education) == [1, 2, 3, 4, 5]
    @test unwrap_or_missing.(out.Income) == [1, 2, 10, 4, 5]
end

@testset "synthetic 2018 preprocessing" begin
    candidates = ["Cand$(i)" for i in 1:21]
    candidates[1] = "Lula"
    cols = Dict{Symbol,Any}(
        sym => Any[mod1(i, 10), 96.0, 11.0, 10.0]
        for (i, sym) in enumerate(build_numbered_symbols("Q16", 21; minwidth = 2))
    )
    df = DataFrame(cols)
    df.D10 = Any[1.0, 97.0, 98.0, missing]
    df.D2_SEXO = [1, 2, 1, 2]
    df.D12A = Any[1.0, 8.0, 9.0, 3.0]
    df.Q18 = Any[0.0, 4.0, 7.0, 95.0]
    df.Q1513 = Any[4.0, 5.0, 96.0, 11.0]
    df.D1A_FAIXAID = [1, 2, 3, 4]
    df.D3_ESCOLA = [1, 2, 3, 4]
    df.D9B_FAIXA_RENDAF = Any[1, 2, missing, 4]

    out = _prepare_e2018_df!(df, candidates)
    @test all(in(names(out)).(candidates))
    @test isequal(out.Lula, Union{Missing,Float64}[1.0, missing, missing, 10.0])
    @test isequal(out.Cand2, Union{Missing,Float64}[2.0, missing, missing, 10.0])
    @test isequal(unwrap_or_missing.(out.LulaScoreGroup), Union{Missing,String}["low_lula", missing, missing, "high_lula"])
    @test isordered(out.LulaScoreGroup)
    @test levels(out.LulaScoreGroup) == LULA_SCORE_GROUP_LEVELS
    @test isequal(unwrap_or_missing.(out.Religion), Union{Missing,Float64}[1.0, 96.0, 99.0, missing])
    @test unwrap_or_missing.(out.Sex) == [1, 2, 1, 2]
    @test out.Race == [1.0, 9.0, 9.0, 3.0]
    @test unwrap_or_missing.(out.Ideology) == [-1, 0, 1, 99]
    @test out.PT == [0.0, 1.0, 99.0, 99.0]
    @test unwrap_or_missing.(out.Age) == [1, 2, 3, 4]
    @test unwrap_or_missing.(out.Education) == [1, 2, 3, 4]
    @test unwrap_or_missing.(out.Income) == [1, 2, 10, 4]
end
