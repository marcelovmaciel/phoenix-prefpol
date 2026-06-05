using Test
using DataFrames
using CategoricalArrays
using PrefPol

_unwrap_group(x) = ismissing(x) ? missing : unwrap(x)

@testset "LulaScoreGroup derived group semantics" begin
    @test PrefPol.LULA_SCORE_GROUP_LEVELS == ["low_lula", "medium_lula", "high_lula"]

    @test PrefPol.lula_score_group_value(missing) === missing
    @test PrefPol.lula_score_group_value(0) == "low_lula"
    @test PrefPol.lula_score_group_value(3) == "low_lula"
    @test PrefPol.lula_score_group_value(4) == "medium_lula"
    @test PrefPol.lula_score_group_value(6) == "medium_lula"
    @test PrefPol.lula_score_group_value(7) == "high_lula"
    @test PrefPol.lula_score_group_value(10) == "high_lula"

    @test PrefPol.lula_score_group_value(96) === missing
    @test PrefPol.lula_score_group_value(97) === missing
    @test PrefPol.lula_score_group_value(98) === missing
    @test PrefPol.lula_score_group_value(99) === missing
    @test PrefPol.lula_score_group_value("96") === missing
    @test PrefPol.lula_score_group_value("99") === missing

    @test PrefPol.lula_score_group_value(Inf) === missing
    @test PrefPol.lula_score_group_value(-Inf) === missing
    @test PrefPol.lula_score_group_value(NaN) === missing
    @test PrefPol.lula_score_group_value(-1) === missing
    @test PrefPol.lula_score_group_value(11) === missing
    @test PrefPol.lula_score_group_value(3.5) === missing
    @test PrefPol.lula_score_group_value(6.5) === missing

    @test PrefPol.lula_score_group_value("0") == "low_lula"
    @test PrefPol.lula_score_group_value(" 5 ") == "medium_lula"
    @test PrefPol.lula_score_group_value("10.0") == "high_lula"
    @test PrefPol.lula_score_group_value("bad") === missing
    @test PrefPol.lula_score_group_value(:bad) === missing

    categorical_scores = categorical(Union{Missing,String}["0", "5", "9", "bad", missing, "96"])
    @test isequal(
        PrefPol.lula_score_group_value.(categorical_scores),
        Union{Missing,String}["low_lula", "medium_lula", "high_lula", missing, missing, missing],
    )
end

@testset "LulaScoreGroup categorical columns" begin
    scores = Any[0, 3, 4, 6, 7, 10, 96, 97, 98, 99, missing, "bad", 11]
    col = PrefPol.lula_score_group_column(scores)

    @test col isa CategoricalVector
    @test isordered(col)
    @test levels(col) == PrefPol.LULA_SCORE_GROUP_LEVELS
    @test isequal(
        _unwrap_group.(col),
        Union{Missing,String}[
            "low_lula", "low_lula",
            "medium_lula", "medium_lula",
            "high_lula", "high_lula",
            missing, missing, missing, missing, missing, missing, missing,
        ],
    )

    df = DataFrame(Lula = scores, existing = 1:length(scores))
    returned = PrefPol.add_lula_score_group!(df)
    @test returned === df
    @test :LulaScoreGroup in propertynames(df)
    @test isordered(df.LulaScoreGroup)
    @test levels(df.LulaScoreGroup) == PrefPol.LULA_SCORE_GROUP_LEVELS
    @test isequal(_unwrap_group.(df.LulaScoreGroup), _unwrap_group.(col))

    PrefPol.add_lula_score_group!(df; source = :Lula, target = :custom_group)
    @test :custom_group in propertynames(df)
    @test levels(df.custom_group) == PrefPol.LULA_SCORE_GROUP_LEVELS
    @test isequal(_unwrap_group.(df.custom_group), _unwrap_group.(col))
end
