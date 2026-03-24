using Test
using PrefPol
using DataFrames

@testset "profile adapter ballot-kind handling" begin
    df = DataFrame(
        profile = [
            Dict(:A => 1, :B => 2, :C => 3),
            Dict(:A => 1, :B => 1, :C => 2),
        ],
        grp = ["x", "y"],
    )
    metadata!(df, "candidates", [:A, :B, :C])

    @test_throws ArgumentError PrefPol.dataframe_to_annotated_profile(df)

    weak_bundle = PrefPol.dataframe_to_annotated_profile(df; ballot_kind = :weak)
    @test !PrefPol.Preferences.is_strict(weak_bundle.profile)
    @test PrefPol.Preferences.is_weak_order(weak_bundle.profile)

    strict_df = DataFrame(profile = [Dict(:A => 1, :B => 2, :C => 3)])
    metadata!(strict_df, "candidates", [:A, :B, :C])
    metadata!(strict_df, "profile_kind", "linearized")

    inferred = PrefPol.dataframe_to_annotated_profile(strict_df)
    @test PrefPol.Preferences.is_strict(inferred.profile)
    @test PrefPol.Preferences.asdict(inferred.profile.ballots[1], inferred.profile.pool) ==
          Dict(:A => 1, :B => 2, :C => 3)
end
