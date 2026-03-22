using Test
using PrefPol
using DataFrames
using OrderedCollections: OrderedDict

@testset "pipeline candidate-set cache" begin
    cfg = PrefPol.ElectionConfig(
        2022,
        "unused_loader",
        "/tmp/unused",
        3,
        [2],
        2,
        2,
        123,
        ["A", "B", "C"],
        ["Age"],
        [PrefPol.Scenario("front", ["C"])],
    )

    raw_df = DataFrame(
        A = [96, 1, 1],
        B = [1, 96, 1],
        C = [1, 1, 96],
        peso = [10.0, 1.0, 1.0],
        Age = [20, 30, 40],
    )

    candidate_sets = PrefPol.compute_global_candidate_sets(cfg; data = raw_df)
    @test candidate_sets["front"] == ["C", "B", "A"]

    mktempdir() do dir
        cached1 = PrefPol.save_or_load_candidate_sets_for_year(cfg;
                                                               data = raw_df,
                                                               dir = dir,
                                                               overwrite = true,
                                                               verbose = false)

        changed_df = DataFrame(
            A = [1, 1, 1],
            B = [96, 96, 96],
            C = [1, 1, 1],
            peso = [1.0, 1.0, 1.0],
            Age = [20, 30, 40],
        )

        cached2 = PrefPol.save_or_load_candidate_sets_for_year(cfg;
                                                               data = changed_df,
                                                               dir = dir,
                                                               overwrite = false,
                                                               verbose = false)
        @test cached2 == cached1
    end

    f3_entry = (
        cfg = cfg,
        data = [
            DataFrame(A = [96, 96], B = [1, 1], C = [1, 1], Age = [20, 30]),
            DataFrame(A = [1, 1], B = [96, 96], C = [1, 1], Age = [20, 30]),
        ],
    )

    imps_entry = (
        data = OrderedDict(
            :zero => [
                DataFrame(A = [10, 6], B = [7, 8], C = [9, 5], Age = [20, 30]),
                DataFrame(A = [5, 8], B = [9, 4], C = [10, 7], Age = [25, 35]),
            ],
        ),
    )

    profiles = PrefPol.generate_profiles_for_year(2022, f3_entry, imps_entry;
                                                  candidate_sets = candidate_sets)
    profs = profiles["front"][2][:zero]

    @test length(profs) == 2
    for df in profs
        @test DataFrames.metadata(df, "candidates") == [:C, :B]
        PrefPol.decode_profile_column!(df)
        @test all(Set(keys(p)) == Set([:C, :B]) for p in df.profile)
    end
end
