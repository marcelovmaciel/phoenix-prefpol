using Test
using PrefPol
using DataFrames
using JLD2
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

@testset "streamed profile linearization is a separate stage" begin
    cfg = PrefPol.ElectionConfig(
        2022,
        "unused_loader",
        "/tmp/unused",
        3,
        [3],
        2,
        3,
        123,
        ["A", "B", "C"],
        ["Age"],
        [PrefPol.Scenario("all", String[])],
    )

    f3_entry = (cfg = cfg, data = DataFrame[])
    candidate_sets = OrderedDict("all" => ["A", "B", "C"])

    mktempdir() do dir
        imp1 = DataFrame(A = [10, 8], B = [10, 7], C = [6, 9], Age = [20, 30])
        imp2 = DataFrame(A = [5, 4], B = [5, 9], C = [1, 2], Age = [25, 35])

        imp_paths = String[]
        for (i, df) in enumerate((imp1, imp2))
            path = joinpath(dir, "imp_$(i).jld2")
            JLD2.@save path df
            push!(imp_paths, path)
        end

        iy = PrefPol.ImputedYear(2022, Dict(:zero => imp_paths))
        weak_dir = joinpath(dir, "weak")
        lin_dir = joinpath(dir, "linearized")
        meas_dir = joinpath(dir, "measures")
        group_dir = joinpath(dir, "group")

        PrefPol.generate_profiles_for_year_streamed_from_index(
            2022, f3_entry, iy;
            candidate_sets = candidate_sets,
            out_dir = weak_dir,
            overwrite = true,
        )
        weak_profiles = PrefPol.load_profiles_index(2022; dir = weak_dir)
        weak_df = weak_profiles["all"][3][:zero, 1]

        @test length(unique(values(weak_df.profile[1]))) < 3
        @test_throws ArgumentError PrefPol.apply_measures_for_year(weak_profiles)

        PrefPol.linearize_profiles_for_year_streamed_from_index(
            2022, f3_entry, weak_profiles;
            out_dir = lin_dir,
            overwrite = true,
        )
        linearized_profiles = PrefPol.load_linearized_profiles_index(2022; dir = lin_dir)
        linearized_df = linearized_profiles["all"][3][:zero, 1]

        @test sort(collect(values(linearized_df.profile[1]))) == [1, 2, 3]
        @test all(length(unique(values(p))) == 3 for p in linearized_df.profile)

        measures = PrefPol.save_or_load_measures_for_year(
            2022, linearized_profiles;
            dir = meas_dir,
            overwrite = true,
            verbose = false,
        )
        @test haskey(measures["all"][3], Symbol("Ψ"))
        @test haskey(measures["all"][3], :calc_total_reversal_component)

        group_metrics = PrefPol.save_or_load_group_metrics_for_year(
            2022, linearized_profiles, f3_entry;
            dir = group_dir,
            overwrite = true,
            verbose = false,
            two_pass = true,
        )
        @test haskey(group_metrics["all"][3], :Age)
        @test haskey(group_metrics["all"][3][:Age], :zero)
        @test haskey(group_metrics["all"][3][:Age][:zero], :C)
        @test haskey(group_metrics["all"][3][:Age][:zero], :D)
    end
end
