using Test
using PrefPol
using DataFrames
using JLD2
using OrderedCollections: OrderedDict

function _have_permallows()
    try
        rcall = PrefPol._require_rcall!()
        return rcall.rcopy(Bool, rcall.reval("requireNamespace('PerMallows', quietly=TRUE)"))
    catch
        return false
    end
end

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
            :random => [
                DataFrame(A = [10, 6], B = [7, 8], C = [9, 5], Age = [20, 30]),
                DataFrame(A = [5, 8], B = [9, 4], C = [10, 7], Age = [25, 35]),
            ],
            :mice => [
                DataFrame(A = [10, 6], B = [7, 8], C = [9, 5], Age = [20, 30]),
                DataFrame(A = [5, 8], B = [9, 4], C = [10, 7], Age = [25, 35]),
            ],
        ),
    )

    profiles = PrefPol.generate_profiles_for_year(2022, f3_entry, imps_entry;
                                                  candidate_sets = candidate_sets)
    @test Set(keys(profiles["front"][2])) == Set((:zero, :mice))
    profs = profiles["front"][2][:zero]

    random_profiles = PrefPol.generate_profiles_for_year(2022, f3_entry, imps_entry;
                                                         candidate_sets = candidate_sets,
                                                         variants = (:random,))
    @test Tuple(keys(random_profiles["front"][2])) == (:random,)

    @test length(profs) == 2
    for bundle in profs
        @test collect(PrefPol.Preferences.candidates(bundle.profile.pool)) == [:C, :B]
        ranking_dicts = PrefPol.profile_to_ranking_dicts(bundle.profile)
        @test all(Set(keys(p)) == Set([:C, :B]) for p in ranking_dicts)
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

        iy = PrefPol.ImputedYear(2022, Dict(:zero => imp_paths,
                                           :random => imp_paths,
                                           :mice => imp_paths))
        weak_dir = joinpath(dir, "weak")
        random_weak_dir = joinpath(dir, "weak_random")
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
        @test Set(keys(weak_profiles["all"][3].paths)) == Set((:zero, :mice))
        weak_bundle = weak_profiles["all"][3][:zero, 1]
        stored_weak = first(values(JLD2.load(weak_profiles["all"][3].paths[:zero][1])))
        @test stored_weak isa DataFrame
        @test metadata(stored_weak, "profile_encoding") == "rank_vector_v1"
        @test !(eltype(stored_weak.profile) <: AbstractDict)

        PrefPol.generate_profiles_for_year_streamed_from_index(
            2022, f3_entry, iy;
            candidate_sets = candidate_sets,
            out_dir = random_weak_dir,
            variants = (:random,),
            overwrite = true,
        )
        random_weak_profiles = PrefPol.load_profiles_index(2022; dir = random_weak_dir)
        @test Tuple(keys(random_weak_profiles["all"][3].paths)) == (:random,)

        weak_rankings = PrefPol.profile_to_ranking_dicts(weak_bundle.profile)
        @test length(unique(values(weak_rankings[1]))) < 3
        @test_throws ArgumentError PrefPol.apply_measures_for_year(weak_profiles)

        PrefPol.linearize_profiles_for_year_streamed_from_index(
            2022, f3_entry, weak_profiles;
            out_dir = lin_dir,
            overwrite = true,
        )
        linearized_profiles = PrefPol.load_linearized_profiles_index(2022; dir = lin_dir)
        linearized_bundle = linearized_profiles["all"][3][:zero, 1]
        linearized_rankings = PrefPol.profile_to_ranking_dicts(linearized_bundle.profile)
        stored_linearized = first(values(JLD2.load(linearized_profiles["all"][3].paths[:zero][1])))
        @test stored_linearized isa DataFrame
        @test metadata(stored_linearized, "profile_encoding") == "rank_vector_v1"
        @test !(eltype(stored_linearized.profile) <: AbstractDict)

        @test PrefPol.Preferences.is_strict(linearized_bundle.profile)
        @test sort(collect(values(linearized_rankings[1]))) == [1, 2, 3]
        @test all(length(unique(values(p))) == 3 for p in linearized_rankings)

        measures = PrefPol.save_or_load_measures_for_year(
            2022, linearized_profiles;
            dir = meas_dir,
            overwrite = true,
            verbose = false,
        )
        @test haskey(measures["all"][3], Symbol("Ψ"))
        @test haskey(measures["all"][3], :calc_total_reversal_component)

        if _have_permallows()
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
        else
            @info "Skipping group-metrics smoke: R package `PerMallows` is unavailable."
        end
    end
end

@testset "compact artifact codec is materially smaller than serialized bundles" begin
    n = 250
    df = DataFrame(
        A = fill(10, n),
        B = fill(8, n),
        C = fill(6, n),
        Age = fill("x", n),
    )

    pdf = PrefPol.profile_dataframe(df; score_cols = [:A, :B, :C], demo_cols = [:Age], kind = :weak)
    metadata!(pdf, "candidates", [:A, :B, :C])
    metadata!(pdf, "profile_kind", "weak")
    bundle = PrefPol.dataframe_to_annotated_profile(pdf; ballot_kind = :weak)
    artifact = PrefPol.compact_profile_artifact_dataframe(bundle)

    mktempdir() do dir
        compact_path = joinpath(dir, "compact.jld2")
        bundle_path = joinpath(dir, "bundle.jld2")
        JLD2.@save compact_path artifact
        JLD2.@save bundle_path bundle
        @test stat(compact_path).size < stat(bundle_path).size
    end
end

@testset "linearization guard rejects incomplete weak orders" begin
    cfg = PrefPol.ElectionConfig(
        2022,
        "unused_loader",
        "/tmp/unused",
        3,
        [3],
        1,
        3,
        123,
        ["A", "B", "C"],
        ["Age"],
        [PrefPol.Scenario("all", String[])],
    )

    bad_df = DataFrame(
        profile = [Dict(:A => 1, :B => 1), Dict(:A => 1, :C => 1)],
        Age = [20, 30],
    )
    metadata!(bad_df, "candidates", [:A, :B, :C])
    metadata!(bad_df, "profile_kind", "weak")
    bad_bundle = PrefPol.dataframe_to_annotated_profile(bad_df)
    bad_artifact = PrefPol.compact_profile_artifact_dataframe(bad_bundle)

    mktempdir() do dir
        weak_path = joinpath(dir, "weak_bad.jld2")
        JLD2.@save weak_path artifact = bad_artifact

        profiles_year = OrderedDict(
            "all" => OrderedDict(
                3 => PrefPol.ProfilesSlice(2022, "all", 3, [:A, :B, :C], Dict(:zero => [weak_path])),
            ),
        )

        @test_throws ArgumentError PrefPol.linearize_profiles_for_year_streamed_from_index(
            2022,
            (cfg = cfg,),
            profiles_year;
            out_dir = joinpath(dir, "lin"),
            overwrite = true,
        )
    end
end

@testset "streamed linearization is reproducible under cfg.rng_seed" begin
    cfg = PrefPol.ElectionConfig(
        2022,
        "unused_loader",
        "/tmp/unused",
        3,
        [3],
        2,
        3,
        777,
        ["A", "B", "C"],
        ["Age"],
        [PrefPol.Scenario("all", String[])],
    )

    candidate_sets = OrderedDict("all" => ["A", "B", "C"])

    mktempdir() do dir
        imp = DataFrame(
            A = [10, 8, 5],
            B = [10, 7, 5],
            C = [6, 9, 1],
            Age = [20, 30, 40],
        )

        imp_paths = String[]
        for i in 1:2
            path = joinpath(dir, "imp_$(i).jld2")
            JLD2.@save path df = imp
            push!(imp_paths, path)
        end

        iy = PrefPol.ImputedYear(2022, Dict(:zero => imp_paths, :mice => imp_paths))
        weak_dir = joinpath(dir, "weak")
        lin_dir_1 = joinpath(dir, "lin1")
        lin_dir_2 = joinpath(dir, "lin2")

        PrefPol.generate_profiles_for_year_streamed_from_index(
            2022,
            (cfg = cfg, data = DataFrame[]),
            iy;
            candidate_sets = candidate_sets,
            out_dir = weak_dir,
            overwrite = true,
        )
        weak_profiles = PrefPol.load_profiles_index(2022; dir = weak_dir)

        lin1 = PrefPol.linearize_profiles_for_year_streamed_from_index(
            2022,
            (cfg = cfg,),
            weak_profiles;
            out_dir = lin_dir_1,
            overwrite = true,
        )
        lin2 = PrefPol.linearize_profiles_for_year_streamed_from_index(
            2022,
            (cfg = cfg,),
            weak_profiles;
            out_dir = lin_dir_2,
            overwrite = true,
        )

        ranks1 = PrefPol.profile_to_ranking_dicts(lin1["all"][3][:zero, 1].profile)
        ranks2 = PrefPol.profile_to_ranking_dicts(lin2["all"][3][:zero, 1].profile)
        @test ranks1 == ranks2
    end
end
