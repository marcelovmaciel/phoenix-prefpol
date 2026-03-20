using Test
using PrefPol
using DataFrames

@testset "raw_profiles wrappers" begin
    df = DataFrame(
        LULA = [10, 96, 8, missing],
        CIRO_GOMES = [8, 96, 8, 5],
        BOLSONARO = [7, 96, missing, 5],
        peso = [1.0, 2.0, -1.0, missing],
    )

    raw = (
        year = 2022,
        df = df,
        candidate_cols = ["LULA", "CIRO_GOMES", "BOLSONARO"],
        candidate_labels = ["LULA", "CIRO GOMES", "BOLSONARO"],
        weight_col = :peso,
    )

    @testset "candidate subset + unweighted build" begin
        prof = PrefPol.build_profile(
            raw;
            weighted = false,
            allow_ties = true,
            allow_incomplete = true,
            candidate_set = ["lula", "ciro gomes"],
        )

        @test collect(prof.pool.names) == [Symbol("LULA"), Symbol("CIRO GOMES")]
        @test PrefPol.Preferences.nballots(prof) == 3

        tbl = PrefPol.profile_pattern_proportions(prof; weighted = false)
        @test names(tbl) == ["pattern", "mass", "proportion"]

        props = Dict(String(r.pattern) => Float64(r.proportion) for r in eachrow(tbl))
        @test props["LULA>CIRO GOMES"] ≈ 1 / 3
        @test props["LULA~CIRO GOMES"] ≈ 1 / 3
        @test props["CIRO GOMES"] ≈ 1 / 3
    end

    @testset "weighted build + metadata recovery" begin
        prof_w = PrefPol.build_profile(
            raw;
            weighted = true,
            allow_ties = true,
            allow_incomplete = true,
            candidate_set = ["lula", "ciro gomes"],
        )

        @test PrefPol.Preferences.nballots(prof_w) == 1
        @test PrefPol.Preferences.weights(prof_w) == [1.0]

        meta = PrefPol.Preferences.profile_build_meta(prof_w)
        @test meta !== nothing
        @test meta.total_rows == 4
        @test meta.kept_rows == 1
        @test meta.skipped_no_ranked == 1
        @test meta.skipped_invalid_weight == 2
        @test meta.zero_rank_weight_mass ≈ 2.0

        nav = PrefPol.profile_ranksize_summary(
            prof_w;
            k = 2,
            weighted = true,
            include_zero_rank = true,
        )
        @test nav.total_mass ≈ 3.0
        @test nav.zero_rank.known
        @test nav.zero_rank.mass ≈ 2.0
        @test nav.zero_rank.proportion ≈ 2 / 3

        type_tbl = PrefPol.profile_ranking_type_proportions(
            prof_w;
            k = 2,
            weighted = true,
            include_zero_rank = true,
        )
        @test haskey(type_tbl.by_size, 2)
        @test haskey(type_tbl.by_size, 1)
    end
end
