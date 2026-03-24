using Test
using Random
using DataFrames
import Preferences
const pp = Preferences

@testset "Profile basics" begin
    pool = pp.CandidatePool([:A, :B, :C])
    b1 = pp.StrictRank(pool, [1, 2, 3])
    b2 = pp.StrictRank(pool, [2, 1, 3])

    p = pp.Profile(pool, [b1, b2])
    @test pp.nballots(p) == 2
    @test pp.is_complete(p)
    @test pp.is_strict(p)
    @test typeof(p) == pp.Profile{typeof(b1)}
    @test pp.validate(p)
end

@testset "WeightedProfile basics and validation" begin
    pool = pp.CandidatePool([:A, :B, :C])
    b1 = pp.StrictRank(pool, [1, 2, 3])
    b2 = pp.StrictRank(pool, [2, 1, 3])
    wts = [0.3, 0.7]

    wp = pp.WeightedProfile(pool, [b1, b2], wts)
    @test pp.weights(wp) == wts
    @test pp.total_weight(wp) ≈ 1.0
    @test pp.validate(wp)

    wp_bad = pp.WeightedProfile(pool, [b1, b2], [-1.0, 1.0])
    @test_throws ArgumentError pp.validate(wp_bad)
end

@testset "Bootstrap utilities" begin
    pool = pp.CandidatePool([:A, :B, :C])
    b1 = pp.StrictRank(pool, [1, 2, 3])
    b2 = pp.StrictRank(pool, [2, 1, 3])
    p = pp.Profile(pool, [b1, b2])
    wp = pp.WeightedProfile(pool, [b1, b2], [1.0, 0.0])

    rng = Random.MersenneTwister(1)
    idx = pp.resample_indices(p; rng=rng, n=5)
    @test length(idx) == 5
    @test all((idx .>= 1) .& (idx .<= 2))

    idx_w = pp.resample_indices(wp; rng=rng, n=5)
    @test idx_w == fill(1, 5)

    counts = pp.bootstrap_counts(wp; rng=rng, n=10)
    @test counts == [10, 0]

    pb = pp.bootstrap(wp; rng=rng, n=3)
    @test pp.nballots(pb) == 3
end

@testset "Restriction" begin
    pool = pp.CandidatePool([:A, :B, :C])
    b1 = pp.StrictRank(pool, [1, 2, 3])
    b2 = pp.StrictRank(pool, [2, 1, 3])
    p = pp.Profile(pool, [b1, b2])

    subset = [:A, :C]
    (p2, new_pool, backmap) = pp.restrict(p, subset)
    @test length(new_pool) == 2
    @test backmap == [1, 3]
    @test pp.nballots(p2) == 2
    @test all(length(pp.to_perm(b)) == 2 for b in p2.ballots)
end

@testset "Uniform ballot types" begin
    pool = pp.CandidatePool([:A, :B, :C])
    b1 = pp.StrictRank(pool, [1, 2, 3])
    w1 = pp.WeakRank(pool, Dict(:A => 1))
    @test_throws ArgumentError pp.Profile(pool, [b1, w1])
end

@testset "Profile traits for weak ballots" begin
    pool = pp.CandidatePool([:A, :B, :C])
    w1 = pp.WeakRank(pool, Dict(:A => 1))
    p = pp.Profile(pool, [w1])
    @test !pp.is_complete(p)
    @test !pp.is_strict(p)
    @test pp.is_weak_order(p)
    @test pp.is_transitive(p)
end

@testset "build_profile_from_scores with custom score normalization" begin
    df = DataFrame(
        A = [10, 9, 96, missing, 4],
        B = [8, 9, 97, 1, missing],
    )

    eseb_score(v) = begin
        v === missing && return missing
        x = v isa Real ? Float64(v) : (v isa AbstractString ? (tryparse(Float64, strip(v)) === nothing ? missing : tryparse(Float64, strip(v))) : missing)
        x === missing && return missing
        x in (96.0, 97.0, 98.0, 99.0) && return missing
        (0.0 <= x <= 10.0) || return missing
        return x
    end

    p = pp.build_profile_from_scores(
        df,
        ["A", "B"],
        [:A, :B];
        weighted = false,
        allow_ties = true,
        allow_incomplete = true,
        score_normalizer = eseb_score,
    )

    @test pp.nballots(p) == 4
    meta = pp.profile_build_meta(p)
    @test meta !== nothing
    @test meta.total_rows == 5
    @test meta.kept_rows == 4
    @test meta.skipped_no_ranked == 1
    @test meta.skipped_incomplete == 0
end

@testset "Weighted diagnostics and zero-rank recovery" begin
    df = DataFrame(
        A = [10, missing, 96, missing, 7],
        B = [9, 8, 97, 5, 6],
        w = [1.0, 2.0, 3.0, -1.0, missing],
    )

    eseb_score(v) = begin
        v === missing && return missing
        x = v isa Real ? Float64(v) : missing
        x === missing && return missing
        x in (96.0, 97.0, 98.0, 99.0) && return missing
        (0.0 <= x <= 10.0) || return missing
        return x
    end

    p = pp.build_profile_from_scores(
        df,
        ["A", "B"],
        [:A, :B];
        weighted = true,
        weight_col = :w,
        allow_ties = true,
        allow_incomplete = false,
        score_normalizer = eseb_score,
    )

    @test pp.nballots(p) == 1
    @test pp.weights(p) == [1.0]

    meta = pp.profile_build_meta(p)
    @test meta !== nothing
    @test meta.skipped_no_ranked == 1
    @test meta.skipped_incomplete == 2
    @test meta.skipped_invalid_weight == 1
    @test meta.zero_rank_weight_mass ≈ 3.0
    @test meta.zero_rank_weight_missing == 0

    nav = pp.profile_ranksize_summary(p; k = 2, weighted = true, include_zero_rank = true)
    @test nav.total_mass ≈ 4.0
    @test nav.zero_rank.known
    @test nav.zero_rank.mass ≈ 3.0
    @test nav.zero_rank.proportion ≈ 0.75
end

@testset "Candidate matching and pattern proportions" begin
    df = DataFrame(LULA = [10, 8], CIRO_GOMES = [8, 8], BOLSONARO = [7, 9], peso = [1.0, 2.0])
    cols = pp.resolve_candidate_cols_from_set(df, ["LULA", "CIRO_GOMES", "BOLSONARO"], ["ciro gomes", "lula"])
    @test cols == ["CIRO_GOMES", "LULA"]

    p = pp.build_profile_from_scores(
        df,
        ["LULA", "CIRO_GOMES"],
        [Symbol("LULA"), Symbol("CIRO GOMES")];
        weighted = false,
    )
    tbl = pp.profile_pattern_proportions(p; weighted = false)
    @test Set(names(tbl)) == Set(["pattern", "mass", "proportion"])
    @test sum(tbl.proportion) ≈ 1.0
end

@testset "candidate_missingness_table" begin
    df = DataFrame(
        CIRO_GOMES = [1, missing, 3, missing],
        LULA = [missing, 2, 3, 4],
        BOLSONARO = [1, 2, 3, 4],
        peso = [1.0, 2.0, 1.0, 2.0],
    )

    tbl_u = pp.candidate_missingness_table(df, [:CIRO_GOMES, :LULA, :BOLSONARO];
                                           weighted = false)
    @test names(tbl_u) == ["candidate", "column", "missing_amount", "total_amount",
                           "missing_proportion", "missing_percent"]
    @test tbl_u.column == ["CIRO_GOMES", "LULA", "BOLSONARO"]
    @test tbl_u.candidate == ["CIRO GOMES", "LULA", "BOLSONARO"]
    @test tbl_u.missing_amount ≈ [2.0, 1.0, 0.0]
    @test tbl_u.total_amount ≈ [4.0, 4.0, 4.0]
    @test tbl_u.missing_proportion ≈ [0.5, 0.25, 0.0]
    @test tbl_u.missing_percent ≈ [50.0, 25.0, 0.0]

    tbl_input_order = pp.candidate_missingness_table(df, ["LULA", "CIRO_GOMES"];
                                                     weighted = false,
                                                     sort_desc = false)
    @test tbl_input_order.column == ["LULA", "CIRO_GOMES"]
    @test tbl_input_order.missing_amount ≈ [1.0, 2.0]

    tbl_w = pp.candidate_missingness_table(df, [:CIRO_GOMES, :LULA];
                                           weighted = true,
                                           weight_col = :peso)
    @test tbl_w.column == ["CIRO_GOMES", "LULA"]
    @test tbl_w.missing_amount ≈ [4.0, 1.0]
    @test tbl_w.total_amount ≈ [6.0, 6.0]
    @test tbl_w.missing_proportion ≈ [4 / 6, 1 / 6]
    @test tbl_w.missing_percent ≈ [100 * (4 / 6), 100 * (1 / 6)]

    @test_throws ArgumentError pp.candidate_missingness_table(df, [:MISSING_COL])
    @test_throws ArgumentError pp.candidate_missingness_table(df, [:LULA]; weighted = true)
    @test_throws ArgumentError pp.candidate_missingness_table(
        DataFrame(LULA = [1, missing], peso = [1.0, missing]),
        [:LULA];
        weighted = true,
        weight_col = :peso,
    )
    @test_throws ArgumentError pp.candidate_missingness_table(
        DataFrame(LULA = [1, missing], peso = [1.0, 0.0]),
        [:LULA];
        weighted = true,
        weight_col = :peso,
    )
    @test_throws ArgumentError pp.candidate_missingness_table(
        DataFrame(LULA = [1, missing], peso = [1.0, -1.0]),
        [:LULA];
        weighted = true,
        weight_col = :peso,
    )
end

@testset "candidate_missingness_table with sentinel codes" begin
    df = DataFrame(
        LULA = [10.0, 99.0, 5.0, 99.0],
        CIRO_GOMES = [99.0, 8.0, 7.0, 6.0],
        peso = [1.0, 2.0, 1.0, 2.0],
    )

    tbl_u = pp.candidate_missingness_table(df, [:LULA, :CIRO_GOMES];
                                           weighted = false,
                                           missing_codes = 99.0,
                                           sort_desc = false)
    @test tbl_u.column == ["LULA", "CIRO_GOMES"]
    @test tbl_u.missing_amount ≈ [2.0, 1.0]
    @test tbl_u.missing_proportion ≈ [0.5, 0.25]

    tbl_w = pp.candidate_missingness_table(df, [:LULA, :CIRO_GOMES];
                                           weighted = true,
                                           weight_col = :peso,
                                           missing_codes = [99.0])
    @test tbl_w.column == ["LULA", "CIRO_GOMES"]
    @test tbl_w.missing_amount ≈ [4.0, 1.0]
    @test tbl_w.total_amount ≈ [6.0, 6.0]
    @test tbl_w.missing_proportion ≈ [4 / 6, 1 / 6]
end

@testset "Profile linearization and strict measures" begin
    pool = pp.CandidatePool([:A, :B, :C])
    weak_ballots = [
        pp.WeakRank(pool, Dict(:A => 1, :B => 1, :C => 2)),
        pp.WeakRank(pool, Dict(:A => 2, :B => 1, :C => 1)),
    ]
    weak_profile = pp.Profile(pool, weak_ballots)

    strict_profile = pp.linearize(
        weak_profile;
        tie_break = :by_name,
        rng = Random.MersenneTwister(1),
    )
    @test pp.is_strict(strict_profile)
    @test pp.ranking_signature(strict_profile.ballots[1], pool) == (:A, :B, :C)
    @test pp.ranking_signature(strict_profile.ballots[2], pool) == (:B, :C, :A)

    abc = pp.StrictRank(pool, [:A, :B, :C])
    cba = pp.StrictRank(pool, [:C, :B, :A])
    @test pp.kendall_tau_distance(abc, abc) == 0
    @test pp.kendall_tau_distance(abc, cba) == 3

    unanim = pp.Profile(pool, [abc, abc, abc])
    @test pp.can_polarization(unanim) == 0.0
    @test pp.total_reversal_component(unanim) == 0.0
    @test pp.reversal_hhi(unanim) == 0.0
    @test pp.reversal_geometric(unanim) == 0.0

    eq = pp.Profile(pool, [
        pp.StrictRank(pool, [:A, :B, :C]),
        pp.StrictRank(pool, [:A, :C, :B]),
        pp.StrictRank(pool, [:B, :A, :C]),
        pp.StrictRank(pool, [:B, :C, :A]),
        pp.StrictRank(pool, [:C, :A, :B]),
        pp.StrictRank(pool, [:C, :B, :A]),
    ])
    @test pp.can_polarization(eq) == 1.0
end

@testset "Incomplete-ballot linearization policy" begin
    pool = pp.CandidatePool([:A, :B, :C])
    incomplete = pp.WeakRank(pool, Dict(:A => 1, :B => 1))

    @test_throws ArgumentError pp.linearize(
        incomplete;
        tie_break = :by_name,
        rng = Random.MersenneTwister(1),
        pool = pool,
    )

    preserved = pp.linearize(
        incomplete;
        tie_break = :by_name,
        rng = Random.MersenneTwister(1),
        pool = pool,
        incomplete_policy = :preserve,
    )
    @test preserved isa pp.WeakRank
    @test isequal(collect(pp.ranks(preserved)), Union{Missing,Int}[1, 2, missing])

    completed = pp.linearize(
        incomplete;
        tie_break = :by_name,
        rng = Random.MersenneTwister(1),
        pool = pool,
        incomplete_policy = :complete,
    )
    @test completed isa pp.StrictRank
    @test pp.ranking_signature(completed, pool) == (:A, :B, :C)

    preserved_profile = pp.linearize(
        pp.Profile(pool, [incomplete]);
        tie_break = :by_name,
        rng = Random.MersenneTwister(1),
        incomplete_policy = :preserve,
    )
    @test preserved_profile isa pp.Profile{<:pp.WeakRank}
    @test isequal(collect(pp.ranks(preserved_profile.ballots[1])), Union{Missing,Int}[1, 2, missing])
end
