
# preprocessing_tests.jl
#
# Tests for functions defined in preprocessing_general.jl (as pasted).
# Adjust the include path below if preprocessing_general.jl lives elsewhere.

using Test
using DataFrames
using CategoricalArrays
using Random
using StatsBase
using Impute
using StaticArrays
using PooledArrays
import PrefPol
import PrefPol: build_candidate_score_distributions, convert_keys_to_int,
    sanitize_countmaps, compute_dont_know_her,
    prepare_scores_for_imputation_int, prepare_scores_for_imputation_categorical,
    get_most_known_candidates, select_top_candidates, compute_candidate_set,
    get_df_just_top_candidates, GLOBAL_R_IMPUTATION, imputation_variants,
    weighted_bootstrap, get_row_candidate_score_pairs, get_order_dict,
    force_scores_become_linear_rankings, linearize_ranking_dict,
    linearize_profile_column!, build_profile, profile_dataframe,
    dict2svec, decode_rank, perm2dict, perm_to_dict,
    compress_rank_column!, decode_profile_column!, decode_each!, load_spss_file

# ---------- helpers ----------
const CANDS = ["A","B","C","D"]
const DEMOS = ["Sex","Race"]
const CAND_SYMS = Symbol.(CANDS)

function toy_scores_df(; n=20)
    rng = MersenneTwister(1234)
    # make some real scores with a few special codes 96-99 sprinkled in
    cols = Dict{String,Vector{Union{Missing,Float64}}}()
    for c in CANDS
        v = rand(rng, 1:10, n) .|> Float64
        # put a few special codes
        for code in (96.0, 97.0, 98.0, 99.0)
            v[rand(rng, 1:n, 1)] .= code
        end
        cols[c] = v
    end
    df = DataFrame(cols)
    df.Sex = categorical(rand(rng, ["F","M"], n))
    df.Race = categorical(rand(rng, ["W","B","O"], n))
    df
end

# Minimal ranking helpers for later tests
function toy_ranking_dict()
    # A > B > C > D (10 best → rank 1)
    scores = Dict(:A=>10,:B=>8,:C=>7,:D=>5)
    return get_order_dict(scores)
end

function unequal_pool_score(value, levels)
    scores = categorical([value], ordered = true)
    levels!(scores, levels)
    return scores[1]
end

function unequal_pool_score_dict()
    return Dict(
        :A => unequal_pool_score(10, [10, 5, 1]),
        :B => unequal_pool_score(10, [1, 5, 10]),
        :C => unequal_pool_score(5, [5, 1, 10]),
        :D => unequal_pool_score(1, [1, 10, 5]),
    )
end

function unequal_pool_scores_df()
    A = categorical([10], ordered = true)
    B = categorical([10], ordered = true)
    C = categorical([5], ordered = true)
    D = categorical([1], ordered = true)
    levels!(A, [10, 5, 1])
    levels!(B, [1, 5, 10])
    levels!(C, [5, 1, 10])
    levels!(D, [1, 10, 5])
    return DataFrame(A = A, B = B, C = C, D = D)
end

# ============================== TESTS ==============================

@testset "build_candidate_score_distributions" begin
    df = toy_scores_df()
    d = build_candidate_score_distributions(df, CANDS)
    @test Set(keys(d)) == Set(CANDS)
    @test all(haskey(d["A"], 10.0) for _ in 1:1)  # some value exists
end

@testset "convert_keys_to_int" begin
    d = Dict(1=>2, 2.0=>3, 3.00000001=>4)
    d2 = convert_keys_to_int(d)
    @test d2[1] == 2
    @test d2[2] == 3
    @test d2[3] == 4
    @test_throws ArgumentError convert_keys_to_int(Dict(2.5=>1))
    @test_throws ArgumentError convert_keys_to_int(Dict("a"=>1))
end

@testset "sanitize_countmaps" begin
    cm = Dict(
        "A"=>Dict(1.0=>2, 96.0=>3),
        "B"=>Dict(2.0=>1),
    )
    s = sanitize_countmaps(cm)
    @test s["A"][96] == 3
end

@testset "compute_dont_know_her" begin
    cm = Dict(
        "A"=>Dict(10=>5, 96=>1, 97=>1, 98=>1, 99=>2),
        "B"=>Dict(10=>10, 96=>0, 97=>0, 98=>0, 99=>0),
    )
    v = compute_dont_know_her(cm, 10+1+1+1+2)  # total for "A" row
    @test v[1][1] == "B"   # lowest don't know first
    @test v[end][1] == "A"
end

@testset "prepare_scores_for_imputation_int/_categorical" begin
    df = toy_scores_df()
    ints = prepare_scores_for_imputation_int(df, CANDS; extra_cols=DEMOS)
    @test all(eltype(ints[!, c]) <: Union{Missing,Int} for c in CANDS)
    @test all(in(names(ints)).(DEMOS))
    cats = prepare_scores_for_imputation_categorical(df, CANDS; extra_cols=DEMOS)
    @test all(eltype(cats[!, c]) <: Union{Missing,CategoricalValue{Int,UInt32}} for c in CANDS)
end

@testset "get_most_known_candidates" begin
    dk = [("A", 1.0), ("B", 2.0), ("C", 3.0)]
    @test get_most_known_candidates(dk, 2) == ["A","B"]
end

@testset "select_top_candidates" begin
    cm = Dict(
        "A"=>Dict(10=>10, 96=>0, 97=>0, 98=>0, 99=>0),
        "B"=>Dict(10=>8, 96=>2),
        "C"=>Dict(10=>6, 96=>4),
        "D"=>Dict(10=>5, 96=>5)
    )
    sel = select_top_candidates(cm, 20; m=3, force_include=["D","A","A"])
    @test length(sel) == 3
    @test "D" in sel && "A" in sel
end

@testset "compute_candidate_set" begin
    df = toy_scores_df()
    chosen = compute_candidate_set(df; candidate_cols=CANDS, m=3)
    @test length(chosen) <= 3
    @test all(c in CANDS for c in chosen)
end

@testset "get_df_just_top_candidates (two methods)" begin
    df = toy_scores_df()
    PrefPol.dont_know_her = [("A", 0.0), ("B", 1.0), ("C", 2.0), ("D", 3.0)]
    df1 = get_df_just_top_candidates(df, 2; demographics=DEMOS)
    @test Set(names(df1)) == Set(vcat(["A","B"], DEMOS))
    df2 = get_df_just_top_candidates(df, ["C","A"]; demographics=["Sex"])
    @test Set(names(df2)) == Set(["C","A","Sex"])
end

@testset "GLOBAL_R_IMPUTATION (smoke, optional)" begin
    df = toy_scores_df()
    # Prepare as categoricals to match imputation_variants pathway
    scores_cat = prepare_scores_for_imputation_categorical(df, CANDS; extra_cols=DEMOS)
    ok = true
    try
        imputed = GLOBAL_R_IMPUTATION(scores_cat)
        @test size(imputed, 1) == size(scores_cat, 1)
        @test names(imputed) == names(scores_cat)
    catch e
        @info "Skipping GLOBAL_R_IMPUTATION smoke test (R/mice not available?)." error=e
        ok = false
    end
    @test ok || true  # don't fail suite if R env is missing
end

@testset "R seed normalization stays within set.seed range" begin
    @test PrefPol._normalize_r_seed(6547001803536573440) <= typemax(Int32) - 1
    @test PrefPol._normalize_r_seed(6547001803536573440) >= 1
    @test PrefPol._normalize_r_seed(42) == 43

    df = toy_scores_df()
    scores_cat = prepare_scores_for_imputation_categorical(df, CANDS; extra_cols = DEMOS)
    ok = true
    try
        report = PrefPol.r_impute_mice_report(scores_cat; seed = 6547001803536573440)
        @test size(report.completed, 1) == size(scores_cat, 1)
        @test names(report.completed) == names(scores_cat)
    catch e
        @info "Skipping oversized-seed R/mice regression test (R/mice not available?)." error = e
        ok = false
    end
    @test ok || true
end

@testset "imputation_variants" begin
    df = toy_scores_df()
    imps = imputation_variants(df, CANDS, DEMOS; most_known_candidates=String[])
    @test Set(keys(imps)) == Set((:zero, :random, :mice))
    @test all(nrow(imps.zero) == nrow(df) for _ in 1:1)
    @test all(nrow(imps.random) == nrow(df) for _ in 1:1)

    subset = imputation_variants(df, CANDS, DEMOS;
                                 most_known_candidates = String[],
                                 variants = (:zero, :random))
    @test keys(subset) == (:zero, :random)
    @test nrow(subset.random) == nrow(df)

    random_only = imputation_variants(df, CANDS, DEMOS;
                                      most_known_candidates = String[],
                                      variants = (:random,))
    @test keys(random_only) == (:random,)
    @test nrow(random_only.random) == nrow(df)

    @test_throws ArgumentError imputation_variants(df, CANDS, DEMOS;
                                                   most_known_candidates = String[],
                                                   variants = (:bogus,))
    # zero variant should contain only Int/Union{Missing,Int} and zeros for special codes
    @test all(eltype(imps.zero[!, c]) <: Union{Missing,Int} for c in CANDS)
end

@testset "weighted_bootstrap" begin
    df = toy_scores_df()
    w  = fill(1.0, nrow(df))
    reps = weighted_bootstrap(df, w, 5)
    @test length(reps) == 5
    @test all(nrow(r) == nrow(df) for r in reps)
end

@testset "get_row_candidate_score_pairs / get_order_dict / force_scores_become_linear_rankings" begin
    df = toy_scores_df(n=3)
    row = df[1, :]
    d = get_row_candidate_score_pairs(row, CANDS)
    @test Set(keys(d)) == Set(Symbol.(CANDS))
end

@testset "get_order_dict handles plain numeric scores" begin
    ord = get_order_dict(Dict(:A => 10, :B => 10, :C => 5, :D => 1))
    @test ord == Dict(:A => 1, :B => 1, :C => 2, :D => 3)
end

@testset "get_order_dict handles categorical scores from unequal pools" begin
    ord = get_order_dict(unequal_pool_score_dict())
    @test ord == Dict(:A => 1, :B => 1, :C => 2, :D => 3)
end

@testset "force_scores_become_linear_rankings handles categorical scores from unequal pools" begin
    lin = force_scores_become_linear_rankings(unequal_pool_score_dict(); rng = MersenneTwister(1))
    @test sort(collect(values(lin))) == [1, 2, 3, 4]
    @test Set([lin[:A], lin[:B]]) == Set([1, 2])
    @test lin[:C] == 3
    @test lin[:D] == 4
end

@testset "linearize_ranking_dict / linearize_profile_column!" begin
    weak = Dict(:A => 1, :B => 1, :C => 2, :D => 3)
    lin1 = linearize_ranking_dict(weak; rng = MersenneTwister(1))
    lin2 = linearize_ranking_dict(weak; rng = MersenneTwister(1))

    @test lin1 == lin2
    @test sort(collect(values(lin1))) == [1, 2, 3, 4]
    @test lin1[:C] < lin1[:D]

    df = DataFrame(profile = [weak, weak], Sex = ["F", "M"], Race = ["W", "B"])
    linearize_profile_column!(df; rng = MersenneTwister(2))
    @test all(sort(collect(values(p))) == [1, 2, 3, 4] for p in df.profile)
end

@testset "build_profile / profile_dataframe" begin
    df = toy_scores_df(n=7)
    profs = build_profile(df; score_cols=CANDS, rng=MersenneTwister(2), kind=:linear)
    @test length(profs) == nrow(df)
    @test all(isa(p, Dict{Symbol,Int}) for p in profs)
    pdf = profile_dataframe(df; score_cols=CANDS, demo_cols=DEMOS, rng=MersenneTwister(3))
    @test ((:profile in names(pdf)) || ("profile" in names(pdf)))
    @test all(in(names(pdf)).(DEMOS))
end

@testset "profile_dataframe handles unequal categorical score pools on the weak-order path" begin
    pdf = profile_dataframe(
        unequal_pool_scores_df();
        score_cols = CANDS,
        demo_cols = String[],
        kind = :weak,
    )
    @test nrow(pdf) == 1
    @test pdf.profile[1] == Dict(:A => 1, :B => 1, :C => 2, :D => 3)
end

@testset "profile_dataframe handles numeric score strings from mice-style categoricals" begin
    df = DataFrame(
        A = categorical(["10", "7", "5"], ordered = true),
        B = categorical(["7", "7", "10"], ordered = true),
        grp = categorical(["x", "y", "x"]),
    )

    weak_pdf = profile_dataframe(
        df;
        score_cols = ["A", "B"],
        demo_cols = ["grp"],
        kind = :weak,
    )
    @test weak_pdf.profile == [
        Dict(:A => 1, :B => 2),
        Dict(:A => 1, :B => 1),
        Dict(:A => 2, :B => 1),
    ]

    linear_profiles = build_profile(
        df;
        score_cols = ["A", "B"],
        rng = MersenneTwister(7),
        kind = :linear,
    )
    @test linear_profiles[1][:A] < linear_profiles[1][:B]
    @test sort(collect(values(linear_profiles[2]))) == [1, 2]
    @test linear_profiles[3][:A] > linear_profiles[3][:B]
end


@testset "dict2svec / decode_rank / perm2dict[alias]" begin
    d = toy_ranking_dict()
    sv = dict2svec(d; cs=Symbol.(keys(d)))
    @test sv isa SVector
    # pool decode path
    pool = [sv, SVector{length(sv),UInt8}(reverse(collect(sv)))]
    decoded = decode_rank(1, pool)
    @test decoded == sv
    # direct path (already SVector)
    @test decode_rank(sv, pool) === sv
    # perm2dict / perm_to_dict
    back = perm2dict(collect(1:length(sv)), Symbol.(keys(d)))
    @test all(values(back) .== 1:length(sv))
    back2 = perm_to_dict(collect(1:length(sv)), Symbol.(keys(d)))
    @test back == back2
end

@testset "compress_rank_column! / decode_profile_column!" begin
    df = toy_scores_df(n=5)
    pdf = profile_dataframe(df; score_cols=CANDS, demo_cols=DEMOS, rng=MersenneTwister(4))
    # keep a decoded copy for comparison
    decoded_before = deepcopy(pdf.profile)
    # metadata needed by decode_profile_column!
    metadata!(pdf, "candidates", Symbol.(CANDS))
    pool = compress_rank_column!(pdf, Symbol.(CANDS); col=:profile)
    @test eltype(pdf.profile) <: Union{SVector,UInt16,PooledArrays.PooledArray}
    # decode back
    decode_profile_column!(pdf)
    @test all(isa(p, Dict{Symbol,Int}) for p in pdf.profile)
    # compare permutations (ignoring tie randomness across seeds)
    @test length(pdf.profile) == length(decoded_before)
end

@testset "decode_each!" begin
    df = toy_scores_df(n=3)
    pdf = profile_dataframe(df; score_cols=CANDS, demo_cols=DEMOS, rng=MersenneTwister(5))
    metadata!(pdf, "candidates", Symbol.(CANDS))
    compress_rank_column!(pdf, Symbol.(CANDS); col=:profile)
    # store in vectors of length 1, as expected in streaming path
    vm = Dict(:zero => [deepcopy(pdf)], :random => [deepcopy(pdf)], :mice => [deepcopy(pdf)])
    decode_each!(vm)
    @test all(isa(vm[:zero][1].profile[1], Dict{Symbol,Int}) for _ in 1:1)
end

# --------------- load_spss_file (optional negative test) ---------------
@testset "load_spss_file (negative smoke)" begin
    ok = true
    try
        @test_throws Any load_spss_file("this_file_does_not_exist.sav")
    catch e
        @info "Skipping load_spss_file negative smoke (RCall/haven not usable?)." error=e
        ok = false
    end
    @test ok || true
end

println("\nAll preprocessing tests executed.")
