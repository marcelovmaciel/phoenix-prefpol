function sample_ranking_frequencies(weak_rank, model, pool; n = 4000, seed = 1)
    rng = Random.MersenneTwister(seed)
    counts = Dict{Tuple,Int}()

    for _ in 1:n
        draw = pp.linearize(weak_rank; tie_break = model, rng = rng)
        sig = pp.ranking_signature(draw, pool)
        counts[sig] = get(counts, sig, 0) + 1
    end

    return Dict(sig => count / n for (sig, count) in counts)
end

@testset "PatternConditionalLinearizer constructor and coercion" begin
    pool = pp.CandidatePool([:A, :B, :C, :D])
    abcd = pp.StrictRank(pool, [:A, :B, :C, :D])
    acbd = pp.StrictRank(pool, [:A, :C, :B, :D])

    strict_profile = pp.Profile(pool, [abcd, acbd])
    strict_weighted = pp.WeightedProfile(pool, [abcd, acbd], [2.0, 1.0])

    model = pp.PatternConditionalLinearizer(strict_profile)
    @test model.reference === strict_profile
    @test model.alpha == 0.5
    @test model.fallback == :uniform

    weighted_model = pp.PatternConditionalLinearizer(strict_weighted; alpha = 1.0, fallback = :error)
    @test weighted_model.reference === strict_weighted
    @test weighted_model.alpha == 1.0
    @test weighted_model.fallback == :error

    mixed_weak_reference = pp.Profile(pool, [
        pp.WeakRank(pool, [1, 2, 3, 4]),
        pp.WeakRank(pool, [1, 1, 2, 3]),
        pp.WeakRank(pool, Union{Missing,Int}[1, 2, 3, missing]),
    ])
    coerced_model = pp.PatternConditionalLinearizer(mixed_weak_reference)
    @test coerced_model.reference isa pp.Profile{<:pp.StrictRank}
    @test pp.nballots(coerced_model.reference) == 1
    @test pp.ranking_signature(coerced_model.reference.ballots[1], pool) == (:A, :B, :C, :D)

    mixed_weighted_reference = pp.WeightedProfile(pool, [
        pp.WeakRank(pool, [1, 2, 3, 4]),
        pp.WeakRank(pool, [1, 1, 2, 3]),
    ], [2.5, 9.0])
    weighted_coerced_model = pp.PatternConditionalLinearizer(mixed_weighted_reference)
    @test weighted_coerced_model.reference isa pp.WeightedProfile{<:pp.StrictRank}
    @test pp.weights(weighted_coerced_model.reference) == [2.5]

    bad_weak_reference = pp.Profile(pool, [
        pp.WeakRank(pool, [1, 1, 2, 3]),
        pp.WeakRank(pool, Union{Missing,Int}[1, 2, missing, 3]),
    ])
    @test_throws ArgumentError pp.PatternConditionalLinearizer(bad_weak_reference)
    @test_throws ArgumentError pp.PatternConditionalLinearizer(strict_profile; alpha = 0.0)
    @test_throws ArgumentError pp.PatternConditionalLinearizer(strict_profile; fallback = :bad)
end

@testset "PatternConditionalLinearizer ballot-level sampling" begin
    pool = pp.CandidatePool([:A, :B, :C, :D])
    weak = pp.WeakRank(pool, [1, 2, 2, 3])
    abcd = pp.StrictRank(pool, [:A, :B, :C, :D])
    acbd = pp.StrictRank(pool, [:A, :C, :B, :D])

    reference = pp.Profile(pool, [abcd])
    model = pp.PatternConditionalLinearizer(reference; alpha = 0.5)

    freqs = sample_ranking_frequencies(weak, model, pool; n = 4000, seed = 11)
    p_abcd = get(freqs, (:A, :B, :C, :D), 0.0)
    p_acbd = get(freqs, (:A, :C, :B, :D), 0.0)

    @test isapprox(p_abcd + p_acbd, 1.0; atol = 1e-8)
    @test 0.70 <= p_abcd <= 0.80
    @test 0.20 <= p_acbd <= 0.30

    weighted_reference = pp.WeightedProfile(pool, [abcd, acbd], [3.0, 1.0])
    weighted_model = pp.PatternConditionalLinearizer(weighted_reference; alpha = 0.5)
    pp.linearize(weak; tie_break = weighted_model, rng = Random.MersenneTwister(3))

    exts, masses = only(values(weighted_model.cache))
    mass_by_sig = Dict(
        pp.ranking_signature(pp.StrictRank(ext), pool) => mass
        for (ext, mass) in zip(exts, masses)
    )
    @test mass_by_sig[(:A, :B, :C, :D)] ≈ 3.5
    @test mass_by_sig[(:A, :C, :B, :D)] ≈ 1.5
end

@testset "PatternConditionalLinearizer fallback behavior" begin
    pool = pp.CandidatePool([:A, :B, :C, :D])
    weak = pp.WeakRank(pool, [1, 2, 2, 3])
    incompatible_reference = pp.Profile(pool, [pp.StrictRank(pool, [:B, :A, :C, :D])])

    uniform_model = pp.PatternConditionalLinearizer(incompatible_reference; alpha = 0.5, fallback = :uniform)
    freqs = sample_ranking_frequencies(weak, uniform_model, pool; n = 4000, seed = 21)
    p_abcd = get(freqs, (:A, :B, :C, :D), 0.0)
    p_acbd = get(freqs, (:A, :C, :B, :D), 0.0)

    @test isapprox(p_abcd + p_acbd, 1.0; atol = 1e-8)
    @test 0.42 <= p_abcd <= 0.58
    @test 0.42 <= p_acbd <= 0.58

    error_model = pp.PatternConditionalLinearizer(incompatible_reference; fallback = :error)
    @test_throws ArgumentError pp.linearize(weak; tie_break = error_model, rng = Random.MersenneTwister(21))
end

@testset "PatternConditionalLinearizer no-ties and profile forwarding" begin
    pool = pp.CandidatePool([:A, :B, :C, :D])
    abcd = pp.StrictRank(pool, [:A, :B, :C, :D])
    model = pp.PatternConditionalLinearizer(pp.Profile(pool, [abcd]))

    already_strict = pp.WeakRank(pool, [1, 2, 3, 4])
    draw = pp.linearize(already_strict; tie_break = model, rng = Random.MersenneTwister(31))
    @test draw isa pp.StrictRank
    @test pp.ranking_signature(draw, pool) == (:A, :B, :C, :D)

    weak_profile = pp.Profile(pool, [
        pp.WeakRank(pool, [1, 2, 2, 3]),
        pp.WeakRank(pool, [1, 1, 2, 3]),
    ])
    strict_profile = pp.linearize(weak_profile; tie_break = model, rng = Random.MersenneTwister(32))
    @test strict_profile isa pp.Profile{<:pp.StrictRank}
    @test all(ballot isa pp.StrictRank for ballot in strict_profile.ballots)

    incomplete = pp.WeakRank(pool, Union{Missing,Int}[1, 2, 2, missing])
    @test_throws ArgumentError pp.linearize(incomplete; tie_break = model, rng = Random.MersenneTwister(33))
    @test_throws ArgumentError pp.linearize(
        already_strict;
        tie_break = model,
        rng = Random.MersenneTwister(34),
        incomplete_policy = :preserve,
    )
end
