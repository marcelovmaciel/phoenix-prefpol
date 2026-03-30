using DataFrames
using Random: MersenneTwister
using StaticArrays: SA

ranking_dict(order::Vector{Symbol}) = Dict(c => i for (i, c) in enumerate(order))

function profile_from_counts(counts::Vector{Tuple{Vector{Symbol},Int}})
    out = Vector{Dict{Symbol,Int}}()
    for (ord, k) in counts
        ranking = ranking_dict(ord)
        append!(out, fill(ranking, k))
    end
    return out
end

@testset "linear order catalog cache uses exact candidate tuples" begin
    cache = Dict{Tuple{Vararg{Symbol}},Any}()

    cat_abc = pp.get_linear_order_catalog((:a, :b, :c); cache = cache)
    cat_abd = pp.get_linear_order_catalog((:a, :b, :d); cache = cache)
    cat_abc_again = pp.get_linear_order_catalog((:a, :b, :c); cache = cache)

    @test length(cat_abc.orders) == factorial(3)
    @test length(cat_abd.orders) == factorial(3)
    @test cat_abc === cat_abc_again
    @test cat_abc !== cat_abd
end

@testset "consensus, distances, and tie handling" begin
    prof = [ranking_dict([:a, :b, :c])]
    subdf = DataFrame(profile = prof)
    out = pp.consensus_for_group(subdf)
    @test out.consensus_ranking == prof[1]

    r1 = ranking_dict([:a, :b, :c])
    r2 = ranking_dict([:a, :c, :b])
    r3 = ranking_dict([:c, :b, :a])
    @test pp.kendall_tau_dict(r1, r1) == 0
    @test pp.kendall_tau_dict(r1, r3) == 3
    @test pp.average_normalized_distance(prof, r1) == 0.0
    @test pp.average_normalized_distance(fill(r3, 5), r1) == 1.0

    gad = pp.group_avg_distance(subdf)
    @test gad.avg_distance == 0.0
    @test gad.group_coherence == 1.0

    prof_unique = profile_from_counts([
        ([:a, :b, :c], 2),
        ([:a, :c, :b], 1),
        ([:b, :a, :c], 1),
    ])
    strict_unique = pp.strict_profile(prof_unique)
    res_unique = pp.consensus_kendall(strict_unique, (:a, :b, :c))

    @test res_unique.consensus_ranking == ranking_dict([:a, :b, :c])
    @test res_unique.consensus_perm == SA[0x01, 0x02, 0x03]
    @test res_unique.min_total_distance == 2.0
    @test res_unique.avg_normalized_distance == 2 / (4 * 3)
    @test !res_unique.is_tied_minimizer
    @test res_unique.n_minimizers == 1
    @test res_unique.tie_rule == :unique
    @test res_unique.all_minimizers == [SA[0x01, 0x02, 0x03]]

    prof_tie = [ranking_dict([:a, :b]), ranking_dict([:b, :a])]
    strict_tie = pp.strict_profile(prof_tie)
    res_tie = pp.consensus_kendall(strict_tie, (:a, :b))
    res_tie_repeat = pp.consensus_kendall(strict_tie, (:a, :b))
    res_tie_rng_1 = pp.consensus_kendall(strict_tie, (:a, :b); rng = MersenneTwister(1))

    @test res_tie.consensus_perm in (SA[0x01, 0x02], SA[0x02, 0x01])
    @test res_tie.min_total_distance == 1.0
    @test res_tie.avg_normalized_distance == 0.5
    @test res_tie.is_tied_minimizer
    @test res_tie.n_minimizers == 2
    @test res_tie.tie_rule == :deterministic_pseudorandom_minimizer
    @test res_tie.all_minimizers == [SA[0x01, 0x02], SA[0x02, 0x01]]
    @test res_tie_repeat.consensus_perm == res_tie.consensus_perm
    @test res_tie_rng_1.tie_rule == :random_minimizer

    out_tie = pp.consensus_for_group(DataFrame(profile = prof_tie))
    @test out_tie.consensus_perm == res_tie.consensus_perm
    @test out_tie.consensus_set == [SA[0x01, 0x02], SA[0x02, 0x01]]
end

@testset "weighted brute-force consensus matches expanded profile" begin
    pool = pp.CandidatePool([:a, :b, :c])
    abc = pp.StrictRank(pool, [:a, :b, :c])
    cba = pp.StrictRank(pool, [:c, :b, :a])

    weighted = pp.WeightedProfile(pp.Profile(pool, [abc, cba]), [2.0, 1.0])
    expanded = pp.Profile(pool, [abc, abc, cba])

    res_weighted = pp.consensus_kendall(weighted, (:a, :b, :c))
    res_expanded = pp.consensus_kendall(expanded, (:a, :b, :c))

    @test res_weighted.consensus_perm == res_expanded.consensus_perm
    @test res_weighted.min_total_distance == res_expanded.min_total_distance
    @test res_weighted.avg_normalized_distance == res_expanded.avg_normalized_distance
end

@testset "divergence measures and grouped metrics" begin
    consA = ranking_dict([:a, :b, :c])
    consB = ranking_dict([:c, :b, :a])
    profA = fill(consA, 4)
    profB = fill(consB, 6)

    @test pp.pairwise_group_divergence(profA, consB, 3) == 1.0
    @test pp.pairwise_group_divergence(profB, consA, 3) == 1.0

    group_profiles = Dict(:A => profA, :B => profB)
    consensus_map = Dict(:A => consA, :B => consB)
    @test pp.overall_divergence(group_profiles, consensus_map) == 1.0

    whole_df = vcat(DataFrame(group = :A, profile = profA),
                    DataFrame(group = :B, profile = profB))
    grouped_consensus = DataFrame(group = [:A, :B], consensus_ranking = Any[consA, consB])
    @test pp.overall_divergences(grouped_consensus, whole_df, :group) == 1.0

    C, D = pp.compute_group_metrics(whole_df, :group)
    @test isapprox(C, 1.0; atol = 1e-12)
    @test isapprox(D, 1.0; atol = 1e-12)

    bt_profiles = Dict(:mice => [whole_df, whole_df], :rand => [whole_df])
    res = pp.bootstrap_group_metrics(bt_profiles, :group)
    @test Set(keys(res)) == Set([:mice, :rand])
    @test res[:mice][:C] == fill(1.0, 2)
    @test res[:mice][:D] == fill(1.0, 2)
    @test res[:rand][:C] == fill(1.0, 1)
    @test res[:rand][:D] == fill(1.0, 1)
end
