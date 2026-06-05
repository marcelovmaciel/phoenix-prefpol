@testset "WeightedProfile internals match expanded Profile semantics" begin
    pool = pp.CandidatePool([:A, :B, :C])
    abc = pp.StrictRank(pool, [:A, :B, :C])
    acb = pp.StrictRank(pool, [:A, :C, :B])
    bac = pp.StrictRank(pool, [:B, :A, :C])
    bca = pp.StrictRank(pool, [:B, :C, :A])
    cab = pp.StrictRank(pool, [:C, :A, :B])
    cba = pp.StrictRank(pool, [:C, :B, :A])

    weighted = pp.WeightedProfile(
        pool,
        [abc, cba, acb, bca, bac, cab],
        [3.0, 2.0, 1.0, 1.0, 0.0, 0.0],
    )
    expanded = pp.Profile(pool, [abc, abc, abc, cba, cba, acb, bca])

    measures = (
        pp.ranking_proportions,
        pp.effective_observed_rankings,
        pp.effective_reversal_rankings,
        pp.effective_reversal_ranking_diagnostics,
        pp.ranking_support_diagnostics,
        p -> pp.average_normalized_distance(p, abc),
        pp.can_polarization,
        pp.total_reversal_component,
        pp.reversal_hhi,
        pp.reversal_geometric,
    )

    for measure in measures
        @test measure(weighted) == measure(expanded)
    end
end
