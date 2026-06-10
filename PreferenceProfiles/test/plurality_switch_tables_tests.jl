@testset "Plurality and target-switch tables" begin
    pool = pp.CandidatePool([:A, :B, :C])
    ballots = [
        pp.StrictRank(pool, [:A, :B, :C]),
        pp.StrictRank(pool, [:A, :C, :B]),
        pp.StrictRank(pool, [:B, :A, :C]),
        pp.StrictRank(pool, [:B, :A, :C]),
        pp.StrictRank(pool, [:C, :A, :B]),
    ]
    profile = pp.Profile(pool, ballots)
    basis = pp.voter_type_basis(pool; order=:lex)

    plurality = pp.plurality_scores_table(profile; basis=basis)
    @test plurality.first_place_count == [2.0, 2.0, 1.0]
    @test plurality.first_place_share ≈ [0.4, 0.4, 0.2]

    decomp = pp.pairwise_vs_plurality_decomposition_table(profile, :A, :B; basis=basis)
    @test sum(decomp.pairwise_contribution) == 1.0
    @test sum(decomp.plurality_contribution) == 0.0
    @test decomp[decomp.current_first .== :C, :pairwise_contribution][1] == 1.0

    pos = pp.candidate_position_by_current_first_table(profile, :A; basis=basis)
    @test pos[pos.current_first .== :B, :mass][1] == 2.0
    @test pos[pos.current_first .== :B, :target_position][1] == 2

    one_swap = pp.one_swap_target_table(profile, :A; current_first_candidates=[:B, :C], basis=basis)
    @test sum(one_swap.mass) == 3.0
    @test one_swap[one_swap.current_first .== :B, :mass][1] == 2.0

    swing = pp.plurality_swing_value_table(profile, :A, :B; current_first_candidates=[:B, :C], basis=basis)
    @test swing[swing.current_first .== :B, :per_voter_swing][1] == 2.0
    @test sum(swing.plurality_swing_value) == 5.0
    @test all(==(0.0), swing.target_opponent_margin_before)

    exact = pp.exact_type_switch_table(profile, :A; current_first_candidates=[:B, :C], basis=basis)
    @test all(exact.type_index .>= 1)
    @test sum(exact.mass) == 3.0

    groups = [:g1, :g1, :g2, :g2, :g2]
    gswitch = pp.group_target_switch_table(profile, groups, :A, :B; current_first_candidates=[:B, :C], basis=basis)
    @test sum(gswitch.plurality_swing_value) == 5.0

    weighted = pp.WeightedProfile(profile, [1.0, 2.0, 3.0, 4.0, 5.0])
    wplurality = pp.plurality_scores_table(weighted; basis=basis)
    @test wplurality.first_place_count == [3.0, 7.0, 5.0]
end
