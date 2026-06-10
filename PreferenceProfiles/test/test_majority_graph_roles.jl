@testset "Majority graph role diagnostics" begin
    pool4 = pp.CandidatePool([:A, :B, :C, :D])

    make_profile() = begin
        abcd = pp.StrictRank(pool4, [:A, :B, :C, :D])
        bacd = pp.StrictRank(pool4, [:B, :A, :C, :D])
        acbd = pp.StrictRank(pool4, [:A, :C, :B, :D])
        pp.Profile(pool4, vcat(fill(abcd, 5), fill(bacd, 4), [acbd]))
    end

    @testset "type role table basics" begin
        profile = make_profile()
        basis = pp.voter_type_basis(pool4; order=:lex)
        result = pp.majority_graph_support(profile; basis=basis)
        roles = pp.voter_type_role_table(result)

        @test nrow(roles) == factorial(4)
        @test roles.type_index == collect(1:24)
        for col in [:anchor, :peripheral_supporter, :counter_graph, :edge_breaker,
                    :roles, :primary_role, :max_breaking_score, :max_breaking_edge]
            @test col in propertynames(roles)
        end

        majority_idx = basis.index_by_signature[(:A, :B, :C, :D)]
        reverse_idx = basis.index_by_signature[(:D, :C, :B, :A)]
        peripheral_idx = basis.index_by_signature[(:A, :C, :B, :D)]

        majority_row = roles[roles.type_index .== majority_idx, :][1, :]
        reverse_row = roles[roles.type_index .== reverse_idx, :][1, :]
        peripheral_row = roles[roles.type_index .== peripheral_idx, :][1, :]

        @test majority_row.anchor
        @test reverse_row.coverage <= 1
        @test reverse_row.counter_graph
        @test peripheral_row.peripheral_supporter
        @test any(roles.anchor .& roles.edge_breaker)
    end

    @testset "edge role table and summaries" begin
        profile = make_profile()
        result = pp.majority_graph_support(profile)
        edge_roles = pp.edge_type_role_table(result)
        weakest = argmin([edge.normalized_margin for edge in result.edges])

        @test any(edge_roles.edge_breaker_for_edge .& (edge_roles.edge_index .== weakest))
        @test all(edge_roles[edge_roles.edge_index .== weakest, :normalized_margin] .≈
                  minimum(edge.normalized_margin for edge in result.edges))

        selected = pp.selected_edge_role_summary(result; top_n=3)
        @test nrow(selected) == 3
        @test all(selected.edge_index .== weakest)

        roles = pp.voter_type_role_table(result)
        summary = pp.role_mass_summary(roles)
        @test Set(summary.role) == Set(["anchor", "peripheral_supporter", "edge_breaker",
                                        "counter_graph", "mixed"])

        primary = pp.primary_role_mass_summary(roles)
        @test sum(primary.mass) ≈ sum(roles.proportion)
        @test sum(primary.share) ≈ 1.0
    end

    @testset "group role tables" begin
        profile = make_profile()
        groups = vcat(fill(:g1, 5), fill(:g2, 5))
        gresult = pp.group_majority_graph_support(profile, groups)
        roles = pp.voter_type_role_table(gresult.base)

        grouped = pp.group_role_table(gresult, roles)
        @test nrow(grouped) == length(gresult.groups)
        share_cols = [:conditional_anchor_share, :conditional_peripheral_supporter_share,
                      :conditional_counter_graph_share, :conditional_edge_breaker_share,
                      :conditional_mixed_share]
        for col in share_cols
            @test all(0.0 .<= grouped[!, col] .<= 1.0)
        end

        primary = pp.group_primary_role_table(gresult, roles)
        for group in gresult.groups
            rows = primary[primary.group .== group, :]
            @test sum(rows.conditional_role_share) ≈ 1.0
        end

        power = pp.group_role_power_table(gresult)
        weakest = argmin([edge.normalized_margin for edge in gresult.base.edges])
        @test all(power.edge_index .== weakest)

        anchor_tbl = pp.group_anchor_table(gresult)
        edge_tbl = pp.group_edge_power_table(gresult)
        breaker_tbl = pp.group_breaker_table(gresult)
        for row in eachrow(power)
            arow = anchor_tbl[anchor_tbl.group .== row.group, :][1, :]
            erow = edge_tbl[(edge_tbl.group .== row.group) .&
                            (edge_tbl.edge_index .== row.edge_index), :][1, :]
            brow = breaker_tbl[(breaker_tbl.group .== row.group) .&
                               (breaker_tbl.edge_index .== row.edge_index), :][1, :]
            @test row.anchoring ≈ arow.anchoring
            @test row.conditional_anchoring ≈ arow.conditional_anchoring
            @test row.edge_margin_contribution ≈ erow.group_margin_contribution
            @test row.edge_support ≈ erow.group_support
            @test row.edge_breaking_score ≈ brow.breaking_score
        end
    end

    @testset "WeightedProfile compatibility" begin
        ballots = [
            pp.StrictRank(pool4, [:A, :B, :C, :D]),
            pp.StrictRank(pool4, [:B, :A, :C, :D]),
            pp.StrictRank(pool4, [:A, :C, :B, :D]),
        ]
        weights = [5.0, 4.0, 1.0]
        profile = pp.WeightedProfile(pool4, ballots, weights)
        basis = pp.voter_type_basis(pool4; order=:lex)
        result = pp.majority_graph_support(profile; basis=basis)
        roles = pp.voter_type_role_table(result)

        abcd_idx = basis.index_by_signature[(:A, :B, :C, :D)]
        @test roles[roles.type_index .== abcd_idx, :mass][1] ≈ 5.0
        @test roles[roles.type_index .== abcd_idx, :proportion][1] ≈ 0.5
    end
end
