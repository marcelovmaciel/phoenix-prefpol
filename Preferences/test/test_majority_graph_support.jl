@testset "Majority graph support diagnostics" begin
    pool4 = pp.CandidatePool([:A, :B, :C, :D])

    @testset "canonical voter type basis" begin
        basis = pp.voter_type_basis(pool4; order = :lex)
        @test length(basis.types) == 24
        @test length(unique(Tuple.(basis.perms))) == 24
        @test length(unique(basis.signatures)) == 24
        @test basis.perms[1] == [1, 2, 3, 4]
        @test basis.perms[end] == [4, 3, 2, 1]
    end

    @testset "Kendall shell ordering" begin
        basis = pp.voter_type_basis(pool4; order = :kendall_shell, reference_order = [:A, :B, :C, :D])
        ref = [1, 2, 3, 4]
        shell_counts = zeros(Int, 7)
        for perm in basis.perms
            d = pp.kendall_tau_distance(pp.StrictRank(pool4, perm), pp.StrictRank(pool4, ref))
            shell_counts[d + 1] += 1
        end
        @test shell_counts == [1, 3, 5, 6, 5, 3, 1]
        @test basis.perms[1] == ref
    end

    @testset "full basis mass vector and zero-mass types" begin
        basis = pp.voter_type_basis(pool4; order = :lex)
        ballots = [
            pp.StrictRank(pool4, [:A, :B, :C, :D]),
            pp.StrictRank(pool4, [:A, :C, :B, :D]),
            pp.StrictRank(pool4, [:B, :A, :C, :D]),
        ]
        profile = pp.Profile(pool4, ballots)
        result = pp.majority_graph_support(profile; basis = basis)
        @test length(result.type_mass) == 24
        @test count(==(0.0), result.type_mass) == 21
        @test sum(result.type_proportion) ≈ 1.0
    end

    @testset "pairwise margins and support matrix" begin
        pool = pp.CandidatePool([:A, :B, :C])
        ballots = [
            pp.StrictRank(pool, [:A, :B, :C]),
            pp.StrictRank(pool, [:A, :C, :B]),
            pp.StrictRank(pool, [:B, :A, :C]),
        ]
        profile = pp.Profile(pool, ballots)
        result = pp.majority_graph_support(profile)
        manual = [0.0 1.0 3.0; -1.0 0.0 1.0; -3.0 -1.0 0.0]
        @test result.margins == manual

        for (eidx, edge) in enumerate(result.edges)
            for (ridx, perm) in enumerate(result.basis.perms)
                pos_w = findfirst(==(edge.winner), perm)
                pos_l = findfirst(==(edge.loser), perm)
                @test result.support_matrix[ridx, eidx] == (pos_w < pos_l)
            end
        end
    end

    @testset "core masses" begin
        ballot = pp.StrictRank(pool4, [:A, :B, :C, :D])
        profile = pp.Profile(pool4, [ballot, ballot])
        result = pp.majority_graph_support(profile)
        @test result.core_mass_by_k[6] ≈ 1.0

        mixed = pp.Profile(pool4, [
            pp.StrictRank(pool4, [:A, :B, :C, :D]),
            pp.StrictRank(pool4, [:D, :C, :B, :A]),
            pp.StrictRank(pool4, [:A, :C, :B, :D]),
        ])
        mixed_result = pp.majority_graph_support(mixed)
        core = [mixed_result.core_mass_by_k[k] for k in 0:length(mixed_result.edges)]
        @test all(core[i] >= core[i + 1] for i in 1:(length(core) - 1))
    end

    @testset "breaker distances and non-supporters" begin
        basis = pp.voter_type_basis(pool4; order = :lex)
        idx = basis.index_by_signature[(:A, :B, :C, :D)]
        @test pp.boundary_distance_to_reverse(basis, idx, 1, 2) == 1
        @test pp.boundary_distance_to_reverse(basis, idx, 1, 4) == 3
        @test pp.boundary_distance_to_reverse(basis, idx, 2, 4) == 2
        @test ismissing(pp.boundary_distance_to_reverse(basis, idx, 2, 1))

        profile = pp.Profile(pool4, [
            pp.StrictRank(pool4, [:A, :B, :C, :D]),
            pp.StrictRank(pool4, [:A, :B, :C, :D]),
            pp.StrictRank(pool4, [:B, :A, :C, :D]),
        ])
        result = pp.majority_graph_support(profile; basis = basis)
        tbl = pp.type_breaker_table(result; supporters_only = false)
        @test any(ismissing, tbl.boundary_distance)
    end

    @testset "WeightedProfile masses" begin
        ballots = [
            pp.StrictRank(pool4, [:A, :B, :C, :D]),
            pp.StrictRank(pool4, [:D, :C, :B, :A]),
        ]
        weights = [2.0, 1.0]
        wp = pp.WeightedProfile(pool4, ballots, weights)
        basis = pp.voter_type_basis(pool4)
        masses = pp.voter_type_masses(wp, basis)
        idx1 = basis.index_by_signature[(:A, :B, :C, :D)]
        idx2 = basis.index_by_signature[(:D, :C, :B, :A)]
        @test masses.counts_or_mass[idx1] ≈ 2.0
        @test masses.counts_or_mass[idx2] ≈ 1.0
        @test masses.proportions[idx1] ≈ 2 / 3
        @test masses.proportions[idx2] ≈ 1 / 3
    end

    @testset "group diagnostics" begin
        ballots = [
            pp.StrictRank(pool4, [:A, :B, :C, :D]),
            pp.StrictRank(pool4, [:A, :C, :B, :D]),
            pp.StrictRank(pool4, [:B, :A, :C, :D]),
            pp.StrictRank(pool4, [:D, :C, :B, :A]),
        ]
        groups = [:g1, :g1, :g2, :g2]
        profile = pp.Profile(pool4, ballots)
        gresult = pp.group_majority_graph_support(profile, groups)
        @test sum(gresult.group_mass) ≈ 1.0
        for eidx in eachindex(gresult.base.edges)
            @test sum(gresult.group_edge_margin[:, eidx]) ≈ gresult.base.edges[eidx].normalized_margin
        end
        @test nrow(pp.group_edge_power_table(gresult)) == length(gresult.groups) * length(gresult.base.edges)
        @test nrow(pp.group_anchor_table(gresult)) == length(gresult.groups)
    end
end

@testset "effective composition and reverse core public tables" begin
    pool = pp.CandidatePool([:A, :B, :C, :D])
    profile = pp.Profile(pool, [
        pp.StrictRank(pool, [:A, :B, :C, :D]),
        pp.StrictRank(pool, [:A, :C, :B, :D]),
        pp.StrictRank(pool, [:D, :C, :B, :A]),
        pp.StrictRank(pool, [:B, :A, :C, :D]),
    ])
    basis = pp.voter_type_basis(pool; order=:kendall_shell, reference_order=[:A, :B, :C, :D])
    result = pp.majority_graph_support(profile; basis=basis)
    E = length(result.edges)

    support_summary = pp.support_core_effective_composition_table(result)
    support_long = pp.support_core_above_threshold_type_table(result)
    reverse_summary = pp.reverse_core_effective_composition_table(result)
    reverse_long = pp.reverse_core_above_threshold_type_table(result)
    edge_summary = pp.edge_effective_composition_table(result)
    edge_long = pp.edge_above_threshold_type_table(result)
    counter = pp.countergraph_summary_table(result)

    @test all([:k, :core_mass, :hhi, :neff, :effective_threshold,
               :n_effective_above_threshold, :above_threshold_rankings] .∈ Ref(propertynames(support_summary)))
    @test all([:k, :reverse_core_mass, :hhi, :neff, :effective_threshold,
               :n_effective_above_threshold, :above_threshold_rankings] .∈ Ref(propertynames(reverse_summary)))
    @test all([:edge_index, :edge, :support_share, :support_hhi, :support_neff,
               :opposition_share, :opposition_hhi, :opposition_neff] .∈ Ref(propertynames(edge_summary)))
    @test all([:edge_index, :edge, :side, :type_index, :ranking, :profile_share,
               :conditional_share, :effective_threshold, :effective_weight] .∈ Ref(propertynames(edge_long)))
    @test nrow(counter) == 1
    @test counter.total_edges[1] == E

    for df in (support_long, reverse_long, edge_long)
        for row in eachrow(df)
            @test row.effective_weight ≈ row.conditional_share / row.effective_threshold
            @test row.effective_weight > 1.0
        end
    end

    @test support_summary.core_mass[support_summary.k .== 0][1] ≈ 1.0
    @test reverse_summary.reverse_core_mass[reverse_summary.k .== 0][1] ≈ 1.0
    @test all(diff(support_summary.core_mass) .<= 1e-8)
    @test all(diff(reverse_summary.reverse_core_mass) .<= 1e-8)
    @test all(result.coverage .+ (E .- result.coverage) .== E)
end
