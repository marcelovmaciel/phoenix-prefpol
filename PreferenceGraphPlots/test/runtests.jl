using Test
using Preferences
using PreferenceGraphPlots

@testset "PreferenceGraphPlots" begin
    pool = CandidatePool([:A, :B, :C])
    profile = Profile(pool, [
        StrictRank(pool, [:A, :B, :C]),
        StrictRank(pool, [:A, :C, :B]),
        StrictRank(pool, [:B, :A, :C]),
        StrictRank(pool, [:C, :A, :B]),
    ])
    basis = voter_type_basis(pool; order=:kendall_shell, reference_order=[:A, :B, :C])
    result = majority_graph_support(profile; basis=basis)
    dir = mktempdir()

    path1 = joinpath(dir, "plurality.png")
    plot_plurality_scores(profile; output_path=path1)
    @test isfile(path1)
    @test filesize(path1) > 0

    path2 = joinpath(dir, "margins.png")
    plot_pairwise_margins(result; output_path=path2)
    @test isfile(path2)
    @test filesize(path2) > 0
end
