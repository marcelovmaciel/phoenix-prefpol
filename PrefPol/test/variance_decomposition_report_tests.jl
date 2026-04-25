using Test
using PrefPol
using DataFrames
using Statistics

function _report_decomposition_fixture()
    return DataFrame(
        year = [2022, 2022, 2022, 2022, 2022, 2022, 2022, 2022, 2022, 2022],
        wave_id = fill("w2022", 10),
        scenario_name = fill("main", 10),
        m = [2, 2, 2, 2, 2, 2, 2, 2, 2, 2],
        grouping = [missing, missing, missing, missing, :grp, :grp, :grp, :grp, :grp, :grp],
        measure = [:Psi, :R, :HHI, :RHHI, :C, :D, :Sep, :S, :D_median, :G],
        estimate = fill(0.5, 10),
        bootstrap_variance = collect(1.0:10.0),
        imputation_variance = collect(11.0:20.0),
        linearization_variance = collect(21.0:30.0),
        total_variance = collect(33.0:42.0),
        empirical_variance = collect(43.0:52.0),
        B = fill(2, 10),
        R = fill(2, 10),
        K = fill(2, 10),
        imputer_backend = fill("mice", 10),
        linearizer_policy = fill("random_ties", 10),
        consensus_tie_policy = fill("average", 10),
    )
end

@testset "variance decomposition report reuses decomposition rows" begin
    input = _report_decomposition_fixture()
    spec = PrefPol.VarianceDecompositionReportSpec(measures = [:Psi, :D], include_empirical = true)
    fine = PrefPol.variance_decomposition_fine_table(input, spec)

    @test nrow(fine) == 10
    @test Set(fine.component) == Set([:bootstrap, :imputation, :linearization, :total, :empirical])
    @test fine[fine.measure .== :Psi .&& fine.component .== :bootstrap, :value] == [1.0]
    @test fine[fine.measure .== :D .&& fine.component .== :empirical, :value] == [48.0]
    @test all(fine.estimator .== :existing_nested_moments)
end

@testset "variance decomposition report recomputes synthetic leaf tables" begin
    leaf_table = DataFrame(
        measure = fill(:Psi, 8),
        grouping = fill(missing, 8),
        year = fill(2022, 8),
        m = fill(2, 8),
        b = repeat(1:2; inner = 4),
        r = repeat(1:2; inner = 2, outer = 2),
        k = repeat(1:2; outer = 4),
        value = [
            -3.5, -2.5, 0.5, 1.5,
            -1.5, -0.5, 2.5, 3.5,
        ],
    )

    fine = PrefPol.variance_decomposition_fine_table(leaf_table)
    values = Dict(row.component => row.value for row in eachrow(fine))

    @test nrow(fine) == 4
    @test values[:bootstrap] > 0
    @test values[:imputation] > 0
    @test values[:linearization] > 0
    @test isapprox(values[:total], values[:bootstrap] + values[:imputation] + values[:linearization]; atol = 1e-12)
    @test fine[1, :B] == 2
    @test fine[1, :R] == 2
    @test fine[1, :K] == 2
end

@testset "variance decomposition report paper defaults and explicit measures" begin
    input = _report_decomposition_fixture()
    fine = PrefPol.variance_decomposition_fine_table(input)
    labels = unique(fine.measure_label)

    @test unique(fine.measure) == [:Psi, :R, :HHI, :RHHI, :C, :D, :Sep, :S]
    @test labels == ["Ψ", "R", "HHI", "RHHI", "C", "D", "1-O", "S"]
    @test !(:D_median in fine.measure)

    explicit = PrefPol.variance_decomposition_fine_table(
        input,
        PrefPol.VarianceDecompositionReportSpec(measures = [:D_median, :D_consensus, Symbol("1-O")]),
    )
    @test unique(explicit.measure) == [:D_median, :D, :Sep]
end

@testset "variance decomposition report derives paper 1-O from O rows" begin
    input = _report_decomposition_fixture()
    input = input[input.measure .!= :Sep, :]
    input.measure[input.measure .== :G] .= :O
    input.estimate[input.measure .== :O] .= 0.3

    fine = PrefPol.variance_decomposition_fine_table(input)
    sep = fine[fine.measure .== :Sep, :]

    @test :Sep in unique(fine.measure)
    @test all(sep.measure_label .== "1-O")
    @test sep[sep.component .== :bootstrap, :value] == [10.0]
    @test !(:O in unique(fine.measure))

    all_measures = PrefPol.variance_decomposition_fine_table(
        input,
        PrefPol.VarianceDecompositionReportSpec(measures = :all),
    )
    @test :Sep in unique(all_measures.measure)
    @test :O in unique(all_measures.measure)
end

@testset "variance decomposition report pooling summarizes cells" begin
    input = DataFrame(
        year = [2022, 2022],
        scenario_name = ["main", "main"],
        m = [2, 3],
        grouping = [missing, missing],
        measure = [:Psi, :Psi],
        bootstrap_variance = [1.0, 3.0],
        imputation_variance = [2.0, 4.0],
        linearization_variance = [5.0, 7.0],
        total_variance = [8.0, 14.0],
        empirical_variance = [8.0, 14.0],
        B = [2, 2],
        R = [2, 2],
        K = [2, 2],
    )
    spec = PrefPol.VarianceDecompositionReportSpec(measures = [:Psi], pool_over_m = true)
    fine, pooled = PrefPol.variance_decomposition_report(input, spec)
    bootstrap = pooled[pooled.component .== :bootstrap, :]

    @test nrow(fine) == 8
    @test nrow(bootstrap) == 1
    @test bootstrap[1, :n_cells_pooled] == 2
    @test bootstrap[1, :median] == 2.0
    @test bootstrap[1, :m_pool_label] == "m=2:3"

    unpooled = PrefPol.variance_decomposition_pooled_table(
        fine,
        PrefPol.VarianceDecompositionReportSpec(measures = [:Psi], pool_over_m = false),
    )
    @test nrow(unpooled[unpooled.component .== :bootstrap, :]) == 2
end

@testset "variance decomposition report keeps global rows once with grouping filters" begin
    input = _report_decomposition_fixture()
    fine = PrefPol.variance_decomposition_fine_table(
        input,
        PrefPol.VarianceDecompositionReportSpec(measures = [:Psi, :C], groupings = [:grp]),
    )

    @test nrow(fine[fine.measure .== :Psi, :]) == 4
    @test all(ismissing, fine[fine.measure .== :Psi, :grouping])
    @test nrow(fine[fine.measure .== :C, :]) == 4
end
