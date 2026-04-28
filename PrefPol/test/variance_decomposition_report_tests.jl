using Test
using PrefPol
using DataFrames
using Statistics

@testset "tree variance decomposition components sum to total" begin
    leaf_table = DataFrame(
        measure = fill(:Psi, 8),
        grouping = fill(missing, 8),
        b = repeat(1:2; inner = 4),
        r = repeat(1:2; inner = 2, outer = 2),
        k = repeat(1:2; outer = 4),
        value = [
            -3.5, -2.5, 0.5, 1.5,
            -1.5, -0.5, 2.5, 3.5,
        ],
    )

    decomposed = PrefPol.tree_variance_decomposition_table(leaf_table)

    @test nrow(decomposed) == 1
    @test isapprox(
        decomposed[1, :total_variance],
        decomposed[1, :bootstrap_variance] +
        decomposed[1, :imputation_variance] +
        decomposed[1, :linearization_variance];
        atol = 1e-12,
    )
end

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

    @test unique(fine.measure) == [:Psi, :R, :HHI, :RHHI, :C, :D, :S, :Sep]
    @test labels == ["Ψ", "R", "HHI", "RHHI", "C", "D", "S", "1-O"]
    @test !(:D_median in fine.measure)

    explicit = PrefPol.variance_decomposition_fine_table(
        input,
        PrefPol.VarianceDecompositionReportSpec(measures = [:HHI, :D_median, :D_consensus, Symbol("1-O")]),
    )
    @test unique(explicit.measure) == [:HHI, :D_median, :D, :Sep]
end

function _compact_boxplot_fixture()
    rows = NamedTuple[]
    for m in (2, 3), (backend, policy) in (("mice", "pattern_conditional"), ("zero", "random_ties"))
        pipeline_offset = backend == "mice" ? 0.0 : 10.0
        for (measure, grouping, measure_offset) in (
            (:Psi, missing, 0.0),
            (:C, :Age, 100.0),
            (:C, :Education, 200.0),
        )
            base = m + pipeline_offset + measure_offset
            push!(rows, (
                year = 2022,
                scenario_name = "main",
                m = m,
                grouping = grouping,
                measure = measure,
                bootstrap_variance = base + 1.0,
                imputation_variance = base + 2.0,
                linearization_variance = base + 3.0,
                total_variance = 3.0 * base + 6.0,
                B = 2,
                R = 2,
                K = 2,
                imputer_backend = backend,
                linearizer_policy = policy,
            ))
        end
    end
    return DataFrame(rows)
end

@testset "variance decomposition compact boxplot table pools grouping rows into measure panels" begin
    input = _compact_boxplot_fixture()

    plot_rows = PrefPol.variance_decomposition_year_scenario_boxplot_table(
        input;
        year = 2022,
        scenario_name = "main",
        measures = [:Psi, :C],
        value_kind = :variance,
    )

    @test Set(plot_rows.panel_label) == Set(["Ψ", "C"])
    @test all(ismissing, plot_rows.panel_grouping)
    @test sort(unique(plot_rows.m)) == [2, 3]
    @test Set(plot_rows.component) == Set([:bootstrap, :imputation, :linearization])

    c_bootstrap_m2 = plot_rows[
        (plot_rows.measure .== :C) .&
        (plot_rows.component .== :bootstrap) .&
        (plot_rows.m .== 2),
        :,
    ]
    @test nrow(c_bootstrap_m2) == 4
    @test Set(skipmissing(c_bootstrap_m2.grouping)) == Set([:Age, :Education])
    @test length(unique(c_bootstrap_m2.pipeline_label)) == 2

    psi_bootstrap_m2 = plot_rows[
        (plot_rows.measure .== :Psi) .&
        (plot_rows.component .== :bootstrap) .&
        (plot_rows.m .== 2),
        :,
    ]
    @test nrow(psi_bootstrap_m2) == 2
    @test all(ismissing, psi_bootstrap_m2.grouping)

    diagnostic_rows = PrefPol.variance_decomposition_year_scenario_boxplot_table(
        input;
        year = 2022,
        scenario_name = "main",
        measures = [:Psi, :C],
        value_kind = :variance,
        group_pooling = :none,
    )
    @test Set(diagnostic_rows.panel_label) == Set(["Ψ", "C | Age", "C | Education"])
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

@testset "variance decomposition report preserves m by default" begin
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
        B = [2, 2],
        R = [2, 2],
        K = [2, 2],
        imputer_backend = ["mice", "mice"],
        linearizer_policy = ["pattern_conditional", "pattern_conditional"],
    )

    fine, pooled = PrefPol.variance_decomposition_report(
        input,
        PrefPol.VarianceDecompositionReportSpec(measures = [:Psi]),
    )
    bootstrap = pooled[pooled.component .== :bootstrap, :]

    @test sort(unique(fine.m)) == [2, 3]
    @test :m in Symbol.(names(pooled))
    @test nrow(bootstrap) == 2
    @test sort(bootstrap.m) == [2, 3]

    pooled_over_m = PrefPol.variance_decomposition_pooled_table(
        fine,
        PrefPol.VarianceDecompositionReportSpec(measures = [:Psi], pool_over_m = true),
    )
    @test !(:m in Symbol.(names(pooled_over_m)))
    @test nrow(pooled_over_m[pooled_over_m.component .== :bootstrap, :]) == 1
end

@testset "variance decomposition by-m plotting table computes shares" begin
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
        B = [2, 2],
        R = [2, 2],
        K = [2, 2],
        imputer_backend = ["mice", "zero"],
        linearizer_policy = ["pattern_conditional", "random_ties"],
    )

    plot_rows = PrefPol.variance_decomposition_by_m_plot_table(input; value_kind = :share)

    @test all(col -> col in Symbol.(names(plot_rows)),
              [:m, :component, :component_label, :value, :measure, :pipeline_label])
    @test Set(plot_rows.component) == Set([:bootstrap, :imputation, :linearization])
    @test "Bootstrap" in plot_rows.component_label
    @test "mice + pattern_conditional" in plot_rows.pipeline_label

    for subdf in groupby(plot_rows, [:year, :scenario_name, :m, :measure, :pipeline_label])
        @test isapprox(sum(subdf.value), 1.0; atol = 1e-12)
    end
end

@testset "variance decomposition year/scenario boxplot table preserves cell keys" begin
    input = DataFrame(
        year = repeat([2022], 4),
        scenario_name = repeat(["main"], 4),
        m = [2, 2, 3, 3],
        grouping = fill(missing, 4),
        measure = fill(:Psi, 4),
        bootstrap_variance = [1.0, 2.0, 3.0, 4.0],
        imputation_variance = [2.0, 3.0, 4.0, 5.0],
        linearization_variance = [5.0, 5.0, 7.0, 7.0],
        total_variance = [8.0, 10.0, 14.0, 16.0],
        B = fill(2, 4),
        R = fill(2, 4),
        K = fill(2, 4),
        imputer_backend = ["mice", "zero", "mice", "zero"],
        linearizer_policy = ["pattern_conditional", "random_ties", "pattern_conditional", "random_ties"],
    )

    plot_rows = PrefPol.variance_decomposition_year_scenario_boxplot_table(
        input;
        year = 2022,
        scenario_name = "main",
        measures = [:Psi],
        value_kind = :share,
    )

    @test all(col -> col in Symbol.(names(plot_rows)),
              [:year, :scenario_name, :m, :measure, :component, :pipeline_label])
    @test sort(unique(plot_rows.m)) == [2, 3]
    @test Set(plot_rows.component) == Set([:bootstrap, :imputation, :linearization])
    @test "mice + pattern_conditional" in plot_rows.pipeline_label

    for subdf in groupby(plot_rows, [:year, :scenario_name, :m, :measure, :pipeline_label])
        if all(subdf.value .> 0)
            @test isapprox(sum(subdf.value), 1.0; atol = 1e-12)
        end
    end
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
