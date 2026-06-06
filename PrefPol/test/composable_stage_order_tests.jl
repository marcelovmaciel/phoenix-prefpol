using TOML

@testset "composable stage order" begin
    repo_root = normpath(joinpath(@__DIR__, "..", ".."))
    stages_dir = joinpath(repo_root, "PrefPol", "composable_running", "stages")
    common_helper = joinpath(repo_root, "PrefPol", "composable_running", "stage_common.jl")
    numbered_stage_files = sort(filter(path -> occursin(r"^\d{2}_.*\.jl$", basename(path)), readdir(stages_dir; join = true)))

    @test isfile(common_helper)
    @test basename.(numbered_stage_files) == [
        "00_validate_configs.jl",
        "01_bootstrap.jl",
        "02_impute.jl",
        "03_linearize.jl",
        "04_measures.jl",
        "05_plot_global.jl",
        "06_plot_group.jl",
        "07_extra_measures.jl",
        "08_tables.jl",
        "09_extra_plots.jl",
        "10_lambda_table.jl",
        "11_collect_paper_artifacts.jl",
    ]

    numbered_include = r"include\s*\([^\n]*\"\d{2}_[^\"]+\.jl\""
    for path in numbered_stage_files
        source = read(path, String)
        @test !occursin(numbered_include, source)
        for match in eachmatch(r"include\s*\([^\n]*\"([^\"]+\.jl)\"", source)
            @test match.captures[1] == "stage_common.jl"
        end
    end

    wrapper = read(joinpath(repo_root, "PrefPol", "composable_running", "run_all_paper.jl"), String)
    called_stages = [match.captures[1] for match in eachmatch(r"joinpath\(stage_dir, \"(\d{2}_[^\"]+)\.jl\"\)", wrapper)]
    @test called_stages == [
        "00_validate_configs",
        "01_bootstrap",
        "02_impute",
        "03_linearize",
        "04_measures",
        "05_plot_global",
        "06_plot_group",
        "07_extra_measures",
        "08_tables",
        "09_extra_plots",
        "10_lambda_table",
        "11_collect_paper_artifacts",
    ]

    stale_refs = ["08_" * "extra_plots.jl", "09_" * "tables.jl"]
    scan_roots = [
        joinpath(repo_root, "PrefPol", "composable_running"),
        joinpath(repo_root, "PrefPol", "docs"),
        joinpath(repo_root, "PrefPol", "README.md"),
        joinpath(repo_root, "README.md"),
    ]
    stale_hits = String[]
    for root in scan_roots
        if isfile(root)
            text = read(root, String)
            any(ref -> occursin(ref, text), stale_refs) && push!(stale_hits, root)
        elseif isdir(root)
            for (dir, _, files) in walkdir(root)
                for file in files
                    path = joinpath(dir, file)
                    text = read(path, String)
                    any(ref -> occursin(ref, text), stale_refs) && push!(stale_hits, path)
                end
            end
        end
    end
    @test stale_hits == String[]

    publication = TOML.parsefile(joinpath(repo_root, "PrefPol", "config", "publication.toml"))
    expected_main_measures = ["Psi", "R", "HHI", "RHHI", "C", "D"]
    @test publication["run"]["measures"] == expected_main_measures
    @test publication["measure_sets"]["main"] == expected_main_measures
    @test isempty(intersect(Set(publication["run"]["measures"]), Set(["S", "O", "Sep", "lambda_sep"])))
end
