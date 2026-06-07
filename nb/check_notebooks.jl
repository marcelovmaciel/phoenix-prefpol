#!/usr/bin/env julia

using Test

const NB_ROOT = @__DIR__
const REPO_ROOT = normpath(joinpath(NB_ROOT, ".."))

function notebook_source_files()
    files = [joinpath(NB_ROOT, name) for name in readdir(NB_ROOT) if occursin(r"^\d{2}_.*\.jl$", name)]
    push!(files, joinpath(NB_ROOT, "notebook_common.jl"))
    return sort(files)
end

function code_lines(path::AbstractString)
    out = String[]
    in_md = false
    for line in eachline(path)
        if in_md
            occursin("\"\"\"", line) && (in_md = false)
            continue
        end
        if occursin("md\"\"\"", line)
            count(==('"'), line) >= 6 || (in_md = true)
            continue
        end
        stripped = strip(line)
        isempty(stripped) && continue
        startswith(stripped, "#") && continue
        push!(out, line)
    end
    return out
end

@testset "notebook source parses and stays notebook-local" begin
    files = notebook_source_files()
    @test !isempty(files)
    for path in files
        rel = relpath(path, REPO_ROOT)
        text = read(path, String)
        @testset "$rel" begin
            @test Meta.parseall(text) !== nothing
            code = code_lines(path)
            @test !any(line -> occursin("publication.toml", line), code)
            @test !any(line -> occursin("run_all_paper.jl", line), code)
            @test !any(line -> occursin("PrefPol/composable_running/output/publication", line) && occursin(r"CSV\.write|write_notebook_csv|open\(|mkpath|output_root|cache_root|write\(", line), code)
        end
    end
end

println("Notebook source check passed. Run nb/test_notebook_provenance.jl for provenance and dynamic notebook integrity checks.")
