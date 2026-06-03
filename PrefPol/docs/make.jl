cd(@__DIR__)

using Documenter

push!(LOAD_PATH, joinpath(@__DIR__, ".."))
using PrefPol

makedocs(;
    sitename = "PrefPol.jl",
    modules = [PrefPol],
    checkdocs = :none,
    format = Documenter.HTML(
        repolink = "https://github.com/marcelovmaciel/phoenix-prefpol",
        size_threshold_warn = 500 * 1024,
        size_threshold = 500 * 1024,
    ),
    remotes = nothing,
    pages = [
        "Home" => "index.md",
        "API Reference" => "api.md",
    ],
)
