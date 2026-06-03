cd(@__DIR__)

using Documenter

push!(LOAD_PATH, joinpath(@__DIR__, ".."))
using Preferences

makedocs(;
    sitename = "Preferences.jl",
    modules = [Preferences],
    checkdocs = :exports,
    format = Documenter.HTML(
        repolink = "https://github.com/marcelovmaciel/phoenix-prefpol",
        size_threshold_warn = 400 * 1024,
        size_threshold = 400 * 1024,
    ),
    remotes = nothing,
    pages = [
        "Home" => "index.md",
        "Examples" => "examples.md",
        "API Reference" => "api.md",
    ],
)
