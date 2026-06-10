cd(@__DIR__)

using Documenter
using Documenter.Remotes

const REPO_ROOT = normpath(joinpath(@__DIR__, "..", ".."))

push!(LOAD_PATH, joinpath(@__DIR__, ".."))
using PreferenceProfiles


makedocs(;
    sitename = "PreferenceProfiles.jl",
    modules = [PreferenceProfiles],
    checkdocs = :none,
    format = Documenter.HTML(
        size_threshold_warn = 400 * 1024,
        size_threshold = 400 * 1024,
    ),
    remotes = Dict(
        REPO_ROOT => Remotes.GitHub("marcelovmaciel", "phoenix-prefpol"),
    ),
    pages = [
        "Home" => "index.md",
        "Workflow" => "workflow.md",
        "Examples" => "examples.md",
        "From Scores to Profiles" => "tabular_profiles.md",
        "Weak Orders and Linearization" => "weak_orders.md",
        "Annotated Profiles" => "annotated_profiles.md",
        "Global Profile Diagnostics" => "global_measures.md",
        "Single-Peakedness Diagnostics" => "single_peakedness.md",
        "Group Diagnostics" => "group_measures.md",
        "Majority-Graph Support" => "majority_support.md",
        "Majority-Graph Roles" => "majority_roles.md",
        "Plurality Switch Tables" => "plurality_switch.md",
        "Advanced Representations" => "advanced_representations.md",
        "API Reference" => "api.md",
        "Full Public API" => "api_full.md",
    ],
)
