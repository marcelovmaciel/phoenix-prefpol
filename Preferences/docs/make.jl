cd(@__DIR__)

using Documenter


makedocs(;
    sitename = "Preferences.jl",
    checkdocs = :none,
    format = Documenter.HTML(
        repolink = "https://github.com/marcelovmaciel/phoenix-prefpol",
        size_threshold_warn = 400 * 1024,
        size_threshold = 400 * 1024,
    ),
    remotes = nothing,
    pages = [
        "Home" => "index.md",
        "Workflow" => "workflow.md",
        "Examples" => "examples.md",
        "From Scores to Profiles" => "tabular_profiles.md",
        "Weak Orders and Linearization" => "weak_orders.md",
        "Annotated Profiles" => "annotated_profiles.md",
        "Global Profile Diagnostics" => "global_measures.md",
        "Group Diagnostics" => "group_measures.md",
        "Majority-Graph Support" => "majority_support.md",
        "Majority-Graph Roles" => "majority_roles.md",
        "Plurality Switch Tables" => "plurality_switch.md",
        "Advanced Representations" => "advanced_representations.md",
        "API Reference" => "api.md",
        "Full Public API" => "api_full.md",
    ],
)
