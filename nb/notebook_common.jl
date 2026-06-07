const _NB_REPO_ROOT = normpath(joinpath(@__DIR__, ".."))
# PrefPol is resolved from the active nb/Project.toml environment. Do not
# force PrefPol/ onto LOAD_PATH, because that bypasses the notebook manifest and
# can trigger a separate package precompile path.

using CSV
using DataFrames
using Markdown
using PrettyTables
using TOML

include(joinpath(@__DIR__, "..", "PrefPol", "composable_running", "stage_common.jl"))

const NOTEBOOK_PUBLICATION_OUTPUT = normpath(
    joinpath(@__DIR__, "..", "PrefPol", "composable_running", "output", "publication"),
)

repo_root() = _NB_REPO_ROOT
prefpol_root() = joinpath(repo_root(), "PrefPol")
nb_root() = @__DIR__

function load_notebook_config(path = joinpath(nb_root(), "notebook_config.toml"))
    resolved = resolve_nb_path(path)
    isfile(resolved) || error("Notebook config not found: $(resolved)")
    cfg = TOML.parsefile(resolved)
    notebook_output_root(cfg)
    notebook_cache_root(cfg)
    return cfg
end

function resolve_nb_path(path)
    path === nothing && return nothing
    text = String(path)
    isempty(text) && return text
    isabspath(text) && return normpath(text)

    first_component = first(splitpath(text))
    if first_component in ("nb", "PrefPol", "Preferences", "writing", "intermediate_data")
        return normpath(joinpath(repo_root(), text))
    end
    return normpath(joinpath(nb_root(), text))
end

function ensure_not_publication_output!(path)
    resolved = resolve_nb_path(path)
    rel = relpath(resolved, NOTEBOOK_PUBLICATION_OUTPUT)
    if rel == "." || (!startswith(rel, "..") && !isabspath(rel))
        error(
            "Notebook output path points inside the publication output tree: $(resolved). " *
            "Use nb/output/... instead.",
        )
    end
    return resolved
end

function _run_table(cfg)
    return get(cfg, "run", Dict{String,Any}())
end

function notebook_output_root(cfg)
    run_cfg = _run_table(cfg)
    path = String(config_value(run_cfg, "output_root", "nb/output/notebook_smoke"))
    return ensure_not_publication_output!(path)
end

function notebook_cache_root(cfg)
    run_cfg = _run_table(cfg)
    path = String(config_value(run_cfg, "cache_root", "nb/output/notebook_smoke/cache"))
    return ensure_not_publication_output!(path)
end

function load_notebook_waves()
    return load_waves()
end

function _notebook_opts()
    return Dict{String,Any}(
        "year" => nothing,
        "scenario" => nothing,
        "m" => nothing,
        "backend" => nothing,
        "linearizer" => nothing,
        "force" => false,
        "dry-run" => false,
        "smoke-test" => true,
    )
end

function notebook_targets(cfg)
    return selected_targets(cfg, _notebook_opts())
end

function notebook_settings(cfg)
    settings = run_settings(cfg, _notebook_opts())
    return merge(settings, (
        output_root = notebook_output_root(cfg),
        cache_root = notebook_cache_root(cfg),
    ))
end

function build_notebook_batch(cfg)
    _, _, wave_by_id = load_notebook_waves()
    return build_batch(notebook_targets(cfg), wave_by_id, notebook_settings(cfg))
end

function small_table(df; n = 10)
    table = first(DataFrame(df), min(n, nrow(df)))
    io = IOBuffer()
    pretty_table(io, table; backend = :markdown)
    return Markdown.parse(String(take!(io)))
end

function write_notebook_csv(path, df)
    resolved = ensure_not_publication_output!(path)
    mkpath(dirname(resolved))
    CSV.write(resolved, df)
    return resolved
end
