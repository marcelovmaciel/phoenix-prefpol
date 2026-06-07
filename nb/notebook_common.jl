const _NB_REPO_ROOT = normpath(joinpath(@__DIR__, ".."))
# PrefPol is resolved from the active nb/Project.toml environment. Do not
# force PrefPol/ onto LOAD_PATH, because that bypasses the notebook manifest and
# can trigger a separate package precompile path.

using CSV
using DataFrames
using Markdown
using Statistics
using PrettyTables
using PlutoUI
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


function notebook_target_table(cfg)
    rows = NamedTuple[]
    for target in notebook_targets(cfg)
        for m in target.m_values
            push!(rows, (
                label = "$(target.wave_id) / $(target.scenario_name) / m=$(m)",
                wave_id = target.wave_id,
                scenario_name = target.scenario_name,
                m = m,
            ))
        end
    end
    return DataFrame(rows)
end

function notebook_target_labels(cfg)
    table = notebook_target_table(cfg)
    return String.(table.label)
end

function selected_target_row(cfg, label::AbstractString)
    rows = notebook_target_table(cfg)
    matches = rows[String.(rows.label) .== String(label), :]
    nrow(matches) == 1 || error("Expected one target row for $(label), found $(nrow(matches)).")
    return matches[1, :]
end

function batch_item_table(batch::pp.StudyBatchSpec)
    return DataFrame([
        (
            batch_index = idx,
            year = item.metadata.year,
            wave_id = item.spec.wave_id,
            scenario_name = item.metadata.scenario_name,
            m = item.metadata.m,
            imputer_backend = String(item.spec.imputer_backend),
            linearizer_policy = String(item.spec.linearizer_policy),
            active_candidates = join(item.spec.active_candidates, ", "),
            groupings = join(String.(item.spec.groupings), ", "),
            measures = join(String.(item.spec.measures), ", "),
            B = item.spec.B,
            R = item.spec.R,
            K = item.spec.K,
        )
        for (idx, item) in enumerate(batch.items)
    ])
end

function select_batch_item(batch::pp.StudyBatchSpec;
                           wave_id,
                           scenario_name,
                           m,
                           backend = nothing,
                           linearizer = nothing)
    for (idx, item) in enumerate(batch.items)
        item.spec.wave_id == String(wave_id) || continue
        String(item.metadata.scenario_name) == String(scenario_name) || continue
        Int(item.metadata.m) == Int(m) || continue
        backend === nothing || String(item.spec.imputer_backend) == String(backend) || continue
        linearizer === nothing || String(item.spec.linearizer_policy) == String(linearizer) || continue
        return idx, item
    end
    error("No notebook batch item matches wave=$(wave_id), scenario=$(scenario_name), m=$(m), backend=$(backend), linearizer=$(linearizer).")
end

function candidate_score_preview(df, candidate_cols; n = 8)
    cols = intersect([:respondent_id, :id, :weight, Symbol.(candidate_cols)...], propertynames(df))
    isempty(cols) && (cols = Symbol.(candidate_cols))
    return first(DataFrame(df[:, cols]), min(n, nrow(df)))
end

function candidate_missingness_table(df, candidate_cols)
    rows = NamedTuple[]
    total_rows = nrow(df)
    for candidate in candidate_cols
        miss = count(value -> pp.is_eseb_missing_score(value), df[!, candidate])
        push!(rows, (
            candidate = String(candidate),
            missing_cells = miss,
            total_rows = total_rows,
            missing_share = total_rows == 0 ? missing : miss / total_rows,
        ))
    end
    return DataFrame(rows)
end

function resample_multiplicity_table(artifact; n = 20)
    multiplicities = Vector{Int}(artifact.multiplicities)
    rows = DataFrame(
        observed_row = collect(eachindex(multiplicities)),
        multiplicity = multiplicities,
    )
    sort!(rows, [:multiplicity, :observed_row]; rev = [true, false])
    return first(rows, min(n, nrow(rows)))
end

function before_after_score_rows(before_df, after_df, candidate_cols, row_indices)
    rows = NamedTuple[]
    for row_index in row_indices
        row_index <= nrow(before_df) || continue
        row_index <= nrow(after_df) || continue
        for candidate in candidate_cols
            push!(rows, (
                row = row_index,
                candidate = String(candidate),
                before = before_df[row_index, candidate],
                after = after_df[row_index, candidate],
                changed = !isequal(before_df[row_index, candidate], after_df[row_index, candidate]),
            ))
        end
    end
    return DataFrame(rows)
end

function is_under_directory(child_path::AbstractString, parent_path::AbstractString)
    relative_path = relpath(normpath(child_path), normpath(parent_path))
    return relative_path == "." || (!startswith(relative_path, "..") && !isabspath(relative_path))
end

function notebook_provenance_table(settings; extra = NamedTuple())
    base = (
        output_root = settings.output_root,
        cache_root = settings.cache_root,
        B = settings.B,
        R = settings.R,
        K = settings.K,
    )
    pairs = collect(pairs(merge(base, extra)))
    return DataFrame(item = String.(first.(pairs)), value = string.(last.(pairs)))
end
