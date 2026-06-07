### A Pluto.jl notebook ###
# v0.20.17

using Markdown

# ╔═╡ 4f9392f4-6ccb-4b08-9ebb-caf4b9970b7b
begin
    import Pkg
    Pkg.activate(@__DIR__)
end

# ╔═╡ 3360df3d-99d3-4b61-8475-883ac08d8a9a
md"""
# Paper Artifact Map

This notebook explains the paper artifact collection layer. It corresponds
conceptually to the production artifact-collection pass, but it does not include
or call that stage file.

The collection layer is intentionally thin: generated figures and tables remain
owned by their source stages, while `PrefPol/config/paper_artifacts.toml`
declares which generated files should be exposed under paper-facing filenames.
This notebook inspects that mapping only. It does not copy artifacts by default.
"""

# ╔═╡ a7719e62-0aa3-4435-b9e9-140e45cedaaa
begin
    # Load shared notebook helpers and the local PrefPol package.
    include(joinpath(@__DIR__, "notebook_common.jl"))
end

# ╔═╡ b912af95-d82e-4b12-b979-92e8b04c7aa7
begin
    cfg = load_notebook_config()
    settings = notebook_settings(cfg)
    artifact_config_path = joinpath(prefpol_root(), "config", "paper_artifacts.toml")
    artifact_cfg = TOML.parsefile(artifact_config_path)
    notebook_orchestration_cfg = cfg
end

# ╔═╡ 3a58d48e-c621-432e-b2c8-40b0ae653ec3
begin
    @assert settings.B <= 5 "Notebook config keeps B <= 5."
    @assert settings.R <= 5 "Notebook config keeps R <= 5."
    @assert settings.K <= 5 "Notebook config keeps K <= 5."
    ensure_not_publication_output!(settings.output_root)
end

# ╔═╡ 26a1422a-9e16-4c38-bcfc-5f0a265696d6
md"""
## Collection Configuration

`paper_artifacts.toml` provides collection defaults and the artifact list. The
notebook config may override the collection defaults for local inspection. The
notebook displays the resolved layer so the source manifests and destination
names are explicit.
"""

# ╔═╡ 50436a4d-d449-4306-82e2-005549db5b07
function merged_collection_config(artifact_cfg, orchestration_cfg)
    collection = Dict{String,Any}()
    haskey(artifact_cfg, "collection") && merge!(collection, artifact_cfg["collection"])

    if haskey(orchestration_cfg, "paper_artifacts")
        override = orchestration_cfg["paper_artifacts"]
        haskey(override, "collection") && merge!(collection, override["collection"])
        for (key, value) in override
            key == "collection" && continue
            collection[key] = value
        end
    end

    return collection
end

# ╔═╡ d68d5803-b80f-4c9a-a161-fd4f4f259b2b
collection_cfg = merged_collection_config(artifact_cfg, notebook_orchestration_cfg)

# ╔═╡ 26484b47-d6fb-4e99-8f1e-8777941c7715
begin
    output_root = resolve_path(String(config_value(collection_cfg, "output_root", DEFAULT_OUTPUT_ROOT)))
    manifests_root = resolve_path(String(config_value(
        collection_cfg,
        "manifests_root",
        joinpath(output_root, "manifests"),
    )))
    destination_root = resolve_path(String(config_value(
        collection_cfg,
        "destination_root",
        joinpath(output_root, "paper_artifacts"),
    )))
    paper_artifact_manifest = resolve_path(String(config_value(
        collection_cfg,
        "paper_artifact_manifest",
        joinpath(manifests_root, "paper_artifact_manifest.csv"),
    )))
end

# ╔═╡ d92263ca-0847-4c14-8e85-32f32f39ab44
collection_summary = DataFrame(
    item = [
        "artifact config",
        "notebook config",
        "output root",
        "manifests root",
        "destination root",
        "paper artifact manifest",
        "copy mode",
        "update writing imgs",
        "default backend",
        "default linearizer",
    ],
    value = [
        artifact_config_path,
        joinpath(nb_root(), "notebook_config.toml"),
        portable_path(output_root),
        portable_path(manifests_root),
        portable_path(destination_root),
        portable_path(paper_artifact_manifest),
        String(config_value(collection_cfg, "copy_mode", "copy")),
        string(Bool(config_value(collection_cfg, "update_writing_imgs", false))),
        String(config_value(collection_cfg, "default_backend", "mice")),
        String(config_value(collection_cfg, "default_linearizer", "pattern_conditional")),
    ],
)

# ╔═╡ 92944e84-f7ed-4a05-9759-502d85cbdd4f
small_table(collection_summary; n = nrow(collection_summary))

# ╔═╡ 6af57865-5965-448c-950c-ab43cf573893
md"""
## Source Manifests

The collector resolves most source files through stage manifests. Global plots,
group plots, extra plots, tables, and the optional lambda table each have their
own source manifest path.
"""

# ╔═╡ 56199d4f-f820-4a3a-9923-c2dd9a310e83
source_manifests = Dict(
    "plot_global" => resolve_path(String(config_value(collection_cfg, "plot_manifest", joinpath(manifests_root, "plot_manifest.csv")))),
    "plot_group" => resolve_path(String(config_value(collection_cfg, "group_plot_manifest", joinpath(manifests_root, "group_plot_manifest.csv")))),
    "extra_plots" => resolve_path(String(config_value(collection_cfg, "extra_plot_manifest", joinpath(manifests_root, "extra_plot_manifest.csv")))),
    "tables" => resolve_path(String(config_value(collection_cfg, "table_manifest", joinpath(manifests_root, "table_manifest.csv")))),
    "lambda_table" => resolve_path(String(config_value(collection_cfg, "lambda_table_manifest", joinpath(manifests_root, "lambda_table_manifest.csv")))),
)

# ╔═╡ d829a85b-c76b-4ea5-a620-a1f0c854e756
source_manifest_table = DataFrame(
    source_stage = collect(keys(source_manifests)),
    source_manifest = portable_path.(collect(values(source_manifests))),
    exists_now = isfile.(collect(values(source_manifests))),
)

# ╔═╡ 8f27f4ac-c7e1-41c6-a7a2-2eedc2d83711
small_table(sort(source_manifest_table, :source_stage); n = nrow(source_manifest_table))

# ╔═╡ b3cb084a-b4dd-4dc9-8fea-e3cf62b33bd8
md"""
## Artifact Specs

Each `[[artifacts]]` row declares a paper-facing destination filename and a
source stage. Some entries identify a source artifact directly. Others rely on
the collector's inference rules: global plots use a `global_measures` artifact
prefix, group plots use a `paper_group_heatmap_panel` prefix, and table/extra
plot/lambda entries default to the destination filename stem.
"""

# ╔═╡ 02d014b9-fc21-4f63-8b8a-c0f1bd4a22bd
function source_stage_label(source_stage::AbstractString)
    if source_stage == "plot_global"
        return "global plots"
    elseif source_stage == "plot_group"
        return "group plots"
    elseif source_stage == "extra_plots"
        return "extra plots"
    elseif source_stage == "tables"
        return "tables"
    elseif source_stage == "lambda_table"
        return "optional lambda table"
    end
    return "other"
end

# ╔═╡ 33a5664d-36e2-42d5-a6fa-72b986b4c893
function lambda_table_enabled(orchestration_cfg)
    lambda_cfg = get(orchestration_cfg, "lambda_table", nothing)
    lambda_cfg isa AbstractDict || return false
    return Bool(get(lambda_cfg, "enabled", false))
end

# ╔═╡ da11b43d-f4ec-4c31-8528-f6c290da8a6d
function inferred_source_reference(spec)
    source_path = config_value(spec, "source_path", nothing)
    source_path === nothing || return String(source_path)

    explicit_id = config_value(spec, "source_artifact_id", nothing)
    explicit_id === nothing || return String(explicit_id)

    source_stage = String(config_value(spec, "source_stage", ""))
    destination_filename = String(config_value(spec, "destination_filename", ""))
    if source_stage in ("extra_plots", "tables", "lambda_table")
        return splitext(destination_filename)[1]
    elseif source_stage == "plot_global"
        return "artifact prefix: global_measures"
    elseif source_stage == "plot_group"
        return "artifact prefix: paper_group_heatmap_panel"
    end
    return ""
end

# ╔═╡ ef47c590-4f75-4352-9ab4-2e19f1d2249b
function source_stage_included(source_stage::AbstractString, orchestration_cfg)
    source_stage == "lambda_table" || return true
    return lambda_table_enabled(orchestration_cfg)
end

# ╔═╡ 8c655e77-6a6a-4197-a3ad-b5c66b5e97b7
begin
    artifact_specs = get(artifact_cfg, "artifacts", Any[])
    artifact_map = DataFrame(
        artifact_id = [String(config_value(spec, "id", "")) for spec in artifact_specs],
        source_stage = [String(config_value(spec, "source_stage", "")) for spec in artifact_specs],
        destination_filename = [String(config_value(spec, "destination_filename", "")) for spec in artifact_specs],
        source_manifest = [
            portable_path(get(source_manifests, String(config_value(spec, "source_stage", "")), ""))
            for spec in artifact_specs
        ],
        stage_family = [source_stage_label(String(config_value(spec, "source_stage", ""))) for spec in artifact_specs],
        included_by_publication_config = [
            source_stage_included(String(config_value(spec, "source_stage", "")), notebook_orchestration_cfg)
            for spec in artifact_specs
        ],
    )
    artifact_map[!, Symbol("source_artifact_id/source_path")] = inferred_source_reference.(artifact_specs)
    artifact_map = select(
        artifact_map,
        [
            :artifact_id,
            :source_stage,
            Symbol("source_artifact_id/source_path"),
            :destination_filename,
            :source_manifest,
            :stage_family,
            :included_by_publication_config,
        ],
    )
end

# ╔═╡ 55d6b07e-31bc-49db-bfcd-f9d98ba6178d
small_table(artifact_map; n = nrow(artifact_map))

# ╔═╡ 4978ed7b-abf4-48e0-8d6f-447d1f974278
compact_artifact_table = select(
    artifact_map,
    [
        :artifact_id,
        :source_stage,
        Symbol("source_artifact_id/source_path"),
        :destination_filename,
    ],
)

# ╔═╡ 19d2ec31-078e-420d-92c6-e02f7e874e0c
small_table(compact_artifact_table; n = nrow(compact_artifact_table))

# ╔═╡ c58f149c-6c78-4fec-a5d2-3436ec63057c
md"""
## Artifact Families

The rows below summarize which paper-facing artifacts come from each upstream
stage family. The lambda table is optional and is marked as excluded unless the
orchestration config explicitly enables `[lambda_table]`.
"""

# ╔═╡ 8908a391-6046-4e4c-80ca-61f84d1a0812
artifact_family_summary = combine(
    groupby(artifact_map, [:stage_family, :source_stage, :included_by_publication_config]),
    :artifact_id => (ids -> join(ids, ", ")) => :artifact_ids,
    nrow => :artifact_count,
)

# ╔═╡ 9728f19e-5753-4464-8997-95f36c27a9dd
small_table(sort(artifact_family_summary, [:stage_family, :source_stage]); n = nrow(artifact_family_summary))

# ╔═╡ 438df8a1-f7ed-4478-8ded-a0e86a8dfe35
md"""
## Destination Names

The collector copies or symlinks generated source files into the configured
destination root using `destination_filename`. That creates stable
paper-facing names such as `2006_global_main.png` and `effective_rankings.tex`
even when the source artifact paths live deeper inside stage-specific output
directories.
"""

# ╔═╡ 7083fe47-cbd1-450e-827f-4e0cf0484116
destination_table = DataFrame(
    artifact_id = artifact_map.artifact_id,
    stage_family = artifact_map.stage_family,
    destination_path = [
        portable_path(joinpath(destination_root, filename))
        for filename in artifact_map.destination_filename
    ],
    would_update_writing_imgs = fill(Bool(config_value(collection_cfg, "update_writing_imgs", false)), nrow(artifact_map)),
)

# ╔═╡ b845a040-1a32-4619-9455-a4309205a047
small_table(destination_table; n = nrow(destination_table))

# ╔═╡ 77c49217-a491-4981-a5cc-734d217b06a4
md"""
## Copy Guard

The production collector can copy or symlink artifacts. This notebook is an
inspection notebook, so copying is disabled by default. Changing
`DO_COPY_ARTIFACTS` is intentionally not enough to write into the publication
artifact directory from this notebook; the local cell reports that the copy
action is disabled and leaves collection to the CLI stage.
"""

# ╔═╡ 01795546-6919-4e86-a6bd-3c126f2062be
DO_COPY_ARTIFACTS = false

# ╔═╡ c6cfcc55-9d86-4c0b-b0e1-7ba6bfaa306e
copy_guard_table = DataFrame(
    setting = ["DO_COPY_ARTIFACTS", "notebook behavior"],
    value = [
        string(DO_COPY_ARTIFACTS),
        DO_COPY_ARTIFACTS ?
            "copy action intentionally not implemented in this notebook" :
            "inspection only; no artifact copies attempted",
    ],
)

# ╔═╡ 4e180592-e235-4dff-81ea-0fdf0ebef91f
small_table(copy_guard_table; n = nrow(copy_guard_table))

# ╔═╡ 37e9dc7f-8357-4bf0-8818-c716fce7dccd
begin
    notebook_artifact_map_dir = joinpath(settings.output_root, "notebook_tables", "artifact_map")
    artifact_map_csv = write_notebook_csv(
        joinpath(notebook_artifact_map_dir, "paper_artifact_map.csv"),
        artifact_map,
    )
    source_manifest_csv = write_notebook_csv(
        joinpath(notebook_artifact_map_dir, "paper_artifact_source_manifests.csv"),
        source_manifest_table,
    )
    family_summary_csv = write_notebook_csv(
        joinpath(notebook_artifact_map_dir, "paper_artifact_family_summary.csv"),
        artifact_family_summary,
    )
end

# ╔═╡ 52c8327b-d744-47a4-95df-8366f834bda9
local_outputs = DataFrame(
    artifact = [
        "artifact map",
        "source manifests",
        "family summary",
    ],
    path = [
        artifact_map_csv,
        source_manifest_csv,
        family_summary_csv,
    ],
    rows = [
        nrow(artifact_map),
        nrow(source_manifest_table),
        nrow(artifact_family_summary),
    ],
)

# ╔═╡ 60f0e1c6-4e8c-4614-9748-76c21497786b
small_table(local_outputs; n = nrow(local_outputs))

# ╔═╡ 46813a9f-6632-4b07-9fcf-661283e6e3d9
function is_under_directory(child_path::AbstractString, parent_path::AbstractString)
    relative_path = relpath(normpath(child_path), normpath(parent_path))
    return relative_path == "." || (!startswith(relative_path, "..") && !isabspath(relative_path))
end

# ╔═╡ 0fc1c04a-a3d2-408b-9c31-a24095ed75ad
validation_table = DataFrame(
    check = [
        "local outputs under notebook output root",
        "copy action disabled",
        "artifact config rows inspected",
    ],
    value = [
        string(all(path -> is_under_directory(path, settings.output_root), local_outputs.path)),
        string(!DO_COPY_ARTIFACTS),
        string(nrow(artifact_map) == length(artifact_specs)),
    ],
)

# ╔═╡ e8ff853a-0c0a-4499-b860-78729e599bdd
small_table(validation_table; n = nrow(validation_table))

# ╔═╡ Cell order:
# ╠═4f9392f4-6ccb-4b08-9ebb-caf4b9970b7b
# ╟─3360df3d-99d3-4b61-8475-883ac08d8a9a
# ╠═a7719e62-0aa3-4435-b9e9-140e45cedaaa
# ╠═b912af95-d82e-4b12-b979-92e8b04c7aa7
# ╠═3a58d48e-c621-432e-b2c8-40b0ae653ec3
# ╟─26a1422a-9e16-4c38-bcfc-5f0a265696d6
# ╠═50436a4d-d449-4306-82e2-005549db5b07
# ╠═d68d5803-b80f-4c9a-a161-fd4f4f259b2b
# ╠═26484b47-d6fb-4e99-8f1e-8777941c7715
# ╠═d92263ca-0847-4c14-8e85-32f32f39ab44
# ╟─92944e84-f7ed-4a05-9759-502d85cbdd4f
# ╟─6af57865-5965-448c-950c-ab43cf573893
# ╠═56199d4f-f820-4a3a-9923-c2dd9a310e83
# ╠═d829a85b-c76b-4ea5-a620-a1f0c854e756
# ╟─8f27f4ac-c7e1-41c6-a7a2-2eedc2d83711
# ╟─b3cb084a-b4dd-4dc9-8fea-e3cf62b33bd8
# ╠═02d014b9-fc21-4f63-8b8a-c0f1bd4a22bd
# ╠═33a5664d-36e2-42d5-a6fa-72b986b4c893
# ╠═da11b43d-f4ec-4c31-8528-f6c290da8a6d
# ╠═ef47c590-4f75-4352-9ab4-2e19f1d2249b
# ╠═8c655e77-6a6a-4197-a3ad-b5c66b5e97b7
# ╟─55d6b07e-31bc-49db-bfcd-f9d98ba6178d
# ╠═4978ed7b-abf4-48e0-8d6f-447d1f974278
# ╟─19d2ec31-078e-420d-92c6-e02f7e874e0c
# ╟─c58f149c-6c78-4fec-a5d2-3436ec63057c
# ╠═8908a391-6046-4e4c-80ca-61f84d1a0812
# ╟─9728f19e-5753-4464-8997-95f36c27a9dd
# ╟─438df8a1-f7ed-4478-8ded-a0e86a8dfe35
# ╠═7083fe47-cbd1-450e-827f-4e0cf0484116
# ╟─b845a040-1a32-4619-9455-a4309205a047
# ╟─77c49217-a491-4981-a5cc-734d217b06a4
# ╠═01795546-6919-4e86-a6bd-3c126f2062be
# ╠═c6cfcc55-9d86-4c0b-b0e1-7ba6bfaa306e
# ╟─4e180592-e235-4dff-81ea-0fdf0ebef91f
# ╠═37e9dc7f-8357-4bf0-8818-c716fce7dccd
# ╠═52c8327b-d744-47a4-95df-8366f834bda9
# ╟─60f0e1c6-4e8c-4614-9748-76c21497786b
# ╠═46813a9f-6632-4b07-9fcf-661283e6e3d9
# ╠═0fc1c04a-a3d2-408b-9c31-a24095ed75ad
# ╟─e8ff853a-0c0a-4499-b860-78729e599bdd
