### A Pluto.jl notebook ###
# v1.0.1

using Markdown
using InteractiveUtils

# This Pluto notebook uses @bind for interactivity. When running this notebook outside of Pluto, the following 'mock version' of @bind gives bound variables a default value (instead of an error).
macro bind(def, element)
    #! format: off
    return quote
        local iv = try Base.loaded_modules[Base.PkgId(Base.UUID("6e696c72-6542-2067-7265-42206c756150"), "AbstractPlutoDingetjes")].Bonds.initial_value catch; b -> missing; end
        local el = $(esc(element))
        global $(esc(def)) = Core.applicable(Base.get, el) ? Base.get(el) : iv(el)
        el
    end
    #! format: on
end

# ╔═╡ 1793d860-770c-4b7c-9c1e-7f3f5d4e7b0e
begin
    import Pkg
    Pkg.activate(@__DIR__)
end

# ╔═╡ 432a8713-f937-46d8-95ea-ed59180d9fbf
begin
    # Load shared notebook helpers and the local PrefPol package.
    include(joinpath(@__DIR__, "notebook_common.jl"))
end

# ╔═╡ cff8cf11-283b-4ae5-9e62-2c4056cd5b13
md"""
# Targets and Specs

This notebook shows how configured notebook targets become `PipelineSpec`s and
batch items. Target selection starts from `nb/notebook_config.toml`, then expands
over candidate-set size `m`, imputation backends, and linearizer policies.
"""

# ╔═╡ ec5caa06-3381-40e5-8675-ad0a6b016f53
md"""
A target names a survey wave and scenario. The candidate-set size `m` controls
how many active candidates are resolved for that scenario. Each requested
backend and linearizer combination becomes a separate planned batch item.

A `PipelineSpec` is the reproducibility contract for one analysis cell: wave,
active candidates, groupings, measures, `B/R/K`, backend, linearizer, tie policy,
seed namespace, and version metadata. This mirrors the early planning logic used
by the production workflow before any stochastic stage runs.
"""

# ╔═╡ c73564a7-96d4-420c-a8f1-26bdb97cdd3a
begin
    # Load the notebook-scale config and run settings.
    cfg = load_notebook_config()
    settings = notebook_settings(cfg)
    targets = notebook_targets(cfg)
end

# ╔═╡ 1e1e8e8a-a3dd-4fde-8376-dd2bb964d7dd
begin
    # Load survey-wave configs for target validation.
    waves, source_registry, wave_by_id = load_notebook_waves()
end

# ╔═╡ 2eda82b7-7913-4e86-bdf7-c0761cf2c5c3
begin
    # Expand targets across backends and linearizer policies.
    batch = build_notebook_batch(cfg)
end

# ╔═╡ 61f42098-fb5d-4e32-bf77-d0db82db0f65
batch_table = pp.DataFrame([
    (
        batch_index = idx,
        wave_id = item.spec.wave_id,
        year = item.metadata.year,
        scenario_name = item.metadata.scenario_name,
        m = item.metadata.m,
        imputer_backend = String(item.spec.imputer_backend),
        linearizer_policy = String(item.spec.linearizer_policy),
        active_candidates = join(item.spec.active_candidates, ", "),
        measures = join(String.(item.spec.measures), ", "),
        B = item.spec.B,
        R = item.spec.R,
        K = item.spec.K,
    )
    for (idx, item) in enumerate(batch.items)
])

# ╔═╡ a706c5e5-e0c4-4cae-a3fc-20e6b8eeb4b7
small_table(batch_table; n = pp.nrow(batch_table))

# ╔═╡ e046fd51-4206-4219-8d0a-491673291e37
md"""
The first batch item is the running example for the remaining planning cells.
It is still only a specification; no bootstrap, imputation, linearization, or
measure artifact is created.
"""

# ╔═╡ 4bf181cd-faa6-4ab5-98b1-0b44f95488d8
begin
    # Select one StudyBatchItem for inspection.
    example_index = 1
    example_item = batch.items[example_index]
    example_spec = example_item.spec
    example_wave = wave_by_id[example_spec.wave_id]
end

# ╔═╡ 95fd3210-a0b6-4a37-bd2c-7f3fb1d0c328
example_wave_table = pp.DataFrame(
    item = [
        "wave_id",
        "year",
        "scenario",
        "m",
        "groupings",
        "max_candidates",
    ],
    value = [
        example_wave.wave_id,
        string(example_wave.year),
        example_item.metadata.scenario_name,
        string(example_item.metadata.m),
        join(example_wave.demographic_cols, ", "),
        string(example_wave.max_candidates),
    ],
)

# ╔═╡ 4a1d27e6-2ab0-4c6a-aa48-b9979a7149cb
small_table(example_wave_table; n = pp.nrow(example_wave_table))

# ╔═╡ fce6c1cd-f0ad-4214-8e2d-b2aa5e8ecbb2
active_candidate_table = pp.DataFrame(
    position = collect(eachindex(example_spec.active_candidates)),
    candidate = example_spec.active_candidates,
)

# ╔═╡ 78859088-0a85-4715-b0e9-cdfec77947f2
small_table(active_candidate_table; n = pp.nrow(active_candidate_table))

# ╔═╡ 3df38422-8c5b-4235-86e7-e289d15ad444
begin
    # Compute the planned cache path for the selected spec.
    spec_hash = isdefined(pp, :_pipeline_cache_key) ?
                getfield(pp, :_pipeline_cache_key)(example_spec) :
                missing
    spec_hash_text = spec_hash === missing ? "" : String(spec_hash)
    planned_cache_dir = isempty(spec_hash_text) ?
                        joinpath(settings.cache_root, "<spec-hash>") :
                        joinpath(settings.cache_root, spec_hash_text)
end

# ╔═╡ 3bb847f1-b350-4836-af55-e6f6a87e4b21
spec_metadata_table = pp.DataFrame(
    item = ["cache directory", "seed namespace", "spec hash"],
    value = [
        planned_cache_dir,
        example_spec.seed_namespace,
        isempty(spec_hash_text) ? "<not available>" : spec_hash_text,
    ],
)

# ╔═╡ d5a88f3b-514e-4d12-b8ec-0e30c5a31213
small_table(spec_metadata_table; n = pp.nrow(spec_metadata_table))

# ╔═╡ e907ed8a-8c65-43d8-9d5c-89fa813e3436
md"""
This completes the notebook-scale planning pass corresponding to the early
target and `PipelineSpec` construction used by CLI stages 01-04.
"""

# ╔═╡ 08975389-d777-4d86-9440-86755be20a2f
TableOfContents()

# ╔═╡ a0f211ce-7d3b-4974-9f2e-c96faa8b8c67
begin
    target_options = notebook_target_labels(cfg)
    backend_options = String.(settings.imputer_backends)
    linearizer_options = String.(settings.linearizer_policies)
    @bind selected_target_label Select(target_options)
end

# ╔═╡ cc8e8118-2723-4bc6-a4b2-285c9972986f
begin
    @bind selected_backend Select(backend_options)
end

# ╔═╡ a4c8a56c-1f85-47e3-bfb6-46db01d10184
begin
    @bind selected_linearizer Select(linearizer_options)
end

# ╔═╡ 15da6c97-42c3-4c15-90c2-3330b9eba68a
begin
    selected_target = selected_target_row(cfg, selected_target_label)
    selected_batch_index, selected_batch_item = select_batch_item(
        batch;
        wave_id = selected_target.wave_id,
        scenario_name = selected_target.scenario_name,
        m = selected_target.m,
        backend = selected_backend,
        linearizer = selected_linearizer,
    )
    selected_spec_rows = batch_table[batch_table.batch_index .== selected_batch_index, :]
end

# ╔═╡ 0597ad10-8be7-4a62-a004-67ef82475c6d
small_table(selected_spec_rows; n = pp.nrow(selected_spec_rows))

# ╔═╡ Cell order:
# ╠═1793d860-770c-4b7c-9c1e-7f3f5d4e7b0e
# ╟─cff8cf11-283b-4ae5-9e62-2c4056cd5b13
# ╠═432a8713-f937-46d8-95ea-ed59180d9fbf
# ╟─ec5caa06-3381-40e5-8675-ad0a6b016f53
# ╠═c73564a7-96d4-420c-a8f1-26bdb97cdd3a
# ╠═1e1e8e8a-a3dd-4fde-8376-dd2bb964d7dd
# ╠═2eda82b7-7913-4e86-bdf7-c0761cf2c5c3
# ╠═61f42098-fb5d-4e32-bf77-d0db82db0f65
# ╠═a706c5e5-e0c4-4cae-a3fc-20e6b8eeb4b7
# ╟─e046fd51-4206-4219-8d0a-491673291e37
# ╠═4bf181cd-faa6-4ab5-98b1-0b44f95488d8
# ╠═95fd3210-a0b6-4a37-bd2c-7f3fb1d0c328
# ╠═4a1d27e6-2ab0-4c6a-aa48-b9979a7149cb
# ╠═fce6c1cd-f0ad-4214-8e2d-b2aa5e8ecbb2
# ╠═78859088-0a85-4715-b0e9-cdfec77947f2
# ╠═3df38422-8c5b-4235-86e7-e289d15ad444
# ╠═3bb847f1-b350-4836-af55-e6f6a87e4b21
# ╠═d5a88f3b-514e-4d12-b8ec-0e30c5a31213
# ╟─e907ed8a-8c65-43d8-9d5c-89fa813e3436
# ╠═08975389-d777-4d86-9440-86755be20a2f
# ╠═a0f211ce-7d3b-4974-9f2e-c96faa8b8c67
# ╠═cc8e8118-2723-4bc6-a4b2-285c9972986f
# ╠═a4c8a56c-1f85-47e3-bfb6-46db01d10184
# ╠═15da6c97-42c3-4c15-90c2-3330b9eba68a
# ╠═0597ad10-8be7-4a62-a004-67ef82475c6d
