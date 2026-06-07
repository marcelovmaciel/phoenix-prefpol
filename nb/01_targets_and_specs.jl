### A Pluto.jl notebook ###
# v0.20.17

using Markdown

# ╔═╡ 1793d860-770c-4b7c-9c1e-7f3f5d4e7b0e
begin
    import Pkg
    Pkg.activate(@__DIR__)
end

# ╔═╡ cff8cf11-283b-4ae5-9e62-2c4056cd5b13
md"""
# Targets and Specs

This notebook shows how configured notebook targets become `PipelineSpec`s and
batch items. Target selection starts from `nb/notebook_config.toml`, then expands
over candidate-set size `m`, imputation backends, and linearizer policies.
"""

# ╔═╡ 432a8713-f937-46d8-95ea-ed59180d9fbf
begin
    # composable-running concept: shared setup from stage_common.jl.
    include(joinpath(@__DIR__, "notebook_common.jl"))
end

# ╔═╡ ec5caa06-3381-40e5-8675-ad0a6b016f53
md"""
A target names a survey wave and scenario. The candidate-set size `m` controls
how many active candidates are resolved for that scenario. Each requested
backend and linearizer combination becomes a separate planned batch item.

A `PipelineSpec` is the reproducibility contract for one analysis cell: wave,
active candidates, groupings, measures, `B/R/K`, backend, linearizer, tie policy,
seed namespace, and version metadata. This mirrors the early planning logic used
by CLI stages 01-04 before any stochastic stage runs.
"""

# ╔═╡ c73564a7-96d4-420c-a8f1-26bdb97cdd3a
begin
    # composable-running concept: orchestration config and run settings.
    cfg = load_notebook_config()
    settings = notebook_settings(cfg)
    targets = notebook_targets(cfg)
end

# ╔═╡ 1e1e8e8a-a3dd-4fde-8376-dd2bb964d7dd
begin
    # composable-running concept: SurveyWaveConfig lookup for target validation.
    waves, source_registry, wave_by_id = load_notebook_waves()
end

# ╔═╡ 2eda82b7-7913-4e86-bdf7-c0761cf2c5c3
begin
    # composable-running concept: build_batch target/backend/linearizer expansion.
    batch = build_notebook_batch(cfg)
end

# ╔═╡ 61f42098-fb5d-4e32-bf77-d0db82db0f65
batch_table = DataFrame([
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
small_table(batch_table; n = nrow(batch_table))

# ╔═╡ e046fd51-4206-4219-8d0a-491673291e37
md"""
The first batch item is the running example for the remaining planning cells.
It is still only a specification; no bootstrap, imputation, linearization, or
measure artifact is created.
"""

# ╔═╡ 4bf181cd-faa6-4ab5-98b1-0b44f95488d8
begin
    # composable-running concept: select one StudyBatchItem for inspection.
    example_index = 1
    example_item = batch.items[example_index]
    example_spec = example_item.spec
    example_wave = wave_by_id[example_spec.wave_id]
end

# ╔═╡ 95fd3210-a0b6-4a37-bd2c-7f3fb1d0c328
example_wave_table = DataFrame(
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
small_table(example_wave_table; n = nrow(example_wave_table))

# ╔═╡ fce6c1cd-f0ad-4214-8e2d-b2aa5e8ecbb2
active_candidate_table = DataFrame(
    position = collect(eachindex(example_spec.active_candidates)),
    candidate = example_spec.active_candidates,
)

# ╔═╡ 78859088-0a85-4715-b0e9-cdfec77947f2
small_table(active_candidate_table; n = nrow(active_candidate_table))

# ╔═╡ 3df38422-8c5b-4235-86e7-e289d15ad444
begin
    # composable-running concept: spec hash and cache path planning.
    spec_hash = isdefined(pp, :_pipeline_cache_key) ?
                getfield(pp, :_pipeline_cache_key)(example_spec) :
                missing
    spec_hash_text = spec_hash === missing ? "" : String(spec_hash)
    planned_cache_dir = isempty(spec_hash_text) ?
                        joinpath(settings.cache_root, "<spec-hash>") :
                        joinpath(settings.cache_root, spec_hash_text)
end

# ╔═╡ 3bb847f1-b350-4836-af55-e6f6a87e4b21
spec_metadata_table = DataFrame(
    item = ["cache directory", "seed namespace", "spec hash"],
    value = [
        planned_cache_dir,
        example_spec.seed_namespace,
        isempty(spec_hash_text) ? "<not available>" : spec_hash_text,
    ],
)

# ╔═╡ d5a88f3b-514e-4d12-b8ec-0e30c5a31213
small_table(spec_metadata_table; n = nrow(spec_metadata_table))

# ╔═╡ e907ed8a-8c65-43d8-9d5c-89fa813e3436
md"""
This completes the notebook-scale planning pass corresponding to the early
target and `PipelineSpec` construction used by CLI stages 01-04.
"""

# ╔═╡ Cell order:
# ╠═1793d860-770c-4b7c-9c1e-7f3f5d4e7b0e
# ╟─cff8cf11-283b-4ae5-9e62-2c4056cd5b13
# ╠═432a8713-f937-46d8-95ea-ed59180d9fbf
# ╟─ec5caa06-3381-40e5-8675-ad0a6b016f53
# ╠═c73564a7-96d4-420c-a8f1-26bdb97cdd3a
# ╠═1e1e8e8a-a3dd-4fde-8376-dd2bb964d7dd
# ╠═2eda82b7-7913-4e86-bdf7-c0761cf2c5c3
# ╠═61f42098-fb5d-4e32-bf77-d0db82db0f65
# ╟─a706c5e5-e0c4-4cae-a3fc-20e6b8eeb4b7
# ╟─e046fd51-4206-4219-8d0a-491673291e37
# ╠═4bf181cd-faa6-4ab5-98b1-0b44f95488d8
# ╠═95fd3210-a0b6-4a37-bd2c-7f3fb1d0c328
# ╟─4a1d27e6-2ab0-4c6a-aa48-b9979a7149cb
# ╠═fce6c1cd-f0ad-4214-8e2d-b2aa5e8ecbb2
# ╟─78859088-0a85-4715-b0e9-cdfec77947f2
# ╠═3df38422-8c5b-4235-86e7-e289d15ad444
# ╠═3bb847f1-b350-4836-af55-e6f6a87e4b21
# ╟─d5a88f3b-514e-4d12-b8ec-0e30c5a31213
# ╟─e907ed8a-8c65-43d8-9d5c-89fa813e3436
