### A Pluto.jl notebook ###
# v0.20.17

using Markdown

# ╔═╡ 7a5d57e2-c695-4d46-95f1-4828ea6633d3
begin
    import Pkg
    Pkg.activate(@__DIR__)
end

# ╔═╡ 07ef75f7-7cbb-4a6e-9783-e831e08b1f4b
md"""
# Setup and Configuration

This notebook introduces the notebook workflow and inspects the small notebook
configuration. The notebook layer is for interactive inspection and teaching; it
is not the production replication path.

The production path is `PrefPol/composable_running/run_all_paper.jl`. This
notebook reads `nb/notebook_config.toml` and writes only under
`nb/output/notebook_smoke`.
"""

# ╔═╡ 6501521b-e6c9-4f0a-9fb2-18203b4d1de0
begin
    # composable-running concept: shared setup from stage_common.jl.
    include(joinpath(@__DIR__, "notebook_common.jl"))
end

# ╔═╡ a065d4cc-4737-4463-a698-77bd18f33dad
md"""
The notebook configuration mirrors the validation concerns of CLI stage
`PrefPol/composable_running/stages/00_validate_configs.jl`: read configuration,
resolve paths, check targets against configured waves, and summarize the planned
execution scale.
"""

# ╔═╡ 5be8706f-3820-42e4-bc29-dd83ee2edac6
begin
    # composable-running concept: orchestration config loading.
    cfg = load_notebook_config()
    settings = notebook_settings(cfg)
    targets = notebook_targets(cfg)
end

# ╔═╡ d813c4f2-1bc1-4842-beb2-cd8a69f856d0
begin
    # composable-running concept: publication-output guard.
    @assert settings.output_root != NOTEBOOK_PUBLICATION_OUTPUT
    ensure_not_publication_output!(settings.output_root)
    ensure_not_publication_output!(settings.cache_root)
end

# ╔═╡ 462ec38b-d714-4079-ad8f-8abf331fb980
run_summary = DataFrame(
    item = [
        "B",
        "R",
        "K",
        "output root",
        "cache root",
        "targets",
        "measures",
        "backends",
        "linearizers",
    ],
    value = [
        string(settings.B),
        string(settings.R),
        string(settings.K),
        settings.output_root,
        settings.cache_root,
        join(["$(t.wave_id)/$(t.scenario_name) m=$(join(t.m_values, ","))" for t in targets], "; "),
        join(String.(settings.measures), ", "),
        join(String.(settings.imputer_backends), ", "),
        join(String.(settings.linearizer_policies), ", "),
    ],
)

# ╔═╡ 97775176-e203-4353-9e84-67be62ae7e13
small_table(run_summary; n = nrow(run_summary))

# ╔═╡ d799f604-81f1-4f59-9801-2250cc568f46
md"""
The next cells load year configurations through `PrefPol`. They inspect
available survey waves only; no resamples, imputations, linearizations, or
measures are created here.
"""

# ╔═╡ 024cc796-d678-4aae-9083-a87ec643bb53
begin
    # composable-running concept: year TOML discovery and SurveyWaveConfig loading.
    waves, source_registry, wave_by_id = load_notebook_waves()
end

# ╔═╡ 166f74a8-e9d9-4b6b-9021-21c3a0511c27
available_waves = DataFrame(
    year = [wave.year for wave in waves],
    wave_id = [wave.wave_id for wave in waves],
    scenarios = [join(sort(collect(keys(wave.scenario_candidates))), ", ") for wave in waves],
    groupings = [join(wave.demographic_cols, ", ") for wave in waves],
    max_candidates = [wave.max_candidates for wave in waves],
)

# ╔═╡ 3e8e9bfc-3627-4b24-a453-964944046151
small_table(available_waves; n = nrow(available_waves))

# ╔═╡ 72d15f3e-3094-4584-8bf9-a1b3e804e51f
selected_targets_table = DataFrame(
    wave_id = [target.wave_id for target in targets],
    scenario_name = [target.scenario_name for target in targets],
    m_values = [join(target.m_values, ", ") for target in targets],
)

# ╔═╡ 7dd23e5e-29c3-4c0e-90d8-381da9b41135
small_table(selected_targets_table; n = nrow(selected_targets_table))

# ╔═╡ 8846c4f7-ee7e-404e-b57a-7df89271c0ad
md"""
At this point the notebook has completed the configuration inspection that
corresponds to CLI stage `00_validate_configs.jl`. Later notebooks will build
and run staged artifacts from this small notebook configuration.
"""

# ╔═╡ Cell order:
# ╠═7a5d57e2-c695-4d46-95f1-4828ea6633d3
# ╟─07ef75f7-7cbb-4a6e-9783-e831e08b1f4b
# ╠═6501521b-e6c9-4f0a-9fb2-18203b4d1de0
# ╟─a065d4cc-4737-4463-a698-77bd18f33dad
# ╠═5be8706f-3820-42e4-bc29-dd83ee2edac6
# ╠═d813c4f2-1bc1-4842-beb2-cd8a69f856d0
# ╠═462ec38b-d714-4079-ad8f-8abf331fb980
# ╟─97775176-e203-4353-9e84-67be62ae7e13
# ╟─d799f604-81f1-4f59-9801-2250cc568f46
# ╠═024cc796-d678-4aae-9083-a87ec643bb53
# ╠═166f74a8-e9d9-4b6b-9021-21c3a0511c27
# ╟─3e8e9bfc-3627-4b24-a453-964944046151
# ╠═72d15f3e-3094-4584-8bf9-a1b3e804e51f
# ╟─7dd23e5e-29c3-4c0e-90d8-381da9b41135
# ╟─8846c4f7-ee7e-404e-b57a-7df89271c0ad
