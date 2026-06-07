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

# ╔═╡ 7a5d57e2-c695-4d46-95f1-4828ea6633d3
begin
    import Pkg
    Pkg.activate(@__DIR__)
end

# ╔═╡ 6501521b-e6c9-4f0a-9fb2-18203b4d1de0
begin
    # Load shared notebook helpers and the local PrefPol package.
    include(joinpath(@__DIR__, "notebook_common.jl"))
end

# ╔═╡ 07ef75f7-7cbb-4a6e-9783-e831e08b1f4b
md"""
# Setup and Configuration

This notebook introduces the notebook workflow and inspects the small notebook
configuration. The notebook layer is for interactive inspection and teaching; it
is not the production replication path.

The production path is the composable replication runner. This notebook reads
`nb/notebook_config.toml` and writes only under
`nb/output/notebook_smoke`.
"""

# ╔═╡ a065d4cc-4737-4463-a698-77bd18f33dad
md"""
The notebook configuration mirrors the validation concerns of the production
configuration pass: read configuration, resolve paths, check targets against
configured waves, and summarize the planned execution scale.
"""

# ╔═╡ 5be8706f-3820-42e4-bc29-dd83ee2edac6
begin
    # Load the notebook-scale orchestration config.
    cfg = load_notebook_config()
    settings = notebook_settings(cfg)
    targets = notebook_targets(cfg)
end

# ╔═╡ d813c4f2-1bc1-4842-beb2-cd8a69f856d0
begin
    # Keep notebook writes isolated from publication outputs.
    @assert settings.output_root != NOTEBOOK_PUBLICATION_OUTPUT
    ensure_not_publication_output!(settings.output_root)
    ensure_not_publication_output!(settings.cache_root)
end

# ╔═╡ 462ec38b-d714-4079-ad8f-8abf331fb980
run_summary = pp.DataFrame(
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
small_table(run_summary; n = pp.nrow(run_summary))

# ╔═╡ d799f604-81f1-4f59-9801-2250cc568f46
md"""
The next cells load year configurations through `PrefPol`. They inspect
available survey waves only; no resamples, imputations, linearizations, or
measures are created here.
"""

# ╔═╡ 024cc796-d678-4aae-9083-a87ec643bb53
begin
    # Discover year TOML files and load SurveyWaveConfig objects.
    waves, source_registry, wave_by_id = load_notebook_waves()
end

# ╔═╡ 166f74a8-e9d9-4b6b-9021-21c3a0511c27
available_waves = pp.DataFrame(
    year = [wave.year for wave in waves],
    wave_id = [wave.wave_id for wave in waves],
    scenarios = [join(sort(collect(keys(wave.scenario_candidates))), ", ") for wave in waves],
    groupings = [join(wave.demographic_cols, ", ") for wave in waves],
    max_candidates = [wave.max_candidates for wave in waves],
)

# ╔═╡ 3e8e9bfc-3627-4b24-a453-964944046151
small_table(available_waves; n = pp.nrow(available_waves))

# ╔═╡ 72d15f3e-3094-4584-8bf9-a1b3e804e51f
selected_targets_table = pp.DataFrame(
    wave_id = [target.wave_id for target in targets],
    scenario_name = [target.scenario_name for target in targets],
    m_values = [join(target.m_values, ", ") for target in targets],
)

# ╔═╡ 7dd23e5e-29c3-4c0e-90d8-381da9b41135
small_table(selected_targets_table; n = pp.nrow(selected_targets_table))

# ╔═╡ 8846c4f7-ee7e-404e-b57a-7df89271c0ad
md"""
At this point the notebook has completed the configuration inspection. Later
notebooks will build and run staged artifacts from this small notebook
configuration.
"""

# ╔═╡ 1659131a-26ed-4b29-9ecb-77dd747435a9
TableOfContents()

# ╔═╡ 9d362793-854c-4598-853e-059c9685a584
begin
    target_options = notebook_target_labels(cfg)
    @bind selected_target_label Select(target_options)
end

# ╔═╡ e317ff9d-ccc5-406e-b932-be8d6a093fc1
begin
    selected_target = selected_target_row(cfg, selected_target_label)
    selected_wave = wave_by_id[String(selected_target.wave_id)]
    selected_candidates = pp.resolve_active_candidate_set(
        selected_wave;
        scenario_name = String(selected_target.scenario_name),
        m = Int(selected_target.m),
    )
    selected_candidate_table = pp.DataFrame(
        position = collect(eachindex(selected_candidates)),
        candidate = selected_candidates,
    )
end

# ╔═╡ fbadb0d9-1cef-41f1-9fdb-a9472eddf609
small_table(selected_candidate_table; n = pp.nrow(selected_candidate_table))

# ╔═╡ 7e758a0e-8c0e-4a5a-9453-945b354ebb1f
provenance_table = notebook_provenance_table(
    settings;
    extra = (
        notebook_config = joinpath(nb_root(), "notebook_config.toml"),
    ),
)

# ╔═╡ 4af6af68-164e-423c-97a8-8e56538f9ffc
small_table(provenance_table; n = pp.nrow(provenance_table))

# ╔═╡ Cell order:
# ╠═7a5d57e2-c695-4d46-95f1-4828ea6633d3
# ╟─07ef75f7-7cbb-4a6e-9783-e831e08b1f4b
# ╠═6501521b-e6c9-4f0a-9fb2-18203b4d1de0
# ╟─a065d4cc-4737-4463-a698-77bd18f33dad
# ╠═5be8706f-3820-42e4-bc29-dd83ee2edac6
# ╠═d813c4f2-1bc1-4842-beb2-cd8a69f856d0
# ╠═462ec38b-d714-4079-ad8f-8abf331fb980
# ╠═97775176-e203-4353-9e84-67be62ae7e13
# ╟─d799f604-81f1-4f59-9801-2250cc568f46
# ╠═024cc796-d678-4aae-9083-a87ec643bb53
# ╠═166f74a8-e9d9-4b6b-9021-21c3a0511c27
# ╠═3e8e9bfc-3627-4b24-a453-964944046151
# ╠═72d15f3e-3094-4584-8bf9-a1b3e804e51f
# ╠═7dd23e5e-29c3-4c0e-90d8-381da9b41135
# ╟─8846c4f7-ee7e-404e-b57a-7df89271c0ad
# ╠═1659131a-26ed-4b29-9ecb-77dd747435a9
# ╠═9d362793-854c-4598-853e-059c9685a584
# ╠═e317ff9d-ccc5-406e-b932-be8d6a093fc1
# ╠═fbadb0d9-1cef-41f1-9fdb-a9472eddf609
# ╠═7e758a0e-8c0e-4a5a-9453-945b354ebb1f
# ╠═4af6af68-164e-423c-97a8-8e56538f9ffc
