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

# ╔═╡ 26f5b7cf-f12f-4ef3-b6e8-9f39e176a9a1
begin
    import Pkg
    Pkg.activate(@__DIR__)
end

# ╔═╡ 4d35c77f-d36b-45d8-a329-7d0ee29760e1
begin
    # Load shared notebook helpers and the local PrefPol package.
    include(joinpath(@__DIR__, "notebook_common.jl"))
    using Preferences
    using Statistics
end

# ╔═╡ a1b9f4a4-01d5-4a85-9ef4-efb3aeeff1f1
md"""
# Extra Diagnostics

This notebook interactively reproduces the downstream diagnostics pass without
including or calling the production stage file.

The production pass reads cached linearized strict profiles, then writes two
families of downstream diagnostics:

- ranking support diagnostics, which describe how much of the `m!` strict
  ranking domain is observed;
- effective-count diagnostics, especially `EO` and `ENRP`, which summarize
  concentration over observed strict rankings and exact reversal pairs.

These are downstream diagnostics because they are computed only after the
survey scores have been resampled, imputed, and linearized into strict
profiles. The notebook keeps the tiny `nb/notebook_config.toml` scale and
writes only compact inspection CSVs under `nb/output/notebook_smoke`.
"""

# ╔═╡ 5c7fb3d7-493e-4344-9e86-67ad86be413d
begin
    cfg = load_notebook_config()
    settings = notebook_settings(cfg)
    targets = notebook_targets(cfg)
end

# ╔═╡ 50c02f04-d465-46da-a921-b30f1ef43322
begin
    @assert settings.B <= 5 "Notebook config keeps B <= 5."
    @assert settings.R <= 5 "Notebook config keeps R <= 5."
    @assert settings.K <= 5 "Notebook config keeps K <= 5."
    ensure_not_publication_output!(settings.output_root)
    ensure_not_publication_output!(settings.cache_root)
end

# ╔═╡ 3d9588c9-d56e-42dd-8685-6ed2ab801ae1
begin
    waves, source_registry, wave_by_id = load_notebook_waves()
    batch = build_notebook_batch(cfg)
    pipeline = pp.NestedStochasticPipeline(source_registry; cache_root = settings.cache_root)
end

# ╔═╡ a07632bf-1858-4905-b93f-476e7856d532
notebook_plan = pp.DataFrame(
    batch_index = collect(eachindex(batch.items)),
    wave_id = [item.spec.wave_id for item in batch.items],
    scenario_name = [item.metadata.scenario_name for item in batch.items],
    m = [item.metadata.m for item in batch.items],
    imputer_backend = String.([item.spec.imputer_backend for item in batch.items]),
    linearizer_policy = String.([item.spec.linearizer_policy for item in batch.items]),
    B = [item.spec.B for item in batch.items],
    R = [item.spec.R for item in batch.items],
    K = [item.spec.K for item in batch.items],
)

# ╔═╡ 3f55b9f1-bcad-4f36-b10b-93cefa9db5ea
small_table(notebook_plan; n = pp.nrow(notebook_plan))

# ╔═╡ 451c9a56-d696-4bdb-b0ff-bc95f40a7953
md"""
## Linearized Profiles

The production diagnostic pass does not re-impute or re-linearize. It reads
the cached linearized leaves produced upstream. For a standalone
notebook, the cell below calls `PrefPol.ensure_linearizations!` on the tiny
notebook batch so the required strict-profile artifacts exist in the notebook
cache.
"""

# ╔═╡ 84491fdb-9c3d-4d39-966e-c4e83f033fb7
begin
    raw_linearization_manifests = pp.DataFrame[]
    for item in batch.items
        push!(raw_linearization_manifests, pp.ensure_linearizations!(
            pipeline,
            item.spec;
            force = settings.force,
        ))
    end

    linearization_manifest = sorted_table(stage_manifest(raw_linearization_manifests, batch, pipeline))
    linearized_rows = linearization_manifest[Symbol.(linearization_manifest.stage) .== :linearized, :]
end

# ╔═╡ 58fc4f56-54ac-44e0-b1c9-65d39efb7601
linearized_leaf_table = pp.select(
    linearized_rows,
    intersect(
        [
            :batch_index,
            :year,
            :wave_id,
            :scenario_name,
            :m,
            :imputer_backend,
            :linearizer_policy,
            :B,
            :R,
            :K,
            :b,
            :r,
            :k,
            :path,
        ],
        propertynames(linearized_rows),
    ),
)

# ╔═╡ a46d2dbb-b962-4e5a-8679-4bf780468f75
small_table(linearized_leaf_table; n = min(12, pp.nrow(linearized_leaf_table)))

# ╔═╡ d8792324-1f16-4c84-bb04-b3701c853132
md"""
## Local Diagnostic Functions

The ranking support calculation compresses a strict profile into a distribution
over ranking signatures. `EO` is the inverse HHI of that distribution. `ENRP`
first pairs each observed ranking with its exact reverse, assigns each pair
local reversal mass `2 * min(p_r, p_reverse(r))`, and then takes the inverse HHI
over positive reversal-pair masses.

That means these diagnostics are not raw-survey diagnostics. They are summaries
of strict rankings after the linearizer has resolved ties in each imputed
profile leaf.
"""

# ╔═╡ 2d489968-dba1-489d-b530-44af894e92f6
function ranking_signature_text(sig::Tuple)
    return join(String.(sig), " > ")
end

# ╔═╡ 856f58b8-65cf-405f-a2cc-aa41f2cf3b77
function ranking_masses(profile)
    masses = Dict{Tuple,Float64}()
    order = Tuple[]
    weights = hasproperty(profile, :weights) ?
        Float64.(profile.weights) :
        ones(Float64, Preferences.nballots(profile))

    for (ballot, weight) in zip(profile.ballots, weights)
        weight == 0.0 && continue
        sig = Preferences.ranking_signature(ballot, profile.pool)
        if !haskey(masses, sig)
            push!(order, sig)
        end
        masses[sig] = get(masses, sig, 0.0) + weight
    end

    return masses, order, sum(values(masses))
end

# ╔═╡ cd762b99-b1cf-442f-a563-1fb8ebea2344
function effective_number_stats(profile)
    masses, order, total = ranking_masses(profile)
    total > 0 || error("Linearized profile has zero ranking mass.")

    ranking_probs = [mass / total for mass in values(masses)]
    HHI_rankings = sum(p^2 for p in ranking_probs)
    EO = 1.0 / HHI_rankings

    paired, _ = Preferences.reversal_pairs(order)
    reversal_values = Float64[
        2.0 * min(masses[pair[1]], masses[pair[3]]) / total
        for pair in paired
    ]
    reversal_total = sum(reversal_values)
    HHI_reversal = reversal_total == 0.0 ? missing :
        sum((value / reversal_total)^2 for value in reversal_values)
    ENRP = HHI_reversal === missing ? missing : 1.0 / HHI_reversal
    m = length(profile.pool)

    return (
        ENRP = ENRP,
        EO = EO,
        reversal_to_ranking_effective_ratio = EO == 0.0 || ENRP === missing ? missing : ENRP / EO,
        HHI_reversal = HHI_reversal,
        HHI_rankings = HHI_rankings,
        n_rankings_observed = length(masses),
        n_reversal_pairs_observed = count(>(0.0), reversal_values),
        max_rankings_possible = factorial(m),
        max_reversal_pairs_possible = div(factorial(m), 2),
    )
end

# ╔═╡ d0f28631-54ed-49f9-82ec-cfe7073145f1
function strict_profile_from_linearized(path::AbstractString)
    artifact_path = resolve_nb_path(path)
    isfile(artifact_path) || error(
        "Linearized profile artifact not found: $(artifact_path). " *
        "Rerun the linearization cell in this notebook, or rerun with force=true if the cache is stale.",
    )
    artifact = pp.load_stage_artifact(artifact_path)
    artifact_table = pp.DataFrame(artifact)
    return Preferences.dataframe_to_annotated_profile(
        artifact_table;
        ballot_kind = :strict,
    ).profile
end

# ╔═╡ 34bfb4d5-c641-4598-a743-d6374450a9e0
function draw_base(row)
    return (
        batch_index = Int(row.batch_index),
        analysis_role = string(row.analysis_role),
        wave_id = string(row.wave_id),
        year = Int(row.year),
        scenario_name = string(row.scenario_name),
        m = Int(row.m),
        active_candidates = string(row.active_candidates),
        imputer_backend = string(row.imputer_backend),
        linearizer_policy = string(row.linearizer_policy),
        B = Int(row.B),
        R = Int(row.R),
        K = Int(row.K),
        b = Int(row.b),
        r = Int(row.r),
        k = Int(row.k),
        linearized_path = string(row.path),
    )
end

# ╔═╡ f0c76616-35cb-4dd0-82b0-ea0c591183a1
function build_extra_diagnostic_draws(rows::pp.DataFrame)
    ranking_rows = NamedTuple[]
    effective_rows = NamedTuple[]

    for row in eachrow(rows)
        profile = strict_profile_from_linearized(string(row.path))
        base = draw_base(row)
        support = Preferences.ranking_support_diagnostics(profile; m = Int(row.m))
        push!(ranking_rows, merge(base, support, (diagnostic = "ranking_support",)))
        push!(effective_rows, merge(base, effective_number_stats(profile)))
    end

    return sorted_table(pp.DataFrame(ranking_rows)), sorted_table(pp.DataFrame(effective_rows))
end

# ╔═╡ e8050948-5074-4eca-91f9-4d3bd27211e2
begin
    ranking_draws, effective_draws = build_extra_diagnostic_draws(linearized_rows)
end

# ╔═╡ a330a8c2-c026-4cd6-9e3b-21dfe5e9952e
ranking_draws_compact = pp.select(
    ranking_draws,
    [
        :year,
        :scenario_name,
        :m,
        :b,
        :r,
        :k,
        :n_observations,
        :possible_rankings,
        :n_unique_rankings,
        :unique_share_of_possible,
        :max_ranking_mass,
        :EO,
        :effective_share_of_possible,
        :support_saturation,
    ],
)

# ╔═╡ ef8d9e29-33e3-497d-813e-6067f3ea24d9
small_table(ranking_draws_compact; n = min(16, pp.nrow(ranking_draws_compact)))

# ╔═╡ 5f6c90d7-7da7-4468-a596-cbf42091f7c8
effective_draws_compact = pp.select(
    effective_draws,
    [
        :year,
        :scenario_name,
        :m,
        :b,
        :r,
        :k,
        :n_rankings_observed,
        :EO,
        :n_reversal_pairs_observed,
        :ENRP,
        :reversal_to_ranking_effective_ratio,
        :max_rankings_possible,
        :max_reversal_pairs_possible,
    ],
)

# ╔═╡ 61ca5f0f-7987-472e-99d3-e553b4a8410a
small_table(effective_draws_compact; n = min(16, pp.nrow(effective_draws_compact)))

# ╔═╡ f8e0409e-192f-42ee-8f38-55d107c55f32
md"""
## Summary Tables

The production stage writes draw-level CSVs and summary CSVs. The summary cells
below keep the same idea but restrict the display to the columns most useful
for notebook inspection.
"""

# ╔═╡ d1353b5c-a142-429e-b3c6-0d9c5cd37a21
function compact_quantiles(values)
    xs = Float64.(collect(skipmissing(values)))
    return isempty(xs) ?
        (mean = missing, median = missing, q25 = missing, q75 = missing, missing = length(values)) :
        (mean = mean(xs), median = median(xs), q25 = quantile(xs, 0.25), q75 = quantile(xs, 0.75), missing = length(values) - length(xs))
end

# ╔═╡ 08035708-2a6d-45fd-b886-c0f349fba4eb
function summarize_compact(draws::pp.DataFrame, metrics)
    group_cols = [
        :analysis_role,
        :wave_id,
        :year,
        :scenario_name,
        :imputer_backend,
        :linearizer_policy,
        :m,
        :active_candidates,
        :B,
        :R,
        :K,
    ]
    rows = NamedTuple[]
    for subdf in pp.groupby(draws, group_cols)
        base = NamedTuple(name => subdf[1, name] for name in group_cols)
        for metric in metrics
            stats = compact_quantiles(subdf[!, metric])
            push!(rows, merge(base, (
                metric = String(metric),
                n_draws = pp.nrow(subdf),
                mean = stats.mean,
                median = stats.median,
                q25 = stats.q25,
                q75 = stats.q75,
                n_missing = stats.missing,
            )))
        end
    end
    return sorted_table(pp.DataFrame(rows))
end

# ╔═╡ 1e19ca53-2716-469d-a214-2dde55fbe55e
begin
    ranking_summary_compact = summarize_compact(
        ranking_draws,
        [:n_unique_rankings, :unique_share_of_possible, :max_ranking_mass, :EO, :support_saturation],
    )
    effective_summary_compact = summarize_compact(
        effective_draws,
        [:n_rankings_observed, :EO, :n_reversal_pairs_observed, :ENRP, :reversal_to_ranking_effective_ratio],
    )
end

# ╔═╡ ed9b3a67-fb13-4c76-8f15-4e991015b294
small_table(ranking_summary_compact; n = min(20, pp.nrow(ranking_summary_compact)))

# ╔═╡ 0a461086-03c5-41ab-b95d-89654ca5d9d0
small_table(effective_summary_compact; n = min(20, pp.nrow(effective_summary_compact)))

# ╔═╡ c18e4a03-c656-4a1a-bf78-0a2022d44dbb
md"""
## Ranking Support Detail

For a single draw, the table below shows the empirical mass of each strict
ranking signature. This is the object behind both support diagnostics and
`EO`.
"""

# ╔═╡ 4f261419-a0f6-40bc-a4e4-955f7c3de79d
begin
    example_row = linearized_rows[1, :]
    example_profile = strict_profile_from_linearized(string(example_row.path))
    example_masses, example_order, example_total = ranking_masses(example_profile)
    example_ranking_support = pp.DataFrame(
        ranking = ranking_signature_text.(example_order),
        mass = [example_masses[sig] for sig in example_order],
        share = [example_masses[sig] / example_total for sig in example_order],
    )
    sort!(example_ranking_support, [:mass, :ranking]; rev = [true, false])
end

# ╔═╡ baebaa2c-17b1-4d8c-b6f3-16f0c9d0663f
small_table(example_ranking_support; n = min(10, pp.nrow(example_ranking_support)))

# ╔═╡ 64350afc-9082-4c3b-8da8-417658697a3f
begin
    paired_rankings, unpaired_rankings = Preferences.reversal_pairs(example_order)
    example_reversal_pairs = pp.DataFrame(
        ranking = [ranking_signature_text(pair[1]) for pair in paired_rankings],
        reverse_ranking = [ranking_signature_text(pair[3]) for pair in paired_rankings],
        local_reversal_mass = [
            2.0 * min(example_masses[pair[1]], example_masses[pair[3]]) / example_total
            for pair in paired_rankings
        ],
    )
    sort!(example_reversal_pairs, :local_reversal_mass; rev = true)
end

# ╔═╡ 94642928-b6ef-4864-9e1b-631d0f325abd
small_table(example_reversal_pairs; n = min(10, pp.nrow(example_reversal_pairs)))

# ╔═╡ f37d3f1a-1f4f-45a1-a37d-991a8c38ea27
begin
    notebook_table_dir = joinpath(settings.output_root, "notebook_tables")
    ranking_draws_csv = write_notebook_csv(
        joinpath(notebook_table_dir, "extra_diagnostics_ranking_draws_compact.csv"),
        ranking_draws_compact,
    )
    effective_draws_csv = write_notebook_csv(
        joinpath(notebook_table_dir, "extra_diagnostics_effective_draws_compact.csv"),
        effective_draws_compact,
    )
    ranking_summary_csv = write_notebook_csv(
        joinpath(notebook_table_dir, "extra_diagnostics_ranking_summary_compact.csv"),
        ranking_summary_compact,
    )
    effective_summary_csv = write_notebook_csv(
        joinpath(notebook_table_dir, "extra_diagnostics_effective_summary_compact.csv"),
        effective_summary_compact,
    )
end

# ╔═╡ 83ce9a92-3e36-4058-ad9b-316c4578a949
local_outputs = pp.DataFrame(
    artifact = [
        "ranking draw diagnostics",
        "effective-count draw diagnostics",
        "ranking support summary",
        "effective-count summary",
    ],
    path = [
        ranking_draws_csv,
        effective_draws_csv,
        ranking_summary_csv,
        effective_summary_csv,
    ],
    rows = [
        pp.nrow(ranking_draws_compact),
        pp.nrow(effective_draws_compact),
        pp.nrow(ranking_summary_compact),
        pp.nrow(effective_summary_compact),
    ],
)

# ╔═╡ ef6b9d33-929a-44f1-b154-c04367dd066c
small_table(local_outputs; n = pp.nrow(local_outputs))

# ╔═╡ b3da13ed-72f8-4429-8a5e-a407d5cb04ef
function is_under_directory(child_path::AbstractString, parent_path::AbstractString)
    relative_path = relpath(normpath(child_path), normpath(parent_path))
    return relative_path == "." || (!startswith(relative_path, "..") && !isabspath(relative_path))
end

# ╔═╡ 696914f3-f9e5-41aa-bbd8-0f97701391b6
validation_table = pp.DataFrame(
    check = [
        "linearized artifacts under notebook cache",
        "notebook CSV outputs under notebook output root",
        "draw count equals B * R * K across batch items",
    ],
    value = [
        string(all(path -> is_under_directory(resolve_nb_path(path), settings.cache_root), linearized_rows.path)),
        string(all(path -> is_under_directory(path, settings.output_root), local_outputs.path)),
        string(pp.nrow(effective_draws) == sum(item.spec.B * item.spec.R * item.spec.K for item in batch.items)),
    ],
)

# ╔═╡ 33895577-0e14-4e65-857f-0fb9bc164aae
small_table(validation_table; n = pp.nrow(validation_table))

# ╔═╡ 36d03605-3266-4ceb-a63c-a78365d6da45
TableOfContents()

# ╔═╡ 7e9afa2b-ccf8-4f0f-94be-6139c65ec58f
begin
    @bind selected_leaf_index Select(1:pp.nrow(linearized_rows))
end

# ╔═╡ 4ed19b2a-1444-49aa-800c-c239b6efddf5
selected_linearized_leaf = linearized_leaf_table[selected_leaf_index:selected_leaf_index, :]

# ╔═╡ 0f77ce68-e455-4243-9d96-b2142f435858
small_table(selected_linearized_leaf; n = pp.nrow(selected_linearized_leaf))

# ╔═╡ 8e9256c6-19e9-4f57-807d-6ddfa9286314
selected_effective_draw = effective_draws_compact[
    (effective_draws_compact.b .== linearized_rows[selected_leaf_index, :b]) .&
    (effective_draws_compact.r .== linearized_rows[selected_leaf_index, :r]) .&
    (effective_draws_compact.k .== linearized_rows[selected_leaf_index, :k]) .&
    (effective_draws_compact.m .== linearized_rows[selected_leaf_index, :m]),
    :,
]

# ╔═╡ e7b62e56-e58c-4048-aba3-fa0010dcf656
small_table(selected_effective_draw; n = pp.nrow(selected_effective_draw))

# ╔═╡ Cell order:
# ╠═26f5b7cf-f12f-4ef3-b6e8-9f39e176a9a1
# ╟─a1b9f4a4-01d5-4a85-9ef4-efb3aeeff1f1
# ╠═4d35c77f-d36b-45d8-a329-7d0ee29760e1
# ╠═5c7fb3d7-493e-4344-9e86-67ad86be413d
# ╠═50c02f04-d465-46da-a921-b30f1ef43322
# ╠═3d9588c9-d56e-42dd-8685-6ed2ab801ae1
# ╠═a07632bf-1858-4905-b93f-476e7856d532
# ╠═3f55b9f1-bcad-4f36-b10b-93cefa9db5ea
# ╟─451c9a56-d696-4bdb-b0ff-bc95f40a7953
# ╠═84491fdb-9c3d-4d39-966e-c4e83f033fb7
# ╠═58fc4f56-54ac-44e0-b1c9-65d39efb7601
# ╠═a46d2dbb-b962-4e5a-8679-4bf780468f75
# ╟─d8792324-1f16-4c84-bb04-b3701c853132
# ╠═2d489968-dba1-489d-b530-44af894e92f6
# ╠═856f58b8-65cf-405f-a2cc-aa41f2cf3b77
# ╠═cd762b99-b1cf-442f-a563-1fb8ebea2344
# ╠═d0f28631-54ed-49f9-82ec-cfe7073145f1
# ╠═34bfb4d5-c641-4598-a743-d6374450a9e0
# ╠═f0c76616-35cb-4dd0-82b0-ea0c591183a1
# ╠═e8050948-5074-4eca-91f9-4d3bd27211e2
# ╠═a330a8c2-c026-4cd6-9e3b-21dfe5e9952e
# ╠═ef8d9e29-33e3-497d-813e-6067f3ea24d9
# ╠═5f6c90d7-7da7-4468-a596-cbf42091f7c8
# ╠═61ca5f0f-7987-472e-99d3-e553b4a8410a
# ╟─f8e0409e-192f-42ee-8f38-55d107c55f32
# ╠═d1353b5c-a142-429e-b3c6-0d9c5cd37a21
# ╠═08035708-2a6d-45fd-b886-c0f349fba4eb
# ╠═1e19ca53-2716-469d-a214-2dde55fbe55e
# ╠═ed9b3a67-fb13-4c76-8f15-4e991015b294
# ╠═0a461086-03c5-41ab-b95d-89654ca5d9d0
# ╟─c18e4a03-c656-4a1a-bf78-0a2022d44dbb
# ╠═4f261419-a0f6-40bc-a4e4-955f7c3de79d
# ╠═baebaa2c-17b1-4d8c-b6f3-16f0c9d0663f
# ╠═64350afc-9082-4c3b-8da8-417658697a3f
# ╠═94642928-b6ef-4864-9e1b-631d0f325abd
# ╠═f37d3f1a-1f4f-45a1-a37d-991a8c38ea27
# ╠═83ce9a92-3e36-4058-ad9b-316c4578a949
# ╠═ef6b9d33-929a-44f1-b154-c04367dd066c
# ╠═b3da13ed-72f8-4429-8a5e-a407d5cb04ef
# ╠═696914f3-f9e5-41aa-bbd8-0f97701391b6
# ╠═33895577-0e14-4e65-857f-0fb9bc164aae
# ╠═36d03605-3266-4ceb-a63c-a78365d6da45
# ╠═7e9afa2b-ccf8-4f0f-94be-6139c65ec58f
# ╠═4ed19b2a-1444-49aa-800c-c239b6efddf5
# ╠═0f77ce68-e455-4243-9d96-b2142f435858
# ╠═8e9256c6-19e9-4f57-807d-6ddfa9286314
# ╠═e7b62e56-e58c-4048-aba3-fa0010dcf656
