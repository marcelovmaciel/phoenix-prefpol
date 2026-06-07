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

# ╔═╡ 4c9c0dc8-1f16-4fcf-b31e-bf348a5c71f1
begin
    import Pkg
    Pkg.activate(@__DIR__)
end

# ╔═╡ 8e6c729a-1671-4e0a-94c2-9c418c8d9bcf
begin
    # Load shared notebook helpers and the local PrefPol package.
    include(joinpath(@__DIR__, "notebook_common.jl"))
end

# ╔═╡ 2dd177aa-ff1a-46c5-9291-7fae041e07a2
md"""
# Resampling and Imputation

This notebook reproduces the first stochastic steps of the nested PrefPol
workflow at notebook scale.

**Resampling** means drawing weighted bootstrap samples from the observed survey
rows. Each bootstrap branch is indexed by `b`, and each branch keeps the same
number of rows as the observed data while allowing respondents to appear zero,
one, or many times.

**Imputation** means completing missing candidate score cells inside each
bootstrap sample. Each imputation replicate is indexed by `r` within a bootstrap
branch. The notebook default uses deterministic `zero` imputation because it is
fast, local, and does not require R or `mice`; if both `zero` and `mice` are
configured, the manifests below show both backends as separate batch items.

In the nested design, `B` is the number of bootstrap samples and `R` is the
number of imputations per bootstrap sample. This corresponds to the production
resampling and imputation passes, but this notebook uses
`nb/notebook_config.toml` and writes only under `nb/output/notebook_smoke`.
"""

# ╔═╡ 98aa628b-9b14-4e6a-a0ca-8d68153c79c4
begin
    # Load the notebook-scale orchestration config.
    cfg = load_notebook_config()
    settings = notebook_settings(cfg)
    targets = notebook_targets(cfg)
end

# ╔═╡ 6ca3f04d-0851-4739-a620-0c6260acac51
begin
    @assert settings.B <= 5 "Notebook config keeps B <= 5."
    @assert settings.R <= 5 "Notebook config keeps R <= 5."
    @assert settings.K <= 5 "Notebook config keeps K <= 5."
    ensure_not_publication_output!(settings.output_root)
    ensure_not_publication_output!(settings.cache_root)
end

# ╔═╡ d26c6c96-4732-4d49-a01a-344627bb117f
begin
    # Load survey-wave configs and construct the notebook batch.
    waves, source_registry, wave_by_id = load_notebook_waves()
    batch = build_notebook_batch(cfg)
end

# ╔═╡ fa45769d-0a25-40c9-90a3-f84278c14875
begin
    # The first batch item is the running notebook example.
    selected_index = 1
    selected_item = batch.items[selected_index]
    selected_spec = selected_item.spec
    selected_batch = pp.StudyBatchSpec([selected_item])
end

# ╔═╡ a58c4ffc-f6dc-472d-bd08-3ab8d0697ba7
selected_spec_table = pp.DataFrame(
    item = [
        "batch index",
        "wave",
        "scenario",
        "m",
        "active candidates",
        "B",
        "R",
        "K",
        "imputer backend",
        "linearizer policy",
        "cache root",
        "output root",
    ],
    value = [
        string(selected_index),
        selected_spec.wave_id,
        selected_item.metadata.scenario_name,
        string(selected_item.metadata.m),
        join(selected_spec.active_candidates, ", "),
        string(selected_spec.B),
        string(selected_spec.R),
        string(selected_spec.K),
        String(selected_spec.imputer_backend),
        String(selected_spec.linearizer_policy),
        settings.cache_root,
        settings.output_root,
    ],
)

# ╔═╡ 7f7a83be-961e-4760-8842-b857c02627f6
small_table(selected_spec_table; n = pp.nrow(selected_spec_table))

# ╔═╡ a99b83ea-bde6-4ccf-9bee-a2f8d92e953c
md"""
## Resampling

The CLI bootstrap stage constructs a `NestedStochasticPipeline` and calls
`PrefPol.ensure_resamples!`. The same public API is used here, with the notebook
cache root from `nb/notebook_config.toml`.
"""

# ╔═╡ e12e9d14-9540-4267-8188-a57707d32b72
begin
    pipeline = pp.NestedStochasticPipeline(source_registry; cache_root = settings.cache_root)
    cache_dir = pp.pipeline_cache_dir(pipeline, selected_spec)
end

# ╔═╡ 40611b6e-c658-47c3-95b9-eb0bd69cc92e
begin
    resample_raw_manifest = pp.ensure_resamples!(
        pipeline,
        selected_spec;
        force = settings.force,
    )
    resample_manifest = sorted_table(stage_manifest([resample_raw_manifest], selected_batch, pipeline))
end

# ╔═╡ 32741471-8900-455d-b149-ae2549f2d31f
resample_manifest_compact = pp.select(
    resample_manifest,
    intersect(
        [:stage, :b, :r, :k, :seed, :artifact_kind, :path, :wave_id, :scenario_name, :m, :B, :R],
        propertynames(resample_manifest),
    ),
)

# ╔═╡ fb58d660-8ada-4382-8053-3a4ab8ed0f5c
small_table(resample_manifest_compact; n = pp.nrow(resample_manifest_compact))

# ╔═╡ 4dc60b1e-545e-4582-9080-7cd663cb1fb6
md"""
Manifest columns identify the nested branch (`stage`, `b`, `r`, `k`), the
reproducibility seed used for stochastic stages, the artifact kind, and the
cache path where the artifact was written. For resampling, rows with
`stage = :resample` are the bootstrap branches; `:spec` and `:observed` are
upstream cache artifacts needed to make the bootstrap reproducible.
"""

# ╔═╡ 47be0c6d-05a3-4d5c-9a86-8e175bf504f6
resample_artifact_locations = pp.DataFrame(
    item = ["pipeline cache directory", "resample artifacts"],
    value = [
        cache_dir,
        join(
            resample_manifest.path[resample_manifest.stage .== :resample],
            "\n",
        ),
    ],
)

# ╔═╡ 3b769033-09e6-4fa7-9fb3-a78391f4d564
small_table(resample_artifact_locations; n = pp.nrow(resample_artifact_locations))

# ╔═╡ 20fb42f3-6cfa-4b14-8a46-b26e5e98acea
md"""
The first bootstrap artifact stores sampled row indices and multiplicities
rather than a large duplicate table. The diagnostics above show the core
bootstrap behavior: the resample has the same row count as the observed data,
but the number of unique respondents is smaller because sampling is with
replacement.
"""

# ╔═╡ 251afc25-d026-4bdc-9e03-2389255783a2
md"""
## Imputation

The CLI imputation stage calls `PrefPol.ensure_imputations!`. That function
creates any missing upstream resamples first, then writes one imputed artifact
for each `(b, r)` branch.
"""

# ╔═╡ bf085e43-0e55-4064-95a9-9e93559f4c7c
begin
    imputation_raw_manifest = pp.ensure_imputations!(
        pipeline,
        selected_spec;
        force = settings.force,
    )
    imputation_manifest = sorted_table(stage_manifest([imputation_raw_manifest], selected_batch, pipeline))
end

# ╔═╡ 11b68308-c611-468b-8315-72ed8786ac4e
imputation_manifest_compact = pp.select(
    imputation_manifest,
    intersect(
        [:stage, :b, :r, :k, :seed, :artifact_kind, :path, :wave_id, :scenario_name, :m, :B, :R, :imputer_backend],
        propertynames(imputation_manifest),
    ),
)

# ╔═╡ e9d2176a-264d-4702-b9e2-1082c8764ac0
small_table(imputation_manifest_compact; n = pp.nrow(imputation_manifest_compact))

# ╔═╡ 02d3bf03-b094-4c2d-a06e-b5836eb9df10
function score_missing_count(df, cols)
    return sum(col -> count(x -> pp.is_eseb_missing_score(x), df[!, col]), cols)
end

# ╔═╡ 619c7969-6255-4d4b-88a7-b72efad68175
begin
    configured_backend_set = Set(Symbol.(settings.imputer_backends))
    backend_note = if (:zero in configured_backend_set) && (:mice in configured_backend_set)
    md"""
    Both `zero` and `mice` are configured, so each backend appears as its own
    batch item. This notebook selected the first configured item above; use
    `selected_index` to inspect the other backend.
    """
else
    md"""
    The notebook default config uses `zero` only. That keeps this interactive
    run self-contained and avoids requiring R, `haven`, and `mice`. To compare
    against `mice`, edit `nb/notebook_config.toml` so
    `imputer_backends = ["mice", "zero"]`, then rerun the notebook.
    """
    end
end

# ╔═╡ 2c6fb4c0-40eb-4d8e-bbdf-48f232d49222
backend_note

# ╔═╡ 2345c5bd-fc0d-4665-aa83-1912038d627c
md"""
## Notebook Artifacts

The pipeline cache artifacts above are written by `PrefPol.ensure_*` under the
notebook cache root. The following compact CSV summaries are notebook-local
inspection tables, not production manifests.
"""

# ╔═╡ 8f393058-eeb3-4426-bd07-e4d5ce7e9a27
begin
    resample_artifacts_exist = all(isfile, resample_raw_manifest.path[resample_raw_manifest.stage .== :resample])
    imputation_artifacts_exist = all(isfile, imputation_raw_manifest.path[imputation_raw_manifest.stage .== :imputed])
    production_manifest_touched = isfile(joinpath(NOTEBOOK_PUBLICATION_OUTPUT, "manifests", "bootstrap_manifest.csv")) ||
                                  isfile(joinpath(NOTEBOOK_PUBLICATION_OUTPUT, "manifests", "imputation_manifest.csv"))
    validation_table = pp.DataFrame(
        check = [
            "resample artifacts exist",
            "imputation artifacts exist",
            "notebook output root",
            "publication manifests present",
        ],
        value = [
            string(resample_artifacts_exist),
            string(imputation_artifacts_exist),
            settings.output_root,
            string(production_manifest_touched),
        ],
    )
end

# ╔═╡ 30f6a4d3-2e6f-4470-bb74-99abe98905dc
small_table(validation_table; n = pp.nrow(validation_table))

# ╔═╡ be46be93-76d2-474b-9d7b-407a193059ad
TableOfContents()

# ╔═╡ 00fb774b-0539-4870-9ea1-12f2bde481bb
begin
    @bind selected_b Select(1:selected_spec.B)
end

# ╔═╡ 71210589-9dc8-41ac-8ed5-17d2c2f79508
begin
    observed = pp.ensure_observed!(pipeline, selected_spec; force = false)
    first_resample_path = only(resample_raw_manifest.path[
        (resample_raw_manifest.stage .== :resample) .& (resample_raw_manifest.b .== selected_b)
    ])
    first_resample_artifact = pp.load_stage_artifact(first_resample_path)
    first_resample_indices = Vector{Int}(first_resample_artifact.indices)
    first_resampled_scores = observed.scores[first_resample_indices, :]
end

# ╔═╡ c4ec6268-a850-4aaa-8e35-54943d2bd7c5
resample_diagnostics = pp.DataFrame(
    item = [
        "observed rows",
        "resampled rows",
        "unique observed rows drawn",
        "observed rows not drawn",
        "largest row multiplicity",
        "resample seed",
    ],
    value = [
        string(pp.nrow(observed.scores)),
        string(length(first_resample_indices)),
        string(length(unique(first_resample_indices))),
        string(count(==(0), Vector{Int}(first_resample_artifact.multiplicities))),
        string(maximum(Vector{Int}(first_resample_artifact.multiplicities))),
        string(first_resample_artifact.seed),
    ],
)

# ╔═╡ 605c4198-1900-47e3-8fc6-3b01af87fa4c
small_table(resample_diagnostics; n = pp.nrow(resample_diagnostics))

# ╔═╡ d4996692-0ee4-468a-85a9-cf429b6f357e
begin
    @bind selected_r Select(1:selected_spec.R)
end

# ╔═╡ b42c3a64-7b56-49e6-a88c-49885dc68373
begin
    first_imputed_path = only(imputation_raw_manifest.path[
        (imputation_raw_manifest.stage .== :imputed) .&
        (imputation_raw_manifest.b .== selected_b) .&
        (imputation_raw_manifest.r .== selected_r)
    ])
    first_imputed_artifact = pp.load_stage_artifact(first_imputed_path)
    first_imputed_table = pp.DataFrame(first_imputed_artifact.table)
end

# ╔═╡ ad1c2021-d1b4-42f9-85e6-0115ae11d10a
begin
    score_cols = selected_spec.active_candidates
    imputation_diagnostics = pp.DataFrame(
        item = [
            "imputed artifact",
            "backend",
            "bootstrap branch b",
            "imputation branch r",
            "rows before imputation",
            "rows after imputation",
            "score columns inspected",
            "missing score cells before",
            "missing score cells after",
            "imputation seed",
        ],
        value = [
            first_imputed_path,
            String(first_imputed_artifact.backend),
            string(selected_b),
            string(selected_r),
            string(pp.nrow(first_resampled_scores)),
            string(pp.nrow(first_imputed_table)),
            join(score_cols, ", "),
            string(score_missing_count(first_resampled_scores, score_cols)),
            string(score_missing_count(first_imputed_table, score_cols)),
            string(first_imputed_artifact.seed),
        ],
    )
end

# ╔═╡ b0c8296b-de01-4ca6-b911-7d0caf90c5fd
small_table(imputation_diagnostics; n = pp.nrow(imputation_diagnostics))

# ╔═╡ d9451515-7216-4131-89fd-9710151616e5
begin
    notebook_table_dir = joinpath(settings.output_root, "notebook_tables")
    bootstrap_summary_csv = write_notebook_csv(
        joinpath(notebook_table_dir, "bootstrap_manifest_compact.csv"),
        resample_manifest_compact,
    )
    imputation_summary_csv = write_notebook_csv(
        joinpath(notebook_table_dir, "imputation_manifest_compact.csv"),
        imputation_manifest_compact,
    )
    resample_diagnostics_csv = write_notebook_csv(
        joinpath(notebook_table_dir, "resample_diagnostics.csv"),
        resample_diagnostics,
    )
    imputation_diagnostics_csv = write_notebook_csv(
        joinpath(notebook_table_dir, "imputation_diagnostics.csv"),
        imputation_diagnostics,
    )
end

# ╔═╡ 14da4a04-ef2c-4a73-b122-9a085d04318f
notebook_csv_table = pp.DataFrame(
    artifact = [
        "bootstrap manifest summary",
        "imputation manifest summary",
        "resample diagnostics",
        "imputation diagnostics",
    ],
    path = [
        bootstrap_summary_csv,
        imputation_summary_csv,
        resample_diagnostics_csv,
        imputation_diagnostics_csv,
    ],
)

# ╔═╡ 0b052b23-e2b5-4090-8d87-4db780c47aa8
small_table(notebook_csv_table; n = pp.nrow(notebook_csv_table))

# ╔═╡ 106274a2-73a9-4475-9e2a-06e00086ecef
observed_score_rows = candidate_score_preview(
    observed.scores,
    selected_spec.active_candidates;
    n = 10,
)

# ╔═╡ dbb733eb-9ec9-4e79-97c8-3a396b177bc2
small_table(observed_score_rows; n = pp.nrow(observed_score_rows))

# ╔═╡ 8260b68e-f14c-4175-8f8b-3ab165fb5933
observed_missingness = candidate_missingness_table(observed.scores, selected_spec.active_candidates)

# ╔═╡ 0d68961c-0b3e-4c7f-b89a-deca0dc61530
small_table(observed_missingness; n = pp.nrow(observed_missingness))

# ╔═╡ a46918b9-dee9-4028-9e7a-280105b7375c
bootstrap_multiplicities = resample_multiplicity_table(first_resample_artifact; n = 20)

# ╔═╡ 06d5cf73-9ccb-4209-8678-85c7cc839942
small_table(bootstrap_multiplicities; n = pp.nrow(bootstrap_multiplicities))

# ╔═╡ 5062b9d6-19e3-4a17-9887-da2111d09133
begin
    @bind selected_row Select(1:min(pp.nrow(first_imputed_table), 20))
end

# ╔═╡ cef385d0-e0a9-46fe-94f9-32b991f9ab6f
imputation_row_comparison = before_after_score_rows(
    first_resampled_scores,
    first_imputed_table,
    selected_spec.active_candidates,
    [selected_row],
)

# ╔═╡ 49d0f8a4-25df-439f-8e65-a99a297ba7be
small_table(imputation_row_comparison; n = pp.nrow(imputation_row_comparison))

# ╔═╡ 07a65c34-94aa-452f-8527-0b8204b8b87b
imputation_missingness_by_candidate = vcat(
    transform(candidate_missingness_table(first_resampled_scores, selected_spec.active_candidates), [] => (() -> "before") => :stage),
    transform(candidate_missingness_table(first_imputed_table, selected_spec.active_candidates), [] => (() -> "after") => :stage);
    cols = :union,
)

# ╔═╡ 2963efd9-152b-4238-9e81-f524e50dead4
small_table(imputation_missingness_by_candidate; n = pp.nrow(imputation_missingness_by_candidate))

# ╔═╡ Cell order:
# ╠═4c9c0dc8-1f16-4fcf-b31e-bf348a5c71f1
# ╟─2dd177aa-ff1a-46c5-9291-7fae041e07a2
# ╠═8e6c729a-1671-4e0a-94c2-9c418c8d9bcf
# ╠═98aa628b-9b14-4e6a-a0ca-8d68153c79c4
# ╠═6ca3f04d-0851-4739-a620-0c6260acac51
# ╠═d26c6c96-4732-4d49-a01a-344627bb117f
# ╠═fa45769d-0a25-40c9-90a3-f84278c14875
# ╠═a58c4ffc-f6dc-472d-bd08-3ab8d0697ba7
# ╠═7f7a83be-961e-4760-8842-b857c02627f6
# ╟─a99b83ea-bde6-4ccf-9bee-a2f8d92e953c
# ╠═e12e9d14-9540-4267-8188-a57707d32b72
# ╠═40611b6e-c658-47c3-95b9-eb0bd69cc92e
# ╠═32741471-8900-455d-b149-ae2549f2d31f
# ╠═fb58d660-8ada-4382-8053-3a4ab8ed0f5c
# ╟─4dc60b1e-545e-4582-9080-7cd663cb1fb6
# ╠═47be0c6d-05a3-4d5c-9a86-8e175bf504f6
# ╠═3b769033-09e6-4fa7-9fb3-a78391f4d564
# ╠═71210589-9dc8-41ac-8ed5-17d2c2f79508
# ╠═c4ec6268-a850-4aaa-8e35-54943d2bd7c5
# ╠═605c4198-1900-47e3-8fc6-3b01af87fa4c
# ╟─20fb42f3-6cfa-4b14-8a46-b26e5e98acea
# ╟─251afc25-d026-4bdc-9e03-2389255783a2
# ╠═bf085e43-0e55-4064-95a9-9e93559f4c7c
# ╠═11b68308-c611-468b-8315-72ed8786ac4e
# ╠═e9d2176a-264d-4702-b9e2-1082c8764ac0
# ╠═b42c3a64-7b56-49e6-a88c-49885dc68373
# ╠═02d3bf03-b094-4c2d-a06e-b5836eb9df10
# ╠═ad1c2021-d1b4-42f9-85e6-0115ae11d10a
# ╠═b0c8296b-de01-4ca6-b911-7d0caf90c5fd
# ╠═619c7969-6255-4d4b-88a7-b72efad68175
# ╠═2c6fb4c0-40eb-4d8e-bbdf-48f232d49222
# ╟─2345c5bd-fc0d-4665-aa83-1912038d627c
# ╠═d9451515-7216-4131-89fd-9710151616e5
# ╠═14da4a04-ef2c-4a73-b122-9a085d04318f
# ╠═0b052b23-e2b5-4090-8d87-4db780c47aa8
# ╠═8f393058-eeb3-4426-bd07-e4d5ce7e9a27
# ╠═30f6a4d3-2e6f-4470-bb74-99abe98905dc
# ╠═be46be93-76d2-474b-9d7b-407a193059ad
# ╠═00fb774b-0539-4870-9ea1-12f2bde481bb
# ╠═d4996692-0ee4-468a-85a9-cf429b6f357e
# ╠═106274a2-73a9-4475-9e2a-06e00086ecef
# ╠═dbb733eb-9ec9-4e79-97c8-3a396b177bc2
# ╠═8260b68e-f14c-4175-8f8b-3ab165fb5933
# ╠═0d68961c-0b3e-4c7f-b89a-deca0dc61530
# ╠═a46918b9-dee9-4028-9e7a-280105b7375c
# ╠═06d5cf73-9ccb-4209-8678-85c7cc839942
# ╠═5062b9d6-19e3-4a17-9887-da2111d09133
# ╠═cef385d0-e0a9-46fe-94f9-32b991f9ab6f
# ╠═49d0f8a4-25df-439f-8e65-a99a297ba7be
# ╠═07a65c34-94aa-452f-8527-0b8204b8b87b
# ╠═2963efd9-152b-4238-9e81-f524e50dead4
