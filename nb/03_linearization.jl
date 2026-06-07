### A Pluto.jl notebook ###
# v0.20.17

using Markdown

# ╔═╡ 2f2ef4ec-51dd-4a42-9d98-0c9f3acfc2f0
begin
    import Pkg
    Pkg.activate(@__DIR__)
end

# ╔═╡ bdbbc6f5-7f3b-4396-a5a2-2b5171f12f8a
md"""
# Linearization

Survey thermometer scores produce weak or evaluative preference data. A voter
can give equal scores to two candidates, and after imputation every active
candidate has a score even when the observed survey row originally had missing
cells. Most downstream preference measures in this pipeline operate on complete
strict rankings, so linearization is the stochastic step that turns tied weak
orders into strict orders while preserving row-level metadata.

`K` is the number of linearization replicates within each imputed bootstrap
branch `(b, r)`. Each `k` uses a reproducible seed and creates one strict-profile
leaf. With `B` bootstraps, `R` imputations, and `K` linearizations, the number
of strict-profile leaves is `B * R * K`.

The two configured policies answer a different question about ties. `random_ties`
breaks tied candidates uniformly at random row by row. `pattern_conditional`
learns from the empirical weak-ranking pattern in the imputed profile and uses
that pattern as a conditional tie-breaking rule, with a uniform fallback when a
tie pattern has no usable information. If both policies are configured, they
become separate batch items with separate manifests and cache paths.

This corresponds to the production linearization pass: both construct a
`NestedStochasticPipeline` and call `PrefPol.ensure_linearizations!`. This
notebook does not call the CLI wrapper; it uses `nb/notebook_config.toml` and
writes only under the notebook output/cache roots.
"""

# ╔═╡ b3483a33-a7b4-4951-830d-44e267d74bf2
begin
    # Load shared notebook helpers and the local PrefPol package.
    include(joinpath(@__DIR__, "notebook_common.jl"))
    using Preferences
end

# ╔═╡ 2ad8a9df-d005-4fc9-b3e5-29f730b18a56
begin
    # Load the notebook-scale orchestration config.
    notebook_config = load_notebook_config()
    notebook_run_settings = notebook_settings(notebook_config)
    notebook_targets_selected = notebook_targets(notebook_config)
end

# ╔═╡ 1d3a2146-37fd-4e0e-9b20-d7da7f8f233c
begin
    @assert notebook_run_settings.B <= 5 "Notebook config keeps B <= 5."
    @assert notebook_run_settings.R <= 5 "Notebook config keeps R <= 5."
    @assert notebook_run_settings.K <= 5 "Notebook config keeps K <= 5."
    ensure_not_publication_output!(notebook_run_settings.output_root)
    ensure_not_publication_output!(notebook_run_settings.cache_root)
end

# ╔═╡ b53e692b-4a5f-4b4d-99ce-16d089cd67b7
begin
    # Load survey-wave configs and construct the notebook batch.
    notebook_waves, notebook_source_registry, notebook_wave_by_id = load_notebook_waves()
    notebook_batch = build_notebook_batch(notebook_config)
end

# ╔═╡ 6f1760ad-4c99-41fd-af95-c40528503ed8
begin
    # The first batch item is the running notebook example.
    selected_batch_index = 1
    selected_batch_item = notebook_batch.items[selected_batch_index]
    selected_pipeline_spec = selected_batch_item.spec
    selected_notebook_batch = pp.StudyBatchSpec([selected_batch_item])
end

# ╔═╡ f48d638e-f92d-419e-b983-1a27f0fae8c5
selected_spec_table = DataFrame(
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
        string(selected_batch_index),
        selected_pipeline_spec.wave_id,
        selected_batch_item.metadata.scenario_name,
        string(selected_batch_item.metadata.m),
        join(selected_pipeline_spec.active_candidates, ", "),
        string(selected_pipeline_spec.B),
        string(selected_pipeline_spec.R),
        string(selected_pipeline_spec.K),
        String(selected_pipeline_spec.imputer_backend),
        String(selected_pipeline_spec.linearizer_policy),
        notebook_run_settings.cache_root,
        notebook_run_settings.output_root,
    ],
)

# ╔═╡ cbf6ae8d-95dc-47b0-8122-599fa00ccf4d
small_table(selected_spec_table; n = nrow(selected_spec_table))

# ╔═╡ 42adf18e-33d1-4d11-847e-930ef50cd509
begin
    configured_linearizer_set = Set(Symbol.(notebook_run_settings.linearizer_policies))
    linearizer_configuration_note = if (:pattern_conditional in configured_linearizer_set) &&
                                       (:random_ties in configured_linearizer_set)
        md"""
        Both `pattern_conditional` and `random_ties` are configured. The batch
        expands them into separate items, so change `selected_batch_index` to
        inspect the other policy.
        """
    else
        md"""
        The current notebook config selects one linearizer policy. To compare
        policies side by side, edit `nb/notebook_config.toml` so
        `linearizer_policies = ["pattern_conditional", "random_ties"]`, then
        rerun this notebook.
        """
    end
end

# ╔═╡ 81a94918-bc86-4368-a5fb-d7630d946f7c
linearizer_configuration_note

# ╔═╡ d3f3ab4d-ea69-4c23-9e7d-76984a7267f9
begin
    # Instantiate the pipeline object used by the production workflow.
    notebook_pipeline = pp.NestedStochasticPipeline(
        notebook_source_registry;
        cache_root = notebook_run_settings.cache_root,
    )
    selected_cache_dir = pp.pipeline_cache_dir(notebook_pipeline, selected_pipeline_spec)
end

# ╔═╡ 30a1bf9c-f7b9-4412-9285-890640dfb0d0
md"""
## Upstream Artifacts

These upstream calls are idempotent and use the notebook cache. They make this
notebook runnable even when `02_resampling_and_imputation.jl` has not already
been evaluated for the selected tiny spec.
"""

# ╔═╡ 2bd86e1d-1356-4a8e-9057-19b85f4d68fd
begin
    upstream_resample_manifest_raw = pp.ensure_resamples!(
        notebook_pipeline,
        selected_pipeline_spec;
        force = notebook_run_settings.force,
    )
    upstream_imputation_manifest_raw = pp.ensure_imputations!(
        notebook_pipeline,
        selected_pipeline_spec;
        force = notebook_run_settings.force,
    )
end

# ╔═╡ 782196ef-1694-4a90-9978-c0a9e90c045b
upstream_artifact_counts = DataFrame(
    stage = ["resample", "imputed"],
    artifacts = [
        count(==(:resample), upstream_resample_manifest_raw.stage),
        count(==(:imputed), upstream_imputation_manifest_raw.stage),
    ],
)

# ╔═╡ 758e847d-195a-43d3-a839-6d576766b46d
small_table(upstream_artifact_counts; n = nrow(upstream_artifact_counts))

# ╔═╡ 86beb017-587c-420f-97a3-c42b0849f7d8
md"""
## Linearization

`PrefPol.ensure_linearizations!` creates one compact strict-profile artifact
for each `(b, r, k)` leaf. It also creates any missing upstream artifacts, but
the explicit upstream calls above make that dependency visible.
"""

# ╔═╡ 2f6953bb-d6b8-402f-862f-f6bd2714e08d
begin
    linearization_manifest_raw = pp.ensure_linearizations!(
        notebook_pipeline,
        selected_pipeline_spec;
        force = notebook_run_settings.force,
    )
    linearization_manifest = sorted_table(
        stage_manifest([linearization_manifest_raw], selected_notebook_batch, notebook_pipeline),
    )
end

# ╔═╡ 6de996a9-6d9b-412d-89da-c58ab958c8a3
linearization_manifest_compact = select(
    linearization_manifest,
    intersect(
        [
            :stage,
            :b,
            :r,
            :k,
            :seed,
            :artifact_kind,
            :path,
            :wave_id,
            :scenario_name,
            :m,
            :B,
            :R,
            :K,
            :imputer_backend,
            :linearizer_policy,
        ],
        propertynames(linearization_manifest),
    ),
)

# ╔═╡ b1c24da9-95b7-4541-94ee-406e91160c28
small_table(linearization_manifest_compact; n = nrow(linearization_manifest_compact))

# ╔═╡ 1f98af1f-14da-47c0-9700-3785cb45095e
begin
    first_linearized_path = only(linearization_manifest_raw.path[
        (linearization_manifest_raw.stage .== :linearized) .&
        (linearization_manifest_raw.b .== 1) .&
        (linearization_manifest_raw.r .== 1) .&
        (linearization_manifest_raw.k .== 1)
    ])
    first_linearized_artifact_table = DataFrame(pp.load_stage_artifact(first_linearized_path))
    first_linearized_bundle = Preferences.dataframe_to_annotated_profile(
        first_linearized_artifact_table;
        ballot_kind = :strict,
    )
    first_linearized_rankings = Preferences.profile_to_ranking_dicts(first_linearized_bundle)
end

# ╔═╡ ad6dc95f-28a2-4710-8fb2-93eb6e811124
first_linearized_artifact_summary = DataFrame(
    item = [
        "artifact path",
        "artifact rows",
        "profile ballots",
        "candidate tuple",
        "policy",
        "seed",
    ],
    value = [
        first_linearized_path,
        string(nrow(first_linearized_artifact_table)),
        string(Preferences.nballots(first_linearized_bundle.profile)),
        join(String.(Preferences.candidates(first_linearized_bundle.profile.pool)), ", "),
        String(selected_pipeline_spec.linearizer_policy),
        string(only(linearization_manifest_raw.seed[
            (linearization_manifest_raw.stage .== :linearized) .&
            (linearization_manifest_raw.b .== 1) .&
            (linearization_manifest_raw.r .== 1) .&
            (linearization_manifest_raw.k .== 1)
        ])),
    ],
)

# ╔═╡ bb3c0e65-db64-4ba9-9124-d29d1da233df
small_table(first_linearized_artifact_summary; n = nrow(first_linearized_artifact_summary))

# ╔═╡ 8a69364d-bd8a-4670-a16a-3cbf68b892e3
function ranking_signature_text(ranking::AbstractDict)
    ranking_pairs = collect(ranking)
    sort!(ranking_pairs; by = ranking_pair -> (Int(last(ranking_pair)), string(first(ranking_pair))))
    return join(String.(first.(ranking_pairs)), " > ")
end

# ╔═╡ 9bfa7f28-c725-4e44-b6fb-1a05895c70bf
begin
    ranking_preview_count = min(8, length(first_linearized_rankings))
    strict_ranking_preview_table = DataFrame(
        row = collect(1:ranking_preview_count),
        strict_ranking = ranking_signature_text.(first_linearized_rankings[1:ranking_preview_count]),
    )
end

# ╔═╡ 35d7d2d1-091d-4619-8613-9e6f18fb9dc9
small_table(strict_ranking_preview_table; n = nrow(strict_ranking_preview_table))

# ╔═╡ 98a37fe5-5dfb-48f1-b3fe-c4722d9e43a7
begin
    strict_signature_frame = DataFrame(
        strict_ranking = ranking_signature_text.(first_linearized_rankings),
    )
    unique_ranking_counts = combine(
        groupby(strict_signature_frame, :strict_ranking),
        nrow => :count,
    )
    sort!(unique_ranking_counts, [:count, :strict_ranking]; rev = [true, false])
end

# ╔═╡ c501c712-ec84-4cd5-8382-6892541d49e3
small_table(unique_ranking_counts; n = min(10, nrow(unique_ranking_counts)))

# ╔═╡ 0447c27e-437a-4e30-979e-974cb22d1ce4
function score_pattern_text(score_row, candidate_cols::Vector{String})
    candidate_score_pairs = [
        (candidate = candidate_name, score = score_row[candidate_name])
        for candidate_name in candidate_cols
    ]
    score_levels = sort!(unique([pair.score for pair in candidate_score_pairs]); rev = true)
    pattern_blocks = String[]
    for score_level in score_levels
        tied_candidates = sort([
            pair.candidate
            for pair in candidate_score_pairs
            if isequal(pair.score, score_level)
        ])
        push!(pattern_blocks, join(tied_candidates, " ~ ") * " (" * string(score_level) * ")")
    end
    return join(pattern_blocks, " > ")
end

# ╔═╡ db22a71d-71a1-4d22-98ee-436f17efbe37
function first_row_with_tied_scores(score_table::AbstractDataFrame, candidate_cols::Vector{String})
    for row_index in 1:nrow(score_table)
        row_scores = [score_table[row_index, candidate_name] for candidate_name in candidate_cols]
        length(unique(row_scores)) < length(row_scores) && return row_index
    end
    return 1
end

# ╔═╡ 4dcb581b-75e3-4fe2-ae1e-1640d13a9939
begin
    first_imputed_path = only(upstream_imputation_manifest_raw.path[
        (upstream_imputation_manifest_raw.stage .== :imputed) .&
        (upstream_imputation_manifest_raw.b .== 1) .&
        (upstream_imputation_manifest_raw.r .== 1)
    ])
    first_imputed_artifact = pp.load_stage_artifact(first_imputed_path)
    first_imputed_table = DataFrame(first_imputed_artifact.table)
    inspection_row_index = first_row_with_tied_scores(
        first_imputed_table,
        selected_pipeline_spec.active_candidates,
    )
end

# ╔═╡ b3833be7-1f78-4528-ab0f-34f8399d3c14
weak_to_strict_example = DataFrame(
    item = [
        "imputed artifact",
        "row",
        "candidate scores",
        "weak/evaluative pattern",
        "linearized strict order",
    ],
    value = [
        first_imputed_path,
        string(inspection_row_index),
        join([
            candidate_name * "=" * string(first_imputed_table[inspection_row_index, candidate_name])
            for candidate_name in selected_pipeline_spec.active_candidates
        ], ", "),
        score_pattern_text(
            first_imputed_table[inspection_row_index, :],
            selected_pipeline_spec.active_candidates,
        ),
        ranking_signature_text(first_linearized_rankings[inspection_row_index]),
    ],
)

# ╔═╡ 0b64c137-ff31-44d1-a22c-fc6c01d93585
small_table(weak_to_strict_example; n = nrow(weak_to_strict_example))

# ╔═╡ 6f8380c3-a8d5-4c8f-9657-7ef743807f01
md"""
## Pedagogical Diagnostics

The strict profile is the leaf of the nested tree. The selected notebook spec
uses tiny `B/R/K` values so the tables remain inspectable.
"""

# ╔═╡ d809438c-06e9-436c-a828-e75e07a77881
begin
    linearized_leaf_manifest = linearization_manifest_raw[
        linearization_manifest_raw.stage .== :linearized,
        :,
    ]
    leaf_count_table = DataFrame(
        item = [
            "B bootstrap branches",
            "R imputations per bootstrap",
            "K linearizations per imputation",
            "(b,r,k) leaves",
        ],
        value = [
            string(selected_pipeline_spec.B),
            string(selected_pipeline_spec.R),
            string(selected_pipeline_spec.K),
            string(nrow(linearized_leaf_manifest)),
        ],
    )
end

# ╔═╡ bb984da3-bc48-4f0f-96d7-f096d0404d67
small_table(leaf_count_table; n = nrow(leaf_count_table))

# ╔═╡ d15b94b3-450e-4bcd-80c7-46fca35c6938
artifact_count_mapping = DataFrame(
    level = [
        "resample",
        "imputed",
        "linearized",
    ],
    index = [
        "b",
        "b, r",
        "b, r, k",
    ],
    count_formula = [
        "B",
        "B * R",
        "B * R * K",
    ],
    artifact_count = [
        count(==(:resample), linearization_manifest_raw.stage),
        count(==(:imputed), linearization_manifest_raw.stage),
        nrow(linearized_leaf_manifest),
    ],
)

# ╔═╡ f8d6786d-a303-4db6-93c0-3f0189affb06
small_table(artifact_count_mapping; n = nrow(artifact_count_mapping))

# ╔═╡ b814ff1a-aabc-4620-bad5-0b0e4ac86eae
linearized_leaves_by_branch = combine(
    groupby(linearized_leaf_manifest, [:b, :r]),
    nrow => :linearized_artifacts,
)

# ╔═╡ 83f5f1ec-212e-47c2-89b6-89fdb066e918
small_table(linearized_leaves_by_branch; n = nrow(linearized_leaves_by_branch))

# ╔═╡ f43c28ef-b57c-4fa4-a237-c10ed1d761d5
md"""
With `K = $(selected_pipeline_spec.K)`, each imputed profile gets only
$(selected_pipeline_spec.K) draws from the tie-breaking distribution. That is
enough to inspect artifact structure, row alignment, policy behavior, and cache
paths. It is not enough for inference because the linearization variance
component is estimated from too few draws; a publication run needs enough `K`
replicates for stable summaries of the tie-breaking uncertainty inside each
`(b, r)` branch.
"""

# ╔═╡ e5d4e324-46c6-4b6e-83c5-92f0f7fc595d
begin
    notebook_table_dir = joinpath(notebook_run_settings.output_root, "notebook_tables")
    linearization_summary_csv = write_notebook_csv(
        joinpath(notebook_table_dir, "linearization_manifest_compact.csv"),
        linearization_manifest_compact,
    )
    linearization_counts_csv = write_notebook_csv(
        joinpath(notebook_table_dir, "linearized_unique_ranking_counts.csv"),
        unique_ranking_counts,
    )
end

# ╔═╡ bcab10ce-5d86-48b6-9679-c66066e2c819
notebook_csv_table = DataFrame(
    artifact = [
        "linearization manifest summary",
        "linearized ranking counts",
    ],
    path = [
        linearization_summary_csv,
        linearization_counts_csv,
    ],
)

# ╔═╡ 26f1025a-e581-4f6f-95cf-984d6e0651db
small_table(notebook_csv_table; n = nrow(notebook_csv_table))

# ╔═╡ c923c6bf-e0d6-4db3-a2e5-a804e7d74de6
function is_under_directory(child_path::AbstractString, parent_path::AbstractString)
    relative_path = relpath(normpath(child_path), normpath(parent_path))
    return relative_path == "." || (!startswith(relative_path, "..") && !isabspath(relative_path))
end

# ╔═╡ aaf4b986-fdf9-48a2-b257-799f6631de42
begin
    linearization_artifacts_exist = all(isfile, linearized_leaf_manifest.path)
    linearization_artifacts_under_cache = all(
        artifact_path -> is_under_directory(artifact_path, notebook_run_settings.cache_root),
        linearized_leaf_manifest.path,
    )
    notebook_outputs_under_output_root = all(
        output_path -> is_under_directory(output_path, notebook_run_settings.output_root),
        notebook_csv_table.path,
    )
    validation_table = DataFrame(
        check = [
            "linearized artifacts exist",
            "linearized artifacts under notebook cache",
            "notebook summaries under notebook output",
            "pipeline cache directory",
        ],
        value = [
            string(linearization_artifacts_exist),
            string(linearization_artifacts_under_cache),
            string(notebook_outputs_under_output_root),
            selected_cache_dir,
        ],
    )
end

# ╔═╡ 090cc7df-6707-49c3-903e-abdbbe688c24
small_table(validation_table; n = nrow(validation_table))

# ╔═╡ Cell order:
# ╠═2f2ef4ec-51dd-4a42-9d98-0c9f3acfc2f0
# ╟─bdbbc6f5-7f3b-4396-a5a2-2b5171f12f8a
# ╠═b3483a33-a7b4-4951-830d-44e267d74bf2
# ╠═2ad8a9df-d005-4fc9-b3e5-29f730b18a56
# ╠═1d3a2146-37fd-4e0e-9b20-d7da7f8f233c
# ╠═b53e692b-4a5f-4b4d-99ce-16d089cd67b7
# ╠═6f1760ad-4c99-41fd-af95-c40528503ed8
# ╠═f48d638e-f92d-419e-b983-1a27f0fae8c5
# ╠═cbf6ae8d-95dc-47b0-8122-599fa00ccf4d
# ╠═42adf18e-33d1-4d11-847e-930ef50cd509
# ╠═81a94918-bc86-4368-a5fb-d7630d946f7c
# ╠═d3f3ab4d-ea69-4c23-9e7d-76984a7267f9
# ╟─30a1bf9c-f7b9-4412-9285-890640dfb0d0
# ╠═2bd86e1d-1356-4a8e-9057-19b85f4d68fd
# ╠═782196ef-1694-4a90-9978-c0a9e90c045b
# ╠═758e847d-195a-43d3-a839-6d576766b46d
# ╟─86beb017-587c-420f-97a3-c42b0849f7d8
# ╠═2f6953bb-d6b8-402f-862f-f6bd2714e08d
# ╠═6de996a9-6d9b-412d-89da-c58ab958c8a3
# ╠═b1c24da9-95b7-4541-94ee-406e91160c28
# ╠═1f98af1f-14da-47c0-9700-3785cb45095e
# ╠═ad6dc95f-28a2-4710-8fb2-93eb6e811124
# ╠═bb3c0e65-db64-4ba9-9124-d29d1da233df
# ╠═8a69364d-bd8a-4670-a16a-3cbf68b892e3
# ╠═9bfa7f28-c725-4e44-b6fb-1a05895c70bf
# ╠═35d7d2d1-091d-4619-8613-9e6f18fb9dc9
# ╠═98a37fe5-5dfb-48f1-b3fe-c4722d9e43a7
# ╠═c501c712-ec84-4cd5-8382-6892541d49e3
# ╠═0447c27e-437a-4e30-979e-974cb22d1ce4
# ╠═db22a71d-71a1-4d22-98ee-436f17efbe37
# ╠═4dcb581b-75e3-4fe2-ae1e-1640d13a9939
# ╠═b3833be7-1f78-4528-ab0f-34f8399d3c14
# ╠═0b64c137-ff31-44d1-a22c-fc6c01d93585
# ╟─6f8380c3-a8d5-4c8f-9657-7ef743807f01
# ╠═d809438c-06e9-436c-a828-e75e07a77881
# ╠═bb984da3-bc48-4f0f-96d7-f096d0404d67
# ╠═d15b94b3-450e-4bcd-80c7-46fca35c6938
# ╠═f8d6786d-a303-4db6-93c0-3f0189affb06
# ╠═b814ff1a-aabc-4620-bad5-0b0e4ac86eae
# ╠═83f5f1ec-212e-47c2-89b6-89fdb066e918
# ╟─f43c28ef-b57c-4fa4-a237-c10ed1d761d5
# ╠═e5d4e324-46c6-4b6e-83c5-92f0f7fc595d
# ╠═bcab10ce-5d86-48b6-9679-c66066e2c819
# ╠═26f1025a-e581-4f6f-95cf-984d6e0651db
# ╠═c923c6bf-e0d6-4db3-a2e5-a804e7d74de6
# ╠═aaf4b986-fdf9-48a2-b257-799f6631de42
# ╠═090cc7df-6707-49c3-903e-abdbbe688c24
