# Document Title


Yes. Do **not** refactor. Do a fresh, isolated paper run using the existing `composable_running` path. The document says the current paper path is `PrefPol/composable_running/run_all_paper.jl` with `PrefPol/config/orchestration.toml`, and that `B`, `R`, `K` are controlled in `[run]`, not by CLI flags. 

## 1. Make a new test config

From repo root:

```bash
cp PrefPol/config/orchestration.toml PrefPol/config/orchestration_b10_r2_k2.toml
```

Edit `PrefPol/config/orchestration_b10_r2_k2.toml`.

In `[run]`, set:

```toml
B = 10
R = 2
K = 2

output_root = "PrefPol/composable_running/output/paper_b10_r2_k2"
cache_root = "PrefPol/composable_running/output/paper_b10_r2_k2/cache"
```

Do **not** use the old default output/cache root for this test. A fresh root gives you a clean cache without destroying the current working artifacts.

## 2. Validate configs

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/stages/00_validate_configs.jl \
  --config PrefPol/config/orchestration_b10_r2_k2.toml
```

If this fails, stop. Fix config only.

## 3. Run the complete paper pipeline

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/run_all_paper.jl \
  --config PrefPol/config/orchestration_b10_r2_k2.toml
```

Because the output/cache roots are new, this is already a brand-new complete run. You do not need `--force` unless rerunning the same `paper_b10_r2_k2` directory after a failed or partial attempt.

If you do rerun and want to overwrite the same cache:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/run_all_paper.jl \
  --config PrefPol/config/orchestration_b10_r2_k2.toml \
  --force
```

## 4. Check that the run really used B=10, R=2, K=2

```bash
julia +1.11.9 --project=PrefPol -e '
using CSV, DataFrames
run = CSV.read("PrefPol/composable_running/output/paper_b10_r2_k2/manifests/run_manifest.csv", DataFrame)
println(unique(select(run, :wave_id, :scenario_name, :m, :imputer_backend, :linearizer_policy, :B, :R, :K)))
'
```

You want to see only:

```text
B = 10
R = 2
K = 2
```

Also check that the expected paper artifacts exist:

```bash
test -s PrefPol/composable_running/output/paper_b10_r2_k2/paper_artifacts/2006_global_main.png
test -s PrefPol/composable_running/output/paper_b10_r2_k2/paper_artifacts/2018_global_main.png
test -s PrefPol/composable_running/output/paper_b10_r2_k2/paper_artifacts/2022_global_main.png

test -s PrefPol/composable_running/output/paper_b10_r2_k2/paper_artifacts/2006_group.png
test -s PrefPol/composable_running/output/paper_b10_r2_k2/paper_artifacts/2018_group.png
test -s PrefPol/composable_running/output/paper_b10_r2_k2/paper_artifacts/2022_group.png

test -s PrefPol/composable_running/output/paper_b10_r2_k2/paper_artifacts/effective_rankings.tex
test -s PrefPol/composable_running/output/paper_b10_r2_k2/paper_artifacts/effective_rankings_evolution_1x2.png
test -s PrefPol/composable_running/output/paper_b10_r2_k2/paper_artifacts/variance_decomposition_2022.png
test -s PrefPol/composable_running/output/paper_b10_r2_k2/paper_artifacts/appendix_lambda_grouping_tables.tex
```

## 5. If B=10 is acceptable, make the final config

```bash
cp PrefPol/config/orchestration.toml PrefPol/config/orchestration_final_b30_r30_k30.toml
```

Edit `[run]`:

```toml
B = 30
R = 30
K = 30

output_root = "PrefPol/composable_running/output/paper_final_b30_r30_k30"
cache_root = "PrefPol/composable_running/output/paper_final_b30_r30_k30/cache"
```

Then run:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/stages/00_validate_configs.jl \
  --config PrefPol/config/orchestration_final_b30_r30_k30.toml
```

Then:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/run_all_paper.jl \
  --config PrefPol/config/orchestration_final_b30_r30_k30.toml
```

If you restart the same final run after partial failure:

```bash
julia +1.11.9 --project=PrefPol \
  PrefPol/composable_running/run_all_paper.jl \
  --config PrefPol/config/orchestration_final_b30_r30_k30.toml \
  --force
```

## 6. Update manuscript images only after inspecting

The current workflow collects final artifacts into:

```text
PrefPol/composable_running/output/<run_name>/paper_artifacts/
```

Your manuscript uses files under:

```text
writing/imgs/
```

After you inspect the new PNGs/Tex files, copy them manually:

```bash
cp PrefPol/composable_running/output/paper_final_b30_r30_k30/paper_artifacts/2006_global_main.png writing/imgs/
cp PrefPol/composable_running/output/paper_final_b30_r30_k30/paper_artifacts/2018_global_main.png writing/imgs/
cp PrefPol/composable_running/output/paper_final_b30_r30_k30/paper_artifacts/2022_global_main.png writing/imgs/

cp PrefPol/composable_running/output/paper_final_b30_r30_k30/paper_artifacts/2006_group.png writing/imgs/
cp PrefPol/composable_running/output/paper_final_b30_r30_k30/paper_artifacts/2018_group.png writing/imgs/
cp PrefPol/composable_running/output/paper_final_b30_r30_k30/paper_artifacts/2022_group.png writing/imgs/

cp PrefPol/composable_running/output/paper_final_b30_r30_k30/paper_artifacts/effective_rankings.tex writing/imgs/
cp PrefPol/composable_running/output/paper_final_b30_r30_k30/paper_artifacts/effective_rankings_evolution_1x2.png writing/imgs/
cp PrefPol/composable_running/output/paper_final_b30_r30_k30/paper_artifacts/variance_decomposition_2022.png writing/imgs/
cp PrefPol/composable_running/output/paper_final_b30_r30_k30/paper_artifacts/appendix_lambda_grouping_tables.tex writing/imgs/
```

## 7. Surgical summary

Change only this:

```text
PrefPol/config/orchestration_b10_r2_k2.toml
PrefPol/config/orchestration_final_b30_r30_k30.toml
```

Run only this:

```bash
julia +1.11.9 --project=PrefPol PrefPol/composable_running/stages/00_validate_configs.jl --config <CONFIG>
julia +1.11.9 --project=PrefPol PrefPol/composable_running/run_all_paper.jl --config <CONFIG>
```

Do not touch:

```text
PrefPol/running
PrefPol/exploratory
single_peakedness configs/scripts
majority_graph_support scripts
PrefPol/src/nested_pipeline.jl
any refactor
```

One warning: `B=R=K=30` means (30 \times 30 \times 30 = 27{,}000) linearized profiles per spec. Given multiple years, (m)-values, imputers, and linearizers, this can become very large. Your test with `B=10, R=2, K=2` is the right first move.

