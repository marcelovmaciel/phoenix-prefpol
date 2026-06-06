# Variance Decomposition

```@meta
CurrentModule = PrefPol
```

PrefPol's variance decomposition is a descriptive partition of variability in
the realized nested pipeline tree. It is not a new estimator of the population
variance of political polarization, and it does not change the underlying
measure definitions.

For scalar leaf outputs `M[b, r, k]`, the tree is:

- bootstrap branch `b`,
- imputation branch `r` within `b`, and
- linearization branch `k` within `(b, r)`.

`tree_variance_decomposition_table` computes:

```text
bootstrap_variance     = Var_b(E[M | b])
imputation_variance    = E_b(Var_r(E[M | b, r]))
linearization_variance = E_b(E_r(Var_k(M | b, r)))
```

The total variance is the population variance across realized leaves with
equal leaf weight. The decomposition assumes a rectangular realized tree:
the same number of imputation branches within each bootstrap branch and the
same number of linearization branches within each `(b, r)` branch.

## Reporting

`VarianceDecompositionReportSpec` filters and formats decomposition tables.
By default, reports preserve candidate-set size `m` and survey/scenario
selection because changing `m` changes the ranking space, sparsity regime, and
scale of several measures. Pooling over `m` or selections is a secondary
descriptive reporting choice.

The default paper measure order is `Psi`, `R`, `HHI`, `RHHI`, `C`, and `D`.
Diagnostic measures can be requested explicitly when present in the input
table, but they are not part of the publication default set.

## API

```@docs; canonical=false
tree_variance_decomposition_table
VarianceComponentSummary
VarianceDecomposition
VarianceDecompositionReportSpec
DEFAULT_PAPER_VARIANCE_MEASURES
DEFAULT_PAPER_VARIANCE_MEASURE_LABELS
normalize_variance_measure
variance_decomposition_report
variance_decomposition_fine_table
variance_decomposition_pooled_table
variance_decomposition_by_m_plot_table
variance_decomposition_year_scenario_boxplot_table
```
