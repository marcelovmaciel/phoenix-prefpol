#!/usr/bin/env julia

# Objective:
# Diagnose whether thermometer ordinality (0..10) is preserved in the Julia -> R
# roundtrip used by the current RCall/mice imputation workflow.
#
# Success criteria:
# 1) Path A (ordered categorical thermometer) must reach R with is.ordered == TRUE
#    and levels exactly "0","1",...,"10".
# 2) If Path A reaches R as factor but is.ordered == FALSE, ordinality is lost.
# 3) If levels are incomplete, report that the scale is being inferred from sample.

using DataFrames
using CategoricalArrays
using RCall
import PrefPol

const THERMO_COL = "thermo"
const EXPECTED_LEVELS = string.(0:10)

function build_base_df()
    thermo = Union{Missing, Int}[0, 1, 2, 3, 4, 5, 6, 7, 8, 10, missing, missing, 2, 4]
    binario_raw = Union{Missing, String}[
        "sim", "nao", missing, "sim", "nao", "sim", missing, "nao",
        "sim", "nao", "sim", missing, "nao", "sim",
    ]
    continuo = Union{Missing, Float64}[0.1, missing, 2.3, 4.4, missing, 6.0, 1.7, missing, 3.2, 5.1, 2.9, missing, 1.0, 7.4]
    constante = Union{Missing, Int}[42, 42, 42, 42, missing, 42, 42, 42, 42, 42, 42, missing, 42, 42]

    return DataFrame(
        thermo = thermo,
        binario = categorical(binario_raw; ordered = false),
        continuo = continuo,
        constante = constante,
    )
end

function build_paths(df::DataFrame)
    path_a = deepcopy(df)
    path_a.thermo = categorical(path_a.thermo; ordered = true)

    path_b = deepcopy(df)
    path_b.thermo = convert(Vector{Union{Missing, Int}}, path_b.thermo)

    return path_a, path_b
end

function probe_in_r(df::DataFrame; label::String, thermo_col::String = THERMO_COL)
    R"""
    suppressPackageStartupMessages(library(mice))

    df <- as.data.frame($df)
    v <- as.character($thermo_col)
    init <- mice(df, maxit = 0, print = FALSE)
    pred <- make.predictorMatrix(df)
    diag(pred) <- 0

    report <- capture.output({
      cat("=== ", $label, " ===\n", sep = "")
      cat("class(df[[v]]):\n"); print(class(df[[v]]))
      cat("typeof(df[[v]]):\n"); print(typeof(df[[v]]))
      cat("is.factor(df[[v]]):\n"); print(is.factor(df[[v]]))
      cat("is.ordered(df[[v]]):\n"); print(is.ordered(df[[v]]))
      cat("levels(df[[v]]):\n"); print(levels(df[[v]]))
      cat("str(df[[v]]):\n"); str(df[[v]])
      cat("summary(df[[v]]):\n"); print(summary(df[[v]]))
      cat("table(df[[v]], useNA='ifany'):\n"); print(table(df[[v]], useNA = "ifany"))
      cat("init$method:\n"); print(init$method)
      cat("predictorMatrix row for ", v, ":\n", sep = ""); print(pred[v, , drop = FALSE])
      cat("predictorMatrix column for ", v, ":\n", sep = ""); print(pred[, v, drop = FALSE])
      cat("predictorMatrix diag[", v, ",", v, "]: ", pred[v, v], "\n", sep = "")
    })

    class_out <- class(df[[v]])
    typeof_out <- typeof(df[[v]])
    is_factor_out <- is.factor(df[[v]])
    is_ordered_out <- is.ordered(df[[v]])
    levels_out <- if (is.factor(df[[v]])) levels(df[[v]]) else character(0)
    thermo_method_out <- unname(init$method[[v]])
    pred_row_out <- as.numeric(pred[v, ])
    pred_col_out <- as.numeric(pred[, v])
    pred_diag_out <- as.integer(pred[v, v])
    """

    report_lines = rcopy(Vector{String}, R"report")
    class_out = rcopy(Vector{String}, R"class_out")
    typeof_out = rcopy(String, R"typeof_out")
    is_factor_out = rcopy(Bool, R"is_factor_out")
    is_ordered_out = rcopy(Bool, R"is_ordered_out")
    levels_out = rcopy(Vector{String}, R"levels_out")
    thermo_method_out = rcopy(String, R"thermo_method_out")
    pred_row_out = rcopy(Vector{Float64}, R"pred_row_out")
    pred_col_out = rcopy(Vector{Float64}, R"pred_col_out")
    pred_diag_out = rcopy(Int, R"pred_diag_out")

    return (
        label = label,
        report = join(report_lines, "\n"),
        class_out = class_out,
        typeof_out = typeof_out,
        is_factor = is_factor_out,
        is_ordered = is_ordered_out,
        levels = levels_out,
        thermo_method = thermo_method_out,
        pred_row_nonzero = any(!iszero, pred_row_out),
        pred_col_nonzero = any(!iszero, pred_col_out),
        pred_diag = pred_diag_out,
    )
end

levels_complete(levels::Vector{String}) = levels == EXPECTED_LEVELS

function print_result_summary(result)
    full_levels = levels_complete(result.levels)
    println("Resultado $(result.label): ordered? $(result.is_ordered) | levels completos? $(full_levels) | metodo init[thermo]=\"$(result.thermo_method)\"")
    println(
        "Resultado $(result.label): predictorMatrix row_nonzero=$(result.pred_row_nonzero) | col_nonzero=$(result.pred_col_nonzero) | diag=$(result.pred_diag)",
    )
    if !full_levels
        missing_levels = setdiff(EXPECTED_LEVELS, result.levels)
        println(
            "Resultado $(result.label): levels recebidos = $(result.levels). Faltantes = $(missing_levels). Escala esta sendo inferida pelo sample.",
        )
    end
end

function final_diagnosis(result_a)
    julia_levels_a = string.(levels(result_a.julia_thermo))
    a_levels_ok = levels_complete(result_a.levels)
    julia_levels_ok = julia_levels_a == EXPECTED_LEVELS
    if result_a.is_ordered && a_levels_ok && julia_levels_ok
        return "ordinalidade preservada", "no roundtrip Julia->R do caminho A (ordered factor com levels 0..10)."
    end
    if !julia_levels_ok
        return "ordinalidade perdida", "antes do envio ao R no caminho A: Julia criou levels incompletos para thermo (escala inferida pelo sample)."
    end
    if result_a.is_factor && !result_a.is_ordered
        return "ordinalidade perdida", "no envio Julia->R do caminho A: thermo chegou como factor nominal (is.ordered=FALSE)."
    end
    if result_a.is_factor && !a_levels_ok
        return "ordinalidade perdida", "na preservacao de levels no caminho A: escala 0..10 nao chegou completa (inferida pelo sample)."
    end
    return "ordinalidade perdida", "no envio Julia->R do caminho A: thermo nao chegou como factor ordenado."
end

function main()
    println("PrefPol carregado: ", isdefined(Main, :PrefPol))

    base_df = build_base_df()
    df_a, df_b = build_paths(base_df)

    println("\nDados de teste: n=$(nrow(base_df)) linhas")
    println("Nivel 9 ausente no sample? ", 9 ∉ collect(skipmissing(base_df.thermo)))
    println("Levels Julia caminho A (thermo): ", levels(df_a.thermo), " | ordered=", isordered(df_a.thermo))
    println("Tipo Julia caminho B (thermo): ", eltype(df_b.thermo))

    result_a_r = probe_in_r(df_a; label = "A (thermo categorical ordered)")
    result_b = probe_in_r(df_b; label = "B (thermo Int/Union{Missing,Int})")
    result_a = merge(result_a_r, (julia_thermo = df_a.thermo,))

    println("\n----- EVIDENCIA R (A) -----")
    println(result_a.report)
    println("\n----- EVIDENCIA R (B) -----")
    println(result_b.report)

    println("\n----- RELATORIO CURTO -----")
    print_result_summary(result_a)
    print_result_summary(result_b)

    diagnosis, point = final_diagnosis(result_a)
    println("Diagnostico final: ", diagnosis, " - ", point)
end

try
    main()
catch err
    println("Falha no teste de ordinalidade: ", sprint(showerror, err))
    rethrow(err)
end
