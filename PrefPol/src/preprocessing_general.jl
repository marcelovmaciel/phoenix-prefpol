
# === Project and Data Setup ===

"""
    load_spss_file(path::String)

Read an SPSS `.sav` file using R's `haven` package and return a `DataFrame`.
"""
function load_spss_file(path::String)
    rcall = _require_rcall!()
    _rcall_eval(rcall, "library(haven)")
    _rcall_setglobal!(rcall, :path, path)
    return _rcall_copy(rcall, DataFrame, _rcall_eval(rcall, "haven::read_sav(path)"))
end


# === Score Processing ===
"""
    build_candidate_score_distributions(df, candidatos)

Create a countmap of scores for each candidate column in `df`.
"""
function build_candidate_score_distributions(df::DataFrame, candidatos::Vector{String})
    return Dict(c => countmap(df[!, c]) for c in candidatos)
end


"""
    convert_keys_to_int(dict)

Return a new dictionary with keys converted to integers. Float keys that
are approximately integers are rounded; others throw an error.
"""
function convert_keys_to_int(dict)
    return Dict(
        if k isa Integer
            k
        elseif k isa AbstractFloat
            r = round(Int, k)
            if !isapprox(k, r; atol=1e-8)
                throw(ArgumentError("Expected near-integer key, got $k"))
            end
            r
        else
            throw(ArgumentError("Unsupported key type: $(typeof(k))"))
        end => v
        for (k, v) in dict
    )
end


"""
    sanitize_countmaps(countmaps)

Convert the keys of each countmap to integers.
"""
function sanitize_countmaps(countmaps::Dict{String,<:AbstractDict})
    return Dict(c => convert_keys_to_int(cm) for (c, cm) in countmaps)
end


"""
    compute_dont_know_her(countmaps, nrespondents)

Compute the percentage of "don't know" codes (96–99) for each candidate and
return a sorted vector of `(name, percent)` pairs.
"""
function compute_dont_know_her(countmaps::Dict{String,Dict{Int,Int}}, nrespondents::Int)
    return sort([
        (k, 100 * sum((count for (score, count) in v if is_eseb_missing_score(score)); init = 0) / nrespondents)
        for (k, v) in countmaps
    ], by = x -> x[2])
end


function _normalize_weight_vector(weights)
    out = zeros(Float64, length(weights))
    have_valid = false

    for i in eachindex(weights)
        w = weights[i]
        if ismissing(w)
            continue
        elseif !(w isa Real)
            throw(ArgumentError("Weight at row $i is not numeric: $(typeof(w))."))
        end

        wf = Float64(w)
        if !isfinite(wf) || wf < 0
            continue
        end
        out[i] = wf
        have_valid = true
    end

    have_valid || throw(ArgumentError("No valid non-negative weights found for weighted candidate-set computation."))
    return out
end

function _select_top_candidates_from_poplist(poplist::Vector{String};
                                             m::Int,
                                             force_include::Vector{String}=String[])
    inc = unique(force_include)

    if length(inc) > m
        inc = inc[1:m]
    end

    filtered = [name for name in poplist if name ∉ inc]
    needed   = m - length(inc)
    extra    = needed > 0 ? filtered[1:min(needed, length(filtered))] : String[]
    selected = vcat(inc, extra)

    if length(selected) < m
        @warn "Only $(length(selected)) unique candidates available; requested $m."
    end

    return selected
end

"""
    compute_weighted_dont_know_her(scores_df, candidate_cols; weights)

Compute the weighted percentage of missing or "don't know" scores for each
candidate in the Brazil/ESEB score table.

PrefPol treats Julia `missing` and survey sentinel codes `96`-`99` as missing
candidate knowledge. The returned `(name, percent)` pairs are sorted by ascending
weighted missingness and then candidate name, making them reproducible inputs to
candidate-set selection. This reads only the provided table and weights; it does
not touch pipeline cache.
"""
function compute_weighted_dont_know_her(scores_df::DataFrame,
                                        candidate_cols;
                                        weights)
    w = _normalize_weight_vector(weights)
    total_weight = sum(w)
    total_weight > 0 || throw(ArgumentError("Total valid weight is zero; cannot rank candidates by weighted missingness."))

    return sort([
        begin
            miss_weight = 0.0
            col = scores_df[!, cand]
            for i in eachindex(col)
                is_eseb_missing_score(col[i]) || continue
                miss_weight += w[i]
            end
            (String(cand), 100 * miss_weight / total_weight)
        end
        for cand in candidate_cols
    ], by = x -> (x[2], x[1]))
end



"""
    prepare_scores_for_imputation_int(df, score_cols; extra_cols=String[])

Prepare Brazil/ESEB score columns for deterministic zero-style imputation.

Candidate score columns are normalized with PrefPol's centralized ESEB score
semantics and converted to `Union{Missing,Int}`. Optional demographic or
auxiliary columns are appended unchanged. This helper is used before the nested
pipeline's imputation stage and does not write cache.
"""
@inline function _can_normalize_eseb_score_column(col)
    return all(x -> ismissing(x) || x isa Real || x isa AbstractString || x isa CategoricalValue, col)
end

@inline function _normalize_eseb_score_to_int(value, col)
    normalized = normalize_eseb_score(value)
    normalized === missing && return missing

    rounded = round(Int, normalized)
    isapprox(normalized, rounded; atol = 1e-8) || throw(ArgumentError(
        "Score column `$col` contains non-integer ESEB score $(repr(value)).",
    ))
    return rounded
end

function prepare_scores_for_imputation_int(df::DataFrame,
    score_cols::Vector{String};
    extra_cols::Vector{String}=String[])
    normalizable_cols = Base.filter(c -> _can_normalize_eseb_score_column(df[!, c]), score_cols)
    nonnormalizable = setdiff(score_cols, normalizable_cols)
    if !isempty(nonnormalizable)
        @warn "prepare_scores_for_imputation_int: skipping non-normalizable columns $(nonnormalizable)"
    end

    scores_int = DataFrame()
    for col in normalizable_cols
        scores_int[!, col] = Union{Missing,Int}[
            _normalize_eseb_score_to_int(x, col) for x in df[!, col]
        ]
    end

    return isempty(extra_cols) ? scores_int : hcat(scores_int, df[:, extra_cols])
end


"""
    prepare_scores_for_imputation_categorical(df, score_cols; extra_cols=String[])

Prepare Brazil/ESEB score columns for stochastic or R/mice imputation.

The candidate score columns are first normalized by
`prepare_scores_for_imputation_int` and then converted to ordered categorical
columns, matching the imputation backends used by PrefPol. Optional demographic
or auxiliary columns are appended unchanged.
"""
function prepare_scores_for_imputation_categorical(df::DataFrame,
                                                   score_cols::Vector{String};
                                                   extra_cols::Vector{String}=String[])
    declared = prepare_scores_for_imputation_int(df, score_cols; extra_cols = String[])
    declared_cat = mapcols(col -> categorical(col, ordered = true), declared)
    return isempty(extra_cols) ? declared_cat : hcat(declared_cat, df[:, extra_cols])
end


"""
    compute_global_candidate_set(scores_df; candidate_cols, m, force_include=String[], weights)

Low-level weighted engine for candidate-set ordering.

This computes the globally ordered candidate set of size `m` using weighted
missing rates from the raw score table. Forced candidates are pinned first in the
order they were declared, and the remaining candidates are filled by ascending
weighted missingness. Publication specs and raw-profile helpers should call
`resolve_active_candidate_set`, which supplies the configured candidate universe,
scenario forcing, survey weight column, and requested candidate count.
"""
function compute_global_candidate_set(scores_df::DataFrame;
                                      candidate_cols,
                                      m::Int,
                                      force_include::Vector{String} = String[],
                                      weights)
    ordering = compute_weighted_dont_know_her(scores_df, candidate_cols; weights = weights)
    poplist = [name for (name, _) in ordering]
    return _select_top_candidates_from_poplist(poplist; m = m, force_include = force_include)
end



# === Reproducibility seed helpers ===

const _R_VALID_SEED_MODULUS = Int128(typemax(Int32) - 1)

"""
    _normalize_r_seed(seed) -> Int

Map any Julia integer seed into R's positive `set.seed` range. This is
intentionally separate from nested-pipeline stage hashing: R/mice needs a
32-bit seed accepted by R, while pipeline stages use SHA-derived `UInt64`
seeds to namespace BRK cache branches.
"""
function _normalize_r_seed(seed::Integer)
    return Int(mod(seed, _R_VALID_SEED_MODULUS) + 1)
end

"""
    r_impute_mice_report(df; seed=nothing) -> NamedTuple

Run one R/mice imputation on a prepared score table and return both the completed
DataFrame and diagnostics.

The report includes the `mice` method map, logged events, and dropped predictors.
When a seed is supplied, PrefPol normalizes it to R's valid seed range so nested
pipeline imputation replicate `r` is reproducible within each bootstrap branch.
This function calls R through `RCall`; it does not write cache.
"""
function r_impute_mice_report(df::DataFrame; seed::Union{Nothing,Integer} = nothing)
    rcall = _require_rcall!()

    normalized_seed = seed === nothing ? rand(1:Int(_R_VALID_SEED_MODULUS)) :
                      _normalize_r_seed(seed)
    _rcall_setglobal!(rcall, :df, df)
    _rcall_eval(rcall, "set.seed($normalized_seed)")

    _rcall_eval(rcall, raw"""
    res <- local({
      df_local <- as.data.frame(df)

      # ---------- boilerplate ----------
      init <- mice::mice(df_local, maxit = 0, print = FALSE)
      meth <- init$method                   # default methods
      pred <- mice::make.predictorMatrix(df_local)
      diag(pred) <- 0                       # no self-prediction
      dropped <- character(0)

      # ---------- customise methods ----------
      for (v in names(df_local)) {
        col <- df_local[[v]]
        if (all(is.na(col)) || length(unique(stats::na.omit(col))) <= 1) {
          meth[v] <- ""                     # constant or all-missing
          dropped <- c(dropped, v)
        } else if (is.factor(col)) {
          n_cat <- nlevels(col)
          if (n_cat == 2) {
            meth[v] <- "logreg"             # binomial GLM
          } else {                          # 3+ categories
            meth[v] <- "cart"               # safe, no weight explosion
          }
        } else if (is.numeric(col)) {
          meth[v] <- "pmm"
        }
      }

      # ---------- one imputation ----------
      imp <- mice::mice(df_local,
                        m               = 1,
                        method          = meth,
                        predictorMatrix = pred,
                        printFlag       = FALSE)

      completed_df <- mice::complete(imp, 1)
      list(
        completed = completed_df,
        meth = meth,
        loggedEvents = imp$loggedEvents,
        dropped_predictors = dropped
      )
    })
    res
    """)

    completed = _rcall_copy(rcall, DataFrame, _rcall_eval(rcall, "res\$completed"))

    meth_names = _rcall_copy(rcall, Vector{String}, _rcall_eval(rcall, "names(res\$meth)"))
    meth_values = _rcall_copy(rcall, Vector{String}, _rcall_eval(rcall, "as.character(res\$meth)"))
    meth = Dict{String, String}(name => value for (name, value) in zip(meth_names, meth_values))

    loggedEvents = if _rcall_copy(rcall, Bool, _rcall_eval(rcall, "is.null(res\$loggedEvents)"))
        nothing
    else
        _rcall_copy(rcall, DataFrame, _rcall_eval(rcall, "as.data.frame(res\$loggedEvents)"))
    end

    dropped_predictors = _rcall_copy(
        rcall,
        Vector{String},
        _rcall_eval(rcall, "as.character(res\$dropped_predictors)"),
    )

    return (
        completed = completed,
        meth = meth,
        loggedEvents = loggedEvents,
        dropped_predictors = dropped_predictors,
    )
end

"""
    GLOBAL_R_IMPUTATION(df; m=1, seed=nothing) -> DataFrame

Backward-compatible callable wrapper around `r_impute_mice_report` that returns
only the completed DataFrame. The nested pipeline uses `r_impute_mice_report` so
R/mice diagnostics can be cached with `ImputedData` artifacts.
"""
const GLOBAL_R_IMPUTATION = let
    function f(df::DataFrame; m::Int = 1, seed::Union{Nothing,Integer} = nothing)
        if m != 1
            @warn "GLOBAL_R_IMPUTATION is fixed at m=1; received m=$m and will ignore it."
        end
        report = r_impute_mice_report(df; seed = seed)
        return report.completed
    end
    f
end

# === End of Slice Top ===
# === Imputation ===

const SUPPORTED_IMPUTATION_VARIANTS = (:zero, :random, :mice)
"""
    normalize_imputation_variants(variants) -> Tuple{Vararg{Symbol}}

Normalize and validate requested PrefPol imputation variants. Supported applied
variants are `:zero`, `:random`, and `:mice`; the formal profile semantics after
imputation are handled by `PreferenceProfiles` adapters downstream.
"""
function normalize_imputation_variants(variants)
    raw = if variants isa Symbol || variants isa AbstractString
        (variants,)
    else
        Tuple(variants)
    end
    isempty(raw) && throw(ArgumentError("provide at least one imputation variant"))

    variant_syms = Tuple(Symbol(var) for var in raw)
    invalid = [var for var in variant_syms if var ∉ SUPPORTED_IMPUTATION_VARIANTS]
    isempty(invalid) || throw(ArgumentError(
        "Unsupported imputation variant(s): $(join(string.(invalid), ", ")). " *
        "Supported variants: $(join(string.(SUPPORTED_IMPUTATION_VARIANTS), ", ")).",
    ))
    return variant_syms
end


"""
    imputation_variants(df, candidates, demographics;
                        most_known_candidates=String[],
                        variants=SUPPORTED_IMPUTATION_VARIANTS)

Return imputed Brazil/ESEB score tables for the requested imputation strategies.

The result is a named tuple keyed by `:zero`, `:random`, and/or `:mice`. Each
value is a completed score table containing the selected candidate columns plus
demographic columns. This legacy helper performs imputation in memory; the
nested pipeline wraps equivalent logic in `_impute_resample` and caches each
`(b, r)` imputation branch as an `ImputedData` artifact.
"""
function imputation_variants(df::DataFrame,
    candidates::Vector{String},
    demographics::Vector{String};
    most_known_candidates::Vector{String}=String[],
    variants=SUPPORTED_IMPUTATION_VARIANTS)

# 1 ─ Determine which score columns to use
use_cols = isempty(most_known_candidates) ? candidates : most_known_candidates
variant_syms = normalize_imputation_variants(variants)

# 2 ─ Subset to relevant columns (if top-candidates requested)
df_subset = isempty(most_known_candidates) ? df : get_df_just_top_candidates(df, use_cols; demographics = demographics)

# 3 ─ Prepare only the tables needed by the requested variants
need_int = :zero in variant_syms
need_cat = any(var in (:random, :mice) for var in variant_syms)

scores_int = need_int ? prepare_scores_for_imputation_int(df_subset, use_cols; extra_cols = demographics) : nothing
scores_cat = need_cat ? prepare_scores_for_imputation_categorical(df_subset, use_cols; extra_cols = demographics) : nothing

# 4 ─ Apply imputation variants
results = Pair{Symbol,DataFrame}[]

for var in variant_syms
    df_imp = if var === :zero
        Impute.replace(scores_int, values = 0)
    elseif var === :random
        Impute.impute(scores_cat, Impute.SRS(; rng = MersenneTwister()))
    elseif var === :mice
        GLOBAL_R_IMPUTATION(scores_cat)
    else
        error("Unhandled imputation variant $(var)")
    end
    push!(results, var => df_imp)
end

return (; results...)
end

# === End of Imputation ===


"""
    weighted_bootstrap(data, weights, B)

Draw `B` weighted bootstrap resamples of a raw applied survey table.

Each output DataFrame has `nrow(data)` rows sampled with replacement using the
provided respondent weights. This helper is kept for older workflows; the nested
pipeline records explicit sampled indices and multiplicities in `Resample` cache
artifacts.
"""
function weighted_bootstrap(data::DataFrame, weights::Vector{Float64}, B::Int)
    n = nrow(data)
    boot_samples = Vector{DataFrame}(undef, B)
    
    for b in 1:B
        idxs = sample(1:n, Weights(weights), n; replace=true)
        boot_samples[b] = data[idxs, :]
    end
    
    return boot_samples
end



"""
    get_row_candidate_score_pairs(row, score_cols)

Create a dictionary mapping candidate names to their scores in `row`.
"""
function get_row_candidate_score_pairs(row, score_cols)
    Dict(Symbol(c) => row[c] for c in score_cols)
end

@inline function _coerce_completed_score_value(value)
    raw = _unwrap_eseb_score(value)
    if !(ismissing(raw) || raw isa Real || raw isa AbstractString)
        throw(ArgumentError(
            "Score values must be numeric, missing, or categorical-wrapped numeric values; got $(typeof(value)).",
        ))
    end
    return normalize_eseb_score(raw)
end

@inline _score_sort_key(value) = ismissing(value) ? -Inf : value

function _normalized_score_dict(score_dict)
    return Dict(cand => _coerce_completed_score_value(score) for (cand, score) in score_dict)
end

"""
    get_order_dict(score_dict)

Convert score values to ranks where higher scores receive smaller rank numbers.
"""
function  get_order_dict(score_dict)
    normalized_scores = _normalized_score_dict(score_dict)
    unique_scores = sort(unique(values(normalized_scores)); by = _score_sort_key, rev = true)
    lookup = Dict(s => r for (r,s) in enumerate(unique_scores))
    Dict(k => lookup[v] for (k,v) in normalized_scores)
end

"""
    force_scores_become_linear_rankings(score_dict; rng=MersenneTwister())

Break ties in `score_dict` at random to obtain a linear ranking.
"""
function force_scores_become_linear_rankings(score_dict; rng=MersenneTwister())
    normalized_scores = _normalized_score_dict(score_dict)

    grouped = Dict(score => Symbol[] for score in unique(values(normalized_scores)))
    
    for (cand, score) in normalized_scores
        push!(grouped[score], cand)
    end

    sorted_scores = sort(collect(keys(grouped)); by = _score_sort_key, rev = true)
    linear_ranking = Dict{Symbol, Int}()
    next_rank = 1

    for score in sorted_scores
        cands = grouped[score]
        shuffle!(rng, cands)
        for cand in cands
            linear_ranking[cand] = next_rank
            next_rank += 1
        end
    end

    return linear_ranking
end

"""
    linearize_ranking_dict(ranking_dict; rng=MersenneTwister())

Break ties in a ranking dictionary where smaller rank numbers are better,
returning a strict linear ranking.
"""
function linearize_ranking_dict(ranking_dict::Dict{Symbol,<:Integer};
                                rng = MersenneTwister())
    grouped = Dict(rank => Symbol[] for rank in unique(values(ranking_dict)))

    for (cand, rank) in ranking_dict
        push!(grouped[rank], cand)
    end

    linear_ranking = Dict{Symbol,Int}()
    next_rank = 1

    for rank in sort(collect(keys(grouped)))
        tied = grouped[rank]
        shuffle!(rng, tied)
        for cand in tied
            linear_ranking[cand] = next_rank
            next_rank += 1
        end
    end

    return linear_ranking
end

"""
    linearize_profile_column!(df; col=:profile, rng=Random.GLOBAL_RNG)

Replace weak rankings in `df[col]` with strict linear rankings by breaking ties
within each rank bucket.
"""
function linearize_profile_column!(df::DataFrame;
                                   col::Symbol = :profile,
                                   rng = Random.GLOBAL_RNG)
    df[!, col] = map(profile -> linearize_ranking_dict(profile; rng = rng), df[!, col])
    return df
end




"""
    build_profile(df; score_cols, rng=Random.GLOBAL_RNG, kind=:linear)

Construct row-level ranking dictionaries from completed Brazil/ESEB score
columns.

`kind=:weak` preserves equal scores as weak-order ties; `kind=:linear` breaks
ties randomly with `rng`. This is an applied preprocessing helper that produces
rank dictionaries later converted to `PreferenceProfiles` profiles by the adapter
layer.
"""
function build_profile(df::DataFrame;
                       score_cols::Vector,
                       rng  = Random.GLOBAL_RNG,
                       kind::Symbol = :linear)   # :linear or :weak
    f = kind === :linear ? force_scores_become_linear_rankings : get_order_dict
    profiles = Vector{Dict{Symbol,Int}}(undef, nrow(df))

    for (idx, row) in enumerate(eachrow(df))
        score_dict = get_row_candidate_score_pairs(row, score_cols)
        profiles[idx] = kind === :linear ? f(score_dict; rng = rng) : f(score_dict)
    end

    return profiles
end


"""
    profile_dataframe(df; score_cols, demo_cols, rng=Random.GLOBAL_RNG, kind=:linear)

Return an applied profile DataFrame with a `:profile` column and selected
demographic columns.

The `:profile` values are ranking dictionaries from `build_profile`. Downstream
helpers attach candidate metadata and convert this table to a
`PreferenceProfiles.AnnotatedProfile`; formal profile invariants live in `PreferenceProfiles`.
"""
function profile_dataframe(df::DataFrame;
                           score_cols::Vector,
                           demo_cols::Vector,
                           rng  = Random.GLOBAL_RNG,
                           kind::Symbol = :linear)
    prof = build_profile(df; score_cols = score_cols, rng = rng, kind = kind)
    demos = df[:, demo_cols]
    return hcat(DataFrame(profile = prof), demos)
end



"""
    dict2svec(d; cs=cands, higher_is_better=false)

Encode a dictionary of candidate scores into a static vector permutation.
"""
@inline function dict2svec(d::Dict{Symbol,<:Integer}; cs::Vector{Symbol}=cands,
                           higher_is_better::Bool=false)
    # 1. pack the m scores into an isbits StaticVector
    m = length(cs)                                # number of candidates
    vals = SVector{m,Int}(map(c -> d[c], cs))

    # 2. permutation that sorts those scores
    perm = sortperm(vals; rev = higher_is_better)           # Vector{Int}

    # 3. return as SVector{m,UInt8} (10 B if m ≤ 10)
    return SVector{m,UInt8}(perm)
end




"""
    decode_rank(code, pool)

Return the ranking `SVector` for a pooled integer `code`. If `code` is
already an `SVector`, it is returned unchanged.
"""
decode_rank(code::Integer,      pool) = pool[code]  # original pool lookup behaviour
decode_rank(r::SVector, _) = r                      # no-op if already SVector


"""
    compress_rank_column!(df, cands; col=:profile)

Encode ranking dictionaries in column `col` into pooled static vectors for
memory efficiency. Returns the pool used for decoding.
"""
function compress_rank_column!(df::DataFrame, cands; col::Symbol=:profile)
    # 1. Dict → SVector
    
    sv = [dict2svec(r[col],cs = cands) for r in eachrow(df)]  # one tiny allocation per row

    # 2. pool identical SVectors (UInt16 index)
    pooled = PooledArray(sv; compress = true)

    # 3. overwrite in-place; let Dict objects be GC’d
    df[!, col] = pooled
    metadata!(df, "candidates", Symbol.(cands)) # new, test
    GC.gc()                       # reclaim Dict storage promptly
    return pooled.pool            # decoder lookup table
end



"""
    perm2dict(perm, cs)

Translate a permutation of candidate indices into a dictionary mapping
candidates to their ranks.
"""
@inline function perm2dict(perm::AbstractVector{<:Integer},
                           cs::Vector{Symbol})
    d = Dict{Symbol,Int}()
    @inbounds for (place, idx) in pairs(perm)          # place = 1,2,…
        d[cs[idx]] = place
    end
    return d
end


perm_to_dict = @inline perm2dict


"""
    decode_profile_column!(df)

Decode a compressed `:profile` column back into dictionaries using candidate
metadata stored in the DataFrame.
"""
function decode_profile_column!(df::DataFrame)
    eltype(df.profile) <: Dict && return df            # nothing to do

    cand_syms = metadata(df, "candidates")
    col       = df.profile
    decoded   = Vector{Dict{Symbol,Int}}(undef, length(col))

    if col isa PooledArray
        pool = col.pool
        for j in eachindex(col)
            perm = decode_rank(col[j], pool)
            decoded[j] = perm_to_dict(perm, cand_syms)
        end
    else                                               # plain Vector{SVector}
        for j in eachindex(col)
            decoded[j] = perm_to_dict(col[j], cand_syms)
        end
    end

    df[!, :profile] = decoded
    return df
end


"""
    decode_each!(var_map)

Decode the profile column of each DataFrame stored in `var_map`.
"""
@inline function decode_each!(var_map)
    for vec in values(var_map)          # vec::Vector{DataFrame}
        decode_profile_column!(vec[1])  # length == 1 in streaming path
    end
end
