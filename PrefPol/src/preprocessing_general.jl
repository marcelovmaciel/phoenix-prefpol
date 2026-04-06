
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
        (k, 100 * sum(get(v, code, 0) for code in (96, 97, 98,99)) / nrespondents)
        for (k, v) in countmaps
    ], by = x -> x[2])
end

@inline function _is_missing_score_value(v)
    ismissing(v) && return true
    v isa Integer && return v in (96, 97, 98, 99)
    if v isa AbstractFloat
        isfinite(v) || return false
        r = round(Int, v)
        return isapprox(v, r; atol = 1e-8) && (r in (96, 97, 98, 99))
    end
    return false
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
        @warn "force_include has more than $m names; truncating to first $m."
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

Compute the weighted percentage of missing / "don't know" scores (96–99 or
`missing`) for each candidate and return a deterministic ascending ordering of
`(name, percent)` pairs.
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
                _is_missing_score_value(col[i]) || continue
                miss_weight += w[i]
            end
            (String(cand), 100 * miss_weight / total_weight)
        end
        for cand in candidate_cols
    ], by = x -> (x[2], x[1]))
end



"""
    prepare_scores_for_imputation_int(df, score_cols; extra_cols=String[])

Convert numeric score columns to `Int` and mark special codes (96–99) as
missing. Optional `extra_cols` are appended unchanged.
"""
function prepare_scores_for_imputation_int(df::DataFrame,
    score_cols::Vector{String};
    extra_cols::Vector{String}=String[])
    # (1) Split truly numeric from anything that isn't
    numeric_cols   = Base.filter(c -> eltype(df[!, c]) <: Union{Missing, Real}, score_cols)
    nonnumeric     = setdiff(score_cols, numeric_cols)
    if !isempty(nonnumeric)
        @warn "prepare_scores_for_imputation_int: skipping non‑numeric columns $(nonnumeric)"
    end

    # (2) Work only on the numeric score columns
    scores_int = mapcols(col -> Int.(col), df[:, numeric_cols])
    declared   = Impute.declaremissings(scores_int; values = (96, 97, 98, 99))

    # (3) Append any extra (demographic) columns, untouched
    return isempty(extra_cols) ? declared : hcat(declared, df[:, extra_cols])
end


"""
    prepare_scores_for_imputation_categorical(df, score_cols; extra_cols = String[])

Same idea as the `_int` version but returns the scores as **ordered categoricals**.
Demographics are still appended unchanged.
"""
function prepare_scores_for_imputation_categorical(df::DataFrame,
                                                   score_cols::Vector{String};
                                                   extra_cols::Vector{String}=String[])
    declared = prepare_scores_for_imputation_int(df, score_cols; extra_cols = String[])
    declared_cat = mapcols(col -> categorical(col, ordered = true), declared)
    return isempty(extra_cols) ? declared_cat : hcat(declared_cat, df[:, extra_cols])
end


# === Slice Top  ===
"""
    get_most_known_candidates(dont_know_her, how_many)

Return the names of the `how_many` candidates with the lowest "don't know"
percentages from a precomputed list.
"""
function get_most_known_candidates(dont_know_her::Vector{Tuple{String, Float64}}, how_many)
    return first.(Iterators.take(dont_know_her, how_many))
end


"""
    select_top_candidates(countmaps, nrespondents; m, force_include=String[])

Select up to `m` candidates with the lowest "don't know" rates. Additional
names in `force_include` are guaranteed to appear in the result.
"""
function select_top_candidates(countmaps::Dict{String,<:AbstractDict},
                               nrespondents::Int;
                               m::Int,
                               force_include::Vector{String}=String[])
    poplist = [name for (name, _) in compute_dont_know_her(countmaps, nrespondents)]
    return _select_top_candidates_from_poplist(poplist; m = m, force_include = force_include)
end


"""
    compute_candidate_set(scores_df; candidate_cols, m, force_include=String[])

Determine the set of `m` candidates to analyze based on score distributions.
"""
function compute_candidate_set(scores_df::DataFrame;
                               candidate_cols,
                               m::Int,
                               force_include::Vector{String} = String[])

    countmaps     = build_candidate_score_distributions(scores_df, candidate_cols)
    countmaps2    = sanitize_countmaps(countmaps)
    nrespondents  = nrow(scores_df)

    return select_top_candidates(countmaps2, nrespondents;
                                 m = m, force_include = force_include)
end

"""
    compute_global_candidate_set(scores_df; candidate_cols, m, force_include=String[], weights)

Determine the globally ordered candidate set of size `m` using weighted missing
rates from the raw score table. Forced candidates are pinned first in the order
they were declared, and the remaining candidates are filled by ascending
weighted missingness.
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


dont_know_her = Tuple{String,Float64}[]



"""
    get_df_just_top_candidates(df, how_top; demographics=String[])
    get_df_just_top_candidates(df, which_ones; demographics=String[])

Return `df` restricted to selected candidate columns plus optional
`demographics`. `how_top` chooses the top `how_top` candidates from the
global `dont_know_her` list, while `which_ones` specifies candidate names
directly.
"""
function get_df_just_top_candidates(df::DataFrame, how_top::Int; demographics = String[] )
    most_known_candidates = get_most_known_candidates(dont_know_her, how_top)
    return df[!, vcat(most_known_candidates, demographics)]
end

function get_df_just_top_candidates(df::DataFrame, which_ones; demographics = String[])
    return df[!, vcat(which_ones, demographics)]
end


"""
    GLOBAL_R_IMPUTATION(df; m=1)

Impute missing values using R's `mice` package and return a completed
DataFrame with a single imputed dataset (`m = 1`). A random seed is set
for reproducibility across bootstraps. The `m` keyword is accepted for
backward compatibility but any value different from `1` is ignored.
"""
function r_impute_mice_report(df::DataFrame; seed::Union{Nothing,Integer} = nothing)
    rcall = _require_rcall!()

    seed = seed === nothing ? rand(1:10^6) : Int(seed)
    _rcall_setglobal!(rcall, :df, df)
    _rcall_eval(rcall, "set.seed($seed)")

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
const DEFAULT_PIPELINE_IMPUTATION_VARIANTS = (:zero, :mice)

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

Return a named tuple of imputed score tables for the requested strategies.
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

Draw `B` bootstrap resamples from `data` using the provided `weights`.
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

"""
    get_order_dict(score_dict)

Convert score values to ranks where higher scores receive smaller rank numbers.
"""
function  get_order_dict(score_dict)
    unique_scores = sort(unique(values(score_dict)); rev = true)
    lookup = Dict(s => r for (r,s) in enumerate(unique_scores))
    Dict(k => lookup[v] for (k,v) in score_dict)
end

"""
    force_scores_become_linear_rankings(score_dict; rng=MersenneTwister())

Break ties in `score_dict` at random to obtain a linear ranking.
"""
function force_scores_become_linear_rankings(score_dict; rng=MersenneTwister())

    grouped = Dict(score => Symbol[] for score in unique(values(score_dict)))
    
    for (cand, score) in score_dict
        push!(grouped[score], cand)
    end

    sorted_scores = sort(collect(keys(grouped)), rev=true)
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

Construct ranking dictionaries for each row of `df`. `kind` may be
`:linear` to break ties randomly or `:weak` to keep weak orderings.
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

Return a DataFrame with a `:profile` column of rankings and the requested
demographic columns.
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
