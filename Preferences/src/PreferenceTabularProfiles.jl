# PreferenceTabularProfiles.jl

using DataFrames
using OrderedCollections: OrderedDict
using Printf

# Generic diagnostics for profile construction from tabular score data.
# PrefPol reuses this metadata, but it is intentionally package-agnostic.
const _PROFILE_BUILD_META = IdDict{Any,NamedTuple}()

"""
    profile_build_meta(profile)

Return diagnostics metadata stored when `profile` was created by
`build_profile_from_scores`. Returns `nothing` when unavailable.
"""
@inline profile_build_meta(profile) = get(_PROFILE_BUILD_META, profile, nothing)

function _store_profile_build_meta!(profile;
                                    weighted::Bool,
                                    allow_ties::Bool,
                                    allow_incomplete::Bool,
                                    all_unranked_as_indifferent::Bool,
                                    total_rows::Int,
                                    kept_rows::Int,
                                    skipped::Dict,
                                    zero_rank_weight_mass::Float64 = 0.0,
                                    zero_rank_weight_missing::Int = 0)
    _PROFILE_BUILD_META[profile] = (
        weighted = weighted,
        allow_ties = allow_ties,
        allow_incomplete = allow_incomplete,
        all_unranked_as_indifferent = all_unranked_as_indifferent,
        total_rows = total_rows,
        kept_rows = kept_rows,
        skipped_no_ranked = Int(get(skipped, :no_ranked_candidates, 0)),
        skipped_incomplete = Int(get(skipped, :incomplete, 0)),
        skipped_invalid_weight = Int(get(skipped, :invalid_weight, 0)),
        zero_rank_weight_mass = weighted ? Float64(zero_rank_weight_mass) : nothing,
        zero_rank_weight_missing = weighted ? Int(zero_rank_weight_missing) : 0,
    )
    return profile
end

"""
    humanize_candidate_name(name)

Normalize a candidate label for display by trimming whitespace and replacing
underscores with spaces.
"""
@inline humanize_candidate_name(name::AbstractString) = replace(strip(String(name)), "_" => " ")

"""
    canonical_candidate_key(x)

Normalize candidate identifiers for matching (`strip`, lowercase,
underscore-to-space).
"""
@inline canonical_candidate_key(x) = lowercase(replace(strip(String(x)), "_" => " "))

"""
    candidate_display_symbols(candidate_cols)

Convert candidate column names to display symbols after humanization.
Throws if normalized labels are not unique.
"""
function candidate_display_symbols(candidate_cols::AbstractVector)
    labels = humanize_candidate_name.(String.(candidate_cols))
    syms = Symbol.(labels)
    length(unique(syms)) == length(syms) || throw(ArgumentError(
        "Missing candidate mapping: non-unique candidate labels after normalization.",
    ))
    return syms
end

"""
    guess_weight_col(df; preferred=(:peso, :weight, :weights))

Try to find a survey-weight column by exact name or case-insensitive match.
Returns a `Symbol` or `nothing`.
"""
function guess_weight_col(df; preferred = (:peso, :weight, :weights))
    name_syms = Symbol.(names(df))
    preferred_syms = Symbol.(collect(preferred))

    for wanted in preferred_syms
        wanted in name_syms && return wanted
    end

    lowers = Dict(lowercase(String(s)) => s for s in name_syms)
    for wanted in preferred_syms
        key = lowercase(String(wanted))
        haskey(lowers, key) && return lowers[key]
    end
    return nothing
end

"""
    resolve_candidate_cols_from_set(df, universe_cols, candidate_set)

Resolve a requested `candidate_set` against a configured `universe_cols`
using canonicalized candidate names. If `candidate_set === nothing`, returns
`universe_cols`.
"""
function resolve_candidate_cols_from_set(df,
                                         universe_cols::Vector{String},
                                         candidate_set)
    available_names = String.(names(df))
    return resolve_candidate_cols_from_set(available_names, universe_cols, candidate_set)
end

function resolve_candidate_cols_from_set(available_names::AbstractVector{<:AbstractString},
                                         universe_cols::Vector{String},
                                         candidate_set)
    candidate_set === nothing && return universe_cols

    req = collect(candidate_set)
    isempty(req) && throw(ArgumentError("`candidate_set` cannot be empty."))

    name_set = Set(String.(available_names))
    available = [c for c in universe_cols if c in name_set]
    isempty(available) && throw(ArgumentError(
        "No candidate columns from the configured universe exist in the input table.",
    ))

    by_canon = Dict{String,String}()
    for c in available
        key = canonical_candidate_key(c)
        if haskey(by_canon, key) && by_canon[key] != c
            throw(ArgumentError(
                "Ambiguous candidate mapping for key `$key` in candidate universe.",
            ))
        end
        by_canon[key] = c
    end

    chosen = String[]
    for x in req
        key = canonical_candidate_key(x)
        haskey(by_canon, key) || throw(ArgumentError(
            "candidate_set entry `$(x)` is not available in this dataset.",
        ))
        push!(chosen, by_canon[key])
    end

    chosen = unique(chosen)
    isempty(chosen) && throw(ArgumentError("`candidate_set` resolved to zero candidate columns."))
    return chosen
end

@inline function _coerce_column_identifier(x, argname::Symbol)
    if x isa Symbol
        return x
    elseif x isa AbstractString
        name = strip(String(x))
        isempty(name) && throw(ArgumentError("`$argname` contains an empty column name."))
        return Symbol(name)
    end
    throw(ArgumentError(
        "`$argname` must contain only Symbols or Strings; got $(typeof(x)).",
    ))
end

function _coerce_column_vector(candidate_cols)
    candidate_cols isa AbstractVector || throw(ArgumentError(
        "`candidate_cols` must be a vector of column identifiers (Symbols or Strings).",
    ))

    cols = Symbol[]
    for x in candidate_cols
        push!(cols, _coerce_column_identifier(x, :candidate_cols))
    end
    isempty(cols) && throw(ArgumentError("`candidate_cols` cannot be empty."))
    length(unique(cols)) == length(cols) || throw(ArgumentError(
        "`candidate_cols` contains duplicates after normalization to Symbols.",
    ))
    return cols
end

function _validated_positive_weights(weight_values, weight_col::Symbol)
    weights = Float64[]
    sizehint!(weights, length(weight_values))

    for (i, raw) in pairs(weight_values)
        raw === missing && throw(ArgumentError(
            "Weight column `$weight_col` has missing values (first at row $i). " *
            "Weighted missingness requires finite positive weights in every row.",
        ))

        w = if raw isa Real
            Float64(raw)
        elseif raw isa AbstractString
            parsed = tryparse(Float64, strip(raw))
            parsed === nothing && throw(ArgumentError(
                "Weight column `$weight_col` must be numeric; found `$(raw)` at row $i.",
            ))
            parsed
        else
            throw(ArgumentError(
                "Weight column `$weight_col` must be numeric; found $(typeof(raw)) at row $i.",
            ))
        end

        isfinite(w) || throw(ArgumentError(
            "Weight column `$weight_col` must be finite; found $w at row $i.",
        ))
        w > 0 || throw(ArgumentError(
            "Weight column `$weight_col` must be strictly positive; found $w at row $i.",
        ))

        push!(weights, w)
    end

    isempty(weights) && throw(ArgumentError("Cannot summarize missingness for an empty DataFrame."))
    return weights
end

@inline function _coerce_missing_codes(missing_codes)
    missing_codes === nothing && return nothing
    if missing_codes isa AbstractVector || missing_codes isa Tuple || missing_codes isa AbstractSet
        return collect(missing_codes)
    end
    return Any[missing_codes]
end

@inline function _value_counts_as_missing(x, missing_codes)
    ismissing(x) && return true
    missing_codes === nothing && return false
    return any(code -> isequal(x, code), missing_codes)
end

"""
    candidate_missingness_table(df, candidate_cols;
                                weighted::Bool=false,
                                weight_col=nothing,
                                missing_codes=nothing,
                                sort_desc::Bool=true)

Return a `DataFrame` with one row per candidate-score column and columns:
`candidate`, `column`, `missing_amount`, `total_amount`,
`missing_proportion`, and `missing_percent`.

In unweighted mode, amounts are raw row counts. In weighted mode, amounts are
weighted totals using `weight_col`, which must contain finite strictly positive
weights in every row. By default, rows are sorted by descending
`missing_percent`.

`missing_codes` can be a scalar or collection of sentinel values to treat as
missing in addition to Julia `missing`. Matching uses `isequal`, so this can
cover numeric survey codes, strings, or `nothing` when those encodings are
actually present in the data.

Warning: unweighted missingness describes sample completeness, while weighted
missingness describes missingness in the weighted survey population. Survey
weights do not generally solve item nonresponse bias. Weighted missingness can
still be descriptively useful, but it should not be overinterpreted.
"""
function candidate_missingness_table(df::AbstractDataFrame,
                                     candidate_cols;
                                     weighted::Bool = false,
                                     weight_col = nothing,
                                     missing_codes = nothing,
                                     sort_desc::Bool = true)
    nrow(df) > 0 || throw(ArgumentError("Cannot summarize missingness for an empty DataFrame."))

    cols = _coerce_column_vector(candidate_cols)
    available = Set(Symbol.(names(df)))
    missing_codes_vec = _coerce_missing_codes(missing_codes)

    missing_cols = [col for col in cols if !(col in available)]
    isempty(missing_cols) || throw(ArgumentError(
        "Candidate columns not found in DataFrame: $(missing_cols).",
    ))

    result = DataFrame(
        candidate = String[],
        column = String[],
        missing_amount = Float64[],
        total_amount = Float64[],
        missing_proportion = Float64[],
        missing_percent = Float64[],
    )

    total_amount = if weighted
        weight_col === nothing && throw(ArgumentError(
            "weighted=true requires `weight_col`.",
        ))

        weight_sym = _coerce_column_identifier(weight_col, :weight_col)
        weight_sym in available || throw(ArgumentError(
            "Weight column `$weight_sym` not found in DataFrame.",
        ))

        # Weighted missingness is descriptive for the weighted population; it is
        # not a correction for item nonresponse bias.
        weights = _validated_positive_weights(df[!, weight_sym], weight_sym)
        total_weight = sum(weights)

        for col in cols
            missing_mask = map(x -> _value_counts_as_missing(x, missing_codes_vec), df[!, col])
            missing_amount = sum(weights[missing_mask])
            missing_proportion = missing_amount / total_weight

            push!(result, (
                candidate = humanize_candidate_name(String(col)),
                column = String(col),
                missing_amount = missing_amount,
                total_amount = total_weight,
                missing_proportion = missing_proportion,
                missing_percent = 100 * missing_proportion,
            ))
        end

        total_weight
    else
        total_rows = Float64(nrow(df))

        for col in cols
            missing_amount = Float64(count(x -> _value_counts_as_missing(x, missing_codes_vec), df[!, col]))
            missing_proportion = missing_amount / total_rows

            push!(result, (
                candidate = humanize_candidate_name(String(col)),
                column = String(col),
                missing_amount = missing_amount,
                total_amount = total_rows,
                missing_proportion = missing_proportion,
                missing_percent = 100 * missing_proportion,
            ))
        end

        total_rows
    end

    if !isempty(result)
        result.total_amount .= total_amount
        sort_desc && sort!(result, :missing_percent, rev = true)
    end

    return result
end

"""
    normalize_numeric_score(v)

Parse scalar score-like values into `Float64`, returning `missing` for
non-numeric or non-finite values.
"""
@inline function normalize_numeric_score(v)
    v === missing && return missing

    x = if v isa Real
        Float64(v)
    elseif v isa AbstractString
        parsed = tryparse(Float64, strip(v))
        parsed === nothing && return missing
        parsed
    else
        return missing
    end

    isfinite(x) || return missing
    return x
end

"""
    normalize_nonnegative_weight(v)

Parse a nonnegative finite weight. Returns `nothing` if invalid.
"""
@inline function normalize_nonnegative_weight(v)
    v === missing && return nothing
    v isa Real || return nothing
    w = Float64(v)
    (isfinite(w) && w >= 0.0) || return nothing
    return w
end

"""
    row_to_weak_rank_from_scores(row, candidate_cols; ...)

Convert one score row into a `WeakRank` ballot.

Returns `(ballot, reason)` where `ballot` is `WeakRank` or `nothing`, and
`reason` is one of:
- `:ok`
- `:no_ranked_candidates`
- `:incomplete`
- `:all_indifferent_from_unranked`
"""
function row_to_weak_rank_from_scores(row,
                                      candidate_cols::Vector{String};
                                      score_normalizer::Function = normalize_numeric_score,
                                      allow_ties::Bool = true,
                                      allow_incomplete::Bool = true,
                                      all_unranked_as_indifferent::Bool = false)
    m = length(candidate_cols)
    scores = Vector{Union{Missing,Float64}}(undef, m)

    @inbounds for (j, col) in enumerate(candidate_cols)
        raw = score_normalizer(row[col])
        if raw === missing || raw === nothing
            scores[j] = missing
        else
            x = Float64(raw)
            scores[j] = isfinite(x) ? x : missing
        end
    end

    if all(ismissing, scores)
        if all_unranked_as_indifferent
            all_tied = Vector{Union{Int,Missing}}(undef, m)
            fill!(all_tied, 1)
            return (WeakRank(all_tied), :all_indifferent_from_unranked)
        end
        return (nothing, :no_ranked_candidates)
    end
    (!allow_incomplete && any(ismissing, scores)) && return (nothing, :incomplete)

    ranks_vec = Vector{Union{Int,Missing}}(undef, m)
    fill!(ranks_vec, missing)

    if allow_ties
        uniq_scores = sort!(unique(collect(skipmissing(scores))); rev = true)
        lookup = Dict{Float64,Int}(s => i for (i, s) in enumerate(uniq_scores))
        @inbounds for j in eachindex(scores)
            s = scores[j]
            ismissing(s) && continue
            ranks_vec[j] = lookup[s]
        end
    else
        ranked_idx = [j for j in eachindex(scores) if !ismissing(scores[j])]
        sort!(ranked_idx; by = j -> (-scores[j], j))
        @inbounds for (rk, j) in enumerate(ranked_idx)
            ranks_vec[j] = rk
        end
    end

    return (WeakRank(ranks_vec), :ok)
end

"""
    build_profile_from_scores(table, candidate_cols, candidate_syms; ...)

Build a `Profile` or `WeightedProfile` from row-like score data.

The table must support either `eachrow(table)` or direct iteration over rows.
Rows must support `row[col]` lookup for each candidate column and (if weighted)
for `weight_col`.

Domain-specific score handling should be supplied via `score_normalizer`.
"""
function build_profile_from_scores(table,
                                   candidate_cols::Vector{String},
                                   candidate_syms::Vector{Symbol};
                                   weighted::Bool = false,
                                   allow_ties::Bool = true,
                                   allow_incomplete::Bool = true,
                                   all_unranked_as_indifferent::Bool = false,
                                   weight_col = nothing,
                                   score_normalizer::Function = normalize_numeric_score,
                                   weight_normalizer::Function = normalize_nonnegative_weight,
                                   empty_profile_error_message = nothing)
    length(candidate_cols) == length(candidate_syms) || throw(ArgumentError(
        "`candidate_cols` and `candidate_syms` must have the same length.",
    ))

    weighted && weight_col === nothing && throw(ArgumentError(
        "weighted=true but no weight column was provided.",
    ))

    rows = applicable(eachrow, table) ? eachrow(table) : table

    pool = CandidatePool(candidate_syms)
    ballots = Any[]
    wts = Float64[]
    skipped = Dict(
        :no_ranked_candidates => 0,
        :incomplete => 0,
        :invalid_weight => 0,
    )
    zero_rank_weight_mass = 0.0
    zero_rank_weight_missing = 0
    total_rows = 0

    for row in rows
        total_rows += 1
        ballot, reason = row_to_weak_rank_from_scores(
            row,
            candidate_cols;
            score_normalizer = score_normalizer,
            allow_ties = allow_ties,
            allow_incomplete = allow_incomplete,
            all_unranked_as_indifferent = all_unranked_as_indifferent,
        )

        if ballot === nothing
            skipped[reason] += 1
            if weighted && reason == :no_ranked_candidates
                w = weight_normalizer(row[weight_col])
                if w === nothing
                    zero_rank_weight_missing += 1
                else
                    zero_rank_weight_mass += w
                end
            end
            continue
        end

        if weighted
            w = weight_normalizer(row[weight_col])
            if w === nothing
                skipped[:invalid_weight] += 1
                continue
            end
            push!(wts, w)
        end

        push!(ballots, ballot)
    end

    if isempty(ballots)
        msg = empty_profile_error_message === nothing ?
              "No valid preference rows remained after filtering." :
              String(empty_profile_error_message)
        throw(ArgumentError(msg))
    end

    if any(>(0), values(skipped))
        @info "build_profile_from_scores skipped rows" skipped
    end

    profile = nothing
    try
        profile = weighted ? WeightedProfile(pool, ballots, wts) :
                  Profile(pool, ballots)
    catch err
        msg = sprint(showerror, err)
        target = weighted ? "WeightedProfile" : "Profile"
        throw(ArgumentError("Failed to construct Preferences.$target: $msg"))
    end

    _store_profile_build_meta!(
        profile;
        weighted = weighted,
        allow_ties = allow_ties,
        allow_incomplete = allow_incomplete,
        all_unranked_as_indifferent = all_unranked_as_indifferent,
        total_rows = total_rows,
        kept_rows = length(ballots),
        skipped = skipped,
        zero_rank_weight_mass = zero_rank_weight_mass,
        zero_rank_weight_missing = zero_rank_weight_missing,
    )
    return profile
end

function _validate_profile_input(profile)
    is_profile = profile isa Profile
    is_weighted_profile = profile isa WeightedProfile
    (is_profile || is_weighted_profile) || throw(ArgumentError(
        "Expected Preferences.Profile or Preferences.WeightedProfile.",
    ))
    return is_weighted_profile
end

function _ballot_pattern_string(ballot::WeakRank, pool)::String
    levels = to_weakorder(ballot)
    has_unranked = any(ismissing, ranks(ballot))
    if has_unranked && !isempty(levels)
        levels = levels[1:end-1]
    end

    isempty(levels) && return ""

    blocks = String[]
    for grp in levels
        names_grp = [String(pool[id]) for id in grp]
        push!(blocks, join(names_grp, "~"))
    end
    return join(blocks, ">")
end

function _ballot_pattern_string(ballot::StrictRank, pool)::String
    return join([String(pool[id]) for id in ballot.perm], ">")
end

function _ballot_pattern_string(ballot, pool)::String
    if applicable(to_perm, ballot)
        return join([String(pool[id]) for id in to_perm(ballot)], ">")
    end
    return string(ballot)
end

function _resolve_profile_subset_symbols(pool, candidate_set)::Vector{Symbol}
    req = collect(candidate_set)
    isempty(req) && throw(ArgumentError("`candidate_set` cannot be empty."))

    pool_syms = collect(pool.names)
    by_canon = Dict{String,Symbol}()
    for s in pool_syms
        key = canonical_candidate_key(s)
        if haskey(by_canon, key) && by_canon[key] != s
            throw(ArgumentError(
                "Ambiguous candidate mapping for key `$key` in profile pool.",
            ))
        end
        by_canon[key] = s
    end

    subset = Symbol[]
    for x in req
        key = canonical_candidate_key(x)
        haskey(by_canon, key) || throw(ArgumentError(
            "candidate_set entry `$(x)` not found in profile candidate pool.",
        ))
        push!(subset, by_canon[key])
    end

    subset = unique(subset)
    isempty(subset) && throw(ArgumentError("`candidate_set` resolved to zero pool candidates."))
    return subset
end

function _profile_pattern_mass_table(profile; weighted::Bool = true,
                                     include_empty::Bool = false)
    is_weighted_profile = _validate_profile_input(profile)

    n = nballots(profile)
    n > 0 || throw(ArgumentError("Cannot aggregate an empty profile."))

    masses = if is_weighted_profile && weighted
        Float64.(weights(profile))
    else
        ones(Float64, n)
    end

    mass_total = sum(masses)
    mass_total > 0 || throw(ArgumentError(
        "Total mass is zero. Check survey weights or use weighted=false.",
    ))

    agg = Dict{String,Float64}()
    pool = profile.pool

    @inbounds for i in 1:n
        key = _ballot_pattern_string(profile.ballots[i], pool)
        if isempty(key) && !include_empty
            continue
        end
        agg[key] = get(agg, key, 0.0) + masses[i]
    end

    isempty(agg) && throw(ArgumentError(
        include_empty ? "No patterns were found after aggregation." :
                        "No non-empty patterns were found after aggregation.",
    ))

    patterns = collect(keys(agg))
    mass = [agg[k] for k in patterns]

    tbl = DataFrame(pattern = patterns, mass = mass)
    sort!(tbl, :mass, rev = true)
    return tbl
end

@inline function _pattern_block_sizes(pattern::AbstractString)::Vector{Int}
    pat = strip(String(pattern))
    isempty(pat) && return Int[]

    blocks = Int[]
    for block in split(pat, '>')
        b = strip(block)
        isempty(b) && continue
        tied = [x for x in split(b, '~') if !isempty(strip(x))]
        push!(blocks, length(tied))
    end
    return blocks
end

@inline _ranking_type_key(blocks::AbstractVector{<:Integer}) = "[" * join(Int.(blocks), ",") * "]"

"""
    ranked_count(pattern_or_blocks) -> Int

Return the number of explicitly ranked alternatives in a rendered pattern
(`"A>B~C"`) or in a ranking-type block vector (`[1,2]`).
"""
ranked_count(pattern::AbstractString) = sum(_pattern_block_sizes(pattern))
ranked_count(blocks::AbstractVector{<:Integer}) = sum(Int(x) for x in blocks)

"""
    has_ties(pattern_or_blocks) -> Bool

Return whether a rendered pattern (`"A>B~C"`) or ranking-type block vector
contains at least one indifference block of size greater than one.
"""
has_ties(pattern::AbstractString) = any(>(1), _pattern_block_sizes(pattern))
has_ties(blocks::AbstractVector{<:Integer}) = any(x -> Int(x) > 1, blocks)

function _resolve_zero_rank_mass(profile; weighted::Bool,
                                 observed_empty_mass::Float64)
    observed_empty_mass > 0 && return (
        mass = observed_empty_mass,
        known = true,
        source = "observed_empty_patterns",
        missing_weight_rows = 0,
    )

    meta = profile_build_meta(profile)
    meta === nothing && return (
        mass = 0.0,
        known = false,
        source = "unavailable",
        missing_weight_rows = 0,
    )

    if getproperty(meta, :all_unranked_as_indifferent)
        return (
            mass = 0.0,
            known = true,
            source = "all_unranked_as_indifferent=true",
            missing_weight_rows = 0,
        )
    end

    if weighted
        z = getproperty(meta, :zero_rank_weight_mass)
        z === nothing && return (
            mass = 0.0,
            known = false,
            source = "metadata_missing_weight_mass",
            missing_weight_rows = Int(getproperty(meta, :zero_rank_weight_missing)),
        )
        return (
            mass = Float64(z),
            known = true,
            source = "build_profile_metadata_weighted",
            missing_weight_rows = Int(getproperty(meta, :zero_rank_weight_missing)),
        )
    end

    return (
        mass = Float64(getproperty(meta, :skipped_no_ranked)),
        known = true,
        source = "build_profile_metadata_unweighted",
        missing_weight_rows = 0,
    )
end

function _profile_pattern_features(profile; weighted::Bool = true,
                                   include_zero_rank::Bool = true)
    is_weighted_profile = _validate_profile_input(profile)
    use_weights = is_weighted_profile && weighted
    base = _profile_pattern_mass_table(profile; weighted = weighted,
                                       include_empty = true)

    observed_empty_mass = 0.0
    keep = trues(nrow(base))
    @inbounds for i in 1:nrow(base)
        if isempty(strip(String(base.pattern[i])))
            observed_empty_mass += Float64(base.mass[i])
            keep[i] = false
        end
    end

    tbl = base[keep, :]
    if nrow(tbl) > 0
        blocks = [_pattern_block_sizes(String(p)) for p in tbl.pattern]
        tbl.ranked_count = [ranked_count(b) for b in blocks]
        tbl.has_ties = [has_ties(b) for b in blocks]
        tbl.type_blocks = blocks
        tbl.type_key = [_ranking_type_key(b) for b in blocks]
    else
        tbl.ranked_count = Int[]
        tbl.has_ties = Bool[]
        tbl.type_blocks = Vector{Vector{Int}}()
        tbl.type_key = String[]
    end

    zero_rank_raw = include_zero_rank ?
        _resolve_zero_rank_mass(profile; weighted = use_weights,
                                observed_empty_mass = observed_empty_mass) :
        (mass = 0.0, known = true, source = "not_requested", missing_weight_rows = 0)

    total_mass = sum(tbl.mass) + Float64(zero_rank_raw.mass)
    total_mass > 0 || throw(ArgumentError(
        "No mass available after filtering patterns and zero-ranked category.",
    ))
    tbl.proportion = tbl.mass ./ total_mass

    zero_rank = (
        included = include_zero_rank,
        mass = Float64(zero_rank_raw.mass),
        proportion = Float64(zero_rank_raw.mass) / total_mass,
        known = Bool(zero_rank_raw.known),
        source = String(zero_rank_raw.source),
        missing_weight_rows = Int(zero_rank_raw.missing_weight_rows),
    )

    return (
        weighted = use_weights,
        table = tbl,
        total_mass = total_mass,
        zero_rank = zero_rank,
        observed_empty_mass = observed_empty_mass,
    )
end

function _empty_ranksize_rows()
    return DataFrame(
        pattern = String[],
        mass = Float64[],
        proportion = Float64[],
        ranked_count = Int[],
        type_blocks = Vector{Vector{Int}}(),
        type_key = String[],
    )
end

"""
    profile_pattern_proportions(profile; weighted::Bool=true, candidate_set=nothing)

Aggregate identical preference patterns and return a table sorted by descending
proportion. Pattern rendering uses:
- `>` for strict preference blocks,
- `~` for ties within a block,
- omission of unranked candidates (incomplete ballots).
"""
function profile_pattern_proportions(profile; weighted::Bool = true,
                                     candidate_set = nothing)
    if candidate_set !== nothing
        _validate_profile_input(profile)
        subset_syms = _resolve_profile_subset_symbols(profile.pool, candidate_set)
        restricted, _, _ = restrict(profile, subset_syms)
        return profile_pattern_proportions(restricted; weighted = weighted,
                                           candidate_set = nothing)
    end

    tbl = _profile_pattern_mass_table(profile; weighted = weighted,
                                      include_empty = false)
    mass_total = sum(tbl.mass)
    mass_total > 0 || throw(ArgumentError(
        "No non-empty patterns were found after aggregation.",
    ))
    tbl.proportion = tbl.mass ./ mass_total
    sort!(tbl, :proportion, rev = true)
    return tbl
end

"""
    ranking_type_support(r::Int) -> Vector{Vector{Int}}

Return the full support of ordered indifference-block compositions for ranking
size `r`. Ordering is lexicographic by block sizes for deterministic output.
"""
function ranking_type_support(r::Int)::Vector{Vector{Int}}
    r >= 1 || throw(ArgumentError("`r` must be >= 1."))

    out = Vector{Vector{Int}}()
    current = Int[]

    function _compose!(remaining::Int)
        if remaining == 0
            push!(out, copy(current))
            return
        end
        for block in 1:remaining
            push!(current, block)
            _compose!(remaining - block)
            pop!(current)
        end
        return nothing
    end

    _compose!(r)
    return out
end

"""
    ranking_type_template(blocks::Vector{Int}) -> String

Render an ordered block-size type with canonical placeholders `A1, A2, ...`.
Examples:
- `[1,1,2] -> "A1 > A2 > A3 ~ A4"`
- `[2,2]   -> "A1 ~ A2 > A3 ~ A4"`
"""
function ranking_type_template(blocks::AbstractVector{<:Integer})::String
    isempty(blocks) && return "(none ranked)"
    any(x -> Int(x) <= 0, blocks) && throw(ArgumentError(
        "All block sizes must be positive integers.",
    ))

    idx = 1
    chunks = String[]
    for size in blocks
        ss = Int(size)
        labels = ["A$j" for j in idx:(idx + ss - 1)]
        push!(chunks, join(labels, " ~ "))
        idx += ss
    end
    return join(chunks, " > ")
end

"""
    profile_ranksize_summary(profile; k::Int, weighted::Bool=true, include_zero_rank::Bool=true)

Build a navigation summary for ranking sizes `k:-1:1`, splitting each size into
strict and tied observed patterns.
If `include_zero_rank=true`, zero-ranked mass is recovered from build metadata
when available (`build_profile_from_scores` stores this metadata).
"""
function profile_ranksize_summary(profile; k::Int, weighted::Bool = true,
                                  include_zero_rank::Bool = true)
    _validate_profile_input(profile)
    m = length(profile.pool)
    1 <= k <= m || throw(ArgumentError("`k` must satisfy 1 <= k <= $m."))

    features = _profile_pattern_features(profile; weighted = weighted,
                                         include_zero_rank = include_zero_rank)
    tbl = features.table
    total_mass = features.total_mass

    by_size = OrderedDict{Int,NamedTuple}()
    selected_cols = [:pattern, :mass, :proportion, :ranked_count, :type_blocks, :type_key]

    for r in k:-1:1
        rows_r = tbl[tbl.ranked_count .== r, :]
        strict_rows = nrow(rows_r) == 0 ? _empty_ranksize_rows() :
                      rows_r[.!rows_r.has_ties, selected_cols]
        tied_rows = nrow(rows_r) == 0 ? _empty_ranksize_rows() :
                    rows_r[rows_r.has_ties, selected_cols]

        nrow(strict_rows) > 0 && sort!(strict_rows, :proportion, rev = true)
        nrow(tied_rows) > 0 && sort!(tied_rows, :proportion, rev = true)

        mass_r = nrow(rows_r) == 0 ? 0.0 : sum(rows_r.mass)
        by_size[r] = (
            mass = mass_r,
            proportion = mass_r / total_mass,
            strict = strict_rows,
            with_ties = tied_rows,
        )
    end

    below_min_mask = tbl.ranked_count .< 1
    above_k_mask = tbl.ranked_count .> k
    below_min_mass = any(below_min_mask) ? sum(tbl.mass[below_min_mask]) : 0.0
    above_k_mass = any(above_k_mask) ? sum(tbl.mass[above_k_mask]) : 0.0

    return (
        weighted = features.weighted,
        k = k,
        m = m,
        total_mass = total_mass,
        zero_rank = features.zero_rank,
        below_min_rank = (
            mass = below_min_mass,
            proportion = below_min_mass / total_mass,
        ),
        above_k_rank = (
            mass = above_k_mass,
            proportion = above_k_mass / total_mass,
        ),
        by_size = by_size,
    )
end

"""
    profile_ranking_type_proportions(profile; k::Int, weighted::Bool=true, include_zero_rank::Bool=true)

For each ranking size `r = k:-1:1`, return all logically possible ranking types
(ordered block compositions), including zero-proportion types.
If `include_zero_rank=true`, zero-ranked mass is recovered from build metadata
when available (`build_profile_from_scores` stores this metadata).
"""
function profile_ranking_type_proportions(profile; k::Int, weighted::Bool = true,
                                          include_zero_rank::Bool = true)
    _validate_profile_input(profile)
    m = length(profile.pool)
    1 <= k <= m || throw(ArgumentError("`k` must satisfy 1 <= k <= $m."))

    features = _profile_pattern_features(profile; weighted = weighted,
                                         include_zero_rank = include_zero_rank)
    tbl = features.table
    total_mass = features.total_mass

    by_size = OrderedDict{Int,NamedTuple}()
    for r in k:-1:1
        support = ranking_type_support(r)
        keys = [_ranking_type_key(blocks) for blocks in support]
        templates = [ranking_type_template(blocks) for blocks in support]

        observed = Dict{String,Float64}()
        rows_r = tbl[tbl.ranked_count .== r, :]
        mass_r = nrow(rows_r) == 0 ? 0.0 : sum(rows_r.mass)
        for row in eachrow(rows_r)
            key = String(row.type_key)
            observed[key] = get(observed, key, 0.0) + Float64(row.mass)
        end

        masses = [get(observed, key, 0.0) for key in keys]
        within = mass_r > 0 ? masses ./ mass_r : zeros(Float64, length(masses))

        type_table = DataFrame(
            type_blocks = [copy(blocks) for blocks in support],
            type_key = keys,
            template = templates,
            mass = masses,
            proportion = masses ./ total_mass,
            within_size_proportion = within,
        )

        by_size[r] = (
            mass = mass_r,
            proportion = mass_r / total_mass,
            table = type_table,
        )
    end

    below_min_mask = tbl.ranked_count .< 1
    above_k_mask = tbl.ranked_count .> k
    below_min_mass = any(below_min_mask) ? sum(tbl.mass[below_min_mask]) : 0.0
    above_k_mass = any(above_k_mask) ? sum(tbl.mass[above_k_mask]) : 0.0

    return (
        weighted = features.weighted,
        k = k,
        m = m,
        total_mass = total_mass,
        zero_rank = features.zero_rank,
        below_min_rank = (
            mass = below_min_mass,
            proportion = below_min_mass / total_mass,
        ),
        above_k_rank = (
            mass = above_k_mass,
            proportion = above_k_mass / total_mass,
        ),
        by_size = by_size,
    )
end

function _print_pattern_rows(io::IO, rows::DataFrame; digits::Int)
    if nrow(rows) == 0
        println(io, "    (none)")
        return nothing
    end
    for row in eachrow(rows)
        @printf(
            io,
            "    %s : %.*f\n",
            String(row.pattern),
            digits,
            Float64(row.proportion),
        )
    end
    return nothing
end

"""
    pretty_print_ranksize_summary(summary; digits::Int=4)

Pretty-print output from `profile_ranksize_summary`.
"""
function pretty_print_ranksize_summary(summary; digits::Int = 4, io::IO = stdout)
    @printf(
        io,
        "Ranking-size summary (k=%d, m=%d, weighted=%s)\n",
        Int(summary.k),
        Int(summary.m),
        string(summary.weighted),
    )
    @printf(io, "Total accounted mass: %.*f\n", digits, Float64(summary.total_mass))

    if summary.zero_rank.included
        label = summary.zero_rank.known ? "Zero-ranked" :
                "Zero-ranked (unknown; assumed 0)"
        @printf(io, "%s : %.*f", label, digits, Float64(summary.zero_rank.proportion))
        if summary.zero_rank.known
            @printf(io, " [source: %s]", String(summary.zero_rank.source))
        end
        println(io)
        if Int(summary.zero_rank.missing_weight_rows) > 0
            @printf(
                io,
                "  note: %d zero-ranked rows had missing/invalid weights in weighted mode.\n",
                Int(summary.zero_rank.missing_weight_rows),
            )
        end
    end

    if summary.below_min_rank.mass > 0
        @printf(
            io,
            "Ranked fewer than 1 candidate: %.*f\n",
            digits,
            Float64(summary.below_min_rank.proportion),
        )
    end
    if summary.above_k_rank.mass > 0
        @printf(
            io,
            "Ranked more than k candidates: %.*f\n",
            digits,
            Float64(summary.above_k_rank.proportion),
        )
    end

    for (r, bucket) in summary.by_size
        @printf(io, "\nr = %d : %.*f\n", Int(r), digits, Float64(bucket.proportion))
        println(io, "  strict:")
        _print_pattern_rows(io, bucket.strict; digits = digits)
        println(io, "  with_ties:")
        _print_pattern_rows(io, bucket.with_ties; digits = digits)
    end
    return nothing
end

"""
    pretty_print_ranking_type_proportions(type_tbl; digits::Int=4)

Pretty-print output from `profile_ranking_type_proportions`.
"""
function pretty_print_ranking_type_proportions(type_tbl; digits::Int = 4, io::IO = stdout)
    @printf(
        io,
        "Ranking-type proportions (k=%d, m=%d, weighted=%s)\n",
        Int(type_tbl.k),
        Int(type_tbl.m),
        string(type_tbl.weighted),
    )
    @printf(io, "Total accounted mass: %.*f\n", digits, Float64(type_tbl.total_mass))

    if type_tbl.zero_rank.included
        label = type_tbl.zero_rank.known ? "Zero-ranked" :
                "Zero-ranked (unknown; assumed 0)"
        @printf(io, "%s : %.*f\n", label, digits, Float64(type_tbl.zero_rank.proportion))
    end
    if type_tbl.below_min_rank.mass > 0
        @printf(
            io,
            "Ranked fewer than 1 candidate: %.*f\n",
            digits,
            Float64(type_tbl.below_min_rank.proportion),
        )
    end
    if type_tbl.above_k_rank.mass > 0
        @printf(
            io,
            "Ranked more than k candidates: %.*f\n",
            digits,
            Float64(type_tbl.above_k_rank.proportion),
        )
    end

    for (r, payload) in type_tbl.by_size
        @printf(io, "\nr = %d : %.*f\n", Int(r), digits, Float64(payload.proportion))
        for row in eachrow(payload.table)
            @printf(
                io,
                "  %s | %s : %.*f (within r: %.*f)\n",
                String(row.type_key),
                String(row.template),
                digits,
                Float64(row.proportion),
                digits,
                Float64(row.within_size_proportion),
            )
        end
    end
    return nothing
end

"""
    pretty_print_profile_patterns(tbl;
                                  digits::Int=4,
                                  others_threshold=nothing,
                                  others_label="Others")

Pretty-print a pattern table as:
`PATTERN : proportion`

If `others_threshold` is set (e.g. `0.01`), any row with proportion strictly
below the threshold is collapsed into one `others_label` row.
"""
function pretty_print_profile_patterns(tbl;
                                       digits::Int = 4,
                                       io::IO = stdout,
                                       others_threshold = nothing,
                                       others_label::AbstractString = "Others")
    (("pattern" in names(tbl)) || (:pattern in names(tbl))) || throw(ArgumentError(
        "Table must contain a `pattern` column.",
    ))
    (("proportion" in names(tbl)) || (:proportion in names(tbl))) || throw(ArgumentError(
        "Table must contain a `proportion` column.",
    ))

    if others_threshold !== nothing
        th = Float64(others_threshold)
        (0.0 <= th <= 1.0) || throw(ArgumentError(
            "`others_threshold` must be between 0 and 1.",
        ))

        others_prop = 0.0
        for row in eachrow(tbl)
            p = Float64(row.proportion)
            if p < th
                others_prop += p
            else
                pat = String(row.pattern)
                @printf(io, "%s : %.*f\n", pat, digits, p)
            end
        end
        if others_prop > 0
            @printf(io, "%s : %.*f\n", others_label, digits, others_prop)
        end
        return nothing
    end

    for row in eachrow(tbl)
        pat = String(row.pattern)
        @printf(io, "%s : %.*f\n", pat, digits, Float64(row.proportion))
    end
    return nothing
end
