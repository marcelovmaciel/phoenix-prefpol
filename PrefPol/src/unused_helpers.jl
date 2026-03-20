
#########################################################################################
# From src/preprocessing_general.jl
########################################################################################
function load_project_path()
    return dirname(Pkg.project().path)
end



function bootstrap_encoded_variants(enc_imps;
weights::AbstractVector,
B::Int = 10,
keys::Tuple = (:zero, :random, :mice))
result = Dict{Symbol,Dict}()

for k in keys
    enc  = enc_imps[k]                          # Dict with :df, :r_to_code, :code_to_r
    df   = enc[:df]
    code = enc[:code_to_r]

    boot = checked_weighted_bootstrap(df, code, weights, B)   # Vector{DataFrame}

    result[k] = Dict(
        :dfs       => boot,
        :r_to_code => enc[:r_to_code],
        :code_to_r => enc[:code_to_r],
    )
end
return result
end

function encoded_imputed_df(df, score_cols, demographics)
    foo = profile_dataframe(normalize_scores!(copy(df), Symbol.(score_cols)); score_cols = score_cols, demo_cols = demographics)
    # foo = profile_dataframe(normalize_scores_int!(copy(df), Symbol.(score_cols)); score_cols = score_cols, demo_cols = demographics)
    # second variant might be faster
    # Build full codebook on *all* rows
    ranks = unique(foo.profile)                     # every ranking present
    r_to_code = Dict(r => i for (i,r) in enumerate(ranks))
    code_to_r = Dict(i => r for (i,r) in enumerate(ranks))

    # --- encode in-place, checking every row -------------------------------
    foo.profile = [ get(r_to_code, r) do
                        error("Row with unseen ranking slipped through the map.")
                    end for r in foo.profile ]

    return Dict(
        :df        => foo,          # fully encoded DataFrame
        :r_to_code => r_to_code,
        :code_to_r => code_to_r,
    )
end



function encode_imputation_variants(variants, score_cols, demographics)
    # pairs(variants) yields (key, value) for NamedTuple, Dict, etc.
    return Dict(k => encoded_imputed_df(v, score_cols, demographics)
                for (k, v) in pairs(variants))
end



function check_keys(bt, key)
    println("values in profile: ", sort(unique(vcat(map(x-> unique(x.profile),bt[key][:dfs])...))) )
    println("type of value in profile: ", typeof(bt[key][:dfs][1].profile[1]))
    println("original values: " ,sort(Int.(keys(bt[key][:code_to_r]))))
    println("type of value in original: ", typeof(bt[key][:code_to_r]))
end



function checked_weighted_bootstrap(df::DataFrame,
                                    code_to_r,
                                    weights::Vector{Float64},
                                    B::Int)

    n = nrow(df)
    boot = Vector{DataFrame}(undef, B)

    # max valid code (for cheap comparison)
    maxcode = maximum(keys(code_to_r))

    for b in 1:B
        idxs = sample(1:n, Weights(weights), n; replace = true)
        sub  = df[idxs, :]

        # inexpensive check: any(code > max?)  — if yes, run full scan
        if any(code -> code > maxcode || !haskey(code_to_r, code),
               sub.profile)
            bad = unique(filter(code -> !haskey(code_to_r, code),
                                sub.profile))
            error("Bootstrap replicate $b contained unknown codes: $bad")
        end
        boot[b] = sub
    end
    return boot
end


"""
    bootstrap_variants(imps; weights, B = 10, keys = (:zero, :random, :mice))

• `imps`    – NamedTuple returned by `imputed_variants`
• `weights` – vector of survey weights (same length as nrow of the imputations)
• `B`       – number of bootstrap replications (default 10)
• `keys`    – which variants inside `imps` to bootstrap (default skips the
              raw tables with missings)

Returns `Dict{Symbol, Vector{DataFrame}}`.
"""
function bootstrap_variants(imps::NamedTuple;
                            weights::AbstractVector,
                            B::Int = 10,
                            keys::Tuple = (:zero, :random, :mice))

    Dict(k => weighted_bootstrap(imps[k], weights, B) for k in keys)
end




function extract_unique_scores(countmaps::Dict{String,<:AbstractDict})
    all_keys = Int[]
    for cm in values(countmaps)
        append!(all_keys, Int.(keys(cm)))
    end
    return sort!(unique(all_keys))
end




function normalize_scores!(df::DataFrame, score_syms::Vector{Symbol})
    for s in score_syms
        col = df[!, s]

        if eltype(col) <: Union{Missing, Int}
            continue                               # already nice
        end

        allowmissing!(df, s)                       # ensure we can keep missings

        if eltype(col) <: CategoricalValue
            # turn categorical → underlying string/int
            raw = levels(col)[levelcode.(col)]
        else
            raw = collect(col)                     # plain Vector
        end

        df[!, s] = Union{Missing, Int}[           # overwrite with Int / missing
            x === missing           ? missing :
            x isa Int               ? x :
            x isa AbstractString    ? tryparse(Int, x) :
            x isa Real              ? Int(x) :
                                      missing
            for x in raw
        ]
    end
    return df
end


function normalize_scores_int!(df::DataFrame, score_syms::Vector{Symbol})
    for s in score_syms
        col = df[!, s]

        eltype(col) <: Int && continue  # already good

        any(ismissing, col) &&
            error("Column $s still contains missing after imputation")

        vec = Vector{Int}(undef, length(col))

        @inbounds for i in eachindex(col)
            x = col[i]
            if x isa CategoricalValue
                raw = levels(x)[levelcode(x)]     # stored value (Int or String)
                vec[i] = raw isa Int ? raw : parse(Int, raw)
            else
                vec[i] = x isa Int ? x :
                          x isa AbstractString ? parse(Int, x) :
                          Int(x)                             # Float64 etc.
            end
        end
        df[!, s] = vec
    end
    return df
end

function decode_profile_vec(prof, code_to_r)
    return map(code -> code_to_r[code], prof)
end


#########################################################################################
# From polarization_measures.jl
########################################################################################

# Apply a measure to encoded bootstrap replicates by decoding profiles on the fly
function apply_measure_to_encoded_bts(bts, measure)
    out = Dict{Symbol,Vector{Float64}}()

    for (variant, enc) in pairs(bts)
        dfs       = enc[:dfs]                # Vector{DataFrame}
        code_to_r = enc[:code_to_r]

        vals = Vector{Float64}(undef, length(dfs))

        for (i, df) in enumerate(dfs)
            decoded = decode_profile_vec(df.profile, code_to_r)
            vals[i] = measure(decoded)
            empty!(decoded)                 # free Vector{Dict} payload
        end

        out[variant] = vals
    end
    return out
end

function apply_all_measures_to_encoded_bts(bts; measures = [Ψ, calc_total_reversal_component,
                     calc_reversal_HHI, fast_reversal_geometric])
    Dict(nameof(measure) => apply_measure_to_encoded_bts(bts, measure) for measure in measures)
end

function encoded_bootstrap_group_metrics(bts::Dict, demo::Symbol)
    out = Dict{Symbol, Dict{Symbol, Vector{Float64}}}()

    for (variant, enc) in pairs(bts)
        c2r            = enc[:code_to_r]
        Cvals, Dvals   = Float64[], Float64[]

        for df in enc[:dfs]
            decoded = decode_profile_vec(df.profile, c2r)

            # build *new* 2-column frame: profile + the grouping column
            tmp = DataFrame(profile = decoded)
            tmp[!, demo] = df[!, demo]        # copy just this one column

            C, D = compute_group_metrics(tmp, demo)
            push!(Cvals, C)
            push!(Dvals, D)
        end

        out[variant] = Dict(:C => Cvals, :D => Dvals)
    end
    return out
end

# *signed* margin, normalised by electorate size
margin(c₁, c₂, profile) = (nab(c₁, c₂, profile) - nab(c₂, c₁, profile)) / length(profile)

function canonical_pair(a, b)
    a < b ? (a, b) : (b, a)
end

function pairwise_margins(profile)
    cs = sort(collect(keys(profile[1])))             # deterministic order
    res = Dict{Tuple{Symbol,Symbol},Float64}()

    for v in combinations(cs, 2)                     # v is Vector of Symbols
        a, b       = v                               # unpack once
        res[(a,b)] = margin(a, b, profile)           # key is Tuple
    end
    return res
end

function margins_for_rep(df, code_to_r)
    prof = decode_profile_vec(df.profile, code_to_r)  # existing helper
    return pairwise_margins(prof)
end

function margins_over_bootstrap(bt_variant::Dict)
    dfs        = bt_variant[:dfs]             # Vector{DataFrame}
    code_to_r  = bt_variant[:code_to_r]
    return map(df -> margins_for_rep(df, code_to_r), dfs)
end

function summarize_margins(margin_dicts)
    pairs = keys(first(margin_dicts))
    stats = Dict{Tuple{Symbol,Symbol},Tuple{Float64,Float64}}()
    for p in pairs
        vals = [d[p] for d in margin_dicts]
        stats[p] = (mean(vals), std(vals))
    end
    return stats
end

function margins_dataframe(stats)
    pairs = collect(keys(stats))
    DataFrame(; cand1 = first.(pairs),
                 cand2 = last.(pairs),
                 mean_margin = first.(values(stats)),
                 sd_margin   = last.(values(stats)))
end

"""
    margin_stats(bt_top) → Dict{Symbol,DataFrame}

For every imputation variant (`:zero`, `:random`, `:mice`) contained in
`bt_top`, returns a DataFrame with columns
`cand1, cand2, mean_margin, sd_margin`.
"""
function margin_stats(bt_top::Dict{Symbol,Dict})
    out = Dict{Symbol,DataFrame}()
    for (var, bt_variant) in pairs(bt_top)
        mdicts = margins_over_bootstrap(bt_variant)
        stats  = summarize_margins(mdicts)
        out[var] = margins_dataframe(stats)
    end
    return out
end


"""Geometric measure of reversal component:
   sqrt( (sum of local reversals) * (sum of squares of local reversals) )"""
function calc_reversal_geometric(paired_accum, proportion_rankings::Dict)
    reversal_component = calc_total_reversal_component(paired_accum, proportion_rankings)
    reversal_hhi       = calc_reversal_HHI(paired_accum, proportion_rankings)
    sqrt(reversal_component * reversal_hhi)
end



function print_reversal_results(paired_accum, unpaired_accum)
    println("Paired Reversals:")
    if isempty(paired_accum)
        println("No reversal pairs found.")
    else
        for (r, i, rev_r, j) in paired_accum
            println("Index $i: $r  <==> Index $j: $rev_r")
        end
    end

    println("\nUnpaired Rankings:")
    if isempty(unpaired_accum)
        println("No unpaired rankings.")
    else
        for (r, i) in unpaired_accum
            println("Index $i: $r")
        end
    end
end




"""
    add_G!(stats)

Given the nested dictionary returned by `bootstrap_group_metrics`

    Dict(
        :zero   => Dict(:C => Vector, :D => Vector),
        :random => Dict(...),
        :mice   => Dict(...)
    )

compute the element-wise geometric mean  G = √(C · D)  for every variant and
store it in the inner dictionary under the key **`:G`**.
The function mutates `stats` and also returns it for convenience.
"""
function add_G!(stats::Dict{Symbol, Dict{Symbol, Vector{Float64}}})
    for sub in values(stats)
        C = sub[:C]
        D = sub[:D]
        @assert length(C) == length(D) "C and D vectors must be the same length"
        sub[:G] = sqrt.(C .* D)
    end
    return stats
end






function bootstrap_demographic_metrics(bt_profiles::Dict,
    demo_map::Dict{Symbol,String})

out = Dict{Symbol, Dict{Symbol, Vector{DataFrame}}}()

for (variant, reps) in bt_profiles
C_list = Vector{DataFrame}()
D_list = Vector{DataFrame}()

for df in reps
mdf = compute_demographic_metrics(df, demo_map)   # Demographic | C | D

push!(C_list,  mdf[:, [:Demographic, :C]])
push!(D_list,  mdf[:, [:Demographic, :D]])
end

out[variant] = Dict(:C => C_list,
:D => D_list)
end

return out
end



"""
    compute_demographic_metrics(df::DataFrame,
                                demo_map::Dict{Symbol,String})

Given `df` with `:profile` plus any number of demographics, `demo_map`
maps each column Symbol to the output label.  Returns a DataFrame
with rows (Demographic, C, D).
"""
function compute_demographic_metrics(df::DataFrame,
                                     demo_map::Dict{Symbol,String})
    result = DataFrame(Demographic=String[], C=Float64[], D=Float64[])
    for (col_sym, label) in demo_map
        @assert hasproperty(df, col_sym) "no column $col_sym in df"
        C, D = compute_group_metrics(df, col_sym)
        push!(result, (label, C, D))
    end
    return result
end


function group_consensus_and_divergence(df::DataFrame, demo::Symbol)
    # 1) group and collect keys in order
    gdf        = groupby(df, demo)
    group_keys = [subdf[1, demo] for subdf in gdf]

    # 2) proportion of each group
    prop_map = proportionmap(df[!, demo])

    # 3) build profiles and consensus maps
    group_profiles = Dict(
        k => collect(subdf.profile)
        for (k, subdf) in zip(group_keys, gdf)
    )
    consensus_map = Dict(
        k => get_consensus_ranking(group_profiles[k])[2]
        for k in group_keys
    )

    # 4) build the consensus_df with avg_distance and proportion
    consensus_df = DataFrame()
    consensus_df[!, demo]               = group_keys
    consensus_df[!, :consensus_ranking] = [consensus_map[k] for k in group_keys]
    consensus_df[!, :avg_distance]      = [
        group_avg_distance(subdf).avg_distance
        for subdf in gdf
    ]
    consensus_df[!, :proportion]        = [prop_map[k] for k in group_keys]

    # 5) compute pairwise divergences
    m     = length(first(values(consensus_map)))
    klen  = length(group_keys)
    M     = zeros(Float64, klen, klen)
    for i in 1:klen, j in 1:klen
        M[i,j] = i == j ? 0.0 :
                 pairwise_group_divergence(
                   group_profiles[group_keys[i]],
                   consensus_map[group_keys[j]],
                   m
                 )
    end

    # 6) build the divergence_df with proportion
    col_syms     = Symbol.(string.(group_keys))
    columns_dict = Dict(col_syms[j] => M[:,j] for j in 1:klen)
    divergence_df = DataFrame(columns_dict)
    divergence_df[!, demo]      = group_keys
    divergence_df[!, :proportion] = [prop_map[k] for k in group_keys]
    select!(divergence_df, [demo, :proportion, col_syms...])

    return consensus_df, divergence_df
end




function compute_coherence_and_divergence(df::DataFrame, key::Symbol)
    # 1) Group the DataFrame by `key`
    grouped_df = groupby(df, key)

    # 2) Compute average distance for each group
    results_distance = combine(grouped_df) do subdf
        group_avg_distance(subdf)  # returns (avg_distance = ..., group_coherence = ...)
    end
    #println(results_distance)
    # 3) Compute group proportions, e.g. proportion of each group in the entire df
    group_proportions = proportionmap(df[!, key])  # your user-defined function
    #println(" \n")
    #println(group_proportions)
    # 4) Compute weighted coherence (sum of group_coherence * group proportion)
    #    or if your code uses the `avg_distance` column, adapt accordingly:
    coherence = weighted_coherence(results_distance, group_proportions, key)
    #println(" \n")
    #println("coherence: ", coherence)
    # 5) Compute the consensus ranking for each group
    #    (assumes `consensus_for_group(subdf)` returns (consensus_ranking = ...))
    grouped_consensus = combine(grouped_df) do subdf
        consensus_for_group(subdf)
    end

    # 6) Compute the overall divergence measure
    divergence = overall_divergences(grouped_consensus, df, key)
    #println(" \n")
    #println("divergence: ", divergence)
    return coherence, divergence
end




##############################################################################
#  Extreme–weighted Can-Özkes-Storcken index Ψ_we
#  – linear shape  g(k) = 1 − (k−1)/(κ−1)   (β = 1)
#  – product rule  w_i(a,b) = g(e_i(a))·g(e_i(b))
#  – returns a value in [0,1]
##############################################################################

extremeness(rank::Int, m::Int) = min(rank, m + 1 - rank)

"""
    g_linear_no_cut(k, κ)
Linear decay (★): g(k) = 1 - (k-1)/κ, always positive.
"""
@generated function g_linear_no_cut(k::Int, κ::Int)
    :( 1.0 - (k - 1) / κ )
end

function w_extreme(c1::Symbol, c2::Symbol, ranking::Dict{Symbol,Int})
    m = length(ranking)
    κ = fld(m, 2)
    g1 = g_linear_no_cut(extremeness(ranking[c1], m), κ)
    g2 = g_linear_no_cut(extremeness(ranking[c2], m), κ)
    return g1 * g2                     # product rule
end

function ptilde_extreme(c1::Symbol, c2::Symbol,
                        profile::Vector{<:Dict{Symbol,Int}})
    num = 0.0;  den = 0.0
    for ranking in profile
        w = w_extreme(c1, c2, ranking)
        den += w
        ranking[c1] < ranking[c2] && (num += w)
    end
    return num / den                   # den > 0 for every pair now
end

function psi_we(profile::Vector{<:Dict{Symbol,Int}})
    @assert !isempty(profile)
    cand_pairs = combinations(collect(keys(profile[1])), 2)
    s = 0.0
    for (c1, c2) in cand_pairs
        p̃ = ptilde_extreme(c1, c2, profile)
        s += 1.0 - abs(2p̃ - 1)
    end
    return s / length(cand_pairs)
end






##############################################################################
#  Bottom-weighted Can-Özkes-Storcken index  Ψ_wb
#  • linear “bottom intensity”  h(k) = (k − 1)/(m − 1)   (top = 0, bottom = 1)
#  • product rule  w_i(a,b) = h(rank_i(a)) · h(rank_i(b))
#  • pairs with total weight 0 are ignored in the average
##############################################################################



# ---------- 1. bottom intensity for a single alternative --------------------

"""
    h_bottom(rank, m) -> Float64

Linear weight favouring the bottom of the ballot:

    h(k) = (k - 1)/(m - 1)         for 1 ≤ k ≤ m,  m ≥ 2
    h(1) = 0   (top),   h(m) = 1   (bottom)
"""
h_bottom(rank::Int, m::Int) = (rank - 1) / (m - 1)

# ---------- 2. pair weight (product rule) -----------------------------------

"""
    w_bottom(c1, c2, ranking) -> Float64

Weight that voter `ranking` assigns to the unordered pair {c1,c2}.
`ranking` is a Dict{Symbol,Int} mapping candidate → rank.
"""
function w_bottom(c1::Symbol, c2::Symbol, ranking::Dict{Symbol,Int})
    m  = length(ranking)
    h1 = h_bottom(ranking[c1], m)
    h2 = h_bottom(ranking[c2], m)
    return h1 * h2
end

# ---------- 3. weighted comparison proportion -------------------------------

"""
    ptilde_bottom(c1, c2, profile) -> Union{Nothing, Float64}

Weighted proportion of voters preferring `c1` to `c2`, using the bottom rule.
Returns `nothing` if the total weight for the pair is zero (then the pair
is skipped in the polarisation average).
"""
function ptilde_bottom(c1::Symbol, c2::Symbol,
                       profile::Vector{<:Dict{Symbol,Int}})
    num = 0.0
    den = 0.0
    for ranking in profile
        w = w_bottom(c1, c2, ranking)
        den += w
        ranking[c1] < ranking[c2] && (num += w)
    end
    return den == 0.0 ? nothing : num / den
end

# ---------- 4. Ψ_wb  ---------------------------------------------------------

"""
    psi_wb(profile) -> Float64

Bottom-weighted polarisation index Ψ_wb.
`profile` is a vector of rankings (Dict{Symbol,Int}), one per voter.

* Range: 0 ≤ Ψ_wb ≤ 1
* Ψ_wb = 0 for unanimous ballots
* Ψ_wb = 1 for the equiprobable profile (all m! permutations once)
"""
function psi_wb(profile::Vector{<:Dict{Symbol,Int}})
    @assert !isempty(profile) "profile must contain at least one ranking"

    candidates      = collect(keys(profile[1]))
    candidate_pairs = collect(combinations(candidates, 2))

    sum_terms = 0.0
    counted   = 0
    for (c1, c2) in candidate_pairs
        p̃ = ptilde_bottom(c1, c2, profile)
        p̃ === nothing && continue             # skip weight-zero pair
        sum_terms += 1.0 - abs(2p̃ - 1)
        counted   += 1
    end
    return counted == 0 ? 0.0 : sum_terms / counted
end



"""
    kendall_tau_perm(p1::AbstractVector{T}, p2::AbstractVector{T}) where T

Given two permutations of the same candidate set (each an ordered collection of candidates),
computes the Kendall tau distance (number of discordant pairs).

This function constructs an index mapping for `p2`, then maps `p1` accordingly,
and counts the number of inversions in the resulting array.
"""
function kendall_tau_perm(p1, p2::AbstractVector{T}) where T
    # Build a mapping: candidate -> position in p2.
    pos = Dict{T,Int}()
    for (i, cand) in enumerate(p2)
         pos[cand] = i
    end
    # Map each candidate in p1 to its position in p2.
    mapped = [ pos[cand] for cand in p1 ]

    # Count inversions in the mapped array.
    d = 0
    n = length(mapped)
    for i in 1:(n-1)
        for j in i+1:n
            if mapped[i] > mapped[j]
                d += 1
            end
        end
    end
    return d
end






function w(c1, c2, ranking)
    1/(ranking[c1] + ranking[c2])
end




"""
    ptilde(c1, c2, profile)

Compute the weighted proportion of voters who prefer `c1` to `c2`:

  \tilde{p}_{c1,c2} = (∑ over i of w(c1, c2, i) * 1{c1 ≻_i c2}) / (∑ over i of w(c1, c2, i))

where `w` is a weighting function, and `1{c1 ≻_i c2}` is an indicator
that voter `i` ranks `c1` strictly above `c2`.
"""
function ptilde(c1, c2, profile::Vector{<:Dict{Symbol, Int}})
    numerator   = 0.0
    denominator = 0.0
    for ranking in profile
        local_weight = w(c1, c2, ranking)
        denominator += local_weight
        if ranking[c1] < ranking[c2]
            numerator += local_weight
        end
    end
    return denominator == 0 ? 0.0 : (numerator / denominator)
end


# The weighted Psi measure using 1 - |2 p_ab - 1|
function weighted_psi_symmetric(profile::Vector{<:Dict{Symbol,Int}})
    # Collect all candidates from the first ranking (assuming all share the same keys)
    candidates = collect(keys(profile[1]))
    # All unordered pairs of distinct candidates
    candidate_pairs = collect(combinations(candidates, 2))

    measure = 0.0
    for (c1, c2) in candidate_pairs
        p_ab = ptilde(c1, c2, profile)          # Weighted proportion c1 ≻ c2
        measure += (1.0 - abs(2.0 * p_ab - 1.0)) # 1 - |2 p_ab - 1|
    end

    return measure / length(candidate_pairs)     # Normalize by number of pairs
end





# from summary_measures.jl



"""
    summarize_measures(measure_dict::Dict)

Given a nested dictionary of the form:
  :measure => :variant => Vector{Float64}
computes summary stats (mean, median, mode, min, max, Q1, Q3) for each measure/variant.

Returns a long-format DataFrame with columns:
  :measure, :variant, :stat, :value
"""
function summarize_measures(measure_dict::Dict)
    rows = NamedTuple[]

    for (measure, variant_dict) in measure_dict
        for (variant, vec) in variant_dict
            # Compute stats
            μ     = mean(vec)
            med   = median(vec)
            q1, q3 = quantile(vec, [0.25, 0.75])
            minv  = minimum(vec)
            maxv  = maximum(vec)
            modev = mode(vec)

            # Push all stats as separate rows
            append!(rows, [
                (measure = String(measure), variant = String(variant), stat = "mean",   value = μ),
                (measure = String(measure), variant = String(variant), stat = "median", value = med),


                (measure = String(measure), variant = String(variant), stat = "min",    value = minv),
                (measure = String(measure), variant = String(variant), stat = "max",    value = maxv),
            ])
        end
         #(measure = String(measure), variant = String(variant), stat = "mode",   value = modev),
          #      (measure = String(measure), variant = String(variant), stat = "q1",     value = q1),
           #     (measure = String(measure), variant = String(variant), stat = "q3",     value = q3),

    end

    return DataFrame(rows)
end



function summarize_measures_labeled(measure_dict::Dict, labels::Dict)
    rows = NamedTuple[]

    for (measure, variant_dict) in measure_dict
        # Lookup display label
        display_label = get(labels, measure, string(measure))

        for (variant, vec) in variant_dict
            # Compute stats
            μ     = round(mean(vec), digits = 3)
            med   = round(median(vec), digits = 3)
            q1, q3 = quantile(vec, [0.25, 0.75])
            minv  = round(minimum(vec), digits = 3)
            maxv  = round(maximum(vec), digits = 3)
            modev = mode(vec)

            # Push all stats as separate rows
            append!(rows, [
                (measure = display_label, variant = String(variant), stat = "mean",   value = μ),
                (measure = display_label, variant = String(variant), stat = "median", value = med),

                (measure = display_label, variant = String(variant), stat = "min",    value = minv),
                (measure = display_label, variant = String(variant), stat = "max",    value = maxv),
            ])
        end
    end

    return DataFrame(rows)
end




"""
    split_variant_tables(summary_df::DataFrame)

Given the long-format summary DataFrame (columns: :measure, :variant, :stat, :value),
returns a Dict with keys being variant names (Strings), and values DataFrames with:

| measure | mean | median | min | max |
"""
function split_variant_tables(summary_df::DataFrame)
    result = Dict{String, DataFrame}()

    variants = unique(summary_df.variant)

    for v in variants
        subdf = Base.filter(:variant => ==(v), summary_df)

        # Pivot stat column to wide format
        wide_df = unstack(subdf, :measure, :stat, :value)

        result[v] = wide_df
    end

    return result
end





"""
    save_variant_tables_latex(tables::Dict{String, DataFrame}, basename::String)

For each entry in `tables` (e.g., "zero", "random", "mice"), write a LaTeX file named
    "<basename>_<variant>.tex"
to the current directory.
"""
function save_variant_tables_latex(tables::Dict{String, DataFrame}, basename::String)
    for (variant, df) in tables
        filename = "$(basename)_$(variant).tex"
        open(filename, "w") do io
            pretty_table(io, df;
                backend = Val(:latex),
                tf = tf_latex_booktabs,   # ← NO COMMA HERE (this was the bug)
                alignment = :c,
                header_alignment = :c,
                title = "Imputation Variant: $(variant)")
        end
        println("Saved LaTeX table to: $filename")
    end
end




function save_sidebyside_latex(tables::Dict{String,DataFrame},
                               variants::Vector{String},
                               filename::String)

    stats     = [:mean, :median, :min, :max]               # assumed order
    stat_str  = ["mean", "median", "min", "max"]

    # 1 ── build the combined DataFrame
    base      = select(tables[variants[1]], :measure)      # first column

    for v in variants
        dfv = select(tables[v], Not(:measure))             # drop measure col
        # rename each stat column → "<variant>_<stat>"
        rename!(dfv, Symbol.(string.(v, "_", stat_str)))
        base = hcat(base, dfv)
    end

    # 2 ── build PrettyTables headers with spanners
    # first row: top spanners ("", 4 cols per variant)
    top_hdr = vcat([""; repeat(variants, inner=length(stats))]...)
    # second row: sub-headers for stats
    sub_hdr = vcat(["measure"; repeat(stat_str, outer=length(variants))]...)

    open(filename, "w") do io
        pretty_table(io, base;
            backend = Val(:latex),
            tf      = tf_latex_booktabs,    # booktabs styl
            header  = (top_hdr, sub_hdr),
            alignment = :c,
            header_alignment = :c,
            title   = "Summary Statistics by Imputation Variant")
    end
    println("Saved side-by-side LaTeX table → $filename")
end




# ======================================================================
# Taken from pipeline.jl
# ======================================================================
#
#
#

function median_neff(all_meas,
                     year::Integer,
                     scenario::AbstractString;
                     variant    = :mice,
                     m_max::Int = typemax(Int),
                     aggregate::Bool = false)

    var = Symbol(variant)                       # normalise the key

    scen_map = all_meas[year][scenario]         # m ⇒ measure ⇒ …

    # collect medians per m
    med_per_m = Dict{Int,Float64}()
    for (m, mdict) in scen_map
        m > m_max && continue
        hhi_vec = mdict[:calc_reversal_HHI][var]        # already normalised
        med_per_m[m] = median(1.0 ./ hhi_vec)           # N_eff = 1/HHI_*
    end

    return aggregate ? median(values(med_per_m)) : med_per_m
end



function enrp_table(all_meas, f3,
year::Integer, scenario::AbstractString;
variant = :mice, m_max::Int = typemax(Int))


var   = Symbol(variant)
data  = all_meas[year][scenario]              # m ⇒ measure ⇒ …
m_vec = sort([m for m in keys(data) if m ≤ m_max])

tbl = OrderedCollections.OrderedDict{Int,NamedTuple}()

for m in m_vec
    hhi_vec = data[m][:calc_reversal_HHI][var]    # already normalised
    enrp    = median(1.0 ./ hhi_vec)
    tbl[m]  = (enrp = enrp, max = factorial(big(m)) / 2)  # big() avoids overflow
end
return tbl
end





function hhi_table(all_meas,
                   f3,
                   year::Integer,
                   scenario::AbstractString;
                   variant = :mice,
                   m_max::Int = typemax(Int))

    var   = Symbol(variant)
    data  = all_meas[year][scenario]                 # m ⇒ measure ⇒ …
    m_vec = sort([m for m in keys(data) if m ≤ m_max])

    tbl = OrderedCollections.OrderedDict{Int,NamedTuple}()

    for m in m_vec
        hhi_vec = data[m][:calc_reversal_HHI][var]   # already normalised
        tbl[m]  = (
            hhi = median(hhi_vec),
            min = 2.0 / float(factorial(big(m))),    # 1 / (#pairs)
        )
    end
    return tbl
end



function polar_table(all_meas,
                     _f3,                         # unused, kept for API parity
                     year::Integer,
                     scenario::AbstractString;
                     variant    = :mice,
                     m_max::Int = typemax(Int))

    var   = Symbol(variant)
    data  = all_meas[year][scenario]                   # m ⇒ measure ⇒ …
    m_vec = sort([m for m in keys(data) if m ≤ m_max])

    tbl = OrderedDict{Int,NamedTuple}()

    for m in m_vec
        md  = data[m]

        psi_vec  = md[Symbol("Ψ")][var]
        rhhi_vec = md[:fast_reversal_geometric][var]
        r_vec    = md[:calc_total_reversal_component][var]

        tbl[m] = (
            psi  = median(psi_vec),
            rhhi = median(rhhi_vec),
            r    = median(r_vec),
        )
    end
    return tbl
end





function build_polar_tables(all_meas)
    # --- collectors ---
    by_combo = Dict{Tuple{Any,Any,Any}, OrderedDict{Int,NamedTuple{(:D,:C,:G),NTuple{3,Union{Missing,Float64}}}}}()
    flat_rows = NamedTuple[]

    # --- traverse the nested structure ---
    for (cset, level_m) in all_meas
        for (m, level_var) in level_m
            for (var, level_variant) in level_var
                for (variant, level_measure) in level_variant
                    # pull vectors (or mark missing if absent)
                    D_vec = get(level_measure, :D, nothing)
                    C_vec = get(level_measure, :C, nothing)
                    G_vec = get(level_measure, :G, nothing)

                    D_med = D_vec === nothing ? missing : median(D_vec)
                    C_med = C_vec === nothing ? missing : median(C_vec)
                    G_med = G_vec === nothing ? sqrt(C_med * D_med) : median(G_vec)

                    # push to flat table
                    push!(flat_rows, (
                        cset    = cset,
                        m       = m,
                        variable= var,
                        variant = variant,
                        D = D_med,
                        C = C_med,
                        G = G_med,
                    ))

                    # fill the polar_table-like view grouped by (cset, variable, variant)
                    key = (cset, var, variant)
                    od = get!(by_combo, key, OrderedDict{Int, NamedTuple{(:D,:C,:G),NTuple{3,Union{Missing,Float64}}}}())
                    od[m] = (D = D_med, C = C_med, G = G_med)
                end
            end
        end
    end

    # ensure stable ordering by m inside each OrderedDict
    for (_, od) in by_combo
        for (k, v) in sort(collect(od); by=first)
            delete!(od, k)  # reinsert in sorted order
            od[k] = v
        end
    end

    return (by_combo = by_combo, flat = flat_rows)
end



function D_ENRP(HHI::Float64,m)

 if m == 2
        error("D_ENRP is undefined for m = 2 (division by zero in normalization).")
end

max_enrp = factorial(m)/2
enrp = 1/HHI
d_enrp = (enrp-1) / (max_enrp -1 )
return d_enrp
end

function D_ENRP(HHIs::Vector{Float64},m)


d_enrps = D_ENRP.(HHIs,m)
return d_enrps
end


function D_ENRP(measure_container, cset, m, variant = :mice)
    HHIS = measure_container[cset][m][:calc_reversal_HHI][variant]
    return D_ENRP(HHIS, m)
end





function summarize_D_ENRP_over_m(measures, cset::String;
                                  ms=2:6, variant=:mice, band=(0.1, 0.9))
    qlo, qhi = band
    meds = Float64[]; lows = Float64[]; highs = Float64[]
    for m in ms
        if haskey(measures, cset) &&
           haskey(measures[cset], m) &&
           haskey(measures[cset][m], :calc_reversal_HHI) &&
           haskey(measures[cset][m][:calc_reversal_HHI], variant)
            HHIs = measures[cset][m][:calc_reversal_HHI][variant]
            dvals = D_ENRP(HHIs, m)
            push!(meds, quantile(dvals, 0.5))
            push!(lows, quantile(dvals, qlo))
            push!(highs, quantile(dvals, qhi))
        else
            push!(meds, NaN); push!(lows, NaN); push!(highs, NaN)
        end
    end
    return (ms = collect(ms), med = meds, low = lows, high = highs)
end


function trace_D_ENRP(measures_list, cset_list, ms, variant)
    for (year_idx, meas) in enumerate(measures_list)
        for cset in cset_list
            for m in ms
                if haskey(meas, cset) && haskey(meas[cset], m)
                    vals = D_ENRP(meas, cset, m, variant)
                    bad_idx = findall(x -> ismissing(x) || isnan(x), vals)
                    if !isempty(bad_idx)
                        @warn "Found NaN/missing in D_ENRP" year_idx=year_idx cset=cset m=m indices=bad_idx bad_values=vals[bad_idx]
                    end
                end
            end
        end
    end
end


function plot_D_ENRP_by_m(
    measures_list::Vector,          # [measures2006, measures2018, measures2022]
    cset_list::Vector{String},      # ["lula_alckmin", "main_four", "lula_bolsonaro"]
    labels::Vector{String};         # ["2006", "2018", "2022"]
    ms = 3:5,
    variant::Symbol = :mice,
    band_q = (0.10, 0.90),          # 10–90% band
    marker_size = 9,
    line_width = 2.0
)
    @assert length(measures_list) == length(cset_list) == length(labels)

    fig = Figure(resolution = (900, 520))

    ax = Axis(fig[1, 1];
        xlabel = "Number of alternatives (m)",
        ylabel = L"D_{\mathrm{ENRP}} \text{  (diversity of reversed pairs)}",
        title  = "Effective diversity of reversed pairs across years",
        xminorticksvisible = true,
        xticks = ms,
        yticks = 0:0.1:1,
    )
    # set y limits AFTER creation (old Makie doesn’t accept ylimits=)
    ylims!(ax, 0.0, 1.02)

    # palette (works across Makie versions)
    cols = try
        Makie.wong_colors()
    catch
        distinguishable_colors(8)
    end

    lines_for_legend = AbstractPlot[]

    for (i, (meas, cset, lab)) in enumerate(zip(measures_list, cset_list, labels))
        xs  = Int[]
        med = Float64[]
        lo  = Float64[]
        hi  = Float64[]

        for m in ms
            haskey(meas, cset)      || continue
            haskey(meas[cset], m)   || continue
            denrp_vals = D_ENRP(meas, cset, m, variant)
            isempty(denrp_vals)     && continue

            push!(xs,  m)
            push!(med, quantile(denrp_vals, 0.5))
            push!(lo,  quantile(denrp_vals, band_q[1]))
            push!(hi,  quantile(denrp_vals, band_q[2]))
        end

        isempty(xs) && continue
        col = cols[mod1(i, length(cols))]

        band!(ax, xs, lo, hi, color = (col, 0.18))
        ln = lines!(ax, xs, med, color = col, linewidth = line_width)
        scatter!(ax, xs, med, color = col, markersize = marker_size)

        push!(lines_for_legend, ln)
    end

    Legend(fig[1, 2], lines_for_legend, labels, "Year"; framevisible = false)
    fig
end



function build_D_ENRP_median_table(D_ENRP_func, measures_list, cset_list, years; m_values=3:6)
    rows = DataFrame(Year = years)
    for m in m_values
        medians = [median(D_ENRP_func(measures, cset, m))
                   for (measures, cset) in zip(measures_list, cset_list)]
        rows[!, Symbol("m=$(m)")] = medians
    end
    return rows
end



function build_ENRP_median_table(measures_list, cset_list, years; m_values=3:6, variant=:mice)
    rows = DataFrame(Year = years)
    for m in m_values
        medians = [median(1 ./ measures[cset][m][:calc_reversal_HHI][variant])
                   for (measures, cset) in zip(measures_list, cset_list)]
        rows[!, Symbol("m=$(m)")] = medians
    end
    return rows
end





    function compare_demographic_across_scenarios(
        all_gm,
        f3,
        scenario_vec::Vector{Tuple{Int,String}};
        demographic::String,
        variant::Symbol           = :mice,
        measures::Vector{Symbol}  = [:C, :D, :G],
        palette::Vector           = Makie.wong_colors()[1:3],
        n_yticks::Int             = 5,
        base_width::Int           = 400,
        base_height::Int          = 360
)

    # ── colour helper (lighten / darken without unqualified imports) ──
    ΔL = Dict(:C=>0, :D=>+20, :G=>-15)
    function shade(rgb::Colors.RGB, meas)
        lch = convert(Colors.LCHab, rgb)
        newL = clamp(lch.l + ΔL[meas], 0, 100)
        convert(Colors.RGB, Colors.LCHab(newL, lch.c, lch.h))
    end

    base_rgbs = Colors.RGB.(palette[1:length(measures)])
    measure_cols = Dict(measures[i] => shade(base_rgbs[i], measures[i])
                        for i in eachindex(measures))

    # ── slice metadata from the first scenario ────────────────────────
    y0, s0   = scenario_vec[1]
    gm0      = all_gm[y0][s0]
    m_vals   = sort(collect(keys(gm0)))           # Int
    xs_m     = Float32.(m_vals)
    n_panels = length(scenario_vec)
    demo_sym = Symbol(demographic)

    n_boot = length(gm0[first(m_vals)][Symbol(demographic)][variant][:C])

    # helper for candidate label
    candidate_label(y, s) = begin
        cfg  = f3[y].cfg
        scenobj = only(filter(t -> t.name == s, cfg.scenarios))
        describe_candidate_set(_full_candidate_list(cfg, f3[y].data, scenobj))
    end

    # ── global y-limits ───────────────────────────────────────────────
    global_all = Float32[]
    for (yr, sc) in scenario_vec, meas in measures, m in m_vals
        v = all_gm[yr][sc][m][Symbol(demographic)][variant]
        arr = meas === :G ? sqrt.(v[:C] .* v[:D]) : v[meas]
        append!(global_all, Float32.(arr))
    end
    y_min, y_max = extrema(global_all)
    y_ticks = collect(range(y_min, y_max; length = n_yticks))

    # ── figure scaffold (3 rows) ──────────────────────────────────────
    fig = Figure(resolution = (base_width*n_panels, base_height),
                 layout = (3, n_panels))
    rowgap!(fig.layout, 20); colgap!(fig.layout, 30)

    header_txt = "$demographic • number of alternatives = $(first(m_vals))…$(last(m_vals)) • $n_boot pseudo-profiles"
    fig[1, 1:n_panels] = Label(fig, header_txt; fontsize = 22, halign = :center)

    legend_handles = Lines[]; legend_labels = String[]

    # ── panel loop ────────────────────────────────────────────────────
    for (i, (yr, sc)) in enumerate(scenario_vec)
        gm_slice = all_gm[yr][sc]
        wrapped_title = join(TextWrap.wrap("Year $yr — $(candidate_label(yr, sc))"; width = 50))

        ax = Axis(fig[2, i];
                  title     = wrapped_title,
                    titlefont = "sans",
                  titlesize = 14,
                  titlegap  = 8,
                  xlabel    = "number of alternatives",
                  ylabel    = "value",
                  xticks    = (xs_m, string.(m_vals)),
                  limits    = (nothing, (y_min, y_max)),
                  yticks    = (y_ticks, string.(round.(y_ticks; digits=2))))

        for meas in measures
            col = measure_cols[meas]
            meds = Float32[]; q25s = Float32[]; q75s = Float32[]; p05s = Float32[]; p95s = Float32[]

            for m in m_vals
                v = gm_slice[m][Symbol(demographic)][variant]
                vals32 = Float32.(meas === :G ? sqrt.(v[:C] .* v[:D]) : v[meas])

                push!(meds, median(vals32))
                push!(q25s, quantile(vals32, 0.25f0)); push!(q75s, quantile(vals32, 0.75f0))
                push!(p05s, quantile(vals32, 0.05f0)); push!(p95s, quantile(vals32, 0.95f0))
            end

            band!(ax, xs_m, p05s, p95s; color = (col, 0.12), linewidth = 0)
            band!(ax, xs_m, q25s, q75s; color = (col, 0.25), linewidth = 0)
            ln = lines!(ax, xs_m, meds; color = col, linewidth = 2)

            if i == 1
                push!(legend_handles, ln)
                push!(legend_labels, string(meas))
            end
        end
    end

    fig[3, 1:n_panels] = Legend(fig, legend_handles, legend_labels;
                                orientation = :horizontal,
                                framevisible = false,
                                halign = :center)

    resize_to_layout!(fig)
    return fig
end

"""
    repair_bad_profiles!(year, bad, profiles_year, iy, cfg)

For each entry in `bad` returned by `audit_profiles_year`, rebuild the file.
Skips :missing_file/:missing_var/:read_error uniformly.
"""
function repair_bad_profiles!(year::Int,
                              bad::Vector{<:NamedTuple},
                              profiles_year,
                              iy::ImputedYear,
                              cfg)
    for b in bad
        scen = b.scenario; m = b.m
        ps = profiles_year[scen][m]
        @info "Rebuilding $(basename(b.file))  (scen=$(scen), m=$(m), var=$(b.variant), rep=$(b.rep))"
        rebuild_profile_file!(ps, iy, cfg, b.variant, b.rep)
    end
    return nothing
end

function repair_bad_profiles!(year::Int, bad, iy::ImputedYear, cfg;
                              dir=PROFILES_DATA_DIR)
    idx = load_profiles_index(year; dir)
    for b in bad
        ps = idx[b.scenario][b.m]
        @info "Rebuilding $(basename(b.file))  (scen=$(b.scenario), m=$(b.m), var=$(b.variant), rep=$(b.rep))"
        rebuild_profile_file!(ps, iy, cfg, b.variant, b.rep)  # will overwrite ps.paths[var][rep]
    end
    nothing
end



"""
    load_profiles_index(year; dir = PROFILES_DATA_DIR)

Load and return the profiles index written by
`generate_profiles_for_year_streamed_from_index`.
"""
function load_profiles_index(year::Int; dir::AbstractString = PROFILES_DATA_DIR)
    idxfile = joinpath(dir, "profiles_index_$(year).jld2")
    isfile(idxfile) || error("Index not found: $idxfile")
    return JLD2.load(idxfile, "result")  # scenario => m => ProfilesSlice
end



"""
    audit_profiles_year(year; dir = PROFILES_DATA_DIR)

Try reading every encoded profile DF referenced from
`profiles_index_YEAR.jld2`. Returns a Vector of NamedTuples with
(problem=:missing_file|:missing_var|:read_error, and file/scen/m/var/rep/err).
"""
function audit_profiles_year(year::Int; dir::AbstractString = PROFILES_DATA_DIR)
    idxfile = joinpath(dir, "profiles_index_$(year).jld2")
    isfile(idxfile) || error("Index not found: $idxfile")
    profiles_year = JLD2.load(idxfile, "result")  # scenario ⇒ m ⇒ ProfilesSlice

    bad = NamedTuple[]
    # count how many to show a progress bar
    total = sum(length(ps.paths[var]) for (_, m_map) in profiles_year
                                   for (_, ps) in m_map
                                   for var in keys(ps.paths))
    prog = pm.Progress(total; desc = "[audit $year]", barlen = 30)

    for (scen, m_map) in profiles_year
        for (m, ps) in m_map
            for var in keys(ps.paths)
                for (i, fpath) in enumerate(ps.paths[var])
                    if !isfile(fpath)
                        push!(bad, (problem=:missing_file, file=fpath, scenario=scen,
                                    m=m, variant=var, rep=i, err=nothing))
                        pm.next!(prog); continue
                    end
                    # Open without reconstructing the whole DF to check presence of "df"
                    ok, err = true, nothing
                    try
                        JLD2.jldopen(fpath, "r") do f
                            if !haskey(f, "df")
                                ok = false
                                err = "missing variable 'df'"
                            else
                                # force a real read to trigger any reconstruction errors
                                _ = read(f, "df")
                            end
                        end
                    catch e
                        ok = false; err = e
                    end
                    if !ok
                        push!(bad, (problem = err == "missing variable 'df'" ? :missing_var : :read_error,
                                    file=fpath, scenario=scen, m=m, variant=var, rep=i, err=err))
                    end
                    pm.next!(prog)
                end
            end
        end
    end
    pm.finish!(prog)
    return bad
end


"""
    rebuild_profile_file!(ps, iy, cfg, var, rep; dir = PROFILES_DATA_DIR)

Recomputes an encoded profile DF for one (variant, rep) and overwrites it.
Requires: `ps::ProfilesSlice`, `iy::ImputedYear`, `cfg::ElectionConfig`.
"""
function rebuild_profile_file!(ps::ProfilesSlice,
                               iy::ImputedYear,
                               cfg,
                               var::Symbol,
                               rep::Int;
                               dir::AbstractString = PROFILES_DATA_DIR)
    df_imp = iy[var, rep]  # loads imputed replicate
    df = profile_dataframe(df_imp;
                           score_cols = ps.cand_list,  # or ps.cand_syms in your code
                           demo_cols  = cfg.demographics)
    compress_rank_column!(df, ps.cand_list; col = :profile)
    metadata!(df, "candidates", ps.cand_list)
    fprof = ps.paths[var][rep]
    # Safer write: avoid mmap + compression and write atomically.
    tmp = fprof * ".tmp"
    isfile(tmp) && rm(tmp; force = true)
    JLD2.jldopen(tmp, "w"; iotype = IOStream, compress = false) do f
        f["df"] = df
    end
    mv(tmp, fprof; force = true)
    return fprof
end



init_accum(vars, met_syms) =
    Dict(met => Dict(var => Float64[] for var in vars) for met in met_syms)




"""
    load_measures_for_year(year;
                          dir     = GLOBAL_MEASURE_DIR,
                          verbose = true) -> measures

Load `dir/measures_YEAR.jld2` and return the `measures` object.
Errors if missing.
"""
function load_measures_for_year(year::Int;
                                dir::AbstractString = GLOBAL_MEASURE_DIR,
                                verbose::Bool       = true)

    path = joinpath(dir, "measures_$(year).jld2")
    isfile(path) || error("No global measures file for $year at $path")
    measures = nothing
    @load path measures
    verbose && @info "Loaded global measures for year $year ← $path"
    return measures
end




"""
    save_or_load_all_measures_per_year(profiles_all;
                                      years     = nothing,
                                      dir       = GLOBAL_MEASURE_DIR,
                                      overwrite = false,
                                      verbose   = true)

Loop over each `year` in `profiles_all` (or the subset `years`) and call
`save_or_load_measures_for_year`. Returns an `OrderedDict{Int,measures}`.
"""
function save_or_load_all_measures_per_year(profiles_all;
                                            years     = nothing,
                                            dir::AbstractString = GLOBAL_MEASURE_DIR,
                                            overwrite ::Bool     = false,
                                            verbose   ::Bool     = true)

    wanted = years === nothing       ? sort(collect(keys(profiles_all))) :
             isa(years, Integer)      ? [years]              :
             sort(collect(years))

    all_measures = OrderedDict{Int,Any}()
    for yr in wanted
        if !haskey(profiles_all, yr)
            @warn "No profiles for year $yr; skipping measures."
            continue
        end
        all_measures[yr] = save_or_load_measures_for_year(
                               yr,
                               profiles_all[yr];
                               dir       = dir,
                               overwrite = overwrite,
                               verbose   = verbose)
    end

    return all_measures
end





function apply_measures_all_years(
    profiles_all::Dict{Int,Any};
    years = nothing
)::OrderedDict{Int,OrderedDict{String,OrderedDict{Int,Dict{Symbol,Dict{Symbol,Vector{Float64}}}}}}

    wanted = years === nothing       ? sort(collect(keys(profiles_all))) :
             isa(years, Integer)      ? [years]                      :
             sort(collect(years))

    all_out = OrderedDict{Int,Any}()
    for yr in wanted
        haskey(profiles_all, yr) || continue
        @info "Applying measures for year $yr"
        all_out[yr] = apply_measures_for_year(profiles_all[yr])
    end
    return all_out
end


# ────────────────────────────────────────────────────────────────────────────────
"""
    load_profiles_for_year(year;
                            dir     = PROFILE_DIR,
                            verbose = true) -> profiles

Loads `dir/profiles_YEAR.jld2` and returns the stored `profiles` object.
Errors if missing.
"""
function load_profiles_for_year(year::Int;
                                dir::AbstractString = PROFILE_DIR,
                                verbose::Bool       = true)

    path = joinpath(dir, "profiles_$(year).jld2")
    isfile(path) || error("No profiles JLD2 found for $year at $path")
    profiles = nothing
    @load path profiles
    verbose && @info "Loaded profiles for year $year ← $path"
    return profiles
end


"""
    save_or_load_all_profiles_per_year(f3, imps;
                                       years     = nothing,
                                       dir::AbstractString = PROFILE_DIR,
                                       overwrite ::Bool     = false,
                                       verbose   ::Bool     = true)

Iterate over each `year` in `f3` (or the subset `years`) and call
`save_or_load_profiles_for_year`.  Returns an `OrderedDict{Int,profiles}`.
"""
function save_or_load_all_profiles_per_year(f3,
                                            imps;
                                            years     = nothing,
                                            dir::AbstractString = PROFILE_DIR,
                                            overwrite ::Bool     = false,
                                            verbose   ::Bool     = true)

    wanted = years === nothing       ? sort(collect(keys(f3))) :
             isa(years, Integer)      ? [years]              :
             sort(collect(years))

    all_profiles = OrderedDict{Int,Any}()
    for yr in wanted
        if !haskey(f3, yr)
            @warn "No bootstrap for year $yr; skipping"
            continue
        end
        all_profiles[yr] = save_or_load_profiles_for_year(
                               yr, f3, imps;
                               dir       = dir,
                               overwrite = overwrite,
                               verbose   = verbose)
    end

    return all_profiles
end



"""
    save_or_load_all_profiles(f3, imps;
                              path         = PROFILE_FILE,
                              overwrite    = false,
                              verbose      = true,
                              years        = nothing)

If `path` already exists *and* `overwrite=false`, emits a warning,
loads the saved `profiles` object, and returns it.

Otherwise, calls `generate_all_profiles(f3, imps; years=years)`,
saves the result to `path`, and returns it.
"""
function save_or_load_all_profiles(f3, imps;
                                   path      ::AbstractString = PROFILE_FILE,
                                   overwrite ::Bool           = false,
                                   verbose   ::Bool           = true,
                                   years               = nothing)

    # ensure directory
    mkpath(dirname(path))

    if isfile(path) && !overwrite
        verbose && @warn "Profiles file already exists at $path; loading it instead of regenerating."
        profiles = nothing
        @load path profiles
        return profiles
    end

    # generate + save
    verbose && @info "Generating all profiles (this may take a while)…"
    profiles = generate_all_profiles(f3, imps; years = years)

    @save path profiles
    verbose && @info "Saved all profiles to $path"
    return profiles
end


"""
    load_all_profiles(; path = PROFILE_FILE, verbose = true) -> OrderedDict

Load the `profiles` object from disk at `path`. Throws an error if the file
does not exist.
"""
function load_all_profiles(; path    ::AbstractString = PROFILE_FILE,
                            verbose ::Bool           = true)

    isfile(path) || error("No profiles file found at $path -- run `save_or_load_all_profiles` first.")
    profiles = nothing
    @load path profiles
    verbose && @info "Loaded profiles from $path"
    return profiles
end



function generate_all_profiles(f3::OrderedDict{Int,NamedTuple},
                               imps::OrderedDict{Int,NamedTuple};
                               years = nothing)

    wanted = years === nothing       ? keys(f3) :
             isa(years, Integer)      ? [years]      :
             years

    all_profiles = OrderedDict{Int,Any}()
    for yr in sort(collect(wanted))
        f3_entry  = f3[yr]
        imps_entry = imps[yr]
        @info "Building profiles for year $yr"
        all_profiles[yr] = generate_profiles_for_year(yr, f3_entry, imps_entry)
    end
    return all_profiles
end



struct BootstrapReplicates
    cfg       :: ElectionConfig
    timestamp :: DateTime
end

bootfile(bc::ElectionConfig) =
    joinpath(INT_DIR, "boot_$(bc.year).jld2")

function save_bootstraps(br::BootstrapReplicates)
    @save bootfile(br.cfg) br
    br
end

function load_bootstraps(bc::ElectionConfig)::BootstrapReplicates
    @load bootfile(bc) br
    return br
end



"""
    save_bootstrap(cfg; dir = INT_DIR, overwrite = false, quiet = false)
        → NamedTuple{(:path,:data,:cached), ...}

Ensure a weighted-bootstrap exists for `cfg`:

  • If `dir/boot_YEAR.jld2` is missing *or* `overwrite=true`, build the
    bootstrap with `weighted_bootstrap(cfg)` and write it to disk.

  • Otherwise reuse the cached file, **loading the replicates** so that
    `.data` is never `nothing`.

Returned fields
---------------
| field   | meaning                                   |
|---------|-------------------------------------------|
| `path`  | full path to the `.jld2` file             |
| `data`  | the `reps` object (always in memory)      |
| `cached`| `true` if we reused an existing file      |
"""
function save_bootstrap(cfg::ElectionConfig;
                        dir::AbstractString = INT_DIR,
                        overwrite::Bool = false,
                        quiet::Bool = false)

    path = joinpath(dir, "boot_$(cfg.year).jld2")

    # ------------------ cache hit ------------------
    if !overwrite && isfile(path)
        !quiet && @warn "Reusing cached bootstrap at $(path); loading into memory"
        reps = nothing
        @load path reps                           # brings `reps` back
        return (path = path, data = reps, cached = true)
    end

    # ------------------ (re)build ------------------
    reps = weighted_bootstrap(cfg)                # heavy call
    @save path reps cfg
    !quiet && @info "Saved bootstrap for year $(cfg.year) → $(path)"
    return (path = path, data = reps, cached = false)
end



function add_imputation_variants_to_bts(bt::NamedTuple;
most_known_candidates::Vector{String}=String[])

reps          = bt.data                       # Vector{DataFrame}
cfg           = bt.cfg
B             = length(reps)

variants = Dict{Symbol, Vector{DataFrame}}(
    :zero   => Vector{DataFrame}(undef, B),
    :random => Vector{DataFrame}(undef, B),
    :mice   => Vector{DataFrame}(undef, B),
)

for (i, df) in enumerate(reps)
    imp = imputation_variants(df,
                              cfg.candidates,
                              cfg.demographics;
                              most_known_candidates)
    variants[:zero][i]   = imp.zero
    variants[:random][i] = imp.random
    variants[:mice][i]   = imp.mice
end

return (data = variants, cfg = cfg, path = bt.path)
end



function impute_and_save(bt::NamedTuple;
                         dir::AbstractString = INT_DIR,
                         overwrite::Bool     = false,
                         most_known_candidates::Vector{String} = String[])

    year  = bt.cfg.year
    path  = joinpath(dir, "boot_imp_$(year).jld2")

    if !overwrite && isfile(path)
        @info "Using cached imputed bootstrap at $(path)"
        return path
    end

    imp   = add_imputation_variants_to_bts(bt; most_known_candidates)
    @save path imp
    @info "Saved imputed bootstrap for year $(year) → $(path)"

    # explicit cleanup
    imp = nothing
    GC.gc()

    return path
end



function load_all_imputed_bootstraps(; years  = nothing,
                                     dir::AbstractString = IMP_DIR,
                                     quiet::Bool = false)

    # 1 — discover candidate files ------------------------------------------------
    allfiles = readdir(dir; join = true)
    paths = filter(p -> startswith(basename(p), IMP_PREFIX) &&
                    endswith(p, ".jld2"), allfiles)

    isempty(paths) && error("No imputed bootstrap files found in $(dir)")

    # 2 — decide which years we actually want ------------------------------------
    wanted = years === nothing       ? nothing :
             isa(years,Integer)      ? Set([years]) :
             Set(years)

    # 3 — load, build OrderedDict -------------------------------------------------
    out = OrderedCollections.OrderedDict{Int,NamedTuple}()

    for p in sort(paths)                           # alphabetical == chronological
        yr = parse(Int, splitext(basename(p)[length(IMP_PREFIX)+1:end])[1])
        (wanted !== nothing && !(yr in wanted)) && continue

        imp = nothing
        @load p imp
        !quiet && @info "Loaded imputed bootstrap $(yr) ← $(p)"
        out[yr] = imp
    end

    return out
end

# ---- Unused plotting helpers from plotting_bts.jl ----
function boxplot_bootstraps(d;
        variants::Vector{String} = ["zero","random","mice"],
        ncols::Int = 3,
        nrows::Int = 2,
        figsize_px::Tuple = (1200,800))

    # helper: map variant => 1,2,3 …
    variant_idx(v) = findfirst(==(v), variants)

    measures = sort(collect(keys(d)))                # stable order
    fig      = Figure(resolution = figsize_px)

    for (i, m) in enumerate(measures)
        row = fld(i-1, ncols) + 1
        col = (i-1) % ncols + 1

        ax = Axis(fig[row, col];
                title  = string(m),
                xlabel = "imputation variant",
                ylabel = "value")

        xs = Int[]
        ys = Float32[]

        for v in variants
            vals = d[m][Symbol(v)]
            append!(xs, fill(variant_idx(v), length(vals)))
            append!(ys, Float32.(vals))
        end

        boxplot!(ax, xs, ys)
        ax.xticks = (1:length(variants), variants)
    end

    return fig
end




function boxplot_overlay(d;
    variants::Vector{String} = ["zero","random","mice"],
    palette ::NTuple = (:dodgerblue3, :darkorange2, :seagreen4),
    boxwidth::Real = 0.18,
    figsize::Tuple = (1200, 500))

    measures = sort(collect(keys(d)))          # stable order on x‑axis
nm, nv   = length(measures), length(variants)

# x‑offsets so the three boxes sit side‑by‑side at each integer tick
offsets = LinRange(-boxwidth, boxwidth, nv)

fig = Figure(resolution = figsize)
ax  = Axis(fig[1,1]; xlabel = "measure", ylabel = "value")

for (j, v) in enumerate(variants)          # one colour per variant
    xs, ys = Float32[], Float32[]

    for (i, m) in enumerate(measures)      # gather all y's for this variant
        vals = d[m][Symbol(v)]
        append!(xs, fill(i + offsets[j], length(vals)))
        append!(ys, Float32.(vals))
    end

    boxplot!(ax, xs, ys;
             color = palette[j],
             width = boxwidth * 0.9,
             label = v)
end

ax.xticks = (1:nm, string.(measures))
axislegend(ax; position = :rt)
return fig
end



function boxplot_cases_by_measure(d;
    variants::Vector{String} = ["zero","random","mice"],
    palette ::Vector = Makie.wong_colors(),
    boxwidth::Real = 0.18,
    figsize::Tuple = (1000, 500), n_alternatives =2, n_bootstrap = 5000)

measures = sort(collect(keys(d)))         # legend order
nv, nm   = length(variants), length(measures)

# offsets so nm boxes sit side‑by‑side at each variant tick
offsets = LinRange(-boxwidth, boxwidth, nm)

fig = Figure(resolution = figsize)
Label(fig[0, 1:2];
          text     = "B = $n_bootstrap • m = $n_alternatives",
          fontsize = 16,
          halign   = :center)
ax  = Axis(fig[1, 1]; xlabel = "imputation variant", ylabel = "value")

for (j, m) in enumerate(measures)         # iterate measures → colour
xs, ys = Float32[], Float32[]

for (i, v) in enumerate(variants)     # gather variant values
vals = d[m][Symbol(v)]
append!(xs, fill(i + offsets[j], length(vals)))
append!(ys, Float32.(vals))
end

col = palette[(j - 1) % length(palette) + 1]
labels = Dict(
    :calc_reversal_HHI             => "HHI",
    :calc_total_reversal_component => "R",
    :fast_reversal_geometric       => "RHHI"
)

    #= :weighted_psi_symmetric        => L"\Psi_{w}",
        :psi_we => L"\Psi_{we}",
        :psi_wb => L"\Psi_{wb}" =#
boxplot!(ax, xs, ys; color = col, 
width = boxwidth * 0.9, label = get(labels, m, string(m)))
end

ax.xticks = (1:nv, variants)
Legend(fig[1, 2], ax)   

return fig
end

 









#fig = plot_divergence_heatmap_cairo(divergence_by_religion, :D10)


function plot_divergence_heatmap_cairo(df::DataFrame, demo::Symbol;
    colormap = :viridis)

labels  = string.(df[!, demo])                               # row / col names
mat     = Matrix{Float64}(df[:, Not([demo, :proportion])])   # numeric part
n       = length(labels)

fig = Figure(resolution = (600, 600))
ax  = Axis(fig[1, 1]; aspect = DataAspect())

hm = heatmap!(ax, mat;                         # ← only the matrix
colormap    = colormap,
colorrange  = (minimum(mat), maximum(mat)),
interpolate = false)
# another option for colorrange would be colorrange =  (0.,1.)

# ticks exactly at the cell centres 1,2,…,n
ax.xticks = (1:n, labels)
ax.yticks = (1:n, labels)
ax.xticklabelrotation = π/4

ax.title  = "Pair-wise Divergence — $(demo)"
ax.xlabel = "Consensus from Group"
ax.ylabel = "Profiles from Group"

Colorbar(fig[1, 2], hm; label = "Divergence", width = 15)

return fig
end







"""
    boxplot_C_D_by_variant(stats::Dict;
                           variants = ["zero","random","mice"],
                           palette  = Makie.wong_colors(),
                           boxwidth = 0.18,
                           figsize  = (700, 400),
                           title    = "")

Produce a side-by-side box-plot of the **C** and **D** vectors stored in
`stats` (as produced by `bootstrap_group_metrics`).  
Each variant (:zero, :random, :mice) appears on the x-axis; the two measures
get different colours and horizontal offsets.

Returns a `Figure`.
"""
function boxplot_C_D_by_variant(stats::Dict;
                                variants::Vector{String}      = ["zero","random","mice"],
                                palette ::Vector              = Makie.wong_colors(),
                                boxwidth::Real                = 0.18,
                                figsize::Tuple{<:Integer,<:Integer} = (700, 400),
                                title::AbstractString         = "")

    measures = [:C, :D]                     # fixed two measures
    nm       = length(measures)
    nv       = length(variants)
    offsets  = LinRange(-boxwidth, boxwidth, nm)

    fig = Figure(resolution = figsize)
    ax  = Axis(fig[1, 1]; xlabel = "imputation variant", ylabel = "value")
    title != "" && (ax.title = title)

    for (j, m) in enumerate(measures)                    # colour loop
        xs, ys = Float32[], Float32[]
        for (i, v) in enumerate(variants)                # variant loop
            vals = stats[Symbol(v)][m]                   # Vector{Float64}
            append!(xs, fill(i + offsets[j], length(vals)))
            append!(ys, Float32.(vals))
        end
        col = palette[(j - 1) % length(palette) + 1]
        lab = m == :C ? "C (coherence)" : "D (divergence)"
        boxplot!(ax, xs, ys; color = col,
                 width = boxwidth*0.9, label = lab)
    end

    ax.xticks = (1:nv, variants)
    Legend(fig[1, 2], ax)
    return fig
end








"""
    combined_C_D_boxplots(race_stats, religion_stats, sex_stats;
                          variants  = ["zero","random","mice"],
                          titles    = ["Race (D12a)", "Religion (D10)", "Sex (D02)"],
                          palette   = Makie.wong_colors(),
                          boxwidth  = 0.18,
                          figsize   = (2100, 500))

Create a 1 × 3 panel of box-plots (one per grouping variable).  
Each panel shows the bootstrap distributions of **C** and **D** across the
imputation variants.  A single legend is placed on the right-hand side.
"""
function combined_C_D_boxplots(race_stats::Dict,
                               religion_stats::Dict,
                               sex_stats::Dict,
                               ideology_stats::Dict;
                               variants  = ["zero","random","mice"],
                               titles    = ["Race (D12a)", "Religion (D10)",
                                            "Sex (D02)",  "Ideology (Q19)"],
                               palette   = Makie.wong_colors(),
                               boxwidth  = 0.18,
                               figsize   = (1400, 900))

    stats_vec = [race_stats, religion_stats, sex_stats, ideology_stats]

    nv        = length(variants)
    measures  = [:C, :D]
    measure_labels = ["C (coherence)", "D (divergence)"]
    offsets   = LinRange(-boxwidth, boxwidth, length(measures))

    fig = Figure(resolution = figsize)

    # ── create 4 axes in a 2×2 grid ──────────────────────────────────
    axes = [Axis(fig[r, c];
                 title  = titles[(r-1)*2 + c],
                 xlabel = "imputation variant",
                 ylabel = "value")
            for r in 1:2, c in 1:2] |> vec

    # ── draw box-plots and capture two handles for legend ────────────
    legend_handles = BoxPlot[]; legend_labels = String[]

    for (p, stats) in enumerate(stats_vec)          # panel index 1–4
        ax = axes[p]

        for (j, m) in enumerate(measures)           # C / D
            xs = Float32[]; ys = Float32[]
            for (i, v) in enumerate(variants)       # zero / random / mice
                vals = stats[Symbol(v)][m]
                append!(xs, fill(i + offsets[j], length(vals)))
                append!(ys, Float32.(vals))
            end
            col = palette[(j-1) % length(palette) + 1]
            bp  = boxplot!(ax, xs, ys;
                           color = col,
                           width = boxwidth*0.9)

            if p == 1                               # take handles once
                push!(legend_handles, bp)
                push!(legend_labels, measure_labels[j])
            end
        end
        ax.xticks = (1:nv, variants)
    end

    # legend occupies the third column spanning both rows
    Legend(fig[1:2, 3], legend_handles, legend_labels)

    return fig
end





"""
    boxplot_alt_by_variant(measures_over_m;
                           variants   = ["zero","random","mice"],
                           palette    = Makie.wong_colors(),
                           boxwidth   = 0.18,
                           figsize    = (1000, 900))

`measures_over_m` is the Dict produced by `compute_measures_over_alternatives`,
mapping   m → Dict(measure → Dict(variant → Vector)).

Creates a 3×1 stacked figure (one row per variant),  
x-axis = number of alternatives, y-axis = value, coloured boxes = measures.
"""
function boxplot_alt_by_variant(measures_over_m::AbstractDict;
                                variants   ::Vector{String} = ["zero","random","mice"],
                                palette    ::Vector         = Makie.wong_colors(),
                                boxwidth   ::Real           = 0.18,
                                figsize    ::Tuple          = (1000, 900))

    # ——— gather keys --------------------------------------------------------
    ms        = sort(collect(keys(measures_over_m)))                    # e.g. [2,3,4,5]
    first_d   = first(values(measures_over_m))
    measures  = sort(collect(keys(first_d)))                            # e.g. [:C,:D,:G]

    nm, nv    = length(measures), length(variants)
    offsets   = LinRange(-boxwidth, boxwidth, nm)

    # legend labels (add more if you need)
    labels = Dict(
        :calc_reversal_HHI             => "HHI",
        :calc_total_reversal_component => "R",
        :fast_reversal_geometric       => "RHHI",
        :weighted_psi_symmetric        => L"\Psi_{w}",
        :psi_we => L"\Psi_{we}",
        :psi_wb => L"\Psi_{wb}"
    )

    # ——— figure & layout ----------------------------------------------------
    fig = Figure(resolution = figsize)
rowgap!(fig.layout, 18)          # <<<<<<  changed
colgap!(fig.layout, 8)           # <<<<<<  changed
fig[1, 2] = GridLayout()    
colsize!(fig.layout, 2, 180)     # legend column ≈180 px

    Label(fig[0, 1:2];
          text     = "Number of alternatives m = $(first(ms)) … $(last(ms))   •   bootstrap by variant",
          fontsize = 18,
          halign   = :center)

    axes = [Axis(fig[i, 1];
                 title  = variants[i],
                 xlabel = "number of alternatives",
                 ylabel = "value") for i in 1:nv]

    legend_handles = BoxPlot[]; legend_labels = AbstractString[]

    # ——— populate axes ------------------------------------------------------
    for (row, var) in enumerate(variants)
        ax = axes[row]

        for (j, meas) in enumerate(measures)
            xs = Float32[]; ys = Float32[]

            for m in ms
                vals = measures_over_m[m][meas][Symbol(var)]
                append!(xs, fill(Float32(m) + offsets[j], length(vals)))
                append!(ys, Float32.(vals))
            end

            col = palette[(j - 1) % length(palette) + 1]
            bp  = boxplot!(ax, xs, ys; color = col, width = boxwidth*0.9,
                                   label = get(labels, meas, string(meas)))

            if row == 1                      # collect legend once
                push!(legend_handles, bp)
                push!(legend_labels, get(labels, meas, string(meas)))
            end
        end
        ax.xticks = (ms, string.(ms))
    end

    Legend(fig[1:nv, 2], legend_handles, legend_labels)
    return fig
end
# Additional helpers
# nx = PyCall.pyimport("networkx")


# function _margin_graph_py(stats;
#                           title="Margin graph",
#                           digits=2,
#                           figsize=(10, 8))
# nx = PyCall.pyimport("networkx") 
#     G = nx.DiGraph()
#     edge_labels = Dict{Tuple{String,String},String}()

#     # ── nodes & edges ─────────────────────────────────────────────────────
#     for ((a,b), (μ,σ)) in stats
#         abs(μ) < 1e-12 && continue
#         G.add_node(string(a));  G.add_node(string(b))

#         from,to = μ > 0 ? (a,b) : (b,a)
#         lbl = string(round(abs(μ);digits=digits)," ± ",round(σ;digits=digits))
#         G.add_edge(string(from), string(to))
#         edge_labels[(string(from), string(to))] = lbl
#     end

#     # layout
#     pos = nx.spring_layout(G, seed=42)

#     # figure
#     plt.figure(figsize=figsize)

#     # nodes
#     nx.draw_networkx_nodes(G, pos,
#                            node_color="#DDDDDD",
#                            edgecolors="black",
#                            node_size=2500)
#     nx.draw_networkx_labels(G, pos, font_size=10)

#     # curved edges (all of them, single path)
#     curved = G.edges()
#     nx.draw_networkx_edges(G, pos,
#                            edgelist=curved,
#                            connectionstyle="arc3,rad=0.15",
#                            arrowstyle="-|>",
#                            arrows=true,
#                            arrowsize=30,
#                            edge_color="#555555",
#                            width=1.,
#                            min_source_margin=15,
#                            min_target_margin=15)

#     # edge labels (offset slightly perpendicular to edge)
#     for (u,v) in curved
#         x1,y1 = pos[u]
#         x2,y2 = pos[v]
#         xm, ym = (x1+x2)/2, (y1+y2)/2           # midpoint

#         # perpendicular offset
#         dx, dy = y2 - y1, -(x2 - x1)
#         norm = sqrt(dx^2 + dy^2) + 1e-9
#         dx, dy = dx/norm, dy/norm
#         xm += 0.05*dx;   ym += 0.05*dy          # nudge

#         plt.text(xm, ym, edge_labels[(u,v)],
#                  fontsize=9, ha="center", va="center", color="black")
#     end

#     plt.title(title)
#     plt.axis("off")
#     plt.tight_layout()
#     return plt.gcf()
# end





#= 

function lines_group_measures_over_m(
    stats_by_m;                                   # Dict{m → …}  from your pipeline
    demographics::Vector{Symbol},
    variants::Vector{String} = ["zero","random","mice"],
    measures::Vector{Symbol} = [:C,:D,:G],
    m_values::Vector{Int}    = sort(collect(keys(stats_by_m))),
    palette                  = Makie.wong_colors(),
    maxcols::Int             = 3,
    n_yticks::Int            = 5,                # total y-ticks incl. min & max
    figsize                  = (300 * min(maxcols, length(demographics)),
                                300 * ceil(Int, length(demographics)/maxcols))
)

    # colour per measure, line-style per variant
    measure_cols   = Dict(measures[i] => palette[i] for i in eachindex(measures))
    variant_styles = Dict("zero"=>:solid, "random"=>:dash, "mice"=>:dot)

    # grid layout
    n_demo = length(demographics)
    ncol   = min(maxcols, n_demo)
    nrow   = ceil(Int, n_demo / ncol)
    fig    = Figure(resolution = figsize)
    rowgap!(fig.layout, 24); colgap!(fig.layout, 24)

    # legend collectors
    legend_handles = Any[]
    legend_labels  = String[]

    for (idx, demo) in enumerate(demographics)
        r, c = fldmod1(idx, ncol)

        ax = Axis(fig[r, c];
                  title  = string(demo),
                  xlabel = "number of alternatives",
                  ylabel = "value",
                  xticks = (m_values, string.(m_values)))

        # gather values to compute min/max later
        allvals = Float64[]

        for meas in measures, var in variants
            data_per_m = [stats_by_m[m][demo][Symbol(var)][meas] for m in m_values]
            append!(allvals, vcat(data_per_m...))

            meds = mean.(data_per_m)
            q25s = map(x -> quantile(x, 0.25), data_per_m)
            q75s = map(x -> quantile(x, 0.75), data_per_m)

            col = measure_cols[meas]
            sty = variant_styles[var]

            band!(ax, m_values, q25s, q75s; color = (col, 0.20), linewidth = 0)
            ln  = lines!(ax, m_values, meds; color = col, linestyle = sty, linewidth = 2)

            if idx == 1
                push!(legend_handles, ln)
                push!(legend_labels, "$(meas) • $var")
            end
        end

        # ---- add min & max to y-ticks ----------------------------------
        y_min, y_max = minimum(allvals), maximum(allvals)
        ticks = range(y_min, y_max; length = n_yticks) |> collect
        labels = string.(round.(ticks; digits = 3))
        ax.yticks[] = (ticks, labels)                 # overwrite ticks
    end

    Legend(fig[1:nrow, ncol+1], legend_handles, legend_labels; tellheight = false)
    resize_to_layout!(fig)
    return fig
end =#


#= function lines_group_measures_over_m(
    stats_by_m;
    year,
    demographics::Vector{Symbol},
    variants::Vector{String} = ["zero","random","mice"],
    measures::Vector{Symbol} = [:C,:D,:G],
    m_values::Vector{Int}    = sort(collect(keys(stats_by_m))),
    candidate_label::String  = "",
    palette                  = Makie.wong_colors(),
    maxcols::Int             = 3,
    n_yticks::Int            = 5,
    figsize                  = (300 * min(maxcols, length(demographics)),
                                300 * ceil(Int, length(demographics)/maxcols)),
)

    # ——— metadata for the title ————————————————————————————————
    first_m      = m_values[1]
    first_demo   = demographics[1]
    first_var    = variants[1]
    first_meas   = measures[1]
    n_boot       = length(stats_by_m[first_m][first_demo][Symbol(first_var)][first_meas])
    first_m, last_m = first(m_values), last(m_values)
    title_txt    = "Year = $(year) • $(n_boot) bootstraps • number of alternatives = $first_m … $last_m"
    subtitle_txt = isempty(candidate_label) ? "" : candidate_label

    # ——— colour & style dictionaries ————————————————————————————
    measure_cols   = Dict(measures[i] => palette[i] for i in eachindex(measures))
    variant_styles = Dict("zero"=>:solid, "random"=>:dash, "mice"=>:dot)

    # ——— layout bookkeeping (extra rows for title / subtitle) ————
    n_demo      = length(demographics)
    ncol        = min(maxcols, n_demo)
    nrow        = ceil(Int, n_demo / ncol)
    extra_rows  = 1 + (!isempty(subtitle_txt) ? 1 : 0)     # title [+ subtitle]
    row_shift   = extra_rows                               # axes start after these

    fig = Figure(resolution = figsize)
    rowgap!(fig.layout, 24);  colgap!(fig.layout, 24)

    # figure-level labels
    fig[1, 1:ncol] = Label(fig, title_txt;  fontsize = 20, halign = :left)
    if !isempty(subtitle_txt)
        fig[2, 1:ncol] = Label(fig, subtitle_txt; fontsize = 14, halign = :left)
    end

    # ——— legend collectors ————————————————————————————————
    legend_handles = Any[]
    legend_labels  = String[]

    # ——— main loop ————————————————————————————————————————————
    for (idx, demo) in enumerate(demographics)
        r, c = fldmod1(idx, ncol)                 # 1-based grid coords for demo
        ax = Axis(fig[r + row_shift, c];
                  title  = string(demo),
                  xlabel = "number of alternatives",
                  ylabel = "value",
                  xticks = (m_values, string.(m_values)))

        # collect data to fix y-ticks per panel
        allvals = Float64[]

        for meas in measures, var in variants
            data_per_m = [stats_by_m[m][demo][Symbol(var)][meas] for m in m_values]
            append!(allvals, vcat(data_per_m...))

            meds = mean.(data_per_m)
            q25s = map(x -> quantile(x, 0.25), data_per_m)
            q75s = map(x -> quantile(x, 0.75), data_per_m)

            col = measure_cols[meas]
            sty = variant_styles[var]

            band!(ax, m_values, q25s, q75s;
                  color = (col, 0.20), linewidth = 0)
            ln = lines!(ax, m_values, meds;
                         color = col, linestyle = sty, linewidth = 2)

            if idx == 1         # only populate legend once
                push!(legend_handles, ln)
                push!(legend_labels, "$(meas) • $var")
            end
        end

        # ——— tidy y-axis ————————————————————————————————
        y_min, y_max = extrema(allvals)
        ticks  = range(y_min, y_max; length = n_yticks) |> collect
        labels = string.(round.(ticks; digits = 3))
        ax.yticks[] = (ticks, labels)
    end

    # place legend at the right of the grid
    Legend(fig[row_shift+1 : row_shift+nrow, ncol+1],
           legend_handles, legend_labels; tellheight = false)

    resize_to_layout!(fig)
    return fig
end =#



function lines_group_measures_over_m(
    stats_by_m;                                    # Dict{m → …}
    year,                                          # Int or String
    demographics,                                  # Vector of Symbols/Strings
    variants         = ["zero","random","mice"],
    measures         = [:C,:D,:G],
    m_values         = nothing,                    # will default below
    candidate_label  = "",
    palette          = Makie.wong_colors(),
    maxcols          = 3,
    n_yticks         = 5,
    figsize          = nothing,                    # will default below
)

    # ── fill in defaults that depend on other kwargs ───────────────────
    m_values === nothing && (m_values = sort(collect(keys(stats_by_m))))
    n_demo  = length(demographics)

    if figsize === nothing
        figsize = (300 * min(maxcols, n_demo),
                   300 * ceil(Int, n_demo / maxcols))
    end

    # ── metadata for the title ─────────────────────────────────────────
    first_m, last_m = first(m_values), last(m_values)
    first_demo      = demographics[1]
    n_boot = length(stats_by_m[first_m][first_demo][Symbol(variants[1])][measures[1]])

    title_txt = "Year = $(year) • $(n_boot) pseudo-profiles • number of alternatives = $first_m … $last_m"
    subtitle_txt = isempty(candidate_label) ? "" : candidate_label

    # ── colour & style dictionaries ────────────────────────────────────
    measure_cols   = Dict(measures[i] => palette[i] for i in eachindex(measures))
    variant_styles = Dict("zero" => :solid, "random" => :dash, "mice" => :dot)

    # ── grid geometry ──────────────────────────────────────────────────
    ncol       = min(maxcols, n_demo)
    nrow       = ceil(Int, n_demo / ncol)
    extra_rows = 1 + (!isempty(subtitle_txt) ? 1 : 0)
    row_shift  = extra_rows

    # ── widen canvas if title lines are long ───────────────────────────
    est_title_px = 10 * max(length(title_txt), length(subtitle_txt))
    fig_width    = max(first(figsize), est_title_px + 60)
    fig_height   = last(figsize)
    fig          = Figure(resolution = (fig_width, fig_height))
    rowgap!(fig.layout, 24);  colgap!(fig.layout, 24)

    # ── figure-level labels ────────────────────────────────────────────
    fig[1, 1:ncol] = Label(fig, title_txt;  fontsize = 20, halign = :left)
    if !isempty(subtitle_txt)
        fig[2, 1:ncol] = Label(fig, subtitle_txt; fontsize = 14, halign = :left)
    end

    # ── legend collectors ──────────────────────────────────────────────
    legend_handles = Any[]
    legend_labels  = String[]

    # ── main loop over demographic panels ──────────────────────────────
    for (idx, demo) in enumerate(demographics)
        r, c = fldmod1(idx, ncol)
        ax = Axis(fig[r + row_shift, c];
                  title  = string(demo),
                  xlabel = "number of alternatives",
                  ylabel = "value",
                  xticks = (m_values, string.(m_values)))

        allvals = Float64[]

        for meas in measures, var in variants
            data_per_m = [stats_by_m[m][demo][Symbol(var)][meas] for m in m_values]
            append!(allvals, vcat(data_per_m...))

            meds = mean.(data_per_m)
            q25s = map(x -> quantile(x, 0.25), data_per_m)
            q75s = map(x -> quantile(x, 0.75), data_per_m)

            col = measure_cols[meas]
            sty = variant_styles[var]

            band!(ax, m_values, q25s, q75s; color = (col, 0.20), linewidth = 0)
            ln = lines!(ax, m_values, meds; color = col, linestyle = sty, linewidth = 2)

            if idx == 1
                push!(legend_handles, ln)
                push!(legend_labels, "$(meas) • $var")
            end
        end

        # nice y-ticks
        y_min, y_max = extrema(allvals)
        ticks  = range(y_min, y_max; length = n_yticks) |> collect
        labels = string.(round.(ticks; digits = 3))
        ax.yticks[] = (ticks, labels)
    end

    # legend at the right of the grid
    Legend(fig[row_shift + 1 : row_shift + nrow, ncol + 1],
           legend_handles, legend_labels; tellheight = false)

    resize_to_layout!(fig)
    return fig
end





const _PRETTY_MEASURE = Dict(
    :calc_reversal_HHI             => "HHI",
    :calc_total_reversal_component => "R",
    :fast_reversal_geometric       => "R HHI",
    :weighted_psi_symmetric        => "Ψ₍w₎",
    :psi_we                        => "Ψ₍we₎",
    :psi_wb                        => "Ψ₍wb₎",
    :C => "C",  :D => "D",  :G => "G"
)

function compare_global_measures_v3(
        res_dict;
        variants ::Vector{String} = ["mice"],
        palette  ::Vector         = Makie.wong_colors(),
        figsize  ::Tuple{<:Integer,<:Integer} = (1200, 850))

    @assert !isempty(variants) "variants list cannot be empty"

    scen_names = sort(collect(keys(res_dict)))
    first_res  = first(values(res_dict))

    ms        = sort(collect(keys(first_res.measures_over_m)))
    measures  = sort(collect(keys(first(values(first_res.measures_over_m)))))
    n_meas    = length(measures)

    year   = first_res.year
    n_boot = length(first(values(first(values(first_res.measures_over_m))))[Symbol(variants[1])])

    # ── grid geometry --------------------------------------------------------
    ncol = min(3, ceil(Int, sqrt(n_meas)))          # 4→2, 5–9→3
    nrow = ceil(Int, n_meas / ncol)

    fig = Figure(resolution = figsize)
    rowgap!(fig.layout, 18); colgap!(fig.layout, 22)

    fig[0, 1:ncol] = Label(fig,
        "Year $year   •   number of alternatives = $(first(ms))…$(last(ms))   •   $n_boot pseudo-profiles",
        fontsize = 20, halign = :left)

    # ── create axes ----------------------------------------------------------
    axes = Dict{Symbol,Axis}()
    for (idx, meas) in enumerate(measures)
        r = div(idx - 1, ncol) + 1
        c = mod(idx - 1, ncol) + 1
        axes[meas] = Axis(fig[r, c];
                          title  = get(_PRETTY_MEASURE, meas, string(meas)),
                          xlabel = "number of alternatives",
                          ylabel = "value",
                          xticks = (ms, string.(ms)))
    end

    # colour per scenario -----------------------------------------------------
    scen_col = Dict(name => palette[(i-1) % length(palette) + 1]
                    for (i, name) in enumerate(scen_names))

    legend_handles = Lines[]; legend_labels = String[]

    # ── drawing loop ---------------------------------------------------------
    for scen in scen_names
        colour = scen_col[scen]
        data   = res_dict[scen].measures_over_m
        first_line = nothing

        for meas in measures, var in variants
            ax   = axes[meas]
            meds = Float64[]; q25s=Float64[]; q75s=Float64[]
            p05s = Float64[]; p95s=Float64[]

            for m in ms
                vals = data[m][meas][Symbol(var)]
                push!(meds, median(vals))
                push!(q25s, quantile(vals, 0.25));  push!(q75s, quantile(vals, 0.75))
                push!(p05s, quantile(vals, 0.05));  push!(p95s, quantile(vals, 0.95))
            end

            # 90 % band (transparent)
            band!(ax, ms, p05s, p95s; color = (colour, 0.12), linewidth = 0)
            # IQR band
            band!(ax, ms, q25s, q75s; color = (colour, 0.25), linewidth = 0)

            ln = lines!(ax, ms, meds; color = colour, linewidth = 2)
            first_line === nothing && (first_line = ln)
        end

        push!(legend_handles, first_line)
        push!(legend_labels,
              describe_candidate_set(res_dict[scen].candidate_set))
    end

    # ── stacked legend at the bottom ----------------------------------------
    legend_row = nrow + 1
    Legend(fig[legend_row, 1:ncol],
           legend_handles, legend_labels;
           orientation = :vertical, framevisible = false, halign = :left)

    resize_to_layout!(fig)      # trims any unused whitespace
    return fig
end




#= 
function compare_demographic_across_scenarios(
        results_all::Dict,
        scenario_vec::Vector{Tuple{Int,String}};
        demographic,
        variant      ::String         = "mice",
        measures     ::Vector{Symbol} = [:C,:D,:G],
        palette      ::Vector         = Makie.wong_colors()[1:3],
        n_yticks     ::Int            = 5,
        figsize      ::Tuple{<:Integer,<:Integer} = (900, 320))

    @assert length(variant) > 0  "provide one imputation variant"
    demo_sym    = Symbol(demographic)
    variant_sym = Symbol(variant)
    n_panels    = length(scenario_vec)

    # —— meta from first scenario ————————————————————————————————
    first_year, first_scen = scenario_vec[1]
    stats_any  = results_all[first_year][first_scen].group_stats
    m_values   = sort(collect(keys(stats_any)))
    n_boot     = length(stats_any[first(m_values)][demo_sym][variant_sym][measures[1]])

    # —— figure & global title ————————————————————————————————
    fig = Figure(resolution = (figsize[1], figsize[2] * n_panels))
    rowgap!(fig.layout, 20)

    fig[0, 1] = Label(fig,
        "$demographic   •   m = $(first(m_values))…$(last(m_values))   •   B = $n_boot",
        fontsize = 20)

    # —— colour helpers ————————————————————————————————————————
    ΔL = Dict(:C => 0,    # base colour
              :D => +20,  # lighter
              :G => -15)  # darker
    RGB = Colors.RGB
    LCHab = Colors.LCHab
    meas_colour(base_rgb, meas) = begin
        base_lch = convert(LCHab, base_rgb)
        convert(RGB, LCHab(clamp(base_lch.l + ΔL[meas], 0, 100),
                           base_lch.c, base_lch.h))
    end

    legend_handles = Lines[]; legend_labels = ["C", "D", "G"]

    # —— panels loop ————————————————————————————————————————
    for (idx, (year, scen)) in enumerate(scenario_vec)
        stats       = results_all[year][scen].group_stats
        cand_label  = describe_candidate_set(results_all[year][scen].candidate_set)
        base_rgb    = convert(RGB, palette[(idx-1) % length(palette) + 1])

        ax = Axis(fig[idx, 1];
                  title  = "Year $year — $cand_label",
                  xlabel = "number of alternatives",
                  ylabel = "value",
                  xticks = (m_values, string.(m_values)))

        all_vals = Float64[]   # gather values for nice y-ticks

        for meas in measures
            col = meas_colour(base_rgb, meas)

            meds = Float64[]; q25 = Float64[]; q75 = Float64[]
            p05  = Float64[]; p95 = Float64[]

            for m in m_values
                vals = stats[m][demo_sym][variant_sym][meas]
                append!(all_vals, vals)

                push!(meds, median(vals))
                push!(q25, quantile(vals, 0.25));  push!(q75, quantile(vals, 0.75))
                push!(p05, quantile(vals, 0.05));  push!(p95, quantile(vals, 0.95))
            end

            band!(ax, m_values, p05, p95; color=(col, 0.12), linewidth=0)
            band!(ax, m_values, q25, q75; color=(col, 0.25), linewidth=0)
            ln = lines!(ax, m_values, meds; color=col, linewidth=2)

            idx == 1 && push!(legend_handles, ln)
        end

        # y-ticks based on all plotted values in this panel
        y_min, y_max = extrema(all_vals)
        ticks = range(y_min, y_max; length=n_yticks) |> collect
        ax.yticks[] = (ticks, string.(round.(ticks; digits=3)))
    end

    # —— one horizontal legend ————————————————————————————————
    Legend(fig[n_panels + 1, 1],
           legend_handles, legend_labels;
           orientation = :horizontal, framevisible = false)

    resize_to_layout!(fig)
    return fig
end =#
#= 

function compare_demographic_across_scenarios(
        results_all::Dict,
        scenario_vec::Vector{Tuple{Int,String}};
        demographic,
        variant      ::String         = "mice",
        measures     ::Vector{Symbol} = [:C,:D,:G],
        palette      ::Vector         = Makie.wong_colors()[1:3],
        n_yticks     ::Int            = 5,
        figsize      ::Tuple{<:Integer,<:Integer} = (900, 320))
    @assert length(variant) > 0  "provide one imputation variant"
    demo_sym    = Symbol(demographic)
    variant_sym = Symbol(variant)
    n_panels    = length(scenario_vec)
    # —— meta from first scenario ————————————————————————————————
    first_year, first_scen = scenario_vec[1]
    stats_any  = results_all[first_year][first_scen].group_stats
    m_values   = sort(collect(keys(stats_any)))
    n_boot     = length(stats_any[first(m_values)][demo_sym][variant_sym][measures[1]])
    # —— figure & global title ————————————————————————————————
    fig = Figure(resolution = (figsize[1], figsize[2] * n_panels))
    rowgap!(fig.layout, 20)
    fig[0, 1] = Label(fig,
        "$demographic   •   m = $(first(m_values))…$(last(m_values))   •   B = $n_boot",
        fontsize = 20)
    # —— colour helpers ————————————————————————————————————————
    ΔL = Dict(:C => 0,    # base colour
              :D => +20,  # lighter
              :G => -15)  # darker
    RGB = Colors.RGB
    LCHab = Colors.LCHab
    meas_colour(base_rgb, meas) = begin
        base_lch = convert(LCHab, base_rgb)
        convert(RGB, LCHab(clamp(base_lch.l + ΔL[meas], 0, 100),
                           base_lch.c, base_lch.h))
    end
    
    # —— assign consistent colors for measures ————————————————
    measure_colors = Dict()
    for (i, meas) in enumerate(measures)
        base_rgb = convert(RGB, palette[min(i, length(palette))])
        measure_colors[meas] = meas_colour(base_rgb, meas)
    end
    
    legend_handles = Lines[]; legend_labels = ["C", "D", "G"]
    # —— panels loop ————————————————————————————————————————
    for (idx, (year, scen)) in enumerate(scenario_vec)
        stats       = results_all[year][scen].group_stats
        cand_label  = describe_candidate_set(results_all[year][scen].candidate_set)
        ax = Axis(fig[idx, 1];
                  title  = "Year $year — $cand_label",
                  titlesize = 14,
                  titlegap = 8,
                  xlabel = "number of alternatives",
                  ylabel = "value",
                  xticks = (m_values, string.(m_values)))
        all_vals = Float64[]   # gather values for nice y-ticks
        for meas in measures
            col = measure_colors[meas]  # Use consistent color for this measure
            meds = Float64[]; q25 = Float64[]; q75 = Float64[]
            p05  = Float64[]; p95 = Float64[]
            for m in m_values
                vals = stats[m][demo_sym][variant_sym][meas]
                append!(all_vals, vals)
                push!(meds, median(vals))
                push!(q25, quantile(vals, 0.25));  push!(q75, quantile(vals, 0.75))
                push!(p05, quantile(vals, 0.05));  push!(p95, quantile(vals, 0.95))
            end
            band!(ax, m_values, p05, p95; color=(col, 0.12), linewidth=0)
            band!(ax, m_values, q25, q75; color=(col, 0.25), linewidth=0)
            ln = lines!(ax, m_values, meds; color=col, linewidth=2)
            idx == 1 && push!(legend_handles, ln)
        end
        # y-ticks based on all plotted values in this panel
        y_min, y_max = extrema(all_vals)
        ticks = range(y_min, y_max; length=n_yticks) |> collect
        ax.yticks[] = (ticks, string.(round.(ticks; digits=3)))
    end
    # —— one horizontal legend ————————————————————————————————
    Legend(fig[n_panels + 1, 1],
           legend_handles, legend_labels;
           orientation = :horizontal, framevisible = false)
    resize_to_layout!(fig)
    return fig
end =#

#= 
function compare_demographic_across_scenarios(
        results_all::Dict,
        scenario_vec::Vector{Tuple{Int,String}};
        demographic,
        variant      ::String         = "mice",
        measures     ::Vector{Symbol} = [:C,:D,:G],
        palette      ::Vector         = Makie.wong_colors()[1:3],
        n_yticks     ::Int            = 5,
        figsize      ::Tuple{<:Integer,<:Integer} = (900, 320))
    @assert length(variant) > 0  "provide one imputation variant"
    demo_sym    = Symbol(demographic)
    variant_sym = Symbol(variant)
    n_panels    = length(scenario_vec)
    # —— meta from first scenario ————————————————————————————————
    first_year, first_scen = scenario_vec[1]
    stats_any  = results_all[first_year][first_scen].group_stats
    m_values   = sort(collect(keys(stats_any)))
    n_boot     = length(stats_any[first(m_values)][demo_sym][variant_sym][measures[1]])
    # —— figure & global title ————————————————————————————————
    fig = Figure(resolution = (figsize[1], figsize[2] * n_panels))
    rowgap!(fig.layout, 20)
    fig[0, 1] = Label(fig,
        "$demographic   •   m = $(first(m_values))…$(last(m_values))   •   B = $n_boot",
        fontsize = 20)
    # —— colour helpers ————————————————————————————————————————
    ΔL = Dict(:C => 0,    # base colour
              :D => +20,  # lighter
              :G => -15)  # darker
    RGB = Colors.RGB
    LCHab = Colors.LCHab
    meas_colour(base_rgb, meas) = begin
        base_lch = convert(LCHab, base_rgb)
        convert(RGB, LCHab(clamp(base_lch.l + ΔL[meas], 0, 100),
                           base_lch.c, base_lch.h))
    end
    
    # —— assign consistent colors for measures ————————————————
    measure_colors = Dict()
    for (i, meas) in enumerate(measures)
        base_rgb = convert(RGB, palette[min(i, length(palette))])
        measure_colors[meas] = meas_colour(base_rgb, meas)
    end
    
    legend_handles = Lines[]; legend_labels = ["C", "D", "G"]
    # —— panels loop ————————————————————————————————————————
    for (idx, (year, scen)) in enumerate(scenario_vec)
        stats       = results_all[year][scen].group_stats
        cand_label  = describe_candidate_set(results_all[year][scen].candidate_set)
        # Wrap long titles by splitting at commas
        wrapped_title = replace(cand_label, ", " => ",\n")
        ax = Axis(fig[idx, 1];
                  title  = "Year $year — $wrapped_title",
                  titlesize = 12,
                  titlefont = :regular,  # Remove bold formatting
                  titlegap = 5,
                  xlabel = "number of alternatives",
                  ylabel = "value",
                  xticks = (m_values, string.(m_values)))
        all_vals = Float64[]   # gather values for nice y-ticks
        for meas in measures
            col = measure_colors[meas]  # Use consistent color for this measure
            meds = Float64[]; q25 = Float64[]; q75 = Float64[]
            p05  = Float64[]; p95 = Float64[]
            for m in m_values
                vals = stats[m][demo_sym][variant_sym][meas]
                append!(all_vals, vals)
                push!(meds, median(vals))
                push!(q25, quantile(vals, 0.25));  push!(q75, quantile(vals, 0.75))
                push!(p05, quantile(vals, 0.05));  push!(p95, quantile(vals, 0.95))
            end
            band!(ax, m_values, p05, p95; color=(col, 0.12), linewidth=0)
            band!(ax, m_values, q25, q75; color=(col, 0.25), linewidth=0)
            ln = lines!(ax, m_values, meds; color=col, linewidth=2)
            idx == 1 && push!(legend_handles, ln)
        end
        # y-ticks based on all plotted values in this panel
        y_min, y_max = extrema(all_vals)
        ticks = range(y_min, y_max; length=n_yticks) |> collect
        ax.yticks[] = (ticks, string.(round.(ticks; digits=3)))
    end
    # —— one horizontal legend ————————————————————————————————
    Legend(fig[n_panels + 1, 1],
           legend_handles, legend_labels;
           orientation = :horizontal, framevisible = false)
    resize_to_layout!(fig)
    return fig
end =#



#= function compare_demographic_across_scenarios(
        results_all::Dict,
        scenario_vec::Vector{Tuple{Int,String}};
        demographic,
        variant      ::String         = "mice",
        measures     ::Vector{Symbol} = [:C,:D,:G],
        palette      ::Vector         = Makie.wong_colors()[1:3],
        n_yticks     ::Int            = 5,
        figsize      ::Tuple{<:Integer,<:Integer} = (900, 320))
    @assert length(variant) > 0  "provide one imputation variant"
    demo_sym    = Symbol(demographic)
    variant_sym = Symbol(variant)
    n_panels    = length(scenario_vec)
    # —— meta from first scenario ————————————————————————————————
    first_year, first_scen = scenario_vec[1]
    stats_any  = results_all[first_year][first_scen].group_stats
    m_values   = sort(collect(keys(stats_any)))
    n_boot     = length(stats_any[first(m_values)][demo_sym][variant_sym][measures[1]])
    # —— figure & global title ————————————————————————————————
    fig = Figure(resolution = (figsize[1], figsize[2] * n_panels))
    rowgap!(fig.layout, 20)
    fig[0, 1:2] = Label(fig,
        "$demographic   •   m = $(first(m_values))…$(last(m_values))   •   B = $n_boot",
        fontsize = 20)
    # —— colour helpers ————————————————————————————————————————
    ΔL = Dict(:C => 0,    # base colour
              :D => +20,  # lighter
              :G => -15)  # darker
    RGB = Colors.RGB
    LCHab = Colors.LCHab
    meas_colour(base_rgb, meas) = begin
        base_lch = convert(LCHab, base_rgb)
        convert(RGB, LCHab(clamp(base_lch.l + ΔL[meas], 0, 100),
                           base_lch.c, base_lch.h))
    end
    
    # —— assign consistent colors for measures ————————————————
    measure_colors = Dict()
    for (i, meas) in enumerate(measures)
        base_rgb = convert(RGB, palette[min(i, length(palette))])
        measure_colors[meas] = meas_colour(base_rgb, meas)
    end
    
    legend_handles = Lines[]; legend_labels = ["C", "D", "G"]
    # —— panels loop ————————————————————————————————————————
    for (idx, (year, scen)) in enumerate(scenario_vec)
        stats       = results_all[year][scen].group_stats
        cand_label  = describe_candidate_set(results_all[year][scen].candidate_set)
        # Wrap long titles by splitting at commas
        wrapped_title = replace(cand_label, ", " => ",\n")
        ax = Axis(fig[1, idx];
                  title  = "Year $year — $wrapped_title",
                  titlesize = 12,
                  titlefont = :regular,  # Remove bold formatting
                  titlegap = 5,
                  xlabel = "number of alternatives",
                  ylabel = "value",
                  xticks = (m_values, string.(m_values)))
        all_vals = Float64[]   # gather values for nice y-ticks
        for meas in measures
            col = measure_colors[meas]  # Use consistent color for this measure
            meds = Float64[]; q25 = Float64[]; q75 = Float64[]
            p05  = Float64[]; p95 = Float64[]
            for m in m_values
                vals = stats[m][demo_sym][variant_sym][meas]
                append!(all_vals, vals)
                push!(meds, median(vals))
                push!(q25, quantile(vals, 0.25));  push!(q75, quantile(vals, 0.75))
                push!(p05, quantile(vals, 0.05));  push!(p95, quantile(vals, 0.95))
            end
            band!(ax, m_values, p05, p95; color=(col, 0.12), linewidth=0)
            band!(ax, m_values, q25, q75; color=(col, 0.25), linewidth=0)
            ln = lines!(ax, m_values, meds; color=col, linewidth=2)
            idx == 1 && push!(legend_handles, ln)
        end
        # y-ticks based on all plotted values in this panel
        y_min, y_max = extrema(all_vals)
        ticks = range(y_min, y_max; length=n_yticks) |> collect
        ax.yticks[] = (ticks, string.(round.(ticks; digits=3)))
    end
    # —— one horizontal legend ————————————————————————————————
    Legend(fig[n_panels + 1, 1:2],
           legend_handles, legend_labels;
           orientation = :horizontal, framevisible = false)
    resize_to_layout!(fig)
    return fig
end =#

#= function compare_demographic_across_scenarios(
        results_all::Dict,
        scenario_vec::Vector{Tuple{Int,String}};
        demographic::String,
        variant      ::String         = "mice",
        measures     ::Vector{Symbol} = [:C,:D,:G],
        palette      ::Vector         = Makie.wong_colors()[1:3],
        n_yticks     ::Int            = 5,
        base_width   ::Int            = 1000,      # a bit wider
        base_height  ::Int            = 360)       # a bit taller

    demo_sym    = Symbol(demographic)
    variant_sym = Symbol(variant)
    n_panels    = length(scenario_vec)

    # pull out m_values and B from first scenario
    y0, s0   = scenario_vec[1]
    stats0   = results_all[y0][s0].group_stats
    m_values = sort(collect(keys(stats0)))
    n_boot   = length(stats0[first(m_values)][demo_sym][variant_sym][:C])

    # helper to shade base colors per measure
    ΔL = Dict(:C=>0, :D=>+20, :G=>-15)
    function meas_colour(base_rgb, meas)
        lch = convert(Colors.LCHab, base_rgb)
        newL = clamp(lch.l + ΔL[meas], 0, 100)
        return convert(Colors.RGB, Colors.LCHab(newL, lch.c, lch.h))
    end

    # precompute one color per measure (so C,D,G stay consistent)
    base_rgbs = convert.(Colors.RGB, palette[1:length(measures)])
    measure_cols = Dict(measures[i] => meas_colour(base_rgbs[i], measures[i])
                        for i in eachindex(measures))

    # create figure: one row per panel + one for legend
    fig = Figure(resolution = (base_width, base_height * (n_panels + 1)))
    rowgap!(fig.layout, 30)      # more vertical space
    colgap!(fig.layout, 20)

    # global header
    fig[0, 1] = Label(fig,
        "$demographic   •   m=$(first(m_values))…$(last(m_values))   •   B=$n_boot",
        fontsize = 22,
        halign   = :left)

    legend_handles = Lines[]
    legend_labels  = String[]

    # loop through scenarios, stack vertically in col=1
    for (idx, (year, scen)) in enumerate(scenario_vec)
        stats    = results_all[year][scen].group_stats
        cand_lbl = describe_candidate_set(results_all[year][scen].candidate_set)
        long_title = "Year $year — $cand_lbl"

        # wrap at ~50 chars
        wrapped_title = join(TextWrap.wrap(long_title; width=50))

        ax = Axis(fig[idx, 1];
            title     = wrapped_title,
            titlesize = 14,
            titlegap  = 8,
            xlabel    = "number of alternatives",
            ylabel    = "value",
            xticks    = (m_values, string.(m_values)))

        allvals = Float64[]

        for meas in measures
            col  = measure_cols[meas]
            meds = Float64[]; q25 = Float64[]; q75 = Float64[]
            p05  = Float64[]; p95 = Float64[]

            for m in m_values
                vals = stats[m][demo_sym][variant_sym][meas]
                append!(allvals, vals)
                push!(meds, median(vals))
                push!(q25, quantile(vals,0.25)); push!(q75, quantile(vals,0.75))
                push!(p05, quantile(vals,0.05)); push!(p95, quantile(vals,0.95))
            end

            # draw 90% & IQR bands + median line
            band!(ax, m_values, p05, p95; color=(col,0.12), linewidth=0)
            band!(ax, m_values, q25, q75; color=(col,0.25), linewidth=0)
            ln = lines!(ax, m_values, meds; color=col, linewidth=2)

            # only grab legend handles on the first panel
            if idx == 1
                push!(legend_handles, ln)
                push!(legend_labels, string(meas))
            end
        end

        # tighten y-ticks to data range
        y_min, y_max = extrema(allvals)
        ticks = range(y_min, y_max; length=n_yticks) |> collect
        ax.yticks[] = (ticks, string.(round.(ticks; digits=3)))
    end

    # single legend in last row, col 1
    Legend(fig[n_panels+1, 1],
        legend_handles, legend_labels;
        orientation = :horizontal,
        framevisible = false,
        halign = :left)

    resize_to_layout!(fig)
    return fig
end =#

#= 
function compare_demographic_across_scenarios(
        results_all::Dict,
        scenario_vec::Vector{Tuple{Int,String}};
        demographic::String,
        variant      ::String         = "mice",
        measures     ::Vector{Symbol} = [:C,:D,:G],
        palette      ::Vector         = Makie.wong_colors()[1:3],
        n_yticks     ::Int            = 5,
        base_width   ::Int            = 400,       # width per panel
        base_height  ::Int            = 360)       # height for plots + header + legend

    # symbol conversions
    demo_sym    = Symbol(demographic)
    variant_sym = Symbol(variant)

    # how many panels
    n_panels = length(scenario_vec)

    # extract m and B from first scenario
    y0, s0   = scenario_vec[1]
    stats0   = results_all[y0][s0].group_stats
    m_values = sort(collect(keys(stats0)))
    n_boot   = length(stats0[first(m_values)][demo_sym][variant_sym][:C])

    # prepare a consistent colour for C/D/G
    ΔL = Dict(:C=>0, :D=>+20, :G=>-15)
    function shade(base_rgb::Colors.RGB, meas::Symbol)
        lch = convert(Colors.LCHab, base_rgb)
        newL = clamp(lch.l + ΔL[meas], 0, 100)
        convert(Colors.RGB, Colors.LCHab(newL, lch.c, lch.h))
    end

    # pick one base RGB per measure
    base_rgbs = convert.(Colors.RGB, palette[1:length(measures)])
    measure_cols = Dict(measures[i] => shade(base_rgbs[i], measures[i])
                        for i in eachindex(measures))

    # make figure: one row of panels + header row + legend row
    fig = Figure(
      resolution = (base_width * n_panels, base_height),
      layout = (3, n_panels)
    )
    rowgap!(fig.layout, 20)
    colgap!(fig.layout, 30)

    # global header spans all columns in row 1
    fig[1, 1:n_panels] = Label(fig,
        "$demographic • number of alternatives =$(first(m_values))…$(last(m_values)) • $n_boot bootstraps",
        fontsize = 22, halign = :center)

    # collect legend handles
    legend_handles = Lines[]
    legend_labels  = String[]

    # panel loop: place each at row 2, col i
    for (i, (year, scen)) in enumerate(scenario_vec)
        stats   = results_all[year][scen].group_stats
        cand_lbl = describe_candidate_set(results_all[year][scen].candidate_set)
        long_title = "Year $year — $cand_lbl"

        # wrap at ~50 chars
        wrapped = join(TextWrap.wrap(long_title; width=50))

        ax = Axis(fig[2, i];
            title     = wrapped,
            titlesize = 14,
            titlefont = "sans",
            titlegap  = 8,
            xlabel    = "number of alternatives",
            ylabel    = "value",
            xticks    = (m_values, string.(m_values))
        )

        allvals = Float64[]

        for meas in measures
            col  = measure_cols[meas]
            meds = Float64[]; q25 = Float64[]; q75 = Float64[]
            p05  = Float64[]; p95 = Float64[]

            for m in m_values
                vals = stats[m][demo_sym][variant_sym][meas]
                append!(allvals, vals)
                push!(meds, median(vals))
                push!(q25, quantile(vals, 0.25)); push!(q75, quantile(vals, 0.75))
                push!(p05, quantile(vals, 0.05)); push!(p95, quantile(vals, 0.95))
            end

            band!(ax, m_values, p05, p95; color=(col,0.12), linewidth=0)
            band!(ax, m_values, q25, q75; color=(col,0.25), linewidth=0)
            ln = lines!(ax, m_values, meds; color=col, linewidth=2)

            # only once, grab for the legend
            if i == 1
                push!(legend_handles, ln)
                push!(legend_labels, string(meas))
            end
        end

        # tighten y‐axis ticks
        y_min, y_max = extrema(allvals)
        ticks = range(y_min, y_max; length = n_yticks) |> collect
        ax.yticks[] = (ticks, string.(round.(ticks; digits=3)))
    end

    # legend under the panels, row 3
    fig[3, 1:n_panels] = Legend(fig,
        legend_handles, legend_labels;
        orientation  = :horizontal,
        framevisible = false,
        halign       = :center
    )

    resize_to_layout!(fig)
    return fig
end =#

function compare_demographic_across_scenariosy(
        results_all::Dict,
        scenario_vec::Vector{Tuple{Int,String}};
        demographic::String,
        variant      ::String         = "mice",
        measures     ::Vector{Symbol} = [:C,:D,:G],
        palette      ::Vector         = Makie.wong_colors()[1:3],
        n_yticks     ::Int            = 5,
        base_width   ::Int            = 400,       # width per panel
        base_height  ::Int            = 360)       # height for plots + header + legend

    # symbol conversions
    demo_sym    = Symbol(demographic)
    variant_sym = Symbol(variant)

    # how many panels
    n_panels = length(scenario_vec)

    # extract m and B from first scenario
    y0, s0   = scenario_vec[1]
    stats0   = results_all[y0][s0].group_stats
    m_values = sort(collect(keys(stats0)))
    n_boot   = length(stats0[first(m_values)][demo_sym][variant_sym][:C])

    # prepare a consistent colour for C/D/G
    ΔL = Dict(:C=>0, :D=>+20, :G=>-15)
    function shade(base_rgb::Colors.RGB, meas::Symbol)
        lch = convert(Colors.LCHab, base_rgb)
        newL = clamp(lch.l + ΔL[meas], 0, 100)
        convert(Colors.RGB, Colors.LCHab(newL, lch.c, lch.h))
    end

    # pick one base RGB per measure
    base_rgbs = convert.(Colors.RGB, palette[1:length(measures)])
    measure_cols = Dict(measures[i] => shade(base_rgbs[i], measures[i])
                        for i in eachindex(measures))

    # FIRST PASS: collect all values across all scenarios to determine global y-limits
    global_allvals = Float64[]
    
    for (year, scen) in scenario_vec
        stats = results_all[year][scen].group_stats
        for meas in measures
            for m in m_values
                vals = stats[m][demo_sym][variant_sym][meas]
                append!(global_allvals, vals)
            end
        end
    end
    
    # Calculate global y-limits
    global_y_min, global_y_max = extrema(global_allvals)
    global_ticks = range(global_y_min, global_y_max; length = n_yticks) |> collect

    # make figure: one row of panels + header row + legend row
    fig = Figure(
      resolution = (base_width * n_panels, base_height),
      layout = (3, n_panels)
    )
    rowgap!(fig.layout, 20)
    colgap!(fig.layout, 30)

    # global header spans all columns in row 1
    fig[1, 1:n_panels] = Label(fig,
        "$demographic • number of alternatives = $(first(m_values))…$(last(m_values)) • $n_boot pseudo-profiles",
        fontsize = 22, halign = :center)

    # collect legend handles
    legend_handles = Lines[]
    legend_labels  = String[]

    # panel loop: place each at row 2, col i
    for (i, (year, scen)) in enumerate(scenario_vec)
        stats   = results_all[year][scen].group_stats
        cand_lbl = describe_candidate_set(results_all[year][scen].candidate_set)
        long_title = "Year $year — $cand_lbl"

        # wrap at ~50 chars
        wrapped = join(TextWrap.wrap(long_title; width=50))

        ax = Axis(fig[2, i];
            title     = wrapped,
            titlesize = 14,
            titlefont = "sans",
            titlegap  = 8,
            xlabel    = "number of alternatives",
            ylabel    = "value",
            xticks    = (m_values, string.(m_values)),
            # Set shared y-limits and ticks
            limits    = (nothing, (global_y_min, global_y_max)),
            yticks    = (global_ticks, string.(round.(global_ticks; digits=3)))
        )

        for meas in measures
            col  = measure_cols[meas]
            meds = Float64[]; q25 = Float64[]; q75 = Float64[]
            p05  = Float64[]; p95 = Float64[]

            for m in m_values
                vals = stats[m][demo_sym][variant_sym][meas]
                push!(meds, median(vals))
                push!(q25, quantile(vals, 0.25)); push!(q75, quantile(vals, 0.75))
                push!(p05, quantile(vals, 0.05)); push!(p95, quantile(vals, 0.95))
            end

            band!(ax, m_values, p05, p95; color=(col,0.12), linewidth=0)
            band!(ax, m_values, q25, q75; color=(col,0.25), linewidth=0)
            ln = lines!(ax, m_values, meds; color=col, linewidth=2)

            # only once, grab for the legend
            if i == 1
                push!(legend_handles, ln)
                push!(legend_labels, string(meas))
            end
        end
    end

    # legend under the panels, row 3
    fig[3, 1:n_panels] = Legend(fig,
        legend_handles, legend_labels;
        orientation  = :horizontal,
        framevisible = false,
        halign       = :center
    )

    resize_to_layout!(fig)
    return fig
end
