# =====================================
# src/PreferenceDisplay.jl
# =====================================

# --- Your existing minimal shows (kept, with fixes for nonparam Strict/Weak) ----

# Minimal show methods (no PrettyTables or reflection)
function Base.show(io::IO, pool::CandidatePool{N}) where {N}
    print(io, "CandidatePool{$N}(")
    print(io, join(String.(pool.names), ", "))
    print(io, ")")
end

function Base.show(io::IO, x::StrictRank)
    print(io, "StrictRank(")
    print(io, join(ranks(x), ", "))
    print(io, ")")
end

function Base.show(io::IO, x::WeakRank)
    vals = [ismissing(v) ? "missing" : string(v) for v in ranks(x)]
    print(io, "WeakRank(", join(vals, ", "), ")")
end

function Base.show(io::IO, pb::PairwiseTriangularStatic{N,T}) where {N,T}
    print(io, "PairwiseTriangularStatic{$N,$T} len=")
    print(io, length(pb.vals))
end

function Base.show(io::IO, pb::PairwiseTriangularMutable{N,T}) where {N,T}
    print(io, "PairwiseTriangularMutable{$N,$T} len=")
    print(io, length(pb.vals))
end

"""
    pretty_pairwise(pb, pool; missingchar='·') -> String

Tab-separated matrix view of a *single* ballot's pairwise comparisons.
Accepts `PairwiseTriangularStatic` or `PairwiseTriangularView`. For `PairwiseTriangularMutable`,
use the convenience method below.
"""
function pretty_pairwise(
    pb::Union{PairwiseTriangularStatic{N,T}, PairwiseTriangularView{N,T}},
    pool::CandidatePool{N};
    missingchar = '·'
) where {N,T<:Integer}

    names = labels(pool)
    rows  = Vector{String}(undef, N + 1)

    # header
    header = [" " ; names]
    rows[1] = join(header, '\t')

    # body
    for i in 1:N
        line = Vector{String}(undef, N + 1)
        line[1] = names[i]
        for j in 1:N
            if i == j
                line[j + 1] = "0"
            else
                v = score(pb, i, j)
                line[j + 1] = v === missing ? string(missingchar) : string(v)
            end
        end
        rows[i + 1] = join(line, '\t')
    end

    return join(rows, '\n')
end

# convenience: allow mutable ballots directly
pretty_pairwise(pbm::PairwiseTriangularMutable{N,T}, pool::CandidatePool{N}; kwargs...) where {N,T<:Integer} =
    pretty_pairwise(pairwise_view(pbm), pool; kwargs...)

# --- Additions below (only new functionality, no removals) --------------

struct StrictRankView
    order::Vector{Symbol}   # best → worst
end

struct WeakOrderView
    groups::Vector{Vector{Symbol}}  # higher groups ≻ lower groups; last may be unranked
end

function pretty(x::StrictRank, pool::CandidatePool)
    perm_ids = to_perm(x)
    return StrictRankView([pool[id] for id in perm_ids])
end

function Base.show(io::IO, v::StrictRankView)
    n = length(v.order)
    for k in 1:n
        print(io, String(v.order[k]))
        k < n && print(io, " ≻ ")
    end
end

# Input is the levels returned by to_weakorder(::WeakRank), with unranked last.
function pretty(levels::Vector{Vector{Int}}, pool::CandidatePool; hide_unranked::Bool=false)
    groups = if hide_unranked && !isempty(levels)
        levels[1:end-1]
    else
        levels
    end
    return WeakOrderView([ [pool[id] for id in grp] for grp in groups ])
end

function Base.show(io::IO, v::WeakOrderView)
    n = length(v.groups)
    for k in 1:n
        print(io, join(String.(v.groups[k]), " ~ "))
        k < n && print(io, " ≻ ")
    end
end

# Dense pairwise wrapper colored table
function show_pairwise_preference_table_color(pw::AbstractPairwise; pool::CandidatePool)
    M = dense(pw)
    N = size(M, 1)
    hdr = labels(pool)

    cell_str(i, j) = i == j ? "—" :
        (ismissing(M[i, j]) ? "·" : (M[i, j] == 0 ? "0" : (M[i, j] > 0 ? "+" : "-")))

    tbl = [cell_str(i, j) for i in 1:N, j in 1:N]

    c_pos  = Crayon(foreground=:green,  bold=true)
    c_neg  = Crayon(foreground=:red,    bold=true)
    c_tie  = Crayon(foreground=:yellow)
    c_miss = Crayon(foreground=:black)
    c_dia  = Crayon(foreground=:black, bold=true)

    colorize(s) = s == "+" ? string(c_pos,  s, Crayon(reset=true)) :
                  s == "-" ? string(c_neg,  s, Crayon(reset=true)) :
                  s == "0" ? string(c_tie,  s, Crayon(reset=true)) :
                  s == "·" ? string(c_miss, s, Crayon(reset=true)) :
                             string(c_dia,  s, Crayon(reset=true))

    ctbl = [ colorize(tbl[i, j]) for i in 1:N, j in 1:N ]

    pretty_table(ctbl; column_labels=hdr, row_labels=hdr, alignment=:c)
    return nothing
end

# Tab-separated pretty for dense pairwise (optional)
function pretty_pairwise(pw::PairwiseDense, pool::CandidatePool; missingchar='·')
    M = pw.matrix
    N = size(M, 1)
    names = labels(pool)

    rows = Vector{String}(undef, N + 1)
    rows[1] = join([" " ; names], '\t')

    for i in 1:N
        line = Vector{String}(undef, N + 1)
        line[1] = names[i]
        for j in 1:N
            if i == j
                line[j + 1] = "0"
            else
                v = M[i, j]
                line[j + 1] = ismissing(v) ? string(missingchar) : string(v)
            end
        end
        rows[i + 1] = join(line, '\t')
    end

    return join(rows, '\n')
end

##############################
# Profile visualization table
##############################

@inline _ballot_key(x::StrictRank) = (:StrictRank, Tuple(ranks(x)))
@inline _ballot_key(x::WeakRank) = (:WeakRank, Tuple(ranks(x)))
@inline _ballot_key(x::StrictRankMutable) = (:StrictRankMutable, Tuple(x.ranks))
@inline _ballot_key(x::PairwiseDense) = (:PairwiseDense, Tuple(vec(x.matrix)))
@inline _ballot_key(x::PairwiseTriangularStatic) = (:PairwiseTriangularStatic, Tuple(x.vals), Tuple(x.mask))
@inline _ballot_key(x::PairwiseTriangularMutable) = (:PairwiseTriangularMutable, Tuple(x.vals), Tuple(x.mask))
@inline _ballot_key(x::PairwiseTriangularView) = (:PairwiseTriangularView, Tuple(x.vals), Tuple(x.mask))
@inline _ballot_key(x) = (:Other, typeof(x), string(x))

function _ballot_label(x::StrictRank, pool::CandidatePool; hide_unranked::Bool=false)
    return string(pretty(x, pool))
end

function _ballot_label(x::WeakRank, pool::CandidatePool; hide_unranked::Bool=false)
    levels = to_weakorder(x)
    return string(pretty(levels, pool; hide_unranked=hide_unranked))
end

function _ballot_label(x::StrictRankMutable, pool::CandidatePool; hide_unranked::Bool=false)
    perm = to_perm(x)
    return string(StrictRankView([pool[id] for id in perm]))
end

_ballot_label(x, pool::CandidatePool; hide_unranked::Bool=false) = string(x)

function _profile_table_data(p::Profile; hide_unranked::Bool=false)
    n = nballots(p)

    keys = Dict{Any,Int}()
    ballots = Vector{eltype(p)}()
    counts = Int[]

    @inbounds for (i, b) in enumerate(p.ballots)
        key = _ballot_key(b)
        idx = get(keys, key, 0)
        if idx == 0
            push!(ballots, b)
            push!(counts, 1)
            keys[key] = length(ballots)
        else
            counts[idx] += 1
        end
    end

    has_counts = length(ballots) != n
    total = n == 0 ? 1 : n
    proportions = [c / total for c in counts]

    sort_key = proportions
    order = sortperm(sort_key; rev=true, alg=MergeSort)

    ballots = ballots[order]
    counts = counts[order]
    proportions = proportions[order]

    labels = [ _ballot_label(ballots[i], p.pool; hide_unranked=hide_unranked) for i in eachindex(ballots) ]

    return (labels=labels, counts=counts, proportions=proportions, has_counts=has_counts)
end

function _profile_table_data(p::WeightedProfile; hide_unranked::Bool=false)
    n = nballots(p)
    w = p.weights

    keys = Dict{Any,Int}()
    ballots = Vector{eltype(p)}()
    counts = Int[]
    weight_sum = Float64[]

    @inbounds for (i, b) in enumerate(p.ballots)
        key = _ballot_key(b)
        idx = get(keys, key, 0)
        if idx == 0
            push!(ballots, b)
            push!(counts, 1)
            push!(weight_sum, float(w[i]))
            keys[key] = length(ballots)
        else
            counts[idx] += 1
            weight_sum[idx] += float(w[i])
        end
    end

    has_counts = length(ballots) != n
    total = sum(weight_sum)
    proportions = total == 0 ? fill(0.0, length(weight_sum)) : [ws / total for ws in weight_sum]

    sort_key = has_counts ? counts : proportions
    order = sortperm(sort_key; rev=true, alg=MergeSort)

    ballots = ballots[order]
    counts = counts[order]
    proportions = proportions[order]

    labels = [ _ballot_label(ballots[i], p.pool; hide_unranked=hide_unranked) for i in eachindex(ballots) ]

    return (labels=labels, counts=counts, proportions=proportions, has_counts=has_counts)
end

function pretty_profile_table(p::Profile; hide_unranked::Bool=false)
    data = _profile_table_data(p; hide_unranked=hide_unranked)

    if data.has_counts
        headers = ["ranking", "count", "proportion"]
        rows = Vector{Vector{String}}(undef, length(data.labels))
        @inbounds for i in eachindex(data.labels)
            rows[i] = [
                data.labels[i],
                string(data.counts[i]),
                string(data.proportions[i]),
            ]
        end
        return _box_table(headers, rows)
    else
        headers = ["ranking", "proportion"]
        rows = Vector{Vector{String}}(undef, length(data.labels))
        @inbounds for i in eachindex(data.labels)
            rows[i] = [
                data.labels[i],
                string(data.proportions[i]),
            ]
        end
        return _box_table(headers, rows)
    end
end

function pretty_profile_table(p::WeightedProfile; hide_unranked::Bool=false)
    data = _profile_table_data(p; hide_unranked=hide_unranked)

    if data.has_counts
        headers = ["ranking", "count", "proportion"]
        rows = Vector{Vector{String}}(undef, length(data.labels))
        @inbounds for i in eachindex(data.labels)
            rows[i] = [
                data.labels[i],
                string(data.counts[i]),
                string(data.proportions[i]),
            ]
        end
        return _box_table(headers, rows)
    else
        headers = ["ranking", "proportion"]
        rows = Vector{Vector{String}}(undef, length(data.labels))
        @inbounds for i in eachindex(data.labels)
            rows[i] = [
                data.labels[i],
                string(data.proportions[i]),
            ]
        end
        return _box_table(headers, rows)
    end
end

# -------------------------------------------------------------------
# NEW: ANSI-colored boxed profile table (no PrettyTables dependency)
# -------------------------------------------------------------------

const _ESC = "\x1b"
const _RESET = _ESC * "[0m"

# simple palette (safe in most terminals)
const _C_CYAN_BOLD = _ESC * "[36;1m"
const _C_GREEN     = _ESC * "[32m"
const _C_MAGENTA   = _ESC * "[35m"

# remove ANSI SGR sequences for width computation
@inline function _strip_ansi(s::AbstractString)
    # matches \x1b[ ... m
    return replace(s, r"\x1b\[[0-9;]*m" => "")
end

@inline function _textwidth_ansi(s::AbstractString)
    return length(_strip_ansi(s))
end

@inline function _pad_ansi(s::AbstractString, w::Int)
    pad = w - _textwidth_ansi(s)
    return pad <= 0 ? String(s) : String(s) * repeat(" ", pad)
end

function _box_table(headers::Vector{String}, data_rows::Vector{Vector{String}})
    ncol = length(headers)
    @assert all(length(r) == ncol for r in data_rows)

    # column widths = max(header, all rows) by visible width (ANSI stripped)
    widths = [ _textwidth_ansi(headers[j]) for j in 1:ncol ]
    for r in data_rows
        for j in 1:ncol
            w = _textwidth_ansi(r[j])
            if w > widths[j]
                widths[j] = w
            end
        end
    end

    # helpers
    hline(left, mid, right) = left *
        join([repeat("─", widths[j] + 2) for j in 1:ncol], mid) *
        right

    function rowline(cells)
        parts = Vector{String}(undef, ncol)
        for j in 1:ncol
            parts[j] = " " * _pad_ansi(cells[j], widths[j]) * " "
        end
        return "│" * join(parts, "│") * "│"
    end

    out = String[]
    push!(out, hline("┌", "┬", "┐"))
    push!(out, rowline(headers))
    push!(out, hline("├", "┼", "┤"))
    for r in data_rows
        push!(out, rowline(r))
    end
    push!(out, hline("└", "┴", "┘"))

    return join(out, "\n")
end

"""
    profile_table_string_color(p; hide_unranked=false) -> String

Returns a unicode-box table string with ANSI colors embedded.
Print it with `print(...)` or call `show_profile_table_color(p)`.
"""
function profile_table_string_color(p::Profile; hide_unranked::Bool=false)
    data = _profile_table_data(p; hide_unranked=hide_unranked)

    if data.has_counts
        headers = ["ranking", "count", "proportion"]
        rows = Vector{Vector{String}}(undef, length(data.labels))
        @inbounds for i in eachindex(data.labels)
            rows[i] = [
                _C_CYAN_BOLD * data.labels[i] * _RESET,
                _C_GREEN * string(data.counts[i]) * _RESET,
                _C_MAGENTA * string(data.proportions[i]) * _RESET,
            ]
        end
    else
        headers = ["ranking", "proportion"]
        rows = Vector{Vector{String}}(undef, length(data.labels))
        @inbounds for i in eachindex(data.labels)
            rows[i] = [
                _C_CYAN_BOLD * data.labels[i] * _RESET,
                _C_MAGENTA * string(data.proportions[i]) * _RESET,
            ]
        end
    end

    return _box_table(headers, rows)
end

function profile_table_string_color(p::WeightedProfile; hide_unranked::Bool=false)
    data = _profile_table_data(p; hide_unranked=hide_unranked)

    if data.has_counts
        headers = ["ranking", "count", "proportion"]
        rows = Vector{Vector{String}}(undef, length(data.labels))
        @inbounds for i in eachindex(data.labels)
            rows[i] = [
                _C_CYAN_BOLD * data.labels[i] * _RESET,
                _C_GREEN * string(data.counts[i]) * _RESET,
                _C_MAGENTA * string(data.proportions[i]) * _RESET,
            ]
        end
    else
        headers = ["ranking", "proportion"]
        rows = Vector{Vector{String}}(undef, length(data.labels))
        @inbounds for i in eachindex(data.labels)
            rows[i] = [
                _C_CYAN_BOLD * data.labels[i] * _RESET,
                _C_MAGENTA * string(data.proportions[i]) * _RESET,
            ]
        end
    end

    return _box_table(headers, rows)
end

"""
    show_profile_table_color(p::Profile; hide_unranked=false, io=stdout)
    show_profile_table_color(p::WeightedProfile; hide_unranked=false, io=stdout)

Prints a colored PrettyTables profile table.
"""
function _show_profile_table_color(data; io::IO=stdout)
    headers = data.has_counts ? ["ranking", "count", "proportion"] : ["ranking", "proportion"]
    nrows = length(data.labels)
    ncols = length(headers)

    c_hi = Crayon(foreground=:green, bold=true)
    c_mid = Crayon(foreground=:yellow)
    c_low = Crayon(foreground=:red)
    c_rank = Crayon(foreground=:cyan, bold=true)
    c_reset = Crayon(reset=true)

    color_by_ratio(s, ratio) = ratio >= 0.66 ? string(c_hi, s, c_reset) :
                              ratio >= 0.33 ? string(c_mid, s, c_reset) :
                                              string(c_low, s, c_reset)

    max_count = data.has_counts && !isempty(data.counts) ? maximum(data.counts) : 0
    max_prop = !isempty(data.proportions) ? maximum(data.proportions) : 0.0

    tbl = Matrix{String}(undef, nrows, ncols)
    for i in 1:nrows
        tbl[i, 1] = string(c_rank, data.labels[i], c_reset)
        if data.has_counts
            count_str = string(data.counts[i])
            ratio = max_count == 0 ? 0.0 : data.counts[i] / max_count
            tbl[i, 2] = color_by_ratio(count_str, ratio)

            prop_str = string(data.proportions[i])
            ratio = max_prop == 0 ? 0.0 : data.proportions[i] / max_prop
            tbl[i, 3] = color_by_ratio(prop_str, ratio)
        else
            prop_str = string(data.proportions[i])
            ratio = max_prop == 0 ? 0.0 : data.proportions[i] / max_prop
            tbl[i, 2] = color_by_ratio(prop_str, ratio)
        end
    end

    pretty_table(tbl; column_labels=headers, alignment=:c, io=io)
    return nothing
end

function show_profile_table_color(p::Profile; hide_unranked::Bool=false, io::IO=stdout)
    data = _profile_table_data(p; hide_unranked=hide_unranked)
    return _show_profile_table_color(data; io=io)
end

function show_profile_table_color(p::WeightedProfile; hide_unranked::Bool=false, io::IO=stdout)
    data = _profile_table_data(p; hide_unranked=hide_unranked)
    return _show_profile_table_color(data; io=io)
end

# keep a fallback for non-profiles
function show_profile_table_color(::Any; kwargs...)
    throw(ArgumentError("show_profile_table_color expects a Profile or WeightedProfile."))
end

##############################
# Pairwise majority tables
##############################

function _pairwise_majority_matrix(pm::PairwiseMajority, kind::Symbol)
    k = kind === :totals ? :counts : (kind === :margin ? :margins : kind)
    if k === :wins
        return pairwise_majority_wins(pm)
    elseif k === :counts
        return pairwise_majority_counts(pm)
    elseif k === :margins
        return pairwise_majority_margins(pm)
    else
        throw(ArgumentError("Unknown kind $kind (use :wins, :counts, or :margins)"))
    end
end

function _pretty_matrix_table(M::AbstractMatrix, pool::CandidatePool)
    N = size(M, 1)
    names = labels(pool)
    rows = Vector{String}(undef, N + 1)
    rows[1] = join([" " ; names], '\t')

    @inbounds for i in 1:N
        line = Vector{String}(undef, N + 1)
        line[1] = names[i]
        for j in 1:N
            line[j + 1] = string(M[i, j])
        end
        rows[i + 1] = join(line, '\t')
    end

    return join(rows, '\n')
end

function pretty_pairwise_majority_table(pm::PairwiseMajority, pool::CandidatePool; kind::Symbol=:wins)
    M = _pairwise_majority_matrix(pm, kind)
    return _pretty_matrix_table(M, pool)
end

pretty_pairwise_majority_table(p::Profile, pool::CandidatePool; kind::Symbol=:wins) =
    pretty_pairwise_majority_table(pairwise_majority(p), pool; kind=kind)

pretty_pairwise_majority(p::PairwiseMajority, pool::CandidatePool) =
    pretty_pairwise_majority_table(p, pool; kind=:wins)
pretty_pairwise_majority(p::Profile, pool::CandidatePool) =
    pretty_pairwise_majority_table(p, pool; kind=:wins)

pretty_pairwise_majority_counts(p::PairwiseMajority, pool::CandidatePool) =
    pretty_pairwise_majority_table(p, pool; kind=:counts)
pretty_pairwise_majority_counts(p::Profile, pool::CandidatePool) =
    pretty_pairwise_majority_table(p, pool; kind=:counts)

pretty_pairwise_majority_margins(p::PairwiseMajority, pool::CandidatePool) =
    pretty_pairwise_majority_table(p, pool; kind=:margins)
pretty_pairwise_majority_margins(p::Profile, pool::CandidatePool) =
    pretty_pairwise_majority_table(p, pool; kind=:margins)

function show_pairwise_majority_table_color(pm::PairwiseMajority;
                                            pool::CandidatePool,
                                            kind::Symbol=:wins)
    k = kind === :totals ? :counts : (kind === :margin ? :margins : kind)
    M = _pairwise_majority_matrix(pm, k)
    N = size(M, 1)
    hdr = labels(pool)

    c_pos = Crayon(foreground=:green, bold=true)
    c_neg = Crayon(foreground=:red, bold=true)
    c_zero = Crayon(foreground=:yellow)
    c_reset = Crayon(reset=true)

    color_by_sign(s, v) = v > 0 ? string(c_pos, s, c_reset) :
                          v < 0 ? string(c_neg, s, c_reset) :
                                  string(c_zero, s, c_reset)

    tbl = Matrix{String}(undef, N, N)
    if k === :counts
        maxv = N == 0 ? 0 : maximum(M)
        color_by_ratio(s, ratio) = ratio >= 0.66 ? string(c_pos, s, c_reset) :
                                  ratio >= 0.33 ? string(c_zero, s, c_reset) :
                                                  string(c_neg, s, c_reset)
        for i in 1:N, j in 1:N
            s = string(M[i, j])
            ratio = maxv == 0 ? 0.0 : M[i, j] / maxv
            tbl[i, j] = color_by_ratio(s, ratio)
        end
    else
        for i in 1:N, j in 1:N
            v = M[i, j]
            tbl[i, j] = color_by_sign(string(v), v)
        end
    end

    pretty_table(tbl; column_labels=hdr, row_labels=hdr, alignment=:c)
    return nothing
end

show_pairwise_majority_table_color(p::Profile; pool::CandidatePool, kind::Symbol=:wins) =
    show_pairwise_majority_table_color(pairwise_majority(p); pool=pool, kind=kind)
