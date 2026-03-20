using Revise

import Preferences
const pp = Preferences

# Small REPL helpers
pretty(x)    = pp.pretty(x, p)
strictify_random(x; rng=Random.MersenneTwister(42)) = pp.to_strict(x; tie_break=:random, rng)

# ------------------------------------------------------------
# 1) CandidatePool & basics
# ------------------------------------------------------------
p = pp.CandidatePool([:Alice, :Bob, :Carol, :Dave])

p
length(p)                # 4
p[:Alice]                # 1
p[:Dave]                 # 4
p[1]                     # :Alice
p[4]                     # :Dave
collect(keys(p))         # [:Alice, :Bob, :Carol, :Dave]
pp.candidates(p)         # same order
pp.to_cmap(p)            # Dict(id=>name)

# ------------------------------------------------------------
# 2) WeakRank / StrictRank constructors & predicates
# ------------------------------------------------------------
# Strict complete
b_strict = pp.StrictRank(p, [1,2,3,4])

# Strict from permutation of names
sr = pp.StrictRank(p, [:Bob, :Dave, :Alice, :Carol])
pp.pretty(sr, p)   # Bob ≻ Dave ≻ Alice ≻ Carol

# Truncated ballot (Dave unranked) — WeakRank
b_trunc = pp.WeakRank(p, Dict(:Bob=>1, :Alice=>2))
pp.rank(b_trunc, p, :Alice)  # 2

# Weak-order view (levels as Vector{Vector{Int}})

wo = pp.to_weakorder(b_trunc)

pretty(wo)                         # groups with unranked last

pp.pretty(wo, p; hide_unranked=true)  # hides last level if unranked exist

# Weak with a tie (Alice ~ Bob), Dave unranked
b_weak = pp.WeakRank(p, Dict(:Carol=>1, :Alice=>2, :Bob=>2))


b_weak

# Vector input as WeakRank
b_vec = pp.WeakRank(p, [1,2,3,4])

pp.pretty(pp.to_weakorder(b_vec), p)

# asdict (present ranks only for WeakRank)
pp.asdict(b_strict, p)
pp.asdict(b_trunc,  p)

# rank / prefers / indifferent
pp.rank(b_strict, p, :Alice)                # 1
pp.prefers(b_strict, p, :Alice, :Bob)       # true
pp.indifferent(b_strict, p, :Alice, :Bob)   # false

pp.rank(b_trunc,  p, :Dave)                 # missing
pp.prefers(b_trunc, p, :Dave, :Alice)       # false (missing treated as not preferred)
pp.indifferent(b_weak, p, :Alice, :Bob)     # true

# Strictify weak ballots
try
    pp.to_strict(b_trunc; tie_break=:error)   # throws (missing present)
catch e
    e
end

import Random

# Random tie-breaking
rng = Random.MersenneTwister()

b_weak

pp.to_strict(b_weak; tie_break=:random, rng)

# Custom linearizers TODO: BUG this breaks
lin_by_name = pp.make_rank_bucket_linearizer(:by_name; pool=p)
try
    pp.to_strict(b_weak; tie_break=lin_by_name)
catch e
    @warn "Custom linearizer example failed; continuing." exception=e
end


b_weak

println("WeakRank as WeakOrder (names):")
println(pretty(pp.to_weakorder(b_weak)))

# ------------------------------------------------------------
# 3) PairwiseDense checks (policies)
# ------------------------------------------------------------
# Preferred: pass explicit policy objects
pp_none = pp.to_pairwise(b_weak, p; policy = pp.NonePolicyMissing())     # any missing ⇒ missing
pp_bot  = pp.to_pairwise(b_weak, p; policy = pp.BottomPolicyMissing())   # ranked ≻ unranked; both unranked ⇒ missing

# Visual check (dense display; optional PrettyTables/Crayons)
try
    import PrettyTables
    import Crayons
    pp.show_pairwise_preference_table_color(pp_bot; pool = p)
catch e
    @warn "PrettyTables/Crayons display not available; skipping." exception=e
end

pp_bot
pp_bot.matrix

# Also check triangular path (static) and textual pretty
pb_tri = pp.pairwise_from_weak(b_weak, p, pp.BottomPolicyMissing())
println(pp.pretty_pairwise(pb_tri, p))

# ------------------------------------------------------------
# 4) Permutations & conversions
# ------------------------------------------------------------
# Candidate-ids (best→worst)
perm1 = pp.to_perm(b_strict)
perm1

perm2 = pp.to_perm(b_trunc)  # unranked last (emits @info if any unranked exist)
perm2

# Ordered candidate names from StrictRank
pp.ordered_candidates(b_strict, p)  # [:Alice,:Bob,:Carol,:Dave]

# ------------------------------------------------------------
# 5) Restriction hooks
# ------------------------------------------------------------
subset_syms = [:Alice, :Carol, :Dave]

(new_b_trunc, np1, bm1) = pp.restrict(b_trunc, p, subset_syms)
new_b_trunc; np1; collect(bm1)      # new→old ids
pp.asdict(new_b_trunc, np1)

# Restrict StrictRank
(new_b_strict, np2, bm2) = pp.restrict(b_strict, p, subset_syms)
new_b_strict; np2; collect(bm2)
pp.to_perm(new_b_strict)

# Restrict PairwiseDense (dense)
ppair = pp.to_pairwise(b_weak, p; policy = pp.BottomPolicyMissing())
(new_ppair, np3, bm3) = pp.restrict(ppair, p, subset_syms)
new_ppair.matrix; np3; collect(bm3)

# Restrict triangular pairwise
(new_pbtri, np4, bm4) = begin
    # build triangular again on the full pool (to show restriction)
    full_tri = pp.pairwise_from_weak(b_weak, p, pp.BottomPolicyMissing())
    # restrict by names
    # translate to dense for slicing demonstration then back (optional)
    dense = pp.pairwise_dense(full_tri)
    sub   = dense[bm3, bm3]
    (pp.PairwiseDense{Union{Missing,Int8}}(sub), np3, bm3)  # re-use computed bm3
end
new_pbtri; np4; collect(bm4)

# ------------------------------------------------------------
# 6) Weak-order name rendering on restricted pool
# ------------------------------------------------------------
pp.weakorder_symbol_groups(pp.to_weakorder(new_b_trunc), np1)

# ------------------------------------------------------------
# 7) Custom policy example
# ------------------------------------------------------------
# Unranked-at-top: missing ≻ ranked; both missing ⇒ missing
struct TopPolicy <: pp.ExtensionPolicy end
pp.compare_maybe(::TopPolicy,
                 ra::Union{Int,Missing}, rb::Union{Int,Missing},
                 i::Int, j::Int, ranks::AbstractVector{Union{Int,Missing}},
                 pool::pp.CandidatePool) =
    ismissing(ra) && ismissing(rb) ? missing :
    ismissing(ra) ? Int8(1) :
    ismissing(rb) ? Int8(-1) :
    (Int(rb) - Int(ra) > 0 ? Int8(1) : Int8(Int(rb) - Int(ra) < 0 ? -1 : 0))

pp_top = pp.to_pairwise(b_trunc, p; policy = TopPolicy())
pp_top.matrix



# ------------------------------------------------------------
# 8) Core label helpers & membership
# ------------------------------------------------------------
@assert pp.labels(p) == String.(pp.candidates(p))
@assert pp.getlabel(p, 2) == "Bob"
@assert pp.candid(p, :Carol) == 3
@assert haskey(p, :Alice) === true
@assert (:Dave in p) === true
@assert (:Eve in p) === false

# ------------------------------------------------------------
# 9) PairwiseDense from StrictRank (dense + quick sanity)
# ------------------------------------------------------------
pw_strict = pp.to_pairwise(b_strict, p; policy = pp.NonePolicyMissing())
@assert size(pw_strict.matrix) == (length(p), length(p))
# Since b_strict = [1,2,3,4], Alice should beat Bob:
@assert pw_strict.matrix[p[:Alice], p[:Bob]] == 1
@assert pw_strict.matrix[p[:Bob],   p[:Alice]] == -1

# ------------------------------------------------------------
# 10) Custom function tie-breaker
#     (Define your own bucket linearizer f(ids, pool, ranks) → Vector{Int})
#     Here: break ties by reverse-lexicographic name just to prove the API.
# ------------------------------------------------------------
tie_revlex = (ids, pool_, ranks_) -> sort(ids; by = i -> pool_[i], rev = true)
b_ties = pp.WeakRank(p, Dict(:Alice=>2, :Bob=>2, :Carol=>1))  # Alice ~ Bob ≻ Carol ≻ Dave(unranked)
sr_rev = pp.to_strict(b_ties; tie_break = tie_revlex, pool = p)
# Reverse-lex between Alice/Bob ⇒ Bob before Alice
@assert pp.ordered_candidates(sr_rev, p)[1:2] == [:Carol, :Bob]

# ------------------------------------------------------------
# 11) Dense pretty for pairwise (optional UI parity)
# ------------------------------------------------------------
println(pp.pretty_pairwise(pw_strict, p))


pw_strict

println("\n[Supplemental checks done]")

# ------------------------------------------------------------
# 12) Profile visualization tables
# ------------------------------------------------------------
pool2 = pp.CandidatePool([:A, :B, :C])
b_a = pp.StrictRank(pool2, [1, 2, 3])
b_b = pp.StrictRank(pool2, [2, 1, 3])
b_c = pp.StrictRank(pool2, [3, 1, 2])



# Duplicates => counts + proportions
prof_dup = pp.Profile(pool2, [b_a, b_a, b_b, b_c])
println(pp.pretty_profile_table(prof_dup))

# Unique ballots + weights => proportions only
prof_unique = pp.Profile(pool2, [b_a, b_b, b_c]; weights=[0.5, 0.3, 0.2])
println(pp.pretty_profile_table(prof_unique))

# Weak ranks (ties/unranked) pretty printing
w_tie = pp.WeakRank(pool2, Dict(:A => 1, :B => 1))  # A ~ B, C unranked
# TODO: note that unranked is thrown to the bottom
w_ranked = pp.WeakRank(pool2, Dict(:B => 1, :C => 2))
prof_weak = pp.Profile(pool2, [w_tie, w_ranked])


println(pp.pretty_profile_table(prof_weak))


println(pp.pretty_profile_table(prof_weak; hide_unranked=true))

# ------------------------------------------------------------
# 13) Pairwise majority aggregation + tables
# ------------------------------------------------------------
# Strict profile


pm_strict = pp.pairwise_majority(prof_dup)

println(pp.pretty_pairwise_majority_table(pm_strict, pool2; kind=:wins))

println(pp.pretty_pairwise_majority_table(pm_strict, pool2; kind=:counts))

println(pp.pretty_pairwise_majority_table(pm_strict, pool2; kind=:margins))

# Weak profile with missing/ties (skipped)
prof_weak2 = pp.Profile(pool2, [w_tie, w_ranked])

pp.pretty_profile_table(prof_weak2) |> println


pm_weak = pp.pairwise_majority(prof_weak2)
println(pp.pretty_pairwise_majority(pm_weak, pool2))


# BUG: it breaks here
# PairwiseDense profile
pw1 = pp.to_pairwise(b_a, pool2; policy = pp.NonePolicyMissing())
pw2 = pp.to_pairwise(b_b, pool2; policy = pp.NonePolicyMissing())


prof_pw = pp.Profile(pool2, [pw1, pw2])

pm_pw = pp.pairwise_majority(prof_pw)

println(pp.pretty_pairwise_majority_counts(pm_pw, pool2))


# BUG: also here
# PairwiseTriangular profile (weak ballot -> triangular)
pw_tri = pp.pairwise_from_weak(w_ranked, pool2, pp.BottomPolicyMissing())
prof_tri = pp.Profile(pool2, [pw_tri])
pm_tri = pp.pairwise_majority(prof_tri)
println(pp.pretty_pairwise_majority(pm_tri, pool2))



# here it works !
# StrictRankMutable profile
srm = pp.StrictRankMutable(b_a)
prof_mut = pp.Profile(pool2, [srm, srm])
pm_mut = pp.pairwise_majority(prof_mut)
println(pp.pretty_pairwise_majority_counts(pm_mut, pool2))


# here breaks
# Colored pairwise majority tables (if PrettyTables/Crayons available)
try
    import PrettyTables
    import Crayons
    pp.show_pairwise_majority_table_color(pm_strict; pool = pool2, kind = :wins)
    pp.show_pairwise_majority_table_color(pm_strict; pool = pool2, kind = :counts)
    pp.show_pairwise_majority_table_color(pm_strict; pool = pool2, kind = :margins)
catch e
    @warn "PrettyTables/Crayons majority tables not available; skipping." exception=e
end
