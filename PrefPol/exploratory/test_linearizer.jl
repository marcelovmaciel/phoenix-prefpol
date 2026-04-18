pref_path = normpath(joinpath(@__DIR__, "..", "..", "Preferences"))
pref_path in LOAD_PATH || pushfirst!(LOAD_PATH, pref_path)

using Preferences
using Random

const pp = Preferences

pool = pp.CandidatePool([:A, :B, :C, :D])

reference_profile = pp.Profile(pool, [
    pp.StrictRank(pool, [:A, :B, :C, :D]),
    pp.StrictRank(pool, [:A, :B, :C, :D]),
    pp.StrictRank(pool, [:A, :B, :C, :D]),
    pp.StrictRank(pool, [:A, :C, :B, :D]),
    pp.StrictRank(pool, [:B, :A, :C, :D]),
    pp.StrictRank(pool, [:D, :C, :B, :A]),
])

model = pp.PatternConditionalLinearizer(reference_profile; alpha = 0.5)
fallback_model = pp.PatternConditionalLinearizer(reference_profile; alpha = 0.5, fallback = :uniform)

println("Preferences path: ", pref_path)
println("Reference profile:")
for ballot in reference_profile.ballots
    println("  ", join(string.(pp.ranking_signature(ballot, pool)), " > "))
end
println()
flush(stdout)

cases = [
    ("A > B ~ C > D", pp.WeakRank(pool, [1, 2, 2, 3]), model),
    ("A ~ B > C > D", pp.WeakRank(pool, [1, 1, 2, 3]), model),
    ("C > A ~ D > B (fallback)", pp.WeakRank(pool, [2, 3, 1, 2]), fallback_model),
]

draws = 5000
seed = 2026

for (label, weak_rank, current_model) in cases
    levels = pp.to_weakorder(weak_rank)
    extensions, masses = pp._compatible_extension_distribution(
        current_model.reference,
        levels,
        current_model.alpha,
        current_model.fallback,
    )

    empirical_counts = Dict{Tuple{Vararg{Int}},Float64}(Tuple(ext) => 0.0 for ext in extensions)
    for ballot in current_model.reference.ballots
        ballot_perm = pp.perm(ballot)
        pp._perm_refines_levels(ballot_perm, levels) || continue
        empirical_counts[Tuple(ballot_perm)] += 1.0
    end

    rng = Random.MersenneTwister(seed)
    sample_counts = Dict{Tuple{Vararg{Int}},Int}(Tuple(ext) => 0 for ext in extensions)
    for _ in 1:draws
        draw = pp.linearize(weak_rank; tie_break = current_model, rng = rng)
        sample_counts[Tuple(pp.perm(draw))] += 1
    end

    println("Weak order: ", label)
    println("  Compatible extensions:")
    for (ext, mass) in zip(extensions, masses)
        sig = pp.ranking_signature(pp.StrictRank(ext), pool)
        ref_count = empirical_counts[Tuple(ext)]
        sample_count = sample_counts[Tuple(ext)]
        sample_freq = sample_count / draws

        println(
            "    ",
            join(string.(sig), " > "),
            " | ref count = ",
            ref_count,
            " | smoothed mass = ",
            round(mass, digits = 3),
            " | sample count = ",
            sample_count,
            " | sample freq = ",
            round(sample_freq, digits = 3),
        )
    end
    println()
    flush(stdout)
end
