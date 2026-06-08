struct SaariBasis{N}
    pool::Preferences.CandidatePool
    permutations::Vector{SVector{N,Int}}
    index::Dict{SVector{N,Int},Int}
end

function _canonical_index(permutations::Vector{SVector{N,Int}}) where {N}
    return Dict(perm => i for (i, perm) in pairs(permutations))
end

function SaariBasis3(pool::Preferences.CandidatePool)
    length(pool) == 3 || throw(ArgumentError("SaariBasis3 requires a 3-candidate pool"))
    permutations = collect(CANONICAL_3C_IDS)
    return SaariBasis{3}(pool, permutations, _canonical_index(permutations))
end

function SaariBasis4(pool::Preferences.CandidatePool)
    length(pool) == 4 || throw(ArgumentError("SaariBasis4 requires a 4-candidate pool"))
    permutations = collect(CANONICAL_4C_IDS)
    return SaariBasis{4}(pool, permutations, _canonical_index(permutations))
end

canonical_permutations(b::SaariBasis) = b.permutations

function ranking_labels(b::SaariBasis; sep = ">")
    names = Preferences.candidates(b.pool)
    return [join((String(names[id]) for id in perm), sep) for perm in b.permutations]
end
