function ranking_to_perm(ranking)
    strict = strict_profile([ranking])
    return Preferences.ranking_signature(strict.ballots[1], strict.pool)
end

function perms_out_of_rankings(profile)
    strict = strict_profile(profile)
    return [Preferences.ranking_signature(ballot, strict.pool) for ballot in strict.ballots]
end

function profile_to_permallows_matrix(profile::Preferences.Profile{<:Preferences.StrictRank})
    n = Preferences.nballots(profile)
    m = length(profile.pool)
    out = Matrix{Int}(undef, n, m)

    @inbounds for (i, ballot) in enumerate(profile.ballots)
        out[i, :] = Preferences.to_perm(ballot)
    end

    return out
end

function get_consensus_ranking(profile::Preferences.Profile{<:Preferences.StrictRank})
    input_for_permallows = profile_to_permallows_matrix(profile)
    label_to_candidate = Dict(i => profile.pool[i] for i in 1:length(profile.pool))

    @rput input_for_permallows
    R"""
    library(PerMallows)
    result <- lmm(input_for_permallows, dist.name = "cayley", estimation = "exact")
    theta <- result$theta
    mode <- result$mode
    """
    @rget mode theta

    consensus_back_from_permallows = [label_to_candidate[i] for i in mode]
    consensus_dict = Dict(c => r for (r, c) in enumerate(consensus_back_from_permallows))
    return consensus_back_from_permallows, consensus_dict
end

function get_consensus_ranking(profile::AbstractVector{<:AbstractDict})
    return get_consensus_ranking(strict_profile(profile))
end
