const LinearOrderCatalog = Preferences.LinearOrderCatalog
const ConsensusResult = Preferences.ConsensusResult
const GLOBAL_LINEAR_ORDER_CACHE = Preferences.GLOBAL_LINEAR_ORDER_CACHE

get_linear_order_catalog(args...; kwargs...) = Preferences.get_linear_order_catalog(args...; kwargs...)
strict_profile(args...; kwargs...) = Preferences.strict_profile(args...; kwargs...)
consensus_kendall(args...; kwargs...) = Preferences.consensus_kendall(args...; kwargs...)
get_consensus_ranking(args...; kwargs...) = Preferences.get_consensus_ranking(args...; kwargs...)

_candidate_tuple(args...; kwargs...) = Preferences._candidate_tuple(args...; kwargs...)
