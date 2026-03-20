# PreferencePairwise.jl

# Pairwise interface
abstract type AbstractPairwise end

# Shared API surface (methods must be provided by concrete subtypes)
function score(::AbstractPairwise, ::Int, ::Int)
    throw(ArgumentError("score not implemented for this pairwise type"))
end

function isdefined(::AbstractPairwise, ::Int, ::Int)
    throw(ArgumentError("isdefined not implemented for this pairwise type"))
end

function dense(::AbstractPairwise)
    throw(ArgumentError("dense not implemented for this pairwise type"))
end
