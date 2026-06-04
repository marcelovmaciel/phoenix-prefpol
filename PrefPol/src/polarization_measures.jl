"""
    Ψ(profile)

Applied wrapper for `Preferences.can_polarization`. PrefPol accepts applied
profile artifacts or annotated profiles, extracts the formal strict profile, and
delegates the polarization statistic to `Preferences`.
"""
function Ψ(profile)
    return Preferences.can_polarization(Preferences.strict_profile(profile))
end


"""
    calc_total_reversal_component(profile)

Applied wrapper for the total reversal component defined in `Preferences`.
"""
function calc_total_reversal_component(profile)
    return Preferences.total_reversal_component(Preferences.strict_profile(profile))
end

"""
    calc_reversal_HHI(profile)

Applied wrapper for reversal concentration (`Preferences.reversal_hhi`).
"""
function calc_reversal_HHI(profile)
    return Preferences.reversal_hhi(Preferences.strict_profile(profile))
end

"""
    fast_reversal_geometric(profile)

Applied wrapper for the reversal geometric measure defined in `Preferences`.
"""
function fast_reversal_geometric(profile)
    return Preferences.reversal_geometric(Preferences.strict_profile(profile))
end


function _measure_input(x)
    if x isa DataFrame
        return x
    elseif hasproperty(x, :profile)
        return getproperty(x, :profile)
    end
    return x
end

function apply_measure_to_bts(bts, measure)
    return Dict(variant => map(rep -> measure(_measure_input(rep)), bts[variant]) for variant in keys(bts))
end

function apply_all_measures_to_bts(
    bts;
    measures = [Ψ, calc_total_reversal_component, calc_reversal_HHI, fast_reversal_geometric],
)
    return Dict(nameof(measure) => apply_measure_to_bts(bts, measure) for measure in measures)
end
