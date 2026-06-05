function pp_proportions(df::DataFrame, cols)
    for col in cols
        v  = df[!, col]                  # keeps missings
        pm = proportionmap(v)            # Dict(value => share)
        N  = length(v)                   # total observations (incl. missings)

        println("\n" * "─"^40)
        @printf("%-15s │ %8s │ %s\n", string(col), "prop.", "count")
        println("─"^40)

        for (val, p) in sort(collect(pm); by = first)  # deterministic order
            @printf("%-15s │ %6.2f%% │ %d\n",
                    val, p * 100, Int(round(p * N)))
        end
    end
    println("─"^40)
end


function build_numbered_symbols(prefix::AbstractString, n::Integer; minwidth::Int = 1)
    return [Symbol(prefix * @sprintf("%0*d", minwidth, i)) for i in 1:n]
end

build_letter_symbols(prefix::AbstractString, letters) =
    [Symbol(prefix * string(letter)) for letter in letters]

@inline _recoder_raw_value(x) = x isa CategoricalValue ? unwrap(x) : x

function _matches_recode_code(x, codes)
    raw = _recoder_raw_value(x)
    ismissing(raw) && return false
    for code in codes
        code_raw = _recoder_raw_value(code)
        if raw isa Real && code_raw isa Real
            isapprox(Float64(raw), Float64(code_raw); atol = 1e-8) && return true
        elseif raw == code_raw
            return true
        end
    end
    return false
end

function trichotomize_ideology_value(x)
    raw = _recoder_raw_value(x)
    ismissing(raw) && return missing
    raw isa Real || return raw

    value = Float64(raw)
    if value <= 3
        return -1
    elseif value <= 6
        return 0
    elseif value <= 10
        return 1
    else
        return raw
    end
end

function trichotomize_ideology_column!(df::DataFrame, source, target;
                                       missing_codes = (),
                                       missing_value = 99,
                                       ordered = true,
                                       levels = [-1, 0, 1, 99])
    df[!, target] = [
        _matches_recode_code(x, missing_codes) ?
            missing_value :
            trichotomize_ideology_value(x)
        for x in df[!, source]
    ]
    df[!, target] = categorical(df[!, target]; ordered = ordered, levels = levels)
    return df
end

function binarize_thermometer_value(x; threshold = 5.0, missing_codes = ())
    raw = _recoder_raw_value(x)
    ismissing(raw) && return missing
    _matches_recode_code(raw, missing_codes) && return 99.0
    raw isa Real || return 99.0

    value = Float64(raw)
    if value < threshold
        return 0.0
    elseif value <= 10
        return 1.0
    else
        return 99.0
    end
end

function binarize_thermometer_column!(df::DataFrame, source, target;
                                      threshold = 5.0,
                                      missing_codes = ())
    df[!, target] = [
        binarize_thermometer_value(x; threshold = threshold, missing_codes = missing_codes)
        for x in df[!, source]
    ]
    return df
end

function categorical_from_column!(df::DataFrame, target, source;
                                  ordered = false,
                                  levels = nothing)
    df[!, target] = levels === nothing ?
        categorical(df[!, source]; ordered = ordered) :
        categorical(df[!, source]; ordered = ordered, levels = levels)
    return df
end

normalize_candidate_score_columns!(df::DataFrame, candidates) =
    normalize_eseb_score_columns!(df, candidates)

function lula_score_group_value(x)
    normalized = normalize_eseb_score(x)
    normalized === missing && return missing

    if 0 <= normalized <= 3
        return "low_lula"
    elseif 4 <= normalized <= 6
        return "medium_lula"
    elseif 7 <= normalized <= 10
        return "high_lula"
    else
        return missing
    end
end

function lula_score_group_column!(df::DataFrame, source = :Lula, target = :LulaScoreGroup)
    groups = Vector{Union{Missing,String}}(lula_score_group_value.(df[!, source]))
    df[!, target] = categorical(
        groups;
        ordered = true,
        levels = ["low_lula", "medium_lula", "high_lula"],
    )
    return df
end

function _prepare_e2022_df!(df_e22::DataFrame, candidates)
    rename!(df_e22, Dict(zip(build_numbered_symbols("Q17_", 13), candidates)))
    normalize_candidate_score_columns!(df_e22, candidates)

    df_e22.D10 = [
        _matches_recode_code(x, (99.0, 100.0, 101.0, 102.0)) ? 95.0 :
        _matches_recode_code(x, (96.0,)) ? 97.0 :
        _matches_recode_code(x, (98.0,)) ? 99.0 :
        x
        for x in df_e22.D10
    ]
    categorical_from_column!(df_e22, :Religion, :D10)

    categorical_from_column!(df_e22, :Sex, :D02)

    df_e22.D12a = [
        _matches_recode_code(x, (97.0, 98.0)) ? 99.0 : x
        for x in df_e22.D12a
    ]
    categorical_from_column!(df_e22, :Race, :D12a)

    trichotomize_ideology_column!(
        df_e22, :Q19, :Ideology;
        missing_codes = (95.0, 96.0, 98.0),
    )

    binarize_thermometer_column!(
        df_e22, :Q18_5, :PT;
        missing_codes = (95.0, 96.0, 97.0, 98.0),
    )

    df_e22.Q31_7 = [
        _matches_recode_code(x, (97.0, 98.0)) ? 99.0 : x
        for x in df_e22.Q31_7
    ]
    df_e22.Abortion = df_e22.Q31_7

    df_e22.Age = categorical(Int.(df_e22.D01A_FX_ID))
    df_e22.Income = categorical(Int.(df_e22.D09a_FX_RENDAF))
    df_e22.Education = categorical(Int.(df_e22.D03))

    return df_e22
end

function _prepare_e2006_df!(df_e06::DataFrame, candidates)
    rename!(df_e06, Dict(zip(build_letter_symbols("eseb16", ['a', 'b', 'c', 'd', 'e', 'f']), candidates)))
    normalize_candidate_score_columns!(df_e06, candidates)

    df_e06.peso = df_e06.peso_1
    categorical_from_column!(df_e06, :Sex, :SEXO)

    binarize_thermometer_column!(
        df_e06, :eseb15a, :PT;
        missing_codes = (11.0, 77.0),
    )

    trichotomize_ideology_column!(
        df_e06, :eseb19, :Ideology;
        missing_codes = (66.0, 77.0),
    )

    df_e06.Age = categorical(Int.(df_e06.FX_IDADE))
    df_e06.Education = categorical(Int.(df_e06.instru))
    df_e06.Income = categorical([ismissing(x) ? 10 : x for x in df_e06.renda1])

    return df_e06
end

function _prepare_e2018_df!(df_e18::DataFrame, candidates)
    rename!(df_e18, Dict(zip(build_numbered_symbols("Q16", 21; minwidth = 2), candidates)))
    normalize_candidate_score_columns!(df_e18, candidates)

    if !("Lula" in names(df_e18))
        throw(ArgumentError(
            "2018 preprocessing needs source column `Lula` to construct `LulaScoreGroup`, " *
            "but `Lula` is absent after candidate-column renaming.",
        ))
    end

    lula_score_group_column!(df_e18, :Lula, :LulaScoreGroup)
    valid_lula_groups = collect(skipmissing(df_e18.LulaScoreGroup))
    isempty(valid_lula_groups) && @warn "LulaScoreGroup has no valid rows after excluding missing and 96/97/98/99 Lula scores."
    for level in levels(df_e18.LulaScoreGroup)
        count(group -> String(group) == String(level), valid_lula_groups) == 0 &&
            @warn "LulaScoreGroup level $(level) has no valid rows."
    end

    df_e18.D10 = [
        _matches_recode_code(x, (97.0,)) ? 96.0 :
        _matches_recode_code(x, (98.0,)) ? 99.0 :
        x
        for x in df_e18.D10
    ]
    categorical_from_column!(df_e18, :D10, :D10)
    df_e18.Religion = df_e18.D10

    categorical_from_column!(df_e18, :Sex, :D2_SEXO)

    df_e18.D12A = [
        _matches_recode_code(x, (8.0, 9.0)) ? 9.0 : x
        for x in df_e18.D12A
    ]
    df_e18.Race = df_e18.D12A

    trichotomize_ideology_column!(
        df_e18, :Q18, :Ideology;
        missing_codes = (95.0, 97.0, 98.0),
    )

    binarize_thermometer_column!(
        df_e18, :Q1513, :PT;
        missing_codes = (96.0, 97.0, 98.0),
    )

    df_e18.Age = categorical(Int.(df_e18.D1A_FAIXAID))
    df_e18.Education = categorical(Int.(df_e18.D3_ESCOLA))
    df_e18.Income = categorical([ismissing(x) ? 10 : x for x in df_e18.D9B_FAIXA_RENDAF])

    return df_e18
end


function load_and_prepare_scores_df(data_path::String; candidates = CANDIDATOS_eseb2022)
    # Load SPSS file
    df_e22 = load_spss_file(data_path)

    # Metadata
    #PARTIDOS = ["PDT", "PL", "PODEMOS", "PP", "PT", "PSB", "PSD", "PSDB", "PSOL", "REDE", "REP", "UB", "MDB"]

    return _prepare_e2022_df!(df_e22, candidates)
end


function load_and_prepare_e2006(df_path; candidates = candidates2006)
    df_e06 = load_spss_file(df_path)
    return _prepare_e2006_df!(df_e06, candidates)
end


function load_and_prepare_e2018(df_path; candidates = candidates2018)
    df_e18 = load_spss_file(df_path)
    return _prepare_e2018_df!(df_e18, candidates)
end
