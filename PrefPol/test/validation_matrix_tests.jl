using Test
using DataFrames
using OrderedCollections: OrderedDict
using PrefPol

function _prefpol_src_texts()
    src_dir = normpath(joinpath(@__DIR__, "..", "src"))
    paths = filter(path -> endswith(path, ".jl"), readdir(src_dir; join = true))
    return Dict(basename(path) => read(path, String) for path in paths)
end

function _match_files(texts::Dict{String,String}, rx::Regex)
    files = String[]
    for (file, text) in texts
        occursin(rx, text) && push!(files, file)
    end
    return sort(files)
end

function _match_count(texts::Dict{String,String}, rx::Regex)
    return sum(length(collect(eachmatch(rx, text))) for text in values(texts))
end

function _validation_ranking_dict(order::Vector{Symbol})
    return Dict(candidate => rank for (rank, candidate) in enumerate(order))
end

function _validation_bundle(groups::Vector{String})
    df = DataFrame(
        profile = [
            _validation_ranking_dict([:A, :B, :C]),
            _validation_ranking_dict([:A, :B, :C]),
            _validation_ranking_dict([:C, :B, :A]),
            _validation_ranking_dict([:C, :B, :A]),
        ],
        grp = groups,
    )
    metadata!(df, "candidates", [:A, :B, :C])
    metadata!(df, "profile_kind", "linearized")
    return PrefPol.Preferences.dataframe_to_annotated_profile(df)
end

function _write_validation_config(path;
                                  year = 2022,
                                  max_candidates = 3,
                                  n_alternatives = max_candidates,
                                  m_values = [2, max_candidates],
                                  candidates = ["A", "B", "C"],
                                  scenarios = [("all", String[])])
    open(path, "w") do io
        println(io, "year = $year")
        println(io, "data_loader = \"__validation_loader__\"")
        println(io, "data_file = \"unused.sav\"")
        println(io, "max_candidates = $max_candidates")
        println(io, "m_values_range = [", join(m_values, ", "), "]")
        println(io, "n_bootstrap = 1")
        println(io, "n_alternatives = $n_alternatives")
        println(io, "rng_seed = 123")
        println(io, "candidates = [", join(("\"$c\"" for c in candidates), ", "), "]")
        println(io, "demographics = [\"grp\"]")
        println(io, "forced_scenarios = [")
        for (name, forced) in scenarios
            println(io, "  { name = \"$name\", candidates = [",
                    join(("\"$c\"" for c in forced), ", "), "] },")
        end
        println(io, "]")
    end
    return path
end

@eval PrefPol begin
    function __validation_loader__(path; candidates)
        return DataFrame(
            A = [10, 10, 96, 96],
            B = [9, 96, 9, 96],
            C = [8, 8, 8, 8],
            peso = [1.0, 10.0, 1.0, 10.0],
            grp = ["x", "x", "y", "y"],
        )
    end
end

@testset "prompt 10 static validation matrix" begin
    prefpol_src = _prefpol_src_texts()
    prefs_src_dir = normpath(joinpath(@__DIR__, "..", "..", "Preferences", "src"))
    prefs_src = Dict(
        basename(path) => read(path, String)
        for path in filter(path -> endswith(path, ".jl"), readdir(prefs_src_dir; join = true))
    )

    @test _match_count(prefpol_src, r"_normalize_score|_is_missing_score_value|function recode19|function recodeQ18|build_column_symbols\(|build_letter_column_symbols\(") == 0
    @test _match_files(prefpol_src, r"function compute_candidate_set|function select_top_candidates|function get_most_known_candidates|^dont_know_her ="m) == ["legacy_preprocessing.jl"]
    @test _match_files(prefpol_src, r"function compute_global_candidate_set|function compute_weighted_dont_know_her|function _select_top_candidates_from_poplist") == ["preprocessing_general.jl"]
    @test _match_files(prefpol_src, r"function _candidate_order") == ["survey_config.jl"]
    @test _match_count(prefpol_src, r"RAW_PROFILE_SUPPORTED_YEARS|_RAW_PROFILE_CFG_DIR|function _election_cfg\(") == 0
    @test _match_files(prefpol_src, r"const LULA_SCORE_GROUP_LEVELS|function lula_score_group_value|function lula_score_group_column|function add_lula_score_group!") == ["eseb_semantics.jl"]
    @test _match_count(prefpol_src, r"if :C in spec\.measures|if :D in spec\.measures") == 0
    @test _match_files(prefs_src, r"grouped_geometric_index\(C::Real, D::Real\)|separation_ratio\(D::Real, W::Real\)|group_coherence_from_within_dispersion\(W::Real\)|within_dispersion_from_group_coherence\(C::Real\)") == ["PreferenceConsensus.jl"]
    @test _match_count(prefpol_src, r"_normalize_group_coherence|_within_dispersion_from_normalized_C|_separation_ratio|sqrt\(max\(C \* D") == 0
    @test _match_count(prefs_src, r"function _ranking_masses\(p::Profile|function _ranking_masses\(p::WeightedProfile|function _pairwise_preference_counts\(p::Profile|function _pairwise_preference_counts\(p::WeightedProfile") == 0
end

@testset "prompt 10 config validation catches drift-prone inputs" begin
    mktempdir() do dir
        @test_throws ArgumentError PrefPol.load_election_cfg(_write_validation_config(
            joinpath(dir, "duplicate-candidates.toml");
            candidates = ["A", "A", "C"],
        ))
        @test_throws ArgumentError PrefPol.load_election_cfg(_write_validation_config(
            joinpath(dir, "duplicate-forced.toml");
            scenarios = [("bad", ["A", "A"])],
        ))
        @test_throws ArgumentError PrefPol.load_election_cfg(_write_validation_config(
            joinpath(dir, "unknown-forced.toml");
            scenarios = [("bad", ["Z"])],
        ))
        @test_throws ArgumentError PrefPol.load_election_cfg(_write_validation_config(
            joinpath(dir, "ambiguous-default-m.toml");
            max_candidates = 3,
            n_alternatives = 2,
        ))
        err = try
            PrefPol.load_election_cfg(_write_validation_config(
                joinpath(dir, "lula-active.toml");
                year = 2018,
                candidates = ["Lula", "Fernando_Haddad", "Jair_Bolsonaro"],
                scenarios = [("main_2018", ["Lula"])],
            ))
        catch err
            err
        end
        @test err isa ArgumentError
        @test occursin("LulaScoreGroup", sprint(showerror, err))
    end
end

@testset "prompt 10 candidate resolution is independent of legacy globals" begin
    mktempdir() do dir
        cfg_path = _write_validation_config(
            joinpath(dir, "2022.toml");
            scenarios = [("force_b", ["B"])],
        )
        wave = PrefPol.load_survey_wave_config(cfg_path; wave_id = "validation")

        PrefPol.dont_know_her = [("A", 0.0), ("B", 0.0), ("C", 100.0)]
        @test PrefPol.resolve_active_candidate_set(wave; m = 2) == ["C", "A"]
        @test PrefPol.resolve_active_candidate_set(wave; scenario_name = "force_b", m = 2) == ["B", "C"]

        @test_throws ArgumentError PrefPol.resolve_active_candidate_set(wave; active_candidates = ["A", "B"], m = 2)
        @test_throws ArgumentError PrefPol.resolve_active_candidate_set(wave; active_candidates = ["A", "B"], scenario_name = "force_b")
    end
end

@testset "prompt 10 grouped algebra boundary is owned by Preferences" begin
    bundle = _validation_bundle(["x", "x", "y", "y"])
    details = PrefPol.compute_group_measure_details(bundle, :grp)

    @test isapprox(details.W, PrefPol.Preferences.within_dispersion_from_group_coherence(details.C); atol = 1e-12)
    @test isapprox(details.C, PrefPol.Preferences.group_coherence_from_within_dispersion(details.W); atol = 1e-12)
    @test isapprox(details.S, PrefPol.Preferences.overall_sstar_from_CD(details.C, details.D); atol = 1e-12)
    @test isapprox(details.G, PrefPol.Preferences.grouped_geometric_index(details.C, details.D); atol = 1e-12)
    @test isapprox(details.lambda_sep, PrefPol.Preferences.separation_ratio(details.D, details.W); atol = 1e-12)
    @test details.diagnostics.n_groups == 2
    @test length(details.diagnostics.group_components) == 2

    one_group = PrefPol.compute_group_measure_details(_validation_bundle(fill("x", 4)), :grp)
    @test one_group.D == 0.0
    @test one_group.D_lo == 0.0
    @test one_group.D_hi == 0.0
    @test one_group.E == 0.0
    @test one_group.diagnostics.n_groups == 1
    @test isapprox(one_group.S, PrefPol.Preferences.overall_sstar_from_CD(one_group.C, one_group.D); atol = 1e-12)
end
