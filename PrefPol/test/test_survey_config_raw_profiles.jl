using Test
using PrefPol
using DataFrames

const _SURVEY_REFACTOR_DF = DataFrame(
    A = [10, 96, 96, 7, 5],
    B = [8, 97, 4, missing, 5],
    C = [6, 98, 3, 2, 5],
    D = [4, 99, missing, 1, 5],
    peso = [1.0, 2.0, 3.0, 4.0, 5.0],
    grp = ["x", "x", "y", "y", "x"],
)

@eval PrefPol begin
    function __survey_refactor_loader__(path; candidates)
        return deepcopy($(_SURVEY_REFACTOR_DF))
    end

    function __survey_refactor_echo_loader__(path; candidates)
        return DataFrame(
            loader_path = [String(path)],
            candidate_count = [length(candidates)],
            candidate_join = [join(candidates, "|")],
        )
    end
end

function _survey_captured_exception(f)
    try
        f()
    catch err
        return err
    end
    return nothing
end

function _year_config_paths()
    config_dir = joinpath(PrefPol.project_root, "config")
    return sort(filter(path -> occursin(r"^[0-9]+\.toml$", basename(path)),
                       readdir(config_dir; join = true)))
end

function _synthetic_df_for(candidates::Vector{String})
    df = DataFrame()
    for (i, cand) in enumerate(candidates)
        df[!, Symbol(cand)] = Union{Missing,Int}[
            10 - mod(i, 4),
            8 - mod(i, 3),
            96,
            missing,
            mod(i + 3, 11),
        ]
    end
    df[!, :peso] = [1.0, 2.0, 3.0, 4.0, 5.0]
    return df
end

function _write_test_config(path;
                            year = 2006,
                            max_candidates = 4,
                            m_values = [2, 3, 4],
                            n_alternatives = 4,
                            candidates = ["A", "B", "C", "D"],
                            demographics = ["grp"],
                            scenarios = [("front", ["D"]), ("pair", ["B", "A"])])
    open(path, "w") do io
        println(io, "year = $year")
        println(io, "data_loader = \"__survey_refactor_loader__\"")
        println(io, "data_file = \"unused.sav\"")
        println(io, "max_candidates = $max_candidates")
        println(io, "m_values_range = [", join(m_values, ", "), "]")
        println(io, "n_bootstrap = 1")
        println(io, "n_alternatives = $n_alternatives")
        println(io, "rng_seed = 123")
        println(io, "candidates = [", join(("\"$c\"" for c in candidates), ", "), "]")
        println(io, "demographics = [", join(("\"$d\"" for d in demographics), ", "), "]")
        println(io, "forced_scenarios = [")
        for (name, cands) in scenarios
            println(io, "  { name = \"$name\", candidates = [",
                    join(("\"$c\"" for c in cands), ", "), "] },")
        end
        println(io, "]")
    end
    return path
end

@testset "survey config/raw-profile consolidation" begin
    @testset "config parsing and SurveyWaveConfig equivalence" begin
        for path in _year_config_paths()
            cfg = PrefPol.load_election_cfg(path)
            wcfg = PrefPol.load_survey_wave_config(path)
            wcfg2 = PrefPol.SurveyWaveConfig(cfg)

            @test wcfg.year == cfg.year == wcfg2.year
            @test wcfg.data_loader == cfg.data_loader == wcfg2.data_loader
            @test wcfg.data_file == cfg.data_file == wcfg2.data_file
            @test wcfg.candidate_universe == cfg.candidates == wcfg2.candidate_universe
            @test wcfg.demographic_cols == cfg.demographics == wcfg2.demographic_cols
            @test wcfg.scenario_candidates == Dict(sc.name => sc.candidates for sc in cfg.scenarios)
            @test wcfg.scenario_candidates == wcfg2.scenario_candidates
            @test wcfg.max_candidates == cfg.max_candidates == wcfg2.max_candidates
            @test wcfg.default_m == cfg.n_alternatives == wcfg2.default_m
            @test wcfg.default_seed == cfg.rng_seed == wcfg2.default_seed
        end
    end

    @testset "internal config interface accessors" begin
        for path in _year_config_paths()
            cfg = PrefPol.load_election_cfg(path)
            wcfg = PrefPol.load_survey_wave_config(path; wave_id = "wave-$(cfg.year)")
            expected_scenarios = Dict(sc.name => Vector{String}(sc.candidates) for sc in cfg.scenarios)

            for obj in (cfg, wcfg)
                @test PrefPol._config_year(obj) == cfg.year
                @test PrefPol._config_data_loader(obj) == cfg.data_loader
                @test PrefPol._config_data_file(obj) == cfg.data_file
                @test PrefPol._config_candidate_universe(obj) == cfg.candidates
                @test PrefPol._config_demographic_cols(obj) == cfg.demographics
                @test PrefPol._config_scenario_candidates(obj) == expected_scenarios
                @test PrefPol._config_max_candidates(obj) == cfg.max_candidates
                @test PrefPol._config_default_m(obj) == cfg.n_alternatives
                @test PrefPol._config_default_seed(obj) == cfg.rng_seed
                @test occursin(string(cfg.year), PrefPol._config_context(obj))
            end
        end

        mktempdir() do dir
            cfg_path = _write_test_config(joinpath(dir, "2006.toml"))
            cfg = PrefPol.load_election_cfg(cfg_path)
            wcfg = PrefPol.load_survey_wave_config(cfg_path)

            from_cfg = PrefPol._load_config_data(cfg)
            from_wave = PrefPol._load_config_data(wcfg)
            public_from_cfg = PrefPol.load_election_data(cfg)
            public_from_wave = PrefPol.load_wave_data(wcfg)

            @test names(from_cfg) == names(_SURVEY_REFACTOR_DF)
            @test names(from_wave) == names(_SURVEY_REFACTOR_DF)
            @test nrow(from_cfg) == nrow(_SURVEY_REFACTOR_DF)
            @test nrow(from_wave) == nrow(_SURVEY_REFACTOR_DF)
            @test isequal(from_cfg, public_from_cfg)
            @test isequal(from_wave, public_from_wave)
            @test PrefPol._scenario_force_include(cfg, "front") == ["D"]
            @test PrefPol._scenario_force_include(wcfg, "pair") == ["B", "A"]
            @test_throws ArgumentError PrefPol._scenario_force_include(cfg, "absent")
            @test_throws ArgumentError PrefPol._scenario_force_include(wcfg, "absent")

            echo_cfg = PrefPol.ElectionConfig(
                2099,
                "__survey_refactor_echo_loader__",
                "/tmp/synthetic-loader.sav",
                2,
                [2],
                1,
                2,
                123,
                ["B", "A"],
                ["grp"],
                [PrefPol.Scenario("all", String[])],
            )
            echo_wave = PrefPol.SurveyWaveConfig(echo_cfg; wave_id = "echo-wave")
            direct_echo = PrefPol._call_configured_loader(
                :__survey_refactor_echo_loader__,
                echo_cfg.data_file,
                echo_cfg.candidates;
                context = "direct echo",
            )
            election_echo = PrefPol.load_election_data(echo_cfg)
            wave_echo = PrefPol.load_wave_data(echo_wave)

            @test isequal(direct_echo, election_echo)
            @test isequal(election_echo, wave_echo)
            @test only(election_echo.loader_path) == echo_cfg.data_file
            @test only(election_echo.candidate_count) == 2
            @test only(election_echo.candidate_join) == "B|A"

            missing_cfg = PrefPol.ElectionConfig(
                2099,
                "__missing_survey_refactor_loader__",
                "/tmp/missing-loader.sav",
                2,
                [2],
                1,
                2,
                123,
                ["A", "B"],
                ["grp"],
                [PrefPol.Scenario("all", String[])],
            )
            missing_err = _survey_captured_exception() do
                PrefPol.load_election_data(missing_cfg)
            end
            @test missing_err isa ArgumentError
            @test occursin("__missing_survey_refactor_loader__", sprint(showerror, missing_err))
            @test occursin("year 2099", sprint(showerror, missing_err))

            missing_wave_err = _survey_captured_exception() do
                PrefPol.load_wave_data(PrefPol.SurveyWaveConfig(missing_cfg; wave_id = "missing-wave"))
            end
            @test missing_wave_err isa ArgumentError
            @test occursin("missing-wave", sprint(showerror, missing_wave_err))
        end
    end

    @testset "candidate resolution equivalence" begin
        for path in _year_config_paths()
            cfg = PrefPol.load_election_cfg(path)
            wcfg = PrefPol.load_survey_wave_config(path)
            df = _synthetic_df_for(cfg.candidates)
            m_values = sort(unique(filter(m -> 1 <= m <= cfg.max_candidates,
                                          [cfg.max_candidates; cfg.m_values_range; cfg.n_alternatives])))

            for m in m_values
                @test PrefPol._candidate_order(cfg; m = m, data = df) ==
                      PrefPol._candidate_order(wcfg; m = m, data = df)
                @test PrefPol._resolve_candidate_cols(df, cfg; m = m) ==
                      PrefPol.resolve_active_candidate_set(wcfg; m = m, data = df)

                for sc in cfg.scenarios
                    @test PrefPol._candidate_order(cfg; scenario = sc.name, m = m, data = df) ==
                          PrefPol._candidate_order(wcfg; scenario = sc.name, m = m, data = df)

                    from_cfg = PrefPol._resolve_candidate_cols(df, cfg; scenario_name = sc.name, m = m)
                    from_wave = PrefPol.resolve_active_candidate_set(wcfg; scenario_name = sc.name, m = m, data = df)
                    raw_cfg = PrefPol.load_raw_pref_data(cfg; scenario_name = sc.name, m = m, data = df)
                    raw_wave = PrefPol.load_raw_pref_data(wcfg; scenario_name = sc.name, m = m, data = df)

                    @test from_cfg == from_wave
                    @test raw_cfg.candidate_cols == from_wave
                    @test raw_wave.candidate_cols == from_wave
                end
            end

            exact = reverse(cfg.candidates[1:2])
            @test PrefPol._resolve_candidate_cols(df, cfg; candidate_set = exact) == exact
            @test PrefPol.resolve_active_candidate_set(wcfg; active_candidates = exact, data = df) == exact
            @test_throws ArgumentError PrefPol._resolve_candidate_cols(df, cfg; candidate_set = ["not_a_candidate"])
            @test_throws ArgumentError PrefPol._resolve_candidate_cols(df, wcfg; candidate_set = ["not_a_candidate"])
            @test_throws ArgumentError PrefPol._resolve_candidate_cols(df, wcfg; candidate_set = exact, active_candidates = exact)
            @test_throws ArgumentError PrefPol._resolve_candidate_cols(df, wcfg; active_candidates = exact, scenario_name = first(cfg.scenarios).name)
            @test_throws ArgumentError PrefPol._resolve_candidate_cols(df, wcfg; k = 2, m = 3)
        end
    end

    @testset "canonical resolver weighted ordering and pipeline specs" begin
        cfg = PrefPol.ElectionConfig(
            2022,
            "__survey_refactor_loader__",
            "/tmp/unused",
            4,
            [2, 3, 4],
            1,
            4,
            123,
            ["A", "B", "C", "D"],
            ["grp"],
            [PrefPol.Scenario("front", ["D"]),
             PrefPol.Scenario("crowded", ["D", "B", "A"])],
        )
        wcfg = PrefPol.SurveyWaveConfig(cfg; wave_id = "canonical-candidates")

        @test PrefPol.compute_global_candidate_set(
            _SURVEY_REFACTOR_DF;
            candidate_cols = wcfg.candidate_universe,
            m = 3,
            weights = _SURVEY_REFACTOR_DF.peso,
        ) == ["C", "A", "D"]
        @test PrefPol.compute_global_candidate_set(
            _SURVEY_REFACTOR_DF;
            candidate_cols = wcfg.candidate_universe,
            m = 2,
            force_include = ["D", "B", "A"],
            weights = _SURVEY_REFACTOR_DF.peso,
        ) == ["D", "B"]

        @test PrefPol.resolve_active_candidate_set(wcfg; m = 3) == ["C", "A", "D"]
        @test PrefPol.resolve_active_candidate_set(wcfg; scenario_name = "front", m = 3) == ["D", "C", "A"]
        @test PrefPol.resolve_active_candidate_set(wcfg; scenario_name = "crowded", m = 2) == ["D", "B"]
        @test PrefPol.resolve_active_candidate_set(wcfg; active_candidates = ["B", "A"]) == ["B", "A"]
        @test_throws ArgumentError PrefPol.resolve_active_candidate_set(wcfg; active_candidates = ["B", "Z"])
        @test_throws ArgumentError PrefPol.resolve_active_candidate_set(wcfg; active_candidates = ["B", "A"], m = 2)
        @test_throws ArgumentError PrefPol.resolve_active_candidate_set(wcfg; scenario_name = "missing", m = 2)
        @test_throws ArgumentError PrefPol.resolve_active_candidate_set(wcfg; m = 5)

        spec = PrefPol.build_pipeline_spec(wcfg;
                                           scenario_name = "front",
                                           m = 3,
                                           groupings = Symbol[],
                                           measures = [:Psi],
                                           B = 1,
                                           R = 1,
                                           K = 1,
                                           imputer_backend = :zero)
        @test spec.active_candidates == PrefPol.resolve_active_candidate_set(wcfg; scenario_name = "front", m = 3)
    end

    @testset "year-config pipeline specs match resolver when raw data are available" begin
        for path in _year_config_paths()
            cfg = PrefPol.load_election_cfg(path)
            if !isfile(cfg.data_file)
                @info "Skipping year-config candidate-set integration; raw data unavailable." year = cfg.year data_file = cfg.data_file
                @test_skip false
                continue
            end

            wcfg = PrefPol.load_survey_wave_config(path)
            for m in cfg.m_values_range
                @test PrefPol.build_pipeline_spec(wcfg;
                                                  m = m,
                                                  groupings = Symbol[],
                                                  measures = [:Psi],
                                                  B = 1,
                                                  R = 1,
                                                  K = 1,
                                                  imputer_backend = :zero).active_candidates ==
                      PrefPol.resolve_active_candidate_set(wcfg; m = m)
                for sc in cfg.scenarios
                    @test PrefPol.build_pipeline_spec(wcfg;
                                                      scenario_name = sc.name,
                                                      m = m,
                                                      groupings = Symbol[],
                                                      measures = [:Psi],
                                                      B = 1,
                                                      R = 1,
                                                      K = 1,
                                                      imputer_backend = :zero).active_candidates ==
                          PrefPol.resolve_active_candidate_set(wcfg; scenario_name = sc.name, m = m)
                end
            end
        end
    end

    @testset "year discovery and default config paths" begin
        tracked_paths = _year_config_paths()
        tracked_years = sort(unique(PrefPol.load_election_cfg(path).year for path in tracked_paths))

        @test PrefPol.available_election_years() == tracked_years
        for year in tracked_years
            path = PrefPol.default_config_path(year)
            @test isfile(path)
            @test PrefPol.load_election_cfg(path).year == year
        end
        @test_throws ArgumentError PrefPol.default_config_path(1999)

        mktempdir() do dir
            path_2006 = _write_test_config(joinpath(dir, "2006.toml"), year = 2006)
            path_2018 = _write_test_config(joinpath(dir, "2018.toml"), year = 2018)
            _write_test_config(joinpath(dir, "not_numeric.toml"), year = 2022)

            @test PrefPol.available_election_years(config_dir = dir) == [2006, 2018]
            @test PrefPol.default_config_path(2006; config_dir = dir) == path_2006
            @test PrefPol.default_config_path(2018; config_dir = dir) == path_2018
            @test_throws ArgumentError PrefPol.default_config_path(2022; config_dir = dir)
            @test_throws ArgumentError PrefPol.load_raw_pref_data(2018; config_path = path_2006)
        end
    end

    @testset "raw loading APIs and backward-compatible calls" begin
        mktempdir() do dir
            cfg_path = _write_test_config(joinpath(dir, "2006.toml"))
            cfg = PrefPol.load_election_cfg(cfg_path)
            wcfg = PrefPol.load_survey_wave_config(cfg_path)

            by_year = PrefPol.load_raw_pref_data(2006; config_path = cfg_path, scenario_name = "front", k = 3)
            by_cfg = PrefPol.load_raw_pref_data(cfg; scenario_name = "front", m = 3)
            by_wave = PrefPol.load_raw_pref_data(wcfg; scenario_name = "front", m = 3)
            resolved = PrefPol.resolve_active_candidate_set(wcfg; scenario_name = "front", m = 3)

            for raw in (by_year, by_cfg, by_wave)
                @test raw.year == 2006
                @test raw.candidate_cols == resolved
                @test raw.candidate_labels == resolved
                @test raw.weight_col == :peso
                @test raw.m == 3
            end

            @test by_year.candidate_cols == by_wave.candidate_cols
            @test by_year.cfg isa PrefPol.ElectionConfig
            @test by_cfg.cfg === cfg
            @test by_wave.cfg === nothing
            @test by_year.wave_config isa PrefPol.SurveyWaveConfig
            @test by_cfg.wave_config.year == cfg.year
            @test by_wave.wave_config === wcfg
            @test PrefPol.load_raw_pref_data(2006; config_path = cfg_path, candidate_set = ["B", "A"]).candidate_cols == ["B", "A"]
            @test PrefPol.load_raw_pref_data(2006; config_path = cfg_path, scenario_name = "front", k = 3).candidate_cols ==
                  PrefPol.load_raw_pref_data(wcfg; scenario_name = "front", m = 3).candidate_cols
            @test_throws ArgumentError PrefPol.load_raw_pref_data(wcfg; scenario_name = "front", k = 2, m = 3)

            prof_year = PrefPol.build_profile(_SURVEY_REFACTOR_DF, 2006;
                                              config_path = cfg_path,
                                              candidate_set = ["A", "B"],
                                              allow_ties = true,
                                              allow_incomplete = true)
            prof_cfg = PrefPol.build_profile(_SURVEY_REFACTOR_DF, cfg;
                                             candidate_set = ["A", "B"],
                                             allow_ties = true,
                                             allow_incomplete = true)
            prof_wave = PrefPol.build_profile(_SURVEY_REFACTOR_DF, wcfg;
                                              candidate_set = ["A", "B"],
                                              allow_ties = true,
                                              allow_incomplete = true)
            prof_raw = PrefPol.build_profile(by_wave; candidate_set = by_wave.candidate_cols[1:2], allow_ties = true)

            @test PrefPol.Preferences.nballots(prof_year) == PrefPol.Preferences.nballots(prof_cfg)
            @test PrefPol.Preferences.nballots(prof_cfg) == PrefPol.Preferences.nballots(prof_wave)
            @test PrefPol.Preferences.nballots(prof_raw) == PrefPol.Preferences.nballots(prof_wave)
        end
    end

    @testset "synthetic ESEB score normalization and profile construction" begin
        cfg = PrefPol.ElectionConfig(
            2022,
            "__survey_refactor_loader__",
            "/tmp/unused",
            4,
            [2, 3, 4],
            1,
            4,
            123,
            ["A", "B", "C", "D"],
            ["grp"],
            [PrefPol.Scenario("front", ["D"])],
        )

        @test PrefPol.normalize_eseb_score(96) === missing
        @test PrefPol.normalize_eseb_score(97) === missing
        @test PrefPol.normalize_eseb_score(98) === missing
        @test PrefPol.normalize_eseb_score(99) === missing
        @test PrefPol.normalize_eseb_score(11) === missing
        @test PrefPol.normalize_eseb_score(-1) === missing
        @test PrefPol.normalize_eseb_score("7") == 7.0

        prof = PrefPol.build_profile(_SURVEY_REFACTOR_DF, cfg;
                                     candidate_set = ["A", "B", "C", "D"],
                                     allow_ties = true,
                                     allow_incomplete = true)
        @test PrefPol.Preferences.nballots(prof) == 4

        prof_complete = PrefPol.build_profile(_SURVEY_REFACTOR_DF, cfg;
                                              candidate_set = ["A", "B", "C", "D"],
                                              allow_ties = true,
                                              allow_incomplete = false)
        @test PrefPol.Preferences.nballots(prof_complete) == 2

        prof_w = PrefPol.build_profile(_SURVEY_REFACTOR_DF, cfg;
                                       weighted = true,
                                       candidate_set = ["A", "B", "C", "D"],
                                       allow_ties = true,
                                       allow_incomplete = true)
        @test PrefPol.Preferences.nballots(prof_w) == 4
        @test PrefPol.Preferences.weights(prof_w) == [1.0, 3.0, 4.0, 5.0]

        patterns = PrefPol.Preferences.profile_pattern_proportions(prof; weighted = false)
        @test any(occursin("A~B~C~D", String(row.pattern)) for row in eachrow(patterns))

        no_weight = select(_SURVEY_REFACTOR_DF, Not(:peso))
        raw_no_weight = (df = no_weight, candidate_cols = ["A", "B"], candidate_labels = ["A", "B"])
        @test_throws ArgumentError PrefPol.build_profile(raw_no_weight; weighted = true)
    end

    @testset "nested pipeline integration uses survey_config resolver" begin
        cfg = PrefPol.ElectionConfig(
            2022,
            "__survey_refactor_loader__",
            "/tmp/unused",
            4,
            [2, 3, 4],
            1,
            4,
            123,
            ["A", "B", "C", "D"],
            ["grp"],
            [PrefPol.Scenario("front", ["D"])],
        )
        wcfg = PrefPol.SurveyWaveConfig(cfg; wave_id = "survey-refactor")
        active = PrefPol.resolve_active_candidate_set(wcfg; scenario_name = "front", m = 3)
        raw = PrefPol.load_raw_pref_data(wcfg; scenario_name = "front", m = 3)
        spec = PrefPol.build_pipeline_spec(wcfg;
                                           scenario_name = "front",
                                           m = 3,
                                           groupings = [:grp],
                                           measures = [:Psi],
                                           B = 1,
                                           R = 1,
                                           K = 1,
                                           imputer_backend = :zero)

        @test raw.candidate_cols == active
        @test spec.active_candidates == active

        nested_src = read(joinpath(PrefPol.project_root, "src", "nested_pipeline.jl"), String)
        @test !occursin("struct SurveyWaveConfig", nested_src)
        @test !occursin("function resolve_active_candidate_set", nested_src)
        @test !occursin("function load_survey_wave_config", nested_src)
    end

    @testset "prompt 04 source invariants" begin
        survey_src = read(joinpath(PrefPol.project_root, "src", "survey_config.jl"), String)
        @test length(collect(eachmatch(r"function _candidate_order", survey_src))) == 1
        @test !occursin(r"\b_election_cfg\s*\(", survey_src)
        @test !occursin("RAW_PROFILE_SUPPORTED_YEARS", survey_src)
    end

    @testset "regression errors" begin
        mktempdir() do dir
            good_path = _write_test_config(joinpath(dir, "good.toml"), year = 2006)
            wcfg = PrefPol.load_survey_wave_config(good_path)

            @test_throws ArgumentError PrefPol.load_raw_pref_data(1999)
            @test_throws ArgumentError PrefPol.load_raw_pref_data(2018; config_path = good_path)
            @test_throws ArgumentError PrefPol.resolve_active_candidate_set(wcfg; scenario_name = "front", m = 10)
            @test_throws ArgumentError PrefPol.resolve_active_candidate_set(wcfg; active_candidates = String[])
            @test_throws ArgumentError PrefPol.resolve_active_candidate_set(wcfg; active_candidates = ["A", "Z"])
            @test_throws ArgumentError PrefPol.resolve_active_candidate_set(wcfg; scenario_name = "missing")

            dup_scenario = _write_test_config(joinpath(dir, "dup_scenario.toml"),
                                              scenarios = [("bad", ["A", "A"])])
            unknown_scenario = _write_test_config(joinpath(dir, "unknown_scenario.toml"),
                                                  scenarios = [("bad", ["A", "Z"])])
            lula_2018 = _write_test_config(joinpath(dir, "lula_2018.toml"),
                                           year = 2018,
                                           candidates = ["A", "B", "Lula"],
                                           max_candidates = 3,
                                           m_values = [2, 3],
                                           n_alternatives = 3,
                                           scenarios = [("main_2018", ["A", "Lula"])])
            bad_m = _write_test_config(joinpath(dir, "bad_m.toml"),
                                       max_candidates = 3,
                                       m_values = [2, 4],
                                       n_alternatives = 3,
                                       candidates = ["A", "B", "C"],
                                       scenarios = [("front", ["A"])])

            @test_throws ArgumentError PrefPol.load_election_cfg(dup_scenario)
            @test_throws ArgumentError PrefPol.load_election_cfg(unknown_scenario)
            @test_throws ArgumentError PrefPol.load_election_cfg(lula_2018)
            @test_throws ArgumentError PrefPol.load_election_cfg(bad_m)
        end
    end
end
