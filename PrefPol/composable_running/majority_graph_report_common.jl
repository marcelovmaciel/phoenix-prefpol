const MGS_SCRIPT_DIR = @__DIR__
const MGS_REPO_ROOT = normpath(joinpath(MGS_SCRIPT_DIR, "..", ".."))

push!(LOAD_PATH, joinpath(MGS_REPO_ROOT, "Preferences"))

using CSV
using CategoricalArrays
using DataFrames
using Dates
using Preferences
using Printf
using Statistics
using TOML
using JLD2

empty!(LOAD_PATH)
push!(LOAD_PATH, joinpath(MGS_REPO_ROOT, "PreferencePlots"))
push!(LOAD_PATH, joinpath(MGS_REPO_ROOT, "Preferences"))
push!(LOAD_PATH, "@stdlib")
using PreferencePlots

function prefpol_module()
    try
        return Base.require(Base.PkgId(Base.UUID("bbc7be70-a2bd-43e6-84e6-8ca7df355e3e"), "PrefPol"))
    catch err
        error("Direct cache reconstruction requires loading PrefPol, but PrefPol could not be loaded: $(err)")
    end
end

Base.@kwdef struct MajorityGraphReportSpec
    year::Int
    scenario_name::String
    m::Int = 4
    input_path::Union{Nothing,String} = nothing
    output_dir::String = joinpath(MGS_SCRIPT_DIR, "output", "majority_graph_support_$(year)")
    candidates::Vector{NamedTuple} = NamedTuple[]
    reference_order::Union{Nothing,Vector{Symbol}} = nothing
    reference_rule::Symbol = :majority_order
    target::Union{Nothing,Symbol} = nothing
    opponent::Union{Nothing,Symbol} = nothing
    current_first_candidates::Union{Nothing,Vector{Symbol}} = nothing
    partitions::Vector{Symbol} = Symbol[]
    role_report_partitions::Vector{Symbol} = Symbol[]
    display_names::Dict{Symbol,String} = Dict{Symbol,String}()
    report_language::String = "en"
    validate_known_values::Bool = false
end

const ROLE_THRESHOLDS = MajorityGraphRoleThresholds()
const ROLE_AMENABILITY = :inverse
const ROLE_LAMBDA = 1.0

const PREFERRED_LABELS = Dict(
    2022 => Dict(
        "LULA" => (:Lu, "Lula"),
        "BOLSONARO" => (:Bo, "Bolsonaro"),
        "CIRO_GOMES" => (:Ci, "Ciro Gomes"),
        "SIMONE_TEBET" => (:Te, "Simone Tebet"),
        "Lula" => (:Lu, "Lula"),
        "Bolsonaro" => (:Bo, "Bolsonaro"),
        "Ciro_Gomes" => (:Ci, "Ciro Gomes"),
        "Simone_Tebet" => (:Te, "Simone Tebet"),
    ),
    2018 => Dict(
        "Fernando_Haddad" => (:Ha, "Fernando Haddad"),
        "Jair_Bolsonaro" => (:Bo, "Jair Bolsonaro"),
        "Ciro_Gomes" => (:Ci, "Ciro Gomes"),
        "Geraldo_Alckmin" => (:Al, "Geraldo Alckmin"),
        "Marina_Silva" => (:Ma, "Marina Silva"),
    ),
    2006 => Dict(
        "Lula" => (:Lu, "Lula"),
        "Geraldo_Alckmin" => (:Al, "Geraldo Alckmin"),
        "Heloísa_Helena" => (:HH, "Heloísa Helena"),
        "Heloisa_Helena" => (:HH, "Heloísa Helena"),
        "José_Serra" => (:Se, "José Serra"),
        "Jose_Serra" => (:Se, "José Serra"),
        "Cristóvam_Buarque" => (:Cr, "Cristóvam Buarque"),
        "Cristovam_Buarque" => (:Cr, "Cristóvam Buarque"),
        "Aécio_Neves" => (:Ae, "Aécio Neves"),
        "Aecio_Neves" => (:Ae, "Aécio Neves"),
    ),
)

function parse_common_args(args)
    opts = Dict{String,String}()
    flags = Set{String}()
    i = 1
    while i <= length(args)
        a = args[i]
        if a in ("--config", "--input", "--output", "--input-root", "--years",
                 "--m", "--backend", "--linearizer", "--b", "--r", "--k")
            i += 1
            i <= length(args) || error("$a requires a value")
            opts[a[3:end]] = args[i]
        elseif a == "--no-validate-known"
            push!(flags, "no_validate_known")
        else
            error("Unknown argument: $a")
        end
        i += 1
    end
    return (;
        config = get(opts, "config", joinpath(MGS_REPO_ROOT, "PrefPol", "local_config", "paper_b2.toml")),
        input = get(opts, "input", nothing),
        output = get(opts, "output", nothing),
        input_root = get(opts, "input-root", nothing),
        years = get(opts, "years", "2006,2018,2022"),
        m = parse(Int, get(opts, "m", "4")),
        backend = Symbol(get(opts, "backend", "mice")),
        linearizer = Symbol(get(opts, "linearizer", "pattern_conditional")),
        b = parse(Int, get(opts, "b", "1")),
        r = parse(Int, get(opts, "r", "1")),
        k = parse(Int, get(opts, "k", "1")),
        validate_known = !("no_validate_known" in flags),
    )
end

resolve_path(path::AbstractString) = isabspath(path) ? String(path) : joinpath(MGS_REPO_ROOT, path)

function run_roots_from_config(config_path::AbstractString)
    cfg = isfile(config_path) ? TOML.parsefile(config_path) : Dict{String,Any}()
    run = get(cfg, "run", Dict{String,Any}())
    output_root = resolve_path(string(get(run, "output_root", joinpath("PrefPol", "composable_running", "output"))))
    cache_root = resolve_path(string(get(run, "cache_root", joinpath("PrefPol", "composable_running", "output", "cache"))))
    B = Int(get(run, "B", 8))
    R = Int(get(run, "R", 2))
    K = Int(get(run, "K", 2))
    measures = Symbol.(string.(get(run, "measures", ["Psi", "R", "HHI", "RHHI", "C", "D", "O", "S", "lambda_sep"])))
    consensus_tie_policy = Symbol(string(get(run, "consensus_tie_policy", "average")))
    return (; output_root, cache_root, B, R, K, measures, consensus_tie_policy)
end

output_root_from_config(config_path::AbstractString) = run_roots_from_config(config_path).output_root

function scenario_for_year(year::Int)
    year == 2006 && return "main_2006"
    year == 2018 && return "main_2018"
    year == 2022 && return "main_2022"
    error("Unsupported majority-graph support year: $year")
end

function candidate_label_for(year::Int, raw::AbstractString, used::Set{Symbol})
    preferred = get(get(PREFERRED_LABELS, year, Dict{String,Tuple{Symbol,String}}()), raw, nothing)
    if preferred !== nothing
        label, display = preferred
    else
        parts = split(raw, "_")
        display = join(parts, " ")
        base = Symbol(join(first.(parts), ""))
        label = base
    end
    if label in used
        label = Symbol(string(label, length(used) + 1))
    end
    push!(used, label)
    return label, display
end

rank_column_candidate(col::Symbol) = replace(String(col), r"^rank_" => "")

function candidates_from_profile(df::DataFrame, year::Int; manifest_candidates=nothing)
    rank_cols = [c for c in propertynames(df) if startswith(String(c), "rank_")]
    if manifest_candidates !== nothing
        wanted = split(String(manifest_candidates), "|")
        ordered = Symbol[]
        for raw in wanted
            matches = [c for c in rank_cols if rank_column_candidate(c) == raw ||
                       replace(rank_column_candidate(c), " " => "_") == raw]
            !isempty(matches) && push!(ordered, first(matches))
        end
        length(ordered) == length(rank_cols) && (rank_cols = ordered)
    end
    isempty(rank_cols) && error("Input profile has no rank_* columns.")
    used = Set{Symbol}()
    out = NamedTuple[]
    for col in rank_cols
        raw = rank_column_candidate(col)
        label, display = candidate_label_for(year, raw, used)
        push!(out, (column=col, source=raw, label=label, display=display))
    end
    return out
end

function candidates_from_active_names(active_candidates, year::Int)
    used = Set{Symbol}()
    out = NamedTuple[]
    for raw in String.(collect(active_candidates))
        label, display = candidate_label_for(year, raw, used)
        push!(out, (column=Symbol(""), source=raw, label=label, display=display))
    end
    return out
end

function build_profile_from_rank_columns(df::DataFrame, candidates)
    pool = CandidatePool([c.label for c in candidates])
    ballots = StrictRank[]
    for row in eachrow(df)
        ranked = Tuple{Int,Symbol}[]
        for c in candidates
            push!(ranked, (Int(row[c.column]), c.label))
        end
        sort!(ranked; by=first)
        push!(ballots, StrictRank(pool, [label for (_, label) in ranked]))
    end
    return Profile(pool, ballots)
end

function relabel_profile(profile::Profile{<:StrictRank}, candidates)
        raw_to_label = Dict(Symbol(string(c.source)) => c.label for c in candidates)
    pool = CandidatePool([c.label for c in candidates])
    ballots = StrictRank[]
    for ballot in profile.ballots
        raw_order = ordered_candidates(ballot, profile.pool)
        mapped = [raw_to_label[Symbol(string(raw))] for raw in raw_order]
        push!(ballots, StrictRank(pool, collect(mapped)))
    end
    length(ballots) == nballots(profile) || error("Relabeled profile row count does not match source profile.")
    return Profile(pool, ballots)
end

function source_group_dataframe(bundle_df::DataFrame)
    return bundle_df
end

function manifest_candidate_list(path::AbstractString, year, scenario, m, backend, linearizer, b, r, k)
    !isfile(path) && return nothing
    df = CSV.read(path, DataFrame)
    rows = df[(string.(df.stage) .== "linearized") .&
              (Int.(df.year) .== Int(year)) .&
              (string.(df.scenario_name) .== string(scenario)) .&
              (Int.(df.m) .== Int(m)) .&
              (string.(df.imputer_backend) .== string(backend)) .&
              (string.(df.linearizer_policy) .== string(linearizer)) .&
              (Int.(df.b) .== Int(b)) .&
              (Int.(df.r) .== Int(r)) .&
              (Int.(df.k) .== Int(k)), :]
    isempty(rows) && return nothing
    return :active_candidates in propertynames(rows) ? string(rows.active_candidates[1]) : nothing
end

function manifest_paths_for_output_root(root)
    return [
        joinpath(root, "manifests", "linearization_manifest.csv"),
        joinpath(root, "linearizations", "linearization_manifest.csv"),
        joinpath(root, "manifests", "run_manifest.csv"),
        joinpath(root, "manifests", "measure_manifest.csv"),
    ]
end

function manifest_linearized_path(root, year, scenario_name, m, backend, linearizer, b, r, k)
    manifests = [
        joinpath(root, "manifests", "linearization_manifest.csv"),
        joinpath(root, "linearizations", "linearization_manifest.csv"),
        joinpath(root, "manifests", "run_manifest.csv"),
        joinpath(root, "manifests", "measure_manifest.csv"),
    ]
    inspected = String[]
    for manifest in manifests
        push!(inspected, manifest)
        isfile(manifest) || continue
        df = CSV.read(manifest, DataFrame)
        needed = [:year, :scenario_name, :m, :imputer_backend, :linearizer_policy, :b, :r, :k]
        all(in.(needed, Ref(propertynames(df)))) || continue
        rows = df[(Int.(df.year) .== Int(year)) .&
                  (string.(df.scenario_name) .== string(scenario_name)) .&
                  (Int.(df.m) .== Int(m)) .&
                  (string.(df.imputer_backend) .== string(backend)) .&
                  (string.(df.linearizer_policy) .== string(linearizer)) .&
                  (Int.(df.b) .== Int(b)) .&
                  (Int.(df.r) .== Int(r)) .&
                  (Int.(df.k) .== Int(k)), :]
        :stage in propertynames(rows) && (rows = rows[string.(rows.stage) .== "linearized", :])
        for row in eachrow(rows)
            for col in (:path, :linearized_artifact_path, :artifact_path, :csv_path, :output_csv, :profile_csv, :linearized_profile_csv)
                col in propertynames(rows) || continue
                p = string(row[col])
                isfile(p) && return (path=p, manifest=manifest,
                                     cache_dir=(:cache_dir in propertynames(rows) ? string(row.cache_dir) : dirname(dirname(p))),
                                     active_candidates=(:active_candidates in propertynames(rows) ? string(row.active_candidates) : nothing),
                                     candidate_label=(:candidate_label in propertynames(rows) ? string(row.candidate_label) : ""))
            end
        end
    end
    return (path=nothing, manifest=nothing, cache_dir=nothing, active_candidates=nothing,
            candidate_label="", inspected=inspected)
end

function load_year_waves()
    pp = prefpol_module()
    config_dir = joinpath(MGS_REPO_ROOT, "PrefPol", "config")
    paths = sort(filter(path -> occursin(r"/\d{4}\.toml$", path), readdir(config_dir; join=true)))
    isempty(paths) && error("No year TOML files found under $config_dir.")
    waves = [pp.load_survey_wave_config(path) for path in paths]
    return waves, pp.build_source_registry(waves), Dict(w.wave_id => w for w in waves)
end

function reconstruct_cache_source(; config_path, year, scenario_name, m, backend, linearizer, b, r, k,
                                  output_root_override=nothing, cache_root_override=nothing)
    pp = prefpol_module()
    roots = run_roots_from_config(config_path)
    output_root = output_root_override === nothing ? roots.output_root : resolve_path(String(output_root_override))
    cache_root = cache_root_override === nothing ? roots.cache_root : resolve_path(String(cache_root_override))
    waves, registry, wave_by_id = load_year_waves()
    wave_id = string(year)
    haskey(wave_by_id, wave_id) || error("No SurveyWaveConfig found for year $year.")
    wave = wave_by_id[wave_id]
    spec = pp.build_pipeline_spec(
        wave;
        scenario_name=scenario_name,
        m=m,
        groupings=Symbol.(wave.demographic_cols),
        measures=roots.measures,
        B=roots.B,
        R=roots.R,
        K=roots.K,
        imputer_backend=Symbol(backend),
        linearizer_policy=Symbol(linearizer),
        consensus_tie_policy=roots.consensus_tie_policy,
    )
    pipeline = pp.NestedStochasticPipeline(registry; cache_root=cache_root)
    cache_dir = pp.pipeline_cache_dir(pipeline, spec)
    stage_paths = pp.pipeline_stage_paths(pipeline, spec)
    rows = stage_paths[(string.(stage_paths.stage) .== "linearized") .&
                       (Int.(stage_paths.b) .== Int(b)) .&
                       (Int.(stage_paths.r) .== Int(r)) .&
                       (Int.(stage_paths.k) .== Int(k)), :]
    nrow(rows) == 1 || error("Could not compute a unique expected linearized path for b=$b r=$r k=$k.")
    path = string(rows.path[1])
    return (; path, cache_dir, active_candidates=join(spec.active_candidates, "|"),
            candidate_label="Candidates: " * join(replace.(spec.active_candidates, "_" => " "), ", "),
            output_root, cache_root, expected_cache_dir=cache_dir)
end

function load_linearized_artifact_source(path::AbstractString, active_candidates)
    if endswith(path, ".csv")
        df = CSV.read(path, DataFrame)
        return (dataframe=df, raw_profile=nothing, row_count=nrow(df),
                active_candidates=active_candidates, group_columns=Symbol[])
    end
    artifact = JLD2.load(path, "artifact")
    bundle = artifact isa AnnotatedProfile ? artifact :
             dataframe_to_annotated_profile(DataFrame(artifact); ballot_kind=:strict)
    df = artifact isa DataFrame ? DataFrame(artifact) : annotated_profile_to_dataframe(bundle)
    profile = strict_profile(bundle)
    nrow(df) == nballots(profile) ||
        error("Cached linearized artifact row count $(nrow(df)) does not match profile ballots $(nballots(profile)).")
    active = active_candidates === nothing ? join(string.(candidates(profile.pool)), "|") : active_candidates
    groups = [c for c in propertynames(df) if c != :profile]
    return (dataframe=df, raw_profile=profile, row_count=nballots(profile),
            active_candidates=active, group_columns=groups)
end

function cache_missing_error(; config_path, output_root, cache_root, year, scenario_name, m,
                             backend, linearizer, b, r, k, inspected, expected_cache_dir, expected_path)
    error("""Could not find cached canonical linearized profile.
Config: $(config_path)
Resolved output_root: $(output_root)
Resolved cache_root: $(cache_root)
Requested: year=$year scenario=$scenario_name m=$m backend=$backend linearizer=$linearizer b=$b r=$r k=$k
Inspected manifests:
$(join(inspected, "\n"))
Expected cache_dir: $(expected_cache_dir)
Expected linearized artifact: $(expected_path)

Run the composable pipeline first, for example:
  julia +1.11.9 --project=PrefPol PrefPol/composable_running/stages/03_linearize.jl --config $(config_path) --year $(year) --scenario $(scenario_name) --m $(m) --backend $(backend) --linearizer $(linearizer)
or:
  julia +1.11.9 --project=PrefPol PrefPol/composable_running/run_all_paper.jl --config $(config_path)
""")
end

function resolve_majority_graph_profile_source(; config_path, year, scenario_name, m,
                                               backend, linearizer, b, r, k,
                                               input_path=nothing,
                                               output_root_override=nothing,
                                               cache_root_override=nothing)
    config_path = resolve_path(String(config_path))
    roots = run_roots_from_config(config_path)
    output_root = output_root_override === nothing ? roots.output_root : resolve_path(String(output_root_override))
    cache_root = cache_root_override === nothing ? roots.cache_root : resolve_path(String(cache_root_override))
    warnings = String[]
    if input_path !== nothing
        path = resolve_path(String(input_path))
        isfile(path) || error("Explicit input CSV not found: $path")
        loaded = load_linearized_artifact_source(path, nothing)
        return merge(loaded, (source_mode="explicit_input", profile_path=path,
            cache_dir="", linearized_artifact_path=path, manifest_path_used="",
            config_path=config_path, output_root=output_root, cache_root=cache_root,
            year=year, scenario_name=scenario_name, m=m, backend=backend,
            linearizer=linearizer, b=b, r=r, k=k, candidate_label="",
            warnings=warnings))
    end

    manifest_hit = manifest_linearized_path(output_root, year, scenario_name, m, backend, linearizer, b, r, k)
    inspected = haskey(manifest_hit, :inspected) ? manifest_hit.inspected : manifest_paths_for_output_root(output_root)
    reconstructed = nothing
    if manifest_hit.path === nothing || !isfile(String(manifest_hit.path)) ||
       manifest_hit.active_candidates === nothing || manifest_hit.cache_dir === nothing
        reconstructed = reconstruct_cache_source(config_path=config_path, year=year,
            scenario_name=scenario_name, m=m, backend=backend, linearizer=linearizer,
            b=b, r=r, k=k, output_root_override=output_root, cache_root_override=cache_root)
    end

    path = manifest_hit.path === nothing ? reconstructed.path : manifest_hit.path
    cache_dir = manifest_hit.cache_dir === nothing ? reconstructed.cache_dir : manifest_hit.cache_dir
    active = manifest_hit.active_candidates === nothing ? reconstructed.active_candidates : manifest_hit.active_candidates
    candidate_label = isempty(manifest_hit.candidate_label) ?
        (reconstructed === nothing ? "" : reconstructed.candidate_label) :
        manifest_hit.candidate_label

    if !isfile(path)
        cache_missing_error(config_path=config_path, output_root=output_root, cache_root=cache_root,
            year=year, scenario_name=scenario_name, m=m, backend=backend, linearizer=linearizer,
            b=b, r=r, k=k, inspected=inspected, expected_cache_dir=reconstructed.expected_cache_dir,
            expected_path=reconstructed.path)
    end
    loaded = load_linearized_artifact_source(path, active)
    return merge(loaded, (source_mode="cache", profile_path=path,
        cache_dir=cache_dir, linearized_artifact_path=path,
        manifest_path_used=manifest_hit.manifest === nothing ? "" : manifest_hit.manifest,
        config_path=config_path, output_root=output_root, cache_root=cache_root,
        year=year, scenario_name=scenario_name, m=m, backend=backend,
        linearizer=linearizer, b=b, r=r, k=k, candidate_label=candidate_label,
        warnings=warnings))
end

resolve_linearized_profile(; kwargs...) = resolve_majority_graph_profile_source(; kwargs...)

function majority_order_if_transitive(result::MajorityGraphSupportResult)
    n = length(result.pool)
    length(result.edges) == n * (n - 1) ÷ 2 || return nothing
    wins = Dict(i => 0 for i in 1:n)
    for e in result.edges
        wins[e.winner] += 1
    end
    vals = sort(collect(wins); by=x -> (-x[2], x[1]))
    expected = collect((n - 1):-1:0)
    [v for (_, v) in vals] == expected || return nothing
    return [result.pool[i] for (i, _) in vals]
end

function kemeny_fallback_reference(profile)
    pool = profile.pool
    best_perm = nothing
    best_score = -Inf
    for p in Preferences._all_permutations(length(pool))
        score = 0.0
        for i in 1:(length(p)-1), j in (i+1):length(p)
            score += max(0.0, Preferences.pairwise_majority_margins(profile)[p[i], p[j]])
        end
        if score > best_score || (score == best_score && Tuple(p) < Tuple(best_perm))
            best_perm = p
            best_score = score
        end
    end
    return [pool[i] for i in best_perm]
end

function infer_reference_order(profile, preliminary_spec)
    tmp_basis = voter_type_basis(profile.pool; order=:lex)
    result = majority_graph_support(profile; basis=tmp_basis)
    maj = majority_order_if_transitive(result)
    maj !== nothing && return (order=Symbol.(maj), rule="computed transitive majority order")
    return (order=Symbol.(kemeny_fallback_reference(profile)), rule="maximum total pairwise support coverage fallback")
end

function latex_escape(x)
    s = string(x)
    for (k, v) in ["\\"=>"\\textbackslash{}", "_"=>"\\_", "%"=>"\\%", "&"=>"\\&",
                   "#"=>"\\#", raw"$"=>"\\\$", "{"=>"\\{", "}"=>"\\}",
                   "~"=>"\\textasciitilde{}", "^"=>"\\textasciicircum{}"]
        s = replace(s, k => v)
    end
    return s
end

function latex_value(x)
    if x === missing || (x isa AbstractFloat && isnan(x))
        return ""
    elseif x isa Integer
        return string(x)
    elseif x isa AbstractFloat
        return isapprox(x, round(x); atol=1e-8) ? string(Int(round(x))) : @sprintf("%.3f", x)
    else
        return latex_escape(x)
    end
end

function latex_table(df::DataFrame; caption="", label="", maxrows=40, fontsize="scriptsize", landscape=false)
    rows = first(df, min(maxrows, nrow(df)))
    cols = propertynames(rows)
    lines = String[]
    landscape && push!(lines, "\\begin{landscape}")
    push!(lines, "\\begin{table}[H]\\centering\\$fontsize")
    push!(lines, "\\resizebox{\\linewidth}{!}{%")
    push!(lines, "\\begin{tabular}{" * "l" ^ length(cols) * "}\\toprule")
    push!(lines, join(latex_escape.(cols), " & ") * " \\\\ \\midrule")
    for row in eachrow(rows)
        push!(lines, join((latex_value(row[c]) for c in cols), " & ") * " \\\\")
    end
    push!(lines, "\\bottomrule\\end{tabular}}")
    !isempty(caption) && push!(lines, "\\caption{$(latex_escape(caption))}")
    !isempty(label) && push!(lines, "\\label{$(latex_escape(label))}")
    nrow(df) > maxrows && push!(lines, "\\par\\smallskip Showing $(maxrows) of $(nrow(df)) rows.")
    push!(lines, "\\end{table}")
    landscape && push!(lines, "\\end{landscape}")
    return join(lines, "\n")
end

figure_tex(file, caption; width="0.78\\linewidth") = """
\\begin{figure}[H]
\\centering
\\includegraphics[width=$width]{../figures/$file}
\\caption{$(latex_escape(caption))}
\\end{figure}
"""

function add_edge_label!(df::DataFrame)
    (:winner in propertynames(df) && :loser in propertynames(df)) && (df.edge = string.(df.winner, ">", df.loser))
    return df
end

compact_edge_label(x) = replace(string(x), " -> " => ">", " > " => ">")

function compact_edge_columns!(df::DataFrame)
    for c in (:edge, :edge_i_label, :edge_j_label, :max_breaking_edge)
        c in propertynames(df) && (df[!, c] = compact_edge_label.(df[!, c]))
    end
    return df
end

write_table(path, df::DataFrame) = (mkpath(dirname(path)); CSV.write(path, df))
safe_partition_labels(df::DataFrame, p::Symbol) = [ismissing(x) ? "NA" : string(x) for x in df[!, p]]

function table_total_row(swing::DataFrame)
    baseline = nrow(swing) == 0 ? 0.0 : swing.target_opponent_margin_before[1]
    total_swing = sum(swing.plurality_swing_value)
    return DataFrame(current_first_id=missing, current_first=:TOTAL,
                     one_swap_mass=sum(swing.one_swap_mass),
                     per_voter_swing=missing,
                     plurality_swing_value=total_swing,
                     target_opponent_margin_before=baseline,
                     target_opponent_margin_after_if_pool_switches=baseline + total_swing)
end

function assert_close(name, actual, expected; atol=1e-6)
    isapprox(Float64(actual), Float64(expected); atol=atol) ||
        error("Validation failed for $name: got $actual, expected $expected")
end

function validate_known_2022(profile, tables)
    nballots(profile) == 2001 || return false
    plurality = tables["plurality_scores"]
    for (candidate, expected) in Dict(:Lu=>823.0, :Bo=>794.0, :Te=>241.0, :Ci=>143.0)
        rows = plurality[plurality.candidate .== candidate, :]
        nrow(rows) == 1 && assert_close("plurality $candidate", rows.first_place_count[1], expected)
    end
    edges = tables["majority_edges"]
    lubo = edges[(edges.winner .== :Lu) .& (edges.loser .== :Bo), :]
    nrow(lubo) == 1 || error("Validation failed: Lu>Bo edge not found")
    assert_close("Lu>Bo support", lubo.support_mass[1], 1116)
    assert_close("Bo>Lu opposition", lubo.opposition_mass[1], 885)
    assert_close("Lu>Bo margin", lubo.margin_mass[1], 231)
    edge_pairs = Set(zip(edges.winner, edges.loser))
    for pair in [(:Lu, :Te), (:Lu, :Ci), (:Lu, :Bo), (:Te, :Ci), (:Te, :Bo), (:Ci, :Bo)]
        pair in edge_pairs || error("Validation failed: expected majority edge $(pair[1])>$(pair[2])")
    end
    for (candidate, expected) in Dict(:Te=>138.0, :Ci=>47.0, :Bo=>69.0)
        rows = tables["one_swap_target"][tables["one_swap_target"].current_first .== candidate, :]
        nrow(rows) == 1 && assert_close("one-swap $candidate", rows.mass[1], expected)
    end
    assert_close("total swing", sum(tables["plurality_swing_values"].plurality_swing_value), 323)
    return true
end

function validate_effective_invariants(result, tables)
    for df in (tables["core_effective_type_composition"], tables["reverse_core_effective_type_composition"])
        for sub in groupby(df[df.included, :], [:k, :core_kind])
            isempty(sub) && continue
            hhi = sum(sub.conditional_share .^ 2)
            neff = 1 / hhi
            threshold = 1 / neff
            positive_types = count(sub.conditional_share .> 0)
            positive_types == 0 && continue
            for row in eachrow(sub)
                isnan(row.effective_weight) && continue
                assert_close("effective weight", row.effective_weight, neff * row.conditional_share)
                row.above_effective_threshold == (row.effective_weight > 1) ||
                    error("Invalid above-threshold flag")
            end
            isnan(first(sub).effective_threshold) || assert_close("effective threshold", first(sub).effective_threshold, threshold)
            neff + 1e-8 >= 1 || error("N_eff below 1")
            neff <= positive_types + 1e-6 || @warn "N_eff exceeds positive type count in invariant check" k=first(sub).k core_kind=first(sub).core_kind neff positive_types
        end
    end
    core = tables["core"]
    all(diff(core.core_mass) .<= 1e-8) || error("gamma_k^+ is not weakly decreasing")
    rev = tables["reverse_core_effective_types"]
    all(diff(rev.reverse_core_mass) .<= 1e-8) || error("gamma_k^- is not weakly decreasing")
    assert_close("gamma_0^+", core.core_mass[core.k .== 0][1], 1.0)
    assert_close("gamma_0^-", rev.reverse_core_mass[rev.k .== 0][1], 1.0)
    E = length(result.edges)
    all(result.coverage .+ (E .- result.coverage) .== E) || error("K^+ + K^- != E")
    return true
end

function selected_current_first(profile, target)
    [profile.pool[i] for i in 1:length(profile.pool) if profile.pool[i] != target]
end

function write_role_outputs!(tables, result, group_results, group_partitions, roles_dir)
    mkpath(roles_dir)
    summary = graph_role_summary(result; thresholds=ROLE_THRESHOLDS,
                                 amenability=ROLE_AMENABILITY, lambda=ROLE_LAMBDA)
    tables["voter_type_roles"] = compact_edge_columns!(summary.role_table)
    tables["role_mass_summary"] = summary.role_mass_summary
    tables["primary_role_mass_summary"] = summary.primary_role_mass_summary
    tables["edge_type_roles"] = compact_edge_columns!(summary.edge_role_table)
    tables["weakest_edge_role_summary"] = compact_edge_columns!(summary.weakest_edge_breakers)
    for p in group_partitions
        gs = group_graph_role_summary(group_results[p], tables["voter_type_roles"];
                                      amenability=ROLE_AMENABILITY, lambda=ROLE_LAMBDA)
        tables["group_roles_$(p)"] = gs.group_role_table
        tables["group_primary_roles_$(p)"] = gs.group_primary_role_table
        tables["group_role_power_$(p)"] = compact_edge_columns!(gs.group_role_power_table)
    end
end

function compute_tables(profile, df, spec, basis)
    result = majority_graph_support(profile; basis=basis)
    tables = Dict{String,DataFrame}()
    tables["majority_edges"] = compact_edge_columns!(add_edge_label!(majority_edges_table(result)))
    tables["voter_types"] = voter_type_table(result)
    tables["edge_support"] = compact_edge_columns!(edge_support_table(result))
    tables["edge_overlap"] = compact_edge_columns!(edge_overlap_table(result))
    tables["core"] = core_table(result)
    tables["countergraph_summary"] = countergraph_summary_table(result)
    tables["type_breakers"] = compact_edge_columns!(add_edge_label!(type_breaker_table(result)))
    tables["minimal_breaking_coalition"] = compact_edge_columns!(add_edge_label!(minimal_breaking_coalition_table(result; by=:amenability)))
    tables["plurality_scores"] = plurality_scores_table(profile; basis=basis)
    tables["pairwise_vs_plurality"] = pairwise_vs_plurality_decomposition_table(profile, spec.target, spec.opponent; basis=basis)
    tables["candidate_position_by_current_first"] = candidate_position_by_current_first_table(profile, spec.target; basis=basis)
    current_first = spec.current_first_candidates === nothing ? selected_current_first(profile, spec.target) : spec.current_first_candidates
    tables["one_swap_target"] = one_swap_target_table(profile, spec.target; current_first_candidates=current_first, basis=basis)
    tables["plurality_swing_values"] = plurality_swing_value_table(profile, spec.target, spec.opponent; current_first_candidates=current_first, basis=basis)
    tables["exact_type_switch"] = exact_type_switch_table(profile, spec.target; current_first_candidates=current_first, basis=basis)
    diag = effective_type_diagnostics(result)
    tables["edge_effective_types"] = compact_edge_columns!(diag.edge_summary)
    tables["edge_effective_type_composition"] = compact_edge_columns!(diag.edge_composition)
    tables["core_effective_types"] = diag.core_summary
    tables["reverse_core_effective_types"] = diag.reverse_core_summary
    tables["core_effective_type_composition"] = diag.core_composition
    tables["reverse_core_effective_type_composition"] = diag.reverse_core_composition
    tables["support_core_effective_composition"] = support_core_effective_composition_table(result)
    tables["support_core_above_threshold_type"] = support_core_above_threshold_type_table(result)
    tables["reverse_core_effective_composition"] = reverse_core_effective_composition_table(result)
    tables["reverse_core_above_threshold_type"] = reverse_core_above_threshold_type_table(result)
    tables["edge_effective_composition"] = compact_edge_columns!(edge_effective_composition_table(result))
    tables["edge_above_threshold_type"] = compact_edge_columns!(edge_above_threshold_type_table(result))

    group_partitions = Symbol[]
    group_results = Dict{Symbol,GroupMajorityGraphSupportResult}()
    for p in spec.partitions
        p in propertynames(df) || continue
        push!(group_partitions, p)
        labels = safe_partition_labels(df, p)
        gr = group_majority_graph_support(profile, labels; basis=basis)
        group_results[p] = gr
        tables["group_edge_power_$(p)"] = compact_edge_columns!(add_edge_label!(group_edge_power_table(gr)))
        tables["group_breakers_$(p)"] = compact_edge_columns!(add_edge_label!(group_breaker_table(gr)))
        tables["group_anchor_$(p)"] = group_anchor_table(gr)
        tables["group_target_switch_$(p)"] = group_target_switch_table(profile, labels, spec.target, spec.opponent;
                                                                        current_first_candidates=current_first, basis=basis)
    end
    return result, tables, group_results, group_partitions
end

function write_all_tables(tables, tables_dir)
    for (name, table) in sort(collect(tables); by=first)
        write_table(joinpath(tables_dir, "$name.csv"), table)
    end
end

function plot_pair(fun, figures_dir, stem)
    for ext in ("png", "pdf")
        try
            fun(joinpath(figures_dir, "$stem.$ext"))
        catch err
            @warn "Plot failed" stem ext exception=(err, catch_backtrace())
        end
    end
end

function write_figures(tables, group_partitions, figures_dir, spec)
    mkpath(figures_dir)
    plot_pair(p -> plot_plurality_scores(tables["plurality_scores"]; output_path=p), figures_dir, "plurality_scores")
    plot_pair(p -> plot_pairwise_margins(tables["majority_edges"]; output_path=p), figures_dir, "pairwise_margins")
    plot_pair(p -> plot_shell_masses(tables["voter_types"]; output_path=p), figures_dir, "shell_masses")
    plot_pair(p -> plot_support_matrix(tables["edge_support"]; output_path=p), figures_dir, "support_matrix")
    plot_pair(p -> plot_edge_overlap_heatmap(tables["edge_overlap"]; output_path=p), figures_dir, "edge_overlap_heatmap")
    plot_pair(p -> plot_type_anchoring(tables["voter_types"]; output_path=p), figures_dir, "type_anchoring_top")
    plot_pair(p -> plot_type_breakers(tables["type_breakers"]; output_path=p), figures_dir, "type_breakers")
    target_display = get(spec.display_names, spec.target, string(spec.target))
    plot_pair(p -> plot_candidate_position_by_current_first(tables["candidate_position_by_current_first"]; output_path=p, target_label=target_display),
              figures_dir, "target_position_by_current_first")
    plot_pair(p -> plot_plurality_swing_values(tables["plurality_swing_values"]; output_path=p), figures_dir, "plurality_swing_values")
    for edge in unique(tables["majority_edges"].edge)[1:min(3, nrow(tables["majority_edges"]))]
        stem = "type_breakers_" * replace(edge, ">" => "")
        plot_pair(p -> plot_type_breakers(tables["type_breakers"]; output_path=p, edge=edge, top_n=10), figures_dir, stem)
    end
    for p in group_partitions
        plot_pair(out -> plot_group_contributions(tables["group_edge_power_$(p)"]; output_path=out),
                  figures_dir, "group_contributions_$(p)")
        plot_pair(out -> plot_group_target_switch(tables["group_target_switch_$(p)"]; output_path=out,
                                                  title="$(target_display) switch by $(p)"),
                  figures_dir, "group_target_switch_$(p)")
    end
end

function table_section(tables, key, title; maxrows=35, landscape=false)
    haskey(tables, key) || return ""
    return "\\subsection{$(latex_escape(title))}\n" *
           latex_table(tables[key]; caption=title, label="tab:$key", maxrows=maxrows, landscape=landscape)
end

function group_appendix(tables, group_partitions, stem, title)
    parts = String[]
    for p in group_partitions
        key = "$(stem)_$(p)"
        haskey(tables, key) && push!(parts, table_section(tables, key, "$title: $p"; maxrows=80, landscape=true))
    end
    return join(parts, "\n")
end

function report_tex(spec, input_path, tables, group_partitions, reference_rule)
    E = nrow(tables["majority_edges"])
    active = join(["$(c.label) = $(c.display)" for c in spec.candidates], ", ")
    plurality = tables["plurality_scores"]
    target_row = plurality[plurality.candidate .== spec.target, :]
    opp_row = plurality[plurality.candidate .== spec.opponent, :]
    plurality_margin = (!isempty(target_row) && !isempty(opp_row)) ?
        target_row.first_place_count[1] - opp_row.first_place_count[1] : missing
    target_edge = tables["majority_edges"][(tables["majority_edges"].winner .== spec.target) .&
                                           (tables["majority_edges"].loser .== spec.opponent), :]
    target_pairwise = nrow(target_edge) == 1 ? target_edge.margin_mass[1] : missing
    weakest = sort(tables["majority_edges"], :normalized_margin)[1, :]
    reverse_definition = "While gamma_k measures mass supporting at least k majority edges, reverse gamma_k measures mass contesting at least k majority edges."
    figs = join([
        figure_tex("plurality_scores.png", "Plurality scores."),
        figure_tex("pairwise_margins.png", "Pairwise majority margins."),
        figure_tex("shell_masses.png", "Kendall shell masses."),
        figure_tex("support_matrix.png", "Type by edge support matrix."),
        figure_tex("edge_overlap_heatmap.png", "Edge coalition overlap."),
        figure_tex("type_anchoring_top.png", "Type anchoring."),
        figure_tex("target_position_by_current_first.png", "Target position by current first choice."),
        figure_tex("plurality_swing_values.png", "Plurality swing values."),
    ], "\n")
    group_figs = join([figure_tex("group_contributions_$(p).png", "Group edge-power heatmap for $(p).") *
                       figure_tex("group_target_switch_$(p).png", "Group target-switch heatmap for $(p).")
                       for p in group_partitions], "\n")
    return """
\\documentclass[11pt]{article}
\\usepackage[margin=0.85in]{geometry}
\\usepackage{booktabs,longtable,array,graphicx,float,caption,pdflscape}
\\usepackage{amsmath,amssymb}
\\usepackage[T1]{fontenc}
\\usepackage[utf8]{inputenc}
\\usepackage{lmodern,microtype,hyperref}
\\hypersetup{colorlinks=true,linkcolor=black,urlcolor=black}
\\setlength{\\parindent}{0pt}
\\setlength{\\parskip}{0.6em}
\\begin{document}
\\title{Majority-Graph Support for the $(spec.year) Canonical Linearized Profile}
\\author{Julia replication workflow}
\\date{$(latex_escape(Dates.format(now(), dateformat"yyyy-mm-dd HH:MM")))}
\\maketitle
\\tableofcontents
\\newpage

\\section{Introduction and scope}
This is a structural diagnostic of one canonical linearized profile from \\texttt{composable\\_running}, not an uncertainty-pooled bootstrap, imputation, or linearization distribution. Year: $(spec.year). Scenario: $(latex_escape(spec.scenario_name)). m = $(spec.m). Input CSV:
\\begin{quote}\\texttt{$(latex_escape(input_path))}\\end{quote}
Active candidates: $(latex_escape(active)). The target/opponent for plurality-switch analysis are $(spec.target) and $(spec.opponent). Group partitions analyzed: $(join(latex_escape.(group_partitions), ", ")).

\\section{Plurality and pairwise majority}
The reference order for Kendall shells is $(join(spec.reference_order, " > ")); rule: $(latex_escape(reference_rule)). The target-opponent plurality margin is $(latex_value(plurality_margin)); the target-opponent pairwise margin is $(latex_value(target_pairwise)). The weakest majority edge is $(latex_escape(weakest.edge)) with normalized margin $(latex_value(weakest.normalized_margin)) and flip count $(latex_value(weakest.integer_flip_count)).
$(latex_table(tables["plurality_scores"]; caption="Plurality scores", maxrows=20))
$(latex_table(tables["majority_edges"]; caption="Pairwise majority edges", maxrows=30))

\\section{Why pairwise and plurality differ}
The pairwise target-opponent margin equals the plurality target-opponent margin plus the surplus contributions from all non-target/non-opponent first-choice groups. The table does not hardcode the number of such groups.
$(latex_table(tables["pairwise_vs_plurality"]; caption="Pairwise-vs-plurality decomposition", maxrows=30))

\\section{Voter-type basis and majority-graph support}
The full strict-ranking basis is ordered by Kendall shell around the reference order. Zero-mass types are retained so type indices remain stable.
$(latex_table(tables["voter_types"]; caption="Full voter-type basis", maxrows=80, landscape=true))

\\section{Support cores}
Use E = |E(G)| = $E, not m, for full-core notation. The strict support core is gamma_E^+; relaxed support cores are gamma_{E-1}^+ and gamma_{E-2}^+ when defined.
$(latex_table(tables["core"]; caption="Support cores", maxrows=20))

\\section{Effective type composition of support cores}
For any conditional distribution q over voter types, HHI(q)=sum q_r^2, N_eff(q)=1/HHI(q), and the effective threshold is 1/N_eff(q). A type is above the effective threshold when N_eff(q) q_r > 1.
$(latex_table(tables["support_core_effective_composition"]; caption="Effective type composition of support cores", maxrows=20, landscape=true))
$(latex_table(tables["support_core_above_threshold_type"]; caption="Above-threshold support-core types", maxrows=80, landscape=true))

\\section{Reverse support cores}
$(latex_escape(reverse_definition))
$(latex_table(tables["reverse_core_effective_composition"]; caption="Effective type composition of reverse support cores", maxrows=20, landscape=true))

\\section{Effective type composition of reverse support cores}
$(latex_table(tables["reverse_core_above_threshold_type"]; caption="Above-threshold reverse-core types", maxrows=80, landscape=true))
$(latex_table(tables["countergraph_summary"]; caption="Countergraph summary", maxrows=5, landscape=true))

\\section{Effective type composition by edge}
$(latex_table(tables["edge_effective_composition"]; caption="Edge-side effective composition", maxrows=40, landscape=true))
$(latex_table(tables["edge_above_threshold_type"]; caption="Above-threshold effective types by edge and side", maxrows=120, landscape=true))

\\section{Edge coalition overlap}
$(latex_table(tables["edge_overlap"]; caption="Edge coalition overlap", maxrows=80, landscape=true))

\\section{Type anchoring}
$(latex_table(sort(tables["voter_types"], :anchoring, rev=true); caption="Top type anchors", maxrows=24, landscape=true))

\\section{Type breakers and edge amenability}
Raw mass and amenability-weighted breaking score are distinct. The breaker table is written for all edges and can be filtered to the weakest edge and target-opponent edge.
$(latex_table(tables["type_breakers"]; caption="Type breakers", maxrows=100, landscape=true))

\\section{Majority-graph role decomposition}
Roles are non-exclusive unless the primary-role table is used. Categories include anchor, peripheral_supporter, edge_breaker, counter_graph, mixed, and anchor_edge_breaker when generated.
$(latex_table(tables["role_mass_summary"]; caption="Non-exclusive role mass summary", maxrows=20))
$(latex_table(tables["primary_role_mass_summary"]; caption="Primary role mass summary", maxrows=20))
$(latex_table(tables["voter_type_roles"]; caption="Voter-type roles", maxrows=80, landscape=true))

\\section{Group roles on the weakest edge}
Group columns decompose graph anchoring, conditional anchoring, weakest-edge margin contribution, weakest-edge support, support share, and edge-breaking capacity.
$(group_appendix(tables, group_partitions, "group_role_power", "Group role power"))

\\section{Minimal breaking coalitions}
$(latex_table(tables["minimal_breaking_coalition"]; caption="Minimal breaking coalitions", maxrows=100, landscape=true))

\\section{Plurality switch analysis}
$(latex_table(tables["candidate_position_by_current_first"]; caption="Target position by current first", maxrows=40))
$(latex_table(tables["one_swap_target"]; caption="One-swap target pools", maxrows=40))
$(latex_table(vcat(tables["plurality_swing_values"], table_total_row(tables["plurality_swing_values"]); cols=:union); caption="Plurality swing values", maxrows=40))
$(latex_table(tables["exact_type_switch"]; caption="Exact type switch table", maxrows=80, landscape=true))

\\section{Group contributions}
$(group_figs)
$(group_appendix(tables, group_partitions, "group_edge_power", "Group edge power"))

\\section{Group target-switch amenability}
$(group_appendix(tables, group_partitions, "group_target_switch", "Group target-switch amenability"))

\\section{Year-specific conclusion}
The $(spec.year) architecture combines plurality-vs-pairwise target-opponent structure, weakest-edge fragility, support and reverse support cores, effective type diversity, role composition, minimal breaking coalitions, switch pools, and group contribution patterns. The CSV appendices contain the full diagnostic architecture.

$(figs)

\\appendix
\\section{Appendix: full edge support table}
$(latex_table(tables["edge_support"]; caption="Full edge support", maxrows=160, landscape=true))
\\section{Appendix: group breaker tables}
$(group_appendix(tables, group_partitions, "group_breakers", "Group breakers"))
\\section{Appendix: effective type composition tables}
$(latex_table(tables["core_effective_type_composition"]; caption="Full support-core effective composition", maxrows=160, landscape=true))
$(latex_table(tables["reverse_core_effective_type_composition"]; caption="Full reverse-core effective composition", maxrows=160, landscape=true))
$(latex_table(tables["edge_effective_type_composition"]; caption="Full edge effective type composition", maxrows=160, landscape=true))
\\section{Appendix: manifest/config}
See \\texttt{../manifest.toml}.
\\end{document}
"""
end

function compile_latex(report_path)
    dir = dirname(report_path)
    file = basename(report_path)
    try
        if Sys.which("latexmk") !== nothing
            cd(dir) do
                run(pipeline(`latexmk -pdf -interaction=nonstopmode $file`; stdout=devnull, stderr=devnull))
            end
        elseif Sys.which("tectonic") !== nothing
            cd(dir) do
                run(pipeline(`tectonic $file`; stdout=devnull, stderr=devnull))
            end
        else
            println("No latexmk or tectonic found; saved .tex report without compiling.")
        end
    catch err
        @warn "LaTeX compilation failed; .tex report was written" exception=(err, catch_backtrace())
    end
end

function default_spec(year::Int; m=4, output=nothing, input=nothing, validate_known=true)
    out = output === nothing ? joinpath(MGS_SCRIPT_DIR, "output", "majority_graph_support_$(year)") : resolve_path(output)
    if year == 2022
        candidates = [
            (column=:rank_LULA, source="LULA", label=:Lu, display="Lula"),
            (column=:rank_BOLSONARO, source="BOLSONARO", label=:Bo, display="Bolsonaro"),
            (column=:rank_CIRO_GOMES, source="CIRO_GOMES", label=:Ci, display="Ciro Gomes"),
            (column=:rank_SIMONE_TEBET, source="SIMONE_TEBET", label=:Te, display="Simone Tebet"),
        ]
        display = Dict(c.label => c.display for c in candidates)
        return MajorityGraphReportSpec(year=year, scenario_name="main_2022", m=m, input_path=input,
            output_dir=out, candidates=candidates, reference_order=[:Lu, :Te, :Ci, :Bo],
            target=:Lu, opponent=:Bo, current_first_candidates=[:Te, :Ci, :Bo],
            partitions=[:Sex, :Religion, :Race, :Ideology, :PT, :Abortion, :Age, :Education, :Income],
            role_report_partitions=[:Ideology, :PT, :Sex, :Religion, :Education, :Income, :Abortion],
            display_names=display, validate_known_values=validate_known)
    elseif year == 2018
        return MajorityGraphReportSpec(year=year, scenario_name="main_2018", m=m, input_path=input, output_dir=out,
            target=nothing, opponent=:Bo, partitions=[:Sex, :Religion, :Race, :Age, :Education, :Income, :Ideology, :LulaScoreGroup],
            role_report_partitions=[:Ideology, :Sex, :Religion, :Education, :Income, :LulaScoreGroup])
    elseif year == 2006
        return MajorityGraphReportSpec(year=year, scenario_name="main_2006", m=m, input_path=input, output_dir=out,
            target=:Lu, opponent=:Al, partitions=[:Sex, :Ideology, :PT, :Age, :Education, :Income],
            role_report_partitions=[:Ideology, :PT, :Sex, :Education, :Income])
    end
    error("Unsupported year: $year")
end

function finalize_spec(spec, candidates, profile)
    display = Dict(c.label => c.display for c in candidates)
    target = spec.target
    opponent = spec.opponent
    labels = Set(c.label for c in candidates)
    if spec.year == 2018
        :Ha in labels || error("2018 target Fernando_Haddad/Ha is not active; target inference is ambiguous for active labels $(collect(labels)).")
        :Bo in labels || error("2018 opponent Jair_Bolsonaro/Bo is not active.")
        target = :Ha
        opponent = :Bo
    elseif spec.year == 2006
        :Lu in labels || error("2006 target Lula/Lu is not active.")
        :Al in labels || error("2006 opponent Geraldo_Alckmin/Al is not active.")
        target = :Lu
        opponent = :Al
    end
    target in labels || error("Target $target is not active in $(collect(labels)).")
    opponent in labels || error("Opponent $opponent is not active in $(collect(labels)).")
    ref = spec.reference_order
    rule = "explicit report specification"
    if ref !== nothing && !all(in.(ref, Ref(collect(labels))))
        ref = nothing
        rule = "explicit reference order incompatible with active candidates; inferred from profile"
    end
    if ref === nothing
        inferred = infer_reference_order(profile, spec)
        ref = inferred.order
        rule = rule == "explicit report specification" ? inferred.rule : string(rule, " (", inferred.rule, ")")
    end
    current_first = spec.current_first_candidates === nothing ?
        [c.label for c in candidates if c.label != target] : spec.current_first_candidates
    return MajorityGraphReportSpec(year=spec.year, scenario_name=spec.scenario_name, m=spec.m,
        input_path=spec.input_path, output_dir=spec.output_dir, candidates=candidates,
        reference_order=ref, target=target, opponent=opponent,
        current_first_candidates=current_first, partitions=spec.partitions,
        role_report_partitions=spec.role_report_partitions, display_names=display,
        report_language=spec.report_language, validate_known_values=spec.validate_known_values), profile, rule
end

function run_majority_graph_report(year::Int; config_path, m=4, backend=:mice,
                                   linearizer=:pattern_conditional, b=1, r=1, k=1,
                                   input=nothing, output=nothing, validate_known=true)
    spec0 = default_spec(year; m=m, output=output, input=input, validate_known=validate_known)
    resolved = resolve_majority_graph_profile_source(config_path=config_path, year=year,
        scenario_name=spec0.scenario_name, m=m, backend=backend, linearizer=linearizer,
        b=b, r=r, k=k, input_path=input)
    df = resolved.dataframe
    if resolved.raw_profile === nothing
        candidates0 = candidates_from_profile(df, year; manifest_candidates=resolved.active_candidates)
        profile0 = build_profile_from_rank_columns(df, candidates0)
    else
        active = split(String(resolved.active_candidates), "|")
        candidates0 = candidates_from_active_names(active, year)
        profile0 = relabel_profile(resolved.raw_profile, candidates0)
    end
    nrow(df) == nballots(profile0) ||
        error("Source row count $(nrow(df)) does not match profile ballot count $(nballots(profile0)).")
    spec, profile, reference_rule = finalize_spec(
        MajorityGraphReportSpec(year=spec0.year, scenario_name=spec0.scenario_name, m=spec0.m,
            input_path=resolved.linearized_artifact_path, output_dir=spec0.output_dir, candidates=candidates0,
            reference_order=spec0.reference_order, target=spec0.target, opponent=spec0.opponent,
            current_first_candidates=spec0.current_first_candidates, partitions=spec0.partitions,
            role_report_partitions=spec0.role_report_partitions, display_names=spec0.display_names,
            report_language=spec0.report_language, validate_known_values=spec0.validate_known_values),
        candidates0, profile0)
    basis = voter_type_basis(profile.pool; order=:kendall_shell, reference_order=spec.reference_order)
    result, tables, group_results, group_partitions = compute_tables(profile, df, spec, basis)
    roles_dir = joinpath(spec.output_dir, "tables", "roles")
    write_role_outputs!(tables, result, group_results, group_partitions, roles_dir)
    spec.validate_known_values && year == 2022 && validate_known_2022(profile, tables) && println("Known 2022 validation checks passed.")
    validate_effective_invariants(result, tables) && println("Effective/core invariant checks passed for $year.")
    tables_dir = joinpath(spec.output_dir, "tables")
    figures_dir = joinpath(spec.output_dir, "figures")
    report_dir = joinpath(spec.output_dir, "report")
    mkpath(tables_dir); mkpath(figures_dir); mkpath(report_dir)
    write_all_tables(tables, tables_dir)
    write_figures(tables, group_partitions, figures_dir, spec)
    manifest = Dict(
        "year" => spec.year,
        "scenario_name" => spec.scenario_name,
        "m" => spec.m,
        "E" => length(result.edges),
        "source_mode" => resolved.source_mode,
        "config_path" => abspath(resolved.config_path),
        "output_root" => abspath(resolved.output_root),
        "cache_root" => abspath(resolved.cache_root),
        "cache_dir" => resolved.cache_dir == "" ? "" : abspath(resolved.cache_dir),
        "linearized_artifact_path" => abspath(resolved.linearized_artifact_path),
        "profile_path" => abspath(resolved.profile_path),
        "manifest_path_used" => isempty(resolved.manifest_path_used) ? "" : abspath(resolved.manifest_path_used),
        "input_csv" => endswith(resolved.linearized_artifact_path, ".csv") ? abspath(resolved.linearized_artifact_path) : "",
        "input_source" => resolved.source_mode,
        "output_dir" => abspath(spec.output_dir),
        "generated_at" => Dates.format(now(), dateformat"yyyy-mm-ddTHH:MM:SS"),
        "imputer_backend" => string(backend),
        "linearizer_policy" => string(linearizer),
        "b" => b, "r" => r, "k" => k,
        "active_candidates" => [Dict("source"=>c.source, "column"=>string(c.column), "label"=>string(c.label), "display"=>c.display) for c in spec.candidates],
        "candidate_label" => resolved.candidate_label,
        "target" => string(spec.target),
        "opponent" => string(spec.opponent),
        "reference_order" => string.(spec.reference_order),
        "reference_rule" => reference_rule,
        "partitions" => string.(group_partitions),
        "row_count" => resolved.row_count,
        "profile_count" => nballots(profile),
        "group_columns_available" => string.(resolved.group_columns),
        "warnings" => resolved.warnings,
    )
    open(joinpath(spec.output_dir, "manifest.toml"), "w") do io
        TOML.print(io, manifest)
    end
    report_path = joinpath(report_dir, "majority_graph_support_$(year).tex")
    write(report_path, report_tex(spec, resolved.linearized_artifact_path, tables, group_partitions, reference_rule))
    compile_latex(report_path)
    println("Wrote majority-graph support outputs to $(spec.output_dir)")
    return spec.output_dir
end
