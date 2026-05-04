#!/usr/bin/env julia

include("majority_graph_report_common.jl")

function main(args=ARGS)
    opts = parse_common_args(args)
    years = parse.(Int, split(opts.years, ","))
    for year in years
        source = resolve_majority_graph_profile_source(
            config_path=opts.config,
            year=year,
            scenario_name=scenario_for_year(year),
            m=opts.m,
            backend=opts.backend,
            linearizer=opts.linearizer,
            b=opts.b,
            r=opts.r,
            k=opts.k,
            input_path=opts.input,
        )
        println("year=", year)
        println("  source_mode=", source.source_mode)
        println("  cache_dir=", source.cache_dir)
        println("  linearized_artifact_path=", source.linearized_artifact_path)
        println("  manifest_path_used=", source.manifest_path_used)
        println("  active_candidates=", source.active_candidates)
        println("  row/profile count=", source.row_count)
        println("  group columns available=", join(string.(source.group_columns), ","))
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
