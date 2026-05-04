#!/usr/bin/env julia

include("majority_graph_report_common.jl")

function main(args=ARGS)
    opts = parse_common_args(args)
    years = parse.(Int, split(opts.years, ","))
    for year in years
        run_majority_graph_report(year; config_path=opts.config, m=opts.m,
            backend=opts.backend, linearizer=opts.linearizer, b=opts.b, r=opts.r, k=opts.k,
            input=nothing, output=nothing, validate_known=opts.validate_known)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
