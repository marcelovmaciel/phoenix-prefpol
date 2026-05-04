#!/usr/bin/env julia

include("majority_graph_report_common.jl")

function main(args=ARGS)
    opts = parse_common_args(args)
    run_majority_graph_report(2018; config_path=opts.config, m=opts.m,
        backend=opts.backend, linearizer=opts.linearizer, b=opts.b, r=opts.r, k=opts.k,
        input=opts.input, output=opts.output, validate_known=false)
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
