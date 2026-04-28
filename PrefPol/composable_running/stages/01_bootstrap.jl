#!/usr/bin/env julia

function main()
    if any(arg -> arg in ("--help", "-h"), ARGS)
        println("""
        Usage:
          julia --project=PrefPol PrefPol/composable_running/stages/01_bootstrap.jl --config PATH

        Phase 4 note:
          Bootstrap, imputation, and linearization are still executed by
          stages/04_measures.jl through the existing PrefPol.run_pipeline /
          PrefPol.run_batch path. This stage becomes independently executable
          in Phase 5.
        """)
        return nothing
    end

    println("""
    Phase 4 bootstrap wrapper.

    No bootstrap-only public runner is wired into the composable workflow yet.
    Run stages/04_measures.jl to execute the current nested pipeline end to end;
    Phase 5 separates bootstrap, imputation, and linearization.
    """)
    return nothing
end

main()
