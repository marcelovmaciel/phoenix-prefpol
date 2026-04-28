#!/usr/bin/env julia

function main()
    if any(arg -> arg in ("--help", "-h"), ARGS)
        println("""
        Usage:
          julia --project=PrefPol PrefPol/composable_running/stages/02_impute.jl --config PATH

        Phase 4 note:
          Imputation is still executed by stages/04_measures.jl through the
          existing PrefPol.run_pipeline / PrefPol.run_batch path. This stage
          becomes independently executable in Phase 5.
        """)
        return nothing
    end

    println("""
    Phase 4 imputation wrapper.

    No imputation-only public runner is wired into the composable workflow yet.
    Run stages/04_measures.jl to execute the current nested pipeline end to end;
    Phase 5 separates bootstrap, imputation, and linearization.
    """)
    return nothing
end

main()
