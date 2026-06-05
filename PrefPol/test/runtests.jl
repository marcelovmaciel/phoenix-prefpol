using Test

include("package_load_tests.jl")

include("eseb_semantics_tests.jl")

include("derived_groups_tests.jl")

include("profile_adapters_tests.jl")

include("raw_profiles_tests.jl")

include("test_survey_config_raw_profiles.jl")

include("processing_tests.jl")

include("preprocessing_tests.jl")

include("year_preprocessing_recoder_tests.jl")

include("polarization_measures_tests.jl")

include("nested_pipeline_tests.jl")

include("validation_matrix_tests.jl")

include("variance_decomposition_report_tests.jl")

try
    @eval using CairoMakie
    include("plotting_extension_tests.jl")
catch err
    @info "Skipping plotting_extension_tests.jl because CairoMakie is unavailable in the active test environment." err
end
