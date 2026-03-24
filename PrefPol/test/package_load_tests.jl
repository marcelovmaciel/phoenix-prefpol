using Test

@testset "package load smoke" begin
    project_dir = normpath(joinpath(@__DIR__, ".."))
    cmd = `$(Base.julia_cmd()) --startup-file=no --project=$(project_dir) -e "using PrefPol; println(\"prefpol-load-ok\")"`
    @test read(cmd, String) == "prefpol-load-ok\n"
end
