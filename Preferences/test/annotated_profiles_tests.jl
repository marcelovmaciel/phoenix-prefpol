using DataFrames
using PooledArrays: PooledArray
using Random: MersenneTwister

ranking_dict(order::Vector{Symbol}) = Dict(candidate => rank for (rank, candidate) in enumerate(order))

function strict_profile_df()
    df = DataFrame(
        profile = [
            ranking_dict([:A, :B, :C]),
            ranking_dict([:A, :B, :C]),
            ranking_dict([:C, :B, :A]),
            ranking_dict([:C, :B, :A]),
        ],
        group = PooledArray([:A, :A, :B, :B]; compress = true),
        wave = [1, 1, 2, 2],
    )
    metadata!(df, "candidates", [:A, :B, :C])
    metadata!(df, "profile_kind", "linearized")
    return df
end

function weak_profile_df()
    df = DataFrame(
        profile = [
            Dict(:A => 1, :B => 1, :C => 2),
            Dict(:A => 2, :B => 1, :C => 1),
        ],
        group = PooledArray(["x", "y"]; compress = true),
        score = [10, 20],
    )
    metadata!(df, "candidates", [:A, :B, :C])
    metadata!(df, "profile_kind", "weak")
    return df
end

@testset "AnnotatedProfile constructors preserve typed metadata" begin
    weak_df = weak_profile_df()
    weak_bundle = pp.dataframe_to_annotated_profile(weak_df)

    @test weak_bundle.metadata.group isa PooledArray
    @test weak_bundle.metadata.score == [10, 20]

    metadata_df = select(weak_df, Not(:profile))
    from_metadata_df = pp.AnnotatedProfile(weak_bundle.profile, metadata_df)
    @test from_metadata_df.metadata.group isa PooledArray
    @test from_metadata_df.metadata.score == [10, 20]

    named_metadata = (
        group = PooledArray(["x", "y"]; compress = true),
        score = [10, 20],
    )
    from_namedtuple = pp.AnnotatedProfile(weak_bundle.profile, named_metadata)
    @test from_namedtuple.metadata.group isa PooledArray
    @test from_namedtuple.metadata.score == [10, 20]
end

@testset "AnnotatedProfile validates metadata length" begin
    weak_bundle = pp.dataframe_to_annotated_profile(weak_profile_df())

    @test_throws ArgumentError pp.AnnotatedProfile(
        weak_bundle.profile,
        (group = PooledArray(["x"]; compress = true),),
    )

    @test_throws ArgumentError pp.AnnotatedProfile(
        weak_bundle.profile,
        DataFrame(group = ["x"]),
    )
end

@testset "AnnotatedProfile DataFrame roundtrips" begin
    weak_df = weak_profile_df()
    weak_bundle = pp.dataframe_to_annotated_profile(weak_df)
    weak_roundtrip = pp.annotated_profile_to_dataframe(weak_bundle)

    @test metadata(weak_roundtrip, "candidates") == [:A, :B, :C]
    @test metadata(weak_roundtrip, "profile_kind") == "weak"
    @test weak_roundtrip.group isa PooledArray
    @test weak_roundtrip.profile == weak_df.profile

    strict_df = strict_profile_df()
    strict_bundle = pp.dataframe_to_annotated_profile(strict_df)
    strict_roundtrip = pp.annotated_profile_to_dataframe(strict_bundle)

    @test pp.is_strict(strict_bundle.profile)
    @test metadata(strict_roundtrip, "candidates") == [:A, :B, :C]
    @test metadata(strict_roundtrip, "profile_kind") == "linearized"
    @test strict_roundtrip.group isa PooledArray
    @test strict_roundtrip.profile == strict_df.profile
end

@testset "Compact strict artifact roundtrip preserves wire metadata" begin
    strict_bundle = pp.dataframe_to_annotated_profile(strict_profile_df())
    artifact = pp.compact_profile_artifact_dataframe(strict_bundle)
    restored = pp.dataframe_to_annotated_profile(artifact)

    @test artifact.profile isa PooledArray
    @test metadata(artifact, "candidates") == [:A, :B, :C]
    @test metadata(artifact, "profile_kind") == "linearized"
    @test metadata(artifact, "profile_encoding") == "rank_vector_v1"
    @test pp.is_strict(restored.profile)
    @test restored.metadata.group isa PooledArray
    @test pp.profile_to_ranking_dicts(restored.profile) ==
          pp.profile_to_ranking_dicts(strict_bundle.profile)
end

@testset "AnnotatedProfile subset and linearization preserve metadata alignment" begin
    weak_bundle = pp.dataframe_to_annotated_profile(weak_profile_df())
    subset = pp.subset_annotated_profile(weak_bundle, [2, 1])

    @test subset.metadata.group isa PooledArray
    @test collect(subset.metadata.group) == ["y", "x"]
    @test subset.metadata.score == [20, 10]
    @test pp.profile_to_ranking_dicts(subset.profile) == reverse(weak_profile_df().profile)

    linearized = pp.linearize_annotated_profile(weak_bundle; rng = MersenneTwister(1))
    @test pp.is_strict(linearized.profile)
    @test linearized.metadata.group isa PooledArray
    @test collect(linearized.metadata.group) == collect(weak_bundle.metadata.group)
    @test linearized.metadata.score == weak_bundle.metadata.score
end

@testset "AnnotatedProfile grouped metrics match DataFrame path" begin
    strict_df = strict_profile_df()
    strict_bundle = pp.dataframe_to_annotated_profile(strict_df)
    grouped_consensus = DataFrame(
        group = [:A, :B],
        consensus_ranking = Any[
            ranking_dict([:A, :B, :C]),
            ranking_dict([:C, :B, :A]),
        ],
    )

    C_df, D_df = pp.compute_group_metrics(strict_df, :group)
    C_bundle, D_bundle = pp.compute_group_metrics(strict_bundle, :group)

    @test isapprox(C_bundle, C_df; atol = 1.0e-12)
    @test isapprox(D_bundle, D_df; atol = 1.0e-12)
    @test isapprox(
        pp.overall_divergences(grouped_consensus, strict_bundle, :group),
        pp.overall_divergences(grouped_consensus, strict_df, :group);
        atol = 1.0e-12,
    )
end
