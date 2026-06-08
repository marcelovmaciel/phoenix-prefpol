module VotingGeometry

using LinearAlgebra
using StaticArrays
using Preferences
using PythonPlot

include("canonical_orders.jl")
include("bases.jl")
include("profile_vectors.jl")
include("scoring.jl")
include("simplex_geometry.jl")
include("decomposition.jl")
include("plots.jl")

export
    CANONICAL_3C_IDS,
    CANONICAL_4C_IDS,
    TETRAHEDRON_TEXT_POSITIONS,
    SaariBasis,
    SaariBasis3,
    SaariBasis4,
    canonical_permutations,
    ranking_labels,
    profile_vector,
    profile_counts,
    validate_profile_vector,
    positional_method_3a,
    plurality_3a,
    borda_3a,
    antiplurality_3a,
    standard_vote_matrix,
    get_4c_w_s,
    q_s_4candidates,
    plurality_4c_q_s,
    vote_for_two_4c_q_s,
    antiplurality_4c_q_s,
    borda_4c_q_s,
    winner_order,
    TRIANGLE_VERTICES,
    TETRAHEDRON_VERTICES,
    tern2cart,
    barycentric_to_cartesian,
    triangle_points_from_profile,
    tetrahedron_points_from_profile,
    opened_tetrahedron_vertices,
    midpoint,
    COMPONENT_LABELS,
    COMPONENT_GROUPS,
    transformation_matrix,
    basis_matrix,
    decomposition_coefficients,
    decompose_profile,
    reconstruct,
    group_component,
    check_basis_identity,
    draw_plain_triangle,
    plot_saari_triangle,
    draw_opened_tetrahedron,
    plot_profile_tetrahedron_freqs,
    positional_comparison_region_masks,
    plot_positional_comparison_regions,
    plot_saari_tetrahedron3d

end
