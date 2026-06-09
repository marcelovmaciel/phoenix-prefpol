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
include("procedure_hull.jl")
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
    procedure_hull_barycentric_4c,
    candidate_share_hull_barycentric_4c,
    procedure_hull_vertices_4c,
    candidate_share_hull_vertices_4c,
    procedure_hull_point_4c,
    candidate_share_hull_point_4c,
    borda_procedure_hull_point_4c,
    procedure_hull_4c,
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
    CONDORCET_GENERATORS_4C,
    DOUBLE_REVERSAL_EDGE_ORDER_4C,
    kernel_profile_4c,
    basic_profile_differential_4c,
    borda_profile_differential_4c,
    condorcet_profile_differential_4c,
    double_reversal_differential_4c,
    source_component_basis_matrix_4c,
    transformation_matrix,
    basis_matrix,
    decomposition_coefficients,
    decompose_profile,
    reconstruct,
    group_component,
    check_basis_identity,
    component_summary,
    draw_plain_triangle,
    plot_saari_triangle,
    draw_opened_tetrahedron,
    plot_opened_representation_tetrahedron,
    plot_profile_tetrahedron_freqs,
    plot_profile_on_opened_tetrahedron,
    plot_signed_profile_tetrahedron,
    plot_decomposition_coefficients,
    plot_decomposition_component_tetrahedra,
    plot_decomposition_reconstruction_check,
    positional_comparison_region_masks,
    positional_comparison_region_table,
    plot_positional_comparison_regions,
    plot_candidate_tally_tetrahedron,
    plot_saari_tetrahedron3d,
    plot_procedure_hull_4c,
    plot_procedure_hull_parameter_triangle

end
