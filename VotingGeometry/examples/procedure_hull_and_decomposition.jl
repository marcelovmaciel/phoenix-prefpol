using VotingGeometry

p4 = collect(1.0:24.0)
labels = ("A", "B", "C", "D")

hull = procedure_hull_4c(p4; labels = labels)
println("Borda Saari hull barycentric coordinates: ", hull.borda_barycentric)
println("Borda raw score tally: ", hull.borda_point)
println("Borda candidate score shares: ", q_s_4candidates(p4, 2 / 3, 1 / 3))

plot_candidate_tally_tetrahedron(; labels = labels)
plot_procedure_hull_4c(p4; labels = labels)
plot_procedure_hull_parameter_triangle()

comparison_rows = positional_comparison_region_table(
    p4,
    labels;
    comparisons = [(:A, ">=", :B), (:C, ">", :D)],
    resolution = 51,
)
println("Diagnostic grid comparison proportions:")
foreach(println, comparison_rows)

dec = decompose_profile(p4)
println("Top-level group summary:")
foreach(println, component_summary(dec; by = :group))

plot_decomposition_coefficients(dec; by = :group)
plot_decomposition_component_tetrahedra(dec; groups = (:kernel, :condorcet, :double_reversals))
plot_decomposition_reconstruction_check(dec; labels = labels)
