using Revise
using DataFrames
using PrefPol
import PrefPol as pp

years = [2006, 2018, 2022]
scenario_by_year = Dict(
    2006 => "lula_alckmin_heloisa_serra_cristovam",
    2018 => "main_four",
    2022 => "lula_bolsonaro_ciro_marina_tebet",
)
k_by_year = Dict(
    2006 => 4,
    2018 => 4,
    2022 => 4,
)

# Full candidate-set summaries
for year in years
    println("\n" * "="^72)
    println("Year $year")
    println("="^72)

    raw = pp.load_raw_pref_data(year)
    println("m = $(raw.m)")
    println("candidates = $(raw.candidate_labels)")

    println("\nUnweighted profile-pattern proportions:")
    prof_u = pp.build_profile(raw.df, year;
                              weighted = false,
                              allow_ties = true,
                              allow_incomplete = true)
    tbl_u = pp.profile_pattern_proportions(prof_u; weighted = false)
    pp.pretty_print_profile_patterns(tbl_u; digits = 4)

    println("\nWeighted profile-pattern proportions:")
    prof_w = pp.build_profile(raw.df, year;
                              weighted = true,
                              allow_ties = true,
                              allow_incomplete = true)
    tbl_w = pp.profile_pattern_proportions(prof_w; weighted = true)
    pp.pretty_print_profile_patterns(tbl_w; digits = 4)
end

# Top-k candidate-set summaries (separate loop)
for year in years
    scenario = scenario_by_year[year]
    k = k_by_year[year]
    println("\n" * "="^72)
    println("Year $year (scenario = $scenario, k = $k)")
    println("="^72)

    # 1) Resolve scenario-specific candidate pool using the project's scenario logic.
    # 2) Apply k as a pure truncation over that pool, so k < |forced scenario set|
    #    does not error in this demo.
    raw = pp.load_raw_pref_data(year; scenario_name = scenario)
    candidate_pool = raw.candidate_cols
    kk = min(k, length(candidate_pool))
    candidate_set = candidate_pool[1:kk]
    println("candidate_pool (scenario): ", candidate_pool)
    println("candidate_set (k = $(length(candidate_set))): ", candidate_set)

    prof_topk = pp.build_profile(raw;
                                 weighted = true,
                                 allow_ties = true,
                                 allow_incomplete = true,
                                 candidate_set = candidate_set)
    tbl_topk = pp.profile_pattern_proportions(
        prof_topk;
        weighted = true,
    )
    pp.pretty_print_profile_patterns(tbl_topk; digits = 4, others_threshold = 0.01)
end

# Top-k candidate-set summaries with fixed k = 3 (bottom loop)
for year in [2018]
    for k in [3,4]
    scenario = scenario_by_year[year]
    
    println("\n" * "="^72)
    println("Year $year (scenario = $scenario, k = $k)")
    println("="^72)

    raw = pp.load_raw_pref_data(year; scenario_name = scenario)
    candidate_pool = raw.candidate_cols
    kk = min(k, length(candidate_pool))
    candidate_set = candidate_pool[1:kk]
    println("candidate_pool (scenario): ", candidate_pool)
    println("candidate_set (k = $(length(candidate_set))): ", candidate_set)

    prof_topk3 = pp.build_profile(raw;
                                  weighted = true,
                                  allow_ties = true,
                                  allow_incomplete = true, all_unranked_as_indifferent = true,
                                  candidate_set = candidate_set)
    tbl_topk3 = pp.profile_pattern_proportions(
        prof_topk3;
        weighted = true,
    )
        pp.pretty_print_profile_patterns(tbl_topk3; digits = 4)
    end
end 

println("\n" * "="^72)
println("Navigation + type decomposition demo (2018, main_four, k = 4)")
println("="^72)

year = 2022
scenario = "lula_bolsonaro_ciro_marina_tebet"
k_demo = 4

raw_demo = pp.load_raw_pref_data(year; scenario_name = scenario)
candidate_set_demo = raw_demo.candidate_cols[1:min(k_demo, length(raw_demo.candidate_cols))]
println("candidate_set_demo: ", candidate_set_demo)

# for weighted_mode in (true)
#     println("\n--- weighted = $weighted_mode ---")
#     prof_demo = pp.build_profile(
#         raw_demo;
#         weighted = weighted_mode,
#         allow_ties = true,
#         allow_incomplete = true,
#         all_unranked_as_indifferent = true,
#         candidate_set = candidate_set_demo,
#     )

#     nav = pp.profile_ranksize_summary(
#         prof_demo;
#         k = length(candidate_set_demo),
#         weighted = weighted_mode,
#         include_zero_rank = true,
#     )
#     pp.pretty_print_ranksize_summary(nav; digits = 4)

#     type_tbl = pp.profile_ranking_type_proportions(
#         prof_demo;
#         k = length(candidate_set_demo),
#         weighted = weighted_mode,
#         include_zero_rank = true,
#     )
#     pp.pretty_print_ranking_type_proportions(type_tbl; digits = 4)

# end


prof_demo = pp.build_profile(
        raw_demo;
        weighted = true,
        allow_ties = true,
        allow_incomplete = true,
        all_unranked_as_indifferent = true,
        candidate_set = candidate_set_demo,)


type_tbl = pp.profile_ranking_type_proportions(prof_demo;
        k = length(candidate_set_demo),
        weighted = true,
        include_zero_rank = true)

pp.pretty_print_ranking_type_proportions(type_tbl; digits = 4)




foo = (vcat(map(x->x[:table],values(type_tbl[8]))...))[!, [:template, :proportion]]






df2 = transform(foo, :proportion => (x -> x .* 100) => :percent)[!, [:template, :percent]]
