################################################################################
## Purpose:   Post-process model results for nonfirstborn proportion and
##            save as modelable entity to be used for births_at_risk_for_rhesus.R
## Input:     MEID 25054 (stgpr)
## Output:    MEID 28665 (custom)
################################################################################

# set up -----------------------------------------------------------------------
`%>%` <- magrittr::`%>%`
params_global <- readr::read_rds("params_global.rds")

# pull draws -------------------------------------------------------------------
birth_order_draws <- ihme::get_draws('modelable_entity_id', gbd_id = 25054,
                                     source = 'stgpr',
                                     status = 'best',
                                     release_id = 16,
                                     sex_id = 3,
                                     year_id = params_global$year_id,
                                     location_id = params_global$location_id)

# pull population and ASFR of mothers ------------------------------------------
pop <- ihme::get_population(
  age_group_id = c(7:15),
  year_id = params_global$year_id,
  location_id = params_global$location_id,
  sex_id = 2,
  release_id = 16
)

asfr <- ihme::get_covariate_estimates(
  covariate_id = 13,
  age_group_id = c(7:15),
  year_id = params_global$year_id,
  location_id = params_global$location_id,
  sex_id = 2,
  release_id = 16
)

births <- merge(
  pop[,c("location_id", "year_id", "population", "age_group_id")],
  asfr[,c("location_id", "year_id", "mean_value", "age_group_id")],
  by = c("location_id", "year_id", "age_group_id")
)

births_count <- births %>%
  dplyr::mutate(births = population * mean_value)

births_count <- merge(
  birth_order_draws,
  births_count[, c("location_id", "year_id", "age_group_id", "births")],
  by = c("location_id",
         "year_id",
         "age_group_id")
)

# calculating each draw value to be the count of nonfirstborns
birth_order_count <- births_count %>%
  dplyr::mutate(across(starts_with("draw_"), ~ . * births)) %>%
  dplyr::select(starts_with("draw_"), location_id, year_id, age_group_id, births)

# collapsing the mother's age groups by summing the count of nonfirstborns for each location-year
birth_order_count <- birth_order_count %>%
  dplyr::group_by(location_id, year_id) %>%
  dplyr::summarise(across(starts_with("draw_"), sum, .names = "{.col}"),
                   total_births = sum(births),
                   .groups = 'drop')

# calculate each draw to be a proportion of livebirths that are nonfirstborn
nonfirstborn_proportion <- birth_order_count %>%
  dplyr::mutate(across(starts_with("draw_"), ~ . / total_births))

# limit proportion value to 1
nonfirstborn_proportion <- nonfirstborn_proportion %>%
  dplyr::mutate(across(starts_with("draw_"), ~ pmin(1, .)))

# validations for saving ME
nonfirstborn_proportion$sex_id = 3
nonfirstborn_proportion$age_group_id = 164
nonfirstborn_proportion$measure_id = 18
nonfirstborn_proportion$metric_id = 3
nonfirstborn_proportion$modelable_entity_id = 28665

# SAVING TO MEID ###############################################################
data.table::fwrite(nonfirstborn_proportion, file.path("FILEPATH"))

index_df <-
  ihme::save_results_epi(
    input_dir = "FILEPATH",
    input_file_pattern = "nonfirstborn_proportion_all_draws.csv",
    modelable_entity_id = 28665,
    description = "neonatal_hemolytic_nonfirstborn_proportion_draws_final_CCF",
    birth_prevalence = TRUE,
    sex_id = 3,
    measure_id = 18,
    metric_id = 3,
    release_id = 16,
    mark_best = TRUE,
    bundle_id = 7667,
    crosswalk_version_id = 45623
  )
