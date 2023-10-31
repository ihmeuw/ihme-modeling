##################################################
## Project: CVPDs
## Script purpose: Get counterfactual, pre-adjusted GBD 2020 estimates for causes of interest
## Date: April 2021
## Author: username
##################################################
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
}
# SOURCE SHARED FUNCTIONS ------------------------------
invisible(sapply(list.files("/filepath", full.names = T), source))

# Source LRI helper functions
source(paste0("filepath/lri_helpers.R"))
# Source ST-GPR flat file helper function
source("/filepath/collapse_point.R")

get_counterfactual <- function(cause, use_stgpr, locs, gbd_round_id = 7, age_group_ids = 22, years = 2020,
                               compare_version = 7347, burdenator_version = 167, como_version = 869){
  if(age_group_ids != 22) stop ("update this code to work with age-specific!")
  # create cause map
  cause_map = data.table(user_cause = c("flu", "measles", "pertussis", "RSV", "lri_pneumo", "lri_hib", "lri"),
                         acause = c("lri", "measles", "pertussis", "lri", "lri", "lri", "lri"),
                         etiology = c("influenza", NA, NA, "rsv", "pneumococcal", "hib", "NA"),
                         cause_id = c(322, 341, 339, 322, 322, 322),
                         rei_id = c(187, NA, NA, 190, 188, 189, NA))
  # get location data
  hierarchy <- get_location_metadata(35, gbd_round_id = gbd_round_id, decomp_step = "iterative")
  rei_map <- get_ids("rei")

  cause_map_tmp <- cause_map[user_cause == cause]
  if(nrow(cause_map_tmp) > 1) stop("need to check on cause map")
  if (!is.na(cause_map_tmp$etiology)){
    # use burdenator to get etiology-specific results
    ### Age set to pull - **must** pull for most granular ages then combine.
    age_meta <- get_age_metadata(age_group_set_id = 19, gbd_round_id = gbd_round_id)
    age_group_detail <- age_meta$age_group_id # most granular age group ids
    n_draws <- 100
    if (n_draws == 100) drawnames <- paste0("draw_", 0:99) else drawnames <- paste0("draw_", 0:999)
    inc_props <- get_draws(source = "burdenator",
                           gbd_id_type = c("rei_id","cause_id"),
                           gbd_id = c(cause_map_tmp$rei_id, cause_map_tmp$cause_id),
                           gbd_round_id = gbd_round_id,
                           decomp_step = "iterative",
                           measure_id = c(1,3), #YLD, deaths
                           metric_id = 2, # percent
                           age_group_id = age_group_detail,
                           year_id = years,
                           location_id = locs,
                           sex_id = c(1,2), # sex aggregation not supported
                           version_id = burdenator_version,
                           num_workers = 10
    )

    inc_props[measure_id == 3, measure_id := 6]

    ### Pull LRI incidence rate.
    lri_inc <- get_draws(source = "como",
                         gbd_id_type = c("cause_id"),
                         gbd_id = cause_map_tmp$cause_id,
                         gbd_round_id = gbd_round_id,
                         decomp_step = "iterative",
                         measure_id = 6, # incidence
                         metric_id = 3, # number
                         age_group_id = age_group_detail,
                         year_id = years,
                         location_id = locs,
                         version_id = como_version,
                         sex_id = c(1,2), # sex aggregation not supported
                         num_workers = 10
    )

    ### Convert rate to number space with population
    pop <- get_population(age_group_id = age_group_detail, gbd_round_id = gbd_round_id, status = "best",
                          decomp_step = "iterative", sex_id = c(1,2), location_id = locs, year_id = years)
    lri_inc <- merge(lri_inc, pop, by = c('location_id', 'sex_id', 'year_id', 'age_group_id'))
    lri_inc[, (drawnames) := lapply(.SD, "*", population), .SDcols = drawnames]

    ### Get case counts by etiology
    inc_cases <- get_counts(proportions=inc_props, overall_nums=lri_inc,n_draws = n_draws)

    ### Sum acrosss all age groups and across sex
    inc_cases_aggregate_age <- inc_cases[, lapply(.SD, sum), by=.(location_id, year_id, rei_id, measure_id), .SDcols=drawnames]
    inc_cases_aggregate_age$sex <- "Both"
    inc_cases_aggregate_age$age_group_id <- age_group_ids

    ### Summarize the draws, remove unneccesary columns, and write out
    gbdestfor2020 <- confidence_intervals(inc_cases_aggregate_age, n_draws = n_draws)
    gbdestfor2020 <- neaten(gbdestfor2020, rei_map, hierarchy)
    gbdestfor2020$measure_name <- "Incidence"
    gbdestfor2020$sex_id <- 3

    # pull deaths from get_ouputs
    deaths_dt <- get_outputs(topic = "rei", cause_id = cause_map_tmp$cause_id,
                             rei_id = cause_map_tmp$rei_id,
                             year_id = years, sex_id = 3,
                             age_group_id = age_group_ids,
                             location_id = locs,
                             gbd_round_id = gbd_round_id,
                             decomp_step = "iterative",
                             measure_id = 1,
                             metric_id = 1,
                             compare_version = compare_version)
    setnames(deaths_dt, "val", "mean")
    deaths_dt <- deaths_dt[, names(gbdestfor2020), with = F]
    gbdestfor2020 <- rbind(gbdestfor2020, deaths_dt, fill = T)
    deaths_dt <- deaths_dt[measure_id == 1,.(location_id, mean, rei_name)]
  } else if (is.na(cause_map_tmp$etiology)){
    gbdestfor2020 <- get_outputs(topic = "cause", cause_id = cause_map_tmp$cause_id,
                                 year_id = years, sex_id = 3,
                                 age_group_id = age_group_ids,
                                 location_id = locs,
                                 gbd_round_id = gbd_round_id,
                                 decomp_step = "iterative",
                                 measure_id = c(1,6),
                                 metric_id = 1,
                                 compare_version = compare_version)
    setnames(gbdestfor2020, "val", "mean")
    deaths_dt <- gbdestfor2020[measure_id == 1]
    if(use_stgpr){
      custom_nf_results <- fread("/filepath/03_case_predictions_rescaled.csv")
      gbdestfor2020 <- custom_nf_results[year_id==years]
      gbdestfor2020 <- collapse_point(gbdestfor2020, draws_name = "case_draw")
      gbdestfor2020[, `:=` (age_group_id = age_group_ids, measure_id = 6, metric_id = 1,
                            sex_id = 3, measure_name = "Incidence")]
      gbdestfor2020 <- gbdestfor2020[location_id %in% locs]
      deaths_dt <- deaths_dt[, names(gbdestfor2020), with = F]
      gbdestfor2020 <- rbind(gbdestfor2020, deaths_dt, fill = T)
      gbdestfor2020 <- merge(gbdestfor2020, hierarchy[,.(location_id, location_name)], by="location_id")
    }
  }
  return(gbdestfor2020)
}
