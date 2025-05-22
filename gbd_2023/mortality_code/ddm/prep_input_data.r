  # Purpose: 1) Pull necessary input data from Mortality and GBD DB
  #          2) Store data in 'FILEPATH/{version_id}/inputs
  #
  # Notes: 1) This script will be launched via qsub by DDM's run_all (ddm_set_up.r)


rm(list=ls())
library(data.table); library(haven); library(argparse); library(readstata13);
library(assertable); library(DBI); library(readr); library(plyr);
library(mortdb); library(mortcore)

if (Sys.info()[1] == "Linux") {
  root <- "FILEPATH"
  username <- Sys.getenv("USER")
} else {
  root <- "FILEPATH"
}

parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version_id for this run of DDM')
parser$add_argument('--ddm_post_5q0', type="integer", required=TRUE,
                    help='Step of DDM: pre or post 5q0')
parser$add_argument('--gbd_year', type="integer", required=TRUE,
                    help='GBD round')
parser$add_argument('--data_5q0_version', type="character", required=FALSE,
                    help='The version_id 5q0 data')
parser$add_argument('--estimate_5q0_version', type="character", required=FALSE,
                    help='The version_id for 5q0 estimates')
parser$add_argument('--population_estimate_version', type="character", required=FALSE,
                    help='The version_id for population estimates')
args <- parser$parse_args()

version_id <- args$version_id
post_5q0 <- args$ddm_post_5q0
gbd_year <- args$gbd_year
population_estimate_version_id <- args$population_estimate_version

gbd_round_id <- get_gbd_round(gbd_year=gbd_year)
prev_gbd_year=get_gbd_year(gbd_round_id-1)
if (post_5q0 == 1){
  data_5q0_version <- args$data_5q0_version
  estimate_5q0_version <- args$estimate_5q0_version
  if (!data_5q0_version %in% c("best", "recent")) data_5q0_version <- as.numeric(data_5q0_version)
  else data_5q0_version <- get_proc_version(model_name = "5q0", model_type = "data", run_id = data_5q0_version)
  if (!estimate_5q0_version %in% c("best", "recent")) estimate_5q0_version <- as.numeric(estimate_5q0_version)
  else estimate_5q0_version <- get_proc_version(model_name = "5q0", model_type = "estimate", run_id = estimate_5q0_version)
} else {
  if (!population_estimate_version_id %in% c("best", "recent")) population_estimate_version_id <- as.numeric(population_estimate_version_id)
  else population_estimate_version_id <- get_proc_version(model_name = "population", model_type = "estimate", run_id = population_estimate_version_id)
}

outdir <- paste0("FILEPATH/", version_id, "/inputs/")

if (post_5q0 == 1){
  print(paste0("5q0 data version id: ", data_5q0_version))
  print(paste0("5q0 estimate version id: ", estimate_5q0_version))
  ####################################################################
  # 5q0 data- used in step c06 to calculate implied child completeness
  ####################################################################
  if((gbd_year==2017) & (estimate_5q0_version!="best")){
  data_5q0 <- get_mort_outputs(model_name = "5q0", model_type = "data", run_id = data_5q0_version, location_set_id = 82, outlier_run_id = estimate_5q0_version, adjustment = 0,gbd_year=prev_gbd_year)
  }else{
  data_5q0 <- get_mort_outputs(model_name = "5q0", model_type = "data", run_id = data_5q0_version, location_set_id = 82, gbd_year = gbd_year, outlier_run_id = estimate_5q0_version, adjustment = 0)
}

  data_5q0[, upload_5q0_data_id := NULL]
  data_5q0 <- unique(data_5q0) # Drop duplicates, if any

  # Check that the necessary variables are in the data set-
  assert_colnames(data_5q0, c('ihme_loc_id', 'location_id', 'year_id', 'viz_year', 'mean', 'source', 'outlier'), F)

  # Check that there are no NA values in any of the columns
  ihme_loc_na <-assert_values(data_5q0, c('ihme_loc_id', 'location_id', 'year_id', 'viz_year', 'mean', 'source', 'outlier'), 'not_na', warn_only = T)

  # Keep dir-unadjusted and CBH
  data_5q0 <- data_5q0[method_id %in% c(1), list(ihme_loc_id, year = viz_year, year_id, source, outlier, q5 = mean, nid, underlying_nid)]
  data_5q0[, "nid"] <- data_5q0[, as.character(nid)]
  data_5q0[, "underlying_nid"] <- data_5q0[, as.character(underlying_nid)]

  save.dta13(data_5q0, paste0("FILEPATH"))


  ######################################################################################################
  # 5q0 estimates (no shocks)- used in step c06 to calculate implied child completeness
  ######################################################################################################
  if((gbd_year==2017) & (estimate_5q0_version!="best")){
  est_5q0 <- get_mort_outputs(model_name = "5q0", model_type = "estimate",
                              run_id = estimate_5q0_version,
                              location_set_id = 82,
                              gbd_year=2017)
  }else{
  est_5q0 <- get_mort_outputs(model_name = "5q0", model_type = "estimate",
                              run_id = estimate_5q0_version,
                              location_set_id = 82,
                              gbd_year = gbd_year)
  }

  if(nrow(est_5q0) == 0) stop(paste0("5q0 estimates not in the database for version ", estimate_5q0_version))

  est_5q0 <- est_5q0[estimate_stage_id == 3]

  # Check that the correct id variables are in the data set-
  # (ihme_loc_id, viz_year (year), mean, lower, upper)
  assert_colnames(est_5q0, c('ihme_loc_id', 'location_id', 'year_id', 'viz_year', 'mean', 'lower', 'upper'), F)
  assert_values(est_5q0, c('ihme_loc_id', 'location_id', 'year_id', 'viz_year', 'mean', 'lower', 'upper'), 'not_na', warn_only = T)

  ages <- 1
  sexes <- 3
  locations <- unique(est_5q0$location_id)
  years <- 1950:gbd_year
  id_vars <- list(location_id = locations, age_group_id = ages, sex_id = sexes, year_id = years)
  test <- assert_ids(est_5q0, id_vars, warn_only = T)

  est_5q0 <- est_5q0[, list(ihme_loc_id, year_id, viz_year, med = mean, lower, upper)]

  save.dta13(est_5q0, paste0(outdir, "estimated_5q0_noshocks.dta"))
} else {

  ################################
  # Pull inputs for stata files
  # ##############################
  old_ap <- get_locations(level = 'estimate', gbd_type = 'ap_old', gbd_year = gbd_year)
  write_csv(old_ap, paste0("FILEPATH"))

  locations <- get_locations(gbd_year = gbd_year)
  write_csv(locations, paste0("FILEPATH"))

  ages <- get_age_map(gbd_year=gbd_year,type="all")
  write_csv(ages, paste0("FILEPATH"))

  ######################################################################################################
  # GBD population- used in c09 to compile populations to be used as denominators for other processes
  ######################################################################################################
  ages <- c(seq(1,26), 28, 30, 31, 32, 235, 158, 159)
  sexes <- 1:3
  years <- 1950:gbd_year

  population <- get_mort_outputs("population", "estimate", run_id = population_estimate_version_id, location_set_id=21, age_group_ids = ages, sex_ids = sexes, year_ids = years,gbd_year=gbd_year)
  population[, run_id := NULL]
  population <- population[age_group_id != 164]
  population <- population[, list(location_id, age_group_id, sex_id, year_id, population = mean)]

  locations <- unique(population[, location_id])
  id_vars <- list(age_group_id = ages, year_id = years, sex_id = sexes, location_id = locations)
  assert_ids(population, id_vars)
  assert_values(population, 'population', 'gte', 0)

  save.dta13(population, paste0("FILEPATH"))
}

# DONE
