  # Author: 
  # Date: 2/14/18
  # Purpose: 1) Pull necessary input data from Mortality and GBD DB
  #          2) Store data in '/ihme/mortality/ddm/{version_id}/inputs
  # 
  # Notes: 1) This script will be launched via qsub by DDM's run_all (ddm_set_up.r)


rm(list=ls())
library(data.table); library(haven); library(readstata13); library(assertable); library(DBI); library(readr); library(plyr); library(mortdb, lib = "FILEPATH"); library(mortcore, lib = "FILEPATH")

if (Sys.info()[1] == "Linux") {
  root <- "/home/j/"
  username <- Sys.getenv("USER")
  version_id <- commandArgs(trailingOnly = T)[1]
  post_5q0 <- as.numeric(commandArgs(trailingOnly = T)[2])
  gbd_year <- as.numeric(commandArgs(trailingOnly = T)[3])
  data_5q0_version <- as.numeric(commandArgs(trailingOnly = T)[4])
  estimate_5q0_version <- as.numeric(commandArgs(trailingOnly = T)[5])
  outdir <- as.character(commandArgs(trailingOnly = T)[6])
} else {
  root <- "J:/"
}

# Source necessary central comp shared functions
source(paste0(root, "FILEPATH/get_population.R"))

if (post_5q0 == 1){
  print(paste0("5q0 data version id: ", data_5q0_version))
  print(paste0("5q0 estimate version id: ", estimate_5q0_version))
  ####################################################################
  # 5q0 data- used in step c06 to calculate implied child completeness
  ####################################################################
  data_5q0 <- data.table(get_mort_outputs(model_name = "5q0", model_type = "data", run_id = data_5q0_version, location_set_id = 82, gbd_year = gbd_year, outlier_run_id = estimate_5q0_version, adjustment = 0))

  # Temporarily drop SAU subnationals and add Old AP
  data_5q0[, upload_5q0_data_id := NULL]
  data_5q0 <- unique(data_5q0) # Drop duplicates, if any

  # Check that the necessary variables are in the data set- 
  # (ihme_loc_id, year_id (data_year), viz_year (year), mean (u5_obs), source, outlier, shock)
  assert_colnames(data_5q0, c('ihme_loc_id', 'location_id', 'year_id', 'viz_year', 'mean', 'source', 'outlier'), F)

  # Check that there are no NA values in any of the columns
  ihme_loc_na <-assert_values(data_5q0, c('ihme_loc_id', 'location_id', 'year_id', 'viz_year', 'mean', 'source', 'outlier'), 'not_na', warn_only = T)

  # Keep dir-unadjusted and CBH
  data_5q0 <- data_5q0[method_id %in% c(1), list(ihme_loc_id, year = viz_year, year_id, source, outlier, q5 = mean, nid, underlying_nid)]
  data_5q0[, "nid"] <- data_5q0[, as.character(nid)]
  data_5q0[, "underlying_nid"] <- data_5q0[, as.character(underlying_nid)]

  save.dta13(data_5q0, paste0(outdir, "raw.5q0.unadjusted.dta"))


  ######################################################################################################
  # 5q0 estimates (no shocks)- used in step c06 to calculate implied child completeness
  ######################################################################################################
  est_5q0 <- data.table(get_mort_outputs(model_name = "5q0", model_type = "estimate", run_id = estimate_5q0_version, location_set_id = 82, gbd_year = gbd_year))
  est_5q0 <- est_5q0[estimate_stage_id == 3]

  # Check that the correct id variables are in the data set- 
  # (ihme_loc_id, viz_year (year), mean, lower, upper)
  assert_colnames(est_5q0, c('ihme_loc_id', 'location_id', 'year_id', 'viz_year', 'mean', 'lower', 'upper'), F)
  assert_values(est_5q0, c('ihme_loc_id', 'location_id', 'year_id', 'viz_year', 'mean', 'lower', 'upper'), 'not_na', warn_only = T)

  ages <- 1
  sexes <- 3
  locations <- unique(est_5q0$location_id)
  years <- 1950:2017
  id_vars <- list(location_id = locations, age_group_id = ages, sex_id = sexes, year_id = years)
  test <- assert_ids(est_5q0, id_vars, warn_only = T)

  est_5q0 <- est_5q0[, list(ihme_loc_id, year_id, viz_year, med = mean, lower, upper)]

  save.dta13(est_5q0, paste0(outdir, "estimated_5q0_noshocks.dta"))
} else {
  ######################################################################################################
  # 45q15 data- used in step c10 to exclude outliers when creating the CoD completeness file
  ######################################################################################################
  data_45q15 <- data.table(get_mort_outputs(model_name = "45q15", "data", run_id = 'best', gbd_year = 2017, outlier_run_id = 'active'))

  # Check that the correct id variables are in the data set
  # Check that there are no NA values
  # Check that the data set is square
  assert_colnames(data_45q15, c('ihme_loc_id', 'method_id', 'location_id', 'year_id', 'viz_year', 'mean'), F)
  data_45q15 <- data_45q15[, list(ihme_loc_id, year_id, method_id, outlier, adjustment, source_name)]
  assert_values(data_45q15, c('ihme_loc_id', 'year', 'year_id', 'method_id','outlier', 'adjustment', 'source_name'), 'not_na')

  # Keep dir-unadjust
  data_45q15 <- data_45q15[adjustment == 0, list(ihme_loc_id, year = year_id, exclude = outlier, source_type = source_name)]
  save.dta13(data_45q15, paste0(outdir, "raw.45q15.dta"))


  ######################################################################################################
  # GBD population- used in c09 to compile populations to be used as denominators for other processes
  ######################################################################################################
  population <- get_population(location_id = 'all',
                               location_set_id = 21,
                               status = 'recent',
                               age_group_id = 'all',
                               year_id = 'all',
                               sex_id = 'all',
                               gbd_round_id = 5)

  population[, run_id := NULL]
  population <- population[age_group_id != 164]

  ages <- c(seq(1,26), 28, 30, 31, 32, 235, 158, 159)
  years <- 1950:2017
  sexes <- 1:3
  locations <- unique(population[, location_id])
  id_vars <- list(age_group_id = ages, year_id = years, sex_id = sexes, location_id = locations)
  assert_ids(population, id_vars)
  assert_values(population, 'population', 'gte', 0)

  save.dta13(population, paste0(outdir, "gbd_population_estimates.dta"))
}

# DONE