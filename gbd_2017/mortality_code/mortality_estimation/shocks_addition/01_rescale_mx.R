

###############################################################################################################
## Set up settings
  rm(list=ls())
  
  if (Sys.info()[1]=="Windows") {
    root <- "J:" 
    user <- Sys.getenv("USERNAME")
  } else {
    root <- "/home/j"
    user <- Sys.getenv("USER")
    task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
    ## If task_id is passed, it's an array job and the location will be determined by the loc_map
    ## If not, it's a restart and will pass in current_ihme as the first argument
    if(length(task_id) != 0 & !is.na(task_id)) {
      shocks_addition_run_id <- as.integer(commandArgs(trailingOnly = T)[1])
      reckoning_run_id <- as.integer(commandArgs(trailingOnly = T)[2])
      gbd_year <- as.integer(commandArgs(trailingOnly = T)[3])
    } else {
      if(length(commandArgs(trailingOnly = T)) != 4) stop("Expecting 4 arguments if not an array job")
      current_ihme <- as.character(commandArgs(trailingOnly = T)[1])
      shocks_addition_run_id <- as.integer(commandArgs(trailingOnly = T)[2])
      reckoning_run_id <- as.integer(commandArgs(trailingOnly = T)[3])
      gbd_year <- as.integer(commandArgs(trailingOnly = T)[4])
    }
  }


## Source libraries and functions
  library(readr)
  library(data.table)
  library(assertable)
  library(haven)
  library(tidyr)
  library(parallel)
  library(rhdf5)
  
  library(mortdb)
  library(mortcore)

## Set primary working directory
  data_dir <- paste0(FILEPATH, shocks_addition_run_id)
  input_dir_versioned <- paste0(data_dir, "/inputs")

## Set input directories for all versioned inputs
  full_lt_dir <- paste0(FILEPATH, reckoning_run_id)
  with_shock_dir <- paste0(full_lt_dir, "/abridged_lt/with_shock")
  no_shock_dir <- paste0(full_lt_dir, "/abridged_lt/with_hiv")
  no_hiv_dir <- paste0(full_lt_dir, "/abridged_lt/no_hiv")
  reckoning_dir <- paste0(FILEPATH, reckoning_run_id)

## Look up parallelization metadata
  loc_map <- fread(paste0(input_dir_versioned,"/lowest_locations.csv"))
  if(length(task_id != 0) & !is.na(task_id)) current_ihme <- loc_map[task_id, ihme_loc_id]
  
  current_location_id <- loc_map[ihme_loc_id == current_ihme, location_id]

## Set general ID variables
  lt_ids <- c("location_id", "year_id", "sex_id", "age_group_id", "draw")

## Age file
  age_map <- fread(paste0(input_dir_versioned,"/age_map.csv"))
  age_map <- age_map[, list(age_group_id, age_group_years_start)]
  setnames(age_map, "age_group_years_start", "age")


###############################################################################################################
## Import map files, assign ID values

## Population file
  population <- fread(paste0(input_dir_versioned, "/population.csv"))

## Create ID combinations to use with assert_ids
  sexes <- c("male", "female")
  years <- c(1950:gbd_year)
  sims <- c(0:999)
  ages <- unique(age_map$age)

  env_ages <- c(1:20, 30:32, 235)

## Create a shuffled year vector to avoid concurrent I/O logjams on various HDF files
  set.seed(current_location_id)
  shuffled_years <- sample(years, replace = FALSE)
  if(!identical(sort(shuffled_years), sort(years))) stop("Year resample failed")

## Child and adult sims
  sim_ids <- list(ihme_loc_id = current_ihme,
                  year = c(1950:gbd_year),
                  sex = c("male", "female"),
                  simulation = c(0:999))

  lt_sim_ids <- list(ihme_loc_id = current_ihme,
                     year = c(1950:gbd_year),
                     sex = c("male", "female"),
                     sim = c(0:999),
                     age = c(0, 1, seq(5, 110, 5)))


###############################################################################################################
## Import reckoning and shocks addition results, and generate mx scalar values
lt_with_shock <- fread(paste0(with_shock_dir, "/lt_abridged_draw_", current_location_id, ".csv"))
lt_no_shock <- fread(paste0(no_shock_dir, "/lt_abridged_draw_", current_location_id, ".csv"))
lt_no_hiv <- fread(paste0(no_hiv_dir, "/lt_abridged_draw_", current_location_id, ".csv"))

setnames(lt_with_shock, c("mx", "ax"), c("mx_with_shock", "ax_with_shock"))
setnames(lt_no_shock, c("mx", "ax"), c("mx_no_shock", "ax_no_shock"))
setnames(lt_no_hiv, c("mx", "ax"), c("mx_no_hiv", "ax_no_hiv"))

final_abridged_lts <- merge(lt_with_shock[, .SD, .SDcols = c("location_id", "year_id", "sex_id", "age", "draw", "mx_with_shock", "ax_with_shock")],
                 lt_no_shock[, .SD, .SDcols = c("location_id", "year_id", "sex_id", "age", "draw", "mx_no_shock", "ax_no_shock")],
                 by = c("location_id", "year_id", "sex_id", "age", "draw"))

final_abridged_lts <- merge(final_abridged_lts,
                            lt_no_hiv[, .SD, .SDcols = c("location_id", "year_id", "sex_id", "age", "draw", "mx_no_hiv", "ax_no_hiv")],
                            by = c("location_id", "year_id", "sex_id", "age", "draw"))

final_abridged_lts <- merge(final_abridged_lts, age_map[, .(age, age_group_id)], by = "age")
final_abridged_lts[, age := NULL]

shock_deaths <- fread(paste0(full_lt_dir, "/shock_numbers/finalizer_shock_deaths_", current_location_id, ".csv"))
setnames(shock_deaths, "deaths", "shock_specific_deaths")

###############################################################################################################
## Directly import and use U-5 Envelope values directly from number-space calculations
abridged_shock <- final_abridged_lts[age_group_id %in% env_ages, .SD, .SDcols = c(lt_ids, "mx_no_shock")]
abridged_shock <- merge(abridged_shock, population, by = lt_ids[lt_ids != "draw"])
abridged_shock <- merge(abridged_shock, shock_deaths, by = lt_ids)
abridged_shock[, deaths_no_shock := mx_no_shock * population]
abridged_shock[, deaths_with_shock := deaths_no_shock + shock_specific_deaths]

env_filenames <- paste0("combined_env_aggregated_", shuffled_years, ".h5")
env_no_shock <- assertable::import_files(env_filenames, 
                                        folder = paste0(reckoning_dir, "/envelope_whiv/result_pre_finalizer"),
                                        FUN = load_hdf,
                                        by_val = current_location_id)
setnames(env_no_shock, "mx_avg_whiv", "deaths_no_shock")
env_no_shock <- env_no_shock[age_group_id %in% c(2:5, 235)]
env_no_shock[, draw := as.integer(draw)]

env_no_hiv <- assertable::import_files(env_filenames, 
                                        folder = paste0(reckoning_dir, "/envelope_hivdel/result_pre_finalizer"),
                                        FUN = load_hdf,
                                        by_val = current_location_id)
setnames(env_no_hiv, "mx_hiv_free", "deaths_no_hiv")
env_no_hiv <- env_no_hiv[age_group_id %in% c(2:5, 235)]
env_no_hiv[, draw := as.integer(draw)]

env_merged <- merge(env_no_shock, env_no_hiv, by = lt_ids)
env_merged <- merge(env_merged, shock_deaths, by = lt_ids)
env_merged[, deaths_with_shock := shock_specific_deaths + deaths_no_shock]
env_merged[, shock_specific_deaths := NULL]

u5_results <- env_merged[age_group_id %in% c(2:5)]

nn_results <- u5_results[age_group_id %in% c(2:4)]

nn_results <- merge(nn_results, population, by = lt_ids[lt_ids != "draw"])
nn_results[, mx_with_shock := deaths_with_shock / population]
nn_results[, mx_no_shock := deaths_no_shock / population]
nn_results[, mx_no_hiv := deaths_no_hiv / population]
nn_results[, c("deaths_with_shock", "deaths_no_shock", "deaths_no_hiv", "population") := NULL]

final_abridged_lts <- rbindlist(list(final_abridged_lts, nn_results), use.names = T, fill = T)

## Generate 95+ envelope results
env_95_plus <- env_merged[age_group_id == 235]

## Combine the abridged + shock results along with the U5 env shock results and the 95+ shock results
final_shock_env <- rbindlist(list(abridged_shock[!age_group_id %in% c(2:5, 28, 235), .SD, .SDcols = c(lt_ids, "deaths_with_shock")],
                                  u5_results[, .SD, .SDcols = c(lt_ids, "deaths_with_shock")],
                                  env_95_plus[, .SD, .SDcols = c(lt_ids, "deaths_with_shock")]),
                             use.names = T)


###############################################################################################################
## Run assertions
final_abridged_lts[, rel_diff := (mx_no_shock - mx_no_hiv) / mx_no_hiv]
final_abridged_lts[rel_diff < 0 & rel_diff > -.000000001, mx_no_shock := mx_no_hiv]

final_abridged_lts[, rel_diff := (mx_with_shock - mx_no_shock) / mx_no_shock]
final_abridged_lts[rel_diff < 0 & rel_diff > -.000000001, mx_with_shock := mx_no_shock]

final_abridged_lts[, rel_diff := NULL]

assert_values(final_abridged_lts, "mx_with_shock", "gte", final_abridged_lts$mx_no_shock, warn_only = T)
assert_values(final_abridged_lts, "mx_no_shock", "gte", final_abridged_lts$mx_no_hiv) 
assert_values(final_abridged_lts, c("mx_with_shock", "mx_no_shock", "mx_no_hiv"), "gte", 0)

## Add assert_ids calls here

###############################################################################################################
## Output files

## Save NN mx and 1-4 for under-5 envelope substitution and NN mx/qx calculation
filepath <- paste0(data_dir, "/lowest_outputs/u5_env_", current_ihme, ".h5")
if(file.exists(filepath)) file.remove(filepath)
rhdf5::h5createFile(filepath)
invisible(lapply(years, save_hdf, data= u5_results, filepath=filepath, by_var="year_id", level=2))
rhdf5::H5close()

## Save 95+ envelope results for envelope substitution
filepath <- paste0(data_dir, "/lowest_outputs/over_95_env_", current_ihme, ".h5")
if(file.exists(filepath)) file.remove(filepath)
rhdf5::h5createFile(filepath)
invisible(lapply(years, save_hdf, data= env_95_plus, filepath=filepath, by_var="year_id", level=2))
rhdf5::H5close()

## Save final with-shock envelope results for envelope substitution
filepath <- paste0(data_dir, "/lowest_outputs/final_wshock_env_", current_ihme, ".h5")
if(file.exists(filepath)) file.remove(filepath)
rhdf5::h5createFile(filepath)
invisible(lapply(years, save_hdf, data= final_shock_env, filepath=filepath, by_var="year_id", level=2))
rhdf5::H5close()

## Save mx and ax for all ages
filepath <- paste0(data_dir, "/lowest_outputs/mx_ax_", current_ihme, ".h5")
if(file.exists(filepath)) file.remove(filepath)
rhdf5::h5createFile(filepath)
invisible(lapply(years, save_hdf, data= final_abridged_lts, filepath=filepath, by_var="year_id", level=2))
rhdf5::H5close()
