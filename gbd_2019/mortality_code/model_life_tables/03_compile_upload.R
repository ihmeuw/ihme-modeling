###############################################################################################################
## Compile summary measures (mean, lower, upper) of MLT outputs (with-HIV envelope and with-HIV and HIV-free lifetable)

## Major outputs:
##        Uploaded results of with-HIV envelope, with-HIV and HIV-free lifetable,
##        Standard qx, and entry weights

###############################################################################################################
## Set up settings
  rm(list=ls())

  if (Sys.info()[1]=="Windows") {
    user <- Sys.getenv("USERNAME")
  } else {
    user <- Sys.getenv("USER")
  }

  library(rhdf5)
  library(parallel)
  library(data.table)
  library(assertable)
  library(readr)
  library(argparse)
  library(mortdb, lib = "FILEPATH/r-pkg")

  db_host <- "ADDRESS"

  parser <- ArgumentParser()
  parser$add_argument('--version_id', type="integer", required=TRUE,
                      help='The version_id for this run')
  parser$add_argument('--mlt_envelope_version', type="integer", required=TRUE,
                      help='MLT envelope run id')
  parser$add_argument('--map_estimate_version', type="integer", required=TRUE,
                      help='MLT map estimate run id')
  parser$add_argument('--gbd_year', type="integer", required=TRUE,
                      help='GBD year')
  parser$add_argument('--mark_best', type="character", required=TRUE,
                      help='Mark run as best')

  args <- parser$parse_args()
  lt_run_id <- args$version_id
  env_run_id <- args$mlt_envelope_version
  lt_map_est_id <- args$map_estimate_version
  gbd_year <- args$gbd_year
  mark_best <- as.logical(args$mark_best)

  master_dir <- "FILEPATH"
  results_dir <- paste0(master_dir, "/summary/intermediary")
  stan_dir <- paste0(master_dir, "/standard_lts")
  upload_dir <- paste0(master_dir, "/summary/upload")
  input_dir <- paste0(master_dir, "/inputs")

  lt_upload_file <- paste0(upload_dir, "/lt_v", lt_run_id, ".csv")
  env_upload_file <- paste0(upload_dir, "/env_v", env_run_id, ".csv")
  map_est_upload_file <- paste0(upload_dir, "/lt_map_est_v", lt_map_est_id, ".csv")

###############################################################################################################
## Import map files, assign ID values
## Location file
  est_locs <- data.table(get_locations(level = "estimate", gbd_year = gbd_year))
  loc_merge_map <- est_locs[, .SD, .SDcols = c("ihme_loc_id", "location_id")]
  all_locs <- data.table(get_locations(level = "all", gbd_year = gbd_year))

  ## Age file
  lt_age_map <- fread(paste0(input_dir,"/age_map.csv"))
  env_age_map <- data.table(get_age_map(type = "all"))

  env_ages <- c(1:22, 23:26, 28, 30:32, 42, 157:159, 162, 235)
  env_age_map <- env_age_map[age_group_id %in% env_ages]

  ## Create ID combinations to use with assert_ids
  years <- c(1950:gbd_year)

  lt_ids <- list(location_id = unique(est_locs$location_id),
                  year_id = years,
                  sex_id = c(1:3),
                  age_group_id = unique(lt_age_map$age_group_id),
                  estimate_stage_id = c(12, 13),
                  life_table_parameter_id = c(1, 2, 3, 5)) # mx, ax, and qx
  qx_ids <- list(location_id = unique(est_locs$location_id),
                  year_id = years,
                  sex_id = c(1:3),
                  age_group_id = c(1, 199),
                  estimate_stage_id = c(12, 13),
                  life_table_parameter_id = c(3))
  env_ids <- list(location_id = unique(all_locs$location_id),
                  year_id = years,
                  sex_id = c(1:3),
                  age_group_id = unique(env_age_map$age_group_id),
                  estimate_stage_id = 12)

  shared_cols <- c(names(env_ids), "mean", "upper", "lower")


###############################################################################################################
## Import lifetable and envelope results (separate by year), and standard lifetable and empirical lifetable results (separate by location)
lt_results <- assertable::import_files(filenames = paste0("lt_", years, ".csv"),
                                       folder = results_dir,
                                       FUN = fread, multicore = F)
setnames(lt_results, "year", "year_id")
lt_results[sex == "male", sex_id := 1]
lt_results[sex == "female", sex_id := 2]
lt_results[sex == "both", sex_id := 3]
lt_results[, sex := NULL]

env_results <- assertable::import_files(filenames = paste0("env_", years, ".csv"),
                                        folder = results_dir,
                                        FUN = fread, multicore = F)

stan_results <- assertable::import_files(filenames = paste0("standard_", unique(est_locs$ihme_loc_id), ".csv"),
                                         folder = stan_dir,
                                         FUN = fread, multicore = F)

map_est_results <- assertable::import_files(filenames = paste0("weights_", unique(est_locs$ihme_loc_id), ".csv"),
                                            folder = stan_dir,
                                            FUN = fread, multicore = F)


###############################################################################################################
## Final formatting
## Life table parameters
lt_param_ids <- data.table(get_mort_ids("life_table_parameter"))
lt_param_ids <- lt_param_ids[, life_table_parameter_id, parameter_name]

lt_results <- lt_results[lt_parameter %in% c("mx", "ax", "qx", "ex")]
lt_results <- merge(lt_results, lt_param_ids, by.x = "lt_parameter", by.y = "parameter_name", all.x = T)
assert_values(lt_results, "life_table_parameter_id", "not_na", quiet = T)
lt_results[, lt_parameter := NULL]

## Standard life table
stan_results[, life_table_parameter_id := 3] # qx
stan_results[, estimate_stage_id := 8] # Standard lifetable

stan_results[sex == "male", sex_id := 1]
stan_results[sex == "female", sex_id := 2]

stan_results <- merge(stan_results, lt_age_map[, list(age_group_years_start, age_group_id)],
                      by.x = "age", by.y = "age_group_years_start", all.x = T)
stan_results <- merge(stan_results, loc_merge_map, by = "ihme_loc_id")

stan_results[, (c("ihme_loc_id", "age", "sex")) := NULL]
setnames(stan_results, "year", "year_id")

stan_results <- stan_results[!is.na(mean)]

## Lifetable map estimate
lt_source_ids <- data.table(get_mort_ids("source_type"))
setnames(lt_source_ids, "type_short", "source_type")
lt_source_ids <- lt_source_ids[, .SD, .SDcols = c("source_type_id", "source_type")]

mapping_file <- fread(paste0(input_dir, "/mlt_db_categories.csv"))
loc_spec_countries <- mapping_file[loc_indic == "location-specific", location_id]
universal_countries <- mapping_file[loc_indic == "universal", location_id]
usa_countries <- mapping_file[loc_indic == "USA", location_id]
zaf_countries <- mapping_file[loc_indic == "ZAF", location_id]

map_est_results <- merge(map_est_results, loc_merge_map, by.x = "ref_ihme", by.y = "ihme_loc_id", all.x = T)
setnames(map_est_results, "location_id", "ref_location_id")
map_est_results <- merge(map_est_results, loc_merge_map, by = "ihme_loc_id", all.x = T)

assert_values(map_est_results, "source_type_id", "not_na")

map_est_results[sex == "male", sex_id := 1]
map_est_results[sex == "female", sex_id := 2]

setnames(map_est_results, c("mean_weight", "n_lts", "ref_year", "year"), c("norm_weight", "num_draws", "ref_year_id", "year_id"))
map_est_results[, (c("ref_ihme", "ihme_loc_id", "sex", "source_type")) := NULL]

###############################################################################################################
## Run assertions on lifetables and envelope

## Check LT results
assert_colnames(lt_results, c(shared_cols, "life_table_parameter_id"))
assert_values(lt_results, c("mean"), "not_na", quiet=T)
assert_values(lt_results[life_table_parameter_id != 5], c("lower", "upper"), "not_na", quiet=T)

assert_ids(lt_results[!age_group_id %in% c(1, 199)],
           lt_ids)
assert_ids(lt_results[age_group_id %in% c(1, 199)],
           qx_ids)

## Check envelope results
assert_colnames(env_results, shared_cols)
assert_values(env_results[year_id >= 1955], c("mean", "lower", "upper"), "not_na", quiet=T)
assert_ids(env_results,
           env_ids)

env_results[year_id < 1955 & is.na(mean), mean := 0]

## Check standard LT results
assert_values(stan_results[!is.na(mean)], colnames(stan_results), "not_na", quiet = T)
assert_values(stan_results[is.na(mean)], colnames(stan_results)[!colnames(stan_results) %in% c("mean", "lower", "upper")], "not_na", quiet = T)

## Check map est results
assert_values(map_est_results, colnames(map_est_results), "not_na", quiet = T)


###############################################################################################################
## Output final csv files
lt_cols <- c("location_id", "year_id", "sex_id",
             "age_group_id", "estimate_stage_id", "life_table_parameter_id",
             "mean", "lower", "upper")
env_cols <- c("location_id", "year_id", "sex_id",
              "age_group_id", "estimate_stage_id",
              "mean", "lower", "upper")
map_est_cols <- c("ref_year_id", "ref_location_id", "sex_id",
                  "year_id", "location_id",
                  "source_type_id", "norm_weight", "num_draws", "life_table_category_id")

lt_results <- lt_results[, .SD, .SDcols = lt_cols]
stan_results <- stan_results[, .SD, .SDcols = lt_cols]
lt_results <- rbindlist(list(lt_results, stan_results), use.names = T)
setkeyv(lt_results, lt_cols[1:6])

env_results <- env_results[, .SD, .SDcols = env_cols]
setkeyv(env_results, env_cols[1:6])

map_est_results <- map_est_results[, .SD, .SDcols = map_est_cols]

write_csv(lt_results, lt_upload_file)
write_csv(env_results, env_upload_file)
write_csv(map_est_results, map_est_upload_file)


###############################################################################################################
## Upload results
upload_results(lt_upload_file, "mlt life table", "estimate",
               run_id = lt_run_id, hostname = db_host)

upload_results(env_upload_file, "mlt death number", "estimate",
               run_id = env_run_id, hostname = db_host)

upload_results(map_est_upload_file, "life table map", "estimate",
               run_id = lt_map_est_id, hostname = db_host)

if (mark_best) {
  update_status(model_name="mlt life table", model_type="estimate", new_status="best",
                  run_id = lt_run_id, send_slack = T )
  update_status(model_name="mlt death number", model_type="estimate", new_status="best",
                run_id = env_run_id, send_slack = T )
  update_status(model_name="life table map", model_type="estimate", new_status="best",
              run_id = lt_map_est_id, send_slack = T )
}
