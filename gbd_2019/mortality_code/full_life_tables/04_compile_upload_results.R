###############################################################################################################
## Compile summary measures (mean, lower, upper) of full life tables

###############################################################################################################
## Set up settings
  rm(list=ls())

  if (Sys.info()[1]=="Windows") {
    root <- "FILEPATH"
    user <- Sys.getenv("USERNAME")
  } else {
    root <- "FILEPATH"
    user <- Sys.getenv("USER")
  }

  # full_lt_run_id <- 71
  # gbd_year <- 2017

  library(rhdf5)
  library(parallel)
  library(data.table)
  library(assertable)
  library(readr)
  library(argparse)
  library(mortdb, lib = "FILEPATH/r-pkg")


  # Parse arguments
  parser <- ArgumentParser()
  parser$add_argument('--no_shock_death_number_estimate_version', type="integer", required=TRUE,
                      help='The run id of no shock death number estimate')
  parser$add_argument('--gbd_year', type="integer", required=TRUE,
                      help="GBD year")
  parser$add_argument('--full_life_table_estimate_version', type="integer", required=TRUE,
                      help="Full life table estimate version")
  parser$add_argument('--upload_all_lt_params_flag', type="character", required=TRUE,
                      help="Boolean to determine upload all lt params")


  args <- parser$parse_args()
  full_lt_run_id <- args$no_shock_death_number_estimate_version
  gbd_year <- args$gbd_year
  upload_all_lt_params <- as.logical(args$upload_all_lt_params_flag)
  full_life_table_estimate_version <- args$full_life_table_estimate_version

  db_host <- "ADDRESS"

  master_dir <- "FILEPATH + full_lt_run_id"
  input_dir <- paste0(master_dir, "/inputs")


###############################################################################################################
## Import map files, assign ID values
## Location file
  all_locations <- fread(paste0(input_dir, "/location_hierarchy.csv"))
  all_locations <- all_locations[level >= 3]
  lowest_locs <- setDT(get_locations(level = "lowest"))
  lowest_locs <- unique(lowest_locs$location_id)
  agg_locs <- unique(all_locations[!location_id %in% lowest_locs, location_id])

  ## Age file
  lt_age_map <- fread(paste0(input_dir,"/age_groups_sy.csv"))[, .(age_group_id, age_group_years_start)]

  ## Life table parameters
  lt_param_map <- setDT(get_mort_ids("life_table_parameter"))[, .(life_table_parameter_id, life_table_parameter_name = parameter_name)]

  ## Create ID combinations to use with assert_ids
  years <- c(1950:gbd_year)

  if(upload_all_lt_params == T) params_lt_upload <- c(1, 2, 3, 4, 5, 7, 8)
  if(upload_all_lt_params == F) params_lt_upload <- 3 # qx only

  lt_ids <- list(location_id = unique(all_locations$location_id),
                  year_id = years,
                  sex_id = c(1:2),
                  age_group_id = unique(lt_age_map$age_group_id),
                  life_table_parameter_id = params_lt_upload,
                  estimate_stage_id = c(5:7)) # All LT outputs except pred_ex


###############################################################################################################
## Create function to programmatically import all files for upload, and run file checks etc.
prep_upload_files <- function(output_type, agg_type) {
  print(paste0("Preparing ", output_type))
  if(output_type == "with_shock") target_est_stage_id <- 6
  if(output_type == "with_hiv") target_est_stage_id <- 5
  if(output_type == "no_hiv") target_est_stage_id <- 7

  import_folder <- paste0(master_dir, "/full_lt/", output_type)
  if(agg_type == "lowest") filenames <- paste0("summary_full_", lowest_locs, ".csv")
  if(agg_type == "aggregated") filenames <- paste0("summary_full_", agg_locs, "_aggregated.csv")

  results <- assertable::import_files(filenames = filenames,
                                      folder = import_folder,
                                      FUN = fread,
                                      multicore = T,
                                      mc.cores = 4)

  results[, estimate_stage_id := target_est_stage_id]

  return(results)
}

compiled_data <- rbindlist(mclapply(c("with_shock", "with_hiv", "no_hiv"), prep_upload_files, agg_type = "lowest", mc.cores = 3))

## Merge on LT parameter name
compiled_data <- merge(compiled_data, lt_param_map, by = "life_table_parameter_name")
compiled_data[, life_table_parameter_name := NULL]

if(upload_all_lt_params == F) compiled_data <- compiled_data[life_table_parameter_id %in% params_lt_upload]

## Merge on age map
compiled_data <- merge(compiled_data, lt_age_map, by.x = "age", by.y = "age_group_years_start")
compiled_data[, age := NULL]

## Import non-lowest results
agg_data <- rbindlist(mclapply(c("with_shock", "with_hiv", "no_hiv"), prep_upload_files, agg_type = "aggregated", mc.cores = 3))

if(upload_all_lt_params == F) agg_data <- agg_data[life_table_parameter_id %in% params_lt_upload]

## Assert IDs at the end
merged_data <- rbindlist(list(compiled_data, agg_data), use.names = T)

assert_ids(merged_data, id_vars = lt_ids)

write_csv(merged_data, paste0(master_dir, "/upload/prepped_full_lt.csv"))


###############################################################################################################
## Upload results

upload_results(paste0(master_dir, "/upload/prepped_full_lt.csv"),
               "full life table", "estimate",
               run_id = full_life_table_estimate_version)

