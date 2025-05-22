################################################################################
## Description: Save final live birth estimate draws and summaries in separate
##              files by location in csvs (rather than h5 files for easy use by
##              other processes and teams).
################################################################################

library(data.table)
library(rhdf5)
library(assertable)
library(readr)
library(mortcore)
library(mortdb)

rm(list = ls())
USER <- Sys.getenv("USER")
task <- Sys.getenv("SLURM_ARRAY_TASK_ID")
Sys.umask(mode = "0002")


# Get arguments -----------------------------------------------------------

# load step specific settings into Global Environment
parser <- argparse::ArgumentParser()
parser$add_argument("--live_births_vid", type = "character", default = "99999",
                    help = 'The version number for this run of live births, used to read in settings file')
parser$add_argument('--task_map_dir', type="character", default = "FILEPATH",
                    help='The filepath to the task map file that specifies the location id to run for')
parser$add_argument("--test", type = "character", default = T,
                    help = 'Whether this is a test run of the process')
parser$add_argument("--code_dir", type = "character", 
                    help = 'Location of code')
args <- parser$parse_args()
args$test <- as.logical(args$test)
list2env(args, .GlobalEnv); rm(args)

# use task id to get arguments from task map
task_map <- fread(task_map_dir)
loc_id <- task_map[task_id == task, location_id]

# load model run specific settings into Global Environment
settings_dir <- paste0("FILEPATH", ifelse(test, "test/", ""), live_births_vid, "/run_settings.csv")
load(settings_dir)
list2env(settings, envir = environment())

age_groups <- fread(paste0(output_dir, "/inputs/age_groups.csv"))
location_hierarchy <- fread(paste0(output_dir, "/inputs/all_reporting_hierarchies.csv"))
location_hierarchy <- location_hierarchy[location_set_name == "live_births" & location_id == loc_id]
ihme_loc <- location_hierarchy[, ihme_loc_id]

source(paste0(code_dir, "functions/files.R"))


# Read in raked and aggregated draws --------------------------------------

births <- lapply(draws, function(draw) {
  print(draw)
  loc_draws <- mortcore::load_hdf(paste0(output_dir, "outputs/raked_aggregated_by_draw/", draw, ".h5"), by_val = loc_id)
  return(loc_draws)
})
births <- rbindlist(births)

# check all draws are available
id_vars <- list(location_id = loc_id, year_id = years, sex_id = 1:3,
                age_group_id = age_groups[fertility_single_year | fertility_reporting, age_group_id], draw = draws)
assertable::assert_ids(births, id_vars)


# Separate out and save single year age group births ----------------------

births_single_year_draws <- births[age_group_id %in% age_groups[(fertility_single_year), age_group_id]]

# check outputs
ids <- list(location_id = loc_id, year_id = years, sex_id = 1:3,
            age_group_id = age_groups[(fertility_single_year), age_group_id], draw = draws)
assertable::assert_ids(births_single_year_draws, ids)
assertable::assert_values(births_single_year_draws, colnames = "value", test = "gte", test_val = 0)

# format and save draws
setcolorder(births_single_year_draws, c("location_id", "year_id", "sex_id", "age_group_id", "draw", "value"))
setkeyv(births_single_year_draws, c("location_id", "year_id", "sex_id", "age_group_id", "draw"))
readr::write_csv(births_single_year_draws, path = paste0(output_dir, "outputs/raked_aggregated_by_loc/", loc_id, "_single_year_draws.csv"))

# format and save summary estimates
births_single_year_summary <- mortcore::summarize_data(births_single_year_draws,
                                                       id_vars = c("location_id", "year_id", "sex_id", "age_group_id"),
                                                       outcome_var = "value", metrics = c("mean", "lower", "upper"))
setcolorder(births_single_year_summary, c("location_id", "year_id", "sex_id", "age_group_id", "mean", "lower", "upper"))
setkeyv(births_single_year_summary, c("location_id", "year_id", "sex_id", "age_group_id"))
readr::write_csv(births_single_year_summary, path = paste0(output_dir, "outputs/raked_aggregated_by_loc/", loc_id, "_single_year_summary.csv"))

rm(births_single_year_draws, births_single_year_summary)


# Separate out and save reporting age group births ------------------------

births_reporting_draws <- births[age_group_id %in% age_groups[(fertility_reporting), age_group_id]]

# check outputs
ids <- list(location_id = loc_id, year_id = years, sex_id = 1:3,
            age_group_id = age_groups[(fertility_reporting), age_group_id], draw = draws)
assertable::assert_ids(births_reporting_draws, ids)
assertable::assert_values(births_reporting_draws, colnames = "value", test = "gte", test_val = 0)

# format and save draws
setcolorder(births_reporting_draws, c("location_id", "year_id", "sex_id", "age_group_id", "draw", "value"))
setkeyv(births_reporting_draws, c("location_id", "year_id", "sex_id", "age_group_id", "draw"))
readr::write_csv(births_reporting_draws, path = paste0(output_dir, "outputs/raked_aggregated_by_loc/", loc_id, "_reporting_draws.csv"))

# format and save summary estimates
births_reporting_summary <- mortcore::summarize_data(births_reporting_draws,
                                                     id_vars = c("location_id", "year_id", "sex_id", "age_group_id"),
                                                     outcome_var = "value", metrics = c("mean", "lower", "upper"))
setcolorder(births_reporting_summary, c("location_id", "year_id", "sex_id", "age_group_id", "mean", "lower", "upper"))
setkeyv(births_reporting_summary, c("location_id", "year_id", "sex_id", "age_group_id"))
readr::write_csv(births_reporting_summary, path = paste0(output_dir, "outputs/raked_aggregated_by_loc/", loc_id, "_reporting_summary.csv"))
