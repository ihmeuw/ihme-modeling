################################################################################
## Description: Rake birth estimates up the location hierarchy and then
##              aggregate birth estimates by location/sex/age.
################################################################################

library(data.table)
library(rhdf5)
library(assertable)
library(mortcore)
library(mortdb)

rm(list = ls())
USER <- Sys.getenv("USER")
task <- Sys.getenv("SLURM_ARRAY_TASK_ID")
Sys.umask(mode = "0002")


# Get arguments -----------------------------------------------------------

# load step specific settings into Global Environment
parser <- argparse::ArgumentParser()
parser$add_argument("--live_births_vid", type = "character",
                    help = 'The version number for this run of live births, used to read in settings file')
parser$add_argument('--task_map_dir', type="character",
                    help='The filepath to the task map file that specifies the location id to run for')
parser$add_argument("--test", type = "character",
                    help = 'Whether this is a test run of the process')
parser$add_argument("--code_dir", type = "character", 
                    help = 'Location of code')
args <- parser$parse_args()
if (interactive()) { # set interactive defaults, should never commit changes away from the test version to be safe
  args$live_births_vid <- "99999"
  args$task_map_dir <- "FILEPATH"
  args$test <- "T"
  args$code_dir <- "CODE_DIR_HERE"
}
args$test <- as.logical(args$test)
list2env(args, .GlobalEnv); rm(args)

# use task id to get arguments from task map
task_map <- fread(task_map_dir)
draw <- task_map[task_id == task, draw]

# load model run specific settings into Global Environment
settings_dir <- paste0("FILEPATH", ifelse(test, "test/", ""), live_births_vid, "/run_settings.csv")
load(settings_dir)
list2env(settings, envir = environment())

age_groups <- fread(paste0(output_dir, "/inputs/age_groups.csv"))
location_hierarchy <- fread(paste0(output_dir, "/inputs/all_reporting_hierarchies.csv"))

source(paste0(code_dir, "functions/files.R"))


# Read in one draw for all estimated locations ----------------------------

births <- lapply(location_hierarchy[location_set_name == "live_births" & is_estimate == 1, location_id], function(loc_id) {
  print(loc_id)
  location_draws <- mortcore::load_hdf(filepath = paste0(output_dir, "outputs/unraked_by_loc/", loc_id, ".h5"), by_val = draw)
  return(location_draws)
})
births <- rbindlist(births)

# check all locations are available
id_vars <- list(location_id = location_hierarchy[location_set_name == "live_births" & is_estimate == 1, location_id], year_id = years,
                sex_id = 1:2, age_group_id = age_groups[(fertility_single_year), age_group_id], draw = draw)
assertable::assert_ids(births, id_vars)


# Scale results -----------------------------------------------------------

# scale results but do not scale level 3 CHN to level 4 subnatonals (do level 5 to level 4 though)
births <- mortcore::scale_results(births, id_vars = c("location_id", "year_id", "sex_id", "age_group_id", "draw"),
                                  value_var = "value", location_set_id = 21, gbd_year = gbd_year,
                                  exclude_parent = c("CHN"))


# Aggregate results -------------------------------------------------------

# aggregate results to all reporting locations and to both sexes combined
births <- mortcore::agg_results(births, id_vars = c("location_id", "year_id", "sex_id", "age_group_id", "draw"),
                                value_vars = "value", location_set_id = 21, gbd_year = gbd_year,
                                agg_hierarchy = T, loc_scalars = T,
                                agg_reporting = nrow(location_hierarchy[location_set_name != "live_births"]) > 0,
                                agg_sex = T, age_aggs = age_groups[(fertility_reporting) & age_group_id != 22, age_group_id])


# Save outputs ------------------------------------------------------------

# check outputs
ids <- list(location_id = location_hierarchy[, location_id], year_id = years, sex_id = 1:3,
            age_group_id = age_groups[fertility_single_year | fertility_reporting, age_group_id], draw = draw)
assertable::assert_ids(births, ids)
assertable::assert_values(births, colnames = "value", test = "gte", test_val = 0)

# format and save outputs
setcolorder(births, c("location_id", "year_id", "sex_id", "age_group_id", "draw", "value"))
setkeyv(births, c("location_id", "year_id", "sex_id", "age_group_id", "draw"))
save_hdf_draws(births, fpath_draws = paste0(output_dir, "outputs/raked_aggregated_by_draw/", draw, ".h5"), by_var = "location_id")
