################################################################################
# Description: Rake and aggregate to reporting locations, both sexes combined
# and reporting age groups.
# - run separately by draw
# - rake population but do not scale level 3 CHN and GBR to level 4 subnatonals
#   (do level 5 to level 4 and so on though)
# - aggregate to reporting locations (sdi, who, etc.) and both sexes combined
# - aggregate from single year age groups to standard gbd age groups and
#   reporting age groups
################################################################################

library(data.table)
library(readr)
library(assertable)
library(parallel)
library(mortdb, lib = "FILEPATH/r-pkg")
library(mortcore, lib = "FILEPATH/r-pkg")

rm(list = ls())
SGE_TASK_ID <- Sys.getenv("SGE_TASK_ID")
MKL_NUM_THREADS <- Sys.getenv("MKL_NUM_THREADS")
USER <- Sys.getenv("USER")
code_dir <- paste0("FILEPATH", "/population/modeling/popReconstruct/")
Sys.umask(mode = "0002")


# Get arguments -----------------------------------------------------------

# load step specific settings into Global Environment
parser <- argparse::ArgumentParser()
parser$add_argument("--pop_vid", type = "character",
                    help = "The version number for this run of population, used to read in settings file")
parser$add_argument('--task_map_dir', type="character",
                    help="The filepath to the task map file that specifies other arguments to run for")
parser$add_argument("--test", type = "character",
                    help = "Whether this is a test run of the process")
args <- parser$parse_args()
if (interactive()) { # set interactive defaults, should never commit changes away from the test version to be safe
  args$pop_vid <- "99999"
  args$task_map_dir <- "FILEPATH"
  args$test <- "T"
}
args$test <- as.logical(args$test)
list2env(args, .GlobalEnv); rm(args)

# use task id to get arguments from task map
task_map <- fread(task_map_dir)
draw <- task_map[task_id == SGE_TASK_ID, draw]

# load model run specific settings into Global Environment
settings_dir <- paste0("FILEPATH", pop_vid, "/run_settings.rdata")
load(settings_dir)
list2env(settings, envir = environment())

source(paste0(code_dir, "functions/files.R"))

location_hierarchy <- fread(paste0(output_dir, "/database/all_location_hierarchies.csv"))
age_groups <- fread(paste0(output_dir, "/database/age_groups.csv"))


# Rake population ---------------------------------------------------------

pop_draws <- parallel::mclapply(location_hierarchy[location_set_name == "population" & is_estimate == 1, location_id], function(loc) {
  print(loc)
  dir <- paste0(output_dir, loc, "/outputs/draws/population_unraked_ui.h5")
  draws <- load_hdf_draws(dir, by_vals = draw)
  return(draws)
}, mc.cores = MKL_NUM_THREADS)
pop_draws <- rbindlist(pop_draws)

population_id_vars <- list(location_id = location_hierarchy[location_set_name == "population" & is_estimate == 1, location_id], year_id = years, sex_id = 1:2, age_group_id = age_groups[(most_detailed), age_group_id], draw = draw)
assertable::assert_ids(pop_draws, population_id_vars)

# scale results but do not scale level 3 CHN and GBR to level 4 subnatonals (do level 5 to level 4 and so on though)
pop_draws <- mortcore::scale_results(pop_draws, id_vars = c("location_id", "year_id", "sex_id", "age_group_id", "draw"),
                                     value_var = "value", location_set_id = ifelse(gbd_year == 2017, 21, 93), gbd_year = gbd_year,
                                     exclude_parent = c("CHN", "GBR"))

# aggregate results to all reporting locations and to both sexes combined
pop_draws <- mortcore::agg_results(pop_draws, id_vars = c("location_id", "year_id", "sex_id", "age_group_id", "draw"),
                                   value_vars = "value", location_set_id = ifelse(gbd_year == 2017, 21, 93), gbd_year = gbd_year,
                                   agg_sex = T, agg_hierarchy = T, loc_scalars = T,
                                   agg_reporting = nrow(location_hierarchy[location_set_name != "population"]) > 0)
# aggregate results to all reporting age groups for all reporting locations and sexes
pop_reporting_draws <- mortcore::agg_results(pop_draws, id_vars = c("location_id", "year_id", "sex_id", "age_group_id", "draw"),
                                             value_vars = "value", gbd_year = gbd_year,
                                             agg_hierarchy = F, agg_sex = F, age_aggs = age_groups[(reporting), age_group_id])
pop_reporting_draws <- pop_reporting_draws[age_group_id %in% age_groups[(reporting), age_group_id]]


# Save results ------------------------------------------------------------

# format
setcolorder(pop_draws, c("location_id", "year_id", "sex_id", "age_group_id", "draw", "value"))
setkeyv(pop_draws, c("location_id", "year_id", "sex_id", "age_group_id", "draw"))
setcolorder(pop_reporting_draws, c("location_id", "year_id", "sex_id", "age_group_id", "draw", "value"))
setkeyv(pop_reporting_draws, c("location_id", "year_id", "sex_id", "age_group_id", "draw"))

# make sure only positive values
assertable::assert_values(pop_draws, colnames = "value", test = "gt", test_val = 0)
assertable::assert_values(pop_reporting_draws, colnames = "value", test = "gt", test_val = 0)

# check the most detailed age group draws
population_id_vars <- list(location_id = location_hierarchy[, unique(location_id)], year_id = years, sex_id = 1:3,
                           age_group_id = age_groups[(most_detailed), age_group_id], draw = draw)
assertable::assert_ids(pop_draws, population_id_vars)

# check the reporting age group draws
population_reporting_id_vars <- list(location_id = location_hierarchy[, unique(location_id)], year_id = years, sex_id = 1:3,
                                     age_group_id = age_groups[(reporting), age_group_id], draw = draw)
assertable::assert_ids(pop_reporting_draws, population_reporting_id_vars)

# save draw files indexed by location id
save_hdf_draws(pop_draws, fpath_draws = paste0(output_dir, "/raking_draws/population_", draw, ".h5"), by_var = "location_id")
save_hdf_draws(pop_reporting_draws, fpath_draws = paste0(output_dir, "/raking_draws/population_reporting_", draw, ".h5"), by_var = "location_id")
readr::write_csv(data.table(test = 1), path = paste0(output_dir, "/raking_draws/confirm_raking_aggregation_completion_", draw, ".csv"))
