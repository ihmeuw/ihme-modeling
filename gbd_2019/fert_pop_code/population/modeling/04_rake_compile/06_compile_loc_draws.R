################################################################################
# Description:
# - compile together the population draws from the draw specific raked files
# - save location specific files
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
loc_id <- task_map[task_id == SGE_TASK_ID, location_id]

# load model run specific settings into Global Environment
settings_dir <- paste0("FILEPATH", pop_vid, "/run_settings.rdata")
load(settings_dir)
list2env(settings, envir = environment())

source(paste0(code_dir, "functions/files.R"))

age_groups <- fread(paste0(output_dir, "/database/age_groups.csv"))

draws_produced <- 0:(draws - 1)


# Read in raked draws -----------------------------------------------------

pop_draws <- parallel::mclapply(draws_produced, function(draw) {
  print(draw)
  dir <- paste0(output_dir, "/raking_draws/population_", draw, ".h5")
  draws <- load_hdf_draws(dir, by_vals = loc_id)
  return(draws)
}, mc.cores = MKL_NUM_THREADS)
pop_draws <- rbindlist(pop_draws)

pop_reporting_draws <- parallel::mclapply(draws_produced, function(draw) {
  print(draw)
  dir <- paste0(output_dir, "/raking_draws/population_reporting_", draw, ".h5")
  draws <- load_hdf_draws(dir, by_vals = loc_id)
  return(draws)
}, mc.cores = MKL_NUM_THREADS)
pop_reporting_draws <- rbindlist(pop_reporting_draws)


# Save results ------------------------------------------------------------

# format
setcolorder(pop_draws, c("location_id", "year_id", "sex_id", "age_group_id", "draw", "value"))
setkeyv(pop_draws, c("location_id", "year_id", "sex_id", "age_group_id", "draw"))
setcolorder(pop_reporting_draws, c("location_id", "year_id", "sex_id", "age_group_id", "draw", "value"))
setkeyv(pop_reporting_draws, c("location_id", "year_id", "sex_id", "age_group_id", "draw"))

# make sure only positive values
assertable::assert_values(pop_draws, colnames = "value", test = "gt", test_val = 0)
assertable::assert_values(pop_reporting_draws, colnames = "value", test = "gt", test_val = 0)

# check the most detailed group draws
population_id_vars <- list(location_id = loc_id, year_id = years, sex_id = 1:3,
                           age_group_id = age_groups[(most_detailed), age_group_id], draw = draws_produced)
assertable::assert_ids(pop_draws, population_id_vars)

# check the reporting age group draws
population_reporting_id_vars <- list(location_id = loc_id, year_id = years, sex_id = 1:3,
                                     age_group_id = age_groups[(reporting), age_group_id], draw = draws_produced)
assertable::assert_ids(pop_reporting_draws, population_reporting_id_vars)

# save summary and draw files indexed by draw number
save_outputs(pop_draws,
             fpath_summary = paste0(output_dir, loc_id, "/outputs/summary/population_raked_ui.csv"),
             fpath_draws = paste0(output_dir, loc_id, "/outputs/draws/population_raked_ui.h5"),
             id_vars = c("location_id", "year_id", "sex_id", "age_group_id"), by_var = "draw", value_var = "value")
save_outputs(pop_reporting_draws,
             fpath_summary = paste0(output_dir, loc_id, "/outputs/summary/population_reporting_raked_ui.csv"),
             fpath_draws = paste0(output_dir, loc_id, "/outputs/draws/population_reporting_raked_ui.h5"),
             id_vars = c("location_id", "year_id", "sex_id", "age_group_id"), by_var = "draw", value_var = "value")
readr::write_csv(data.table(test = 1), path = paste0(output_dir, loc_id, "/outputs/confirm_compile_loc_completion.csv"))
