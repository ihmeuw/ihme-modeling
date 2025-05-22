################################################################################
## Description: Use srb, population and asfr to calculate live births.
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


# Read in inputs ----------------------------------------------------------

# read in sex ratio at birth and transform to proportion of births of each sex
srb <- fread(paste0(output_dir, "/inputs/srb.csv"))
srb <- srb[location_id == loc_id, list(location_id, year_id, srb = mean)]
srb <- srb[, list(sex_id = 1:2, srb_proportion = c((srb / (srb + 1)), (1 / (srb + 1)))), by = c("location_id", "year_id")]

# read in female population
population <- fread(paste0(output_dir, "/inputs/population.csv"))
population <- population[location_id == loc_id, list(location_id, year_id, age_group_id, population)]

# read in single year age group asfr draws
asfr <- fread(paste0("FILEPATH", asfr_vid,
                     "/loop2/results/population_input/one_by_one/",
                     ihme_loc, "_fert_1by1_draws.csv"))
setnames(asfr, "age", "age_group_years_start")

if ("sim" %in% colnames(asfr)) {
  setnames(asfr, "sim", "draw")
}

asfr <- mortcore::age_start_to_age_group_id(asfr, id_vars = c("ihme_loc_id", "year_id", "draw"), keep_age_group_id_only = T)
asfr <- asfr[age_group_id %in% age_groups[(fertility_single_year), age_group_id], list(location_id = loc_id, year_id, age_group_id, draw, asfr = value)]


# Calculate births --------------------------------------------------------

# merge all together and calculate female-age, child-sex specific live birth draws
births <- merge(population, asfr, by = c("location_id", "year_id", "age_group_id"), all = T)
births <- merge(births, srb , by = c("location_id", "year_id"), all = T, allow.cartesian = T)
births[, value := population * asfr * srb_proportion]
births[, c("population", "asfr", "srb_proportion") := NULL]


# Save outputs ------------------------------------------------------------

# check outputs
ids <- list(location_id = loc_id, year_id = years, sex_id = 1:2,
            age_group_id = age_groups[(fertility_single_year), age_group_id], draw = draws)
assertable::assert_ids(births, ids)
assertable::assert_values(births, colnames = "value", test = "gte", test_val = 0)

# format and save outputs
setcolorder(births, c("location_id", "year_id", "sex_id", "age_group_id", "draw", "value"))
setkeyv(births, c("location_id", "year_id", "sex_id", "age_group_id", "draw"))
save_hdf_draws(births, fpath_draws = paste0(output_dir, "outputs/unraked_by_loc/", loc_id, ".h5"), by_var = "draw")
