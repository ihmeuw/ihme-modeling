####################################################################################################
##
## Description: Run All script for code producing full (single-year age group) lifetables.
####################################################################################################

############
# Settings
############

rm(list=ls())

library(data.table)
library(readr)
library(RMariaDB)
library(assertable)

library(ltcore)
library(mortdb)
library(mortcore)

## Option for project_name
# CLUSTER_PROJECT <- ""
CLUSTER_PROJECT <- ""
USER <- Sys.getenv("USER")
CODE_DIR <- FILEPATH
CENTRAL_COMP_FUNCTION_DIR <- FILEPATH
GBD_ROUND_ID <- 5
GBD_YEAR <- 2017
RESUB <- F
SINGULARITY_IMAGE <- "r_shell_singularity_mort.sh"
ENABLE_ASSERTIONS <- T
# if rerunning and results already exist. If True, deletes the previous output for a fresh start
CLEAR_PREVIOUS_OUTPUT <- T

# source(paste0(CODE_DIR, "/qsub.R"))
source(paste0(CENTRAL_COMP_FUNCTION_DIR, "/get_location_metadata.R"))

##########################
# Functions
##########################
remove_csv_from_dir <- function(directory) {
  system(paste0("perl -e 'unlink <", directory, "/*.csv>'"))
}

compile_csv <- function(directory) {
  if (file.exists(paste0(directory, "compiled.csv"))) {
    setwd(directory)
    unlink("compiled.csv")
    setwd(CODE_DIR)
  }
  files <- list.files(path = directory, pattern = "*.csv", full.names = T)
  dataset <- rbindlist(lapply(files, fread))
  write_csv(dataset, paste0(directory, "/compiled.csv"))
}

##########################
# Versioning
##########################

VERSION_NO_SHOCK_DEATH_NUMBER <- get_proc_version(model_name = "no shock death number", model_type = "estimate", run_id = "recent_completed")

# TODO: switch to different auth method
## cod correct version of shock aggregator outputs
db_connection <- dbConnect(RMySQL::MySQL())
sql_command <- paste0("SELECT shock_version_id FROM cod.shock_version WHERE gbd_round_id = ", GBD_ROUND_ID, " AND shock_version_status_id = 1;")
v_id <- dbGetQuery(db_connection, sql_command)
VERSION_SHOCK_AGGREGATOR <- as.integer(v_id[1, 'shock_version_id'])
print(paste0("using shock_aggregator output version: ", VERSION_SHOCK_AGGREGATOR))
dbDisconnect(db_connection)

no_shock_death_number_lineage <- get_proc_lineage(model_name = "no shock death number", model_type = "estimate", run_id = VERSION_NO_SHOCK_DEATH_NUMBER)
VERSION_POPULATION <- no_shock_death_number_lineage[parent_process_name == "population estimate", parent_run_id]

#######################################
# Create directories and files for run
#######################################

# define directories
OUTPUT_DIR <- paste0(FOLDER, VERSION_NO_SHOCK_DEATH_NUMBER)
INPUT_DIR <- paste0(OUTPUT_DIR, "/inputs")
SHOCK_OUTPUT_DIR <- paste0(OUTPUT_DIR, "/shock_numbers")

FULL_LT_DIR <- paste0(OUTPUT_DIR, "/full_lt")
FULL_LT_NO_HIV_DIR <- paste0(FULL_LT_DIR, "/no_hiv")
FULL_LT_WITH_HIV_DIR <- paste0(FULL_LT_DIR, "/with_hiv")
FULL_LT_SHOCK_DIR <- paste0(FULL_LT_DIR, "/with_shock")

ABRIDGED_LT_DIR <- paste0(OUTPUT_DIR, "/abridged_lt")
ABRIDGED_LT_NO_HIV_DIR <- paste0(ABRIDGED_LT_DIR, "/no_hiv")
ABRIDGED_LT_WITH_HIV_DIR <- paste0(ABRIDGED_LT_DIR, "/with_hiv")
ABRIDGED_LT_SHOCK_DIR <- paste0(ABRIDGED_LT_DIR, "/with_shock")

#######################################
# Cache files for run
#######################################

create_single_abridged_age_map <- function() {
  abridged_ages <- c(0, 1, seq(5, 110, 5))
  ages <- data.table("age" = 0:110)
  ages[, abridged_age := cut(age, breaks = c(abridged_ages, Inf), labels = abridged_ages, right = F)]
  ages[, abridged_age := as.integer(as.character(abridged_age))]
  return(ages)
}

age_groups <- setDT(get_age_map(type = "lifetable"))
age_groups <- age_groups[, list(age_group_id, age = as.integer(age_group_name_short), age_group_years_end, age_group_years_start)]
age_groups[, age_length := age_group_years_end - age_group_years_start]
age_groups[, c("age_group_years_end", "age_group_years_start") := NULL]
write_csv(age_groups, path = paste0(OUTPUT_DIR, "/inputs/age_groups.csv"))

single_abridged_age_map <- create_single_abridged_age_map()
write_csv(single_abridged_age_map, path = paste0(OUTPUT_DIR, "/inputs/single_abridged_age_map.csv"))

location_hierarchy = setDT(get_locations(level = "all"))
write_csv(location_hierarchy, path = paste0(OUTPUT_DIR, "/inputs/location_hierarchy.csv"))

population <- get_mort_outputs("population", "estimate", run_id = VERSION_POPULATION, gbd_year = GBD_YEAR)
population[, c("upload_population_estimate_id", "run_id") := NULL]
setnames(population, "mean", "mean_population")
assert_values(population, 'mean', test = "gt", test_val = 0)
write_csv(population, paste0(OUTPUT_DIR, "/inputs/population.csv"))

##########################
# Submit jobs
##########################

locations <- location_hierarchy[level >= 3, location_id]
full_lt_jobs <- c()
for (loc_id in locations) {
  jobname <- paste0("full_lt_", loc_id)
  full_lt_jobs <- c(full_lt_jobs, jobname)
  qsub(jobname = jobname,
       code = paste0("full_lt.R"),
       pass = list(VERSION_NO_SHOCK_DEATH_NUMBER, loc_id, VERSION_SHOCK_AGGREGATOR, GBD_YEAR, ENABLE_ASSERTIONS),
       slots = 2,
       hostgroup = 'c2',
       proj = CLUSTER_PROJECT,
       shell = SINGULARITY_IMAGE,
       submit = T
       )
}
