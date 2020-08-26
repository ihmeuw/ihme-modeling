################################################################################
# Description: Distribute population counts of people of unknown sex and/or age.
# - distribute population counts of people of unknown sex by age (assumes same
#   sex distribution as known population)
# - distribute population counts of people of unknown age by sex (assumes same
#   age distribution as known population)
# - data_stage: 'processed, distributed unknown'
################################################################################

library(data.table)
library(readr)
library(assertable)
library(mortdb, lib.loc = "FILEPATH/r-pkg")
library(mortcore, lib.loc = "FILEPATH/r-pkg")

rm(list = ls())
USER <- Sys.getenv("USER")
code_dir <- paste0("FILEPATH", "/population/data_processing/census_processing/")
Sys.umask(mode = "0002")


# Get arguments -----------------------------------------------------------

# load step specific settings into Global Environment
parser <- argparse::ArgumentParser()
parser$add_argument("--pop_processing_vid", type = "character",
                    help = "The version number for this run of population data processing, used to read in settings file")
parser$add_argument("--test", type = "character",
                    help = "Whether this is a test run of the process")
args <- parser$parse_args()
if (interactive()) { # set interactive defaults, should never commit changes away from the test version to be safe
  args$pop_processing_vid <- "99999"
  args$test <- "T"
}
args$test <- as.logical(args$test)
list2env(args, .GlobalEnv); rm(args)

# load model run specific settings into Global Environment
settings_dir <- paste0("FILEPATH", pop_processing_vid, "/run_settings.rdata")
load(settings_dir)
list2env(settings, envir = environment())

unknown_age_group_id <- 283
unknown_sex_id <- 4

location_hierarchy <- fread(paste0(output_dir, "/inputs/location_hierarchy.csv"))
gbd_pop <- fread(paste0(output_dir, "/inputs/gbd_pop_current_round_best.csv"))
gbd_pop[, c("run_id", "lower", "upper") := NULL]


# Define functions for distributing unknowns ------------------------------

#' Distributes counts of unknown sex proportionally by age group.
#' If no counts of known sex available, distribute 50/50.
#'
#' @param censuses data.table for censuses that includes columns for 'sex_id', 'age_group_id', 'mean'.
#' @param id_vars vector of id variables for censuses table ('ihme_loc_id', 'year_id', etc.)
#' @param unknown_sex_id_value value for unknown sex_id that should be used
#' @return data.table for censuses that includes columns for 'id_vars', 'sex_id', 'age_group_id', 'mean'.
#'
#' @export
#' @import data.table
distribute_unknown_sex <- function(censuses, id_vars, unknown_sex_id_value) {

  # initial checks
  if(!any(class(censuses) == "data.table")) stop("data.table required")
  if(!all(c(id_vars, "sex_id", "age_group_id", "mean") %in% names(censuses))) stop("'id_vars', 'sex_id', 'age_group_id', 'mean' columns not in data.table")
  original_total_pop <- sum(as.numeric(censuses$mean))

  # split out the tabulations of unknown sex
  unknown_sex_id <- censuses[sex_id == unknown_sex_id_value & mean > 0]
  unknown_sex_id[, sex_id := NULL]
  censuses <- censuses[sex_id != unknown_sex_id_value]

  # calculate sex proportions by age group
  proportions <- copy(censuses)
  proportions[, total_pop := sum(mean), by = c(id_vars, "age_group_id")]
  proportions[, prop := mean / total_pop]
  proportions[, c("mean", "total_pop") := NULL]

  # merge proportions onto the data set of tabulations of unknown sex
  unknown_sex_id <- merge(unknown_sex_id, proportions, by = c(id_vars, "age_group_id"), all.x = T, allow.cartesian = T)

  # some ages don't have any one of known sex, so use previous gbd population estimates to split
  still_unknown <- unknown_sex_id[is.na(prop)]
  still_unknown_location_years <- unique(still_unknown[, list(location_id, year_id)])
  still_unknown <- lapply(1:nrow(still_unknown_location_years), function(i) {
    loc_id <- still_unknown_location_years[i, location_id]
    year <- still_unknown_location_years[i, year_id]

    # do we use same location estimates? or one level above?
    pop <- gbd_pop[location_id == loc_id & year_id == year]

    # collapse gbd population to census age groups
    data <- still_unknown[location_id == loc_id & year_id == year]
    data_ages <- unique(data$age_group_id)
    pop <- mortcore::agg_results(pop, id_vars = c("location_id", "year_id", "sex_id", "age_group_id"), value_vars = "mean",
                                 age_aggs = data_ages, agg_hierarchy = F)
    pop <- pop[age_group_id %in% data_ages]

    # calculate sex proportion
    pop[, combined_sex_total := sum(mean), by = c("location_id", "year_id", "age_group_id")]
    pop[, prop := mean / combined_sex_total]
    pop[, c("mean", "combined_sex_total") := NULL]

    # use proportions to split data
    data[, c("sex_id", "prop") := NULL]
    data <- merge(data, pop, by = c("location_id", "year_id", "age_group_id"))

    return(data)
  })
  still_unknown <- rbindlist(still_unknown)
  unknown_sex_id <- unknown_sex_id[!is.na(prop)]
  unknown_sex_id <- rbind(unknown_sex_id, still_unknown, use.names = T)

  # split population proportionally
  unknown_sex_id[, mean := mean * prop]
  unknown_sex_id[, prop := NULL]

  # combine data back together
  censuses <- rbind(censuses, unknown_sex_id, use.names=T)
  censuses <- censuses[, list(mean = sum(as.numeric(mean))), by = c(id_vars, "sex_id", "age_group_id")]

  # final checks
  if ((original_total_pop - sum(censuses$mean)) > 0.01) stop("population doesn't match before and after distribution")
  assertable::assert_values(censuses, colnames="sex_id", test="in", test_val = c(1, 2), quiet = T)

  return(censuses)
}


#' Distributes counts of unknown age proportionally.
#'
#' @param censuses data.table for censuses that includes columns for 'age_group_id', 'mean'.
#' @param id_vars vector of id variables for censuses table ('ihme_loc_id', 'year_id', etc.)
#' @param unknown_age_group_id_value value for unknown age_group_id that should be used
#' @return data.table for censuses that includes columns for 'id_vars', 'age_group_id', 'mean'.
#'
#' @export
#' @import data.table
distribute_unknown_age <- function(censuses, id_vars, unknown_age_group_id_value) {

  # initial checks
  if(!any(class(censuses) == "data.table")) stop("data.table required")
  if(!all(c(id_vars, "age_group_id", "mean") %in% names(censuses))) stop("'id_vars', 'age_group_id', 'mean' columns not in data.table")

  original_total_pop <- sum(censuses$mean)

  # split out the tabulations of unknown age
  if(nrow(censuses[age_group_id == unknown_age_group_id_value]) > 0) {
    unknown_age_pop <- censuses[age_group_id == unknown_age_group_id_value]
    unknown_age_pop[, age_group_id := NULL]
    censuses <- censuses[age_group_id != unknown_age_group_id_value]

    # For each set of censuses tabulations, calculate the proportion in each age group
    proportions <- copy(censuses)
    proportions[, total_pop := sum(mean), by = c(id_vars)]
    proportions[, prop := mean / total_pop]
    proportions[, c("mean", "total_pop") := NULL]

    # merge proportions onto the data set of tabulations of unknown age
    unknown_age_pop <- merge(unknown_age_pop, proportions, by = c(id_vars), all.x = T)

    # split population proportionally
    unknown_age_pop[, mean := mean * prop]
    unknown_age_pop[, prop := NULL]

    # combine data back together
    censuses <- rbind(censuses, unknown_age_pop, use.names=T)
  }

  censuses <- censuses[, list(mean = sum(mean)), by = c(id_vars, "age_group_id")]

  # final checks
  if ((original_total_pop - sum(censuses$mean)) > 0.01) stop("population doesn't match before and after distribution")
  assertable::assert_values(censuses, colnames="age_group_id", test="not_equal", test_val = unknown_age_group_id_value, quiet = T)

  return(censuses)
}


# Distribute unknown sex and age proportionally ---------------------------

census_data <- fread(paste0(output_dir, "/outputs/01_raw.csv"))
census_data[mean == 0, mean := 0.01] # need to offset zeroes in the data, especially older age groups

# for now assign sex_id 3 to unknown so that we split using previous gbd population estimates
census_data[sex_id == 3, sex_id := unknown_sex_id]

census_data_sex <- distribute_unknown_sex(census_data, id_vars = census_id_vars, unknown_sex_id_value = unknown_sex_id)
census_data_age <- distribute_unknown_age(census_data_sex, id_vars = c(census_id_vars, "sex_id"), unknown_age_group_id_value = unknown_age_group_id)
census_data_age[, data_stage := "distributed unknown"]
setcolorder(census_data_age, c(census_id_vars, "sex_id", "age_group_id", "mean"))
setkeyv(census_data_age, c(census_id_vars, "sex_id", "age_group_id"))
readr::write_csv(census_data_age, path = paste0(output_dir, "/outputs/02_distribute_unknown_sex_age.csv"))
