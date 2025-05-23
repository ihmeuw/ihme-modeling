################################################################################
# Description: Pick the best model version for each location
# - calculate the mean average percent error (mape) for each census point
#   included in the model fitting process.
# - calculate a summary value for the older age net migration values by
#   weighting older age values more.
# - select the best version by keeping versions that are within some tolerance
#   from the best mape and migration index value. Then select the most granular
#   age group used.
################################################################################

library(data.table)
library(readr)
library(assertable)

rm(list = ls())
USER <- Sys.getenv("USER")
code_dir <- paste0("FILEPATH")
Sys.umask(mode = "0002")


# Get arguments -----------------------------------------------------------

# load step specific settings into Global Environment
parser <- argparse::ArgumentParser()
parser$add_argument("--pop_vid", type = "character",
                    help = "The version number for this run of population, used to read in settings file")
parser$add_argument("--test", type = "character",
                    help = "Whether this is a test run of the process")
args <- parser$parse_args()
if (interactive()) { # set interactive defaults, should never commit changes away from the test version to be safe
  args$pop_vid <- "99999"
  args$test <- "T"
}
args$test <- as.logical(args$test)
list2env(args, .GlobalEnv); rm(args)

# load model run specific settings into Global Environment
settings_dir <- paste0("FILEPATH", ifelse(test, "test/", ""), pop_vid, "/run_settings.rdata")
load(settings_dir)
list2env(settings, envir = environment())

# read in population modeling location hierarchy
location_hierarchy <- fread(paste0(output_dir, "/database/all_location_hierarchies.csv"))
location_hierarchy <- location_hierarchy[location_set_name == "population"]

# read in location specific settings
location_specific_settings <- fread(paste0(output_dir, "/database/location_specific_settings.csv"))

# get weights for each version of pooling, weight oldest age groups most
old_people_migration_age_groups <- seq(55, terminal_age, 5)
old_people_age_group_weights <- 1:length(old_people_migration_age_groups)

mape_tolerance <- 5
old_mig_index_tolerance <- 0.005


# Read in data to compare model versions ----------------------------------

compare_versions <- lapply(location_hierarchy[is_estimate == 1, location_id], function(loc_id) {
  ihme_loc <- location_hierarchy[location_id == loc_id, ihme_loc_id]
  print(ihme_loc)

  # determine the drop ages that were used
  drop_ages <- location_specific_settings[ihme_loc_id == ihme_loc, pooling_ages]
  drop_ages <- eval(parse(text=drop_ages))

  # get the mape and old_ages_migration_index
  loc_data <- lapply(drop_ages, function(age) {

    error_file_dir <- paste0(output_dir, loc_id, "/outputs/model_fit/errors_drop", age, ".csv")
    migration_file_dir <- paste0(output_dir, loc_id, "/outputs/model_fit/migration_proportion_drop", age, ".csv")

    if (all(file.exists(error_file_dir, migration_file_dir))) {

      error <- fread(error_file_dir)


      migration <- fread(migration_file_dir)
      ages <- c(-age_int, seq(-0, terminal_age, 5))
      migration[
        ,
        age_group_years_start := as.integer(as.character(cut(
          age_group_years_start, breaks = c(ages, Inf), labels = ages, right = F
        ))),
        by = c("location_id", "year_id", "sex_id")
      ]
      migration <- migration[age_group_years_start >= min(old_people_migration_age_groups), list(mig_value = mean(abs(mean))), by = "age_group_years_start"]

      data <- data.table(ihme_loc_id = ihme_loc, drop_age = age,
                         mape = error[outlier_type == "not outliered" & !drop, mean(abs(pct_error))],
                         old_mig_index = weighted.mean(migration$mig_value, old_people_age_group_weights))
    } else {
      data <- data.table(ihme_loc_id = ihme_loc, drop_age = age,
                         mape = NA, old_mig_index = NA)
    }
    return(data)
  })
  loc_data <- rbindlist(loc_data)
  return(loc_data)
})
compare_versions <- rbindlist(compare_versions)

compare_versions[, still_running := F]

compare_versions[, submitted := ihme_loc_id %in% location_hierarchy[is_estimate == 1, ihme_loc_id]]
compare_versions[, completed := !(is.na(mape) & !is.na(old_mig_index))]
compare_versions[submitted & completed, status := 1]
compare_versions[submitted & still_running, status := 2]
compare_versions[submitted & !completed, status := 3]
compare_versions[submitted == F, status := 4]
compare_versions[, status := factor(status, levels = 1:4, labels = c("Submitted and\ncompleted", "Submitted and\nstill running", "Submitted and\nnot completed", "Not submitted"))]


# Select best versions ----------------------------------------------------

best_versions <- copy(compare_versions)

# Find version within some tolerance of mean absolute percent error from census data
best_versions[, min_mape := min(mape, na.rm = T), by = .(ihme_loc_id)]
best_versions[, abs_diff_mape := abs(mape - min_mape) ]
best_versions[, mape_inclusion := ifelse(abs_diff_mape < mape_tolerance, T, F)]
best_versions <- best_versions[mape_inclusion == T]

# Find version with best migration value and keep versions within relative tol
best_versions[, min_old_mig_index := min(old_mig_index, na.rm = T), by = .(ihme_loc_id)]
best_versions[, abs_diff_old_mig_index := abs(old_mig_index - min_old_mig_index) ]
best_versions[, old_mig_index_inclusion := ifelse(abs_diff_old_mig_index < old_mig_index_tolerance, T, F)]
best_versions <- best_versions[old_mig_index_inclusion == T]

# Pick remaining version with highest drop age
best_versions[, highest_drop_age := max(drop_age), by = ihme_loc_id]
best_versions <- best_versions[drop_age == highest_drop_age]
best_versions <- best_versions[, list(ihme_loc_id, drop_age)]

# Add NAs for locations that were supposed to run but did not complete
best_versions <- rbind(best_versions,
                       data.table(ihme_loc_id = location_hierarchy[is_estimate == 1 & !ihme_loc_id %in% best_versions$ihme_loc_id, ihme_loc_id]),
                       use.names = T, fill = T)


# Force a different best drop age -----------------------------------------

# High population in 95+ category broke scaling of extended age groups in ELT.
# changing all USA locations to use oldest drop age
usa_locs <- location_hierarchy[ihme_loc_id %like% "USA", ihme_loc_id]

# Mainland CHN provinces have raised mortality from previous estimates
# this is due to instability in older ages that we will fix with a higher
# drop age
chn_locs <- location_hierarchy[parent_id == 44533, ihme_loc_id]

# Unusually high mortality in ARE and QAT in older ages means we assume
# the population denominator is too low.
other_locs <- c("QAT", "ARE")

force_oldest_drop_age <- c(usa_locs, chn_locs, other_locs)
force_oldest_drop_age_versions <- compare_versions[
  ihme_loc_id %in% force_oldest_drop_age &
    completed &
    (!is.na(mape) & !is.na(old_mig_index))
]
force_oldest_drop_age_versions <- force_oldest_drop_age_versions[, list(force_drop_age = max(drop_age)), by = "ihme_loc_id"]
best_versions <- merge(best_versions, force_oldest_drop_age_versions, all.x = T, by = "ihme_loc_id")
best_versions[!is.na(force_drop_age), drop_age := force_drop_age]
best_versions[, force_drop_age := NULL]

if (!isFALSE(copy_previous_drop_age)) {

  stopifnot(length(copy_previous_drop_age) == 1)
  path_best_version <- fs::path(
    "FILEPATH",
    copy_previous_drop_age,
    "versions_best.csv"
  )
  stopifnot(fs::file_exists(path_best_version))

  best_versions_old <- fread(path_best_version)
  best_versions_old[compare_versions, completed := i.completed, on = .(ihme_loc_id, drop_age)]

  stopifnot("Not every best version exists" = best_versions_old[!(completed), .N] > 0)

  best_versions <- best_versions_old[, -"completed"]

}


# Check and save ----------------------------------------------------------

# Make sure every modeled location is present
assertable::assert_ids(best_versions, id_vars = list(ihme_loc_id = location_hierarchy[is_estimate == 1, ihme_loc_id]))
assertable::assert_values(best_versions, colnames = "drop_age", test = "not_na")

readr::write_csv(compare_versions, paste0(output_dir, "/versions_compare.csv"))
readr::write_csv(best_versions, paste0(output_dir, "/versions_best.csv"))
system(paste0("chmod 777 ", paste0(output_dir, "/versions_best.csv")))
