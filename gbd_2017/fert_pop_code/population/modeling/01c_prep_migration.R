library(data.table)
library(stringr)
library(ggplot2)
library(readr)

rm(list=ls())


# Get settings ------------------------------------------------------------

main_dir <- commandArgs(trailingOnly=T)[1]
ihme_loc <- commandArgs(trailingOnly=T)[2]

source("settings.R")
get_settings(main_dir)

source("modeling/helper_functions.R")

location_hierarchy <- fread(paste0(temp_dir, "/../database/location_hierarchy.csv"))
loc_id <- location_hierarchy[ihme_loc_id == ihme_loc, location_id]

gbd_pop_1_2017_last_pop_run <- fread(paste0(temp_dir, "/../database/gbd_pop_1_2017_last_pop_run.csv"))
setnames(gbd_pop_1_2017_last_pop_run, "value_mean", "pop")

# Set up id vars for assertion checks later -------------------------------

migration_1_id_vars <- list(ihme_loc_id = ihme_loc,
                            year_id = seq(min(years), max(years), 1),
                            sex_id = 1:2,
                            age = seq(-1, terminal_age, 1))
migration_5_id_vars <- list(ihme_loc_id = ihme_loc,
                            year_id = seq(min(years), max(years), 5),
                            sex_id = 1:2,
                            age = seq(-5, terminal_age, 5))

# Prep net refugee flow ---------------------------------------------------

migration_data <- fread(input_migration_data_dir)
migration_data <- migration_data[!is.na(n_migrants)]
setnames(migration_data, "age_start", "age")
migration_1_data <- migration_data[status == "best" & ihme_loc_id == ihme_loc & between(year_id, min(years), max(years))]
data_keys <- c("ihme_loc_id", "year_id", "sex_id", "age", "source", "status", "nid")
migration_data_available <- ihme_loc %in% unique(migration_1_data$ihme_loc_id)

if (migration_data_available & use_migration_data) {
  migration_1_data <- migration_1_data[, list(ihme_loc_id, year_id, sex_id, age, source, status, nid, value_mean=n_migrants)]
  migration_1_data <- migration_1_data[age > terminal_age, age := terminal_age]
  migration_1_data <- migration_1_data[, list(value_mean = sum(value_mean)), by = data_keys]

  # in cases where our migration data ends before the last year we are trying to model, we don't want the
  # prior to abruptly go back to a zero prior.
  if (duplicate_recent_migration_data) {
    migration_1_recent <- migration_1_data[year_id == max(year_id) & status == "best"]
    migration_1_recent <- lapply((migration_1_recent[, unique(year_id)] + 1):max(years), function(y) {
      data <- copy(migration_1_recent)
      data[, year_id := y]
      data[, source := paste0(source, ", duplicated for recent years")]
    })
    migration_1_recent <- rbindlist(migration_1_recent)
    migration_1_data <- rbind(migration_1_data, migration_1_recent)
  }

  age_split_data <- migration_1_data[sex_id == 3 & age == 0]
  migration_1_data <- migration_1_data[!(sex_id == 3 & age == 0)]

  # need to age split data
  if (nrow(age_split_data) > 0) {

    if (ihme_loc %in% c("RWA", "ERI")) { # uniform age pattern
      age_sex_pattern <- CJ(ihme_loc_id = ihme_loc, sex_id = 1:2, age = 0:95, prop = 0.001)

    } else if (ihme_loc %in% c('QAT', 'SAU', 'BHR', 'ARE', 'OMN')) { # QAT migration age pattern, not doing KWT because refugee migration a good part of the time series
      QAT_migration_file <- "FILEPATH"
      age_sex_pattern <- fread(QAT_migration_file)
      age_sex_pattern <- age_sex_pattern[year_id == 2010, list(ihme_loc_id = ihme_loc, sex_id, age, prop = value_mean)]
      age_sex_pattern <- age_sex_pattern[age != -1]
      age_sex_pattern[age < 5, prop := 0] ## DESTROYING THE BABY TRAIN

    } else { # EUROSTAT age pattern
      age_sex_pattern <- migration_data[source %like% 'Eurostat' & !((age == 0 & is.na(age_end)) | sex_id == 3)] # subset to Eurostat that is not just totals
      age_sex_pattern <- age_sex_pattern[n == 1]
      age_sex_pattern <- merge(age_sex_pattern, gbd_pop_1_2017_last_pop_run, by = c("ihme_loc_id", "year_id", "sex_id", "age"))
      age_sex_pattern[, prop := n_migrants / pop]
      age_sex_pattern <- age_sex_pattern[, list(ihme_loc_id = ihme_loc, prop = abs(weighted.mean(prop, pop))), keyby = c("sex_id", "age")]
    }

    age_split_data[, c("sex_id", "age") := NULL]
    age_split_data <- merge(age_split_data, age_sex_pattern, by = "ihme_loc_id", allow.cartesian = T)
    setnames(age_split_data, "value_mean", "original_total")
    age_split_data <- merge(age_split_data, gbd_pop_1_2017_last_pop_run, by = c("ihme_loc_id", "year_id", "sex_id", "age"))
    age_split_data[, original_net_mig_prop := original_total / sum(pop), by = setdiff(data_keys, c("sex_id", "age"))]
    age_split_data[original_total < 0, prop := prop * -1]
    age_split_data[, predicted_migrants := pop * prop]
    age_split_data[, predicted_total := sum(predicted_migrants), by = setdiff(data_keys, c("sex_id", "age"))]
    age_split_data[, predicted_net_mig_prop := predicted_total / sum(pop), by = setdiff(data_keys, c("sex_id", "age"))]
    age_split_data[, scalar := original_net_mig_prop / predicted_net_mig_prop]
    age_split_data[, adjusted_prop := prop * scalar]
    age_split_data[, predicted_migrants_adjusted := pop * adjusted_prop]
    age_split_data[, predicted_total_adjusted := sum(predicted_migrants_adjusted), by = setdiff(data_keys, c("sex_id", "age"))]

    age_split_data <- age_split_data[, c(data_keys, "adjusted_prop"), with = F]
    setnames(age_split_data, "adjusted_prop", "value_mean")

  }

  # transform age specific migration counts into proportions
  if (nrow(migration_1_data) > 0) {
    migration_1_data <- merge(migration_1_data, gbd_pop_1_2017_last_pop_run, by = c("ihme_loc_id", "year_id", "sex_id", "age"), all.x = T)
    migration_1_data <- migration_1_data[, list(value_mean = value_mean / pop), by = data_keys]
  }

  migration_1_data <- rbind(migration_1_data, age_split_data, use.names = T, fill = T)

  # add on years that we didn't neccesarily have values for and assign value to be zero
  all_years <- CJ(ihme_loc_id = ihme_loc, year_id = years, sex_id = 1:2, age = 0:terminal_age, zero = 0)
  migration_1_data <- merge(migration_1_data, all_years, by = c("ihme_loc_id", "year_id", "sex_id", "age"), all = T)
  migration_1_data[is.na(value_mean), c("source", "status", "nid", "value_mean") := list("zero_prior", "best", NA, 0)]
  migration_1_data[, zero := NULL]

  # aggregate data to five year and five year age group intervals
  years_needed <- seq(min(years), max(years), 5)
  ages_needed <- seq(0, terminal_age, 5)
  migration_5_data <- migration_1_data[, list(ihme_loc_id, year_id = cut(year_id, breaks=c(years_needed, Inf), labels=years_needed, right=F),
                                              sex_id, age = cut(age, breaks=c(ages_needed, Inf), labels=ages_needed, right=F),
                                              source, status, nid,
                                              value_mean)]
  migration_5_data[, year_id := as.integer(as.character(year_id))]
  migration_5_data[, age := as.integer(as.character(age))]
  migration_5_data <- migration_5_data[, list(value_mean = mean(value_mean)), by=data_keys]
  migration_5_data <- migration_5_data[, if(.N > 1) .SD[source != "zero_prior"] else .SD, by = c("ihme_loc_id", "year_id", "sex_id", "age")]
  migration_5_data <- migration_5_data[, if(.N > 1) .SD[.N] else .SD, by = c("ihme_loc_id", "year_id", "sex_id", "age")]

  # add in rows for children born in the projection interval
  migration_1_data_babies <- migration_1_data[age == 0]
  migration_5_data_babies <- migration_5_data[age %in% 0:4]
  migration_1_data_babies <- migration_1_data_babies[, age := -1]
  migration_5_data_babies <- migration_5_data_babies[, age := -5]
  migration_1_data <- rbind(migration_1_data_babies, migration_1_data)
  migration_5_data <- rbind(migration_5_data_babies, migration_5_data)

} else {
  migration_1_data <- CJ(ihme_loc_id=ihme_loc, year_id=migration_1_id_vars$year_id,
                         sex_id=1:2, age=migration_1_id_vars$age,
                         source = "zero_prior", status = "best", nid = NA, value_mean=0)
  migration_5_data <- CJ(ihme_loc_id=ihme_loc, year_id=migration_5_id_vars$year_id,
                         sex_id=1:2, age=migration_5_id_vars$age,
                         source = "zero_prior", status = "best", nid = NA, value_mean=0)
}

# zero out migration prior above age 70
migration_1_data[age >= 70, value_mean := 0]
migration_5_data[age >= 70, value_mean := 0]


# Calculate values of interest --------------------------------------------

# 1x1 migration
setkeyv(migration_1_data, c("ihme_loc_id", "year_id", "sex_id", "age"))
migration_1_matrix <- format_as_matrix_by_sex(migration_1_data[status == "best"], "value_mean",
                                              ages = migration_1_id_vars$age)

# 5x5 migration
setkeyv(migration_5_data, c("ihme_loc_id", "year_id", "sex_id", "age"))
migration_5_matrix <- format_as_matrix_by_sex(migration_5_data[status == "best"],
                                              "value_mean", ages = migration_5_id_vars$age)


# Assertion checks --------------------------------------------------------

assertable::assert_ids(migration_1_data[status == "best"], id_vars = migration_1_id_vars)
assertable::assert_values(migration_1_data[status == "best"], colnames="value_mean", test="not_na")

assertable::assert_ids(migration_5_data[status == "best"], id_vars = migration_5_id_vars)
assertable::assert_values(migration_5_data[status == "best"], colnames="value_mean", test="not_na")


# Save prepped data -------------------------------------------------------

write_csv(migration_1_data, path = paste0(temp_dir, "/inputs/migration_1.csv"))
write_csv(migration_5_data, path = paste0(temp_dir, "/inputs/migration_5.csv"))

save(migration_1_matrix, migration_5_matrix,
     file=paste0(temp_dir, "/inputs/migration_matrix.rdata"))
