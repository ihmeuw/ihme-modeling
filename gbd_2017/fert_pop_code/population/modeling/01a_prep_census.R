library(data.table)
library(stringr)
library(ggplot2)
library(readr)
library(GBDpop, lib.loc = "FILEPATH")

rm(list=ls())

# Get settings ------------------------------------------------------------

main_dir <- commandArgs(trailingOnly=T)[1]
ihme_loc <- commandArgs(trailingOnly=T)[2]

source("settings.R")
get_settings(main_dir)

set.seed(version)

source("modeling/helper_functions.R")

location_hierarchy <- fread(paste0(temp_dir, "/../../../database/location_hierarchy.csv"))
loc_id <- location_hierarchy[ihme_loc_id == ihme_loc, location_id]
super_region <- location_hierarchy[ihme_loc_id == ihme_loc, super_region_name]

census_specific_settings <- fread(paste0(temp_dir, "/../../../database/census_specific_settings.csv"), na.strings = c("", "NA"))[ihme_loc_id == ihme_loc]

gbd_pop <- fread(paste0(temp_dir, "/../../../database/gbd_pop_", age_int, "_2017_baseline.csv"))
gbd_pop <- gbd_pop[ihme_loc_id == ihme_loc]
if (nrow(gbd_pop) == 0 | ihme_loc == "KWT") {
  gbd_pop <- fread(paste0(temp_dir, "/../../../database/gbd_pop_", age_int, "_2017_last_pop_run.csv"))
  gbd_pop <- gbd_pop[ihme_loc_id == ihme_loc]
}

super_regions_to_drop_under5_counts <- c("South Asia", "Sub-Saharan Africa",
                                         "North Africa and Middle East",
                                         "Latin America and Caribbean",
                                         "Southeast Asia, East Asia, and Oceania")

# define scalars to multiply sd by dependent on age start of census data point
sd_age_scalar <- CJ(age_middle_floor = 0:terminal_age, sd_age_scalar = 1)
sd_age_scalar[age_middle_floor <= 5, sd_age_scalar := seq(inflate_young_old_sd_scalar, 1, length.out = 6)]
sd_age_scalar[age_middle_floor >= 50, sd_age_scalar := seq(1, inflate_young_old_sd_scalar, length.out = 46)]

# Set up id vars for assertion checks later -------------------------------

baseline_1_id_vars <-  list(ihme_loc_id = ihme_loc,
                            year_id = min(years),
                            sex_id = 1:2,
                            age = seq(0, terminal_age, 1))
baseline_5_id_vars <-  list(ihme_loc_id = ihme_loc,
                            year_id = min(years),
                            sex_id = 1:2,
                            age = seq(0, terminal_age, 5))

# later censuses we won't neccesarily know the age groups or years
census_id_vars <- list(ihme_loc_id = ihme_loc,
                       sex_id = 1:2)

# Read in processed census data --------------------------------------------

census_data_keys <- c("ihme_loc_id", "year_id", "nid", "underlying_nid", "source", "status",
                      "record_type", "split", "aggregate", "smoother", "data_step")
census_data <- fread(paste0(temp_dir, "/../../../database/census_data.csv"))
census_data <- census_data[ihme_loc_id == ihme_loc & between(year_id, min(years), max(years))]
setnames(census_data, c("age_start", "pop"), c("age", "value_mean"))

# Calculate values of interest --------------------------------------------

prep_for_age_interval <- function(data, use_age_int)  {
  baseline_data <- data[year_id == min(years)]
  non_baseline_data <- data[year_id > min(years)]

  # use gbd baseline populations
  if (!use_backprojected_baseline) {
    gbd_baseline_data <- gbd_pop[year_id == min(years)]
    gbd_baseline_data[, source := "GBD"]
    gbd_baseline_data[, status := "best"]

    baseline_data[status == "best", status := "duplicate"]

    baseline_data <- rbind(baseline_data, gbd_baseline_data, fill = T, use.names = T)
  }

  setnames(non_baseline_data, c("age", "value_mean"), c("age_start", "pop"))
  setnames(baseline_data, c("age", "value_mean"), c("age_start", "pop"))
  # aggregate censuses to age interval if at a more granular level
  ages_needed <- seq(0, terminal_age, use_age_int)
  baseline_data <- baseline_data[, GBDpop::collapse_age_groups(.SD, ages_needed, id_vars = c("sex_id")),
                                 by = census_data_keys]
  non_baseline_data <- non_baseline_data[, GBDpop::collapse_age_groups(.SD, ages_needed, id_vars = c("sex_id")),
                                         by = census_data_keys]

  data <- rbind(baseline_data, non_baseline_data, use.names = T)
  data <- GBDpop::calculate_full_age_groups(data, id_vars = c(census_data_keys, "sex_id"))

  setnames(data, c("age_start", "pop"), c("age", "value_mean"))
  return(data)
}

mark_drops <- function(data, id_vars) {

  census_years <- unique(data$year_id)

  new_data <- lapply(census_years, function(year) {
    agg_data <- data[year_id == year]

    agg_data <- agg_data[, list(value_mean = sum(value_mean)), by = c(id_vars, "age", "age_end", "n")]
    agg_data[, drop := F]

    # drop data if not in baseline year
    if (year != min(years)) {
      # drop age groups above the modeling pooling age
      agg_data[age > collapse_age, drop := T]

      # automatically drop under 5 population in these super regions to deal with
      # age misreporting and undercounts
      if (super_region %in% super_regions_to_drop_under5_counts) {
        agg_data[, age_middle_floor := floor(age + (n / 2))]
        agg_data[n == 0 & is.na(age_end), age_middle_floor := (terminal_age - age) / 2]
        agg_data[age_middle_floor <= 5, drop := T]
        agg_data[, age_middle_floor := NULL]
      }

      # drop certain age groups for modeling purposes
      if (nrow(census_specific_settings[year_id == year & !is.na(drop_age_groups)]) > 0) {
        ages_to_drop <- eval(parse(text=census_specific_settings[year_id == year, drop_age_groups]))
        agg_data[age %in% ages_to_drop, drop := T]
      }
    }
    return(agg_data)
  })
  new_data <- rbindlist(new_data)

  setcolorder(new_data, c(id_vars, "drop", "age", "age_end", "n", "value_mean"))
  setkeyv(new_data, c(id_vars, "drop", "age", "age_end", "n"))
  return(new_data)
}

format_row_matrix <- function(data, use_age_int, use_terminal_age, id_vars) {
  # determine columns of full TMB projection matrix to pull from
  data[, year_column := year_id - min(years)]

  # determine rows of full TMB projection matrix that should be included in the aggregate census age groups
  data[, age_start_row := age / use_age_int] # inclusive
  data[, age_end_row_exclusive := age_start_row + (n / use_age_int)] # exclusive
  data[n == 0 & is.na(age_end), age_end_row_exclusive := (use_terminal_age / use_age_int) + 1] # terminal age group

  setkeyv(data, c("ihme_loc_id", "year_id", "sex_id", "age"))
  matrices <- lapply(1:2, function(s) {
    sex_specific_data <- data[sex_id == s, list(year_column, age_start_row, age_end_row_exclusive)]
    sex_specific_data <- as.matrix(sex_specific_data)
    return(sex_specific_data)
  })
  names(matrices) <- c("male", "female")

  return(matrices)
}

format_value_matrix <- function(data, use_age_int, use_terminal_age, id_vars, baseline = F) {

  # determine how many single or five year age groups are included in each row
  data[, age_groups := n / use_age_int]
  data[n == 0 & is.na(age_end), age_groups := ((use_terminal_age - age) / use_age_int) + 1]   # terminal age group need to add additional age group

  # use the location or region specific standard deviation
  data[, sd := census_default_sd]

  # use location year specific standard deviation if available
  if (nrow(census_specific_settings[!is.na(census_specific_sd_scalar)]) > 0) {
    census_specific_sd_scalars <- census_specific_settings[!is.na(census_specific_sd_scalar), list(ihme_loc_id, year_id, census_specific_sd_scalar)]
    data <- merge(data, census_specific_sd_scalars, all.x = T, by = c("ihme_loc_id", "year_id"))
    data[!is.na(census_specific_sd_scalar), sd := sd * census_specific_sd_scalar]
    data[, census_specific_sd_scalar := NULL]
  }

  # calculate scalar based on the beginning age of each data point
  data[, age_middle_floor := floor(age + (n / 2))]
  data[n == 0 & is.na(age_end), age_middle_floor := (use_terminal_age - age) / 2]
  data <- merge(data, sd_age_scalar, by = "age_middle_floor", all.x = T)

  # calculate scalar for the number of age groups included in a data point so that larger age groups get more weight
  data[, sd_scalar_age_groups := (1 / age_groups)]

  # get ready to apply all the sd scalars
  data[, sd_scaled := sd]

  # inflate the baseline standard deviation
  data[, inflate_baseline_sd_scalar := inflate_baseline_sd_scalar]
  data[year_id == min(years), sd_scaled := sd_scaled * inflate_baseline_sd_scalar]

  # apply scalar
  data[, sd_scaled := sd_scaled * sd_scalar_age_groups]
  data[, sd_scaled := sd_scaled * sd_age_scalar]

  setkeyv(data, c("ihme_loc_id", "year_id", "sex_id", "age"))
  matrices <- lapply(1:2, function(s) {
    sex_specific_data <- data[sex_id == s, list(value_mean, sd_scaled, year_id, age, sd, sd_scalar_age_groups, sd_age_scalar)]
    sex_specific_data <- as.matrix(sex_specific_data)
    return(sex_specific_data)
  })
  names(matrices) <- c("male", "female")
  return(matrices)
}

# pred census data for desired age interval and terminal age group
census_1_data <- prep_for_age_interval(census_data, use_age_int = 1)
census_5_data <- prep_for_age_interval(census_data, use_age_int = 5)

# mark certain age groups to be dropped from the model
census_1_data <- mark_drops(census_1_data, id_vars = c(census_data_keys, "sex_id"))
census_5_data <- mark_drops(census_5_data, id_vars = c(census_data_keys, "sex_id"))

if (knockouts) {
  # determine which years we have census data in
  non_baseline_censuses_currently <- census_1_data[status == "best" & year_id != min(years), unique(year_id)]
  number_non_baseline_censuses_currently <- length(non_baseline_censuses_currently)

  # sample how many censuses to keep in the knockout process (minimum is 1, maximum is either 5 or the number of non-baseline censuses we have)
  number_censuses_to_keep <- sample(1:min(5, number_non_baseline_censuses_currently), 1)
  census_years_to_keep <- c(min(years), sample(non_baseline_censuses_currently, number_censuses_to_keep))

  # mark censuses years we aren't keeping as knockouts
  census_1_data[!year_id %in% census_years_to_keep & status == "best", status := "knockout"]
  census_5_data[!year_id %in% census_years_to_keep & status == "best", status := "knockout"]
}


# make matrices needed for TMB
census_1_matrix <- format_value_matrix(census_1_data[status == "best" & year_id > min(year_id) & !drop],
                                       use_age_int = 1, use_terminal_age = terminal_age,
                                       id_vars = c(census_data_keys, "sex_id"))
census_ages_1_matrix <- format_row_matrix(census_1_data[status == "best" & year_id > min(year_id) & !drop],
                                          use_age_int = 1, use_terminal_age = terminal_age,
                                          id_vars = c(census_data_keys, "sex_id"))
baseline_1_matrix <- format_value_matrix(census_1_data[year_id == min(year_id) & status == "best"],
                                         use_age_int = 1, use_terminal_age = terminal_age,
                                         id_vars = c(census_data_keys, "sex_id"), baseline = T)

census_5_matrix <- format_value_matrix(census_5_data[status == "best" & year_id > min(year_id) & !drop],
                                       use_age_int = 5, use_terminal_age = terminal_age,
                                       id_vars = c(census_data_keys, "sex_id"))
census_ages_5_matrix <- format_row_matrix(census_5_data[status == "best" & year_id > min(year_id) & !drop],
                                          use_age_int = 5, use_terminal_age = terminal_age,
                                          id_vars = c(census_data_keys, "sex_id"))
baseline_5_matrix <- format_value_matrix(census_5_data[year_id == min(year_id) & status == "best"],
                                         use_age_int = 5, use_terminal_age = terminal_age,
                                         id_vars = c(census_data_keys, "sex_id"), baseline = T)


# Assertion checks --------------------------------------------------------

# check baseline populations
assertable::assert_ids(census_1_data[year_id == min(year_id) & status == "best"], id_vars = baseline_1_id_vars)
assertable::assert_ids(census_5_data[year_id == min(year_id) & status == "best"], id_vars = baseline_5_id_vars)

# check non-baseline populations
assertable::assert_ids(census_1_data[year_id != min(year_id) & status == "best"], id_vars = census_id_vars, assert_dups = F)
assertable::assert_ids(census_5_data[year_id != min(year_id) & status == "best"], id_vars = census_id_vars, assert_dups = F)

# check all populations
assertable::assert_values(census_1_data[status == "best"], colnames="value_mean", test="gte", test_val=0)
assertable::assert_values(census_5_data[status == "best"], colnames="value_mean", test="gte", test_val=0)


# Save prepped data -------------------------------------------------------

write_csv(census_1_data, path = paste0(temp_dir, "/inputs/census_1.csv"))
write_csv(census_5_data, path = paste0(temp_dir, "/inputs/census_5.csv"))
save(baseline_1_matrix, census_1_matrix, census_ages_1_matrix,
     baseline_5_matrix, census_5_matrix, census_ages_5_matrix,
     file=paste0(temp_dir, "/inputs/census_matrix.rdata"))
