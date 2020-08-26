################################################################################
# Description: Create 1950 population data with single year age groups up to 95+
# as the prior to the population model.
# - split the oldest census into single year age groups up to 95+ using nLx
#   proportions from the corresponding year full lifetable
# - do backwards ccmpp to get initial population estimates in 1950
# - when doing backwards ccmpp the upper left triangle of the Lexis diagram is
#   missing because we can't split the terminal age group and can't fill in
#   earlier years terminal age group. Fill these missing age groups by using the
#   nLx age pattern from the 1950 full lifetable (scale the nLx age pattern above
#   the oldest overlapping age group to match the census population in the oldest
#   non-missing age group)
# - for specified locations use annualized rate of change of total population
#   between the two oldest censuses to estimate 1950 total population. Scale the
#   backwards ccmpp populations to this arc total population.
# - data_stage: 'processed, baseline included'
################################################################################

library(data.table)
library(readr)
library(parallel)
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

source(paste0(code_dir, "helper_functions.R"))

age_groups <- fread(paste0(output_dir, "/inputs/age_groups.csv"))
location_hierarchy <- fread(paste0(output_dir, "/inputs/location_hierarchy.csv"))
full_lifetables <- fread(paste0(output_dir, "/inputs/full_lt.csv"))
location_specific_settings <- fread(paste0(output_dir, "/inputs/location_specific_settings.csv"))

census_id_vars <- c(census_id_vars, "split", "aggregate", "smoother")

# can't use this method for Eritrea since we only have total population in one year
no_baseline_locs <- location_hierarchy[ihme_loc_id %in% no_baseline_locs, location_id]


# Create single year age groups for the oldest census available -----------

census_data <- fread(paste0(output_dir, "/outputs/08_apply_pes_correction.csv"))
census_data_baseline <- census_data[outlier_type == "not outliered" & year_id >= year_start & location_id %in% location_hierarchy[is_estimate == 1, location_id] & !location_id %in% no_baseline_locs]
# can't use this method for Eritrea since we only have total population in one year
census_data_baseline <- census_data_baseline[!location_id %in% no_baseline_locs]

# check that we don't have locations where we only have total population censuses
census_data_baseline_total <- census_data_baseline[, list(total_pop_only = all(age_group_id == 22)), by = c("location_id", "sex_id")]
if (nrow(census_data_baseline_total[(total_pop_only)]) > 0) {
  print(census_data_baseline_total[(total_pop_only)])
  stop("No non-total pop censuses exist for the following locations and cannot packproject to 1950 currently")
}

# keep the oldest age-specific census counts we have for each census
census_data_baseline <- census_data_baseline[age_group_id != 22, .SD[year_id == min(year_id)], by = c("location_id", "sex_id")]

# determine which censuses need to be split to single year age groups up to 95+
census_data_baseline <- merge(census_data_baseline, age_groups[, list(age_group_id, age_group_years_start)], by = "age_group_id", all.x = T)
census_data_baseline[, full_age_groups := all(diff(age_group_years_start) == 1) & max(age_group_years_start) == terminal_age,
                     by =c("location_id", "sex_id")]
census_data_baseline_split <- census_data_baseline[!(full_age_groups)]
census_data_baseline <- census_data_baseline[(full_age_groups)]

# aggregate the full lifetables into the census age groups
census_data_baseline_split_age_groups <- census_data_baseline_split[, list(age_groups = list(sort(unique(age_group_years_start)))),
                                                                    by = c("location_id", "year_id", "sex_id")]
full_lifetables_split <- merge(census_data_baseline_split_age_groups, full_lifetables,
                               by = c("location_id", "year_id", "sex_id"), all.x = T)
full_lifetables_split[, age_group_years_start := as.integer(as.character(cut(age, breaks = c(unique(unlist(age_groups)), Inf), labels = unique(unlist(age_groups)), right = F))),
                      by = c("location_id", "year_id", "sex_id")]
full_lifetables_split[, age_group_years_start := as.integer(as.character(age_group_years_start))]

# calculate nLx proportions in each aggregate age group and use to split
full_lifetables_split[, prop := nLx / sum(nLx), by = c("location_id", "year_id", "sex_id", "age_group_years_start")]
census_data_baseline_split <- merge(census_data_baseline_split, full_lifetables_split,
                                    by = c("location_id", "year_id", "sex_id", "age_group_years_start"), all = T, allow.cartesian = T)
census_data_baseline_split <- census_data_baseline_split[, list(age_group_years_start = age, mean = mean * prop, max_age_int = max(diff(age_group_years_start))),
                                                         by = c(census_id_vars, "sex_id")]

# collapse to original census terminal age group or terminal_age
census_data_baseline_split[age_group_years_start > terminal_age, age_group_years_start := terminal_age]
census_data_baseline_split <- census_data_baseline_split[, list(mean = sum(mean), max_age_int = unique(max_age_int)),
                                                         by = c(census_id_vars, "sex_id", "age_group_years_start")]

# interpolate to smooth non-terminal age groups
census_data_baseline_split[, bw := 5]
census_data_baseline_split[max_age_int <= 5, bw := 2]
census_data_baseline_split[, new_mean := c(exp(KernSmooth::locpoly(x = age_group_years_start[-.N], y = log(mean[-.N]),
                                                                   degree = 1, bandwidth = unique(bw), gridsize = (.N - 1),
                                                                   range.x = c(age_group_years_start[1], age_group_years_start[.N - 1]))$y), mean[.N]),
                           by = c(census_id_vars, "sex_id")]
# rescale to original total population
census_data_baseline_split[, original_total_pop := sum(mean), by = c(census_id_vars, "sex_id")]
census_data_baseline_split <- census_data_baseline_split[, list(age_group_years_start, mean = new_mean * (original_total_pop / sum(new_mean))),
                                                         by = c(census_id_vars, "sex_id")]

# add back on after splitting to single year age groups
census_data_baseline <- census_data_baseline[, c(census_id_vars, "sex_id", "age_group_years_start", "mean"), with = F]
census_data_baseline <- rbind(census_data_baseline, census_data_baseline_split, use.names = T)


# Calculate nLx survival ratios -------------------------------------------

# calculate the nLx survival ratio
age_int <- 1
survival <- full_lifetables[age <= (terminal_age + age_int),
                            list(age = seq(-age_int, terminal_age, age_int),
                                 value = c((nLx[1] / (age_int * lx[1])),
                                           (shift(nLx, type="lead") / nLx)[-c(.N - 1, .N)],
                                           (Tx[.N] / Tx[.N - 1]))),
                            by = c("location_id", "year_id", "sex_id")]

# average the nLx survival ratio between two year periods since cohorts are exposed to survival ratios over half of each year
survival[, next_year_value := shift(value, type = "lead"), by = c("location_id", "sex_id", "age")]
survival <- survival[!is.na(next_year_value)]
survival <- survival[, list(value = ((value + next_year_value) / 2)), by = c("location_id", "year_id", "sex_id", "age")]


# Do backwards CCMPP ------------------------------------------------------

#' Projects population backwards in time using a modified Leslie matrix. Cannot
#' predict population for the oldest age groups because it is impossible to know how
#' many people to resurect or when to split them out of the terminal age group. This
#' currently assumes zero migration when projecting backwards.
#'
#' @param baseline data.table of the initial population in the most recent year, should have at least columns for 'age', 'year_id', 'value'.
#' @param surv data.table of the nLx survival ratios over the projection period of interest, should have at least columns for 'age', 'year_id', 'value'.
#' @param years vector of years to do the projection over.
#' @param value_name name of the value column in the input data.tables.
#' @return data.table with the new baseline population estimates,
#' NAs filled in for the missing triangle.
#'
#' @export
#' @import data.table
backwards_ccmpp <- function(baseline, surv,
                            years, value_name = "value") {

  ## setting the number of age groups and the number of projection/backcasting steps
  ages <- unique(baseline$age)
  terminal_age_group <- max(ages)
  n_age_grps <- length(ages)
  proj_steps <- length(years) - 1

  # convert data tables to matrices with year wide, age as rownames
  baseline_matrix <- matrix_w_rownames(dcast(baseline, age ~ year_id, value.var = value_name))
  surv_matrix <- matrix_w_rownames(dcast(surv, age ~ year_id, value.var = value_name))

  # initialize population matrix
  pop_mat <- matrix(0, nrow = n_age_grps, ncol = 1 + proj_steps)
  pop_mat[, proj_steps + 1] <- baseline_matrix

  ## project population backward one projection period
  for (i in proj_steps:1) {
    leslie <- make_backwards_leslie_matrix(surv_matrix[, i])
    pop_mat[, i] <- leslie %*% pop_mat[, i + 1]
  }

  # Assign NA the missing upper triangle (we don't know how many dead people to resurrect)
  for (i in proj_steps:1) {
    pop_mat[n_age_grps:(n_age_grps - (proj_steps - i + 1)), i] <- NA
  }

  # convert to data table
  melt_matrix <- function(mat, ages, years) {
    colnames(mat) <- years
    data <- data.table(mat)
    data[, age := ages]
    data <- melt(data, id.vars=c("age"), variable.name = "year_id", value.name = "value")
    data[, year_id := as.integer(as.character(year_id))]
    setcolorder(data, c("year_id", "age", value_name))
    return(data)
  }
  population <- melt_matrix(pop_mat, ages, years)
  return(population)
}

#' Fills in the Leslie matrix for projecting populations back in time
#'
#' @param nLx_survival vector of nLx survival ratio values.
#' @return Leslie matrix with survival values filled in just above the diagonal instead of below
#' like in forwards ccmpp.
#'
#' @export
#' @import data.table
make_backwards_leslie_matrix <- function(nLx_survival) {
  age_groups <- length(nLx_survival) - 1

  # Initialize Leslie matrix
  leslie <- matrix(0, nrow = age_groups, ncol = age_groups)

  # Take recipricol of survival values in order to go backwards
  nLx_survival <- 1 / nLx_survival

  # fill survival
  leslie[1:(age_groups - 1), 2:age_groups] <- diag(nLx_survival[2:age_groups])

  # Terminal age group and age group just below need to be missing
  leslie[age_groups - 1, age_groups] <- 0
  leslie[age_groups, age_groups] <- 0

  # add dimension names
  dimnames(leslie) <- list(names(nLx_survival)[-1], names(nLx_survival)[-1])
  return(leslie)
}

#' Used internally to convert a data.table to a matrix with the rownames
#' equal to the first column which is usually 'age'
#'
#' @param data data.table with 'age' in the first column and 'years' in the remaining columns.
#' @return matrix with yearly values and rownames equal to the age groups.
#'
#' @import data.table
matrix_w_rownames <- function(data) {
  # Convert a data.table to matrix with the first column as the rownames
  #
  # Args:
  #   data: data.table with age in the first column and years after that
  # Returns:
  #   matrix with age column as row names

  m <- as.matrix(data[, names(data)[-1], with=F])
  rownames(m) <- data[[1]]
  return(m)
}

# determine which censuses need to be backprojected to 1950
census_data_backwards_ccmpp <- census_data_baseline[year_id != year_start]
census_data_baseline <- census_data_baseline[year_id == year_start]

census_data_backwards_ccmpp <- census_data_backwards_ccmpp[, list(location_id, year_id, sex_id, age = age_group_years_start, value = mean)]
census_data_backwards_ccmpp <- mclapply(unique(census_data_backwards_ccmpp$location_id), function(loc_id) {
  print(loc_id)
  census <- lapply(1:2, function(sex) {
    print(sex)
    # project population backwards to the baseline year
    oldest_census <- census_data_backwards_ccmpp[location_id == loc_id & sex_id == sex]
    oldest_census_year <- unique(oldest_census$year_id)
    surv <- survival[location_id == loc_id & sex_id == sex & year_id <= oldest_census_year]
    baseline_census <- backwards_ccmpp(baseline = oldest_census, surv = surv, years = year_start:oldest_census_year)

    # force more older ages to be missing
    fill_missing_triangle_above_age <- location_specific_settings[location_id == loc_id, fill_missing_triangle_above]
    baseline_census[age > (fill_missing_triangle_above_age + (year_id - year_start)), value := NA]

    # fill in columns needed
    baseline_census[, location_id := loc_id]
    baseline_census[, sex_id := sex]
    baseline_census[, nid := 375236]
    baseline_census[, underlying_nid := NA_integer_]
    baseline_census[, record_type := "backwards ccmpp"]
    baseline_census[, method_short := "Unknown"]
    baseline_census[, pes_adjustment_type := "not applicable"]
    baseline_census[, outlier_type := "not outliered"]
    baseline_census[, source_name := "backwards ccmpp"]
    baseline_census[, data_stage := NA]
    baseline_census[, split := F]
    baseline_census[, aggregate := F]
    baseline_census[, smoother := "none"]

    setnames(baseline_census, c("age", "value"), c("age_group_years_start", "mean"))
    setcolorder(baseline_census, c(census_id_vars, "sex_id", "age_group_years_start", "mean"))
    return(baseline_census)
  })
  census <- rbindlist(census)
  return(census)
}, mc.cores = 5)
census_data_backwards_ccmpp <- rbindlist(census_data_backwards_ccmpp)


# Fill the missing upper triangle -----------------------------------------

# collapse the nLx age pattern to the correct terminal age group
missing_triangle_lifetables <- full_lifetables[location_id %in% unique(census_data_backwards_ccmpp[, location_id]),
                                               list(location_id, year_id, sex_id, age_group_years_start = age, nLx)]
missing_triangle_lifetables[age_group_years_start > terminal_age, age_group_years_start := terminal_age]
missing_triangle_lifetables <- missing_triangle_lifetables[, list(nLx = sum(nLx)), by = c("location_id", "year_id", "sex_id", "age_group_years_start")]

# merge on the nLx value from the corresponding full lifetable in the same location-year
census_data_backwards_ccmpp <- merge(census_data_backwards_ccmpp, missing_triangle_lifetables,
                                     by = c("location_id", "year_id", "sex_id", "age_group_years_start"), all.x = T)

# determine the relationship between the oldest age group we have a population count for
# and the nLx value for the same age group. Use this scalar to scale up the nLx values for
# ages above this oldest age group.
census_data_backwards_ccmpp[, oldest_age_not_missing := max(age_group_years_start[!is.na(mean)]), by = c(census_id_vars, "sex_id")]
census_data_backwards_ccmpp[, scalar := mean[age_group_years_start == oldest_age_not_missing] / nLx[age_group_years_start == oldest_age_not_missing],
                            by = c(census_id_vars, "sex_id")]
census_data_backwards_ccmpp[, missing_triangle := is.na(mean)]
census_data_backwards_ccmpp[missing_triangle == T, mean := nLx * scalar]
census_data_backwards_ccmpp <- census_data_backwards_ccmpp[, c(census_id_vars, "sex_id", "age_group_years_start", "missing_triangle", "mean"), with = F]


# Do annualized rate of change interpolation of total pop for 1950 --------

scale_backprojected_baselines <- census_data[outlier_type == "not outliered" & year_id >= year_start]
scale_backprojected_baselines <- scale_backprojected_baselines[location_id %in% unique(census_data_backwards_ccmpp[, location_id])]
scale_backprojected_baselines <- scale_backprojected_baselines[location_id %in% location_specific_settings[(scale_backprojected_baseline), location_id]]

# calculate 1950 population using arc backprojected total population
scale_backprojected_baselines <- scale_backprojected_baselines[, list(mean = sum(mean)), by = c("location_id", "year_id", "sex_id")] # sum to total population
setkeyv(scale_backprojected_baselines, c("location_id", "year_id", "sex_id"))
scale_backprojected_baselines <- scale_backprojected_baselines[, .SD[1:2], by = c("location_id", "sex_id")] # keep the oldest two censuses
scale_backprojected_baselines <- scale_backprojected_baselines[, list(years_to_baseline = year_id[1] - year_start,
                                                                      arc = (log(mean[2] / mean[1]) / diff(year_id)),
                                                                      oldest_total_pop = mean[1]),
                                                               by = c("location_id", "sex_id")]
scale_backprojected_baselines <- scale_backprojected_baselines[, list(year_id = year_start,
                                                                      scale_backprojected_baseline = oldest_total_pop * exp(-years_to_baseline * arc)),
                                                               by = c("location_id", "sex_id")]

census_data_backwards_ccmpp <- census_data_backwards_ccmpp[, backwards_ccmp_total := sum(mean), by = c(census_id_vars, "sex_id")]
census_data_backwards_ccmpp <- merge(census_data_backwards_ccmpp, scale_backprojected_baselines, by = c("location_id", "sex_id", "year_id"), all.x = T)
census_data_backwards_ccmpp[is.na(scale_backprojected_baseline), scale_backprojected_baseline := backwards_ccmp_total]
census_data_backwards_ccmpp[, scalar := scale_backprojected_baseline / backwards_ccmp_total]
census_data_backwards_ccmpp[, mean := mean * scalar]
census_data_backwards_ccmpp <- census_data_backwards_ccmpp[, c(census_id_vars, "sex_id", "age_group_years_start", "missing_triangle", "mean"), with = F]


# Combine everything together ---------------------------------------------

# combine together all the backwards ccmpp results and the actual censuses in 1950
census_data_baseline[, missing_triangle := F]
baseline_censuses <- rbind(census_data_backwards_ccmpp, census_data_baseline, use.names = T)
baseline_censuses[, data_stage := "processed, baseline included"]

# check that all modeled locations have a 1950 baseline with single year age groups up to 95+
assertable::assert_ids(baseline_censuses[year_id == year_start],
                       id_vars = list(location_id = location_hierarchy[is_estimate == 1 & !location_id %in% no_baseline_locs, location_id],
                                      year_id = year_start,
                                      sex_id = 1:2,
                                      age_group_years_start = seq(0, terminal_age, age_int)))

# add on age_group_id to combine with actual census data
baseline_censuses <- mortcore::age_start_to_age_group_id(baseline_censuses, id_vars = c(census_id_vars, "sex_id"), keep_age_group_id_only = T)

# save backwards ccmpp results
setcolorder(baseline_censuses, c(census_id_vars, "missing_triangle", "sex_id", "age_group_id", "mean"))
setkeyv(baseline_censuses, c(census_id_vars, "missing_triangle", "sex_id", "age_group_id"))
readr::write_csv(baseline_censuses, path = paste0(output_dir, "/diagnostics/backwards_ccmpp_results.csv"))

# add on just the 1950 populations to the original census data
baseline_censuses <- baseline_censuses[year_id == year_start]
baseline_censuses[, missing_triangle := NULL]
census_data <- census_data[year_id != year_start] # these have been split and included in "baseline_censuses"
census_data <- rbind(census_data, baseline_censuses, use.names = T)
census_data[, data_stage := "baseline included"]

setcolorder(census_data, c(census_id_vars, "sex_id", "age_group_id", "mean"))
setkeyv(census_data, c(census_id_vars, "sex_id", "age_group_id", "mean"))
readr::write_csv(census_data, path = paste0(output_dir, "/outputs/09_generate_baseline.csv"))
