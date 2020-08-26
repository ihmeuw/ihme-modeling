################################################################################
# Description: Correct for age heaping in the population data. Can specify via
# location or census specific settings not to over smooth real age-cohort
# patterns in high quality data locations.
# - standardize age groups to 1, 5, 10 year and all ages population data
# - calculate age and sex ratios, and joint scores prior to correcting for
#   age heaping
# - smooth 1 year age group censuses with joint score greater than 20 or
#   pre-specified with Feeney correction
# - smooth 5 year age group censuses with joint score between 20 and 40 or
#   pre-specified with Arriaga smoothing
# - smooth 5 year age group censuses with joint score greater than 40 or
#   pre-specified with Arriaga Strong smoothing
# - smooth 10 year age group censuses with joint score greater than 20 or
#   pre-specified with Arriaga Strong smoothing
# - calculate age and sex ratios, and joint scores after correcting for age
#   heaping
# - data_stage: 'processed, corrected age heaping'
################################################################################

library(data.table)
library(readr)
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
                    help = 'The version number for this run of population data processing, used to read in settings file')
parser$add_argument("--test", type = "character",
                    help = 'Whether this is a test run of the process')
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

location_hierarchy <- fread(paste0(output_dir, "/inputs/location_hierarchy.csv"))
age_groups <- fread(paste0(output_dir, "/inputs/age_groups.csv"))
location_specific_settings <- fread(paste0(output_dir, "/inputs/location_specific_settings.csv"))
census_specific_settings <- fread(paste0(output_dir, "/inputs/census_specific_settings.csv"))

census_id_vars <- c(census_id_vars, "split", "aggregate")


# Standardize to 1, 5 and 10 year age groups ------------------------------

census_data <- fread(paste0(output_dir, "/outputs/04_aggregate_subnationals.csv"))

# determine the maximum age group width in each census
census_data <- merge(census_data, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], by = "age_group_id", all.x = T)
census_data[, n := age_group_years_end - age_group_years_start]
census_data[age_group_years_end == age_group_table_terminal_age, n := 0]
census_data[, max_age_group_width := max(n, na.rm = T), by = census_id_vars]

# separate out into standard age group widths 1, 5, 10
single_year_age_group_censuses <- census_data[max_age_group_width == 1]
five_year_age_group_censuses <- census_data[max_age_group_width == 5]
ten_year_age_group_censuses <- census_data[max_age_group_width == 10]
total_pop_censuses <- census_data[!max_age_group_width %in% c(1, 5, 10)]

# collapse into these standard 1, 5, 10 year age groups
single_year_age_group_censuses[, c("age_group_years_start", "age_group_years_end", "n", "max_age_group_width") := NULL]
five_year_age_group_censuses[, c("n") := NULL]
ten_year_age_group_censuses[, c("n") := NULL]
five_year_age_group_censuses <- agg_age_data(five_year_age_group_censuses, id_vars = census_id_vars, age_grouping_var = "max_age_group_width")
ten_year_age_group_censuses <- agg_age_data(ten_year_age_group_censuses, id_vars = census_id_vars, age_grouping_var = "max_age_group_width")

# if collapsing leads to too few age groups, just collapse to total pop
ten_year_age_group_censuses <- merge(ten_year_age_group_censuses, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], by = "age_group_id", all.x = T)
ten_year_age_group_censuses[, max_age := max(age_group_years_start), by = census_id_vars]
collapse_to_total_actually <- ten_year_age_group_censuses[max_age < 40]
ten_year_age_group_censuses <- ten_year_age_group_censuses[max_age >= 40]
collapse_to_total_actually[, c("max_age", "age_group_years_end") := NULL];
ten_year_age_group_censuses[, c("max_age", "age_group_years_start", "age_group_years_end") := NULL];
total_pop_censuses[, c("n", "max_age_group_width", "age_group_years_end") := NULL]
total_pop_censuses <- rbind(total_pop_censuses, collapse_to_total_actually, use.names = T)

# determine what locations to keep broad age groups in
# if doing this for more location, add to census settings csv
total_pop_censuses[, keep_broad_age_groups := F]
total_pop_censuses[location_id == 69, keep_broad_age_groups := T]
preserve_censuses <- total_pop_censuses[(keep_broad_age_groups)]
preserve_censuses[, c("keep_broad_age_groups", "age_group_years_start") := NULL]

# collapse to total population
total_pop_censuses <- total_pop_censuses[!(keep_broad_age_groups)]
total_pop_censuses[, keep_broad_age_groups := NULL]
total_pop_censuses[, max_age_group_width := 0]
total_pop_censuses <- agg_age_data(total_pop_censuses, id_vars = census_id_vars, age_grouping_var = "max_age_group_width")
total_pop_censuses <- rbind(total_pop_censuses, preserve_censuses, use.names = T)


# Calculate age-sex ratios and accuracy index prior to smoothing ----------

#' Calculates age and sex ratios as described in "Population Analysis with Microcomputers, Volume I"
#'
#' @param censuses data.table for a given census that includes columns for id_vars, 'sex_id', 'age_group_years_start', 'mean'.
#' @param id_vars vector of id variables for censuses table ('ihme_loc_id', 'year_id', etc.)
#' @return data.table for a given census that includes columns for 'age_start', 'sex_ratio', 'male_age_ratio', 'female_age_ratio'.
#'
#' @export
#' @import data.table
calculate_age_sex_ratios <- function(censuses, id_vars) {

  # initial checks
  if(!all(c(id_vars, "sex_id", "age_group_years_start", "mean") %in% names(censuses))) stop("'id_vars', 'sex_id', 'age_group_years_start', 'mean' columns not in data.table")
  setkeyv(censuses, c(id_vars, "sex_id", "age_group_years_start"))

  ratios <- censuses[, list(age_group_years_start = unique(age_group_years_start),
                            sex_ratio = ((mean[sex_id == 1] / mean[sex_id == 2]) * 100),
                            male_age_ratio = ((c(NA, (mean[sex_id == 1][c(-1, -((.N / 2) - 1), -(.N / 2))]), NA, NA) / ((shift(mean[sex_id == 1], type = "lag") + shift(mean[sex_id == 1], type = "lead")) / 2)) * 100),
                            female_age_ratio = ((c(NA, (mean[sex_id == 2][c(-1, -((.N / 2) - 1), -(.N / 2))]), NA, NA) / ((shift(mean[sex_id == 2], type = "lag") + shift(mean[sex_id == 2], type = "lead")) / 2)) * 100)),
                     by = c(id_vars)]

  return(ratios)
}

#' Calculates age-sex accuracy index and its components as described in "Population Analysis with Microcomputers, Volume I"
#'
#' @param censuses data.table for a given census that includes columns for id_vars, 'sex_ratio', 'male_age_ratio', 'female_age_ratio'.
#' @param id_vars vector of id variables for censuses table ('ihme_loc_id', 'year_id', etc.)
#' @return data.table for a given census that includes columns for 'SRS' (sex-ratio score), 'ARSM' (age-ratio score males),
#' 'ARSF' (age-ratio score females), 'age_sex_accuracy_index'.
#'
#' @export
#' @import data.table
calculate_age_sex_accuracy_index <- function(censuses, id_vars, age_start_min = 10, age_start_max = 69) {

  # initial checks
  if(!all(c(id_vars, "sex_ratio", "male_age_ratio", "female_age_ratio") %in% names(censuses))) stop("'id_vars', 'sex_ratio', 'male_age_ratio', 'female_age_ratio' columns not in data.table")

  index <- censuses[between(age_group_years_start, age_start_min, age_start_max), list(SRS = mean(abs(diff(sex_ratio))),
                                                                                       ARSM = mean(abs(100 - male_age_ratio), na.rm = T),
                                                                                       ARSF = mean(abs(100 - female_age_ratio), na.rm = T)),
                    by = id_vars]
  index <- index[, age_sex_accuracy_index := (3 * SRS) + ARSM + ARSF]
  return(index)
}

single_year_age_group_censuses <- merge(single_year_age_group_censuses, age_groups[, list(age_group_id, age_group_years_start)], by = "age_group_id", all.x = T)
five_year_age_group_censuses <- merge(five_year_age_group_censuses, age_groups[, list(age_group_id, age_group_years_start)], by = "age_group_id", all.x = T)
ten_year_age_group_censuses <- merge(ten_year_age_group_censuses, age_groups[, list(age_group_id, age_group_years_start)], by = "age_group_id", all.x = T)
total_pop_censuses <- merge(total_pop_censuses, age_groups[, list(age_group_id, age_group_years_start)], by = "age_group_id", all.x = T)

# calculate age and sex ratios for all censuses
single_year_age_group_ratios <- calculate_age_sex_ratios(single_year_age_group_censuses, census_id_vars)
five_year_age_group_ratios <- calculate_age_sex_ratios(five_year_age_group_censuses, census_id_vars)
ten_year_age_group_ratios <- calculate_age_sex_ratios(ten_year_age_group_censuses, census_id_vars)
total_pop_year_age_group_ratios <- calculate_age_sex_ratios(total_pop_censuses, census_id_vars)
age_sex_ratios <- rbind(single_year_age_group_ratios, five_year_age_group_ratios,
                        ten_year_age_group_ratios, total_pop_year_age_group_ratios, use.names = T)
readr::write_csv(age_sex_ratios, path = paste0(output_dir, "/diagnostics/age_sex_ratios_reported.csv"))

# calculate age sex accuracy index
single_year_age_group_accuracy_indexes <- calculate_age_sex_accuracy_index(single_year_age_group_ratios, census_id_vars, age_start_min = 10, age_start_max = 69)
five_year_age_group_accuracy_indexes <- calculate_age_sex_accuracy_index(five_year_age_group_ratios, census_id_vars, age_start_min = 10, age_start_max = 69)
ten_year_age_group_accuracy_indexes <- calculate_age_sex_accuracy_index(ten_year_age_group_ratios, census_id_vars, age_start_min = 10, age_start_max = 69)
age_sex_index <- rbind(single_year_age_group_accuracy_indexes, five_year_age_group_accuracy_indexes,
                       ten_year_age_group_accuracy_indexes, use.names = T)
readr::write_csv(age_sex_index, path = paste0(output_dir, "/diagnostics/age_sex_index_reported.csv"))
age_sex_index[, c("SRS", "ARSM", "ARSF") := NULL]


# Smooth single-year age group censuses -----------------------------------

#' Apply feeney correction to a given census-year-sex.
#'
#' @param censuses data.table for a given census that includes columns for id_vars, 'age_start' and 'pop'.
#' @param id_vars vector of id variables for censuses table ('ihme_loc_id', 'year_id', etc.)
#' @return data.table with feeney corrected population values.
#'
#' @export
#' @import data.table
feeney_ageheaping_correction <- function(censuses, id_vars = NULL) {

  corrected <- copy(censuses)

  # initial checks
  if(!any(class(corrected) == "data.table")) stop("data.table required")
  if(!all(c(id_vars, "age_group_years_start", "mean") %in% names(corrected))) stop("'id_vars', 'age_group_years_start', 'mean' columns not in data.table")

  # collapse to highest multiple of five age group
  corrected[, max_age_multiple_five := max(floor(age_group_years_start / 5) * 5)]
  corrected[age_group_years_start > max_age_multiple_five, age_group_years_start := max_age_multiple_five]
  corrected <- corrected[, list(mean = sum(mean)), by = c(id_vars, "age_group_years_start")]
  original_pop_total <- corrected[, list(original_pop_total = sum(mean)), by = id_vars]

  # drop missing age groups, will still scale up to total that includes them
  corrected <- corrected[!is.na(age_group_years_start) & age_group_years_start >= 0]

  corrected[, c("age1", "age5") := list(age_group_years_start, floor(age_group_years_start / 5) * 5)]
  corrected[, age_multiple_five := age1 == age5]

  corrected <- corrected[, list(p_x = mean[age_multiple_five],
                                p_x_plus = sum(mean[!age_multiple_five])), by = c(id_vars, "age5")]

  # save this for later
  census_before_convergence <- corrected[, list(age5 = age5[c(1, (.N - 1), .N)],
                                                replace_p_x_interp = p_x[c(1, (.N - 1), .N)] +
                                                  p_x_plus[c(1, (.N - 1), .N)]),
                                         by = id_vars]

  iterations <- 0
  tol <- 1e-10
  corrected[, total_abs_residual := 1]
  while(any(corrected$total_abs_residual > tol)) {
    iterations <- iterations + 1

    # calculate delta x
    corrected[, p_x_minus_and_p_x_plus := shift(p_x_plus) + p_x_plus, by = id_vars]
    corrected[, delta_x := (8 / 9) * ((p_x_minus_and_p_x_plus + p_x) / p_x_minus_and_p_x_plus), by = id_vars]

    # edge cases
    corrected[age5 == 0, p_x_minus_and_p_x_plus := p_x_plus, by = id_vars]
    corrected[age5 == 0 | age5 == max(age5), delta_x := 1, by = id_vars]

    # increment
    corrected[, p_x := p_x - ((delta_x - 1) * p_x_minus_and_p_x_plus), by = id_vars]
    corrected[, p_x_plus := (delta_x + shift(delta_x, type = "lead") - 1) * p_x_plus, by = id_vars]
    corrected[, p_x_plus := c(p_x_plus[-.N], 0), by = id_vars]

    corrected[, total_abs_residual := abs(sum(delta_x - 1)), by = id_vars]
  }

  # apply linear interpolation as described by Feeney with weights 0.6 and 0.4
  corrected[, p_x_interp := (0.6 * p_x) + (0.4 * shift(p_x, type = "lead")), by = id_vars]

  # multiplied by 5 to scale up to 5 year age groups
  corrected[, p_x_interp := p_x_interp * 5, by = id_vars]

  # original recorded values are used for the youngest, terminal
  # and one below the terminal age groups
  corrected <- merge(corrected, census_before_convergence, by = c(id_vars, "age5"), all = T)
  corrected[!is.na(replace_p_x_interp), p_x_interp := replace_p_x_interp]

  # scale back up to original total
  corrected <- merge(corrected, original_pop_total, by = id_vars, all = T)
  corrected[, interp_pop_total := sum(p_x_interp), by = id_vars]
  corrected[, p_x_interp := p_x_interp * (original_pop_total / interp_pop_total)]
  corrected <- corrected[, list(age_group_years_start = age5, mean = p_x_interp), by = id_vars]

  return(corrected)
}


# apply Feeney correction to single year age group censuses
single_year_age_group_censuses <- merge(single_year_age_group_censuses, age_sex_index, by = census_id_vars, all.x = T)
single_year_age_group_censuses <- merge(single_year_age_group_censuses, location_specific_settings[, list(location_id, location_use_age_heaping_smoother)], by = c("location_id"), all.x = T)
single_year_age_group_censuses <- merge(single_year_age_group_censuses, census_specific_settings[, list(location_id, year_id, census_use_age_heaping_smoother)], by = c("location_id", "year_id"), all.x = T)
single_year_age_group_censuses[!is.na(census_use_age_heaping_smoother), location_use_age_heaping_smoother := census_use_age_heaping_smoother]

# no feeney correction applied
single_year_age_group_censuses_none <- single_year_age_group_censuses[(is.na(location_use_age_heaping_smoother) &  age_sex_accuracy_index <= 20) | !location_use_age_heaping_smoother]
single_year_age_group_censuses_none[, c("age_group_years_start", "age_sex_accuracy_index", "location_use_age_heaping_smoother", "census_use_age_heaping_smoother") := NULL]
single_year_age_group_censuses_none[, smoother := "none"]

# feeney correction applied
single_year_age_group_censuses_feeney <- feeney_ageheaping_correction(single_year_age_group_censuses[(is.na(location_use_age_heaping_smoother) &  age_sex_accuracy_index > 20) | location_use_age_heaping_smoother],
                                                                      id_vars = c(census_id_vars, "sex_id"))
single_year_age_group_censuses_feeney[, smoother := "feeney"]
single_year_age_group_censuses_feeney <- mortcore::age_start_to_age_group_id(single_year_age_group_censuses_feeney, id_vars = c(census_id_vars, "sex_id"), keep_age_group_id_only = T)
single_year_age_group_censuses_smoothed <- rbind(single_year_age_group_censuses_none, single_year_age_group_censuses_feeney, use.names = T)
rm(single_year_age_group_censuses, single_year_age_group_censuses_none, single_year_age_group_censuses_feeney)


# Smooth five-year age group censuses -------------------------------------

#' Smooth age distribution using various methods as described in "Population Analysis with Microcomputers, Volume I".
#' Input census data must be in five or ten year age groups to start
#'
#' @param censuses data.table for a given census that includes columns for id_vars, 'age_group_years_start', 'mean'.
#' @param id_vars vector of id variables for censuses table ('ihme_loc_id', 'year_id', etc.)
#' @param method smoothing method to use ('carrier_farrag', 'karup_king_newton', 'arriaga', 'arriaga_strong')
#' @param split_10_year whether to split the smoothed 10 year counts into 5 year counts using arriaga forumulas
#' @return data.table for a given census that gives the smoothed counts back.
#' If the method does not smooth a given age group (youngest and oldest for non "arriaga" methods) or the smoothed value is negative,
#' uses the original data for that age group.
#'
#' @export
#' @import data.table
smooth_age_distribution <- function(censuses, id_vars, method, split_10_year = T) {

  censuses <- copy(censuses)

  # initial checks
  if(!any(class(censuses) == "data.table")) stop("data.table required")
  if(!all(c(id_vars, "age_group_years_start", "mean") %in% names(censuses))) stop("'id_vars', 'age_group_years_start', 'mean' columns not in data.table")
  if(!method %in% c("carrier_farrag", "karup_king_newton", "arriaga", "united_nations", "arriaga_strong")) stop("method must be one of 'carrier_farrag', 'karup_king_newton', 'arriaga', 'united_nations', 'arriaga_strong'")

  # round down to the 10 year age groups
  censuses[, age_start_10 := plyr::round_any(age_group_years_start, 10, f = floor)]

  # collapse to terminal age group ending in zero
  censuses[, max_age_10 := max(age_start_10), by = id_vars]
  censuses[age_group_years_start > max_age_10, age_group_years_start := max_age_10, by = c(id_vars, "age_group_years_start", "age_start_10")]
  censuses <- censuses[, list(mean = sum(mean)), by = c(id_vars, "age_group_years_start", "age_start_10")]

  # sum to 10 year age groups
  adjusted_censuses <- censuses[, list(pop_10 = sum(mean)), by = c(id_vars, "age_start_10")]
  adjusted_censuses <- adjusted_censuses[, list(age_start_10 = age_start_10[-.N], pop_10 = pop_10[-.N]), by = id_vars] # drop the terminal age group

  # smooth 10 year age groups
  if (method == "arriaga_strong") {
    adjusted_censuses <- adjusted_censuses[, list(age_start_10, pop_10,
                                                  pop_10_adjusted = ((shift(pop_10, type = "lag") +
                                                                        (2 * pop_10) +
                                                                        shift(pop_10, type = "lead")) / 4)),
                                           by = id_vars]

    # rescale to total population in the non-extreme age groups
    adjusted_censuses[, total_original_pop := sum(pop_10[!is.na(pop_10_adjusted)]), by = id_vars]
    adjusted_censuses[, total_adjusted_pop := sum(pop_10_adjusted[!is.na(pop_10_adjusted)]), by = id_vars]
    adjusted_censuses[, pop_10_adjusted_scaled := pop_10_adjusted * (total_original_pop / total_adjusted_pop)]
    adjusted_censuses[, total_adjusted_scaled_pop := sum(pop_10_adjusted_scaled[!is.na(pop_10_adjusted_scaled)]), by = id_vars]

    adjusted_censuses[is.na(pop_10_adjusted_scaled), pop_10_adjusted_scaled := pop_10]
    adjusted_censuses[, c("pop_10", "pop_10_adjusted", "total_original_pop", "total_adjusted_pop", "total_adjusted_scaled_pop") := NULL]
    setnames(adjusted_censuses, "pop_10_adjusted_scaled", "pop_10")
  }

  # apply Carrier-Farrag formula
  if (split_10_year) {
    if (method == "carrier_farrag") {
      adjusted_censuses <- adjusted_censuses[, list(age_start_10, pop_10,
                                                    age_group_years_start = age_start_10 + 5,
                                                    mean = (pop_10 / (1 + ((shift(pop_10, type = "lag") / shift(pop_10, type = "lead")) ^ (1/4))))),
                                             by = c(id_vars)]
      adjusted_censuses <- adjusted_censuses[, list(pop_10, age_group_years_start = c(age_start_10, age_start_10 + 5),
                                                    adjusted_pop = c(pop_10 - mean, mean)), by = c(id_vars, "age_start_10")]
    } else if (method == "karup_king_newton") {
      adjusted_censuses <- adjusted_censuses[, list(age_start_10, pop_10,
                                                    age_group_years_start = age_start_10,
                                                    mean = (((1 / 2) * pop_10) + ((1 / 16) * (shift(pop_10, type = "lag") - shift(pop_10, type = "lead"))))),
                                             by = c(id_vars)]
      adjusted_censuses <- adjusted_censuses[, list(pop_10, age_group_years_start = c(age_start_10, age_start_10 + 5),
                                                    adjusted_pop = c(mean, pop_10 - mean)), by = c(id_vars, "age_start_10")]
    } else if ((method == "arriaga" | method == "arriaga_strong")) {
      adjusted_censuses <- adjusted_censuses[, list(age_start_10, pop_10,
                                                    age_group_years_start = age_start_10 + 5,
                                                    mean = (((-1 * shift(pop_10, type = "lag")) + (11 * pop_10) + (2 * shift(pop_10, type = "lead"))) / 24)),
                                             by = c(id_vars)]
      adjusted_censuses <- adjusted_censuses[, list(pop_10, age_group_years_start = c(age_start_10, age_start_10 + 5),
                                                    adjusted_pop = c(pop_10 - mean, mean)), by = c(id_vars, "age_start_10")]
      # fill extreme age groups
      adjusted_censuses[, adjusted_pop := c(NA, (((8 * pop_10[age_group_years_start == age_start_10][1]) +
                                                    (5 * pop_10[age_group_years_start == age_start_10][2]) +
                                                    (-1 * pop_10[age_group_years_start == age_start_10][3])) / 24),
                                            adjusted_pop[c(-1, -2, -(.N - 1), -.N)],
                                            (((-1 * pop_10[age_group_years_start == age_start_10][(.N / 2) - 2]) +
                                                (5 * pop_10[age_group_years_start == age_start_10][(.N / 2) - 1]) +
                                                (8 * pop_10[age_group_years_start == age_start_10][(.N / 2)])) / 24), NA),
                        by = id_vars]
      adjusted_censuses[, adjusted_pop := c(pop_10[1] - adjusted_pop[2],
                                            adjusted_pop[c(-1, -.N)],
                                            pop_10[.N] - adjusted_pop[(.N - 1)]),
                        by = id_vars]
    }
  } else {
    adjusted_censuses[, age_group_years_start := age_start_10]
    adjusted_censuses[, adjusted_pop := pop_10]
  }

  # merge on original totals in order to get original total for youngest and oldest 10 year age groups
  adjusted_censuses <- merge(adjusted_censuses, censuses, by = c(id_vars, "age_group_years_start", "age_start_10"), all = T)
  adjusted_censuses[is.na(adjusted_pop) | adjusted_pop <= 0, adjusted_pop := mean] # sometimes oldest ages can be negative with arriaga formula

  # scale up to original census total
  adjusted_censuses[, total_pop := sum(mean, na.rm = T), by = id_vars]
  adjusted_censuses[, adjusted_total_pop := sum(adjusted_pop, na.rm = T), by = id_vars]
  adjusted_censuses[, adjusted_pop := adjusted_pop * (total_pop / adjusted_total_pop)]

  adjusted_censuses[, c("age_start_10", "mean", "pop_10", "total_pop", "adjusted_total_pop") := NULL]
  setnames(adjusted_censuses, "adjusted_pop", "mean")
  return(adjusted_censuses)
}

# apply Arriaga corrections to five year age group censuses
five_year_age_group_censuses <- merge(five_year_age_group_censuses, age_sex_index, by = census_id_vars, all.x = T)
five_year_age_group_censuses <- merge(five_year_age_group_censuses, location_specific_settings[, list(location_id, location_use_age_heaping_smoother)], by = c("location_id"), all.x = T)
five_year_age_group_censuses <- merge(five_year_age_group_censuses, census_specific_settings[, list(location_id, year_id, census_use_age_heaping_smoother)], by = c("location_id", "year_id"), all.x = T)
five_year_age_group_censuses[!is.na(census_use_age_heaping_smoother), location_use_age_heaping_smoother := census_use_age_heaping_smoother]

# no smoothing applied
five_year_age_group_censuses_none <- five_year_age_group_censuses[(is.na(location_use_age_heaping_smoother) & age_sex_accuracy_index < 20) | !location_use_age_heaping_smoother]
five_year_age_group_censuses_none[, c("age_group_years_start", "age_sex_accuracy_index", "location_use_age_heaping_smoother", "census_use_age_heaping_smoother") := NULL]
five_year_age_group_censuses_none[, smoother := "none"]

# arriaga smoothing applied
five_year_age_group_censuses_arriaga <- smooth_age_distribution(five_year_age_group_censuses[(is.na(location_use_age_heaping_smoother) & between(age_sex_accuracy_index, 20, 40)) | location_use_age_heaping_smoother],
                                                                id_vars = c(census_id_vars, "sex_id"), method = "arriaga")
five_year_age_group_censuses_arriaga[, smoother := "arriaga"]
five_year_age_group_censuses_arriaga <- mortcore::age_start_to_age_group_id(five_year_age_group_censuses_arriaga, id_vars = c(census_id_vars, "sex_id"), keep_age_group_id_only = T)

# arriaga strong smoothing applied
five_year_age_group_censuses_arriaga_strong <- smooth_age_distribution(five_year_age_group_censuses[(is.na(location_use_age_heaping_smoother) & age_sex_accuracy_index > 40)],
                                                                       id_vars = c(census_id_vars, "sex_id"), method = "arriaga_strong")
five_year_age_group_censuses_arriaga_strong[, smoother := "arriaga_strong"]
five_year_age_group_censuses_arriaga_strong <- mortcore::age_start_to_age_group_id(five_year_age_group_censuses_arriaga_strong, id_vars = c(census_id_vars, "sex_id"), keep_age_group_id_only = T)

five_year_age_group_censuses_smoothed <- rbind(five_year_age_group_censuses_none,
                                               five_year_age_group_censuses_arriaga,
                                               five_year_age_group_censuses_arriaga_strong,
                                               use.names = T)
rm(five_year_age_group_censuses, five_year_age_group_censuses_none, five_year_age_group_censuses_arriaga, five_year_age_group_censuses_arriaga_strong)


# Smooth ten-year age group censuses --------------------------------------

ten_year_age_group_censuses <- merge(ten_year_age_group_censuses, age_sex_index, by = census_id_vars, all.x = T)
ten_year_age_group_censuses <- merge(ten_year_age_group_censuses, location_specific_settings[, list(location_id, location_use_age_heaping_smoother)], by = c("location_id"), all.x = T)
ten_year_age_group_censuses <- merge(ten_year_age_group_censuses, census_specific_settings[, list(location_id, year_id, census_use_age_heaping_smoother)], by = c("location_id", "year_id"), all.x = T)
ten_year_age_group_censuses[!is.na(census_use_age_heaping_smoother), location_use_age_heaping_smoother := census_use_age_heaping_smoother]

# no smoothing applied
ten_year_age_group_censuses_none <- ten_year_age_group_censuses[(is.na(location_use_age_heaping_smoother) & age_sex_accuracy_index <= 20) | !location_use_age_heaping_smoother]
ten_year_age_group_censuses_none[, c("age_group_years_start", "age_sex_accuracy_index", "location_use_age_heaping_smoother", "census_use_age_heaping_smoother") := NULL]
ten_year_age_group_censuses_none[, smoother := "none"]

# smoothing applied
ten_year_age_group_censuses_arriaga_strong <- smooth_age_distribution(ten_year_age_group_censuses[(is.na(location_use_age_heaping_smoother) & age_sex_accuracy_index > 20) | location_use_age_heaping_smoother],
                                                                      id_vars = c(census_id_vars, "sex_id"), method = "arriaga_strong", split_10_year = F)
ten_year_age_group_censuses_arriaga_strong[, smoother := "arriaga_strong"]
ten_year_age_group_censuses_arriaga_strong <- mortcore::age_start_to_age_group_id(ten_year_age_group_censuses_arriaga_strong, id_vars = c(census_id_vars, "sex_id"), keep_age_group_id_only = T)

ten_year_age_group_censuses_smoothed <- rbind(ten_year_age_group_censuses_none, ten_year_age_group_censuses_arriaga_strong, use.names = T)
rm(ten_year_age_group_censuses, ten_year_age_group_censuses_none, ten_year_age_group_censuses_arriaga_strong)


# Calculate age-sex ratios and accuracy index post smoothing --------------

single_year_age_group_censuses_smoothed <- merge(single_year_age_group_censuses_smoothed, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], by = "age_group_id", all.x = T)
five_year_age_group_censuses_smoothed <- merge(five_year_age_group_censuses_smoothed, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], by = "age_group_id", all.x = T)
ten_year_age_group_censuses_smoothed <- merge(ten_year_age_group_censuses_smoothed, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], by = "age_group_id", all.x = T)

# calculate age and sex ratios for all censuses
single_year_age_group_ratios_smoothed <- calculate_age_sex_ratios(single_year_age_group_censuses_smoothed, census_id_vars)
five_year_age_group_ratios_smoothed <- calculate_age_sex_ratios(five_year_age_group_censuses_smoothed, census_id_vars)
ten_year_age_group_ratios_smoothed <- calculate_age_sex_ratios(ten_year_age_group_censuses_smoothed, census_id_vars)
total_pop_year_age_group_ratios_smoothed <- calculate_age_sex_ratios(total_pop_censuses, census_id_vars)
age_sex_ratios_smoothed <- rbind(single_year_age_group_ratios_smoothed, five_year_age_group_ratios_smoothed,
                                 ten_year_age_group_ratios_smoothed, total_pop_year_age_group_ratios_smoothed, use.names = T)
readr::write_csv(age_sex_ratios_smoothed, path = paste0(output_dir, "/diagnostics/age_sex_ratios_smoothed.csv"))

# calculate age sex accuracy index
single_year_age_group_accuracy_indexes_smoothed <- calculate_age_sex_accuracy_index(single_year_age_group_ratios_smoothed, census_id_vars, age_start_min = 10, age_start_max = 69)
five_year_age_group_accuracy_indexes_smoothed <- calculate_age_sex_accuracy_index(five_year_age_group_ratios_smoothed, census_id_vars, age_start_min = 10, age_start_max = 69)
ten_year_age_group_accuracy_indexes_smoothed <- calculate_age_sex_accuracy_index(ten_year_age_group_ratios_smoothed, census_id_vars, age_start_min = 10, age_start_max = 69)
age_sex_index_smoothed <- rbind(single_year_age_group_accuracy_indexes_smoothed, five_year_age_group_accuracy_indexes_smoothed,
                                ten_year_age_group_accuracy_indexes_smoothed, use.names = T)
readr::write_csv(age_sex_index_smoothed, path = paste0(output_dir, "/diagnostics/age_sex_index_smoothed.csv"))

single_year_age_group_censuses_smoothed[, c("age_group_years_start", "age_group_years_end") := NULL]
five_year_age_group_censuses_smoothed[, c("age_group_years_start", "age_group_years_end") := NULL]
ten_year_age_group_censuses_smoothed[, c("age_group_years_start", "age_group_years_end") := NULL]
total_pop_censuses[, "age_group_years_start" := NULL]
total_pop_censuses[, smoother := "none"]
census_data_unheaped <- rbind(single_year_age_group_censuses_smoothed, five_year_age_group_censuses_smoothed,
                              ten_year_age_group_censuses_smoothed, total_pop_censuses, use.names = T)

census_data_unheaped[, data_stage := "corrected age heaping"]
setcolorder(census_data_unheaped, c(census_id_vars, "smoother", "sex_id", "age_group_id", "mean"))
setkeyv(census_data_unheaped, c(census_id_vars, "smoother", "sex_id", "age_group_id"))
readr::write_csv(census_data_unheaped, path = paste0(output_dir, "/outputs/05_correct_age_heaping.csv"))
