library(data.table)
library(readr)
library(stringr)
library(ggplot2)
library(boot)
library(ltcore, lib = "FILEPATH")
library(mortcore, lib = "FILEPATH")

rm(list=ls())
lt_keys <- c("ihme_loc_id", "year_id", "sex_id")
lt_max_age <- 110


# Get settings ------------------------------------------------------------

main_dir <- commandArgs(trailingOnly=T)[1]
ihme_loc <- commandArgs(trailingOnly=T)[2]

source("settings.R")
get_settings(main_dir)

age_groups <- fread(paste0(temp_dir, "/../database/age_group_ids.csv"))
location_hierarchy <- fread(paste0(temp_dir, "/../database/location_hierarchy.csv"))

loc_name <- location_hierarchy[ihme_loc_id == ihme_loc, location_name]
loc_id <- location_hierarchy[ihme_loc_id == ihme_loc, location_id]

source("modeling/helper_functions.R")
source(paste0(popcore_dir, "collapse_draws.R"))


# Set up id vars for assertion checks later -------------------------------

full_survival_id_vars <- list(ihme_loc_id = ihme_loc,
                              year_id = seq(min(years), max(years), 1),
                              sex_id = 1:2,
                              age = seq(-1, terminal_age, 1),
                              draw = 0:999)
abridged_survival_id_vars <- list(ihme_loc_id = ihme_loc,
                                  year_id = seq(min(years), max(years), 5),
                                  sex_id = 1:2,
                                  age = seq(-5, terminal_age, 5),
                                  draw = 0:999)


# Read in lifetable data and prep for interpolation -----------------------

lifetable_dir <- "FILEPATH"

# read in lifetable with shocks added
full_lt_draws <- fread(paste0(lifetable_dir, "/full_lt/with_shock/lt_full_draw_", loc_id, ".csv"))
full_lt_draws <- full_lt_draws[sex_id != 3]

full_lt_draws <- full_lt_draws[, list(ihme_loc_id = ihme_loc, year_id, sex_id, age, draw, qx, ax)]
setkeyv(full_lt_draws, c(lt_keys, "age", "draw"))


# Lifetable functions -----------------------------------------------------

gen_lifetable <- function(lt, lt_id_vars) {
  if (!setequal(c(lt_id_vars, "qx", "ax"), names(lt))) stop("assumes only columns present are 'lt_id_vars' and 'qx', 'ax'")

  setkeyv(lt, lt_id_vars)
  qx_to_lx(lt)
  lx_to_dx(lt)
  gen_age_length(lt, terminal_age = lt_max_age, terminal_length = 0)
  lt[, mx := qx_ax_to_mx(q = qx, a = ax, t = age_length)]
  gen_nLx(lt)
  gen_Tx(lt, id_vars = key(lt))
  gen_ex(lt)
  setnames(lt, "age_length", "n")
  setcolorder(lt, c(lt_id_vars, "n", "qx", "ax", "mx", "lx", "dx", "nLx", "Tx", "ex"))
  setkeyv(lt, lt_id_vars)
  return(lt)
}

abridged_lifetable <- function(lt, abridged_ages) {
  agg_lt_draws <- copy(lt)

  # assign single year ages to abridged ages
  agg_lt_draws[, abridged_age := cut(age, breaks = c(abridged_ages, Inf), labels = abridged_ages,
                                     right = F)]
  agg_lt_draws[, abridged_age := as.integer(as.character(abridged_age))]

  # aggregate qx and ax
  agg_lt_draws[, px := 1 - qx]
  agg_lt_draws[, axdx_full_years := age - abridged_age]
  agg_lt_draws <- agg_lt_draws[, list(qx = (1 - prod(px)), ax = (sum((ax + axdx_full_years) * dx) / sum(dx))),
                               by = c(lt_keys, "draw", "abridged_age")]
  setnames(agg_lt_draws, "abridged_age", "age")

  return(agg_lt_draws)
}

# Use (nLx / nLx-5)
# Also need the probability of surviving another 5 years in the terminal age group
# which is T100 / T95
calc_nLx_ratio_survival <- function(lt, use_age_int) {
  survival <- lt[age <= (terminal_age + use_age_int),
                 list(age = seq(-use_age_int, terminal_age, use_age_int),
                      value = c((nLx[1] / (use_age_int * lx[1])), (shift(nLx, type="lead") / nLx)[-c(.N - 1, .N)], (Tx[.N] / Tx[.N - 1]))),
                 by = c(lt_keys, "draw")]
  setcolorder(survival, c(lt_keys, "age", "draw", "value"))
  setkeyv(survival, c(lt_keys, "age", "draw"))
  return(survival)
}

calc_mx <- function(lt, use_age_int) {
  mx <- lt[age <= terminal_age]
  mx[age < terminal_age, value := mx]
  mx[age == terminal_age, value := lx / Tx]
  mx <- mx[, list(ihme_loc_id, year_id, sex_id, age, draw, value)]
  setcolorder(mx, c(lt_keys, "age", "draw", "value"))
  setkeyv(mx, c(lt_keys, "age", "draw"))
  return(mx)
}

# Average survival over five year periods
average_survival_5 <- function(survival_draws) {
  survival_draws <- survival_draws[, year_id := cut(year_id, breaks=c(abridged_survival_id_vars$year_id, Inf),
                                                    labels=abridged_survival_id_vars$year_id, right=F)]
  survival_draws[, year_id := as.integer(as.character(year_id))]
  survival_draws <- survival_draws[, list(value = mean(value)), by=c(lt_keys, "age", "draw")]
  return(survival_draws)
}

# Average survival over two year periods, since cohorts are exposed to survival proportions over half of each year.
average_survival_1 <- function(survival_draws) {

  # shift values up one year, replace the last year value with the same value
  survival_draws[, next_year_value := shift(value, type = "lead"), by = c("ihme_loc_id", "sex_id", "age", "draw")]
  survival_draws[is.na(next_year_value), next_year_value := value]

  # average the survival values over the two year period
  survival_draws <- survival_draws[, list(value = ((value + next_year_value) / 2)), by = c(lt_keys, "age", "draw")]
  return(survival_draws)
}

# calculate the standard deviation in the input logit survival proportion draws
calc_input_sd <- function(draws, model_age_int) {
  sd <- draws[, list(value_sd = sd(logit(value))), by = c("ihme_loc_id", "year_id", "sex_id", "age")]
  return(sd)
}


# Calculate values of interest --------------------------------------------

full_lt_draws <- full_lt_draws[between(year_id, min(years), max(years))]

## full lifetables
gen_lifetable(full_lt_draws, lt_id_vars = c(lt_keys, "draw", "age"))

full_survival_draws <- calc_nLx_ratio_survival(full_lt_draws, use_age_int = 1)
full_survival_draws <- average_survival_1(full_survival_draws)
full_survival_data <- collapse_draws(full_survival_draws, var = "value", id_vars = c(lt_keys, "age"))
full_survival_matrix <- format_as_matrix_by_sex(full_survival_data, "value_mean",
                                                ages = full_survival_id_vars$age)
full_survival_sd <- calc_input_sd(full_survival_draws, model_age_int = 1)
full_survival_sd_matrix <- format_as_matrix_by_sex(full_survival_sd, "value_sd",
                                                   ages = full_survival_id_vars$age)

full_mx_draws <- calc_mx(full_lt_draws, use_age_int = 1)
full_mx_data <- collapse_draws(full_mx_draws, var = "value", id_vars = c(lt_keys, "age"))

full_e0_draws <- full_lt_draws[age == 0, list(ihme_loc_id, year_id, sex_id, draw, value = ex)]
setkeyv(full_e0_draws, c(lt_keys, "draw"))
full_e0_data <- collapse_draws(full_e0_draws, var = "value", id_vars = lt_keys)

## abridged lifetables
abridged_lt_draws <- abridged_lifetable(full_lt_draws, abridged_ages = seq(0, lt_max_age, 5))
gen_lifetable(abridged_lt_draws, lt_id_vars = c(lt_keys, "draw", "age"))

abridged_survival_draws <- calc_nLx_ratio_survival(abridged_lt_draws, use_age_int = 5)
abridged_survival_draws <- average_survival_5(abridged_survival_draws)
abridged_survival_data <- collapse_draws(abridged_survival_draws, var = "value", id_vars = c(lt_keys, "age"))
abridged_survival_matrix <- format_as_matrix_by_sex(abridged_survival_data, "value_mean",
                                                    ages = abridged_survival_id_vars$age)
abridged_survival_sd <- calc_input_sd(abridged_survival_draws, model_age_int = 1)
abridged_survival_sd_matrix <- format_as_matrix_by_sex(abridged_survival_sd, "value_sd",
                                                       ages = abridged_survival_id_vars$age)


abridged_mx_draws <- abridged_lt_draws[, list(ihme_loc_id, year_id, sex_id, age, draw, value = mx)]
abridged_mx_draws <- calc_mx(abridged_lt_draws, use_age_int = 5)
abridged_mx_data <- collapse_draws(abridged_mx_draws, var = "value", id_vars = c(lt_keys, "age"))


# Assertion checks --------------------------------------------------------

assertable::assert_ids(full_survival_draws, id_vars = full_survival_id_vars)
assertable::assert_values(full_survival_draws, colnames="value", test="gte", test_val=0)
assertable::assert_values(full_survival_draws, colnames="value", test="lte", test_val=1)

assertable::assert_ids(abridged_survival_draws, id_vars = abridged_survival_id_vars)
assertable::assert_values(abridged_survival_draws, colnames="value", test="gte", test_val=0)
assertable::assert_values(abridged_survival_draws, colnames="value", test="lte", test_val=1)


# Save prepped data -------------------------------------------------------

write_csv(full_survival_data, path = paste0(temp_dir, "/inputs/survival_1.csv"))
write_csv(abridged_survival_data, path = paste0(temp_dir, "/inputs/survival_5.csv"))
write_csv(full_survival_sd, path = paste0(temp_dir, "/inputs/survival_sd_1.csv"))
write_csv(abridged_survival_sd, path = paste0(temp_dir, "/inputs/survival_sd_5.csv"))
write_csv(full_mx_data, path = paste0(temp_dir, "/inputs/mx_1.csv"))
write_csv(abridged_mx_data, path = paste0(temp_dir, "/inputs/mx_5.csv"))
write_csv(full_e0_data, path = paste0(temp_dir, "/inputs/original_e0.csv"))

write_csv(full_survival_draws, path = paste0(temp_dir, "/inputs/survival_1_draws.csv"))
write_csv(abridged_survival_draws, path = paste0(temp_dir, "/inputs/survival_5_draws.csv"))
write_csv(full_mx_draws, path = paste0(temp_dir, "/inputs/mx_1_draws.csv"))
write_csv(abridged_mx_draws, path = paste0(temp_dir, "/inputs/mx_5_draws.csv"))

save(full_survival_matrix, abridged_survival_matrix,
     full_survival_sd_matrix, abridged_survival_sd_matrix,
     file=paste0(temp_dir, "/inputs/survival_matrix.rdata"))
