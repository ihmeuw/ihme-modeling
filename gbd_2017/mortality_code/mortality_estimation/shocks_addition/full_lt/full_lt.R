####################################################################################################
##
## Description: Predict full lifetables from GBD abridged lifetables using regression fits from HMD
##              full lifetables. Makes key assumption that ax is 0.5 for all age groups besides the
##              first and last age groups.
####################################################################################################

# Import
library(data.table)
library(readr)
library(rhdf5)
library(assertable)

library(ltcore)
library(mortdb)
library(mortcore)


# Get settings ------------------------------------------------------------

# Retrieve command arguments and define global variables
VERSION_NO_SHOCK_DEATH_NUMBER <- commandArgs(trailingOnly = T)[1]
LOCATION_ID <- as.integer(commandArgs(trailingOnly = T)[2])
VERSION_SHOCK_AGGREGATOR <- commandArgs(trailingOnly = T)[3]
GBD_YEAR <- commandArgs(trailingOnly = T)[4]
ENABLE_ASSERTIONS <- as.logical(commandArgs(trailingOnly = T)[5])

MAX_AGE <- 110
INPUT_YEARS <- 1950:GBD_YEAR

CENTRAL_COMP_FUNCTIONS <- ""

OUTPUT_DIR <- paste0(FILEPATH, VERSION_NO_SHOCK_DEATH_NUMBER)
REGRESSION_DIR <- ""
RECKONING_DIR <- paste0(FILEPATH, VERSION_NO_SHOCK_DEATH_NUMBER)
NO_HIV_LT_DIR <- paste0(RECKONING_DIR, "/lt_hivdel/mx_ax_pre_finalizer/result")
WITH_HIV_LT_DIR <- paste0(RECKONING_DIR, "/lt_whiv/mx_ax_pre_finalizer/result")


# Load cached datasets
age_groups <- fread(file = paste0(OUTPUT_DIR, "/inputs/age_groups.csv"))
population <- fread(paste0(OUTPUT_DIR, "/inputs/population.csv"))[location_id == LOCATION_ID,]
single_abridged_age_map <- fread(paste0(OUTPUT_DIR, "/inputs/single_abridged_age_map.csv"))


# Functions ---------------------------------------------------------------

format_mx_ax_for_full_gen_lt <- function(dataset, age_groups) {
  dataset = dataset[sex_id != 3]
  dataset = merge(dataset, age_groups, by = "age_group_id")
  dataset[, qx := mx_ax_to_qx(mx, ax, age_length)]
  dataset[age == MAX_AGE, qx := 1]

  # cap any qx greater than .9999 at .9999
  if (nrow(dataset[qx > .9999]) > 0) {
    if (nrow(dataset[qx > 1.5]) > 0) {
      print(dataset[qx > 1.5])
      stop("Found rows where qx is greater than 1.5 after conversion.")
    }
    print(dataset[qx > .9999])
    warning("Found rows where qx is greater than .9999 after conversion. Capping at .9999")
    dataset[qx > .9999, qx := .9999]
  }
  dataset[, px := 1 - qx]
  dataset[, age_group_id := NULL]
  
  return(dataset)
}

# expand 5-year age groups in shock deaths to single-year age groups
expand_shock_ages <- function(abridged_shock_rates, single_abridged_age_map) {
  expanded_dataset <- CJ(sex_id = unique(abridged_shock_rates[, sex_id]),
                       year_id = unique(abridged_shock_rates[, year_id]),
                       age = unique(single_abridged_age_map[, age]),
                       draw = 0:999)
  expanded_dataset <- merge(expanded_dataset, single_abridged_age_map, by = "age", all.x = T)
  expanded_dataset <- merge(expanded_dataset, abridged_shock_rates, by = c("year_id", "draw", "abridged_age", "sex_id"), all.x = T)
  
  return(expanded_dataset)
}

# merge population on to shock deaths, calculate shock rate
calculate_shock_rates <- function(shock_deaths, population) {
  shock_rates <- merge(shock_deaths, population, by = c("year_id", "location_id", "sex_id", "age_group_id"), all.x = T)
  shock_rates[, shock_rate := deaths / mean_population]
  shock_rates[, c("deaths", "mean_population") := NULL]
  
  return(shock_rates)
}

aggregate_shock_deaths <- function(dataset, target_age_group_id, child_age_group_ids, id_vars) {
  # subset dataset to only contain the age group ids that are used to aggregate up to the target age group id
  child_ages <- dataset[age_group_id %in% child_age_group_ids]
  # select any age groups that are missing
  child_age_groups_missing <- child_age_group_ids[!(child_age_group_ids %in% unique(child_ages[, age_group_id]))]
  if (length(child_age_groups_missing) > 0) {
    stop(paste0("The following age groups id(s) required for aggregation up to age group id ", target_age_group_id," are missing: ", paste0(child_age_groups_missing, collapse = ", ")))
  }
  
  target_age <- child_ages[, lapply(.SD, sum), .SDcols = "deaths", by = id_vars]
  target_age[, age_group_id := target_age_group_id]
  return(target_age)
}

# create summary qx and ax, with associated upper and lower bounds
summarize_lt <- function(lifetable, full_lt) {
  lt_ids <- c("location_id", "year_id", "sex_id", "draw", "age")
  mean_lt_ids <- lt_ids[lt_ids != "draw"]
  measure_vars <- c("ax", "qx")

  ## For full lifetables, also generate nLx
  if(full_lt == T) {
    replacement_lt_params <- c("nLx", "Tx", "lx")

    measure_vars <- c(measure_vars, replacement_lt_params)
    lifetable <- copy(lifetable)
    setkeyv(lifetable, lt_ids)
    lifetable[, age_length := 1]
    qx_to_lx(lifetable)
    lx_to_dx(lifetable)
    gen_nLx(lifetable)
    gen_Tx(lifetable, id_vars = lt_ids)
  }

  life_table_summary <- melt(lifetable, 
                             value.name = "value", 
                             id.vars = lt_ids,
                             measure.vars = measure_vars,
                             variable.name = "life_table_parameter_name",
                             variable.factor = F)

  life_table_summary <- life_table_summary[, list(lower = quantile(value, probs = c(.025)), upper = quantile(value, probs = c(.975)), mean = mean(value)), 
                                           by = c(mean_lt_ids, "life_table_parameter_name")]

  ## Calculate mean nLx, lx, Tx based off of the mean LT rather than mean of draw-level nLx values
  if(full_lt == T) {
    mean_qx <- life_table_summary[life_table_parameter_name == "qx"]
    setnames(mean_qx, "mean", "qx")
    mean_ax <- life_table_summary[life_table_parameter_name == "ax"]
    setnames(mean_ax, "mean", "ax")

    mean_lt <- merge(mean_qx, mean_ax, by = mean_lt_ids)
    mean_lt[, age_length := 1]
    mean_lt[, mx := qx_ax_to_mx(qx, ax, 1)]
    setkeyv(mean_lt, mean_lt_ids)
    qx_to_lx(mean_lt)
    lx_to_dx(mean_lt)
    gen_nLx(mean_lt)
    gen_Tx(mean_lt, id_vars = mean_lt_ids)

    mean_lt <- melt(mean_lt, 
                    value.name = "replacement_mean", 
                    id.vars = mean_lt_ids,
                    measure.vars = replacement_lt_params,
                    variable.name = "life_table_parameter_name",
                    variable.factor = F)

    life_table_summary <- merge(life_table_summary, mean_lt, by = c(mean_lt_ids, "life_table_parameter_name"), all.x = T)
    life_table_summary[life_table_parameter_name %in% replacement_lt_params, mean := replacement_mean]
    life_table_summary[, replacement_mean := NULL]
  }

  return(life_table_summary)
}

gen_abridged_mx_ax_qx <- function(draw_level_full_lt, id_vars) {
  abridged_lt <- copy(draw_level_full_lt)
  setkeyv(abridged_lt, c(id_vars, "age"))
  qx_to_lx(abridged_lt, terminal_age = MAX_AGE)
  lx_to_dx(abridged_lt, terminal_age = MAX_AGE)
  abridged_lt[, lx := NULL]
  
  abridged_lt <- gen_abridged_lt(abridged_lt, id_vars, assert_qx = ENABLE_ASSERTIONS)
  
  abridged_lt <- merge(abridged_lt, age_groups[, list(age, age_length)], by = "age")
  
  if (nrow(abridged_lt[qx > .9999]) > 0) {
    if (nrow(abridged_lt[qx > 1.5]) > 0) {
      print(abridged_lt[qx > 1.5])
      stop("Found rows where qx is greater than 1.5 after conversion.")
    }
    print(abridged_lt[qx > .9999])
    warning("Found rows where qx is greater than 1 after conversion. Capping at .9999")
    abridged_lt[qx > .9999, qx := .9999]
  }

  abridged_lt[age == MAX_AGE, qx := 1]
  
  abridged_lt[, mx := qx_ax_to_mx(qx, ax, age_length)]
  return(abridged_lt)
}


# Validation Functions ----------------------------------------------------
validate_shock_aggregator <- function(dataset) {
  # assert that location_id matches the given location_id
  assert_values(dataset, colnames = c('location_id'), test = 'equal', test_val = LOCATION_ID)
  # No data should be missing
  assert_values(dataset, colnames = names(dataset), test = "not_na", quiet = T)
  assert_values(dataset, colnames = names(dataset), test = "gte", test_val = 0, quiet = T)
  
  # assert_ids for ages, years, sexes, that are required
  ages <- c(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 32, 235)
  sexes <- c(1, 2)
  
  id_vars <- list("age_group_id" = ages, "sex_id" = sexes, "year_id" = INPUT_YEARS)
  assert_ids(dataset, id_vars = id_vars)
}

validate_mx_ax <- function(dataset) {
  # assert that location_id matches the given location_id
  assert_values(dataset, colnames = c('location_id'), test = 'equal', test_val = LOCATION_ID)
  # No data should be missing
  assert_values(dataset, colnames = names(dataset), test = "not_na")
  
  # assert_ids for ages, years, sexes, that are required
  ages <- c(5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 28, 30, 31, 32, 33, 44, 45, 148)
  sexes <- c(1, 2, 3)
  
  id_vars <- list("age_group_id" = ages, "sex_id" = sexes, "year_id" = INPUT_YEARS, "draw" = 0:999)
  assert_ids(dataset, id_vars = id_vars)
}

# make sure mx is: no_hiv_lt < with_hiv_lt < shock_lt
validate_draw_lt_mx <- function(no_hiv_lt, with_hiv_lt, shock_lt) {
  no_hiv <- no_hiv_lt[, list(location_id, year_id, sex_id, draw, age, mx)]
  with_hiv <- with_hiv_lt[, list(location_id, year_id, sex_id, draw, age, mx)]
  shock <- shock_lt[, list(location_id, year_id, sex_id, draw, age, mx)]
  
  setnames(no_hiv, "mx", "no_hiv_mx")
  setnames(with_hiv, "mx", "with_hiv_mx")
  setnames(shock, "mx", "shock_mx")
  
  comparison <- merge(no_hiv, with_hiv, by = c("location_id", "year_id", "sex_id", "draw", "age"))
  comparison <- merge(comparison, shock, by = c("location_id", "year_id", "sex_id", "draw", "age"))
  
  comparison[, no_hiv_mx := signif(no_hiv_mx, 5)]
  comparison[, with_hiv_mx := signif(with_hiv_mx, 5)]
  comparison[, shock_mx := signif(shock_mx, 5)]
  
  diagnostics <- list(
    "with_hiv_vs_no_hiv" = assert_values(comparison, colnames = c("with_hiv_mx"), test = "gte", test_val = comparison[, no_hiv_mx], warn_only = T),
    "shock_vs_with_hiv" = assert_values(comparison, colnames = c("shock_mx"), test = "gte", test_val = comparison[, with_hiv_mx], warn_only = T)
  )
  return(diagnostics)
}

validate_abridged_qx <- function(dataset, log_dir) {
  rows_over_1_5 <- dataset[qx >= 1.5]
  if (nrow(rows_over_1_5) > 0) {
    diagnostic_file <- paste0(log_dir, "/", LOCATION_ID, ".csv")
    warning(paste0("Found rows where abridged qx is equal to or over 1.5, saving affected rows here: ", diagnostic_file))
    write_csv(rows_over_1_5, diagnostic_file)
  }
}

validate_full_lifetable_draw <- function(dataset) {
  lt_ids <- list(location_id = LOCATION_ID,
                 year_id = INPUT_YEARS,
                 sex_id = 1:2,
                 age = 0:MAX_AGE,
                 draw = 0:999)
  
  assert_ids(dataset, id_vars = lt_ids)
  assert_values(dataset, colnames = c("qx", 'ax'), test = "gte", test_val = 0)
  assert_values(dataset, colnames = c("qx"), test = "lte", test_val = 1)
}

validate_abridged_lifetable_draw <- function(dataset, compare_dataset = NULL, log_dir = NULL) {
  lt_ids <- list(location_id = LOCATION_ID,
                 year_id = INPUT_YEARS,
                 sex_id = 1:2,
                 age = unique(age_groups[, age]),
                 draw = 0:999)
  
  assert_ids(dataset, id_vars = lt_ids)
  assert_values(dataset, colnames = c("qx", 'ax'), test = "gte", test_val = 0)
  assert_values(dataset, colnames = "ax", test = "lte", test_val = 5)
  assert_values(dataset[age == 0], colnames = "ax", test = "lte", test_val = 1)
  assert_values(dataset, colnames = c("qx"), test = "lte", test_val = 1)
  assert_values(dataset[age != 110], colnames = c("qx"), test = "lt", test_val = 1)

  ## Make draw-level diagnostic if ax drifts more than 25%
  if(!is.null(compare_dataset)) {
    lt_cols <- names(lt_ids)
    setnames(compare_dataset, "ax", "ax_input")
    abridged_ax <- merge(dataset[, .SD, .SDcols = c(lt_cols, "ax")], 
                         compare_dataset[, .SD, .SDcols = c(lt_cols, "ax_input")],
                         by = lt_cols)
    abridged_ax <- abridged_ax[abs((ax - ax_input) / ax_input) > .25]
    if(nrow(abridged_ax) > 0) {
      warning(paste0(nrow(abridged_ax), " Rows have ax drift over 25% in abridged lifetable"))
      write_csv(abridged_ax, paste0(log_dir, "/", LOCATION_ID, ".csv"))
    }
  }
}

validate_full_lifetable_summary <- function(dataset) {
  lt_ids <- list(location_id = LOCATION_ID,
                 year_id = INPUT_YEARS,
                 sex_id = 1:2,
                 age = 0:MAX_AGE,
                 life_table_parameter_name = c("ax", "qx", "nLx", "lx", "Tx"))
  
  assert_ids(dataset, id_vars = lt_ids)
  assert_values(dataset, colnames = c("mean"), test="gte", test_val = 0)
}

validate_abridged_lifetable_summary <- function(dataset) {
  lt_ids <- list(location_id = LOCATION_ID,
                 year_id = INPUT_YEARS,
                 sex_id = 1:2,
                 age_group_id = age_groups[, age_group_id],
                 life_table_parameter_name = c("ax", "qx"))
  
  assert_ids(dataset, id_vars = lt_ids)
  assert_values(dataset, colnames = c("mean"), test="gte", test_val = 0)

  ## Get rid of floating point error
  dataset[mean > .9999 & mean < 1 & lower > .9999 & lower < 1 & upper > .9999 & upper < 1, mean := .99999]
  dataset[mean > .9999 & mean < 1 & lower > .9999 & lower < 1 & upper > .9999 & upper < 1, lower := .99999]
  dataset[mean > .9999 & mean < 1 & lower > .9999 & lower < 1 & upper > .9999 & upper < 1, upper := .99999]

  assert_values(dataset[life_table_parameter_name == "qx"], colnames = c("mean"), test="lte", test_val = dataset[life_table_parameter_name == "qx", upper])
  assert_values(dataset[life_table_parameter_name == "qx"], colnames = c("mean"), test="gte", test_val = dataset[life_table_parameter_name == "qx", lower])
  assert_values(dataset[life_table_parameter_name == "ax"], colnames = "mean", test = "lte", test_val = 5)
  assert_values(dataset[life_table_parameter_name == "ax" & age_group_id == 28], colnames = "ax", test = "lte", test_val = 1)
}



# Load shock deaths
shock_numbers <- tryCatch({
  fread(paste0(FILEPATH, VERSION_SHOCK_AGGREGATOR , "/draws/shocks_", LOCATION_ID, ".csv"))
}, warning = function(w) {
  warning("fread failed to read the shock aggregator file - trying read_csv from readr")
  read_csv(paste0(FILEPATH, VERSION_SHOCK_AGGREGATOR , "/draws/shocks_", LOCATION_ID, ".csv"))
}, error = function(e) {
  warning("fread failed to read the shock aggregator file - trying read_csv from readr")
  read_csv(paste0(FILEPATH, VERSION_SHOCK_AGGREGATOR , "/draws/shocks_", LOCATION_ID, ".csv"))
})

shock_numbers <- shock_numbers[cause_id == 294,] ## in shocks file, cause_id 294 is all-cause shocks

validate_shock_aggregator(shock_numbers)

shock_numbers <- shock_numbers[, cause_id := NULL]

shock_numbers <- melt(shock_numbers, value.name = "deaths",
                      id.vars = c("location_id", "sex_id", "age_group_id", "year_id"),
                      measure.vars = grep("draw", names(shock_numbers), value = T),
                      variable.name = "draw",
                      variable.factor = F)

shock_numbers[, draw := as.integer(substr(draw, 6, nchar(draw)))]

shock_numbers_28 <- aggregate_shock_deaths(dataset = shock_numbers, target_age_group_id = 28, child_age_group_ids = c(2, 3, 4), id_vars = c("location_id", "year_id", "sex_id", "draw"))
shock_numbers <- rbindlist(list(shock_numbers, shock_numbers_28), use.names = T)
rm(shock_numbers_28)

shock_numbers_to_save <- shock_numbers[age_group_id != 28]
shock_numbers_to_save[, location_id := LOCATION_ID]
write_csv(shock_numbers_to_save, paste0(OUTPUT_DIR, "/shock_numbers/finalizer_shock_deaths_", LOCATION_ID, ".csv"))

rm(shock_numbers_to_save)

shock_rates <- calculate_shock_rates(shock_numbers, population)

age_ids_to_append <- lapply(list(33, 44, 45, 148), function(age_group) {
  new_shock_rates <- shock_rates[age_group_id == 235, list(location_id, year_id, sex_id, draw, ihme_loc_id, shock_rate)]
  new_shock_rates[, age_group_id := age_group]
  return(new_shock_rates)
})
age_ids_to_append <- rbindlist(age_ids_to_append, use.names = T)
shock_rates <- rbindlist(list(age_ids_to_append, shock_rates), use.names = T)

shock_rates <- merge(shock_rates, age_groups[, list(age_group_id, age)], by = "age_group_id")
setnames(shock_rates, "age", "abridged_age")
shock_rates[, age_group_id := NULL]

single_year_shock_rates <- expand_shock_ages(shock_rates, single_abridged_age_map)
assert_values(single_year_shock_rates, names(single_year_shock_rates), test = "not_na")

rm("shock_numbers", "population", "shock_rates")


# Read in lifetable data and prep for interpolation -----------------------

## Load no-hiv mx and ax reckoning file
no_hiv_mx_ax_files <- paste0(FILEPATH, VERSION_NO_SHOCK_DEATH_NUMBER, "/lt_hivdel/mx_ax_pre_finalizer/result/mx_ax_", INPUT_YEARS, ".h5")
no_hiv_lt_draw <- data.table(rbindlist(lapply(no_hiv_mx_ax_files, load_hdf, by_val = LOCATION_ID)))

validate_mx_ax(no_hiv_lt_draw)

no_hiv_lt_draw <- format_mx_ax_for_full_gen_lt(no_hiv_lt_draw, age_groups)

## Load with-hiv mx and ax reckoning file
hiv_mx_ax_files <- paste0(FILEPATH, VERSION_NO_SHOCK_DEATH_NUMBER, "/lt_whiv/mx_ax_pre_finalizer/result/mx_ax_", INPUT_YEARS, ".h5")
hiv_lt_draw <- data.table(rbindlist(lapply(hiv_mx_ax_files, load_hdf, by_val = LOCATION_ID)))

validate_mx_ax(hiv_lt_draw)

hiv_lt_draw <- format_mx_ax_for_full_gen_lt(hiv_lt_draw, age_groups)


# Load in regression and run full lifetable for no-hiv and with-hiv -------
fits <- fread(paste0(REGRESSION_DIR, "log_fit_coefficients.csv"))

id_vars <- c("location_id", "year_id", "sex_id", "draw")

no_hiv_full_lt_draw <- gen_full_lt(no_hiv_lt_draw, fits, id_vars, max_age = MAX_AGE, assert_qx = ENABLE_ASSERTIONS)
no_hiv_full_lt_draw[, mx := qx_ax_to_mx(qx, ax, 1)]

hiv_full_lt_draw <- gen_full_lt(hiv_lt_draw, fits, id_vars, max_age = MAX_AGE, assert_qx = ENABLE_ASSERTIONS)

hiv_full_lt_draw <- merge(no_hiv_full_lt_draw[, .(location_id, year_id, sex_id, draw, age, no_hiv_qx = qx)], 
                          hiv_full_lt_draw, 
                          by = c(id_vars, "age"))
hiv_full_lt_draw[qx < no_hiv_qx, qx := no_hiv_qx]
hiv_full_lt_draw[, no_hiv_qx := NULL]

hiv_full_lt_draw[, mx := qx_ax_to_mx(qx, ax, 1)]


# Calculate with-shock with-hiv single-year lifetables --------------------
shock_hiv_full_lt_draw <- merge(hiv_full_lt_draw, single_year_shock_rates, by = c("location_id", "year_id", "sex_id", "draw", "age"), all.x = T)
# add shocks by multiplying mx with shock rate
shock_hiv_full_lt_draw[, mx := qx_ax_to_mx(qx, ax, 1) + shock_rate]
# recalculate qx using with-shock mx
shock_hiv_full_lt_draw[, qx := mx_ax_to_qx(mx, ax, 1)]
shock_hiv_full_lt_draw[age == MAX_AGE, qx := 1]

cap_full_lt_draws <- function(dt) {
  if (nrow(dt[age != MAX_AGE & qx > .9]) > 0) {
    if (nrow(dt[qx > 1.5]) > 0) {
      print(dt[qx > 1.5])
      stop("Found rows in draw-level life table where qx is greater than 1.5 after conversion.")
    }
    print(dt[age != MAX_AGE & qx > .9])
    warning("Found rows in life table where qx is greater than .9 after conversion. Capping at .9")
    dt[age != MAX_AGE & qx > .9, qx := .9]
    dt[age != MAX_AGE, mx := qx_ax_to_mx(qx, ax, 1)]
  }
}

cap_full_lt_draws(no_hiv_full_lt_draw)
cap_full_lt_draws(hiv_full_lt_draw)
cap_full_lt_draws(shock_hiv_full_lt_draw)

rm(single_year_shock_rates)

# validate mx for single-year lifetables ----------------------------------

diagnostics <- validate_draw_lt_mx(no_hiv_full_lt_draw, hiv_full_lt_draw, shock_hiv_full_lt_draw)

if (!is.null(diagnostics$with_hiv_vs_no_hiv)) {
  diagnostic_file <- paste0(OUTPUT_DIR, "/logs/full_with_hiv_mx_vs_no_hiv/with_hiv_mx_vs_no_hiv_", LOCATION_ID, ".csv")
  warning(paste0("Found rows where single-year with-hiv mx is less than no-hiv mx, saving affected rows here: ", diagnostic_file))
  write_csv(diagnostics$with_hiv_vs_no_hiv, diagnostic_file)
}

if (!is.null(diagnostics$shock_vs_with_hiv)) {
  diagnostic_file <- paste0(OUTPUT_DIR, "/logs/full_shock_mx_vs_with_hiv/shock_mx_vs_with_hiv_", LOCATION_ID, ".csv")
  warning(paste0("Found rows where single-year with-shock mx is less than with-hiv mx, saving affected rows here: ", diagnostic_file))
  write_csv(diagnostics$shock_vs_with_hiv, diagnostic_file)
}


# Calculate abridged life tables ------------------------------------------

# no-hiv life tables
no_hiv_abridged_lt_draw <- gen_abridged_mx_ax_qx(no_hiv_full_lt_draw, id_vars)
validate_abridged_qx(no_hiv_abridged_lt_draw, paste0(OUTPUT_DIR, "/logs/abridged_no_hiv_qx_1_5"))

# no-shock with-hiv life tables
hiv_abridged_lt_draw <- gen_abridged_mx_ax_qx(hiv_full_lt_draw, id_vars)
validate_abridged_qx(hiv_abridged_lt_draw, paste0(OUTPUT_DIR, "/logs/abridged_with_hiv_qx_1_5"))

# with-shock with-hiv life tables
shock_hiv_abridged_lt_draw <- gen_abridged_mx_ax_qx(shock_hiv_full_lt_draw, id_vars)
validate_abridged_qx(shock_hiv_abridged_lt_draw, paste0(OUTPUT_DIR, "/logs/abridged_shock_qx_1_5"))


# validate mx for abridged lifetables ------------------------------------------

diagnostics <- validate_draw_lt_mx(no_hiv_abridged_lt_draw, hiv_abridged_lt_draw, shock_hiv_abridged_lt_draw)

if (!is.null(diagnostics$with_hiv_vs_no_hiv)) {
  diagnostic_file <- paste0(OUTPUT_DIR, "/logs/abridged_with_hiv_mx_vs_no_hiv/with_hiv_mx_vs_no_hiv_", LOCATION_ID, ".csv")
  warning(paste0("Found rows where abridged with-hiv mx is less than no-hiv mx, saving affected rows here: ", diagnostic_file))
  write_csv(diagnostics$with_hiv_vs_no_hiv, diagnostic_file)
}

if (!is.null(diagnostics$shock_vs_with_hiv)) {
  diagnostic_file <- paste0(OUTPUT_DIR, "/logs/abridged_shock_mx_vs_with_hiv/shock_mx_vs_with_hiv_", LOCATION_ID, ".csv")
  warning(paste0("Found rows where abridged with-shock mx is less than with-hiv mx, saving affected rows here: ", diagnostic_file))
  write_csv(diagnostics$shock_vs_with_hiv, diagnostic_file)
}

rm(diagnostics)


# Calculate summary lifetables -------------------------------------------------

# summary lts
# no-hiv summary life tables
no_hiv_full_lt_summary <- summarize_lt(no_hiv_full_lt_draw, full_lt = T)

# no-shock with-hiv summary life tables
hiv_full_lt_summary <- summarize_lt(hiv_full_lt_draw, full_lt = T)

# with-shock with-hiv summary life tables
shock_hiv_full_lt_summary <- summarize_lt(shock_hiv_full_lt_draw, full_lt = T)

# Abridged lts
# no-hiv summary life tables
no_hiv_abridged_lt_summary <- summarize_lt(no_hiv_abridged_lt_draw, full_lt = F)
no_hiv_abridged_lt_summary <- merge(no_hiv_abridged_lt_summary, age_groups[, list(age, age_group_id)], by = "age")

# no-shock with-hiv summary life tables
hiv_abridged_lt_summary <- summarize_lt(hiv_abridged_lt_draw, full_lt = F)
hiv_abridged_lt_summary <- merge(hiv_abridged_lt_summary, age_groups[, list(age, age_group_id)], by = "age")

# with-shock with-hiv summary life tables
shock_hiv_abridged_lt_summary <- summarize_lt(shock_hiv_abridged_lt_draw, full_lt = F)
shock_hiv_abridged_lt_summary <- merge(shock_hiv_abridged_lt_summary, age_groups[, list(age, age_group_id)], by = "age")


# Prep and check draws and summary lifetables -----------------------------

if (ENABLE_ASSERTIONS) {
  print("Validating full LT draws")
  validate_full_lifetable_draw(no_hiv_full_lt_draw)
  validate_full_lifetable_draw(hiv_full_lt_draw)
  validate_full_lifetable_draw(shock_hiv_full_lt_draw)
  
  print("Validating abridged LT draws")
  validate_abridged_lifetable_draw(no_hiv_abridged_lt_draw, no_hiv_lt_draw, paste0(OUTPUT_DIR, "/logs/ax_compare"))
  validate_abridged_lifetable_draw(hiv_abridged_lt_draw, hiv_lt_draw, paste0(OUTPUT_DIR, "/logs/ax_compare"))
  validate_abridged_lifetable_draw(shock_hiv_abridged_lt_draw)

  print("Validating full LT summary")
  validate_full_lifetable_summary(no_hiv_full_lt_summary)
  validate_full_lifetable_summary(hiv_full_lt_summary)
  validate_full_lifetable_summary(shock_hiv_full_lt_summary)
}


# Save lifetables ---------------------------------------------------------

# no hiv
write_csv(no_hiv_full_lt_draw, path = paste0(OUTPUT_DIR, "/full_lt/no_hiv/lt_full_draw_", LOCATION_ID, ".csv"))
write_csv(no_hiv_abridged_lt_draw, path = paste0(OUTPUT_DIR, "/abridged_lt/no_hiv/lt_abridged_draw_", LOCATION_ID, ".csv"))
write_csv(no_hiv_full_lt_summary, path = paste0(OUTPUT_DIR, "/full_lt/no_hiv/summary_full_", LOCATION_ID, ".csv"))
write_csv(no_hiv_abridged_lt_summary, path = paste0(OUTPUT_DIR, "/abridged_lt/no_hiv/summary_abridged_", LOCATION_ID, ".csv"))

# with hiv
write_csv(hiv_full_lt_draw, path = paste0(OUTPUT_DIR, "/full_lt/with_hiv/lt_full_draw_", LOCATION_ID, ".csv"))
write_csv(hiv_abridged_lt_draw, path = paste0(OUTPUT_DIR, "/abridged_lt/with_hiv/lt_abridged_draw_", LOCATION_ID, ".csv"))
write_csv(hiv_full_lt_summary, path = paste0(OUTPUT_DIR, "/full_lt/with_hiv/summary_full_", LOCATION_ID, ".csv"))
write_csv(hiv_abridged_lt_summary, path = paste0(OUTPUT_DIR, "/abridged_lt/with_hiv/summary_abridged_", LOCATION_ID, ".csv"))

# with-shock
write_csv(shock_hiv_full_lt_draw, path = paste0(OUTPUT_DIR, "/full_lt/with_shock/lt_full_draw_", LOCATION_ID, ".csv"))
write_csv(shock_hiv_abridged_lt_draw, path = paste0(OUTPUT_DIR, "/abridged_lt/with_shock/lt_abridged_draw_", LOCATION_ID, ".csv"))
write_csv(shock_hiv_full_lt_summary, path = paste0(OUTPUT_DIR, "/full_lt/with_shock/summary_full_", LOCATION_ID, ".csv"))
write_csv(shock_hiv_abridged_lt_summary, path = paste0(OUTPUT_DIR, "/abridged_lt/with_shock/summary_abridged_", LOCATION_ID, ".csv"))

