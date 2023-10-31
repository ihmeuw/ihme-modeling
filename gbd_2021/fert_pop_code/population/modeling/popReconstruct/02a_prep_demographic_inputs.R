################################################################################
# Description: Prep inputs for fitting the population model.
# - load and format mortality, fertility, migration, srb inputs
# - prep for the appropriate age and year intervals
# - save input data if not fixing component
################################################################################

library(data.table)
library(readr)
library(assertable)
library(ltcore, lib.loc = "FILEPATH")
library(mortdb, lib.loc = "FILEPATH")
library(mortcore, lib.loc = "FILEPATH")

rm(list = ls())
USER <- Sys.getenv("USER")
code_dir <- "FILEPATH"
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

source(paste0(code_dir, "functions/formatting.R"))
source(paste0(code_dir, "functions/ccmpp.R"))

# read in population modeling location hierarchy
location_hierarchy <- fread(paste0(output_dir, "/database/all_location_hierarchies.csv"))
location_hierarchy <- location_hierarchy[location_set_name == "population"]

# read in gbd age group ids
age_groups <- fread(paste0(output_dir, "/database/age_groups.csv"))

# read in location specific settings and set defaults if they are not specified
location_specific_settings <- fread(paste0(output_dir, "/database/location_specific_settings.csv"))
location_specific_settings <- merge(location_specific_settings, location_hierarchy[, list(ihme_loc_id, location_id)], by = "ihme_loc_id", all.x = T)


# Id variables needed -----------------------------------------------------

fertility_age_groups_needed <- age_groups[(single_year_model), list(age_group_id, age_group_years_start, age_group_years_end)]
survival_age_groups_needed <- age_groups[single_year_model | age_group_id == 164, list(age_group_id, age_group_years_start, age_group_years_end)]
migration_age_groups_needed <- age_groups[(if (age_int == 1) single_year_model else five_year_model) | age_group_id == 164,
                                          list(age_group_id, age_group_years_start, age_group_years_end)]
setkeyv(fertility_age_groups_needed, c("age_group_years_start"))
setkeyv(survival_age_groups_needed, c("age_group_years_start"))
setkeyv(migration_age_groups_needed, c("age_group_years_start"))

asfr_id_vars <- list(location_id = location_hierarchy[is_estimate == 1, location_id],
                     year_id = years[c(-length(years))],
                     age_group_id = fertility_age_groups_needed$age_group_id)
srb_id_vars <- list(location_id = location_hierarchy[is_estimate == 1, location_id],
                    year_id = years[c(-length(years))])
survival_id_vars <- list(location_id = location_hierarchy[is_estimate == 1, location_id],
                         year_id = years[c(-length(years))],
                         sex_id = 1:2,
                         age_group_id = survival_age_groups_needed$age_group_id)
migration_id_vars <- list(location_id = location_hierarchy[is_estimate == 1, location_id],
                          year_id = years[c(-length(years))],
                          sex_id = 1:2,
                          age_group_id = migration_age_groups_needed$age_group_id)


# Load fertility estimates ------------------------------------------------

## LOAD
asfr_data <- fread(paste0(output_dir, "/database/asfr.csv"))
asfr_data <- average_adjacent_year_rates(asfr_data, id_vars = c("location_id", "year_id", "age_group_years_start", "age_group_years_end", "age_group_id"))

## FORMAT FOR TMB
setcolorder(asfr_data, c("location_id", "year_id", "age_group_years_start", "age_group_years_end", "age_group_id", "mean"))
setkeyv(asfr_data, c("location_id", "year_id", "age_group_years_start"))

## CHECK
assertable::assert_ids(asfr_data, id_vars = asfr_id_vars)
assertable::assert_values(asfr_data, colnames="mean", test="gte", test_val=0)


# Load srb estimates ------------------------------------------------------

## LOAD
srb_data <- fread(paste0(output_dir, "/database/srb.csv"))

# aggregate into five year intervals
if (year_int == 5) {
  srb_data[, year_id := plyr::round_any(year_id, accuracy = 5, f = floor)]
  srb_data <- srb_data[, list(mean = mean(mean)), by = c("ihme_loc_id", "year_id")]
}
srb_data <- average_adjacent_year_rates(srb_data, id_vars = c("location_id", "year_id"))

## FORMAT FOR TMB
srb_data <- srb_data[, c("location_id", "year_id", "mean"), with = F]
setkeyv(srb_data, c("location_id", "year_id"))

## CHECK
assertable::assert_ids(srb_data, id_vars = srb_id_vars)
assertable::assert_values(srb_data, colnames="mean", test="gte", test_val=0)


# Load lifetable estimates ------------------------------------------------

## LOAD
lt <- fread(paste0(output_dir, "/database/full_lt.csv"))
lt <- lt[location_id %in%  location_hierarchy[is_estimate == 1, location_id]]

lt_id_vars <- c("location_id", "year_id", "sex_id", "age")
lt_max_age <- max(lt$age)
setkeyv(lt, lt_id_vars)

# collapse to appropriate age groups (collapse under 5)
survival_age_int <- 1
if (survival_age_int == 5) {
  # need dx
  ltcore::qx_to_lx(lt)
  ltcore::lx_to_dx(lt)

  # assign single year ages to abridged ages
  abridged_ages <- seq(0, lt_max_age, survival_age_int)
  lt[, abridged_age := cut(age, breaks = c(abridged_ages, Inf), labels = abridged_ages, right = F)]
  lt[, abridged_age := as.integer(as.character(abridged_age))]

  # aggregate qx and ax. Aggregates ax by solving equation for average number of person years
  # lived by people dying in the age group. 0a5 * 0d5 = (1a0 * 1d0) + ((4a1 * 4d1) + 1)
  # For the 1-4 age group have to add on the years they
  lt[, px := 1 - qx]
  lt[, axdx_full_years := age - abridged_age]
  lt <- lt[, list(qx = (1 - prod(px)), ax = (sum((ax + axdx_full_years) * dx) / sum(dx))),
           by = c(setdiff(lt_id_vars, "age"), "abridged_age")]
  setnames(lt, "abridged_age", "age")
}

# calculate rest of lifetable to get nLx and Tx
ltcore::qx_to_lx(lt)
ltcore::lx_to_dx(lt)
ltcore::gen_age_length(lt, terminal_age = lt_max_age, terminal_length = 0)
lt[, mx := ltcore::qx_ax_to_mx(q = qx, a = ax, t = age_length)]
ltcore::gen_nLx(lt)
ltcore::gen_Tx(lt, id_vars = key(lt))
ltcore::gen_ex(lt)
setnames(lt, "age_length", "n")
setcolorder(lt, c(lt_id_vars, "n", "qx", "ax", "mx", "lx", "dx", "nLx", "Tx", "ex"))
setkeyv(lt, c(lt_id_vars))

# calculate survival ratio and average over adjacent years
survival_data <- lt[age <= (terminal_age + survival_age_int),
                    list(age = seq(-1, terminal_age, survival_age_int),
                         mean = c((nLx[1] / (survival_age_int * lx[1])),
                                  (shift(nLx, type="lead") / nLx)[-c(.N - 1, .N)],
                                  (Tx[.N] / Tx[.N - 1]))),
                    by = setdiff(lt_id_vars, "age")]
setnames(survival_data, c("age"), c("age_group_years_start"))
survival_data <- mortcore::age_start_to_age_group_id(survival_data, id_vars = c("location_id", "year_id", "sex_id"), keep_age_group_id_only = F)
survival_data <- average_adjacent_year_rates(survival_data, id_vars = c("location_id", "year_id", "sex_id", "age_group_years_start", "age_group_years_end", "age_group_id"))

## FORMAT FOR TMB
setcolorder(survival_data, c("location_id", "year_id", "sex_id", "age_group_id", "age_group_years_start", "age_group_years_end", "mean"))
setkeyv(survival_data, c("location_id", "year_id", "sex_id", "age_group_years_start"))

## CHECK
assertable::assert_ids(survival_data, id_vars = survival_id_vars)
assertable::assert_values(survival_data, colnames="mean", test="gte", test_val=0)
assertable::assert_values(survival_data, colnames="mean", test="lte", test_val=1)


# Load migration data -----------------------------------------------------

migration_data_id_vars <- c("location_id", "year_id", "nid", "underlying_nid")

## LOAD
migration_data <- fread(paste0(output_dir, "/database/migration_data.csv"))
migration_data[, year_id := (year_start + year_end) / 2]
migration_data[, c("year_start", "year_end") := NULL]
migration_data <- migration_data[outlier_type_id == 1 & between(year_id, year_start, year_end),
                                 list(location_id, year_id, nid, underlying_nid, sex_id, age_group_id, mean = net_flow)]

# subset to locations where we want to use migration data
migration_data <- migration_data[location_id %in% location_specific_settings[use_migration_data == "T", location_id]]

if (nrow(migration_data) > 0) {

  migration_data <- merge(migration_data, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], all.x = T, by = "age_group_id")

  # need last population run for age-sex splitting
  gbd_pop_previous <- fread(paste0(output_dir, "/database/gbd_population_current_round_best.csv"))
  gbd_pop_previous <- gbd_pop_previous[location_id %in% unique(migration_data$location_id), list(location_id, year_id, sex_id, age_group_id, pop = population)]
  gbd_pop_previous_aggregated <- mortcore::agg_results(gbd_pop_previous, id_vars = c("location_id", "year_id", "sex_id", "age_group_id"), value_vars = "pop",
                                                       agg_hierarchy = F, age_aggs = migration_age_groups_needed[age_group_id != 164, age_group_id])
  gbd_pop_previous_aggregated <- gbd_pop_previous_aggregated[age_group_id %in% migration_age_groups_needed[age_group_id != 164, age_group_id]]

  gbd_pop_previous <- merge(gbd_pop_previous, age_groups[, list(age_group_id, age_group_years_start)], all.x = T, by = "age_group_id")
  gbd_pop_previous_aggregated <- merge(gbd_pop_previous_aggregated, age_groups[, list(age_group_id, age_group_years_start)], all.x = T, by = "age_group_id")

  # collapse to the terminal age group
  migration_data[, collapse_to_age := terminal_age]
  migration_data <- agg_age_data(migration_data, id_vars = migration_data_id_vars, age_grouping_var = "collapse_to_age")

  # separate out data that needs to be age-sex split
  age_split_data <- migration_data[sex_id == 3 & age_group_id == 22]

  # transform age specific migration counts into proportions

  migration_data <- migration_data[!(sex_id == 3 & age_group_id == 22)]
  migration_data <- merge(migration_data, age_groups[, list(age_group_id, age_group_years_start)], all.x = T, by = "age_group_id")
  migration_data <- merge(migration_data, gbd_pop_previous, by = c("location_id", "year_id", "sex_id", "age_group_years_start"), all.x = T)
  migration_data <- migration_data[, list(mean = mean / pop), by = c(migration_data_id_vars, "sex_id", "age_group_years_start")]
  migration_data <- mortcore::age_start_to_age_group_id(migration_data, id_vars = c(migration_data_id_vars, "sex_id"), keep_age_group_id_only = T)

  ## prep age sex patterns to split data with

  # uniform age pattern to be used in extreme refugee migration situations
  uniform_age_sex_pattern <- CJ(sex_id = 1:2, age_group_years_start = 0:terminal_age, prop = 0.001)

# add on years that we don't have values for and assign value to be zero
migration_data_all <- CJ(location_id = location_hierarchy[is_estimate == 1, location_id], year_id = years,
                         sex_id = 1:2, age_group_id = migration_age_groups_needed$age_group_id,
                         missing_value = 0)
migration_data <- merge(migration_data, migration_data_all,
                        by = c("location_id", "year_id", "sex_id", "age_group_id"), all = T)
migration_data[, mean := as.numeric(mean)] # need this when no migration data is used in testing cases
migration_data[is.na(mean), mean := missing_value]
migration_data[, missing_value := NULL]

# zero out migration prior above age 70
migration_data <- merge(migration_data, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], all.x = T, by = "age_group_id")
migration_data[age_group_years_start >= 70, mean := 0]

migration_data <- average_adjacent_year_rates(migration_data, id_vars = c("location_id", "year_id", "sex_id", "age_group_years_start", "age_group_years_end", "age_group_id"))

## FORMAT FOR TMB
setcolorder(migration_data, c(migration_data_id_vars, "sex_id", "age_group_years_start", "age_group_years_end", "age_group_id", "mean"))
setkeyv(migration_data, c(migration_data_id_vars, "sex_id", "age_group_years_start", "age_group_years_end", "age_group_id"))

## CHECK
assertable::assert_ids(migration_data, id_vars = migration_id_vars)
assertable::assert_values(migration_data, colnames = "mean", test = "not_na")


# Save inputs -------------------------------------------------------------

readr::write_csv(asfr_data, path = paste0(output_dir, "/inputs/asfr.csv"))
readr::write_csv(srb_data, path = paste0(output_dir, "/inputs/srb.csv"))
readr::write_csv(survival_data, path = paste0(output_dir, "/inputs/survival.csv"))
readr::write_csv(migration_data, path = paste0(output_dir, "/inputs/migration.csv"))
