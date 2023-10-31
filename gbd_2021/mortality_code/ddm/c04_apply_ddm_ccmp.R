###################################################################
# Description:
# 1) Reads in outputted outliered/formatted population and mortality data
#    from c03 in STATA pipeline, and reformats from wide to long
# 2) Runs ccmp method on each pair of censuses-mortality sources
#    - uses asfr and srb estimates
#    - uses top ranked age trims from simulation work
#
# Notes:
# - only applying this to Census, VR, SRS, DSP data
# - not evaluating both sexes combined sources
# - not currently evaluating sources prior to 1950 due to not having GBD population estimates
#
# Outputs:
# - CCMP completeness estimates in "FILEPATH"
#   which is formatted similarly to "FILEPATH"
########################################################################


library(haven)
library(data.table)
library(dplyr)
library(assertable)
library(argparse)
library(parallel)
library(readstata13)

library(mortcore)
library(mortdb)
library(ltcore)

library(TMB)
library(boot)

rm(list = ls())

parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version_id for this run of DDM')
parser$add_argument('--gbd_year', type="integer", required=TRUE,
                    help='GBD round')
parser$add_argument('--pop_sy_vid', type="integer", required=TRUE,
                    help='The version_id for the input population single year')
parser$add_argument('--asfr_vid', type="integer", required=TRUE,
                    help='The version_id for the input asfr')
parser$add_argument('--srb_vid', type="integer", required=TRUE,
                    help='The version_id for the input sex ratio at birth')
parser$add_argument('--code_dir', type="character", required=TRUE,
                    help='Directory where ddm code is cloned')
args <- parser$parse_args()
list2env(args, .GlobalEnv)

main_dir <- paste0("FILEPATH")

source(paste0(code_dir, "/functions/ccmp_method_functions.R"))

age_groups <- get_age_map(gbd_year=gbd_year,type = "all")
age_group_table_terminal_age <- 125

location_hierarchy <- get_locations(gbd_year = gbd_year, gbd_type = "mortality")

# compile the TMB model
TMB::compile(paste0(code_dir, "/functions/ccmp_model.cpp"))  # Compile the C++ file
dyn.load(dynlib(paste0(code_dir, "/functions/ccmp_model")))  # Dynamically link the C++ code
TMB::openmp(n = 1)
TMB::config(tape.parallel=0, DLL="ccmp_model")

zero_offset <- 0.001
all_terminal_age <- 95
age_int <- 1
census_default_sd <- 0.01
inflate_baseline_sd_scalar <- 1
# give equal weight to each age group
sd_age_scalar <- CJ(location_id = location_hierarchy[, location_id],
                    year_id = 1950:gbd_year, sex_id = 1:2,
                    age_middle_floor = 0:all_terminal_age,
                    sd_age_scalar = 1)


# Get large inputs --------------------------------------------------------

gbd_population <- get_mort_outputs(model_name = "population single year", model_type = "estimate", run_id = pop_sy_vid, sex_ids = 1:2,gbd_year=gbd_year)
gbd_population <- merge(gbd_population, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], by = "age_group_id", all.x = T)

asfr_dir <- paste0("FILEPATH")
age_int_name <- "one"
fert_age_int <- 1
all_asfr_data <- lapply(location_hierarchy[is_estimate == 1, ihme_loc_id], function(ihme_loc) {
  loc_id <- location_hierarchy[ihme_loc_id == ihme_loc, location_id]
  data <- fread(paste(asfr_dir, paste0(age_int_name, "_by_", age_int_name),
                      paste0(ihme_loc, "_fert_", fert_age_int, "by", fert_age_int, ".csv"), sep = "/"))
  data <- data[, list(location_id = loc_id, year_id, age_group_years_start = age, mean = value_mean)]
})
all_asfr_data <- rbindlist(all_asfr_data)

all_srb_data <- get_mort_outputs(model_name = "birth sex ratio", model_type = "estimate", run_id = srb_vid,gbd_year=gbd_year)
all_srb_data <- all_srb_data[, list(location_id, year_id, mean)]

eurostat_migration_data <- fread("FILEPATH")
setnames(eurostat_migration_data, "age_start", "age_group_years_start")

method_trim_rankings <- fread("FILEPATH")
method_trim_rankings <- method_trim_rankings[method == "ccmp"]
method_trim_rankings[, full_method := paste0("CCMP", ifelse(aplus, "_aplus", ""), ifelse(fix_migration, "_no_migration", "_migration"))]


# Read in initial----------------------------------------------------------------------------------------------

# helper function to extract digits at beginning or end of string
get_int_from_str <- function(string, start=T) {
  if (start) {
    out <- as.integer(regmatches(string, regexpr('^[0-9]+', string)))
  } else {
    out <- as.integer(regmatches(string, regexpr('[0-9]+$',string)))
  }
  return(out)
}

input_03 <- setDT(read_dta(paste0("FILEPATH")))
input_03 <- input_03[sex != 'both' & source_type %in% c('VR', 'DSP', 'SRS', "MCCD")]
input_03[, sex_id := ifelse(sex == "male", 1L, 2L)]

# only keep pairs from adjacent censuses
input_03 <- input_03[, `:=`(popyear1 = get_int_from_str(pop_years, T),
                            popyear2 = get_int_from_str(pop_years, F))]
input_03[, min_popyear2:=min(popyear2), by=c('popyear1','sex','source_type','ihme_loc_id')]
input_03 <- input_03[popyear2==min_popyear2]
input_03[, min_popyear2:=NULL]

# Grab non standard population age groups
ten_yr_groups <- unique(input_03[agegroup1 - agegroup0 == 10, c('id', 'pop_years')])
twenty_yrs <- unique(input_03[agegroup1 - agegroup0 == 20, c('id', 'pop_years')])

# determine important sets of variable names
age_cols <- grep('agegroup', names(input_03), value=T)
pop1_cols <- grep('^c1', names(input_03), value=T)
pop2_cols <- grep('^c2', names(input_03), value=T)
mort_cols <- grep('^vr', names(input_03), value=T)
id_cols <- names(input_03)[!(names(input_03) %in% c(age_cols, pop1_cols, pop2_cols, mort_cols))]

reshape_long <- function(data, idvars, measurevars, valuename, is_pop=F, is_c1=T) {
  subs <- data[, .SD, .SDcols = c(idvars, measurevars)]
  subs <- melt(subs, id.vars= idvars, measure.vars= measurevars, variable.name='age_group', value.name=valuename, na.rm=T)

  # If is pop, distinguish pop1 from pop2
  if (is_pop) {
    if(is_c1) {
      subs[, year_id := get_int_from_str(pop_years, T)]
    } else {
      subs[, year_id := get_int_from_str(pop_years, F)]
    }
  }

  return(subs)
}

# Reshape age groups long
age_groups <- reshape_long(input_03, id_cols, age_cols, 'age', F)
age_groups[, age_group := as.numeric(gsub("agegroup", "", age_group))]

# Reshape population long
pop1 <- reshape_long(input_03, id_cols, pop1_cols, 'population', T, T)
pop2 <- reshape_long(input_03, id_cols, pop2_cols, 'population', T, F)
population <- rbind(pop1, pop2, use.names=T)
population[, age_group := as.numeric(gsub("c1_|c2_", "", age_group))]
population <- merge(population, age_groups, by = c(id_cols, "age_group"), all = T)
population[, age_group := NULL]
setkeyv(population, c('location_id', 'pop_years', 'year_id', 'sex_id', 'age'))

# Reshape deaths long
deaths <- reshape_long(input_03, id_cols, mort_cols, 'deaths', F)
deaths[, age_group := as.numeric(gsub("vr_", "", age_group))]
deaths <- merge(deaths, age_groups, by = c(id_cols, "age_group"), all = T)
deaths[, age_group := NULL]
setkeyv(deaths, c('location_id', 'pop_years', 'sex_id', 'age'))

# Output all ages deaths, for use in c08 ccmp weighting
all_ages_deaths <- deaths[, list(ihme_loc_id, sex, popyear1, popyear2, source_type, deaths_years, age, deaths)]
# sum across ages
all_ages_deaths <- all_ages_deaths[, sum(deaths), by=c('ihme_loc_id','sex','popyear1','popyear2','deaths_years', 'source_type')]
setnames(all_ages_deaths, 'V1', 'deaths')

# output
fwrite(all_ages_deaths, paste0("FILEPATH"))


combined_pop_vr <- merge(population, deaths, by=c(id_cols, 'age'), all=T)

dropped_points <- NULL

combined_pop_vr[, max_age := max(age), by = setdiff(id_cols, c("id", "sex", "sex_id"))]
dropped_points_temp <- combined_pop_vr[!max_age >= 60]
dropped_points_temp[, reason := "terminal age group below 60"]
dropped_points_temp[, max_age := NULL]
dropped_points <- rbind(dropped_points, dropped_points_temp, use.names = T)
combined_pop_vr <- combined_pop_vr[max_age >= 60]
combined_pop_vr[, max_age := NULL]

# drop data where we only have female or male, not both
combined_pop_vr[, count_sexes := length(unique(sex)), by = setdiff(id_cols, c("id", "sex", "sex_id"))]
dropped_points_temp <- combined_pop_vr[!count_sexes == 2]
dropped_points_temp[, reason := "Does not have both female and male data"]
dropped_points_temp[, count_sexes := NULL]
dropped_points <- rbind(dropped_points, dropped_points_temp, use.names = T)
combined_pop_vr <- combined_pop_vr[count_sexes == 2]
combined_pop_vr[, count_sexes := NULL]

# drop locations where some deaths are NA
combined_pop_vr[, na_deaths := nrow(.SD[is.na(deaths)]) > 0, by = setdiff(id_cols, c("id", "sex", "sex_id"))]
dropped_points_temp <- combined_pop_vr[(na_deaths)]
dropped_points_temp[, reason := "Deaths column has missing values, likely formatting error"]
dropped_points_temp[, na_deaths := NULL]
dropped_points <- rbind(dropped_points, dropped_points_temp, use.names = T)
combined_pop_vr <- combined_pop_vr[!(na_deaths)]
combined_pop_vr[, na_deaths := NULL]

# drop locations where some age have zero pop
combined_pop_vr[, zero_pop := nrow(.SD[population == 0]) > 0, by = setdiff(id_cols, c("id", "sex", "sex_id"))]
dropped_points_temp <- combined_pop_vr[(zero_pop)]
dropped_points_temp[, reason := "Pop column has zero values, likely formatting error"]
dropped_points_temp[, zero_pop := NULL]
dropped_points <- rbind(dropped_points, dropped_points_temp, use.names = T)
combined_pop_vr <- combined_pop_vr[!(zero_pop)]
combined_pop_vr[, zero_pop := NULL]

# drop locations not in the location hierarchy like Old Andhra Pradesh
dropped_points_temp <- combined_pop_vr[!(location_id %in% location_hierarchy$location_id)]
dropped_points_temp[, reason := "Population estimates don't exist for location"]
dropped_points <- rbind(dropped_points, dropped_points_temp, use.names = T)
combined_pop_vr <- combined_pop_vr[location_id %in% location_hierarchy$location_id]

fwrite(dropped_points, paste0("FILEPATH"))

# determine the id variables that should be in the final file
final_id_vars <- c("ihme_loc_id", "pop_years", "deaths_years", "country",
                   "pop_source", "pop_footnote", "pop_nid", "underlying_pop_nid",
                   "deaths_source", "deaths_footnote", "source_type", "deaths_nid", "deaths_underlying_nid",
                   "sex")
# add on variables we need for ccmp data prep
use_id_vars <- c(final_id_vars, "location_id", "sex_id", "popyear1", "popyear2")

all_pairs <- unique(combined_pop_vr[, setdiff(use_id_vars, c("sex", "sex_id")), with = F])
all_pairs <- all_pairs[popyear1 >= 1950 & popyear2 >= 1950]


all_comp_func <- function(pair) {

  loc_id <- all_pairs[pair, location_id]
  year_start <- all_pairs[pair, popyear1]
  year_end <- all_pairs[pair, popyear2]

  midyear <- mean(c(year_start, year_end))

  print(paste0("Pair ", pair, " of ", nrow(all_pairs), ". Location id: ", loc_id, ", ", year_start, "-", year_end))

  combined_pop_vr_subset <- merge(combined_pop_vr, all_pairs[pair,], by = setdiff(use_id_vars, c("sex", "sex_id")))
  census_data <- combined_pop_vr_subset[, list(location_id, year_id, sex_id, age, population)]
  mx_data <- combined_pop_vr_subset[year_id == popyear1, list(location_id, year_id = midyear, sex_id, age, deaths)]
  mx_data[deaths == 0, deaths := zero_offset]
  terminal_age <- max(census_data$age)


  # Prep mx data ------------------------------------------------------------

  ## interpolate population between year_start and year_end to get population at the midpoint
  midpop <- census_data[, list(year_id = midyear, population = exp(approx(year_id, log(population), xout = midyear)$y)), by = c("location_id", "sex_id", "age")]

  ## divide deaths by midyear population to get mx
  mx_data <- merge(mx_data, midpop, by = c("location_id", "year_id", "sex_id", "age"))
  mx_data[, mx := deaths / population]

  ## generate single year age group mx values
  fits <- fread("FILEPATH")

  # prep for gen_full_lt function
  ltcore::gen_age_length(mx_data, terminal_age = terminal_age, terminal_length = age_group_table_terminal_age - terminal_age)
  mx_data[, qx := ltcore::mx_to_qx(mx, age_length)]
  mx_data[age == terminal_age, qx := 1]
  mx_data[, ax := ltcore::mx_qx_to_ax(m = mx, q = qx, t = age_length)]
  mx_data[, px := 1 - qx]

  # use lx interpolation to generate single year age group qx and ax values
  mx_data <- ltcore::gen_full_lt(mx_data, fits, id_vars = c("location_id", "year_id", "sex_id"), terminal_age = terminal_age,
                                 lx_spline_start_age = 0, lx_spline_end_age = terminal_age, preserve_input_ax_ages = terminal_age)

  # calculate single year age group mx values
  ltcore::gen_age_length(mx_data, terminal_age = terminal_age, terminal_length = age_group_table_terminal_age - terminal_age)
  mx_data[, mx := qx_ax_to_mx(q = qx, a = ax, t = age_length)]

  # duplicate midyear data for all years between censuses
  mx_data <- mx_data[, list(year_id = year_start:year_end, mean = mx), by = c("location_id", "sex_id", "age")]
  mx_data <- average_adjacent_year_rates(mx_data, id_vars = c("location_id", "year_id", "sex_id", "age"))

  # format final data
  mx_data <- mx_data[, list(location_id, year_id, sex_id, age_group_years_start = age, mean)]
  setkeyv(mx_data, c("location_id", "year_id", "sex_id", "age_group_years_start"))

  # check data
  id_vars <- list(location_id = loc_id, year_id = year_start:(year_end - 1), sex_id = 1:2, age_group_years_start = 0:terminal_age)
  assertable::assert_ids(mx_data, id_vars)
  assertable::assert_values(mx_data, test = "gt", test_val = 0, colnames = "mean")

  # prep TMB matrix
  mx_matrix <- format_as_matrix_by_sex(mx_data, "mean", ages = 0:terminal_age)


  # Prep census data --------------------------------------------------------
  ## interpolate population over age in year_start and year_end to get single year age group populations

  gbd_pop <- gbd_population[location_id == loc_id & year_id %in% c(year_start, year_end), list(location_id, year_id, sex_id, age_group_years_start, mean = population)]
  gbd_pop[age_group_years_start > terminal_age, age_group_years_start := terminal_age]
  gbd_pop <- gbd_pop[, list(mean = sum(mean)), by = c("location_id", "year_id", "sex_id", "age_group_years_start")]

  # aggregate the population estimates into the census age groups
  census_data_age_groups <- census_data[, list(age_groups = list(sort(unique(age)))), by = c("location_id", "year_id", "sex_id")]

  gbd_pop <- merge(census_data_age_groups, gbd_pop, by = c("location_id", "year_id", "sex_id"), all.x = T)
  gbd_pop[, age := as.integer(as.character(cut(age_group_years_start, breaks = c(unique(unlist(age_groups)), Inf), labels = unique(unlist(age_groups)), right = F))),
          by = c("location_id", "year_id", "sex_id")]
  gbd_pop[, age := as.integer(as.character(age))]

  # calculate proportions in each aggregate age group and use to split
  gbd_pop <- gbd_pop[, list(age_group_years_start, prop = mean / sum(mean)), by = c("location_id", "year_id", "sex_id", "age")]
  census_data <- merge(census_data, gbd_pop, by = c("location_id", "year_id", "sex_id", "age"), all = T, allow.cartesian = T)
  census_data <- census_data[, list(location_id, year_id, sex_id, age = age_group_years_start, population = population * prop)]

  # interpolate to smooth non-terminal age groups
  census_data[, new_mean := c(exp(KernSmooth::locpoly(x = age[-.N], y = log(population[-.N]), degree = 1, bandwidth = 2, gridsize = (.N - 1),
                                                      range.x = c(age[1], age[.N - 1]))$y), population[.N]),
              by = c("location_id", "year_id", "sex_id")]

  # rescale to original total population
  census_data[, original_total_pop := sum(population), by = c("location_id", "year_id", "sex_id")]
  census_data[, correction_factor := (original_total_pop / sum(new_mean))]
  census_data[, mean := new_mean * correction_factor]

  # format final data
  setnames(census_data, "age", "age_group_years_start")
  census_data <- mortcore::age_start_to_age_group_id(census_data, id_vars = c("location_id", "year_id", "sex_id"))
  census_data[, n := age_group_years_end - age_group_years_start]
  census_data[, drop := F]
  setcolorder(census_data, c("location_id", "year_id", "sex_id", "age_group_years_start", "age_group_years_end", "age_group_id", "n", "drop", "mean"))
  setkeyv(census_data, c("location_id", "year_id", "sex_id", "age_group_years_start"))

  # check data
  id_vars <- list(location_id = loc_id, year_id = c(year_start, year_end), sex_id = 1:2, age_group_years_start = 0:terminal_age)
  assertable::assert_ids(census_data, id_vars)
  assertable::assert_values(census_data, test = "gt", test_val = 0, colnames = "mean")


  # Prep asfr data ----------------------------------------------------------

  asfr_data <- all_asfr_data[location_id == loc_id & between(year_id, year_start, year_end)]
  asfr_data <- asfr_data[age_group_years_start <= terminal_age]
  asfr_data <- average_adjacent_year_rates(asfr_data, id_vars = c("location_id", "year_id", "age_group_years_start"))

  # format final data
  setcolorder(asfr_data, c("location_id", "year_id", "age_group_years_start", "mean"))
  setkeyv(asfr_data, c("location_id", "year_id", "age_group_years_start"))

  # check data
  id_vars <- list(location_id = loc_id, year_id = year_start:(year_end - 1), age_group_years_start = 0:terminal_age)
  assertable::assert_ids(asfr_data, id_vars)
  assertable::assert_values(asfr_data, colnames="mean", test="gte", test_val=0)

  # prep TMB matrix
  asfr_matrix <- format_as_matrix(asfr_data[between(age_group_years_start, 10, 54)], "mean", ages = 10:54)


  # Prep srb data -----------------------------------------------------------

  srb_data <- all_srb_data[location_id == loc_id & between(year_id, year_start, year_end)]
  srb_data <- average_adjacent_year_rates(srb_data, id_vars = c("location_id", "year_id"))

  # format final data
  setcolorder(srb_data, c("location_id", "year_id", "mean"))
  setkeyv(srb_data, c("location_id", "year_id"))

  # check data
  id_vars <- list(location_id = loc_id, year_id = year_start:(year_end - 1))
  assertable::assert_ids(srb_data, id_vars)
  assertable::assert_values(srb_data, colnames="mean", test="gte", test_val=0)

  # prep TMB matrix
  srb_matrix <- matrix(srb_data$mean, ncol = length(srb_data$year_id))
  colnames(srb_matrix) <- srb_data$year_id


  # Prep prior for completeness ---------------------------------------------

  completeness_years <- year_start
  completeness_ages <- 0
  completeness_prior_value <- 0.8
  completeness_prior <- CJ(location_id = loc_id, year_id = completeness_years, sex_id = 1:2,
                           age_group_years_start = completeness_ages, mean = completeness_prior_value)
  setkeyv(completeness_prior, c("location_id", "year_id", "sex_id", "age_group_years_start"))
  completeness_matrix <- format_as_matrix_by_sex(completeness_prior, "mean", ages = 0)

  # make matrix to help with completeness reformatting in tmb code when not estimating year specific completeness
  completeness_parameters_info <- data.table(year_id = unique(completeness_prior[, year_id]))
  completeness_parameters_info[, c_index := .I - 1]

  full_completeness_info <- data.table(year_id = year_start:(year_end - 1))
  full_completeness_info <- completeness_parameters_info[full_completeness_info, roll = T, on = "year_id"]
  full_completeness_info <- full_completeness_info[, list(full_c_index = year_id - min(year_id), c_index)]
  full_completeness_info <- as.matrix(full_completeness_info)


  # Prep migration age-sex pattern ------------------------------------------

  in_flow_total <- copy(eurostat_migration_data)

  in_flow_total[age_group_years_start > terminal_age, age_group_years_start := terminal_age]
  in_flow_total <- in_flow_total[!is.na(in_flow) & !is.na(out_flow) & sex_id %in% c(1, 2) & age_group_years_start > -1,
                                 list(in_flow = sum(in_flow)),
                                 by = c("sex_id", "age_group_years_start")]
  in_flow_total <- in_flow_total[, list(sex_id, age_group_years_start, proportion = in_flow / sum(in_flow))]


  # add in rows for ages with zero proportion
  zero_flow <- CJ(sex_id = 1:2, age_group_years_start = -1:terminal_age, proportion_zero = 0)
  in_flow_total <- merge(in_flow_total, zero_flow, by = c("sex_id", "age_group_years_start"), all = T)
  in_flow_total[is.na(proportion), proportion := proportion_zero]
  in_flow_total[, proportion_zero := NULL]

  # rescale to sum to one
  in_flow_total[, proportion := proportion / sum(proportion)]

  # format final data
  setcolorder(in_flow_total, c("sex_id", "age_group_years_start", "proportion"))
  setkeyv(in_flow_total, c("sex_id", "age_group_years_start"))

  # check data
  id_vars <- list(sex_id = 1:2, age_group_years_start = -1:terminal_age)
  assertable::assert_ids(in_flow_total, id_vars)
  assertable::assert_values(in_flow_total, colnames = "mean", test = "gte", test_val = 0)


  # Determine what trims to run based on input data -------------------------

  run_method_trims <- method_trim_rankings[end_age_trim <= terminal_age & aplus == T,
                                           list(start_age_trim = start_age_trim[1], end_age_trim = end_age_trim[1],
                                                full_method = full_method[1]),
                                           by = c("method", "aplus", "fix_migration")]

  completeness <- lapply(1:nrow(run_method_trims), function(i) {

    # run for age-trim, aplus and fix_migration combination
    aplus <- run_method_trims[i, aplus]
    fix_migration <- run_method_trims[i, fix_migration]
    start_age_trim <- run_method_trims[i, start_age_trim]
    end_age_trim <- run_method_trims[i, end_age_trim]
    full_method <- run_method_trims[i, full_method]

    print(paste0("CCMP method. Aplus: ", aplus, ", fix_migration: ", fix_migration, ", ", start_age_trim, "-", end_age_trim))


    # Prep census data for specified trims ------------------------------------

    census_data_test_case <- copy(census_data)

    # mark certain age groups to be dropped from the model (age trimming)
    census_data_test_case[age_group_years_start < start_age_trim | age_group_years_start >= end_age_trim, drop := T]

    # calculate aplus age groups
    if (aplus) {
      baseline_data_test_case <- census_data_test_case[year_id == year_start, list(location_id, year_id, sex_id, drop, age_group_years_start, age_group_years_end, n, mean)]
      census_data_test_case <- census_data_test_case[year_id != year_start, list(drop, age_group_years_start, age_group_years_end = age_group_table_terminal_age,
                                                                                 n = 0, mean = rev(cumsum(rev(mean)))),
                                                     by = c("location_id", "year_id", "sex_id")]
      census_data_test_case <- rbind(baseline_data_test_case, census_data_test_case, use.names = T)
    }

    # convert to matrices for TMB
    baseline_matrix <- format_value_matrix(census_data_test_case[year_id == year_start],
                                           use_age_int = age_int, use_terminal_age = terminal_age,
                                           id_vars = c("location_id", "simulation", "year_id", "sex_id"), year_start = year_start, baseline = T)
    census_matrix <- format_value_matrix(census_data_test_case[year_id > year_start & !drop],
                                         use_age_int = age_int, use_terminal_age = terminal_age,
                                         id_vars = c("location_id", "simulation", "year_id", "sex_id"), year_start = year_start)
    census_ages_matrix <- format_row_matrix(census_data_test_case[year_id > year_start & !drop],
                                            use_age_int = age_int, use_terminal_age = terminal_age,
                                            id_vars = c("location_id", "simulation", "year_id", "sex_id"), year_start = year_start)


    # Prep TMB inputs ---------------------------------------------------------

    row_fert_start <- asfr_data[mean != 0, min(age_group_years_start)] + 1
    row_fert_end <- asfr_data[mean != 0, max(age_group_years_start)] + 1
    tmb_data <- list(
      input_n_female = census_matrix$female,
      input_n_male = census_matrix$male,
      input_n_ages_female = census_ages_matrix$female,
      input_n_ages_male = census_ages_matrix$male,

      input_n0_female = baseline_matrix$female,
      input_n0_male = baseline_matrix$male,
      input_mx_female = mx_matrix$female,
      input_mx_male = mx_matrix$male,
      input_logit_c_female = boot::logit(completeness_matrix$female),
      input_logit_c_male = boot::logit(completeness_matrix$male),
      full_completeness_info = full_completeness_info,
      input_net_flow_pattern_female = as.matrix(in_flow_total[sex_id == 2, list(proportion)]),
      input_net_flow_pattern_male = as.matrix(in_flow_total[sex_id == 1, list(proportion)]),
      input_f = asfr_matrix,
      input_srb = srb_matrix,

      terminal_age = terminal_age,
      age_int = age_int,
      row_fert_start = row_fert_start - 1,
      row_fert_end = row_fert_end - 1,

      mu_c = log(1),
      sig_c = 0.1,

      rho_c_t_mu = 5,
      rho_c_t_sig = 0.1,
      rho_c_a_mu = 0,
      rho_c_a_sig = 0.1,

      c_upper = 1,
      c_lower = 0
    )

    tmb_par <- list(
      logit_c_female = scaled_logit(completeness_matrix$female, tmb_data$c_upper, tmb_data$c_lower),
      logit_c_male = scaled_logit(completeness_matrix$male, tmb_data$c_upper, tmb_data$c_lower),
      log_sigma_c = log(1),
      logit_rho_c_t = 0,
      logit_rho_c_a = 0,
      total_net_flow = 0
    )

    # check if fixing certain parameters
    map <- NULL
    map$logit_rho_c_a <- factor(NA)
    if (fix_migration) {
      map$total_net_flow <- factor(NA)
    }


    # Run TMB models ----------------------------------------------------------

    # make objective function
    cat("\n\n***** Make objective function\n"); flush.console()
    obj <- TMB::MakeADFun(tmb_data, tmb_par, map=map, DLL="ccmp_model")

    # optimize objective function
    cat("\n\n***** Optimize objective function\n"); flush.console()

    # first try nlminb optimizer since most locations fit with this and is a commonly used optimzer
    fit_model <- function(timelimit) {
      setTimeLimit(elapsed = timelimit)
      nlminb(start=obj$par, objective=obj$fn, gradient=obj$gr, control=list(eval.max=1e5, iter.max=1e5))
    }
    opt_time <- proc.time()
    opt <- try(fit_model(30))
    (proc.time() - opt_time)


    # Extract estimated completeness values -----------------------------------

    # set default in case ccmp did not work
    c_mean <- CJ(location_id = loc_id, year_id = completeness_years, sex_id = 1:2, age_group_years_start = completeness_ages, mean = NA)

    if (class(opt) != "try-error") {
      # only produce outputs if the model converged
      if (opt$convergence == 0) {
        # get standard errors
        cat("\n\n***** Extract standard errors\n"); flush.console()
        se_time <- proc.time()
        out <- TMB::sdreport(obj, getJointPrecision=T, getReportCovariance = T)
        (se_time <- proc.time() - se_time)

        # Extract model outputs for ccmpp -----------------------------------------

        # function to extract means from the TMB fit
        extract_mean <- function(mean_vector, variable_name, years, ages = NULL) {
          # get the rows of the matrix that correspond to the variable of interest
          var_mean <- data.table(mean = mean_vector[names(mean_vector) == variable_name])
          var_mean[, year_id := rep(years, each=ifelse(!is.null(ages), length(ages), 1))]
          if (!is.null(ages)) var_mean[, age_group_years_start := ages]
          return(var_mean)
        }

        # extract mean and covariance matrix for all model parameters
        mean <- out$par.fixed

        ## extract completeness mean
        c_female_mean <- extract_mean(mean, "logit_c_female", years = completeness_years, ages = completeness_ages)
        c_male_mean <- extract_mean(mean, "logit_c_male", years = completeness_years, ages = completeness_ages)
        c_mean <- rbind(c_male_mean[, list(location_id = loc_id, year_id, sex_id = 1L, age_group_years_start, mean = inv_scaled_logit(mean, tmb_data$c_upper, tmb_data$c_lower))],
                        c_female_mean[, list(location_id = loc_id, year_id, sex_id = 2L, age_group_years_start, mean = inv_scaled_logit(mean, tmb_data$c_upper, tmb_data$c_lower))])

        ## extract migration totals
        if (fix_migration) {
          migration_totals <- data.table(location_id = loc_id, variable = c("net_flow"),
                                         value = c(tmb_par$total_net_flow))
        } else {
          migration_totals <- data.table(location_id = loc_id, variable = c("net_flow"),
                                         value = c(mean[names(mean) == "total_net_flow"]))
        }
      } else {
        print("ccmp did not converge or timed out")
      }
    } else {
      print("ccmp did not converge or timed out")
    }

    c_mean[, method := full_method]
    setcolorder(c_mean, c("location_id", "year_id", "sex_id", "age_group_years_start", "method", "mean"))
    return(c_mean)
  })
  completeness <- rbindlist(completeness)

  # merge on original id variables and format the same as other DDM scripts
  methods_used <- unique(completeness$method)
  completeness <- dcast(completeness, location_id + sex_id ~ method, value.var = "mean")
  completeness <- merge(unique(combined_pop_vr_subset[, use_id_vars, with = F]), completeness, by = c("location_id", "sex_id"))
  completeness <- completeness[, c(final_id_vars, methods_used), with = F]
  return(completeness)
}

total_time <- proc.time()
all_completeness <- mclapply(1:nrow(all_pairs), all_comp_func, mc.cores = 20)

# Check for errors
if (any(sapply(all_completeness, class) == "try-error")) {

  print(all_completeness)
  stop("You had issues in your mclapply, check the output log for errors")

}

all_completeness <- rbindlist(all_completeness)
(total_time <- proc.time() - total_time)
print(total_time)

assertthat::assert_that(nrow(all_completeness[!is.na(CCMP_aplus_migration)]) > 0)
assertthat::assert_that(nrow(all_completeness[!is.na(CCMP_aplus_no_migration)]) > 0)


# this final file is formatted the same as d05_formatted_ddm.dta
readstata13::save.dta13(all_completeness, paste0("FILEPATH"))
