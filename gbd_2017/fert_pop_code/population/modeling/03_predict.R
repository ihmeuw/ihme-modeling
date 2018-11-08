library(data.table)
library(boot)
library(mvnfast)
library(Matrix)
library(parallel)
library(Hmisc)
library(assertable)

rm(list=ls())
set.seed(98121)

main_dir <- commandArgs(trailingOnly=T)[1]
ihme_loc <- commandArgs(trailingOnly=T)[2]
run <- as.integer(commandArgs(trailingOnly=T)[3])

source("settings.R")
get_settings(main_dir)

source("modeling/helper_functions.R")

location_hierarchy <- fread(paste0(temp_dir, '/../../../database/location_hierarchy.csv'))[ihme_loc_id == ihme_loc]
national_location <- location_hierarchy[, level == 3 | ihme_loc_id %in% c("CHN_354", "CHN_361", "CHN_44533", "GBR_433", "GBR_434", "GBR_4636", "GBR_4749")]

# Parse tmb model output --------------------------------------------------

# load model fit output
load(paste0(temp_dir, "/model/run_", run, "/fit/model_fit.rdata"))

# extract mean and covariance matrix for all model parameters
mean <- out$value
cov <- out$cov

# determine which hyperprior parameters were fit
hyperprior_parameters <- names(out$par.fixed)

rm(out, opt)


# Extract model outputs for ccmpp -----------------------------------------

# function to extract means from the TMB fit
extract_mean <- function(mean_vector, variable_name, years, ages = NULL) {
  # get the rows of the matrix that correspond to the variable of interest
  var_mean <- data.table(value_mean = mean_vector[names(mean_vector) == variable_name])
  var_mean[, year_id := rep(years, each=ifelse(!is.null(ages), length(ages), 1))]
  if (!is.null(ages)) var_mean[, age := ages]
  return(var_mean)
}

# extract mean and covariance matrix for all model parameters
mean <- out$value
# determine which hyperprior parameters were fit
hyperprior_parameters <- names(out$par.fixed)
rm(out)

# extract hyperprior mean
hyperprior_mean <- data.table(value_mean = mean[names(mean) %in% hyperprior_parameters])
hyperprior_mean[, parameter := hyperprior_parameters]
hyperprior_mean[, ihme_loc_id := ihme_loc]

# extract correlation mean
rho_mean <- hyperprior_mean[grepl("logit_rho", parameter)]
rho_mean[, value_mean := boot::inv.logit(value_mean)]
# map correlation parameter names to better names
rho_parameter_names <- data.table(parameter = c("logit_rho_g_t", "logit_rho_g_a"),
                                  parameter_name = c("rho_migration_time", "rho_migration_age"))
rho_mean <- merge(rho_mean, rho_parameter_names, all.x=T, by = "parameter")
rho_mean <- rho_mean[, list(ihme_loc_id, parameter = parameter_name, value_mean)]

# extract standard_deviation mean
sd_mean <- hyperprior_mean[grepl("log_sigma", parameter)]
sd_mean[, value_mean := exp(value_mean)]
# map standard_deviation parameter names to better names
sd_parameter_names <- data.table(parameter = c("log_sigma_n", "log_sigma_n0", "log_sigma_s", "log_sigma_g", "log_sigma_f", "log_sigma_srb"),
                                 parameter_name = c("census", "baseline", "survival", "migration", "fertility", "srb"))
sd_parameter_names[, parameter_name := paste0("sd_", parameter_name)]
sd_mean <- merge(sd_mean, sd_parameter_names, all.x=T, by = "parameter")
sd_mean <- sd_mean[, list(ihme_loc_id, parameter = parameter_name, value_mean)]

hyperprior_mean <- rbind(sd_mean, rho_mean, use.names = T)
setkeyv(hyperprior_mean, c("ihme_loc_id", "parameter"))

## extract baseline population mean
n0_female_mean <- extract_mean(mean, "log_n0_female", years = year_start, ages = seq(0, terminal_age, age_int))
n0_male_mean <- extract_mean(mean, "log_n0_male", years = year_start, ages = seq(0, terminal_age, age_int))
n0_mean <- rbind(n0_male_mean[, list(ihme_loc_id = ihme_loc, year_id, sex_id = 1L,
                                     age, value_mean = exp(value_mean))],
                 n0_female_mean[, list(ihme_loc_id = ihme_loc, year_id, sex_id = 2L,
                                       age, value_mean = exp(value_mean))])
setkeyv(n0_mean, c("ihme_loc_id", "year_id", "sex_id", "age"))
rm(n0_female_mean, n0_male_mean)

## extract survival ratio mean
s_female_mean <- extract_mean(mean, "logit_s_female", years = seq(year_start, year_end, age_int),
                              ages = seq(-age_int, terminal_age, age_int))
s_male_mean <- extract_mean(mean, "logit_s_male", years = seq(year_start, year_end, age_int),
                            ages = seq(-age_int, terminal_age, age_int))
s_mean <- rbind(s_male_mean[, list(ihme_loc_id = ihme_loc, year_id, sex_id = 1L, age, value_mean = boot::inv.logit(value_mean))],
                s_female_mean[, list(ihme_loc_id = ihme_loc, year_id, sex_id=2L, age, value_mean = boot::inv.logit(value_mean))])
setkeyv(s_mean, c("ihme_loc_id", "year_id", "sex_id", "age"))
rm(s_female_mean, s_male_mean)

## extract net migration proportion mean
g_female_mean <- extract_mean(mean, "g_female", years = seq(year_start, year_end, age_int),
                              ages = seq(-age_int, terminal_age, age_int))
g_male_mean <- extract_mean(mean, "g_male", years = seq(year_start, year_end, age_int),
                            ages = seq(-age_int, terminal_age, age_int))
g_mean <- rbind(g_male_mean[, list(ihme_loc_id = ihme_loc, year_id, sex_id = 1L, age, value_mean)],
                g_female_mean[, list(ihme_loc_id = ihme_loc, year_id, sex_id = 2L, age, value_mean)])
setkeyv(g_mean, c("ihme_loc_id", "year_id", "sex_id", "age"))
rm(g_female_mean, g_male_mean)

## extract age specific fertility mean
fert_ages <- as.integer(rownames(tmb_data$input_log_f))
f_mean <- extract_mean(mean, "log_f", years = seq(year_start, year_end, age_int), ages = fert_ages)
f_mean <- f_mean[, list(ihme_loc_id = ihme_loc, year_id, age, value_mean = exp(value_mean))]

# in order to use the ccmpp function, need to add on values for all ages
fert_zero_ages <- setdiff(unique(n0_mean$age), unique(f_mean$age))
if (length(fert_zero_ages > 0)) {
  add_fert <- CJ(ihme_loc_id = unique(f_mean$ihme_loc_id), year_id = unique(f_mean$year_id),
                 age = fert_zero_ages, value_mean = 0)
  f_mean <- rbind(f_mean, add_fert, fill=T)
  setkeyv(f_mean, c("ihme_loc_id", "year_id", "age"))
}
setkeyv(f_mean, c("ihme_loc_id", "year_id", "age"))
rm(fert_zero_ages)

## extract sex ratio at birth mean
srb_mean <- extract_mean(mean, "log_srb", years = seq(year_start, year_end, age_int))
srb_mean <- srb_mean[, list(ihme_loc_id = ihme_loc, year_id, value_mean = exp(value_mean))]
setkeyv(srb_mean, c("ihme_loc_id", "year_id"))
rm(mean); gc()


# Run ccmpp to get unraked population -------------------------------------

counts <- ccmpp_female_male(n0_mean, s_mean, g_mean, f_mean, srb_mean,
                            migration_type = "proportion",
                            years = seq(year_start, year_end, age_int), age_int = age_int,
                            value_name = "value_mean")
counts[, ihme_loc_id := ihme_loc]
setcolorder(counts, c("ihme_loc_id", "year_id", "sex_id", "age", "variable", "value_mean"))
setkeyv(counts, c("ihme_loc_id", "year_id", "sex_id", "age", "variable"))

# parse out count draws into separate objects
pop <- counts[variable == "population", list(ihme_loc_id, year_id, sex_id, age, value_mean)]
deaths_cohort <- counts[variable == "deaths", list(ihme_loc_id, year_id, sex_id, age, value_mean)]
births <- counts[variable == "births", list(ihme_loc_id, year_id, sex_id, age, value_mean)]
net_migrants_cohort <- counts[variable == "net_migrants", list(ihme_loc_id, year_id, sex_id, age, value_mean)]

# create period counts
deaths <- cohort_to_period_counts(deaths_cohort,
                                  id_vars = c("ihme_loc_id", "year_id", "sex_id", "age"),
                                  use_age_int = age_int, value_name = "value_mean")
net_migrants <- cohort_to_period_counts(net_migrants_cohort,
                                        id_vars = c("ihme_loc_id", "year_id", "sex_id", "age"),
                                        use_age_int = age_int, value_name = "value_mean")


# Split under 1 populations -----------------------------------------------

if (age_int == 1) {
  under1_pop_proportions_dir <- "FILEPATH"
  under1_pop <- fread(paste0(under1_pop_proportions_dir, loc_id, ".csv"))

  under1_age_groups <- data.table(age_group_id = c(2, 3, 4), age_group = c("enn", "lnn", "pnn"))
  under1_pop <- under1_pop[age_group %in% c("enn", "lnn", "pnn")]
  under1_pop <- merge(under1_pop, under1_age_groups, by = c("age_group"), all.x = T)

  # split under five age groups using gbd under 5 population process proportions
  under1_pop <- under1_pop[, list(ihme_loc_id, year_id, sex_id, age_group_id, population = mean)]
  under1_pop <- under1_pop[, prop := population/sum(population), by = c("ihme_loc_id", "year_id", "sex_id")]

  under1_pop <- merge(under1_pop, counts[variable == "population" & age == 0], by = c("ihme_loc_id", "year_id", "sex_id"), allow.cartesian = T)
  under1_pop[, value_mean := prop * value_mean]
  under1_pop[, c("population", "prop", "age", "variable") := NULL]
  setcolorder(under1_pop, c("ihme_loc_id", "year_id", "sex_id", "age_group_id", "value_mean"))
  setkeyv(under1_pop, c("ihme_loc_id", "year_id", "sex_id", "age_group_id"))
}


# Check outputs -----------------------------------------------------------
pop_id_vars <- list(ihme_loc_id = ihme_loc,
                    year_id = seq(min(years), max(years) + age_int, age_int),
                    sex_id = 1:2, age = seq(0, terminal_age, age_int))
births_id_vars <- list(ihme_loc_id = ihme_loc,
                       year_id = seq(min(years), max(years), age_int),
                       sex_id = 1:2, age = seq(0, terminal_age, age_int))
deaths_net_migrants_cohort_id_vars <- list(ihme_loc_id = ihme_loc,
                                           year_id = seq(min(years), max(years), age_int),
                                           sex_id = 1:2, age = seq(-age_int, terminal_age, age_int))
deaths_net_migrants_id_vars <- list(ihme_loc_id = ihme_loc,
                                    year_id = seq(min(years), max(years), age_int),
                                    sex_id = 1:2, age = seq(0, terminal_age, age_int))

assert_ids(pop, pop_id_vars)
assert_ids(births, births_id_vars)
assert_ids(deaths_cohort, deaths_net_migrants_cohort_id_vars)
assert_ids(net_migrants_cohort, deaths_net_migrants_cohort_id_vars)
assert_ids(deaths, deaths_net_migrants_id_vars)
assert_ids(net_migrants, deaths_net_migrants_id_vars)

assert_values(pop, colnames = "value_mean", test = "gte", test_val = 0)
assert_values(births, colnames = "value_mean", test = "gte", test_val = 0)
assert_values(deaths_cohort, colnames = "value_mean", test = "gte", test_val = 0)
assert_values(deaths, colnames = "value_mean", test = "gte", test_val = 0)
assert_values(net_migrants_cohort, colnames = "value_mean", test = "not_na")
assert_values(net_migrants, colnames = "value_mean", test = "not_na")


# Save all outputs --------------------------------------------------------

# save everything directly modeled
save_outputs(n0_mean, fpath = paste0(temp_dir, "/model/run_", run, "/results/uncorrected/modeled/n0"),
             id_vars = c("ihme_loc_id", "year_id", "sex_id", "age"), draws_to_save = draws_produced)
# save_outputs(s_mean, fpath = paste0(temp_dir, "/model/run_", run, "/results/uncorrected/modeled/survival"),
#              id_vars = c("ihme_loc_id", "year_id", "sex_id", "age"), draws_to_save = draws_produced)
save_outputs(g_mean, fpath = paste0(temp_dir, "/model/run_", run, "/results/uncorrected/modeled/migration"),
             id_vars = c("ihme_loc_id", "year_id", "sex_id", "age"), draws_to_save = draws_produced)
# save_outputs(f_mean, fpath = paste0(temp_dir, "/model/run_", run, "/results/uncorrected/modeled/fertility"),
#              id_vars = c("ihme_loc_id", "year_id", "age"), draws_to_save = draws_produced)
save_outputs(srb_mean, fpath = paste0(temp_dir, "/model/run_", run, "/results/uncorrected/modeled/srb"),
             id_vars = c("ihme_loc_id", "year_id"), draws_to_save = draws_produced)
save_outputs(sd_mean, fpath = paste0(temp_dir, "/model/run_", run, "/results/uncorrected/modeled/standard_deviation"),
             id_vars = c("ihme_loc_id", "parameter"), draws_to_save = draws_produced)
if (model[run] == 4) {
  save_outputs(rho_mean, fpath = paste0(temp_dir, "/model/run_", run, "/results/uncorrected/modeled/correlation"),
               id_vars = c("ihme_loc_id", "parameter"), draws_to_save = draws_produced)
}

# save all counts
save_outputs(pop, fpath = paste0(temp_dir, "/model/run_", run, "/results/uncorrected/counts/population", ifelse(national_location, "_raked", "_unraked")),
             id_vars = c("ihme_loc_id", "year_id", "sex_id", "age"), draws_to_save = draws_produced)
save_outputs(births, fpath = paste0(temp_dir, "/model/run_", run, "/results/uncorrected/counts/births"),
             id_vars = c("ihme_loc_id", "year_id", "sex_id", "age"), draws_to_save = draws_produced)
# save_outputs(deaths_cohort, fpath = paste0(temp_dir, "/model/run_", run, "/results/uncorrected/counts/deaths_cohort"),
#              id_vars = c("ihme_loc_id", "year_id", "sex_id", "age"), draws_to_save = draws_produced)
save_outputs(deaths, fpath = paste0(temp_dir, "/model/run_", run, "/results/uncorrected/counts/deaths"),
             id_vars = c("ihme_loc_id", "year_id", "sex_id", "age"), draws_to_save = draws_produced)
save_outputs(net_migrants_cohort, fpath = paste0(temp_dir, "/model/run_", run, "/results/uncorrected/counts/net_migrants_cohort"),
             id_vars = c("ihme_loc_id", "year_id", "sex_id", "age"), draws_to_save = draws_produced)
save_outputs(net_migrants, fpath = paste0(temp_dir, "/model/run_", run, "/results/uncorrected/counts/net_migrants"),
             id_vars = c("ihme_loc_id", "year_id", "sex_id", "age"), draws_to_save = draws_produced)
