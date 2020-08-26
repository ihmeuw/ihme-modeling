################################################################################
# Description: Fit the population model for a given location and drop age
# - combine all demographic and population matrices and other information into
#   TMB data and parameter list
# - create map object to optionally fix different demographic components
# - fit model or load previously fit model
# - save tmb model fit object
# - extract model fit results
# - run prior and posterior ccmpp
# - split to get under 1 populations using gbd under5 mortality proportions
# - save outputs
# - calculate absolute and error and save
################################################################################

library(data.table)
library(readr)
library(assertable)
library(mortdb, lib = "FILEPATH/r-pkg")
library(mortcore, lib = "FILEPATH/r-pkg")

rm(list = ls())
SGE_TASK_ID <- Sys.getenv("SGE_TASK_ID")
USER <- Sys.getenv("USER")
code_dir <- paste0("FILEPATH", "/population/modeling/popReconstruct/")
Sys.umask(mode = "0002")


# Get arguments -----------------------------------------------------------

# load step specific settings into Global Environment
parser <- argparse::ArgumentParser()
parser$add_argument("--pop_vid", type = "character",
                    help = "The version number for this run of population, used to read in settings file")
parser$add_argument('--task_map_dir', type="character",
                    help="The filepath to the task map file that specifies other arguments to run for")
parser$add_argument("--test", type = "character",
                    help = "Whether this is a test run of the process")
args <- parser$parse_args()
if (interactive()) { # set interactive defaults, should never commit changes away from the test version to be safe
  args$pop_vid <- "99999"
  args$task_map_dir <- "FILEPATH"
  args$test <- "T"
}
args$test <- as.logical(args$test)
list2env(args, .GlobalEnv); rm(args)

# use task id to get arguments from task map
task_map <- fread(task_map_dir)
loc_id <- task_map[task_id == SGE_TASK_ID, location_id]
drop_above_age <- task_map[task_id == SGE_TASK_ID, drop_above_age]
copy_results <- task_map[task_id == SGE_TASK_ID, as.logical(copy_results)]

# load model run specific settings into Global Environment
settings_dir <- paste0("FILEPATH", pop_vid, "/run_settings.rdata")
load(settings_dir)
list2env(settings, envir = environment())

source(paste0(code_dir, "functions/ccmpp.R"))

# read in population modeling location hierarchy
location_hierarchy <- fread(paste0(output_dir, "/database/all_location_hierarchies.csv"))
location_hierarchy <- location_hierarchy[location_set_name == "population" & location_id == loc_id]
ihme_loc <- location_hierarchy[, ihme_loc_id]
super_region <- location_hierarchy[, super_region_name]

# read in age group
age_groups <- fread(paste0(output_dir, "/database/age_groups.csv"))

# under1 pop
under1_pop <- fread(paste0(output_dir, "/database/under1_pop.csv"))[location_id == loc_id]

ccmp_age_int <- 1
knockouts <- F

if (!copy_results) {

  # Load inputs -------------------------------------------------------------

  # load demographic data
  asfr_data <- f_mean <- fread(file = paste0(output_dir, "/inputs/asfr.csv"))[location_id == loc_id]
  srb_data <- srb_mean <- fread(file = paste0(output_dir, "/inputs/srb.csv"))[location_id == loc_id]
  survival_data <- s_mean <- fread(file = paste0(output_dir, "/inputs/survival.csv"))[location_id == loc_id]
  migration_data <- g_mean <- fread(file = paste0(output_dir, "/inputs/migration.csv"))[location_id == loc_id]

  # load census data
  census_data <- fread(file = paste0(output_dir, "/inputs/population.csv"))[location_id == loc_id]
  # drop age groups above the modeling pooling age except for the baseline year
  census_data[year_id != year_start & age_group_years_start > drop_above_age, drop := T]
  n0_mean <- census_data[outlier_type == "not outliered" & year_id == year_start,
                         list(location_id = loc_id, year_id, sex_id, age_group_years_start, mean)]

  # load model estimates and overwrite the input prior if the value was estimated in the model
  # save demographic input parameters if not fixed during model estimation
  if (!fix_baseline) n0_mean <- fread(paste0(output_dir, loc_id, "/outputs/model_fit/baseline_pop_drop", drop_above_age, ".csv"))
  if (!fix_migration) g_mean <- fread(paste0(output_dir, loc_id, "/outputs/model_fit/migration_proportion_drop", drop_above_age, ".csv"))
  if (!fix_fertility) f_mean <- fread(paste0(output_dir, loc_id, "/outputs/model_fit/asfr_drop", drop_above_age, ".csv"))
  if (!fix_survival) s_mean <- fread(paste0(output_dir, loc_id, "/outputs/model_fit/survival_ratio_drop", drop_above_age, ".csv"))
  if (!fix_srb) srb_mean <- fread(paste0(output_dir, loc_id, "/outputs/model_fit/srb_drop", drop_above_age, ".csv"))


  # Run initial inputs through ccmpp (prior) --------------------------------

  prior <- ccmpp_female_male(census_data[outlier_type == "not outliered" & year_id == year_start,
                                         list(location_id = loc_id, year_id, sex_id, age_group_years_start, mean)],
                             survival_data, migration_data,
                             asfr_data, srb_data,
                             years = years[c(-length(years))],
                             age_int = ccmp_age_int, value_name = "mean")
  prior <- prior[, list(location_id = loc_id, year_id, sex_id, age_group_years_start, variable, mean)]
  prior[variable == "deaths", variable := "deaths_cohort"]
  prior[variable == "net_migrants", variable := "net_migrants_cohort"]

  # create period counts
  prior_deaths <- cohort_to_period_counts(prior[variable == "deaths_cohort", list(location_id, year_id, sex_id, age_group_years_start, mean)],
                                          id_vars = c("location_id", "year_id", "sex_id", "age_group_years_start"),
                                          use_age_int = ccmp_age_int, value_name = "mean")
  prior_deaths[, variable := "deaths"]
  prior_net_migrants <- cohort_to_period_counts(prior[variable == "net_migrants_cohort", list(location_id, year_id, sex_id, age_group_years_start, mean)],
                                                id_vars = c("location_id", "year_id", "sex_id", "age_group_years_start"),
                                                use_age_int = ccmp_age_int, value_name = "mean")
  prior_net_migrants[, variable := "net_migrants"]

  prior <- rbind(prior, prior_deaths, prior_net_migrants, use.names = T)
  prior <- mortcore::age_start_to_age_group_id(prior, id_vars = c("location_id", "year_id", "sex_id", "variable"), keep_age_group_id_only = F)
  setcolorder(prior, c("location_id", "year_id", "sex_id", "age_group_id", "age_group_years_start",
                       "age_group_years_end", "variable", "mean"))
  setkeyv(prior, c("location_id", "year_id", "sex_id", "age_group_years_start", "variable"))


  # Run ccmpp to get unraked population -------------------------------------

  counts <- ccmpp_female_male(n0_mean, s_mean, g_mean, f_mean, srb_mean,
                              migration_type = "proportion",
                              years = years[-length(years)], age_int = ccmp_age_int,
                              value_name = "mean")
  counts[, location_id := loc_id]
  setcolorder(counts, c("location_id", "year_id", "sex_id", "age_group_years_start", "variable", "mean"))
  setkeyv(counts, c("location_id", "year_id", "sex_id", "age_group_years_start", "variable"))

  # create period counts
  deaths <- cohort_to_period_counts(counts[variable == "deaths_cohort", list(location_id, year_id, sex_id, age_group_years_start, mean)],
                                    id_vars = c("location_id", "year_id", "sex_id", "age_group_years_start"),
                                    use_age_int = ccmp_age_int, value_name = "mean")
  deaths[, variable := "deaths"]
  deaths[, age_group_years_start := as.integer(age_group_years_start)]
  net_migrants <- cohort_to_period_counts(counts[variable == "net_migrants_cohort", list(location_id, year_id, sex_id, age_group_years_start, mean)],
                                          id_vars = c("location_id", "year_id", "sex_id", "age_group_years_start"),
                                          use_age_int = ccmp_age_int, value_name = "mean")
  net_migrants[, variable := "net_migrants"]
  net_migrants[, age_group_years_start := as.integer(age_group_years_start)]

  counts <- rbind(counts, deaths, net_migrants, use.names = T)
  counts[, age_group_years_start := as.integer(age_group_years_start)]
  counts <- mortcore::age_start_to_age_group_id(counts, id_vars = c("location_id", "year_id", "sex_id", "variable"), keep_age_group_id_only = F)


  # Split under 1 populations -----------------------------------------------

  # split under five age groups using gbd under 5 population process proportions
  under1_pop <- under1_pop[, prop := mean/sum(mean), by = c("location_id", "year_id", "sex_id")]

  under1_pop <- merge(under1_pop, counts[variable == "population" & age_group_id == 28, list(location_id, year_id, sex_id, under1_mean = mean)],
                      by = c("location_id", "year_id", "sex_id"), allow.cartesian = T)
  under1_pop[, mean := prop * under1_mean]
  under1_pop[, c("prop", "under1_mean") := NULL]

  under1_pop <- merge(under1_pop, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], all.x = T, by = "age_group_id")
  under1_pop[, variable := "population"]

  counts <- rbind(counts, under1_pop, use.names = T)
  counts <- counts[!(variable == "population" & age_group_id == 28)]
  setcolorder(counts, c("location_id", "year_id", "sex_id", "age_group_id", "age_group_years_start",
                        "age_group_years_end", "variable", "mean"))
  setkeyv(counts, c("location_id", "year_id", "sex_id", "age_group_years_start", "variable"))


  # Aggregate migration estimates -------------------------------------------

  aggregate_migrants <- function(counts_to_aggregate) {

    # aggregate net migrant counts
    net_migrants_reporting <- mortcore::agg_results(counts_to_aggregate[variable == "net_migrants", list(location_id, year_id, sex_id, age_group_id, mean)],
                                                    id_vars = c("location_id", "year_id", "sex_id", "age_group_id"),
                                                    value_vars = "mean", agg_hierarchy = F, agg_sex = T, age_aggs = age_groups[(reporting_migration), age_group_id])
    # aggregate population
    pop_net_migrants_reporting <- mortcore::agg_results(counts_to_aggregate[variable == "population", list(location_id, year_id, sex_id, age_group_id, mean)],
                                                        id_vars = c("location_id", "year_id", "sex_id", "age_group_id"),
                                                        value_vars = "mean", agg_hierarchy = F, agg_sex = T, age_aggs = age_groups[(reporting_migration), age_group_id])
    pop_net_migrants_reporting <- pop_net_migrants_reporting[age_group_id %in% unique(net_migrants_reporting[, age_group_id])]
    setnames(pop_net_migrants_reporting, "mean", "pop")

    # merge together and calculate net migrant proportions
    net_migration_proportion_reporting <- merge(pop_net_migrants_reporting, net_migrants_reporting,
                                                by = c("location_id", "year_id", "sex_id", "age_group_id"), all = T)
    net_migration_proportion_reporting <- net_migration_proportion_reporting[, list(mean = mean / pop),
                                                                             by = c("location_id", "year_id", "sex_id", "age_group_id")]

    net_migrants_reporting[, measure_id := 19] # measure id 19 is for continuous counts
    net_migration_proportion_reporting[, measure_id := 18] # measure id 18 is for proportions
    net_migration <- rbind(net_migration_proportion_reporting, net_migrants_reporting)
    setcolorder(net_migration, c("location_id", "year_id", "sex_id", "age_group_id", "measure_id", "mean"))
    setkeyv(net_migration, c("location_id", "year_id", "sex_id", "age_group_id", "measure_id"))
    return(net_migration)
  }

  posterior_net_migration <- aggregate_migrants(counts)
  prior_net_migration <- aggregate_migrants(prior)


  # Calculate errors --------------------------------------------------------

  # aggregate populations in each census year to the census age groups
  # split the predicted population counts into the census age groups by year and sex
  census_data <- census_data[outlier_type %in% c("not outliered", if (knockouts) "knockout")]
  census_years <- unique(census_data$year_id)
  pop_census_ages <- lapply(census_years, function(year) {
    data <- census_data[year_id == year]
    census_age_groups <- unique(data$age_group_id)
    pop <- mortcore::agg_results(counts[variable == "population" & year_id == year, list(location_id, year_id, sex_id, age_group_id, mean)],
                                 id_vars = c("location_id", "year_id", "sex_id", "age_group_id"), value_vars = "mean", agg_hierarchy = F,
                                 age_aggs = census_age_groups)
    pop <- pop[age_group_id %in% census_age_groups]
    return(pop)
  })
  pop_census_ages <- rbindlist(pop_census_ages)

  # calculate percent and absolute difference between estimates and census data
  setnames(pop_census_ages, "mean", "estimate")
  setnames(census_data, c("mean"), c("data"))
  error <- merge(pop_census_ages, census_data, by = c("location_id", "year_id", "sex_id", "age_group_id"), all = T)
  error[, abs_error := (estimate - data)]
  error[, pct_error := ((estimate - data) / data) * 100]
  setkeyv(error, c("location_id", "year_id"))

  # determine the year of the previous census
  censuses_used <- unique(census_data[outlier_type == "not outliered", list(location_id, year_id, previous_census_year = year_id)])
  setkeyv(censuses_used, c("location_id", "year_id"))
  error <- censuses_used[error, roll = +Inf]
  error[, years_to_previous_census := year_id - previous_census_year]

  # determine the year of the next census
  censuses_used <- unique(census_data[outlier_type == "not outliered", list(location_id, year_id, next_census_year = year_id)])
  setkeyv(censuses_used, c("location_id", "year_id"))
  error <- censuses_used[error, roll = -Inf]
  error[, years_to_next_census := next_census_year - year_id]

  error[, c("next_census_year", "previous_census_year") := NULL]
  setcolorder(error, c(census_data_id_vars, "sex_id", "age_group_id", "age_group_years_start", "age_group_years_end", "n", "drop", "years_to_previous_census", "years_to_next_census", "estimate", "data", "abs_error", "pct_error"))
  setkeyv(error, c(census_data_id_vars, "sex_id", "age_group_id", "age_group_years_start", "age_group_years_end", "n", "drop",
                   "years_to_previous_census", "years_to_next_census"))


  # Check outputs -----------------------------------------------------------

  population_id_vars <- list(location_id = loc_id, year_id = years, sex_id = 1:2, age_group_id = age_groups[(most_detailed), age_group_id])
  assertable::assert_ids(counts[variable == "population"], population_id_vars)
  assertable::assert_values(counts[variable == "population"], colnames = "mean", test = "gt", test_val = 0)


  # Save outputs ------------------------------------------------------------

  # save net migration counts and proportions
  readr::write_csv(posterior_net_migration, path = paste0(output_dir, loc_id, "/outputs/model_fit/net_migration_posterior_drop", drop_above_age, ".csv"))
  readr::write_csv(prior_net_migration, path = paste0(output_dir, loc_id, "/outputs/model_fit/net_migration_prior_drop", drop_above_age, ".csv"))

  # save absolute and percent errors
  readr::write_csv(error, path = paste0(output_dir, loc_id, "/outputs/model_fit/errors_drop", drop_above_age, ".csv"))

  # save output derived counts of population, births, migrants, deaths
  readr::write_csv(prior, path = paste0(output_dir, loc_id, "/outputs/model_fit/counts_prior_drop", drop_above_age, ".csv"))
  readr::write_csv(counts, path = paste0(output_dir, loc_id, "/outputs/model_fit/counts_drop", drop_above_age, ".csv"))
} else {

  # Copy over outputs from previous version ---------------------------------

  # useful if testing just post model fit code and want to reduce run time from fitting the whole population model
  # or if just rerunning a subset of locations
  copy_output_dir <- "FILEPATH + copy_vid"
  file.copy(from = paste0(copy_output_dir, loc_id, "/outputs/model_fit/net_migration_posterior_drop", drop_above_age, ".csv"),
            to = paste0(output_dir, loc_id, "/outputs/model_fit/net_migration_posterior_drop", drop_above_age, ".csv"), overwrite = T)
  file.copy(from = paste0(copy_output_dir, loc_id, "/outputs/model_fit/net_migration_prior_drop", drop_above_age, ".csv"),
            to = paste0(output_dir, loc_id, "/outputs/model_fit/net_migration_prior_drop", drop_above_age, ".csv"), overwrite = T)
  file.copy(from = paste0(copy_output_dir, loc_id, "/outputs/model_fit/errors_drop", drop_above_age, ".csv"),
            to = paste0(output_dir, loc_id, "/outputs/model_fit/errors_drop", drop_above_age, ".csv"), overwrite = T)
  file.copy(from = paste0(copy_output_dir, loc_id, "/outputs/model_fit/counts_prior_drop", drop_above_age, ".csv"),
            to = paste0(output_dir, loc_id, "/outputs/model_fit/counts_prior_drop", drop_above_age, ".csv"), overwrite = T)
  file.copy(from = paste0(copy_output_dir, loc_id, "/outputs/model_fit/counts_drop", drop_above_age, ".csv"),
            to = paste0(output_dir, loc_id, "/outputs/model_fit/counts_drop", drop_above_age, ".csv"), overwrite = T)

}
