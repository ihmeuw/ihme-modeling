################################################################################
# Description: Create draws of population from two sources of uncertainty.
# - determine how far away from a census each estimation year is.
# - merge on the predicted root mean squared error from the most recent knockout
#   run by only years away from closest census.
# - merge on predicted pes correction variance based on location-year sdi value.
# - combine variance from these two sources, create draws of percent error. Sort
#   values by year to create perfect correlation over time (needed for change
#   over time calculations).
# - create location independence by setting the seed to the location id,
#   maintain indepence after creating perfect temporal correlation by resorting
#   draw numbers.
# - use draws of percent error to create draws of population and rescale to the
#   original model fit population values in order to maintain ccmpp calculation
#   at the mean level.
################################################################################

library(data.table)
library(readr)
library(assertable)
library(mortdb, lib = "FILEPATH/r-pkg")
library(mortcore, lib = "FILEPATH/r-pkg")

rm(list = ls())
SGE_TASK_ID <- Sys.getenv("SGE_TASK_ID")
USER <- Sys.getenv("USER")
code_dir <- paste0("FILEATH", "/population/modeling/popReconstruct/")
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

# load model run specific settings into Global Environment
settings_dir <- paste0("FILEPATH", pop_vid, "/run_settings.rdata")
load(settings_dir)
list2env(settings, envir = environment())

source(paste0(code_dir, "functions/files.R"))
source(paste0(code_dir, "functions/formatting.R"))

age_groups <- fread(paste0(output_dir, "/database/age_groups.csv"))

# read in population modeling location hierarchy
location_hierarchy <- fread(paste0(output_dir, "/database/all_location_hierarchies.csv"))
location_hierarchy <- location_hierarchy[location_set_name == "population" & location_id == loc_id]
ihme_loc <- location_hierarchy[, ihme_loc_id]

# read in best drop age model version
drop_above_age <- fread(paste0(output_dir, "/versions_best.csv"))[ihme_loc_id == ihme_loc, drop_age]

draws_produced <- 0:(draws - 1)

set.seed(loc_id)

sdi <- fread(paste0(output_dir, "/database/sdi.csv"))
sdi <- sdi[location_id == loc_id, list(location_id, year_id, sdi = mean_value)]


# Load model fit population values ----------------------------------------

pop_fit <- fread(paste0(output_dir, loc_id, "/outputs/model_fit/counts_drop", drop_above_age, ".csv"))
pop_fit <- pop_fit[variable == "population", list(location_id, year_id, sex_id, age_group_id, mean)]


# Determine how far each estimation year is from a census -----------------

# data.table to merge everything on to
pop_draws <- data.table(location_id = loc_id, year_id = years)
census_data <- fread(file = paste0(output_dir, "/inputs/population.csv"))[location_id == loc_id]

# determine the year of the previous census
censuses_used <- unique(census_data[outlier_type == "not outliered" & !grepl("GBD|backcalculated", source_name), list(location_id, year_id, previous_census_year = year_id)])
setkeyv(censuses_used, c("location_id", "year_id"))
pop_draws <- censuses_used[pop_draws, roll = +Inf]
pop_draws[, years_to_previous_census := year_id - previous_census_year]

# determine the year of the next census
censuses_used <- unique(census_data[outlier_type == "not outliered" & !grepl("GBD|backcalculated", source_name), list(location_id, year_id, next_census_year = year_id)])
setkeyv(censuses_used, c("location_id", "year_id"))
pop_draws <- censuses_used[pop_draws, roll = -Inf]
pop_draws[, years_to_next_census := next_census_year - year_id]

# determine year of closest census
pop_draws[, years_to_closest_census := pmin(years_to_previous_census, years_to_next_census, na.rm = T)]
pop_draws[, c("next_census_year", "previous_census_year",
              "years_to_previous_census", "years_to_next_census") := NULL]

# merge on record type
censuses_used <- unique(census_data[outlier_type == "not outliered" & !grepl("GBD|backcalculated", source_name), list(location_id, year_id, record_type)])
pop_draws <- merge(pop_draws, censuses_used, by = c("location_id", "year_id"), all.x = T)


# Merge on predicted rmse -------------------------------------------------

cv_rmse <- fread(paste0(output_dir, "/database/fit_predictions.csv"))
pop_draws <- merge(pop_draws, cv_rmse, by = "years_to_closest_census", all.x = T)

# make the rmse zero in registry years
pop_draws[grepl("registry", record_type), pred_rmse := 0]


# Merge on predicted pes correction variance ------------------------------

pop_draws <- merge(pop_draws, sdi, by = c("location_id", "year_id"), all.x = T)
pop_draws[, sdi := plyr::round_any(sdi, 0.0001)]

pes_corrections <- fread(paste0("FILEPATH", census_processed_data_vid, "/outputs/08_pes_correction_variance.csv"))
pes_corrections <- pes_corrections[, list(sdi, pred_net_pes_correction_var)]
pop_draws <- merge(pop_draws, pes_corrections, by = "sdi", all.x = T)


# Make draws of the correction values -------------------------------------

pop_draws[, c("sdi", "years_to_closest_census") := NULL]
pop_draws[, pred_rmse_var := pred_rmse ^ 2]
pop_draws[, combined_var := pred_rmse_var + pred_net_pes_correction_var]
pop_draws[, combined_sd := sqrt(combined_var)]

# sort corrections for each year so that we enforce perfect correlation over time
pop_draws <- pop_draws[, list(draw = draws_produced,
                              correction = sort(rnorm(length(draws_produced), mean = 0, sd = combined_sd))),
                       by = c("location_id", "year_id")]

# then to preserve location independence, shift draw numbers around while preserving correlation over time
new_draws <- data.table(draw = draws_produced, new_draw = sample(draws_produced, length(draws_produced), replace = F))
pop_draws <- merge(pop_draws, new_draws, by = "draw", all = T)
pop_draws <- pop_draws[, list(location_id, year_id, draw = new_draw, correction)]
setkeyv(pop_draws, c("location_id", "year_id", "draw"))

pop_draws[, correction := correction / 100]


# Apply correction --------------------------------------------------------

pop_draws <- merge(pop_fit, pop_draws,
                   by = c("location_id", "year_id"), allow.cartesian = T)
pop_draws[, value := mean + (mean * correction)]
pop_draws <- pop_draws[, list(location_id, year_id, sex_id, age_group_id, draw, value)]


# Rake draws to original mean ---------------------------------------------

pop_draws <- merge(pop_draws, pop_fit, by = c("location_id", "year_id", "sex_id", "age_group_id"), all = T)
pop_draws[, new_mean := mean(value), by = c("location_id", "year_id", "sex_id", "age_group_id")]
pop_draws[, scaled_value := value * (mean / new_mean)]
pop_draws[, c("value", "mean", "new_mean") := NULL]
setnames(pop_draws, c("scaled_value"), c("value"))


# Make aggregate sex and age groups ---------------------------------------

# aggregate results to all reporting age groups and to both sexes combined
pop_reporting_draws <- mortcore::agg_results(pop_draws, id_vars = c("location_id", "year_id", "sex_id", "age_group_id", "draw"),
                                             value_vars = "value", gbd_year = gbd_year,
                                             agg_hierarchy = F, agg_sex = T, age_aggs = age_groups[(reporting), age_group_id])
pop_reporting_draws <- pop_reporting_draws[age_group_id %in% age_groups[(reporting), age_group_id]]


# Save results ------------------------------------------------------------

# format
setcolorder(pop_draws, c("location_id", "year_id", "sex_id", "age_group_id", "value"))
setkeyv(pop_draws, c("location_id", "year_id", "sex_id", "age_group_id"))
setcolorder(pop_reporting_draws, c("location_id", "year_id", "sex_id", "age_group_id", "value"))
setkeyv(pop_reporting_draws, c("location_id", "year_id", "sex_id", "age_group_id"))

# make sure only positive values
assertable::assert_values(pop_draws, colnames="value", test="gt", test_val=0)
assertable::assert_values(pop_reporting_draws, colnames="value", test="gt", test_val=0)

# check the most detailed age group draws
population_id_vars <- list(location_id = loc_id, year_id = years, sex_id = 1:2, age_group_id = age_groups[(most_detailed), age_group_id], draw = draws_produced)
assertable::assert_ids(pop_draws, population_id_vars)

# check the reporting age group draws
population_reporting_id_vars <- list(location_id = loc_id, year_id = years, sex_id = 1:3, age_group_id = age_groups[(reporting), age_group_id], draw = draws_produced)
assertable::assert_ids(pop_reporting_draws, population_reporting_id_vars)

# save summary and draw files indexed by draw number
save_outputs(pop_draws,
             fpath_summary = paste0(output_dir, loc_id, "/outputs/summary/population_unraked_ui.csv"),
             fpath_draws = paste0(output_dir, loc_id, "/outputs/draws/population_unraked_ui.h5"),
             id_vars = c("location_id", "year_id", "sex_id", "age_group_id"), by_var = "draw", value_var = "value")
save_outputs(pop_reporting_draws,
             fpath_summary = paste0(output_dir, loc_id, "/outputs/summary/population_reporting_unraked_ui.csv"),
             fpath_draws = paste0(output_dir, loc_id, "/outputs/draws/population_reporting_unraked_ui.h5"),
             id_vars = c("location_id", "year_id", "sex_id", "age_group_id"), by_var = "draw", value_var = "value")
readr::write_csv(data.table(test = 1), path = paste0(output_dir, loc_id, "/outputs/confirm_create_ui_completion.csv"))
