################################################################################
# Description: Format data processing outputs and upload to mortality database.
# - compile together data processing stages and merge on ids
# - aggregate both sexes combined
# - check formatting of data
# - upload data
################################################################################

library(data.table)
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


# Load processed census data ----------------------------------------------

files <- c("01_raw.csv",
           "02_distribute_unknown_sex_age.csv",
           "03_split_historical_subnationals.csv",
           "04_aggregate_subnationals.csv",
           "05_correct_age_heaping.csv",
           "06_make_manual_age_group_changes.csv",
           "07_make_national_location_adjustments.csv",
           "08_apply_pes_correction.csv",
           "09_generate_baseline.csv")

census_data <- lapply(files, function(f) {
  data <- fread(paste0(output_dir, "/outputs/", f))
  return(data)
})
census_data <- rbindlist(census_data, use.names = T, fill = T)


# Final formatting --------------------------------------------------------

upload_data_stages <- c("national location adjustments", "baseline included")
census_data <- census_data[data_stage %in% upload_data_stages]

record_type_ids <- mortdb::get_mort_ids(type = "record_type")
census_data <- merge(census_data, record_type_ids, by = "record_type", all.x = T)
method_ids <- mortdb::get_mort_ids(type = "method")[, list(method_id, method_short)]
census_data <- merge(census_data, method_ids, by = "method_short", all.x = T)
pes_adjustment_type_ids <- mortdb::get_mort_ids(type = "pes_adjustment_type")
census_data <- merge(census_data, pes_adjustment_type_ids, by = "pes_adjustment_type", all.x = T)
data_stage_ids <- mortdb::get_mort_ids(type = "data_stage")
census_data <- merge(census_data, data_stage_ids, by = "data_stage", all.x = T)
outlier_type_ids <- mortdb::get_mort_ids(type = "outlier_type")
census_data <- merge(census_data, outlier_type_ids, by = "outlier_type", all.x = T)

# TODO: update this so that we keep the actual midpoint date from data prep
census_data[, census_midpoint_date := paste0(year_id,"-06-15")]

census_data[is.na(aggregate), aggregate := F]
census_data[, aggregate := as.integer(aggregate)]

census_data <- census_data[, list(location_id, year_id, sex_id, age_group_id,
                                  nid, underlying_nid, census_midpoint_date,
                                  record_type_id, method_id, pes_adjustment_type_id,
                                  data_stage_id, outlier_type_id,
                                  sub_agg = aggregate, source_name, mean, notes = NA)]

census_data <- mortcore::agg_results(census_data, id_vars = setdiff(colnames(census_data), "mean"),
                                     value_vars = "mean", agg_sex = T, agg_hierarchy = F, id_assertion = F)
setkeyv(census_data, setdiff(colnames(census_data), "mean"))


# Formatting checks -------------------------------------------------------

# test that all processed data is greater than zero
assertable::assert_values(census_data, colnames = "mean", test = "gt", test_val = 0)

# Check that no NAs in relevant id variables
assertable::assert_values(census_data, colnames = c("location_id", "year_id", "sex_id", "age_group_id",
                                                    "nid", "census_midpoint_date", "record_type_id",
                                                    "method_id", "pes_adjustment_type_id",
                                                    "data_stage_id", "outlier_type_id", "sub_agg", "source_name"),
                          test = "not_na")


# Upload census data to database ------------------------------------------

upload_dir <- paste0(output_dir, "/outputs/upload.csv")
readr::write_csv(census_data, paste0(output_dir, "/outputs/upload.csv"))

if (!test) {
  mortdb::upload_results(filepath = upload_dir,
                         model_name = "census processed",
                         model_type = "data",
                         run_id = pop_processing_vid,
                         check_data_drops = T,
                         hostname = hostname)

  if (best) {
    # Change the model status to "best"
    mortdb::update_status(model_name = "census processed",
                          model_type = "data",
                          run_id = pop_processing_vid,
                          new_status = "best",
                          new_comment = comment,
                          assert_parents = F,
                          hostname = hostname)
  }

  # compare to current rounds best upload
  comparison <- mortcore::compare_uploads(model_name = "census processed", model_type = "data",
                                          current_run_id = pop_processing_vid,
                                          comparison_run_id = pop_processing_current_round_vid,
                                          return_granular = T, send_slack = T)
  setkeyv(comparison, c("ihme_loc_id", "year_id", "sex_id", "age_group_id", "outlier_type_id", "nid", "underlying_nid", "data_stage_id"))
  readr::write_csv(comparison, paste0(output_dir, "/diagnostics/compare_uploads_current_round_best_", pop_processing_current_round_vid, ".csv"))

  # compare to previous rounds best upload
  comparison <- mortcore::compare_uploads(model_name = "census processed", model_type = "data",
                                          current_run_id = pop_processing_vid,
                                          comparison_run_id = pop_processing_previous_round_vid,
                                          return_granular = T, send_slack = T)
  setkeyv(comparison, c("ihme_loc_id", "year_id", "sex_id", "age_group_id", "outlier_type_id", "nid", "underlying_nid", "data_stage_id"))
  readr::write_csv(comparison, paste0(output_dir, "/diagnostics/compare_uploads_previous_round_best_", pop_processing_previous_round_vid, ".csv"))
}
