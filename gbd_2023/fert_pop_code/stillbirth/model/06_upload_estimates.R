####################################################################
##                                                                ##
## Purpose: Upload stillbirth estimates to the mortality database ##
##                                                                ##
####################################################################

rm(list = ls())
Sys.umask(mode = "0002")

library(data.table)
library(readr)

library(mortcore)
library(mortdb)

user <- "USERNAME"

# Get settings
args <- commandArgs(trailingOnly = TRUE)
new_settings_dir <- args[1]

if (interactive()) {
  version_estimate <- "Run id"
  main_std_def <- "20_weeks"
  new_settings_dir <- paste0("FILEPATH/new_run_settings_", main_std_def, ".csv")
}

load(new_settings_dir)
list2env(new_settings, envir = environment())

# Reformat definition files
final_estimate <- data.table()

for (i in 1:length(defs)) {

  results_def <- fread(
    paste0(estimate_dir, version_estimate, "/FILEPATH/all_location_stillbirth_results_", defs[i], "_weeks.csv")
  )
  results_def[, wk_cutoff := defs[i]]

  sb_def <- results_def[, c("year_id", "location_id", "sb_mean", "sb_lower", "sb_upper", "wk_cutoff")]
  sb_def[, dem_measure_id := 7]
  setnames(sb_def, c("sb_mean", "sb_lower", "sb_upper"), c("mean", "lower", "upper"))

  sbr_def <- results_def[, c("year_id", "location_id", "sbr_mean", "sbr_lower", "sbr_upper", "wk_cutoff")]
  sbr_def[, dem_measure_id := 6]
  setnames(sbr_def, c("sbr_mean", "sbr_lower", "sbr_upper"), c("mean", "lower", "upper"))

  reformatted_def <- rbind(sb_def, sbr_def, use.names = TRUE)

  final_estimate <- rbind(final_estimate, reformatted_def)

}

# Clean file
final_estimate[, age_group_id := 169]
final_estimate[, sex_id := 3]

final_estimate <- final_estimate[, c("year_id", "location_id", "age_group_id", "sex_id", "dem_measure_id", "wk_cutoff", "mean", "lower", "upper")]

readr::write_csv(
  final_estimate,
  paste0(estimate_dir, version_estimate, "/FILEPATH/stillbirths_estimates_database.csv")
)

# Assert that cumulative stillbirths are monotonically decreasing
sbr_test <- final_estimate[dem_measure_id == 6, c("wk_cutoff", "location_id", "year_id", "mean")]
sbr_test[, mean := mean * 1000]

sbr_test_wide <- dcast(sbr_test, location_id + year_id ~ wk_cutoff, value.var = "mean")

bad_sbr <- sbr_test_wide[! (`20` > `22` | `22` > `24` | `24` > `26` | `26` > `28`)]
assertable::assert_nrows(bad_sbr, 0)

sb_test <- final_estimate[dem_measure_id == 6, c("wk_cutoff", "location_id", "year_id", "mean")]

sb_test_wide <- dcast(sb_test, location_id + year_id ~ wk_cutoff, value.var = "mean")

bad_sb <- sb_test_wide[! (`20` > `22` | `22` > `24` | `24` > `26` | `26` > `28`)]
assertable::assert_nrows(bad_sb, 0)

# Archive outlier set
mortdb::archive_outlier_set("stillbirth", version_estimate)
