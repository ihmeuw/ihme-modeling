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

user <- Sys.getenv("USER")

# Get settings
args <- commandArgs(trailingOnly = TRUE)
new_settings_dir <- args[1]

if (interactive()) { # when interactive, only one definition can be run at a time
  version_estimate <- 999
  main_std_def <- "28_weeks"
  new_settings_dir <- "FILEPATH"
}

load(new_settings_dir)
list2env(new_settings, envir = environment())

# Reformat definition 1 file
results_def1 <- fread("FILEPATH")
results_def1[, wk_cutoff := defs[1]]

sb_def1 <- results_def1[, c("year_id", "location_id", "sb_mean", "sb_lower", "sb_upper", "wk_cutoff")]
sb_def1[, dem_measure_id := 7]
setnames(sb_def1, c("sb_mean", "sb_lower", "sb_upper"), c("mean", "lower", "upper"))

sbr_def1 <- results_def1[, c("year_id", "location_id", "sbr_mean", "sbr_lower", "sbr_upper", "wk_cutoff")]
sbr_def1[, dem_measure_id := 6]
setnames(sbr_def1, c("sbr_mean", "sbr_lower", "sbr_upper"), c("mean", "lower", "upper"))

reformatted_def1 <- rbind(sb_def1, sbr_def1, use.names = TRUE)

# Reformat definition 2 file
results_def2 <- fread("FILEPATH")
results_def2[, wk_cutoff := defs[2]]

sb_def2 <- results_def2[, c("year_id", "location_id", "sb_mean", "sb_lower", "sb_upper", "wk_cutoff")]
sb_def2[, dem_measure_id := 7]
setnames(sb_def2, c("sb_mean", "sb_lower", "sb_upper"), c("mean", "lower", "upper"))

sbr_def2 <- results_def2[, c("year_id", "location_id", "sbr_mean", "sbr_lower", "sbr_upper", "wk_cutoff")]
sbr_def2[, dem_measure_id := 6]
setnames(sbr_def2, c("sbr_mean", "sbr_lower", "sbr_upper"), c("mean", "lower", "upper"))

reformatted_def2 <- rbind(sb_def2, sbr_def2)

# Combine files
final_estimate <- rbind(reformatted_def1, reformatted_def2)

final_estimate[, age_group_id := 169]
final_estimate[, sex_id := 3]

final_estimate <- final_estimate[, c("year_id", "location_id", "age_group_id", "sex_id", "dem_measure_id", "wk_cutoff", "mean", "lower", "upper")]

readr::write_csv(
  final_estimate,
  "FILEPATH"
)

test20 <- final_estimate[wk_cutoff == 20 & dem_measure_id == 6, c("location_id", "year_id", "dem_measure_id", "mean")]
test28 <- final_estimate[wk_cutoff == 28 & dem_measure_id == 6, c("location_id", "year_id", "dem_measure_id", "mean")]

test20[, mean := mean * 1000]
test28[, mean := mean * 1000]

setnames(test20, "mean", "wks_20")
setnames(test28, "mean", "wks_28")

compare_20wks_28wks <- merge(
  test20,
  test28,
  by = c("location_id", "year_id", "dem_measure_id"),
  all = TRUE
)
bad_20wks_28wks <- compare_20wks_28wks[wks_28 >= wks_20 & dem_measure_id == 6]

assertable::assert_nrows(bad_20wks_28wks, 0)

# Archive outlier set
mortdb::archive_outlier_set("stillbirth", version_estimate)

## UPLOAD STILLBIRTH ESTIMATES

# Upload to the mortality database
if (!test_estimate) {
    mortdb::upload_results(
      filepath = "FILEPATH",
      model_name = "stillbirth",
      model_type = "estimate",
      run_id = version_estimate,
      hostname = hostname,
      check_data_drops = TRUE,
      send_slack = TRUE
    )

  # Change the model status to "best"
  if (best_estimate) {
      mortdb::update_status(
        model_name = "stillbirth",
        model_type = "estimate",
        run_id = version_estimate,
        hostname = hostname,
        new_status = "best",
        send_slack = TRUE
      )
  }
}
