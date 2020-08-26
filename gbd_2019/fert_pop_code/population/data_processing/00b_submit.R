################################################################################
# Description: Submit all jobs needed for the census processing process.
################################################################################

library(data.table)
library(readr)
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
parser$add_argument("--resubmit", type = "character",
                    help = "Whether to check for output files before resubmitting a previous run")
args <- parser$parse_args()
if (interactive()) { # set interactive defaults, should never commit changes away from the test version to be safe
  args$pop_processing_vid <- "99999"
  args$test <- "T"
  args$resubmit <- "T"
}
args$test <- as.logical(args$test)
args$resubmit <- as.logical(args$resubmit)
list2env(args, .GlobalEnv); rm(args)

# load model run specific settings into Global Environment
settings_dir <- paste0("FILEPATH", pop_processing_vid, "/run_settings.rdata")
load(settings_dir)
list2env(settings, envir = environment())

location_hierarchy <- fread(paste0(output_dir, "/inputs/location_hierarchy.csv"))


# Submit jobs -------------------------------------------------------------

output_file_exists <- ifelse(resubmit, file.exists(paste0(output_dir, "/inputs/location_specific_settings.csv")), F)
if (any(!output_file_exists)) {
  mortcore::qsub(jobname = paste0("download_inputs_", pop_processing_vid),
               cores = 1, mem = 10, wallclock = "00:30:00", archive_node = T,
               queue = queue,
               proj = submission_project_name,
               shell = "FILEPATH + r_shell_singularity_version",
               code = paste0(code_dir, "01_download_inputs.R"),
               pass_argparse = list(pop_processing_vid = pop_processing_vid,
                                    test = test),
               sub = T)
}

output_file_exists <- ifelse(resubmit, file.exists(paste0(output_dir, "/outputs/02_distribute_unknown_sex_age.csv")), F)
if (any(!output_file_exists)) {
  mortcore::qsub(jobname = paste0("distribute_unknown_sex_age_", pop_processing_vid),
               cores = 1, mem = 2, wallclock = "00:05:00", archive_node = F,
               queue = queue,
               proj = submission_project_name,
               shell = "FILEPATH + r_shell_singularity_version",
               code = paste0(code_dir, "02_distribute_unknown_sex_age.R"),
               pass_argparse = list(pop_processing_vid = pop_processing_vid,
                                    test = test),
               hold = paste0("download_inputs_", pop_processing_vid),
               sub = T)
}

output_file_exists <- ifelse(resubmit, file.exists(paste0(output_dir, "/outputs/03_split_historical_subnationals.csv")), F)
if (any(!output_file_exists)) {
  mortcore::qsub(jobname = paste0("split_historical_subnationals_", pop_processing_vid),
               cores = 1, mem = 2, wallclock = "00:10:00", archive_node = F,
               queue = queue,
               proj = submission_project_name,
               shell = "FILEPATH + r_shell_singularity_version",
               code = paste0(code_dir, "03_split_historical_subnationals.R"),
               pass_argparse = list(pop_processing_vid = pop_processing_vid,
                                    test = test),
               hold = paste0("distribute_unknown_sex_age_", pop_processing_vid),
               sub = T)
}

output_file_exists <- ifelse(resubmit, file.exists(paste0(output_dir, "/diagnostics/subnational_aggregates.csv")), F)
if (any(!output_file_exists)) {
  mortcore::qsub(jobname = paste0("aggregate_subnationals_", pop_processing_vid),
               cores = 1, mem = 2, wallclock = "00:05:00", archive_node = F,
               queue = queue,
               proj = submission_project_name,
               shell = "FILEPATH + r_shell_singularity_version",
               code = paste0(code_dir, "04_aggregate_subnationals.R"),
               pass_argparse = list(pop_processing_vid = pop_processing_vid,
                                    test = test),
               hold = paste0("split_historical_subnationals_", pop_processing_vid),
               sub = T)
}

output_file_exists <- ifelse(resubmit, file.exists(paste0(output_dir, "/outputs/05_correct_age_heaping.csv")), F)
if (any(!output_file_exists)) {
  mortcore::qsub(jobname = paste0("correct_age_heaping_", pop_processing_vid),
               cores = 1, mem = 2, wallclock = "00:05:00", archive_node = F,
               queue = queue,
               proj = submission_project_name,
               shell = "FILEPATH + r_shell_singularity_version",
               code = paste0(code_dir, "05_correct_age_heaping.R"),
               pass_argparse = list(pop_processing_vid = pop_processing_vid,
                                    test = test),
               hold = paste0("aggregate_subnationals_", pop_processing_vid),
               sub = T)
}

output_file_exists <- ifelse(resubmit, file.exists(paste0(output_dir, "/outputs/06_make_manual_age_group_changes.csv")), F)
if (any(!output_file_exists)) {
  mortcore::qsub(jobname = paste0("make_manual_age_group_changes_", pop_processing_vid),
               cores = 1, mem = 2, wallclock = "00:05:00", archive_node = F,
               queue = queue,
               proj = submission_project_name,
               shell = "FILEPATH + r_shell_singularity_version",
               code = paste0(code_dir, "06_make_manual_age_group_changes.R"),
               pass_argparse = list(pop_processing_vid = pop_processing_vid,
                                    test = test),
               hold = paste0("correct_age_heaping_", pop_processing_vid),
               sub = T)
}

output_file_exists <- ifelse(resubmit, file.exists(paste0(output_dir, "/outputs/07_make_national_location_adjustments.csv")), F)
if (any(!output_file_exists)) {
  mortcore::qsub(jobname = paste0("make_national_location_adjustments_", pop_processing_vid),
               cores = 1, mem = 2, wallclock = "00:05:00", archive_node = F,
               queue = queue,
               proj = submission_project_name,
               shell = "FILEPATH + r_shell_singularity_version",
               code = paste0(code_dir, "07_make_national_location_adjustments.R"),
               pass_argparse = list(pop_processing_vid = pop_processing_vid,
                                    test = test),
               hold = paste0("make_manual_age_group_changes_", pop_processing_vid),
               sub = T)
}

output_file_exists <- ifelse(resubmit, file.exists(paste0(output_dir, "/outputs/08_pes_correction_variance.csv")), F)
if (any(!output_file_exists)) {
  mortcore::qsub(jobname = paste0("apply_pes_correction_", pop_processing_vid),
               cores = 1, mem = 2, wallclock = "00:05:00", archive_node = F,
               queue = queue,
               proj = submission_project_name,
               shell = "FILEPATH + r_shell_singularity_version",
               code = paste0(code_dir, "08_apply_pes_correction.R"),
               pass_argparse = list(pop_processing_vid = pop_processing_vid,
                                    test = test),
               hold = paste0("make_national_location_adjustments_", pop_processing_vid),
               sub = T)
}

output_file_exists <- ifelse(resubmit, file.exists(paste0(output_dir, "/outputs/09_generate_baseline.csv")), F)
if (any(!output_file_exists)) {
  mortcore::qsub(jobname = paste0("generate_baseline_", pop_processing_vid),
               cores = 5, mem = 30, wallclock = "00:20:00", archive_node = F,
               queue = queue,
               proj = submission_project_name,
               shell = "FILEPATH + r_shell_singularity_version",
               code = paste0(code_dir, "09_generate_baseline.R"),
               pass_argparse = list(pop_processing_vid = pop_processing_vid,
                                    test = test),
               hold = paste0("apply_pes_correction_", pop_processing_vid),
               sub = T)
}

## make diagnostic plots for each location-drop_above_age
task_map_07a <- location_hierarchy[is_estimate == 1, list(location_id, ihme_loc_id)]
if (resubmit) {
  # check whether final output file exists
  task_map_07a[, output_file := paste0(output_dir, "/diagnostics/location_specific/", ihme_loc_id, "_age_pattern_pop.pdf")]
  task_map_07a[, output_file_exists := file.exists(output_file)]

  # subset to location-ages that have not yet finished
  task_map_07a <- task_map_07a[!(output_file_exists)]
}

if (nrow(task_map_07a) > 0) {
  task_map_07a[, task_id := .I]
  time <- format(Sys.time(), "%Y_%m_%d_%H_%M_%S")
  task_map_dir <- paste0(output_dir, "task_map/07a_", time, ".csv")
  readr::write_csv(task_map_07a, task_map_dir)

  mortcore::array_qsub(jobname = paste0("generate_diagnostics_", pop_processing_vid),
                       cores = 1, mem = 4, wallclock = "00:30:00", archive_node = F,
                       queue = queue,
                       proj = submission_project_name,
                       shell = "FILEPATH + r_shell_singularity_version",
                       code = paste0(code_dir, "10a_generate_diagnostics.R"),
                       pass_argparse = list(pop_processing_vid = pop_processing_vid,
                                            task_map_dir = task_map_dir,
                                            test = test),
                       hold = paste0("generate_baseline_", pop_processing_vid),
                       num_tasks = nrow(task_map_07a),
                       log = T,
                       submit = T)
}

output_file_exists <- ifelse(resubmit, file.exists(paste0(sandbox_dir, "/index.html")), F)
if (any(!output_file_exists)) {
  mortcore::qsub(jobname = paste0("generate_sandbox_diagnostics_", pop_processing_vid),
               cores = 1, mem = 10, wallclock = "00:30:00", archive_node = F,
               queue = queue,
               proj = submission_project_name,
               shell = paste0(code_dir, "shells/python_shell.sh"),
               code = "FILEPATH",
               pass = list(ifelse(test, paste0("test/", pop_processing_vid), pop_processing_vid), comment),
               hold = paste0("generate_diagnostics_", pop_processing_vid),
               sub = T)
}

output_file_exists <- ifelse(resubmit, file.exists(paste0(output_dir, "/outputs/upload.csv")), F)
if (any(!output_file_exists)) {
  mortcore::qsub(jobname = paste0("upload_data_", pop_processing_vid),
                 cores = 1, mem = 10, wallclock = "00:30:00", archive_node = F,
                 queue = queue,
                 proj = submission_project_name,
                 shell = "FILEPATH + r_shell_singularity_version",
                 code = paste0(code_dir, "11_upload_data.R"),
                 pass_argparse = list(pop_processing_vid = pop_processing_vid,
                                      test = test),
                 hold = paste0("generate_baseline_", pop_processing_vid),
                 sub = T)
}

