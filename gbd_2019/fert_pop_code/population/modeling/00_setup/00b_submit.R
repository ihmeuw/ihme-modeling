################################################################################
## Description: Submit all jobs needed for the population process.
################################################################################

library(data.table)
library(readr)
library(mortdb, lib = "FILEPATH/r-pkg")
library(mortcore, lib = "FILEPATH/r-pkg")

rm(list = ls())
USER <- Sys.getenv("USER")
code_dir <- paste0("FILEPATH", "/population/modeling/popReconstruct/")
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
settings_dir <- paste0("FILEPATH", pop_vid, "/run_settings.rdata")
load(settings_dir)
list2env(settings, envir = environment())

all_hierarchies <- fread(paste0(output_dir, "/database/all_location_hierarchies.csv"))
location_hierarchy <- all_hierarchies[location_set_name == "population"]
location_specific_settings <- fread(paste0(output_dir, "/database/location_specific_settings.csv"))

shell_dir <- "FILEPATH + r_shell_singularity_version"
hostnames_to_avoid <- NULL
submit_jobs <- T


# Helper functions --------------------------------------------------------

submit_with_resubs <- function(step_description, jobname, tasks, submission_function,
                               resubmission_attempts = default_resubmission_attempts,
                               slack_success_message = F, slack_error_message = T,
                               stop_submissions_on_error = T, ...) {
  output_files_exist <- F
  submission_attempt <- 1

  # check if step already completed
  check <- suppressWarnings(assertable::check_files(filenames = tasks[, output_file], warn_only = T))
  output_files_exist <- is.null(check)

  while(!output_files_exist & submission_attempt <= resubmission_attempts) {
    print(paste0("Submission attempt: ", submission_attempt))
    print(paste0("Step: ", step_description))

    # submit the job and then wait for job completion
    submission_function(jobname, ...)
    wait_for_job_completion(jobname)

    # check if all expected output files exist
    check <- suppressWarnings(assertable::check_files(filenames = tasks[, output_file], warn_only = T))
    output_files_exist <- is.null(check)
    num_files_missing <- ifelse(output_files_exist, 0, length(check))

    if (!output_files_exist & submission_attempt == resubmission_attempts & slack_error_message) {
      slack_broken_model_message(step_description, resubmission_attempts, num_files_missing, nrow(tasks))
      if (stop_submissions_on_error) stop()
    } else if (output_files_exist & slack_success_message) {
      slack_successful_model_step_message(step_description)
    }
    submission_attempt <- submission_attempt + 1
  }
  return(output_files_exist)
}


wait_for_job_completion <- function(query_jobname, sleep_time = 60, sleep_end = (60 * 48), warn_only = T) {

  sleep_end <- sleep_end * 60 # convert to seconds
  finished <- FALSE
  start_time <- Sys.time()
  current_time <- Sys.time()
  elapsed_time <- current_time - start_time

  while(!finished & elapsed_time < sleep_end) {

    current_time <- Sys.time()

    # parse currently submitted jobs job name
    jobs <- data.table(jobname = system("qstat -xml", intern = T))
    jobs <- jobs[grepl("<JB_name>", jobname)]
    jobs[, jobname := gsub("<JB_name>", "", jobname)]
    jobs[, jobname := gsub("</JB_name>", "", jobname)]
    jobs[, jobname := trimws(jobname)]
    jobs <- jobs[jobname == query_jobname]

    finished <- nrow(jobs) == 0

    print(current_time)
    if (finished) {
      print(paste0("All jobs with name '", query_jobname, "' are no longer running"))
    } else {
      print(paste0("Still waiting for ", nrow(jobs), " jobs to finish with job name '", query_jobname, "'"))
      Sys.sleep(sleep_time)
    }

    # update elapsed time
    elapsed_time <- current_time - start_time
  }

  if (!finished) {
    message <- paste0("Jobs not complete; stopping execution after ", sleep_end, " minutes")
    if (!warn_only) {
      stop(message)
    } else {
      warning(message)
    }
  }
  return(finished)
}


slack_broken_model_message <- function(step_description, resubmission_attempts, num_files_missing, num_expected_files) {
  message <- paste0("Population model broke during step that ", step_description, " after ", resubmission_attempts, " resubmission attempts. ",
                    num_files_missing, " out of ", num_expected_files, " files are still missing.")
  mortdb::send_slack_message(message = message, channel = paste0("@", USER), icon = "broken_heart", botname = "PopBot")
  if (!test & slack_channel_updates) mortdb::send_slack_message(message = message)
}

slack_successful_model_step_message <- function(step_description) {
  message <- paste0("Population model finished with step that ", step_description, ".")
  mortdb::send_slack_message(message = message, channel = paste0("@", USER), icon = "heavy_check_mark", botname = "PopBot")
  if (!test & slack_channel_updates) mortdb::send_slack_message(message = message)
}


subset_unfinished_tasks <- function(tasks) {
  # check whether final output file exists
  tasks[, output_file_exists := file.exists(output_file)]

  # subset to locations that have not yet finished
  tasks <- tasks[!(output_file_exists)]

  return(tasks)
}

create_task_map_fpath <- function(jobname) {
  time <- format(Sys.time(), "%Y_%m_%d_%H_%M_%S")
  fpath <- paste0(output_dir, "task_map/", jobname, "_", time, ".csv")
  return(fpath)
}

write_tasks <- function(tasks, task_map_fpath) {
  tasks[, task_id := .I]
  readr::write_csv(tasks, task_map_fpath)
}


# Submission functions ----------------------------------------------------

## download all inputs from databases for model fitting
submit_download_model_inputs <- function(jobname) {
  output_file_exists <- file.exists(paste0(output_dir, "/database/gbd_population_current_round_best.csv"))
  if (!output_file_exists) {
    mortcore::qsub(jobname = jobname,
                   cores = 5, mem = 20, wallclock = "00:30:00", archive_node = T,
                   queue = queue, proj = submission_project_name,
                   shell = shell_dir, code = paste0(code_dir, "01a_download_model_inputs.R"),
                   pass_argparse = list(pop_vid = pop_reporting_vid,
                                        test = test),
                   not_hostname = hostnames_to_avoid,
                   submit = submit_jobs)
  }
}

## download all other inputs from databases
submit_download_other_inputs <- function(jobname) {
  output_file_exists <- file.exists(paste0(output_dir, "/database/sdi.csv"))
  if (!output_file_exists) {
    mortcore::qsub(jobname = jobname,
                   cores = 1, mem = 20, wallclock = "00:30:00", archive_node = T,
                   queue = queue, proj = submission_project_name,
                   shell = shell_dir, code = paste0(code_dir, "01b_download_other_inputs.R"),
                   pass_argparse = list(pop_vid = pop_reporting_vid,
                                        test = test),
                   not_hostname = hostnames_to_avoid,
                   submit = submit_jobs)
  }
}

## prep demographic inputs (fertility, mortality, migration, srb)
submit_prep_demographic_inputs <- function(jobname) {
  output_file_exists <- file.exists(paste0(output_dir, "/inputs/migration.csv"))
  if (!output_file_exists) {
    mortcore::qsub(jobname = jobname,
                   cores = 1, mem = 10, wallclock = "00:20:00", archive_node = F,
                   queue = queue, proj = submission_project_name,
                   shell = shell_dir, code = paste0(code_dir, "02a_prep_demographic_inputs.R"),
                   pass_argparse = list(pop_vid = pop_reporting_vid,
                                        test = test),
                   hold = paste0("download_model_inputs_", pop_reporting_vid),
                   not_hostname = hostnames_to_avoid,
                   submit = submit_jobs)
  }
}

## prep population input
submit_prep_population_inputs <- function(jobname) {
  output_file_exists <- file.exists(paste0(output_dir, "/inputs/population.csv"))
  if (!output_file_exists) {
    mortcore::qsub(jobname = jobname,
                   cores = 1, mem = 4, wallclock = "00:05:00", archive_node = F,
                   queue = queue, proj = submission_project_name,
                   shell = shell_dir, code = paste0(code_dir, "02b_prep_population_inputs.R"),
                   pass_argparse = list(pop_vid = pop_reporting_vid,
                                        test = test),
                   hold = paste0("download_model_inputs_", pop_reporting_vid),
                   not_hostname = hostnames_to_avoid,
                   submit = submit_jobs)
  }
}

generate_tasks_fit_model <- function(return_only_unfinished_tasks = T) {
  tasks <- location_specific_settings[, list(drop_above_age = eval(parse(text = pooling_ages))), by = "ihme_loc_id"]
  tasks <- merge(tasks, location_hierarchy[, list(ihme_loc_id, location_id, is_estimate, copy_results)], by = "ihme_loc_id", all.x = T)
  tasks <- tasks[is_estimate == 1]
  tasks[, output_file := paste0(output_dir, location_id, "/outputs/model_fit/migration_proportion_drop", drop_above_age, ".csv")]
  if (return_only_unfinished_tasks) tasks <- subset_unfinished_tasks(tasks)
  return(tasks)
}

submit_fit_model <- function(jobname) {
  tasks <- generate_tasks_fit_model()

  if (nrow(tasks) > 0) {
    task_map_fpath <- create_task_map_fpath(jobname)
    write_tasks <- write_tasks(tasks, task_map_fpath)

    mortcore::array_qsub(jobname = jobname,
                         cores = 8, mem = 60, wallclock = "24:00:00", archive_node = F,
                         queue = queue, proj = submission_project_name,
                         shell = paste0(code_dir, "FILEPATH/tmb-shell"),
                         code = paste0(code_dir, "03a_fit_model.R"),
                         pass_argparse = list(pop_vid = pop_reporting_vid,
                                              task_map_dir = task_map_fpath,
                                              test = test,
                                              USER = USER),
                         hold = paste(c(paste0("prep_demographic_inputs_", pop_reporting_vid),
                                        paste0("prep_population_inputs_", "_", pop_reporting_vid)), collapse = ","),
                         num_tasks = nrow(tasks),
                         log = T, not_hostname = hostnames_to_avoid,
                         submit = submit_jobs)
  }
}

generate_tasks_predict_model <- function(return_only_unfinished_tasks = T) {
  tasks <- location_specific_settings[, list(drop_above_age = eval(parse(text = pooling_ages))), by = "ihme_loc_id"]
  tasks <- merge(tasks, location_hierarchy[, list(location_id, ihme_loc_id, is_estimate, copy_results)], by = "ihme_loc_id", all.x = T)
  tasks <- tasks[is_estimate == 1]
  tasks[, output_file := paste0(output_dir, location_id, "/outputs/model_fit/counts_drop", drop_above_age, ".csv")]
  if (return_only_unfinished_tasks) tasks <- subset_unfinished_tasks(tasks)
  return(tasks)
}

submit_predict_model <- function(jobname) {
  tasks <- generate_tasks_predict_model()

  if (nrow(tasks) > 0) {
    task_map_fpath <- create_task_map_fpath(jobname)
    write_tasks <- write_tasks(tasks, task_map_fpath)

    mortcore::array_qsub(jobname = jobname,
                         cores = 1, mem = 2, wallclock = "00:10:00", archive_node = F,
                         queue = queue, proj = submission_project_name,
                         shell = shell_dir, code = paste0(code_dir, "03b_predict_model.R"),
                         pass_argparse = list(pop_vid = pop_reporting_vid,
                                              task_map_dir = task_map_fpath,
                                              test = test),
                         hold = paste(c(paste0("fit_model_", pop_reporting_vid),
                                        paste0("download_other_inputs_", pop_reporting_vid)), collapse = ","),
                         num_tasks = nrow(tasks),
                         log = T, not_hostname = hostnames_to_avoid,
                         submit = submit_jobs)
  }
}

submit_select_best_versions <- function(jobname) {

  # Always rerun this step just in case the model fits were rerun
  file.remove(c(paste0(output_dir, "/versions_best.csv"),
                paste0(output_dir, "/versions_compare.csv")))

  mortcore::qsub(jobname = jobname,
                 cores = 1, mem = 5, wallclock = "00:30:00", archive_node = F,
                 queue = queue, proj = submission_project_name,
                 shell = shell_dir, code = paste0(code_dir, "03c_select_best_model.R"),
                 pass_argparse = list(pop_vid = pop_reporting_vid,
                                      test = test),
                 hold = paste0("predict_model_", pop_reporting_vid),
                 not_hostname = hostnames_to_avoid,
                 submit = submit_jobs)

}

generate_tasks_create_ui <- function(return_only_unfinished_tasks = T) {
  tasks <- location_hierarchy[is_estimate == 1, list(location_id)]
  tasks[, output_file := paste0(output_dir, location_id, "/outputs/confirm_create_ui_completion.csv")]
  if (return_only_unfinished_tasks) tasks <- subset_unfinished_tasks(tasks)
  return(tasks)
}

## create unraked draws of population
submit_create_ui <- function(jobname) {

  tasks <- generate_tasks_create_ui()

  if (nrow(tasks) > 0) {
    task_map_fpath <- create_task_map_fpath(jobname)
    write_tasks <- write_tasks(tasks, task_map_fpath)

    mortcore::array_qsub(jobname = jobname,
                         cores = 1, mem = 6, wallclock = "01:00:00", archive_node = F,
                         queue = queue, proj = submission_project_name,
                         shell = shell_dir, code = paste0(code_dir, "04_create_ui.R"),
                         pass_argparse = list(pop_vid = pop_reporting_vid,
                                              task_map_dir = task_map_fpath,
                                              test = test),
                         hold = paste0("select_best_versions_", pop_reporting_vid),
                         num_tasks = nrow(tasks),
                         log = T, not_hostname = hostnames_to_avoid,
                         submit = submit_jobs)
  }
}

generate_tasks_rake_aggregate <- function(return_only_unfinished_tasks = T) {
  tasks <- data.table(draw = 0:(draws - 1))
  tasks[, output_file := paste0(output_dir, "/raking_draws/confirm_raking_aggregation_completion_", draw, ".csv")]
  if (return_only_unfinished_tasks) tasks <- subset_unfinished_tasks(tasks)
  return(tasks)
}

submit_rake_aggregate <- function(jobname) {

  tasks <- generate_tasks_rake_aggregate()

  if (nrow(tasks) > 0) {
    task_map_fpath <- create_task_map_fpath(jobname)
    write_tasks <- write_tasks(tasks, task_map_fpath)

    mortcore::array_qsub(jobname = jobname,
                         cores = 2, mem = 9, wallclock = "03:00:00", archive_node = F,
                         queue = queue, proj = submission_project_name,
                         shell = shell_dir, code = paste0(code_dir, "05_rake_aggregate.R"),
                         pass_argparse = list(pop_vid = pop_reporting_vid,
                                              task_map_dir = task_map_fpath,
                                              test = test),
                         hold = paste0("create_ui_", pop_reporting_vid),
                         num_tasks = nrow(tasks),
                         log = T, not_hostname = hostnames_to_avoid,
                         submit = submit_jobs)
  }
}

generate_tasks_compile_loc_draws <- function(return_only_unfinished_tasks = T) {
  tasks <- all_hierarchies[, list(location_id = unique(location_id))]
  tasks[, output_file := paste0(output_dir, location_id, "/outputs/confirm_compile_loc_completion.csv")]
  if (return_only_unfinished_tasks) tasks <- subset_unfinished_tasks(tasks)
  return(tasks)
}

## compile together raked draws of population by location
submit_compile_loc_draws <- function(jobname) {

  tasks <- generate_tasks_compile_loc_draws()

  if (nrow(tasks) > 0) {
    task_map_fpath <- create_task_map_fpath(jobname)
    write_tasks <- write_tasks(tasks, task_map_fpath)

    mortcore::array_qsub(jobname = jobname,
                         cores = 2, mem = 8, wallclock = "02:00:00", archive_node = F,
                         queue = queue, proj = submission_project_name,
                         shell = shell_dir, code = paste0(code_dir, "06_compile_loc_draws.R"),
                         pass_argparse = list(pop_vid = pop_reporting_vid,
                                              task_map_dir = task_map_fpath,
                                              test = test),
                         hold = paste0("rake_aggregate_", pop_reporting_vid),
                         num_tasks = nrow(tasks),
                         log = T, not_hostname = hostnames_to_avoid,
                         submit = submit_jobs)
  }
}

generate_tasks_plot_diagnostics <- function(best_ages, return_only_unfinished_tasks = T) {
  if (best_ages) {
    tasks <- location_hierarchy[is_estimate == 1, list(location_id, drop_above_age = "best")]
    tasks <- merge(tasks, location_hierarchy[, list(location_id, ihme_loc_id)], by = "location_id", all.x = T)
    tasks[, output_file := paste0(output_dir, "/diagnostics/location_best/model_fit_", ihme_loc_id, "_best.pdf")]
  } else {
    tasks <- location_specific_settings[, list(drop_above_age = eval(parse(text = pooling_ages))), by = "ihme_loc_id"]
    tasks <- tasks[ihme_loc_id %in% location_hierarchy[is_estimate == 1, ihme_loc_id]]
    tasks <- merge(tasks, location_hierarchy[, list(location_id, ihme_loc_id)], by = "ihme_loc_id", all.x = T)
    tasks[, output_file := paste0(output_dir, "/diagnostics/location_drop_age/model_fit_", ihme_loc_id, "_drop", drop_above_age, ".pdf")]
  }

  if (return_only_unfinished_tasks) tasks <- subset_unfinished_tasks(tasks)
  return(tasks)
}

## make diagnostic plots for each location-drop_above_age
submit_plot_diagnostics <- function(jobname, best_ages) {

  tasks <- generate_tasks_plot_diagnostics(best_ages = best_ages)
  type <- ifelse(best_ages, "best", "drop_ages")

  if (nrow(tasks) > 0) {
    task_map_fpath <- create_task_map_fpath(jobname)
    write_tasks <- write_tasks(tasks, task_map_fpath)

    mortcore::array_qsub(jobname = jobname,
                         cores = 1, mem = 3, wallclock = "00:05:00", archive_node = F,
                         queue = queue, proj = submission_project_name,
                         shell = shell_dir, code = paste0(code_dir, "07a_plot_diagnostics.R"),
                         pass_argparse = list(pop_vid = pop_reporting_vid,
                                              task_map_dir = task_map_fpath,
                                              test = test),
                         hold = paste(c(paste0("create_ui_", pop_reporting_vid),
                                        paste0("compile_loc_draws_", pop_reporting_vid)), collapse = ","),
                         num_tasks = nrow(tasks),
                         log = T, not_hostname = hostnames_to_avoid,
                         submit = submit_jobs)
  }
}

generate_tasks_pct_change_heatmap <- function(return_only_unfinished_tasks = T) {
  tasks <- data.table(comparison_round = comparison_heatmap_rounds)
  tasks[, output_file := paste0(output_dir, "/diagnostics/pct_change_heatmap_", comparison_round, ".pdf")]
  if (return_only_unfinished_tasks) tasks <- subset_unfinished_tasks(tasks)
  return(tasks)
}

## make percent change heatmaps comparing to other population estimates
submit_pct_change_heatmap <- function(jobname, comparison_round) {
  tasks <- generate_tasks_pct_change_heatmap()

  if (nrow(tasks) > 0) {
    task_map_fpath <- create_task_map_fpath(jobname)
    write_tasks <- write_tasks(tasks, task_map_fpath)

    mortcore::array_qsub(jobname = jobname,
                         cores = 1, mem = 8, wallclock = "01:00:00", archive_node = F,
                         queue = queue, proj = submission_project_name,
                         shell = shell_dir, code = paste0(code_dir, "07b_pct_change_heatmap.R"),
                         pass_argparse = list(pop_vid = pop_reporting_vid,
                                              task_map_dir = task_map_fpath,
                                              test = test),
                         hold = paste(c(paste0("create_ui_", pop_reporting_vid),
                                        paste0("compile_loc_draws_", pop_reporting_vid)), collapse = ","),
                         num_tasks = nrow(tasks),
                         not_hostname = hostnames_to_avoid,
                         submit = submit_jobs)
  }
}

## make sandbox landing page
submit_generate_sandbox_diagnostics <- function(jobname) {
  output_file_exists <- file.exists(paste0(sandbox_dir, "/index.html"))
  if (!output_file_exists) {
    mortcore::qsub(jobname = jobname,
                   cores = 1, mem = 2, wallclock = "01:00:00", archive_node = F,
                   queue = queue, proj = submission_project_name,
                   shell = paste0(code_dir, "FILEPATH/python-shell"),
                   code = paste0(code_dir, "07c_generate_sandbox_diagnostics.py"),
                   pass = list(ifelse(test, paste0("test/", pop_reporting_vid), pop_reporting_vid), census_processed_data_vid, comment),
                   hold = paste(c(paste0("plot_diagnostics_by_drop_age_", pop_reporting_vid),
                                  paste0("plot_diagnostics_best_", pop_reporting_vid),
                                  paste0("pct_change_heatmaps_", pop_reporting_vid)), collapse = ","),
                   not_hostname = hostnames_to_avoid,
                   submit = submit_jobs)
  }
}

submit_upload <- function(jobname) {
  output_file_exists <- file.exists(paste0(output_dir, "/diagnostics/baby_migration.csv"))
  if (!output_file_exists) {
    mortcore::qsub(jobname = jobname,
                   cores = 5, mem = 10, wallclock = "01:00:00", archive_node = F,
                   queue = queue, proj = submission_project_name,
                   shell = shell_dir, code = paste0(code_dir, "08_upload.R"),
                   pass_argparse = list(pop_vid = pop_reporting_vid,
                                        test = test),
                   hold = paste0("compile_loc_draws_", pop_reporting_vid),
                   not_hostname = hostnames_to_avoid,
                   submit = submit_jobs)
  }
}


# Submit all jobs ---------------------------------------------------------

step_worked <- submit_with_resubs(step_description = "downloads model inputs",
                                  jobname = paste0("download_model_inputs_", pop_reporting_vid),
                                  tasks = data.table(output_file = paste0(output_dir, "/database/gbd_population_current_round_best.csv")),
                                  submission_function = submit_download_model_inputs, resubmission_attempts = 1)

step_worked <- submit_with_resubs(step_description = "downloads other inputs",
                                  jobname = paste0("download_other_inputs_", pop_reporting_vid),
                                  tasks = data.table(output_file = paste0(output_dir, "/database/sdi.csv")),
                                  submission_function = submit_download_other_inputs, resubmission_attempts = 1)

step_worked <- submit_with_resubs(step_description = "preps other demographic inputs",
                                  jobname = paste0("prep_demographic_inputs_", pop_reporting_vid),
                                  tasks = data.table(output_file = paste0(output_dir, "/inputs/migration.csv")),
                                  submission_function = submit_prep_demographic_inputs, resubmission_attempts = 1)

step_worked <- submit_with_resubs(step_description = "preps population data inputs",
                                  jobname = paste0("prep_population_inputs_", pop_reporting_vid),
                                  tasks = data.table(output_file = paste0(output_dir, "/inputs/population.csv")),
                                  submission_function = submit_prep_population_inputs, resubmission_attempts = 1)

step_worked <- submit_with_resubs(step_description = "fits population model for each location-drop_age combination",
                                  jobname = paste0("fit_model_", pop_reporting_vid),
                                  tasks = generate_tasks_fit_model(return_only_unfinished_tasks = F),
                                  submission_function = submit_fit_model, resubmission_attempts = 1,
                                  stop_submissions_on_error = F)

step_worked <- submit_with_resubs(step_description = "predicts from the fitted population model for each location-drop_age combination",
                                  jobname = paste0("predict_model_", pop_reporting_vid),
                                  tasks = generate_tasks_predict_model(return_only_unfinished_tasks = F),
                                  submission_function = submit_predict_model, resubmission_attempts = 2,
                                  stop_submissions_on_error = F)

step_worked <- submit_with_resubs(step_description = "selects best drop-age version for each location",
                                  jobname = paste0("select_best_versions_", pop_reporting_vid),
                                  tasks = data.table(output_file = paste0(output_dir, "/versions_best.csv")),
                                  submission_function = submit_select_best_versions,
                                  slack_success_message = T, resubmission_attempts = 2)

step_worked <- submit_with_resubs(step_description = "plots location specific diagnostics for all drop-ages",
                                  jobname = paste0("plot_diagnostics_drop_age_", pop_reporting_vid),
                                  tasks = generate_tasks_plot_diagnostics(best_ages = F, return_only_unfinished_tasks = F),
                                  submission_function = submit_plot_diagnostics,
                                  stop_submissions_on_error = F, best_ages = F)

step_worked <- submit_with_resubs(step_description = "creates population draws",
                                  paste0("create_ui_", pop_reporting_vid),
                                  tasks = generate_tasks_create_ui(return_only_unfinished_tasks = F),
                                  submission_function = submit_create_ui, slack_success_message = T)

if (length(test_locations) == 0 | !test) {

  step_worked <- submit_with_resubs(step_description = "rakes & aggregate population draws",
                                    jobname = paste0("rake_aggregate_", pop_reporting_vid),
                                    tasks = generate_tasks_rake_aggregate(return_only_unfinished_tasks = F),
                                    submission_function = submit_rake_aggregate, slack_success_message = T)

  step_worked <- submit_with_resubs(step_description = "compiles raked & aggregated location specific population estimates & draws",
                                    jobname = paste0("compile_loc_draws_", pop_reporting_vid),
                                    tasks = generate_tasks_compile_loc_draws(return_only_unfinished_tasks = F),
                                    submission_function = submit_compile_loc_draws, slack_success_message = T)
}

step_worked <- submit_with_resubs(step_description = "plots location specific diagnostics",
                                  jobname = paste0("plot_diagnostics_best_", pop_reporting_vid),
                                  tasks = generate_tasks_plot_diagnostics(best_ages = T, return_only_unfinished_tasks = F),
                                  submission_function = submit_plot_diagnostics, best_ages = T)

step_worked <- submit_with_resubs(step_description = "plots percent change heatmap diagnostics",
                                  jobname = paste0("pct_change_heatmaps_", pop_reporting_vid),
                                  tasks = generate_tasks_pct_change_heatmap(return_only_unfinished_tasks = F),
                                  submission_function = submit_pct_change_heatmap, resubmission_attempts = 3)

step_worked <- submit_with_resubs(step_description = "generates sandbox diagnostics page",
                                  jobname = paste0("generate_sandbox_diagnostics_", pop_reporting_vid),
                                  tasks = data.table(output_file = paste0(sandbox_dir, "/index.html")),
                                  submission_function = submit_generate_sandbox_diagnostics,
                                  resubmission_attempts = 1, slack_success_message = T)

step_worked <- submit_with_resubs(step_description = "uploads population and migration estimates",
                                  jobname = paste0("upload_", pop_reporting_vid),
                                  tasks = data.table(output_file = paste0(output_dir, "/diagnostics/baby_migration.csv")),
                                  submission_function = submit_upload, resubmission_attempts = 1,
                                  slack_success_message = T)
