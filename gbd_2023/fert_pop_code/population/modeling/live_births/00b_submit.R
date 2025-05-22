################################################################################
## Description: Submit all jobs needed for the live births process.
################################################################################

library(data.table)
library(readr)
library(parallel)
library(mortdb)
library(mortcore)

rm(list = ls())
USER <- Sys.getenv("USER")
Sys.umask(mode = "0002")


# Get arguments -----------------------------------------------------------

# load step specific settings into Global Environment
parser <- argparse::ArgumentParser()
parser$add_argument("--live_births_vid", type = "character",
                    help = 'The version number for this run of live births, used to read in settings file')
parser$add_argument("--test", type = "character",
                    help = 'Whether this is a test run of the process')
parser$add_argument("--check_misformatted_files", type = "character",
                    help = "Whether to check that hdf5 output files have all expected outputs")
parser$add_argument("--code_dir", type = "character", 
                    help = 'Location of code')
args <- parser$parse_args()
if (interactive()) { # set interactive defaults, should never commit changes away from the test version to be safe
  args$live_births_vid <- "99999"
  args$test <- "T"
  args$check_misformatted_files <- "T"
  args$code_dir <- "CODE_DIR_HERE"
}
args$test <- as.logical(args$test)
args$check_misformatted_files <- as.logical(args$check_misformatted_files)
list2env(args, .GlobalEnv); rm(args)

# load model run specific settings into Global Environment
settings_dir <- paste0("FILEPATH", ifelse(test, "test/", ""), live_births_vid, "/run_settings.csv")
load(settings_dir)
list2env(settings, envir = environment())

all_hierarchies <- fread(paste0(output_dir, "/inputs/all_reporting_hierarchies.csv"))

source(paste0(code_dir, "functions/files.R"))

path_to_shell <- 'FILEPATH'
path_to_image <- paste0("FILEPATH")
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
    jobs <- data.table(jobname = system("squeue --me", intern = T))
    jobs <- jobs[grepl(substr(query_jobname, 1, 8), jobname)]

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
  message <- paste0("Live births broke during step that ", step_description, " after ", resubmission_attempts, " resubmission attempts. ",
                    num_files_missing, " out of ", num_expected_files, " files are still missing.")
  mortdb::send_slack_message(message = message, channel = paste0("@", USER), icon = "broken_heart", botname = "BirthsBot")
  mortdb::send_slack_message(message = message, channel = "#fertility", icon = "broken_heart", botname = "BirthsBot")
}

slack_successful_model_step_message <- function(step_description) {
  message <- paste0("Live births finished with step that ", step_description, ".")
  mortdb::send_slack_message(message = message, channel = paste0("@", USER), icon = "heavy_check_mark", botname = "BirthsBot")
  mortdb::send_slack_message(message = message, channel = "#fertility", icon = "heavy_check_mark", botname = "BirthsBot")
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

submit_download_inputs <- function(jobname) {
  output_file_exists <- file.exists(paste0(output_dir, "/inputs/population.csv"))
  if (!output_file_exists) {
    mortcore::qsub(jobname = jobname,
                   cores = 1, mem = 2, wallclock = "00:25:00", archive_node = F,
                   queue = queue, proj = submission_project_name,
                   shell = path_to_shell, pass_shell = list(i = path_to_image),
                   code = paste0(code_dir, "01_download_inputs.R"),
                   pass_argparse = list(live_births_vid = live_births_vid,
                                        test = test, 
                                        code_dir = code_dir),
                   not_hostname = hostnames_to_avoid,
                   submit = submit_jobs)
  }
}

generate_tasks_calculate <- function(return_only_unfinished_tasks = T) {
  tasks <- unique(all_hierarchies[location_set_name == "live_births" & is_estimate == 1, list(location_id)])
  tasks[, output_file := paste0(output_dir, "outputs/unraked_by_loc/", location_id, ".h5")]
  if (return_only_unfinished_tasks) subset_unfinished_tasks(tasks)
  return(tasks)
}

submit_calculate <- function(jobname) {

  tasks <- generate_tasks_calculate()

  if (nrow(tasks) > 0) {
    task_map_fpath <- create_task_map_fpath(jobname)
    write_tasks <- write_tasks(tasks, task_map_fpath)

    mortcore::array_qsub(jobname = jobname,
                         cores = 1, mem = 10, wallclock = "00:45:00", archive_node = F,
                         queue = queue, proj = submission_project_name,
                         shell = path_to_shell, pass_shell = list(i = path_to_image),
                         code = paste0(code_dir, "02_calculate.R"),
                         pass_argparse = list(live_births_vid = live_births_vid,
                                              task_map_dir = task_map_fpath,
                                              test = test, 
                                              code_dir = code_dir),
                         hold = paste0("download_inputs_", live_births_vid),
                         num_tasks = nrow(tasks),
                         step_size = nrow(tasks),
                         log = T, not_hostname = hostnames_to_avoid,
                         submit = submit_jobs)
  }
}

generate_tasks_rake_aggregate <- function(return_only_unfinished_tasks = T) {
  tasks <- data.table(draw = draws)
  tasks[, output_file := paste0(output_dir, "outputs/raked_aggregated_by_draw/", draw, ".h5")]
  if (return_only_unfinished_tasks) subset_unfinished_tasks(tasks)
  return(tasks)
}

submit_rake_aggregate <- function(jobname) {

  tasks <- generate_tasks_rake_aggregate()

  if (nrow(tasks) > 0) {
    task_map_fpath <- create_task_map_fpath(jobname)
    write_tasks <- write_tasks(tasks, task_map_fpath)

    mortcore::array_qsub(jobname = jobname,
                         cores = 1, mem = 10, wallclock = "00:45:00", archive_node = F,
                         queue = queue, proj = submission_project_name,
                         shell = path_to_shell, pass_shell = list(i = path_to_image),
                         code = paste0(code_dir, "03_rake_aggregate.R"),
                         pass_argparse = list(live_births_vid = live_births_vid,
                                              task_map_dir = task_map_fpath,
                                              test = test, 
                                              code_dir = code_dir),
                         hold = paste0("calculate_", live_births_vid),
                         num_tasks = nrow(tasks),
                         step_size = nrow(tasks),
                         log = T, not_hostname = hostnames_to_avoid,
                         submit = submit_jobs)
  }
}

generate_tasks_rake_aggregate <- function(return_only_unfinished_tasks = T) {
  tasks <- data.table(draw = draws)
  tasks[, output_file := paste0(output_dir, "outputs/raked_aggregated_by_draw/", draw, ".h5")]
  if (return_only_unfinished_tasks) subset_unfinished_tasks(tasks)
  return(tasks)
}

submit_rake_aggregate <- function(jobname) {

  tasks <- generate_tasks_rake_aggregate()

  if (nrow(tasks) > 0) {
    task_map_fpath <- create_task_map_fpath(jobname)
    write_tasks <- write_tasks(tasks, task_map_fpath)

    mortcore::array_qsub(jobname = jobname,
                         cores = 3, mem = 20, wallclock = "02:00:00", archive_node = F,
                         queue = queue, proj = submission_project_name,
                         shell = path_to_shell, pass_shell = list(i = path_to_image),
                         code = paste0(code_dir, "03_rake_aggregate.R"),
                         pass_argparse = list(live_births_vid = live_births_vid,
                                              task_map_dir = task_map_fpath,
                                              test = test, 
                                              code_dir = code_dir),
                         hold = paste0("calculate_", live_births_vid),
                         num_tasks = nrow(tasks),
                         step_size = nrow(tasks),
                         log = T, not_hostname = hostnames_to_avoid,
                         submit = submit_jobs)
  }
}

generate_tasks_compile_by_location <- function(return_only_unfinished_tasks = T) {
  tasks <- unique(all_hierarchies[, list(location_id)])
  tasks[, output_file := paste0(output_dir, "outputs/raked_aggregated_by_loc/", location_id, "_reporting_summary.csv")]
  if (return_only_unfinished_tasks) subset_unfinished_tasks(tasks)
  return(tasks)
}

submit_compile_by_location <- function(jobname) {

  tasks <- generate_tasks_compile_by_location()

  if (nrow(tasks) > 0) {
    task_map_fpath <- create_task_map_fpath(jobname)
    write_tasks <- write_tasks(tasks, task_map_fpath)

    mortcore::array_qsub(jobname = jobname,
                         cores = 1, mem = 5, wallclock = "00:45:00", archive_node = F,
                         queue = queue, proj = submission_project_name,
                         shell = path_to_shell, pass_shell = list(i = path_to_image),
                         code = paste0(code_dir, "04_compile_by_location.R"),
                         pass_argparse = list(live_births_vid = live_births_vid,
                                              task_map_dir = task_map_fpath,
                                              test = test, 
                                              code_dir = code_dir),
                         hold = paste0("rake_aggregate_", live_births_vid),
                         num_tasks = nrow(tasks),
                         step_size = nrow(tasks),
                         log = T, not_hostname = hostnames_to_avoid,
                         submit = submit_jobs)
  }
}

submit_upload <- function(jobname) {
  output_file_exists <- file.exists(paste0(output_dir, "upload/births_reporting.csv"))
  if (!output_file_exists) {
    mortcore::qsub(jobname = jobname,
                   cores = 1, mem = 5, wallclock = "00:45:00", archive_node = F,
                   queue = queue, proj = submission_project_name,
                   shell = path_to_shell, pass_shell = list(i = path_to_image),
                   code = paste0(code_dir, "05_upload.R"),
                   pass_argparse = list(live_births_vid = live_births_vid,
                                        test = test, 
                                        code_dir = code_dir),
                   hold = paste0("compile_by_location_", live_births_vid),
                   not_hostname = hostnames_to_avoid,
                   submit = submit_jobs)
  }
}

submit_plot <- function(jobname) {
  output_file_exists <- file.exists(paste0(output_dir, "diagnostics/run_id_",
                                           live_births_vid,
                                           "_total_births_by_loc.pdf"))
  if (!output_file_exists) {
    mortcore::qsub(jobname = jobname,
                   cores = 1, mem = 20, wallclock = "00:45:00", archive_node = F,
                   queue = queue, proj = submission_project_name,
                   shell = path_to_shell, pass_shell = list(i = path_to_image),
                   code = paste0(code_dir, "06_plot_tot.R"),
                   pass_argparse = list(live_births_vid = live_births_vid,
                                        test = test, 
                                        code_dir = code_dir),
                   hold = paste0("upload_", live_births_vid),
                   not_hostname = hostnames_to_avoid,
                   submit = submit_jobs)
  }
}


# Submit all jobs ---------------------------------------------------------

step_worked <- submit_with_resubs(step_description = "downloads inputs",
                                  jobname = paste0("download_inputs_", live_births_vid),
                                  tasks = data.table(output_file = paste0(output_dir, "/inputs/population.csv")),
                                  submission_function = submit_download_inputs, resubmission_attempts = 1)

step_worked <- submit_with_resubs(step_description = "calculates live births",
                                  paste0("calculate_", live_births_vid),
                                  tasks = generate_tasks_calculate(return_only_unfinished_tasks = F),
                                  submission_function = submit_calculate, slack_success_message = T)

step_worked <- submit_with_resubs(step_description = "rakes & aggregate live birth draws",
                                  paste0("rake_aggregate_", live_births_vid),
                                  tasks = generate_tasks_rake_aggregate(return_only_unfinished_tasks = T),
                                  submission_function = submit_rake_aggregate, slack_success_message = T)

step_worked <- submit_with_resubs(step_description = "compiles raked & aggregated location specific live birth estimates & draws",
                                  jobname = paste0("compile_by_location_", live_births_vid),
                                  tasks = generate_tasks_compile_by_location(return_only_unfinished_tasks = F),
                                  submission_function = submit_compile_by_location, slack_success_message = T)

step_worked <- submit_with_resubs(step_description = "uploads live birth estimates",
                                  jobname = paste0("upload_", live_births_vid),
                                  tasks = data.table(output_file = paste0(output_dir, "upload/births_reporting.csv")),
                                  submission_function = submit_upload, resubmission_attempts = 1, slack_success_message = T)

step_worked <- submit_with_resubs(step_description = "plot live birth estimates",
                                  jobname = paste0("plot_", live_births_vid),
                                  tasks = data.table(output_file = paste0(output_dir, "diagnostics/run_id_",
                                                                          live_births_vid,
                                                                          "_total_births_by_loc.pdf")),
                                  submission_function = submit_plot, resubmission_attempts = 1, slack_success_message = T)
