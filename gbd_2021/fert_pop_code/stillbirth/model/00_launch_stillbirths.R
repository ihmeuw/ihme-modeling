###################################################################
##                                                               ##
## Purpose: This is the stillbirths model run all which launches ##
##          each step in the stillbirth estimation process:      ##
##             1) Data - A) Prep, B) Upload, C) Compare          ##
##             2) Model                                          ##
##             3) GPR                                            ##
##             4) Raking                                         ##
##             5) Aggregation                                    ##
##             6) Upload estimates                               ##
##             7) Graphing                                       ##
##                                                               ##
###################################################################

rm(list = ls())
Sys.umask(mode = "0002")

user <- Sys.getenv("USER")

library(argparse)
library(data.table)
library(readr)

library(demInternal)
library(mortcore)
library(mortdb)

path_to_shell <- "FILEPATH"
path_to_image_crosswalk <- "FILEPATH"
path_to_image <- "FILEPATH"
python_shell <- "FILEPATH"

clone_dir <- paste0("FILEPATH",
                    format(Sys.time(), "%Y%m%d%H%M"), "/")
branch <- "insert-branch-name-here"

best_queue <- "long"
slack <- FALSE

# Choose database to upload
hostname = "FILEPATH"

# Set upload arguments
test_data <- TRUE # test: T = don't upload, F = upload
best_data <- FALSE # best: T = best the upload, F: don't best the upload
comment_data <- "insert data comment here"

test_estimate <- TRUE
best_estimate <- FALSE
comment_estimate <- "insert estimate comment here"

model <- "SBR/NMR"

# Get year arguments
gbd_year <- 2021

year_start <- 1980
year_end <- 2022

# Select which two definitions are estimated
defs <- c(20, 28)

# Get comparison versions
stillbirth_data_version_compare <- mortdb::get_proc_version(
  model_name = "stillbirth",
  model_type = "data",
  run_id = "best",
  gbd_year = gbd_year
)

stillbirth_model_version_compare <- mortdb::get_proc_version(
  model_name = "stillbirth",
  model_type = "estimate",
  run_id = "best",
  gbd_year = gbd_year
)

# Create version numbers
stillbirth_model_version_data <- ifelse(
  test_data,
  VERSION,
  mortdb::gen_new_version(
    model_name = "stillbirth",
    model_type = "data",
    hostname = hostname,
    comment = comment_data,
    gbd_year = gbd_year
  )
)

stillbirth_model_version_estimate <- ifelse(
  test_estimate,
  VERSION,
  mortdb::gen_new_version(
    model_name = "stillbirth",
    model_type = "estimate",
    hostname = hostname,
    comment = comment_estimate,
    gbd_year = gbd_year
  )
)

# Create parent/child relationships
parents <- list(

  `age sex data` = mortdb::get_proc_version(
    model_name = "age_sex",
    model_type = "data",
    run_id = 308, # "best",
    gbd_year = gbd_year
  ),

  `age sex estimate` = mortdb::get_proc_version(
    model_name = "age sex",
    model_type = "estimate",
    run_id = 386, # "best",
    gbd_year = gbd_year
  ),

  `asfr data` = mortdb::get_proc_version(
    model_name = "asfr",
    model_type = "data",
    run_id = 348, # "best",
    gbd_year = gbd_year
  ),

  `birth estimate` = mortdb::get_proc_version(
    model_name = "birth",
    model_type = "estimate",
    run_id = 107, # "best",
    gbd_year = gbd_year
  ),

  `ddm estimate` = mortdb::get_proc_version(
    model_name = "ddm",
    model_type = "estimate",
    run_id = 516, # "best",
    gbd_year = gbd_year
  ),

  `population estimate` = mortdb::get_proc_version(
    model_name = "population",
    model_type = "estimate",
    run_id = 359, # "best",
    gbd_year = gbd_year
  ),

  `no shock death number estimate` = mortdb::get_proc_version(
    model_name = "no shock death number",
    model_type = "estimate",
    run_id = 659, # "best",
    gbd_year = gbd_year
  ),

  `no shock life table estimate` = mortdb::get_proc_version(
    model_name = "no shock life table",
    model_type = "estimate",
    run_id = 616, # "best",
    gbd_year = gbd_year
  ),

  `with shock death number estimate` = mortdb::get_proc_version(
    model_name = "with shock death number",
    model_type = "estimate",
    run_id = 587, # "best",
    gbd_year = gbd_year
  ),

  `stillbirth data` = stillbirth_model_version_data

)

maternal_edu_decomp_step <- "iterative"
maternal_edu_version <- 35547

if (!test_estimate) {

  mortdb::gen_parent_child(
    child_process = "stillbirth estimate",
    child_id = stillbirth_model_version_estimate,
    parent_runs = parents
  )

  mortdb::gen_external_input_map(
    process_name = "stillbirth estimate",
    run_id = stillbirth_model_version_estimate,
    external_input_versions = list("maternal_edu" = maternal_edu_version)
  )

}

# Create settings list
settings <- list(

  defs = defs,

  version_data = stillbirth_model_version_data,
  version_estimate = stillbirth_model_version_estimate,
  test_version_data = stillbirth_data_version_compare,
  test_version = stillbirth_model_version_compare,

  parents = parents,
  maternal_edu_decomp_step = maternal_edu_decomp_step,
  maternal_edu_version = maternal_edu_version,
  nmr_with_shock_draws_version = 573,
  nmr_no_shock_draws_version = 616,

  hostname = hostname,
  slack = slack,

  shared_functions_dir = "FILEPATH",

  working_dir = paste0(clone_dir, "FILEPATH"),
  j_dir = "FILEPATH",

  data_dir = "FILEPATH",
  estimate_dir = "FILEPATH",

  test_data = test_data,
  best_data = best_data,
  comment_data = comment_data,

  test_estimate = test_estimate,
  best_estimate = best_estimate,
  comment_estimate = comment_estimate,

  year_start = year_start,
  year_end = year_end,

  gbd_year = gbd_year,
  gbd_round_id = mortdb::get_gbd_round(gbd_year),

  # arguments used in 01a_prep_stillbirth_input.R and/or 01c_automatic_outliering.R

  sbh_version = "VERSION",
  birth_death_completeness_version = "VERSION",
  drop_non_rep_sci_lit = TRUE,

  # arguments used in 02_stillbirth_model.R

  use_crosswalk_stage1ensemble = use_crosswalk_stage1ensemble,
  model = model,

  st_loess = TRUE,

  test_lambda = NA, # when == NA: lambda based on data density; when != NA: all locs assigned to specific lambda value

  # arguments used in 04_subnational_raking.R
  #   -> ex: c("loc", "loc")

  agg_locs = c(
    "BRA", "CHN", "GBR", "IDN", "IND", "KEN",
    "MEX", "NGA", "NOR", "NZL", "USA"
  ),

  split_rake_agg_GBR  = TRUE,

  # argument used in 05_aggregate_stillbirths.R

  include_sdi_locs = TRUE,

  # arguments used in 07_graph_stillbirths.R

  graph_nat_est_only = FALSE,
  graph_ind_only = FALSE

)

list2env(settings, envir = environment())

# Create directories
dir.create(paste0(data_dir, version_data))
dir.create(paste0(data_dir, version_data, "/inputs"))
dir.create(paste0(data_dir, version_data, "/outputs"))
dir.create(paste0(data_dir, version_data, "/diagnostics"))

dir.create(paste0(estimate_dir, version_estimate))
dir.create(paste0(estimate_dir, version_estimate, "/model"))
dir.create(paste0(estimate_dir, version_estimate, "/gpr"))
dir.create(paste0(estimate_dir, version_estimate, "/scaled"))
dir.create(paste0(estimate_dir, version_estimate, "/results"))
dir.create(paste0(estimate_dir, version_estimate, "/graphs"))
dir.create(paste0(estimate_dir, version_estimate, "/inputs"))

# Save settings
settings_dir_data <- "FILEPATH"
crosswalk_data2[, main_std_def := paste0(defs[2], "_weeks")]
save(settings, file = settings_dir_data)
settings_dir_estimate <- "FILEPATH"
save(settings, file = settings_dir_estimate)

# Pull locations list to set parameters and parent locations for raking
locs <- mortdb::get_locations(level = "all", gbd_year = gbd_year)
locs_to_est <- locs[is_estimate == 1]

locs_w_regions <- locs_to_est[, c("region_name", "ihme_loc_id")]
readr::write_csv(
  locs_w_regions,
  "FILEPATH"
)

parent_locs <- as.data.table(
  sort(unique(locs_to_est[grepl("_", ihme_loc_id), gsub("_.*", "", ihme_loc_id)]))
)
colnames(parent_locs) <- "loc"
readr::write_csv(
  parent_locs,
  "FILEPATH"
)

# Create clones
if (!dir.exists(clone_dir)) {

  dir.create(clone_dir)

  repositories <- data.table(
    repo = "FILEPATH",
    branch = branch,
    remote = "stash"
  )

  readr::write_csv(repositories, paste0(clone_dir, "repositories.csv"))

  mortcore::qsub(
    jobname = paste0("clone_", version_data, "_", version_estimate),
    shell = path_to_shell,
    pass_shell = list(i = path_to_image),
    code = paste0("FILEPATH", user,
                  "FILEPATH"),
    proj = "proj_fertilitypop",
    pass_argparse = list("clone_dir" = clone_dir,
                         "create_git_tag" = TRUE),
    queue = best_queue,
    wallclock = "01:00:00",
    mem = 20,
    cores = 1,
    log = TRUE,
    submit = TRUE
  )

}

# Check that files exist in cloned directory

assertable::check_files(
  filenames = "01a_prep_stillbirth_input.R",
  folder = working_dir,
  continual = TRUE,
  sleep_time = 15,
  sleep_end = 10
)

# Select which steps to run
start <- 1L    ## step to start on
end <- 7L      ## step to end on

############################### DATA PREP SECTION ###############################

## STEP 1A: DATA PREP

if (start <= 1 & end >= 1) {
  mortcore::qsub(
    jobname = paste0("FILEPATH", version_data),
    shell = path_to_shell,
    pass_shell = list(i = path_to_image),
    code = paste0(working_dir, "01a_prep_stillbirth_input.R"),
    proj = "FILEPATH",
    pass = list(settings_dir_data),
    queue = best_queue,
    archive_node = TRUE,
    wallclock = "00:25:00",
    mem = 15,
    cores = 1,
    log = TRUE,
    submit = TRUE
  )
}

## STEP 1B: UPLOAD DATA TO DATABASE

if (start <= 1 & end >= 1) {
  mortcore::qsub(
    jobname = paste0("FILEPATH", version_data),
    hold = paste0("FILEPATH", version_data),
    shell = path_to_shell,
    pass_shell = list(i = path_to_image),
    code = paste0(working_dir, "01b_upload_data.R"),
    proj = "FILEPATH",
    pass = list(settings_dir_data),
    queue = best_queue,
    archive_node = FALSE,
    wallclock = "00:05:00",
    mem = 2,
    cores = 1,
    log = TRUE,
    submit = TRUE
  )
}

## STEP 1C: AUTOMATIC OUTLIERING

if (start <= 1 & end >= 1) {
  mortcore::qsub(
    jobname = paste0("FILEPATH", version_data),
    hold = paste0("FILEPATH", version_data),
    shell = path_to_shell,
    pass_shell = list(i = path_to_image),
    code = paste0(working_dir, "01c_automatic_outliering.R"),
    proj = "FILEPATH",
    pass = list(settings_dir_data),
    queue = best_queue,
    archive_node = FALSE,
    wallclock = "00:10:00",
    mem = 5,
    cores = 1,
    log = TRUE,
    submit = TRUE
  )
}

## STEP 1D: COMPARE NEWLY UPLOAD DATA TO PREVIOUS VERSION

if (start <= 1 & end >= 1) {
  mortcore::qsub(
    jobname = paste0("FILEPATH", version_data),
    hold = paste0("FILEPATH", version_data),
    shell = path_to_shell,
    pass_shell = list(i = path_to_image),
    code = paste0(working_dir, "01d_compare_data_versions.R"),
    proj = "FILEPATH",
    pass = list(settings_dir_data),
    queue = best_queue,
    archive_node = FALSE,
    wallclock = "00:20:00",
    mem = 2,
    cores = 1,
    log = TRUE,
    submit = TRUE
  )
}

############################ DATA PROCESSING SECTION ############################

# Splits model so it estimates for two definitions: 20 weeks gestation and 28 weeks gestation
for (pair in list(list(main = defs[2], other = defs[1]), list(main = defs[1], other = defs[2]))) {

  main_std_def <- paste0(pair$main, "_weeks")
  other_std_def <- paste0(pair$other, "_weeks")

  new_settings <- append(
    settings,
    list(
      main_std_def = paste0(pair$main, "_weeks"),
      other_std_def = paste0(pair$other, "_weeks")
    )
  )

  list2env(new_settings, envir = environment())

  new_settings_dir_estimate <- paste0(estimate_dir, version_estimate, "FILEPATH", main_std_def, ".csv")
  save(new_settings, file = new_settings_dir_estimate)

  ## RUN CROSSWALKING & STAGE 1 ENSEMBLE MODEL (IF APPLICABLE)

  if (start <= 1 & end >= 1) {

    if (use_crosswalk_stage1ensemble) {

      ## STEP 1E: CROSSWALKING
      ##   - Run interactively for now -> requires reticulate package & need to type "exit" for python graphing

      mortcore::qsub(
        jobname = paste0("sb01e_crosswalk_data_", main_std_def, "_", version_data),
        hold = "FILEPATH",
        shell = path_to_shell,
        pass_shell = list(i = path_to_image_crosswalk),
        code = paste0(working_dir, "01e_crosswalking.R"),
        proj = "FILEPATH",
        pass = list(new_settings_dir_estimate),
        queue = best_queue,
        archive_node = FALSE,
        wallclock = "00:30:00",
        mem = 10,
        cores = 1,
        log = TRUE,
        submit = TRUE
      )

      ## STEP 1F: AUTOMATIC OUTLIERING PT2
      ##  -> specifying main_std_def b/c additional outliering only needs to be done once

      hold_jobs_step_1f <- c(
        paste0("FILEPATH", defs[1], "_weeks_", version_data),
        paste0("FILEPATH", defs[2], "_weeks_", version_data)
      )

      if (main_std_def == "28_weeks") {
        mortcore::qsub(
          jobname = paste0("FILEPATH", version_data),
          hold = paste(hold_jobs_step_1f, collapse = ","),
          shell = path_to_shell,
          pass_shell = list(i = path_to_image),
          code = paste0(working_dir, "01f_automatic_outliering_pt2.R"),
          proj = "FILEPATH",
          pass = list(settings_dir_data),
          queue = best_queue,
          archive_node = FALSE,
          wallclock = "00:10:00",
          mem = 5,
          cores = 1,
          log = TRUE,
          submit = TRUE
        )
      }

############################### MODELING SECTION ###############################

      ## STEP 1G: STAGE 1 ENSEMBLE MODEL

      mortcore::qsub(
        jobname = paste0("FILEPATH", main_std_def, "_", version_estimate),
        hold = paste0("FILEPATH", version_data),
        shell = path_to_shell,
        pass_shell = list(i = path_to_image),
        code = paste0(working_dir, "01g_launch_covariates_test.R"),
        proj = "FILEPATH",
        pass = list(new_settings_dir_estimate),
        queue = best_queue,
        archive_node = FALSE,
        wallclock = "00:30:00",
        mem = 10,
        cores = 1,
        log = TRUE,
        submit = TRUE
      )

    }

  }

  ## STEP 2: MODEL

  hold_jobs_step_2 <- c(
    paste0("FILEPATH", version_data),
    paste0("FILEPATH", main_std_def, "_", version_estimate)
  )

  if (start <= 2 & end >= 2) {
    mortcore::qsub(
      jobname = paste0("FILEPATH", main_std_def, "_", version_estimate),
      hold = paste(hold_jobs_step_2, collapse = ","),
      shell = path_to_shell,
      pass_shell = list(i = path_to_image),
      code = paste0(working_dir, "02_stillbirth_model.R"),
      proj = "FILEPATH",
      pass = list(new_settings_dir_estimate),
      queue = best_queue,
      archive_node = FALSE,
      wallclock = "00:30:00",
      mem = 15,
      cores = 1,
      log = TRUE,
      submit = TRUE
    )
  }

  ## STEP 3: GPR (array runs all locations at once)

  if (start <= 3 & end >= 3) {
    njobs <- nrow(locs_w_regions)

    mortcore::array_qsub(
      jobname = paste0("FILEPATH", version_estimate),
      hold = paste0("FILEPATH", main_std_def, "_", version_estimate),
      shell = python_shell,
      code = paste0(working_dir, "03_fit_gpr.py"),
      proj = "FILEPATH",
      pass = list(
        "--main_std_def", main_std_def,
        "--version_estimate", version_estimate,
        "--working_dir", working_dir
      ),
      num_tasks = njobs,
      step_size = njobs,
      queue = best_queue,
      archive_node = FALSE,
      wallclock = "00:30:00",
      mem = 5,
      cores = 1,
      log = TRUE,
      submit = TRUE
    )
  }

  ## STEP 4: RAKING

  if (start <= 4 & end >= 4) {
    njobs <- nrow(parent_locs)

    mortcore::array_qsub(
      jobname = paste0("FILEPATH", main_std_def, "_", version_estimate),
      hold = paste0("FILEPATH", version_estimate),
      shell = path_to_shell,
      pass_shell = list(i = path_to_image),
      code = paste0(working_dir, "04_subnational_raking.R"),
      proj = "FILEPATH",
      pass = list(new_settings_dir_estimate),
      num_tasks = njobs,
      step_size = njobs,
      queue = best_queue,
      archive_node = FALSE,
      wallclock = "00:20:00",
      mem = 10,
      cores = 1,
      log = TRUE,
      submit = TRUE
    )
  }

  ## STEP 5: AGGREGATION

  if (start <= 5 & end >= 5) {
    mortcore::qsub(
      jobname = paste0("FILEPATH", main_std_def, "_", version_estimate),
      hold = paste0("FILEPATH", main_std_def, "_", version_estimate),
      shell = path_to_shell,
      pass_shell = list(i = path_to_image),
      code = paste0(working_dir, "05_aggregate_stillbirths.R"),
      proj = "FILEPATH",
      pass = list(new_settings_dir_estimate),
      queue = best_queue,
      archive_node = FALSE,
      wallclock = "00:30:00",
      mem = 30,
      cores = 1,
      log = TRUE,
      submit = TRUE
    )
  }

  ## STEP 6: UPLOAD ESTIMATES TO DATABASE
  ##  -> specifying main_std_def b/c estimates only need to be uploaded once

  hold_jobs_step_6 <- c(
    paste0("FILEPATH", defs[1], "FILEPATH", version_estimate),
    paste0("FILEPATH", defs[2], "FILEPATH", version_estimate)
  )

  if (main_std_def == "28_weeks" & start <= 6 & end >= 6) {
    mortcore::qsub(
      jobname = paste0("FILEPATH", version_estimate),
      hold = paste(hold_jobs_step_6, collapse = ","),
      shell = path_to_shell,
      pass_shell = list(i = path_to_image),
      code = paste0(working_dir, "06_upload_estimates.R"),
      proj = "FILEPATH",
      pass = list(new_settings_dir_estimate),
      queue = best_queue,
      archive_node = FALSE,
      wallclock = "00:05:00",
      mem = 2,
      cores = 1,
      log = TRUE,
      submit = TRUE
    )
  }

  ## STEP 7: GRAPHING

  if (start <= 7 & end >= 7) {
    mortcore::qsub(
      jobname = paste0("FILEPATH", main_std_def, "_", version_estimate),
      hold = paste0("FILEPATH", version_estimate),
      shell = path_to_shell,
      pass_shell = list(i = path_to_image),
      code = paste0(working_dir, "07_graph_stillbirths.R"),
      proj = "FILEPATH",
      pass = list(new_settings_dir_estimate),
      queue = best_queue,
      archive_node = TRUE,
      wallclock = "01:00:00",
      mem = 2,
      cores = 1,
      log = TRUE,
      submit = TRUE
    )
  }

}
