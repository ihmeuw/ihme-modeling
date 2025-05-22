
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

user <- "USERNAME"

library(argparse)
library(data.table)
library(readr)

library(demInternal)
library(mortcore)
library(mortdb)

path_to_shell <- "FILEPATH"
path_to_image_old <- "FILEPATH"
path_to_image <- "FILEPATH"
python_shell <- "FILEPATH"

clone_dir <- paste0("FILEPATH/Stillbirth_Pipeline_",
                    format(Sys.time(), "%Y%m%d%H%M"), "/")
branch <- "insert-branch-name-here"

best_queue <- "long"
slack <- FALSE

# Choose database to upload
hostname = "HOSTNAME"

# Set upload arguments
test_data <- TRUE # test: T = don't upload, F = upload
best_data <- FALSE # best: T = best the upload, F: don't best the upload
comment_data <- "insert data comment here"

test_estimate <- TRUE
best_estimate <- FALSE
comment_estimate <- "insert estimate comment here"

# Select stage 1 model:
# TRUE = new crosswalking; FALSE = old stage 1
use_crosswalk_stage1ensemble <- TRUE

# Select what is being modeled: SBR/NMR or SBR + NMR
## Note: the code is only set up to run SBR/NMR for old stage 1 model
model <- "SBR/NMR"

# Get year arguments
gbd_year_prev <- 2021
gbd_year <- 2023
release_id <- 16

year_start <- 1980
year_end <- 2024

# Select which definitions are estimated
defs <- c(20, 22, 24, 26, 28)

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
  "test version id",
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
  "test version id",
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
    model_name = "age sex",
    model_type = "data",
    run_id = "Run id",
    gbd_year = gbd_year
  ),

  `age sex estimate` = mortdb::get_proc_version(
    model_name = "age sex",
    model_type = "estimate",
    run_id = "Run id",
    gbd_year = gbd_year
  ),

  `asfr data` = mortdb::get_proc_version(
    model_name = "asfr",
    model_type = "data",
    run_id = "Run id",
    gbd_year = gbd_year
  ),

  `birth estimate` = mortdb::get_proc_version(
    model_name = "birth",
    model_type = "estimate",
    run_id = "Run id",
    gbd_year = gbd_year
  ),

  `ddm estimate` = mortdb::get_proc_version(
    model_name = "ddm",
    model_type = "estimate",
    run_id = "Run id",
    gbd_year = gbd_year
  ),

  `population estimate` = mortdb::get_proc_version(
    model_name = "population",
    model_type = "estimate",
    run_id = "Run id",
    gbd_year = gbd_year
  ),

  `no shock death number estimate` = mortdb::get_proc_version(
    model_name = "no shock death number",
    model_type = "estimate",
    run_id = "Run id",
    gbd_year = gbd_year
  ),

  `no shock life table estimate` = mortdb::get_proc_version(
    model_name = "no shock life table",
    model_type = "estimate",
    run_id = "Run id",
    gbd_year = gbd_year
  ),

  `with shock death number estimate` = mortdb::get_proc_version(
    model_name = "with shock death number",
    model_type = "estimate",
    run_id = "Run id",
    gbd_year = gbd_year
  ),

  `with shock life table estimate` = mortdb::get_proc_version(
    model_name = "with shock life table",
    model_type = "estimate",
    run_id = "Run id",
    gbd_year = gbd_year
  ),

  `stillbirth data` = stillbirth_model_version_data

)

maternal_edu_decomp_step <- "iterative"
maternal_edu_version <- "maternal education version id"

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
  nmr_with_shock_draws_version = parents$`with shock life table estimate`,
  nmr_no_shock_draws_version = parents$`no shock life table estimate`,

  hostname = hostname,
  slack = slack,

  shared_functions_dir = "FILEPATH",

  working_dir = "FILEPATH",
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

  gbd_year_prev = gbd_year_prev,
  gbd_year = gbd_year,
  gbd_round_id = mortdb::get_gbd_round(gbd_year),
  release_id = release_id,

  # arguments used in 01a_prep_stillbirth_input.R and/or 01c_automatic_outliering.R

  sbh_version = "2024-12-03",
  birth_death_completeness_version = "2024-11-20",
  drop_non_rep_sci_lit = TRUE,

  # arguments used in 01e_crosswalking.R
  direct_matches_only = FALSE,

  # arguments used in 02_stillbirth_model.R

  use_crosswalk_stage1ensemble = use_crosswalk_stage1ensemble,
  model = model,

  st_loess = TRUE,

  test_lambda = NA, # all locations receive specific lambda value; otherwise, base lambda off of data density

  # arguments used in 04_subnational_raking.R

  agg_locs = c(
    "BRA", "ETH", "GBR", "IDN", "IND", "KEN"
  ),

  split_rake_agg_GBR = TRUE,

  # argument used in 05_aggregate_stillbirths.R

  include_sdi_locs = TRUE, # location set not yet available for GBD 2021

  # arguments used in 07_graph_stillbirths.R

  graph_nat_est_only = FALSE,
  graph_ind_only = FALSE

)

list2env(settings, envir = environment())

# Create directories
dir.create("FILEPATH")
dir.create("FILEPATH")
dir.create("FILEPATH")
dir.create("FILEPATH")

dir.create("FILEPATH")
dir.create("FILEPATH")
dir.create("FILEPATH")
dir.create("FILEPATH")
dir.create("FILEPATH")
dir.create("FILEPATH")
dir.create("FILEPATH")

# Save settings
settings_dir_data <- paste0(data_dir, version_data, "/FILEPATH/run_settings.csv")
save(settings, file = settings_dir_data)
settings_dir_estimate <- paste0(estimate_dir, version_estimate, "/FILEPATH/run_settings.csv")
save(settings, file = settings_dir_estimate)

# Pull locations list to set parameters and parent locations for raking
locs <- mortdb::get_locations(level = "all", gbd_year = gbd_year)
locs_to_est <- locs[is_estimate == 1]

locs_w_regions <- locs_to_est[, c("region_name", "ihme_loc_id")]
readr::write_csv(
  locs_w_regions,
  paste0(estimate_dir, version_estimate, "/FILEPATH/locs_w_regions.csv")
)

parent_locs <- as.data.table(
  sort(unique(locs_to_est[grepl("_", ihme_loc_id), gsub("_.*", "", ihme_loc_id)]))
)
colnames(parent_locs) <- "loc"
readr::write_csv(
  parent_locs,
  paste0(estimate_dir, version_estimate, "/FILEPATH/parent_locs.csv")
)

# Create clones
if (!dir.exists(clone_dir)) {

  dir.create(clone_dir)

  repositories <- data.table(
    repo = "dem/Stillbirths",
    branch = branch,
    remote = "stash"
  )

  readr::write_csv(repositories, paste0(clone_dir, "repositories.csv"))

  mortcore::qsub(
    jobname = paste0("clone_", version_data, "_", version_estimate),
    shell = path_to_shell,
    pass_shell = list(i = path_to_image),
    code = paste0("FILEPATH/pipeline_fresh_clones.R"),
    proj = "PROJ NAME",
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
    jobname = paste0("sb01a_input_", version_data),
    shell = path_to_shell,
    pass_shell = list(i = path_to_image),
    code = paste0(working_dir, "01a_prep_stillbirth_input.R"),
    proj = "PROJ NAME",
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
    jobname = paste0("sb01b_upload_data_", version_data),
    hold = paste0("sb01a_input_", version_data),
    shell = path_to_shell,
    pass_shell = list(i = path_to_image),
    code = paste0(working_dir, "01b_upload_data.R"),
    proj = "PROJ NAME",
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
    jobname = paste0("sb01c_outlier_", version_data),
    hold = paste0("sb01b_upload_data_", version_data),
    shell = path_to_shell,
    pass_shell = list(i = path_to_image),
    code = paste0(working_dir, "01c_automatic_outliering.R"),
    proj = "PROJ NAME",
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
    jobname = paste0("sb01d_compare_data_", version_data),
    hold = paste0("sb01c_outlier_", version_data),
    shell = path_to_shell,
    pass_shell = list(i = path_to_image),
    code = paste0(working_dir, "01d_compare_data_versions.R"),
    proj = "PROJ NAME",
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

def_pars <- list(
  list(main = defs[1], other = defs[5]),
  list(main = defs[2], other = defs[1]),
  list(main = defs[3], other = defs[1]),
  list(main = defs[4], other = defs[1]),
  list(main = defs[5], other = defs[1])
)

# Splits model so it estimates either 20 or 28 weeks of gestation
for (pair in def_pars) {

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

  new_settings_dir_estimate <- paste0(estimate_dir, version_estimate, "/FILEPATH/new_run_settings_", main_std_def, ".csv")
  save(new_settings, file = new_settings_dir_estimate)

  ## RUN CROSSWALKING & STAGE 1 ENSEMBLE MODEL (IF APPLICABLE)

  if (start <= 1 & end >= 1) {

    if (use_crosswalk_stage1ensemble) {

      ## STEP 1E: CROSSWALKING

      mortcore::qsub(
        jobname = paste0("sb01e_crosswalk_data_", main_std_def, "_", version_data),
        hold = "sb01c_outlier",
        shell = path_to_shell,
        pass_shell = list(i = path_to_image_old),
        code = paste0(working_dir, "01e_crosswalking.R"),
        proj = "PROJ NAME",
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

      hold_jobs_step_1f <- c(
        paste0("sb01e_crosswalk_data_", defs[1], "_weeks_", version_data),
        paste0("sb01e_crosswalk_data_", defs[2], "_weeks_", version_data)
      )

      if (main_std_def == "28_weeks") {
        mortcore::qsub(
          jobname = paste0("sb01f_outlier_", version_data),
          hold = paste(hold_jobs_step_1f, collapse = ","),
          shell = path_to_shell,
          pass_shell = list(i = path_to_image),
          code = paste0(working_dir, "01f_automatic_outliering_pt2.R"),
          proj = "PROJ NAME",
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
        jobname = paste0("sb_stage_1_ensemble_model_", main_std_def, "_", version_estimate),
        hold = paste0("sb01f_outlier_", version_data),
        shell = path_to_shell,
        pass_shell = list(i = path_to_image),
        code = paste0(working_dir, "01g_launch_covariates_test.R"),
        proj = "PROJ NAME",
        pass = list(new_settings_dir_estimate),
        queue = best_queue,
        archive_node = FALSE,
        wallclock = "01:00:00",
        mem = 10,
        cores = 1,
        log = TRUE,
        submit = TRUE
      )

    }

  }

  ## STEP 2: MODEL

  hold_jobs_step_2 <- c(
    paste0("sb01c_outlier_", version_data),
    paste0("sb_stage_1_ensemble_model_", main_std_def, "_", version_estimate)
  )

  if (start <= 2 & end >= 2) {
    mortcore::qsub(
      jobname = paste0("sb02_model_", main_std_def, "_", version_estimate),
      hold = paste(hold_jobs_step_2, collapse = ","),
      shell = path_to_shell,
      pass_shell = list(i = path_to_image),
      code = paste0(working_dir, "02_stillbirth_model.R"),
      proj = "PROJ NAME",
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
      jobname = paste0("gpr_stillbirth_", version_estimate),
      hold = paste0("sb02_model_", main_std_def, "_", version_estimate),
      shell = python_shell,
      code = paste0(working_dir, "03_fit_gpr.py"),
      proj = "PROJ NAME",
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
      jobname = paste0("sb04_raking_", main_std_def, "_", version_estimate),
      hold = paste0("gpr_stillbirth_", version_estimate),
      shell = path_to_shell,
      pass_shell = list(i = path_to_image),
      code = paste0(working_dir, "04_subnational_raking.R"),
      proj = "PROJ NAME",
      pass = list(new_settings_dir_estimate),
      num_tasks = njobs,
      step_size = njobs,
      queue = best_queue,
      archive_node = FALSE,
      wallclock = "01:00:00",
      mem = 10,
      cores = 1,
      log = TRUE,
      submit = TRUE
    )
  }

  ## STEP 5: AGGREGATION

  if (start <= 5 & end >= 5) {
    mortcore::qsub(
      jobname = paste0("sb05_agg_sb_", main_std_def, "_", version_estimate),
      hold = paste0("sb04_raking_", main_std_def, "_", version_estimate),
      shell = path_to_shell,
      pass_shell = list(i = path_to_image),
      code = paste0(working_dir, "05_aggregate_stillbirths.R"),
      proj = "PROJ NAME",
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

  hold_jobs_step_6 <- c()
  for (i in 1:length(defs)) {
    hold_jobs_step_6_temp <- paste0("sb05_agg_sb_", defs[i], "_weeks_", version_estimate)
    hold_jobs_step_6 <- c(hold_jobs_step_6, hold_jobs_step_6_temp)
  }

  if (main_std_def == "28_weeks" & start <= 6 & end >= 6) {
    mortcore::qsub(
      jobname = paste0("sb06_upload_estimates_", version_estimate),
      hold = paste(hold_jobs_step_6, collapse = ","),
      shell = path_to_shell,
      pass_shell = list(i = path_to_image),
      code = paste0(working_dir, "06_upload_estimates.R"),
      proj = "PROJ NAME",
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
      jobname = paste0("sb07_graph_", main_std_def, "_", version_estimate),
      hold = paste0("sb06_upload_estimates_", version_estimate),
      shell = path_to_shell,
      pass_shell = list(i = path_to_image),
      code = paste0(working_dir, "07_graph_stillbirths.R"),
      proj = "PROJ NAME",
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
