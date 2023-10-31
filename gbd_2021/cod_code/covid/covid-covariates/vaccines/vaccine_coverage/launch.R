

# start with clean environment
if(length( names(sessionInfo()$otherPkgs) )){
  to_detach <- paste0("package:", names(sessionInfo()$otherPkgs))
  lapply(to_detach, detach, character.only=TRUE, unload=TRUE, force = TRUE)
} 
rm(list = ls(all.names = TRUE))

# PACKAGES ------------------------------------------------------------------

.start_time <- proc.time()
library(boot)
library(cowplot)
library(data.table)
library(dplyr)
library(glue)
library(ggplot2)
library(ggrepel)
library(gridExtra)
library(scales)
library(zoo)
library(RColorBrewer)
library(foreach)
library(cowplot)
library(sf)
library(tictoc)

library(farver, lib.loc = "FILEPATH")
library(gsheet, lib.loc = "FILEPATH")
library(labeling, lib.loc = "FILEPATH")
library(pbmcapply, lib.loc = "FILEPATH")
library(pbapply, lib.loc = "FILEPATH")
library(plyr, lib.loc = "FILEPATH")

library('ihme.covid', lib.loc='FILEPATH')

source(paste0("FILEPATH", Sys.info()['user'], "FILEPATH/paths.R"))
source(file.path("FILEPATH/get_location_metadata.R"))
source(file.path("FILEPATH/get_population.R"))
source(file.path("FILEPATH/get_covariate_estimates.R"))

source("FILEPATH/rstudio_singularity_4034_patch.R")
remove_python_objects()
library(mrbrt002, lib.loc = "FILEPATH")


# Where da funcs at? ------------------------------------------------------------------
f <- function(path) for (i in list.files(path, pattern = "\\.[Rr]$")) source(file.path(path, i))
f(CODE_PATHS$FUNCTIONS_PATH); rm(f)

##------------------------------------------------------------------------#
# Set output directory and versions and do not touch in subsequent scripts
##------------------------------------------------------------------------#

# VERSIONS ------------------------------------------------------------------
.model_inputs_version <- '2022_12_10.01' # prod 

.previous_best_version <- '2022_11_13.04'

# RECONNECT --------------------------------------------------------------------
.output_path = ""

if (F) {
  # Use at own risk: this reconnects to a previously existing vaccine output directory.
  
  # Params:
  reconnect_version <- "2022_12_11.02" # The vaccine-coverage version to work from.
  reconnect_purpose = ""
  
  # Constants:
  .output_path <- file.path(DATA_ROOTS$VACCINE_OUTPUT_ROOT, reconnect_version)
  .reconnect_meta_yaml_fp = file.path(.output_path, "metadata_reconnect.yaml")
  
  # Construction:
  .metadata_reconnect <- list()
  
  if(file.exists(.reconnect_meta_yaml_fp)) {
    .metadata_reconnect <- yaml::read_yaml(file = .reconnect_meta_yaml_fp)
  }
  
  .metadata_reconnect[[length(.metadata_reconnect) + 1]] <- list(
    rerun_time = as.character(Sys.time()),
    reconnect_purpose = reconnect_purpose,
    model_inputs_path = file.path(DATA_ROOTS$MODEL_INPUTS_ROOT, .model_inputs_version),
    previous_best_path = file.path(DATA_ROOTS$VACCINE_OUTPUT_ROOT, .previous_best_version),
    output_dir = .output_path,
    git_branch = gsub("\n", "", readr::read_file(paste0("FILEPATH", Sys.info()['user'],  "FILEPATH", ".git/HEAD"))),
    git_hash = gsub("\n", "", readr::read_file(paste0("FILEPATH", Sys.info()['user'],  "FILEPATH", ".git/ORIG_HEAD")))
  )
  
  # Write:
  yaml::write_yaml(.metadata_reconnect, file = .reconnect_meta_yaml_fp)
  
  rm(reconnect_version, reconnect_purpose)
  
} else {
  
  .output_path <- ihme.covid::get_output_dir(root = DATA_ROOTS$VACCINE_OUTPUT_ROOT, date = "today")
  
}

.output_version <- .get_version_from_path(.output_path) 
.previous_best_path <- file.path(DATA_ROOTS$VACCINE_OUTPUT_ROOT, .previous_best_version)
.county_path <- vaccine_metadata.get_county_version(.output_path)
.model_inputs_path <- file.path(DATA_ROOTS$MODEL_INPUTS_ROOT, .model_inputs_version)
.previous_model_inputs_path = yaml::read_yaml(
  file = file.path(.previous_best_path, "metadata.yaml")
)$model_inputs_path
.previous_model_inputs_version = .get_version_from_path(.previous_model_inputs_path)

# METADATA ------------------------------------------------------------------
# Which exact hash/commit were outputs run from?
CODE_ROOT <- gsub("/vaccine_coverage$", "", CODE_PATHS$VACCINE_CODE_ROOT)
git_logs <- fread(file.path(CODE_ROOT, "FILEPATH"), header = F)
git_log_last <- git_logs[nrow(git_logs)]
git_hash <- stringr::str_split_fixed(git_log_last[["V1"]], " ", n = Inf)[2]

.metadata <- list(
  script = "launch.R",
  model_inputs_path = normalizePath(.model_inputs_path),
  previous_best_path = normalizePath(.previous_best_path),
  previous_model_inputs_path = normalizePath(.previous_model_inputs_path),
  output_dir = normalizePath(.output_path),
  git_branch = gsub("\n", "", readr::read_file(file.path(CODE_ROOT, ".git/HEAD"))),
  git_log_last = git_log_last,
  git_hash = git_hash,
  submission_commands = .extract_submission_commands()
)

yaml::write_yaml(.metadata, file = file.path(.output_path, "metadata.yaml"))
rm(CODE_ROOT, git_logs, git_log_last, git_hash) # clean up


# LOGGING ON ------------------------------------------------------------------
# Create run_log.txt with all messages and errors for overnight runs
# sdtout will go to one file, conditions (stderr()) will go to another

# WARNING: function calls will NOT print to logs, ONLY messages/errors/warnings
# WARNING: function calls WILL print to console, but NOT messages/errors/warnings
# use sink.number() to find open connections & ?sink to troubleshoot connections

LOG_CONDITIONS_TO_FILE <- TRUE

if(LOG_CONDITIONS_TO_FILE) {
  LOG_CONDITIONS <- file(file.path(.output_path, "LOG_CONDITIONS.txt"), open = "wt")
  LOG_OUTPUTS <- file(file.path(.output_path, "LOG_OUTPUTS.TXT"), open = "wt")
  sink(LOG_CONDITIONS, type = "message", append = T)
  sink(LOG_OUTPUTS, type = "output", append = T)
}


# PARAMETERS ------------------------------------------------------------------

message(glue("Writing results to {.output_path}"))

.model_parameters <- list(
  
  # Model versions
  model_inputs_path = .model_inputs_path,
  previous_best_path = .previous_best_path,
  output_path = .output_path,
  county_path = .county_path,
  
  ## Spliced locations ---------------------------------------------------------
  # - use all time series from past production in process_observed_vaccinations.R
  splice_locs = c(
    "Niue" = 374
    ), 
  
  # Booster parameters
  booster_courses = 2L, # vaccine_courses defined dynamically below
  booster_1_min_obs = 390L, # Minimum number of locations to correct per booster course
  booster_2_min_obs = 160L,
  booster_correct_n_cores = 20L, 
  
  # booster_3_min_obs = 200,
  
  # Supply model parameters
  # GAVI scenarios
  # Old data: 'more' (1.8B) or 'less' (1.3B)
  # New data: 'low' (~15%, 50+), 'medium' (~60%, 18+), 'high' (~70%, 12+)
  use_gavi = FALSE, # Logical indicating whether to include gavi scenario doses
  gavi_dose_scenario = "high",
  projection_end_date = as.Date("2023-12-31"),
  
  # Hesitancy model parameters
  
  data_start_date = as.Date("2020-12-01"),
  end_date = as.Date(Sys.Date() + 90),
  
  # Definition of survey responses ('yes_responses' arg)
  # 'yes_definitely' = only 'yes' responses
  # 'yes_probably' = yes + yes probably (default)
  # 'no_probably' = yes + yes probably + no probably
  # 'no_definitely' = yes + yes probably + no probably + no definitely
  survey_yes_responses = 'yes_definitely',
  survey_yes_responses_reference = 'yes_definitely',
  survey_use_appointment = FALSE,
  
  
  # The problem is that this gets updated when ETL is run, so we would need to wait
  # until Tuesday at the earliest if FB data are updated Monday (overnight ETL)
  # If true, pull from limited use directories instead of model-inputs
  use_limited_use = FALSE,
  
  # Forecast model parameters
  adults_o65 = T,
  include_o12 = T,
  include_o5 = F,
  child_vaccination_scenario = F,
  long_range = F,
  save_counties = F,
  cdc_scenarios = F,
  
  # Args for correcting historical model values
  set_past_to_reference = F,
  reference_path = 'FILEPATH',
  
  run_brand_stratified = TRUE,
  
  # Params for age-stratified models
  run_age_stratified = FALSE,
  n_cores = 60L,
  age_starts = c(0,5,12,18,40,65),
  include_all_ages = TRUE,
  
  # Diagnostics params:
  write_full_brand_splitcheck_space = F,
  intake_dx_types = NULL, # "all" for non-stratified, "brand", and/or "age"; or, just an empty vector or NULL for no intake diagnostics.
  intake_dx_output_path = "" # Will set to versioned path below if intake_dx_types is not empty.
)
# booster_courses + 1 (1 = fully/initially)
.model_parameters$vaccine_courses = .model_parameters$booster_courses + 1L

if (!is_empty(.model_parameters$intake_dx_types)) {
  .model_parameters$intake_dx_output_path = ihme.covid::get_output_dir(
    "FILEPATH",
    date = "today"
  )
}


# Main Pipeline ------------------------------------------------------------

message("Writing model parameters.")
message(str(.model_parameters))
vaccine_data$write_model_parameters(.model_parameters, .output_path)


if (!is_empty(.model_parameters$intake_dx_types)) {
  .launch_intake_dx(
    intake_dx_output_path = .model_parameters$intake_dx_output_path,
    model_inputs_version = .model_inputs_version,
    previous_model_inputs_version = .previous_model_inputs_version,
    types = .model_parameters$intake_dx_types
  )
}

message("Copying vaccine efficacy table.")
vaccine_data$write_vaccine_efficacy(model_inputs_data$load_vaccine_efficacy_table(.model_inputs_path), .output_path)

message("Processing observed vaccination data.")
process_observed_vaccinations(vaccine_output_root = .output_path)

message("Estimating vaccine supply.")
estimate_available_doses(.output_path)

message("Submitting vaccine supply diagnostics.")
submit_supply_plot(.output_path)

message("Estimating vaccine hesitancy.")
smooth_vaccine_hesitancy(vaccine_output_root = .output_path, plot_timeseries = T,  plot_maps = T)

.check_hesitancy_model_output(.output_path)

#message("Fitting spline scale up.")
# fit_spline_scale_up(version=..., best_version=..)

message("Forecasting vaccine scenarios.")
make_final_vaccine_estimate(.output_path)

.check_vaccine_projections(.output_path)

calc_demand(vaccine_output_root = .output_path, plot_maps=T, plot_timeseries=T)



#------------------------------------------------------------#
# Vaccination by brand---------------------------------------
#------------------------------------------------------------#

if (.model_parameters$run_brand_stratified) {
  
  message("Estimating vaccine supply by brand.")
  
  estimate_available_doses_brand(.output_path)
  
  .get_boosters_point_est(vaccine_output_root = .output_path) 
  
  tic(glue("scenario_wrapper, {.model_parameters$vaccine_courses} courses"))
  .scenario_wrapper(vaccine_output_root = .output_path)
  toc()
  
  tic("booster correction")
  .booster_correction(
    vaccine_output_root = .output_path, 
    n_cores = .model_parameters$booster_correct_n_cores 
  )
  toc()
  
  .validate_last_shots(vaccine_output_root = .output_path)
  
  # Run pre-/post-split by brand/riskgroup cumulative checks.
  # Write and plot the brand-risk-group cumulatives in addition to the total cumulatives?
  runtime = "30"
  if (.model_parameters$write_full_brand_splitcheck_space) runtime = "90"
  
  
}


#------------------------------------------------------------#
# Diagnostics -----------------------------------------------
#------------------------------------------------------------#

message('Submitting plotting jobs')

.submit_plot_job(
  plot_script_path = file.path(CODE_PATHS$VACCINE_DIAGNOSTICS_ROOT, "supply_diagnostics.R"),
  current_version = .output_path
)

.submit_plot_job(
  plot_script_path = file.path(CODE_PATHS$VACCINE_DIAGNOSTICS_ROOT, "plot_hesitancy_scenario_compare.R"),
  current_version = .output_version,
  compare_version = .previous_best_version
)

#------------------------------------------------------------#
# Vaccination and hesitancy by age (in development) ---------
#------------------------------------------------------------#

if (.model_parameters$run_age_stratified) {
  
  .t <- Sys.time()
  
  hierarchy <- gbd_data$get_covid_modeling_hierarchy()
  
  population <- .get_age_group_populations(age_starts = .model_parameters$age_starts,
                                           model_inputs_path = .model_parameters$model_inputs_path,
                                           include_all_ages = .model_parameters$include_all_ages)
  
  # Process non-age-stratified data
  #process_observed_vaccinations(vaccine_output_root = .output_path)
  
  # Make EUA dates here and have down stream funcs pick up versioned csv
  get_eua_dates(vaccine_output_root = .output_path,
                hierarchy = hierarchy)
  
  # Load and process age-stratified vaccination data
  process_observed_vaccinations_age(vaccine_output_root = .output_path,
                                    hierarchy = hierarchy,
                                    population = population)
  
  # Load and process FB survey data (includes vaccination and hesitancy)
  process_survey_data_age(vaccine_output_root = .output_path,
                          hierarchy = hierarchy,
                          split_under_18 = T)
  
  # Fit models to observed non-age-stratified vaccination data
  smooth_pct_vaccination(vaccine_output_root = .output_path,
                         hierarchy = hierarchy,
                         population = population)
  
  # Fit models of vaccination coverage to age-stratified FB survey data
  smooth_pct_vaccination_age_survey(vaccine_output_root = .output_path,
                                    hierarchy = hierarchy,
                                    population = population,
                                    use_eua_dates = FALSE,
                                    knock_out_locations = c(hierarchy[parent_id == 135, location_id], # Mexican subnats
                                                            hierarchy[parent_id == 163, location_id], # Indian subnats
                                                            376, 38:40, 49, 60, 61, 349, 83, 367,
                                                            106:108, 305, 110, 112, 113, 393, 116, 118,
                                                            119, 162, 320, 23, 26:30, 413, 416, 14, 183,
                                                            186, 168, 169, 172, 175:177, 182, 185, 186,
                                                            435, 197, 194, 195, 203, 204, 206, 209, 210,
                                                            213, 215, 217, 218, 22))
  
  smooth_pct_vaccination_age_method_1(vaccine_output_root = .output_path,
                                      hierarchy = hierarchy,
                                      population = population)
  
  # Estimate time-varying bias in survey vaccination coverage
  estimate_survey_bias(vaccine_output_root = .output_path,
                       hierarchy = hierarchy,
                       population = population,
                       knock_out_locations = c(376, 38, 89))
  
  # Fit models of vaccination coverage to age-stratified reported vaccination data
  # Uses multiple methods to infer coverage when age-stratified data are missing or sparse
  smooth_pct_vaccination_age(vaccine_output_root = .output_path,
                             hierarchy = hierarchy,
                             population = population,
                             time_varying_bias = TRUE)
  
  # Fit models of vaccine acceptance to age-stratified FB survey data
  smooth_vaccine_hesitancy_age(vaccine_output_root = .output_path,
                               hierarchy = hierarchy,
                               population = population,
                               child_method = 'weighted_mean',
                               remove_wave_12_trial = TRUE,
                               knock_out_locations = c(hierarchy[parent_id == 81, location_id],  # Germany subnats
                                                       hierarchy[parent_id == 135, location_id], # Mexican subnats
                                                       hierarchy[parent_id == 163, location_id], # Indian subnats
                                                       376, 38, 35, 50, 49, 59, 66, 83, 107, 305,
                                                       150, 151, 320, 22, 351, 25, 380, 26, 28,
                                                       413, 29, 416, 30, 19, 169, 173, 175))
  
  # Calculate vaccinate + willing
  calc_vaccinated_willing_age(vaccine_output_root = .output_path,
                              hierarchy = hierarchy)
  
  
  # Estimate fully vaccinated and boosters
  
  # Combine with brand data
  
  # Forecast vaccine uptake by age and brand
  
  
  # Plot diagnostics of vaccine hesitancy
  .submit_plot_job(
    plot_script_path = file.path(CODE_PATHS$VACCINE_DIAGNOSTICS_ROOT, "vaccine_hesitancy_age.R"),
    current_version = .output_path
  )
  
  # Plot age stratified models
  .submit_plot_job(
    plot_script_path = file.path(CODE_PATHS$VACCINE_DIAGNOSTICS_ROOT, "plot_vaccination_coverage_age.R"),
    current_version = .output_path
  )
  
  # Plot vaccinated + willing
  .submit_plot_job(
    plot_script_path = file.path(CODE_PATHS$VACCINE_DIAGNOSTICS_ROOT, "plot_vaccinated_and_willing.R"),
    current_version = .output_version,
    compare_version = .previous_best_version
  )
  
  print(Sys.time() - .t)
}


# LOGGING OFF --------------------------------------------------------------
# Closing sink() connections to complete logging
if(LOG_CONDITIONS_TO_FILE) {
  sink(type = "output")
  sink(type = "message")
  close(LOG_OUTPUTS)
  close(LOG_CONDITIONS)
}

# METADATA2 ------------------------------------------------------------------
.run_time <- proc.time() - .start_time
.metadata$run_time = paste0(format(.run_time[3]/60, digits=3), " minutes")
.metadata$input_files = ihme.covid::get.input.files()
if (.model_parameters$save_counties == T) {
  yaml::write_yaml(.metadata, file = file.path(.county_path, "metadata.yaml"))
  .metadata$county_path = .county_path
}
yaml::write_yaml(.metadata, file = file.path(.output_path, "metadata.yaml"))

message(glue("Pipeline run complete in {format(.run_time[3]/60, digits=3)} minutes."))
