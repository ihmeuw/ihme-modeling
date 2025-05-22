library(stgpr)
library(data.table)
suppressPackageStartupMessages(library(tidyverse))
source("FILEPATH")
dir_proj <- fs::path_real("FILEPATH")
source(fs::path(dir_proj, "FILEPATH"))

args <- commandArgs(trailingOnly = TRUE)
run_linear_prior <- args[1]
plot_linear_prior_run_stgpr_plot_gpr <- args[2]
cgf_type <- args[3]
params <- readRDS(args[4])


if (run_linear_prior == TRUE) {
  ## Build output directories to hold all modeling results. The function arguments
  # are provided and modified from the parameter maps generated above.
  cli::cli_progress_step("Building output directories...", msg_done = "Output directories built.")
  stgpr::build_output_directories(team_directory = purrr::chuck(params, cgf_type, "team_directory"), 
                                  cause_name = purrr::chuck(params, cgf_type, "cause_name"), 
                                  cause_subdirectory = purrr::chuck(params, cgf_type, "cause_subdirectory"))
  
  # Save the new parameter maps to the output directories
  cli::cli_progress_step("Creating parameter maps...", msg_done = "Parameter maps saved.")
  stgpr::build_covariate_map(
    team_directory = purrr::chuck(params, cgf_type, "team_directory"),
    cause_name = purrr::chuck(params, cgf_type, "cause_name"),
    cause_subdirectory = purrr::chuck(params, cgf_type, "cause_subdirectory"),
    covariates = unlist(purrr::chuck(params, cgf_type, "covariates")),
    covariate_ids = unlist(purrr::chuck(params, cgf_type, "covariate_ids")),
    cov_directions = unlist(purrr::chuck(params, cgf_type, "cov_directions")),
    cov_transforms = unlist(purrr::chuck(params, cgf_type, "cov_transforms"))
  )
  
  stgpr::build_param_map(
    team_directory = purrr::chuck(params, cgf_type, "team_directory"),
    cause_name = purrr::chuck(params, cgf_type, "cause_name"),
    cause_subdirectory = purrr::chuck(params, cgf_type, "cause_subdirectory"),
    crosswalk_version_id = purrr::chuck(params, cgf_type, "crosswalk_version_id"),
    age_group_id = unlist(purrr::chuck(params, cgf_type, "age_group_id")),
    sex_id = unlist(purrr::chuck(params, cgf_type, "sex_id")),
    location_set_id = purrr::chuck(params, cgf_type, "location_set_id"),
    release_id = purrr::chuck(params, cgf_type, "release_id"),
    year_start = purrr::chuck(params, cgf_type, "year_start"),
    year_end = purrr::chuck(params, cgf_type, "year_end"),
    cause_transform = purrr::chuck(params, cgf_type, "cause_transform"),
    kos = purrr::chuck(params, cgf_type, "kos"),
    ko_proportion = purrr::chuck(params, cgf_type, "ko_proportion"),
    number_submodels = purrr::chuck(params, cgf_type, "number_submodels"),
    cluster_project = purrr::chuck(params, cgf_type, "cluster_project")
  )
  
  stgpr::ensemble_linear_prior(team_directory = purrr::chuck(params, cgf_type, "team_directory"),
                               cause_name = purrr::chuck(params, cgf_type, "cause_name"),
                               cause_subdirectory = purrr::chuck(params, cgf_type, "cause_subdirectory"))
  
}

if (plot_linear_prior_run_stgpr_plot_gpr == TRUE) {
  # Plot the Stage 1 linear ensemble prior at the super region level and for
  # each age group. Each line is a location in that super region.
  cli::cli_progress_step("Plotting ensemble linear prior models...", msg_done = "Ensemble priors plotted")
  stgpr::plot_ensemble_model(
    team_directory = purrr::chuck(params, cgf_type, "team_directory"),
    cause_name = purrr::chuck(params, cgf_type, "cause_name"),
    cause_subdirectory = purrr::chuck(params, cgf_type, "cause_subdirectory"),
    sex_id = unlist(purrr::chuck(params, cgf_type, "sex_id"))
  )
  
  # Plot the ensemble model with submodel fits for each location and age group.
  # Note that this can take over an hour and require more cluster
  # resources (i.e., memory) if subnationals are included.
  cli::cli_progress_step("Plotting ensemble sub-models...", msg_done = "Ensemble sub-models plotted")
  stgpr::plot_ensemble_submodels(
    team_directory = purrr::chuck(params, cgf_type, "team_directory"),
    cause_name = purrr::chuck(params, cgf_type, "cause_name"),
    cause_subdirectory = purrr::chuck(params, cgf_type, "cause_subdirectory"),
    sex_id = unlist(purrr::chuck(params, cgf_type, "sex_id")),
    remove_subnationals = TRUE
  )
  
  # Plot the covariate beta weights and correlations
  cli::cli_progress_step("Plotting covariate influence...", msg_done = "Covariate influence plotted")
  stgpr::plot_covariate_influence(
    team_directory = purrr::chuck(params, cgf_type, "team_directory"),
    cause_name = purrr::chuck(params, cgf_type, "cause_name"),
    cause_subdirectory = purrr::chuck(params, cgf_type, "cause_subdirectory"),
    sex_id = unlist(purrr::chuck(params, cgf_type, "sex_id"))
  )
  
  # Note that 'add_row' needs to be FALSE to create a new config file. 
  # If 'add_row' is set to TRUE, it will try to find an existing file to append 
  # the new settings to (and throw an error if one does not exist).
  cli::cli_progress_step("Creating ST-GPR config...", msg_done = "ST-GPR config created")
  if (purrr::chuck(params, cgf_type, "add_row") == TRUE) {
    stgpr::build_stgpr_config(
      add_row = purrr::chuck(params, cgf_type, "add_row"),
      team_directory = purrr::chuck(params, cgf_type, "team_directory"),
      cause_name = purrr::chuck(params, cgf_type, "cause_name"),
      cause_subdirectory = purrr::chuck(params, cgf_type, "cause_subdirectory"),
      model_index_id = purrr::chuck(params, cgf_type, "model_index_id"),
      modelable_entity_id = purrr::chuck(params, cgf_type, "modelable_entity_id"),
      modelable_entity_name = purrr::chuck(params, cgf_type, "modelable_entity_name"),
      crosswalk_version_id = purrr::chuck(params, cgf_type, "crosswalk_version_id"),
      bundle_id = purrr::chuck(params, cgf_type, "bundle_id"),
      release_id = purrr::chuck(params, cgf_type, "release_id"),
      location_set_id = purrr::chuck(params, cgf_type, "location_set_id"),
      year_start = purrr::chuck(params, cgf_type, "year_start"),
      year_end = purrr::chuck(params, cgf_type, "year_end"),
      description = purrr::chuck(params, cgf_type, "description"),
      data_transform = purrr::chuck(params, cgf_type, "data_transform"),
      prediction_units = purrr::chuck(params, cgf_type, "prediction_units"),
      prediction_age_group_ids = as.character(purrr::chuck(params, cgf_type, "prediction_age_group_ids")),
      prediction_sex_ids = as.character(purrr::chuck(params, cgf_type, "prediction_sex_ids")),
      gbd_covariates = purrr::chuck(params, cgf_type, "gbd_covariates"),
      stage_1_model_formula = purrr::chuck(params, cgf_type, "stage_1_model_formula"),
      path_to_custom_stage_1 = purrr::chuck(params, cgf_type, "path_to_custom_stage_1"),
      predict_re = purrr::chuck(params, cgf_type, "predict_re"),
      add_nsv = purrr::chuck(params, cgf_type, "add_nsv"),
      st_omega = purrr::chuck(params, cgf_type, "st_omega"),
      st_zeta = purrr::chuck(params, cgf_type, "st_zeta"),
      st_lambda = purrr::chuck(params, cgf_type, "st_lambda"),
      gpr_amp_factor = purrr::chuck(params, cgf_type, "gpr_amp_factor"),
      gpr_amp_method = purrr::chuck(params, cgf_type, "gpr_amp_method"),
      gpr_amp_cutoff = purrr::chuck(params, cgf_type, "gpr_amp_cutoff"),
      gpr_scale = purrr::chuck(params, cgf_type, "gpr_scale"),
      agg_level_4_to_3 = as.character(purrr::chuck(params, cgf_type, "agg_level_4_to_3")),
      agg_level_5_to_4 = as.character(purrr::chuck(params, cgf_type, "agg_level_5_to_4")),
      agg_level_6_to_5 = as.character(purrr::chuck(params, cgf_type, "agg_level_6_to_5")),
      rake_logit = purrr::chuck(params, cgf_type, "rake_logit"),
      gpr_draws = purrr::chuck(params, cgf_type, "gpr_draws"),
      transform_offset = purrr::chuck(params, cgf_type, "transform_offset"),
      metric_id = purrr::chuck(params, cgf_type, "metric_id"),
      notes = purrr::chuck(params, cgf_type, "notes"),
      run_id = ""
    )
  }
  
  # Registers and sends off the model
  cli::cli_progress_step("Running ST-GPR model...", msg_done = "ST-GPR model run")
  stgpr_id <- stgpr::run_stgpr(team_directory = purrr::chuck(params, cgf_type, "team_directory"),
                               cause_name = purrr::chuck(params, cgf_type, "cause_name"),
                               cause_subdirectory = purrr::chuck(params, cgf_type, "cause_subdirectory"),
                               cluster_project = purrr::chuck(params, cgf_type, "cluster_project"))
  
  # Get ST-GPR model status
  message(stgpr_id)
  status <- get_model_status(stgpr_id)
  while (status == 2) {
    cat("Model still running! Waiting a minute...\n")
    Sys.sleep(180)
    status <- get_model_status(stgpr_id, verbose = TRUE)
  }
  
  # Update cgf_ids.csv with run_id
  cli::cli_progress_step("Updating CGF ids...", msg_done = "CGF ids updated")
  ids <- get_ids()
  data.table::set(
    ids,
    i = which(ids$me_name == purrr::chuck(params, cgf_type, "cause_name")),
    j = as.character(purrr::chuck(params, cgf_type, "run_id_col_name")),
    value = stgpr_id
  )
  set_ids(ids)
  
  # Source the plot_gpr() function
  cli::cli_progress_step("Plotting ST-GPR model...", msg_done = "ST-GPR model plotted")
  source("FILEPATH")
  plot_gpr(
    run.id = stgpr_id,
    output.path = paste0(
      purrr::chuck(params, cgf_type, "team_directory"),
      "/",
      purrr::chuck(params, cgf_type, "cause_name"),
      "/",
      purrr::chuck(params, cgf_type, "cause_subdirectory"),
      "/plots/",
      stgpr_id,
      "_plot_gpr.pdf"
    ),
    cluster.project = purrr::chuck(params, cgf_type, "cluster_project")
  )

}


cli::cli_progress_done()