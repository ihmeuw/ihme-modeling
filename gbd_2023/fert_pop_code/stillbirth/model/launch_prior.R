###########################################################################################################################################
##                                                                                                                                       ##
## Purpose: Launch custom first stage prior testing and assemble for upload to ST-GPR                                                    ##
##                                                                                                                                       ##
## Overview: This script is for launching two functions: test_prior() and assemble_prior().                                              ##
##           Together they allow the creation of an ensemble linear prior for ST-GPR.                                                    ##
##                                                                                                                                       ##
## test_prior()                                                                                                                          ##
##   - This function runs all combinations of provided covariates on a provided dataset                                                  ##
##   - It ranks each sub-model by out-of-sample predictive validity and marks models containing insignificant betas and/or betas that    ##
##     represent the 'wrong' relationship with your outcome                                                                              ##
##   - The output is a list that includes a dataframe with a row for each model, the fit betas/SE, and whether or not that model         ##
##     violated significance or prior signs                                                                                              ##
##                                                                                                                                       ##
## assemble_prior()                                                                                                                      ##
##   - This function creates out-of-sample predictive validity-weighted predictions of your outcome using the results from the           ##
##     test_prior() function                                                                                                             ##
##   - This can be included as a 'cv_custom_prior' in an STGPR data upload                                                               ##
##   - This function can also plot a time-series of predictions for submodels, top weighted model, and the ensemble model                ##
##                                                                                                                                       ##
###########################################################################################################################################

launch_prior <- function(me, release_id, crosswalk_version_id = NA, path_to_data = NA, test_mods = TRUE, average = TRUE, pred_ages,
                         n_mods = 50, plot_aves = TRUE, plot_betas = TRUE, age_trend = FALSE,
                         cov_ids, prior_sign, ban_pairs = NULL, polynoms = NULL, modtype = "lmer",
                         rank_method = "oos.rmse", forms_per_job = 30, drop_nids = FALSE, fixed_covs = NULL,
                         custom_covs = NULL, random_effects = NULL, username = username, by_sex, version_estimate = 999) {

  ###############
  ## SET PATHS ##
  ###############

  output_dir <- "FILEPATH"
  dir.create(output_dir, recursive = T)
  dir.create(paste0(output_dir, "FILEPATH"))
  dir.create(paste0(output_dir, "FILEPATH"))

  rmse_output <- paste0(output_dir, "FILEPATH/prior_test_output.csv") # where to save object returned by test_prior
  rmse_output_archive <- paste0(output_dir, "FILEPATH/prior_test_output_", Sys.Date(), ".csv") # where to save object returned by test_prior

  data_output <- paste0(output_dir, "best_data.csv")
  crosswalk_data_output <- paste0(output_dir, "crosswalk_data.csv")

  ensemble_output <- paste0(output_dir, "custom_prior_", Sys.Date(), ".csv")

  plot_mods_path <- output_dir

  ####################
  ## SOURCE SCRIPTS ##
  ####################

  source("FILEPATH/get_ids.R")

  source(paste0(working_dir, "test_prior.R"))
  source(paste0(working_dir, "assemble_prior.R"))

  source(paste0(working_dir, "helper_functions.R"))

  #####################
  ## PULL COVARIATES ##
  #####################

  covs <- get_ids("covariate")
  cov_list <- lapply(cov_ids, function(X) covs[covariate_id == X, covariate_name]) %>% unlist

  ###################
  ## LAUNCH MODELS ##
  ###################

  if (test_mods == T) {

    message(paste0("Testing submodels for me ", me))

    rmses_and_data <- test_prior(
      crosswalk_version_id = crosswalk_version_id, path_to_data = path_to_data, release_id = release_id,
      cov_list = cov_list, data_transform = data_transform, rank_method = rank_method, modtype = modtype,
      custom_covs = custom_covs, fixed_covs = fixed_covs, random_effects = random_effects, ban_pairs = ban_pairs,
      prior_sign = prior_sign, by_sex = by_sex, polynoms = polynoms, ko_prop = ko_prop, kos = kos, drop_nids = FALSE,
      remove_subnats = T, proj = proj, m_mem_free = m_mem_free, username = username, forms_per_job = forms_per_job
    )

    rmses <- rmses_and_data[[1]]
    data <- rmses_and_data[[2]]
    crosswalk_data <- rmses_and_data[[3]]

    # write out data and rmses
    write.csv(crosswalk_data, file = crosswalk_data_output, row.names = F)
    write.csv(data, file = data_output, row.names = F)
    write.csv(rmses, file = rmse_output, row.names = F)
    write.csv(rmses, file = rmse_output_archive, row.names = F)

  }

  #################################
  ## CREATE ENSEMBLE PREDICTIONS ##
  #################################

  if (average == T) {

    message("Averaging submodels and predicting")

    rmses <- fread(rmse_output)
    data <- fread(data_output)

    ave_models <- assemble_prior(
      data, rmses[drop == 0,], cov_list, data_transform, pred_ages = pred_ages, me = me,
      custom_cov_list = custom_covs, polynoms = polynoms, n_mods = n_mods, plot_mods = plot_aves,
      age_trend = age_trend, plot_mods_path = plot_mods_path, username = username, proj,
      weight_col = ifelse(rank_method == "oos.rmse", "out_rmse", "aic"), by_sex = by_sex,
      location_set_id = 22, release_id = release_id
    )

    setnames(ave_models, "ave_result", "cv_custom_stage_1")

    # drop some uneccessary cols for saving
    ave_models[, c("location_name", "region_name", "super_region_name", "age_group_name") := NULL]

    # save
    write.csv(ave_models, file = ensemble_output, row.names = F)
    message(paste0("Success! Ensemble prior saved here: ", ensemble_output))

  }

  #######################
  ## CREATE BETA PLOTS ##
  #######################

  if (plot_betas == T) {

    source(paste0(working_dir, "plot_prior_betas.R"))

  }

}
