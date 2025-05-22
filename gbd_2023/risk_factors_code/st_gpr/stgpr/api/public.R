# Top-level public stgpr functions. Reticulate pass-throughs of public Python functions

#' Load stgpr module via reticulate
#'
#' Checks environment variables for optional override of conda env
load_stgpr <- function() {
	CENTRAL_CONDA_ENV = "FILEPATH"
	USE_CURRENT_ENV <- Sys.getenv(
	    "STGPR_USE_CURRENT_ENV",
	    unset = "0"
	)
	CONDA_PREFIX <- Sys.getenv(
	    "CONDA_PREFIX",
	    unset = CENTRAL_CONDA_ENV
	)
	CONDA_ENV = ifelse(USE_CURRENT_ENV != "0", CONDA_PREFIX, CENTRAL_CONDA_ENV)

	# Activating a conda env and importing stgpr via reticulate is slow, ~15 seconds,
	# so let users know what's happening
	message(paste("Loading ST-GPR public functions from", CONDA_ENV, "via reticulate..."))
	reticulate::use_condaenv(CONDA_ENV, required = TRUE)
	return(reticulate::import("stgpr"))
}

stgpr <- load_stgpr()
rm(load_stgpr)  # Clean-up: don't leave load function floating around

# NOTE: register_stgpr_model and stgpr_sendoff should get replaced with pass-through wrappers
# to Reticulate.

register_stgpr_model <- function(path_to_config, model_index_id = NULL){
###################################################################################################
#   Create an ST-GPR model version. Populates run root with needed inputs
# 	for model flow given the settings related in the config.
#   
#   Args:
#   
#   * path_to_config (str) : A path (available on the cluster) to an ST-GPR model config,
#                           with all entries filled out in ways that will pass validations.
#                           See example configs on the HUB if your config fails validations!
#   
#   * model_index_id (int) : In the config, there should be a column called model_index_id with unique
#                         integer values for each entry in the config. Specify model_index_id to
#                         indicate which row of the config you want to upload.
#
###################################################################################################
    source("FILEPATH")
    register_stgpr_model_helper(path_to_config, model_index_id)
}

stgpr_sendoff <- function(
    run_id,
    project,
    log_path = NULL,
    nparallel = NULL
){
###################################################################################################
# Function to submit ST-GPR models to the cluster! This is it!
# This is where it all begins!
# 
# Args:
#
# * run_id (int) : A legit ST-GPR run_id
#   
# * project (str) : The cluster project to run jobs from (ex. proj_custom_models). If
#                           you don't know what this is, ask your PO.
# 
# * nparallel (int) : Number of parallelizations to split your data over (by location_id).
#                     TLDR; set to 50 for small datasets, 100 for large datasets
# 
#                     More parallelizations means each job is running fewer locations,
#                     which makes each job faster. But if the cluster is swamped,
#                     it takes more time for each of these jobs to get space,
#                     so that makes it slower...I usually just set to 50 for
#                     smaller datasets (ie all-age, both-sex models)
#                     or 100 for larger datasets (by-sex, by-age models)
# 
#                     Default: 50
# 
# * log_path (str) : Path to a directory for saving logfiles.
# 
# 
# It's time to toss the dice!
###################################################################################################
    source("FILEPATH")
    stgpr_sendoff_helper(run_id, project, log_path, nparallel)
}

#' Gets ST-GPR model status. See public.py for full docstring.
get_model_status <- function(version_id, verbose = TRUE) {
	return(stgpr$get_model_status(version_id, verbose))
}

#' Gets parameters. See public.py for full docstring.
get_parameters <- function(version_id, unprocess = TRUE) {
	params <- stgpr$get_parameters(version_id, unprocess)
	if (unprocess) {
		return(data.table::as.data.table(params))
	} else {
		return(params)
	}
}

#' Gets input data. See public.py for full docstring.
get_input_data <- function(
	version_id,
	data_stage_name,
	year_start = NULL,
	year_end = NULL,
	location_id = NULL,
	sex_id = NULL,
	age_group_id = NULL
) {
	return(data.table::as.data.table(stgpr$get_input_data(
		version_id, data_stage_name, year_start, year_end, location_id, sex_id, age_group_id
	)))
}

#' Gets estimates of a particular stage for an ST-GPR version. See public.py for full docstring.
get_estimates <- function(
	version_id,
	entity,
	year_start = NULL,
	year_end = NULL,
	location_id = NULL,
	sex_id = NULL,
	age_group_id = NULL
) {
	return(data.table::as.data.table(stgpr$get_estimates(
		version_id, entity, year_start, year_end, location_id, sex_id, age_group_id
	)))
}

#' Gets custom covariates associated with a model. See public.py for full docstring.
get_custom_covariates <- function(
	version_id,
	year_start = NULL,
	year_end = NULL,
	location_id = NULL,
	sex_id = NULL,
	age_group_id = NULL
) {
	custom_covariates <- stgpr$get_custom_covariates(
		version_id, year_start, year_end, location_id, sex_id, age_group_id
	)
	if (is.null(custom_covariates)) {
		return(NULL)
	} else {
		return(data.table::as.data.table(custom_covariates))
	}
}

#' Gets ST-GPR versions associated with the given arguments. See public.py for full docstring.
get_stgpr_versions <- function(
	version_id = NULL,
	modelable_entity_id = NULL,
	release_id = NULL,
	model_status_id = NULL,
	bundle_id = NULL,
	crosswalk_version_id = NULL
) {
	return(data.table::as.data.table(stgpr$get_stgpr_versions(
		version_id,
		modelable_entity_id,
		release_id,
		model_status_id,
		bundle_id,
		crosswalk_version_id
	)))
}
