#!/usr/local/bin/R
#########################################
## Description: Loads functions to calculate incidence data
## Input(s)/Output(s): see individual functions
## How To Use: intended for submission as a cluster job in the nonfatal pipeline (see "Run Functions" below),
##                  can also be sourced to retrieve results for a single cause-location_id
## Notes: See additional notes in the format_mi_draws script
#########################################
## load Libraries

source(file.path(h, 'cancer_estimation/utilities.R'))  # enables generation of common filepaths
source(get_path('handlers', process="nonfatal_model"))

## Run function if not using R interactively
if(!interactive()) {
  handle_script(script_name="load_mi_draws",remove_old_outputs=TRUE, what_to_iterate = load_location_list())
  handle_script(script_name="load_mortality_draws",remove_old_outputs=TRUE, what_to_iterate = load_location_list())
  handle_script(script_name="generate_incidence",remove_old_outputs=TRUE, what_to_iterate = load_location_list())
}
