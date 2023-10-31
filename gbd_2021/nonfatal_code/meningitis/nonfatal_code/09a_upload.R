#####################################################################################################################################################################################
#####################################################################################################################################################################################
##                                                                                                                                                                                 ##
## Purpose:		This sub-step template is for parallelized jobs submitted from main step code                                                                                        ##
## Author:	USERNAME                                                                                                              ##                                                                                                                                                          ##
## Description:	Parallelization of 09_collate_outputs                                                                                                                              ##
##                                                                                                                                                                                 ##
#####################################################################################################################################################################################
#####################################################################################################################################################################################
rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- paste0("FILEPATH")
} else { 
  j <- "FILEPATH"
  h <- "FILEPATH"
}

# LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION) ----------------
# Load functions and packages
pacman::p_load(R.utils, openxlsx, data.table)

# Get arguments from R.utils version of commandArgs
args <- commandArgs(trailingOnly = TRUE, asValues = TRUE)
print(args)
list2env(args, environment()); rm(args)

# Source shared functions-------------------------------------------------------
invisible(sapply("FILEPATH"))

# User specified options -------------------------------------------------------
bundle <- 28
# Use viral bundle if viral meningiits
if(meid == 24161) bundle <- 833
mark_best <- FALSE
gbd_round <- as.integer(gbd_round)

# Get trackers
bundle_version_dir <- paste0("FILEPATH")
cv_tracker <- as.data.table(read.xlsx(paste0("FILEPATH")))
cv_row <- cv_tracker[bundle_id == bundle & current_best == 1]
crosswalk_version_id <- cv_row$crosswalk_version

# Get upload parameters
dim.dt <- fread(file.path(code_dir, paste0(cause, "_dimension.csv")))
parent_meid <- dim.dt[grouping == "cases" & acause == cause & healthstate == "_parent"]$modelable_entity_id

group <- unique(dim.dt[modelable_entity_id == meid, grouping])
state <- unique(dim.dt[modelable_entity_id == meid, healthstate])

# upload prev and incidence for acute meningitis (BACTERIAL AND VIRAL)
if (meid == 24068 | meid == 24161) {
  measure <- c(5,6)
} else {
  measure <- 5
}

# Get the best parent MEID for documentation
parent <- get_model_results('epi', 
                            parent_meid, 
                            location_id = 1,
                            gbd_round_id = gbd_round,
                            decomp_step = ds, 
                            status = 'best')
parent_mvid <- unique(parent$model_version_id)

# # Upload results ---------------------------------------------------------------

# Define patterns
my_filedir <- file.path(tmp_dir, '05b_sequela_split_woseiz', "03_outputs", "01_draws", cause, group, state)
my_file_pat <- "FILEPATH"
my_desc <- paste("viral update use AMR prop", cause, group, state, date, "from parent model version ID", parent_mvid)

# Set exceptions for hearing, vision, and epilepsy, whose output files remain in their original step directories
if (group %like% "hearing" | group %like% "vision"){
  my_filedir <- file.path(tmp_dir, '04e_outcome_prev_womort', '03_outputs', '01_draws', group, '_unsqueezed')
  my_desc <- paste(my_desc, "unsqueezed")
}
if(group %like% "epilepsy"){
  my_filedir <- file.path(tmp_dir, '04d_ODE_run', 'save_results_epilepsy', cause)
  my_file_pat <- "{location_id}.csv"
  my_desc <- paste(my_desc, "ODE epilepsy results")
}

# Set exception for acute bacterial and viral
if(state == "inf_sev" & group == "cases"){
  my_filedir <- file.path(tmp_dir, '08a_acute_bacterial_and_viral', "03_outputs", "01_draws", "acute_bacterial")
}
if(state == "inf_sev" & group == "viral"){
  my_filedir <- file.path(tmp_dir, '08a_acute_bacterial_and_viral', "03_outputs", "01_draws", "acute_viral")
}

# Run save_results
save_results_epi(
  input_dir = my_filedir,
  input_file_pattern = my_file_pat,
  modelable_entity_id = meid,
  description = my_desc,
  measure_id = measure,
  sex_id = c(1,2),
  decomp_step = ds,
  gbd_round_id = gbd_round,
  bundle_id = bundle,
  crosswalk_version_id = crosswalk_version_id,
  mark_best = mark_best
)
# # ------------------------------------------------------------------------------

# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
file.create("FILEPATH")