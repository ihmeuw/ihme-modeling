#####################################################################################################################################################################################
#####################################################################################################################################################################################
##                                                                                                                                                                                 ##
## Purpose:		This sub-step template is for parallelized jobs submitted from main step code                                                                                        ##
## Description:	Parallelization of 09_collate_outputs                                                                                                                              ##
##                                                                                                                                                                                 ##
#####################################################################################################################################################################################
#####################################################################################################################################################################################
rm(list=ls())

# LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION) ----------------
# Load functions and packages
pacman::p_load(R.utils, openxlsx, data.table)

# Get arguments from R.utils version of commandArgs
args <- commandArgs(trailingOnly = TRUE, asValues = TRUE)
print(args)
list2env(args, environment()); rm(args)

# Source shared functions-------------------------------------------------------
invisible(sapply(list.files("FILEPATH", full.names = T), source))

# User specified options -------------------------------------------------------
bundle <- 28
# Use viral bundle if viral meningitis
if(meid == 24161) bundle <- 833
mark_best <- TRUE
release <- as.integer(release)

# Get trackers
bundle_version_dir <- paste0("FILEPATH")
cv_tracker <- as.data.table(read.xlsx("FILEPATH"))
cv_row <- cv_tracker[bundle_id == bundle & current_best == 1]
crosswalk_version_id <- cv_row$crosswalk_version

# Get upload parameters
dim.dt <- fread(file.path(code_dir, paste0("FILEPATH")))
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
                            release_id = release,
                            status = 'best')
parent_mvid <- unique(parent$model_version_id)

# # Upload results ---------------------------------------------------------------

# Define patterns
my_filedir <- file.path("FILEPATH")
my_file_pat <- "FILEPATH"
my_desc <- paste("Using AMR prop from release_id",release, cause, group, state, date, "from parent model version ID", parent_mvid, ". Running with ratios from GBD21.")

# Set exceptions for hearing, vision, and epilepsy, whose output files remain in their original step directories
if (group %like% "hearing" | group %like% "vision"){
  my_desc <- paste(my_desc, "unsqueezed")
}

# Set exception for acute bacterial and viral
if(state == "inf_sev" & group == "cases"){
  my_filedir <- file.path("FILEPATH")
}
if(state == "inf_sev" & group == "viral"){
  my_filedir <- file.path("FILEPATH")
}

# Pull parent bacterial meningitis for me_id 24068 (acute bacterial meningitis) as they should be identical
if(meid == 24068){
  
  # Copy and rename the parent dismod file for upload
  file <- list.files("FILEPATH", full.names = TRUE, pattern = "meningitis_dismod_")
  parent <- fread(file)
  parent <- parent[, modelable_entity_id := 24068]
  fwrite(parent, paste0("FILEPATH"))
  
  # Run save_results
  model_info <- save_results_epi(
    input_dir = paste0("FILEPATH"),
    input_file_pattern = "all_draws.csv",
    modelable_entity_id = meid,
    description = paste0("Copy of parent bacterial meningitis model version id: ", parent$model_version_id),
    measure_id = measure,
    sex_id = c(1,2),
    release_id = release,
    bundle_id = bundle,
    crosswalk_version_id = crosswalk_version_id,
    mark_best = mark_best
    
  )
} else {

# Run save_results
model_info <- save_results_epi(
  input_dir = my_filedir,
  input_file_pattern = my_file_pat,
  modelable_entity_id = meid,
  description = my_desc,
  measure_id = measure,
  sex_id = c(1,2),
  release_id = release,
  bundle_id = bundle,
  crosswalk_version_id = crosswalk_version_id,
  mark_best = mark_best
)
}
# # ------------------------------------------------------------------------------

# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
file.create(paste0("FILEPATH"), overwrite=T)