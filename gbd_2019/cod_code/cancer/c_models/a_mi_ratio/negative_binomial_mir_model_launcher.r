##################################################################################
# Name of Script: negative_binomial_mir_model_launcher.R                         #
# Description: Launches negative binomial regression on mirs for new causes      #
# Arguments: list of causes                                                      #
# Output: check worker script                                                    #
##################################################################################

####################################
## Set up workspace and libraries  #
####################################
rm(list=ls())

# set working directories by operating system
user <- Sys.info()[["user"]]
if (Sys.info()["sysname"]=="Linux") {
  j_root <- "/home/j" 
  h_root <- file.path("/homes", user)
} else if (Sys.info()["sysname"]=="Darwin") {
  j_root <- "~/J"
  h_root <- "~/H"
} else { 
  j_root <- "J:"
  h_root <- "H:"
}

if (!exists("code_repo")) code_repo <- sub("cancer_estimation.*", 'cancer_estimation', here())
if (!grepl("cancer_estimation", code_repo)) code_repo <- file.path(code_repo, 'cancer_estimation')

source(file.path(code_repo, '/r_utils/cluster_tools.r'))
source(file.path(code_repo, 'r_utils/utilities.r'))

if (!interactive()) {
  mir_version <- commandArgs()[6]
}else{
  mir_version <- 74
}

# our list of causes to run worker script on 
new_causes <- list(c("neo_bone", "neo_bone_VR"), c("neo_eye", "neo_eye_VR"), c("neo_eye_other", "neo_eye_other_VR"),
                   c("neo_eye_rb", "neo_eye_rb_VR"), c("neo_lymphoma_burkitt", "neo_lymphoma_burkitt_VR"),
                   c("neo_lymphoma_other", "neo_lymphoma_other_VR"), c("neo_neuro", "neo_neuro_VR"),
                   c("neo_tissue_sarcoma", "neo_tissue_sarcoma_VR"), c("neo_liver_hbl", "neo_liver_hbl_VR"))

for(cancer in new_causes){
  launch_jobs(<FILEPATH>,
              script_arguments = c(cancer, mir_version), memory_request = 30, 
              time = "5:00:00", num_threads = 20,
              output_path = <FILEPATH>,
              error_path = <FILEPATH>,
              job_header = paste0("mir_age_", cancer)) 
} 
