##################################################################################
# Name of Script: negative_binomial_mir_model_launcher.R                         
# Description: Launches negative binomial regression on mirs for new causes      
# Arguments: 
#     cause_name: see list of new causes in launcher script
#     mir_version: see mir_model_version table mir_version_ids
#     draw_count: [0, 100, 1000]#
# Output: check worker script                                                    

##################################################################################

####################################
## Set up workspace and libraries  #
####################################
rm(list=ls())


user <- Sys.info()[["user"]]
if (Sys.info()["sysname"]=="Linux") {
  j_root <- "FILEPATH" 
  h_root <- file.path("/homes", user)
} else if (Sys.info()["sysname"]=="Darwin") {
  j_root <- "~/J"
  h_root <- "~/H"
} else { 
  j_root <- "J:"
  h_root <- "H:"
}

if (!exists("code_repo"))  {
  code_repo <-  sub("cancer_estimation.*", 'cancer_estimation', here::here())
  if (!grepl("cancer_estimation", code_repo)) code_repo <- file.path(code_repo, 'cancer_estimation')
}

source(file.path(code_repo, 'FILEPATH/cluster_tools.r'))
source(file.path(code_repo, 'FILEPATH/utilities.r'))

# can select the desired mir_version
REDACTED
if (!interactive()) {
  mir_version <- commandArgs()[6]
  draw_count <- commandArgs()[7]
  acause <- commandArgs()[8] # option to run it for certain causes
}else{
  ## Enter specs here to run for a single cause
  mir_version <- #REDACTED
  draw_count <- 1000
  acause <- "neo_eye_rb" 
  combine_age_types <- F #T or F
  mir_version_peds <- "" 
  mir_version_all_ages <- "" 
  description <- "REDACTED"
}

# our list of causes to run worker script on 
if(acause != ""){
  new_causes <- list(paste0(acause, c("","_VR")))
} else{
  new_causes <- list(c("neo_bone", "neo_bone_VR"),
                     c("neo_eye_other", "neo_eye_other_VR"),
                     c("neo_eye_rb", "neo_eye_rb_VR"), 
                     c("neo_lymphoma_burkitt", "neo_lymphoma_burkitt_VR"),
                    c("neo_neuro", "neo_neuro_VR"),
                     c("neo_tissue_sarcoma", "neo_tissue_sarcoma_VR"),
                     c("neo_liver_hbl", "neo_liver_hbl_VR"),
                    c("neo_ben_brain", "neo_ben_brain_VR"))
  
  
  #REDACTED
}

# running jobs for each of new causes to generate the mir estimate summaries (draws optional)
for(cancer in new_causes){
  
  # settings for shorter run time causes
  if(cancer[1] %in% c("neo_eye_rb", "neo_liver_hbl")) time_req <- "06:00:00"
  else time_req <- "15:00:00"
  
  launch_jobs("FILEPATH.R",
              script_arguments = c(cancer, mir_version, as.numeric(draw_count), as.logical(combine_age_types), mir_version_peds, mir_version_all_ages, description), memory_request = 45, 
              time = time_req, num_threads = 10,
              output_path = paste0(get_path(key = "new_mir_tempFolder", process = "mir_model"), "FILEPATH"),
              error_path = paste0(get_path(key = "new_mir_tempFolder", process = "mir_model"), "FILEPATH"),
              job_header = paste0("NB_mir_", cancer))
} 



## ls -1 | grep '<DESCRIPTION>' | wc -l
