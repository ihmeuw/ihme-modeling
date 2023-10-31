#
# 06_upload_models.R
# Save the mrbrt models to a central folder so  can upload them to the visualization tool
#
#
library(data.table)
library(dplyr)
library(mrbrt001, lib.loc = "FILEPATH")

# set seed
np <- import("numpy")
np$random$seed(as.integer(2738))
set.seed(2738)
## 
diet_ro_pair_map <- "FILEPATH/diet_ro_map.csv"
results_folder <- "FILEPATH"

if(interactive()){
  
  version <- "VERSION"
  out_dir <- paste0("FILEPATH", version, "/")
  WORK_DIR <- "FILEPATH"
  
}else{
  args <- commandArgs(trailingOnly = TRUE)
  out_dir <- args[2]
  WORK_DIR <- args[3]
  
}

setwd(WORK_DIR)
source(paste0(out_dir, "/config.R"))
source("FILEPATH/upload_continuous_functions.R")


#
diet_ro_pair <- fread(diet_ro_pair_map)
diet_ro_pair[, risk_cause_id := paste0(risk_cause, "_", cause_id)]
included_ro_pairs <- diet_ro_pair[include==1, risk_cause_id]


for(ro_pair in included_ro_pairs){
  
  # make clean name 
  ro_pair_clean <- diet_ro_pair[risk_cause_id == ro_pair, risk_cause]
  
  # name folder accordingly 
  if(ro_pair_clean %like% "hemstroke"){
    ro_results_folder <- file.path(results_folder, ro_pair)
  }else{
    ro_results_folder <- file.path(results_folder, ro_pair_clean)
  }
  
  # make results folder
  if (!dir.exists(ro_results_folder)) {
  dir.create(ro_results_folder)}
  
  # make units correct for pufa and transfat
  unit <- "g/day"
  if(ro_pair %like% "pufa" | ro_pair %like% "transfat"){
    unit <- "%E/day"
  }
  

  a <- upload_results( signal_model_path = paste0(out_dir, "/01_template_pkl_files/", ro_pair_clean, ".pkl"),
                  linear_model_path = paste0(out_dir, "/04_mixed_effects_pkl_files/", ro_pair_clean, ".pkl"), 
                  results_folder = ro_results_folder,
                  rei_id = diet_ro_pair[risk_cause_id == ro_pair, rei_id],
                  cause_id=diet_ro_pair[risk_cause_id == ro_pair, cause_id],
                  risk_unit = unit,
                  age_group_id = 22,
                  sex_id = 3,
                  location_id = 1,
                  normalize_to_tmrel = ro_pair_clean %in% J_SHAPE_RISKS)
  

}

