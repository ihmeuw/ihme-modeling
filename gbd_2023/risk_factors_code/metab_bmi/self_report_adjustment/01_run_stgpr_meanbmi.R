################################################################################
## DESCRIPTION: launch st-gpr models for self-report adjustment
## INPUTS: Mean BMI datasets ##
## OUTPUTS: STGPR model version country, region, and super region level for crosswalk ##
## AUTHOR: ##
## DATE:
################################################################################
rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "FILEPATH"

# Base filepaths
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""

## LOAD DEPENDENCIES
source(paste0(code_dir, 'FILEPATH'))

## Load configuration for meta data variables
load_config(paste0(code_dir, 'FILEPATH'), c('gbd', 'qsub', 'bmi'))

# Set arguments
args <- commandArgs(trailingOnly = TRUE)
bmi_bundle_version_ids <- as.numeric(args[1])
data_dir <- args[2]

# Read in data tracker sheet
tracking <- fread("FILEPATH/mean_bmi_data_tracker.csv")

## LOAD DEPENDENCIES
source(paste0(code_dir, 'FILEPATH'))
source("FILEPATH/plot_gpr.R")
source("FILEPATH/stgpr/api/public.R") 

# register the models
stgpr_ids <- NULL
cluster_project <- "PROJECT"
save_dir <- "FILEPATH"

## Run model based on config file specifications
for (run_id in c(1:30)) {
  stgpr_id <- register_stgpr_model(  paste0(save_dir, "mean_bmi_cat_config.csv"),
                                     model_index_id = run_id)
  Sys.sleep(2)
  stgpr_ids <- c(stgpr_ids, stgpr_id)
  
  # run stgpr
  stgpr_sendoff(stgpr_id, cluster_project)
  Sys.sleep(10)
}

# collapse the ids
run_ids <- paste(stgpr_ids, collapse=",")

# Save to tracking document
tracking <- fread("FILEPATH/mean_bmi_data_tracker.csv")

# tracking_temp <- tracking[bundle_id == 10106]
tracking_temp <- tail(tracking,30)
tracking_temp[, `:=` (stgpr_run_id = stgpr_ids)]
tracking <- rbind(tracking[!(crosswalk_version_id %in% tracking_temp$crosswalk_version_id)], tracking_temp, fill = T)

fwrite(tracking, "FILEPATH/mean_bmi_data_tracker.csv")
