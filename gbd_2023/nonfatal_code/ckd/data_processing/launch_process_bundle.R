##--------------------------------------------------------------
# Project: CKD data processing pipeline
# Purpose: Process and upload crosswalk versions for CKD bundles (except age splitting)
#--------------------------------------------------------------

rm(list=ls())
user <- Sys.getenv('USER')
# setup -------------------------------------------------------------------

if (Sys.info()['sysname'] == 'Linux') {
  j_root <- 'FILEPATH' 
  h_root <- 'FILEPATH'
} else { 
  j_root <- 'FILEPATH'
  h_root <- 'FILEPATH'
}

ckd_repo<- paste0("FILEPATH", user,"FILEPATH")
source(paste0(ckd_repo, "general_func_lib.R"))
library(data.table)

# Set release id
release_id <-16

# to run (1) or not run (0) data processing (within-study age-sex split, sex split, crosswalk)
run_process <- 1

# to save (1) or not (0) a processed but not age split crosswalk version and/or an age-specific-data-only crosswalk version (for modeling age pattern)
save_cx_pre_age_split <- 1
save_cx_age_spec <- 0

# Set bundles to run for
# ckd_bids<-c(670, 182, 183, 186, 760, 185, 184, 551, 3110, 3113, 552, 553, 554)
ckd_bids<-c(10528) 

# Pull in bundle version and crosswalk version id map (must manually update ids first!)
process_cv_ref_map_path <- paste0(j_root,"FILEPATH/process_cv_reference_map.csv")
bvids<-fread(process_cv_ref_map_path)
cvids <- bvids

# Supply path to .csv mapping bundle ids to crosswalks 
cv_ref_map_path<-paste0(j_root,"FILEPATH/stage_cv_reference_map.csv")

# Supply path to .csv mapping bundle ids to sex splitting model settings
sex_split_ref_map_path<-paste0(j_root,"FILEPATH/sex_split_reference_map.csv")


# Set path to script to launch
process_bundles<-paste0(ckd_repo,"FILEPATH/process_bundle.R")

# Specify usage params
threads <- 4
memory <- '20G'
runtime <- "20:00:00"
partition <- "long.q"
submit <- T

# LAUNCH
if(run_process==1) {
  for (bid in ckd_bids){
    bvid<-bvids[bundle_id==bid, bundle_version_id]
    acause<-bvids[bundle_id==bid, acause]
    output_file_name<-paste0("post_sex_split_and_crosswalk_bid_", bid, "_bvid_",bvid)
    pass_args<-list(bid, bvid, acause, release_id, output_file_name, cv_ref_map_path, save_cx_pre_age_split, 
                    save_cx_age_spec, sex_split_ref_map_path, process_cv_ref_map_path)
    job_name <- paste0("process_bundle_data_bid_",bid)
    
    construct_sbatch(
      memory = memory,
      threads = threads,
      runtime = runtime,
      script = process_bundles,
      job_name = job_name,
      partition = partition,
      pass = pass_args,
      submit = TRUE
    )
  }
}