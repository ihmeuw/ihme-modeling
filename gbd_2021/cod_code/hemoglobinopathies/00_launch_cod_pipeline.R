##' *************************************************************************************
##' Title: 00_launch_cod_pipeline.R
##' Purpose: Save custom cod model results 
##' *************************************************************************************
rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

library('openxlsx')
library('tidyr')
library('dplyr')
library('data.table')
library('matrixStats')


source("FILEPATH")
source("FILEPATH")

years <- c(1980:2022)
gbd_round <- 7
decomp_step <- 'step3'
repo_dir <- "FILEPATH"
out_dir <- "FILEPATH"
causes <- c(614, 615, 616, 618)
date <- Sys.Date() 
date <- gsub('-', '_', date)


##' *******************************************************
##' 1. Run pooling for other hemog
##' @param repo_dir
##' @param gbd_round
##' @param decomp_step
##' @param out_dir
##' *******************************************************
pool_script <- paste0(repo_dir, "1_run_pooling.R")

pool_job_name <- "pooling_other_hemog_618"

system(paste("qsub -P proj_nch -l m_mem_free=5G -l fthread=2 -q all.q  -N", pool_job_name,
             "-e FILEPATH", "-o FILEPATH", "FILEPATH", "-s ", 
             pool_script, gbd_round, decomp_step, repo_dir, out_dir))


##' *******************************************************
##' 2. Pull and interpolate DisMod results
##' @param param_map
##' *******************************************************
lm <- get_location_metadata(location_set_id=35, gbd_round_id=gbd_round)


param_map$location_id <- unique(lm$location_id)
param_map$gbd_round <- 7
param_map$decomp_step <- 'iterative'
param_map$repo_dir <- repo_dir
param_map$out_dir <- out_dir


param_map_filepath <- "FILEPATH" ## these are ALL cause of death locations
write.csv(param_map, param_map_filepath, row.names = F)

## QSUB array job
job_name <- "interpolate_hemog"
thread_flag <- "-l fthread=1"
mem_flag <- "-l m_mem_free=10G"
runtime_flag <- "-l h_rt=18:00:00"
jdrive_flag <- "-l archive"
queue_flag <- "-q long.q"
throttle_flag <- "-tc 800"
n_jobs <- paste0("1:", nrow(param_map))
prev_job <- pool_job_name #hold until this job completes
next_script <- paste0(repo_dir, "2_run_interpolation.R") #run_interpolatation.R
error_filepath <- "FILEPATH"
output_filepath <- "FILEPATH"
project_flag<- "-P proj_nch"

qsub_command <- paste( "qsub", thread_flag, "-N", job_name, project_flag, mem_flag, runtime_flag, throttle_flag, queue_flag, 
                       "-t", n_jobs, "-e", error_filepath, "-o", output_filepath, "-hold_jid", prev_job,  
                       "FILEPATH -i FILEPATH -s", 
                       next_script, param_map_filepath )
system(qsub_command)


interp_hold <- "interpolate_hemog_*"

##' *******************************************************
##' 3. Save unscaled results
##' @param mark_best_flag
##' @param sex_id
##' @param save_description
##' @param input_file_pattern
##' @param causes
##' *******************************************************

scaled_results <- FALSE
mark_best_flag <- FALSE
save_description <- paste0("Hemog_Save_Unscaled_", date)
sex_ids <- c(1,2)
input_file_pattern = "interp_hemog_{location_id}.csv"

save_script <- paste0(repo_dir, "3_run_save_hemog.R")

save_hold <- c()


for (cause in causes){
  for (sex_id in sex_ids){
    save_script_name <- paste0("saving_hemog_unscaled_", cause)
    
    qsub_command <- paste("qsub -hold_jid", interp_hold, "-P proj_nch -l m_mem_free=110G -l fthread=15 -q long.q  -N", save_script_name, 
                          "-e FILEPATH", "-o FILEPATH", "FILEPATH", "-s ",
                          save_script, cause, sex_id, 
                          gbd_round, decomp_step, out_dir, mark_best_flag, save_description, input_file_pattern, scaled_results)
    
    system(qsub_command)
    
    save_hold <- paste0(save_hold, ',', save_script_name)
    
  }
}


##' *******************************************************
##' 4. Scale results (squeezes interpolated Dismod results to parent hemog CoD model)
##' Duration 
##' @param param_map
##' *******************************************************


## QSUB array job
job_name <- "scaling_hemog"
thread_flag <- "-l fthread=1"
mem_flag <- "-l m_mem_free=10G"
runtime_flag <- "-l h_rt=18:00:00"
jdrive_flag <- "-l archive"
queue_flag <- "-q all.q"
throttle_flag <- "-tc 800"
n_jobs <- paste0("1:", nrow(param_map))
prev_job <- save_hold #hold until this job completes
next_script <- paste0(repo_dir, "4_run_scaling.R") #run_scaling.R
error_filepath <- "FILEPATH"
output_filepath <- "FILEPATH"
project_flag<- "-P proj_nch"

qsub_command <- paste( "qsub", thread_flag, "-N", job_name, project_flag, mem_flag, runtime_flag, throttle_flag, queue_flag, 
                       "-t", n_jobs, "-e", error_filepath, "-o", output_filepath, "-hold_jid", prev_job,  
                       "FILEPATH -i FILEPATH -s", 
                       next_script, param_map_filepath )
system(qsub_command)

scale_hold <- "scaling_hemog_*"

##' ******************************************************************
##' 5. Replace Dismod scaled results with cod data from data rich locs
##' Duration 
##' @param param_map
##' @param version_map
##' ******************************************************************
version_map <- read.xlsx(paste0(out_dir, '/data_rich_version_map.xlsx')) ### This must be edited each time

dr_locs <- get_location_metadata(location_set_id = 43, gbd_round_id = gbd_round) # loc set 43 is for data dense/data sparse specifications in CoD
dr_locs <- dr_locs[parent_id == 44640]

###########################################################################

param_map_2$location_id <- unique(dr_locs$location_id)
param_map_2$gbd_round <- 7
param_map_2$decomp_step <- 'step3'
param_map_2$repo_dir <- repo_dir
param_map_2$out_dir <- out_dir
param_map_2 <- as.data.frame(param_map_2)

param_map_filepath_2 <- "FILEPATH" ## these are only DR locs
write.csv(param_map_2, param_map_filepath_2, row.names = F)

## QSUB array job
job_name <- "datarich_hemog_sub"
thread_flag <- "-l fthread=1"
mem_flag <- "-l m_mem_free=10G"
runtime_flag <- "-l h_rt=18:00:00"
jdrive_flag <- "-l archive"
queue_flag <- "-q long.q"
throttle_flag <- "-tc 800"
n_jobs <- paste0("1:", nrow(param_map_2))
prev_job <- scale_hold #hold until this job completes
next_script <- paste0(repo_dir, "5_run_synthesis.R") #run_synthesis.R
error_filepath <- "FILEPATH"
output_filepath <- "FILEPATH"
project_flag<- "-P proj_nch"

qsub_command <- paste( "qsub -hold_jid", prev_job, thread_flag, "-N", job_name, project_flag, mem_flag, runtime_flag, throttle_flag, queue_flag, 
                       "-t", n_jobs, "-e", error_filepath, "-o", output_filepath,   
                       "FILEPATH -i FILEPATH -s",  
                       next_script, param_map_filepath_2 )
system(qsub_command)

synth_hold <- "data_rich_hemog_sub_*"


##' *****************************************************************
##' 6. Save scaled results as a custom cod model
##' @param mark_best_flag
##' @param decomp_step
##' @param save_script
##' @param save_description
##' @param causes
##' @param sex_ids
##' *****************************************************************
all_wait_scripts <- paste0(save_hold, ",", synth_hold)


scaled_results <- TRUE
mark_best_flag <- TRUE
save_description <- paste0("Hemog_Save_Scaled_", date)
sex_ids <- c(1,2)
decomp_step <- 'step3'
save_script <- paste0(repo_dir, "3_run_save_hemog.R")


for (cause in causes){
  for (sex_id in sex_ids){
    
    save_script_name <- paste0("saving_hemog_scaled_", cause, "_", sex_id)
    
    input_file_pattern = "{sex_id}/final_hemog_{location_id}.csv"
    
    
    system(paste("qsub -P proj_nch -l m_mem_free=90G -l fthread=15 -q all.q  -N", save_script_name, 
                 "-e FILEPATH", "-o FILEPATH", 
                 "FILEPATH", "-s ", save_script, cause, sex_id, 
                 gbd_round, decomp_step, out_dir, mark_best_flag, save_description, input_file_pattern, scaled_results))
    
  } 
}


