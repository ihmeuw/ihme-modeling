#!FILEPATH

##########################################################################################
####### Steps the script perform:
####### 1. Launch qsub parallelized by locs to generate corrected prevalence 
####### for HF etiologies (23 total - 21 CVD-processed causes & 3 Chagas severity causes) 
####### 2. Save_results 
##########################################################################################

#######################################
## PREP ENVIRONMENT & GLOBAL VAR
#######################################
rm(list = ls())

if (Sys.info()[1] == "Linux") folder_path <- "FOLDER" 
if (Sys.info()[1] == "Windows") folder_path <- "FOLDER"

# source the central functions
source(paste0(folder_path, "/FUNCTION_PATH/get_draws.R"))
source(paste0(folder_path, "/FUNCTION_PATH/get_location_metadata.R"))
source(paste0(folder_path, "/FUNCTION_PATH/get_demographics.R"))
source(paste0(folder_path, "/FUNCTION_PATH/save_results_epi.R"))

# add libs
library(data.table)
library(foreign)
library(parallel)
library(rhdf5)

# get choices for locations
locations <- get_location_metadata(location_set_id=9, gbd_round_id=5)
dismod_locs <- unique(get_demographics('epi')$location_id)
loc_choices <- locations[location_type_id==2 | location_id %in%dismod_locs]$location_id

# cvd output folder
cvd_path = "/CVD_FOLDER"

# hf etiology map
composite <- fread(paste0(cvd_path, '/composite_cause_list.csv'))

# delete all existing files from output folder
cleanup = F
if (cleanup==T){
  for (folder in unique(composite$prev_folder)){
    do.call(file.remove, list(list.files(paste0(cvd_path, "/post_dismod/1_corrected_prev_draws/version/", folder), pattern='.csv', full.names = TRUE))) 
  }
}

# qsub diagnostic file paths
qsub_output = paste0("/LOG_FOLDER/postdismod/output") 
qsub_errors = paste0("/LOG_FOLDER/postdismod/errors") 

#######################################
## RUNS CHILD SCRIPT IN PARALLELIZATION
#######################################
file_list <- NULL

for (loc in loc_choices){
  if (Sys.info()[["user"]]!="EXTRACTOR") {
    command <- paste("qsub -P proj_custom_models -l mem_free=4g -pe multi_slot 2 -N ",
                     paste0("hfpost_", loc),
                     paste0("-o ", qsub_output, " -e ", qsub_errors, " /CODE_REPO/post_dismod_submitProcessing.sh /CODE_REPO/post_dismod_processing.R"),
                     loc)
  } 
  system(command)
  #if (loc %in% seq(50,700,50)) Sys.sleep(100) # submit in batches to not overwhelm gbd db
}

# wait 20 min then relaunch slow/failed jobs
Sys.sleep(1200)

folder_list <- unique(na.omit(composite[!cause_name%like%'Chagas']$prev_folder))
for (loc in loc_choices){
  for (folder in folder_list) {
    if (folder !=""){
      output_path <- paste0(cvd_path, '/post_dismod/1_corrected_prev_draws/version/', folder , '/', loc, '.csv')
      file_list <- c(file_list, output_path)
    }
  }
}
## Wait until all jobs are done
job_hold("hfpost_", file_list = file_list)

# Stop if file missing
for (file in file_list){
  if (!file.exists(file)) (stop(paste0('Missing ', file)))
}

#######################################
## UPLOAD PREV TO APPRORIATE MES
#######################################
save_results <- T
if (save_results ==T){
  upload_me_list <- unique(na.omit(composite$prev_me_id))
  exclude <- NA 
  
  if (!is.na(exclude)){
    upload_me_list <- upload_me_list[!upload_me_list%in%exclude]
  }
  for (me in upload_me_list){
    if (Sys.info()[["user"]]!="EXTRACTOR") {
      command <- paste("qsub -P proj_custom_models -l mem_free=4g -pe multi_slot 20 -N ",
                       paste0("hfsave_", me),
                       paste0("-o ", qsub_output, " -e ", qsub_errors, " /CODE_REPO/post_dismod_submitProcessing.sh /CODE_REPO/save_hfprev_results.R"),
                       me)
    } 
    system(command)
  }
}