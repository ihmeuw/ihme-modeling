#*******************************************************************************
#----HEADER---------------------------------------------------------------------
# Author:       REDACTED
# Date:         June 2020, updated October 2023
# Purpose:      SEX AND AGE SPLIT CFR DIPHERIA SYSTEMATIC REVIEW DATA
#               subset to age specific and save second crosswalk version to 
#               run DisMod age pattern model
# PART A: SET UP
# PART B: SEX SPLIT DATA
# PART C: PREPARE DATA FOR DISMOD AGE PATTERN GENERATION
# PART D: PREPARE DATA FOR DISMOD SEX AND AGE SPECIFIC CFR MODEL
#*******************************************************************************

#PART A: SETUP------------------------------------------------------------------

## EMPTY THE ENVIRONMENT
rm(list = ls())

## SET UP FOCAL DRIVES
os <- .Platform$OS.type
if (os=="windows") {
  j <-"J:/"
  h <-"H:/"
} else {
  j <-"/FILEPATH/"
  h <-paste0("FILEPATH/", Sys.info()[7], "/")
}

## Load MR_BRT and XW modules: 
Sys.setenv("RETICULATE_PYTHON" = "/FILEPATH/mrtool_0.0.1/bin/python")
reticulate::use_python("/FILEPATH/mrtool_0.0.1/bin/python") 
library(reticulate)
mr <- import("mrtool")
xwalk <- import("crosswalk")

## LIBRARIES
pacman::p_load(openxlsx, ggplot2, data.table)
library(dplyr)
library(stringr)
library(tidyr)
library(boot)
library(crosswalk002) # this only works in singularity 4222!
library(data.table)
library(mrbrt003) #ignore error message
setDTthreads(1)

## SOURCE FUNCTIONS
source("FILEPATH/get_ids.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_population.R")
source('FILEPATH/save_bundle_version.R')
source('FILEPATH/get_bundle_version.R')
source('FILEPATH/save_crosswalk_version.R')
source('FILEPATH/get_crosswalk_version.R')
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_elmo_ids.R")

## custom helpers: 
source("/FILEPATH/within_study_sex_splitting.R")
source("/FILEPATH/apply_age_crosswalk_copy.R") 
source("/FILEPATH/mrbrt_sex_split.R")
source("/FILEPATH/add_population_cols_fxn.R")

## SET OBJECTS
release_id <- 16
bundle_id <- 407
acause <- "diptheria"
date <- gsub("-", "_", Sys.Date())
xw_measure <- "proportion"
dismod_locs <- get_location_metadata(location_set_id = 9, release_id = release_id)

# rerun mrbrt sex splitting model or use weights from previous model? should rerun if any of the sex specific data has changed
use_prev_mrbrt <- FALSE

#Do you want to run a new age pattern model? Default is TRUE. This needs to stay TRUE if any changes to the data were made. 
run_new_age_pattern <- TRUE

# create directories
out_dir <- paste0(j, "FILEPATH", "/") 
if(!(dir.exists(out_dir))) dir.create(out_dir, recursive = T)

upload_dir <- paste0(j, "FILEPATH/")
path_to_sex_crosswalk <- paste0(upload_dir, date,"_", release_id, "sex_split_no_age_split.xlsx")
path_to_dismod_age_data <- paste0(upload_dir, date,"_", release_id, "_dismod_age_pattern_data.xlsx")
path_to_final_xw <- paste0(upload_dir, date, "_", release_id, "_sex_split_age_split_final_xw.xlsx")

# file path for bundle tracker:
bt_path <- paste0(j, "FILEPATH/gbd2022_bundle_tracker.csv") 

plot <- T
offset <- F
drop_zeros <- T
fix_ones <- F
inlier_pct <- 0.9

## change these to set model description
model_descrip <- "rerunning sex split from 07.17.24 to confirm data in model"
cat(date, model_descrip, sep="\n", file=paste0(out_dir, "model_details.txt"))

## don't change these!
sex_xw_descrip <- "sex split, no age split"
dismod_xw_descrip <- "for dismod age pattern model: sex split data, with age bin restriction"
final_xw_descrip <- "sex split, age split"

# get bundle tracker to find bundle vers to xw 
bundle_tracker <- fread(bt_path)
# bv_id <- unique(bundle_tracker[current_best_bv==1, bundle_version_id]) #use if most recent is marked best
bv_id <- max(bundle_tracker$bundle_version_id, na.rm = T) #if most recent is not marked best

#*******************************************************************************

# PART B: SEX SPLIT CFR DATA--------------------------------------------------
print(paste("Sex splitting on bundle version", bv_id))
cat(paste("Sex splitting on bundle version", bv_id), sep="\n", file=paste0(out_dir, "model_details.txt"), append=T)

data <- get_bundle_version(bundle_version_id = bv_id, fetch="all") 

## within study sex splitting
within_study_ss <- age_sex_split(data) 

#run sex split, from mrbrt_sex_split_script
sex_split <- run_sex_split(dt=within_study_ss, 
                           out_dir=out_dir, 
                           offset=offset, 
                           fix_ones=fix_ones, 
                           drop_zeros=drop_zeros, 
                           plot=plot,  
                           inlier_pct = inlier_pct,
                           use_prev_mrbrt = use_prev_mrbrt,
                           path_to_prev_mrbrt = NULL) 

sex_split_no_age_split <- sex_split$data
fwrite(sex_split_no_age_split, file=paste0(out_dir, "sex_split_data.csv"))

## Make the funnel plot
plots <- import("crosswalk.plots")

repl_python()
plots$funnel_plot(
  cwmodel = sex_results, 
  cwdata = formatted_data,
  continuous_variables = list(),
  obs_method = 'Female',
  plot_note = paste0('Sex splitting funnel plot: ', acause), 
  plots_dir = out_dir, 
  file_name = "funnel_plot",
  write_file = TRUE
)

## Remove locations not modelled in DisMod
sex_split_no_age_split <- sex_split_no_age_split[location_id %in% dismod_locs$location_id] 

# cap standard error at 1, all points that are coming back with se > 1 are outliers (and all have orig mean of 0) - 6 rows
sex_split_no_age_split[ standard_error > 1, standard_error :=1] 

# SAVE SEX SPLIT DATA
write.xlsx(sex_split_no_age_split, file=path_to_sex_crosswalk, sheetName="extraction", rowNames=FALSE, colNames=TRUE)

# SAVE CROSSWALK
sex_crosswalk <- save_crosswalk_version(bundle_version_id = bv_id, 
                                        data_filepath = path_to_sex_crosswalk, 
                                        description = paste(sex_xw_descrip,model_descrip, sep=" - "))

if(sex_crosswalk$request_status=="Successful"){
  tracker_temp <- data.table(bundle_id = bundle_id,
                             date = date,
                             bundle_version_id = bv_id,
                             crosswalk_id = sex_crosswalk$crosswalk_version_id,
                             is_bulk_outlier = 0, #0 toggle
                             descrip = paste(sex_xw_descrip,model_descrip, sep=" - "),
                             current_best_bv = 0,
                             current_best_xw = 0)
  bundle_tracker <- rbind(bundle_tracker, tracker_temp)
  fwrite(bundle_tracker, bt_path)
  
}

if(run_new_age_pattern){
  
## SUBSET SEX SPLIT DATA IN DESIRED AGE BINS
  dismod_age_data <- copy(sex_split_no_age_split)
  dismod_age_data <- dismod_age_data[, age_band := abs(age_end - age_start)]
  age_spec_subset <- dismod_age_data[age_band <= 10] 
  
  # save age bin restricted data as an as .xlsx file 
  openxlsx::write.xlsx(age_spec_subset, file=path_to_dismod_age_data, sheetName="extraction", rowNames=FALSE, colNames=TRUE)
  
  # upload age bin rescrited data to bundle with new crosswalk version
  dismod_xw <- save_crosswalk_version(bundle_version_id = bv_id, 
                                      data_filepath = path_to_dismod_age_data, 
                                      description = paste(dismod_xw_descrip,model_descrip, sep=" - "))
  
  if(dismod_xw$request_status=="Successful"){
    tracker_temp <- data.table(bundle_id = bundle_id,
                               date = date,
                               bundle_version_id = bv_id,
                               crosswalk_id = dismod_xw$crosswalk_version_id,
                               is_bulk_outlier = 0,
                               descrip = paste(dismod_xw_descrip,model_descrip, sep=" - "),
                               current_best_bv = 0, 
                               current_best_xw = 0)
    bundle_tracker <- fread(bt_path)
    bundle_tracker <- rbind(bundle_tracker, tracker_temp)
    fwrite(bundle_tracker, bt_path)
  }
}

#*******************************************************************************
# STOP!!! Launch DisMod CFR age pattern model in EpiViz. 
# When model is finished proceed with age splitting below. 
stop()
#*******************************************************************************

#*******************************************************************************
# PART C: APPLY AGE SPLIT --------------------------------------------------------------

# SET OBJECTS
run_date <- gsub("-", "_", Sys.Date()) #if age split done on date of sex split, otherwise enter date of run
path_to_final_xw <- paste0(upload_dir, run_date, "_", "release_id_", release_id, "_sex_split_age_split_final_xw.xlsx")

# get bundle version with sex split data
bundle_tracker <- fread(bt_path)
#ss_xw_row <- bundle_tracker[current_best_bv==1 & descrip %like% "sex split, no age split"]
ss_xw_row <- bundle_tracker[bundle_version_id==(max(bundle_version_id, na.rm = T)) & descrip %like% "sex split, no age split"] #if newest model is not best

# find date directory to save final split in - this assumes we are using most recent xw that is sex split
date_folder <- ss_xw_row[nrow(ss_xw_row), date]
# this directory already exists from generating when used mrbrt to sex split
out_dir <- paste0(j, "FILEPATH/")

# find bv and xw id of the data to be age split
ss_xw_id <- ss_xw_row[nrow(ss_xw_row), crosswalk_id]
bv_id <- ss_xw_row[nrow(ss_xw_row), bundle_version_id]

# age pattern model details, 
age_meid <- 24318
age_mvid <- 834504 #see epiviz for age pattern model version id

## get sex split data to age split
ss_data <- get_crosswalk_version(crosswalk_version_id = ss_xw_id) 

## split the data. this function appropriately sets crosswalk_parent_seq and clears seq for the rows that are split.
ss_age_split <- apply_age_split(data=ss_data,
                                dismod_meid = age_meid,
                                dismod_mvid = age_mvid,
                                release_id_meid = release_id,
                                release_id = release_id,
                                write_out = T,
                                out_file = paste0(out_dir, run_date, "_sex_split_age_split.xlsx"))

## for rows with a value in crosswalk parent seq, clear the seq column (seq from when saved ss no age split xw) 
## because the parent will be same as it was when sex xw version was saved
# if doesn't have a value in crosswalk parent seq that means it was neither age nor sex split 
ss_age_split[!(is.na(crosswalk_parent_seq)), seq:= NA] 

## need to cap standard error at 1 for prevalence or proportion models 
if(xw_measure %in% c("prevalence", "proportion")){
  cat(paste("There are: ", nrow(ss_age_split[standard_error>1,]), " rows of data with standard error >1. Capping Standard error at 1 becuase measure is seroprevalence or cfr.\n"))
  ss_age_split[standard_error >1, standard_error:=1]
}
## save age split data as xlsx 
openxlsx::write.xlsx(ss_age_split, file=path_to_final_xw, sheetName="extraction", rowNames=FALSE, colNames=TRUE)

#upload age split data as crosswalk version
crosswalk_metadata <- save_crosswalk_version(bundle_version_id = bv_id, 
                                             data_filepath = path_to_final_xw,
                                             description = paste(final_xw_descrip, model_descrip, sep=" - "))

#update bundle tracker
if(crosswalk_metadata$request_status=="Successful"){
  tracker_temp_final <- data.table(bundle_id = bundle_id,
                                   date = date,
                                   bundle_version_id = bv_id,
                                   crosswalk_id = crosswalk_metadata$crosswalk_version_id,
                                   is_bulk_outlier = 1,
                                   descrip = paste(final_xw_descrip,model_descrip, sep=" - "),
                                   current_best_bv = 0,
                                   current_best_xw = 0)
  bundle_tracker <- fread(bt_path)
  bundle_tracker[, current_best_xw:=0]
  bundle_tracker <- rbind(bundle_tracker, tracker_temp_final)
  fwrite(bundle_tracker, bt_path)
}

# Launch Dismod model with: 
crosswalk_metadata$crosswalk_version_id

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
###END SCRIPT###
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
