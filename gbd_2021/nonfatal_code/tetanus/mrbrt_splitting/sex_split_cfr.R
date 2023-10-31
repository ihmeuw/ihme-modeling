#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:      USERNAME
# Date:         June 2020
# Purpose:      Sex split tetanus cfr and save xw, subset to age specific and save second xw to run DisMod age pattern model on
# Location:     /FILEPATH/
# Notes: Run this to sex split and save xw of tetanus cfr bundle data before launching age pattern model
#---------------------------------------------------------------------------------------------

## EMPTY THE ENVIRONMENT
rm(list = ls())

## LIBRARIES
pacman::p_load(openxlsx, ggplot2, data.table)
library(crosswalk, lib.loc = "/FILEPATH/")
library(dplyr)
library(stringr)
library(tidyr)
library(boot)
setDTthreads(1)

## SOURCE FUNCTIONS
## central
source("/FILEPATH/get_ids.R")
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/get_age_metadata.R")
source("/FILEPATH/get_population.R")
source('/FILEPATH/save_bundle_version.R')
source('/FILEPATH/get_bundle_version.R')
source('/FILEPATH/save_crosswalk_version.R')
source('/FILEPATH/get_crosswalk_version.R')
source("/FILEPATH/get_bundle_data.R")

## custom helpers
source('/FILEPATH/mrbrt_sex_split.R')
source('/FILEPATH/within_study_sex_splitting.R')
source('/FILEPATH/apply_age_crosswalk_copy.R')


## SET OBJECTS
gbd_round <- 7
decomp_step <- "iterative"
bundle_id <- 406
acause <- "tetanus"
date <- gsub("-", "_", Sys.Date())
xw_measure <- "proportion"
dismod_locs <- get_location_metadata(location_set_id = 9, decomp_step = decomp_step, gbd_round_id = gbd_round)

#rerun mrbrt sex splitting model or use weights from previous model? should rerun if any of the sex specific data has changed
use_prev_mrbrt <- FALSE

# save age/sex specific data crosswalk to run dismod age pattern model? or use existing age pattern model
run_new_age_pattern <- TRUE

out_dir <- paste0("FILEPATH", bundle_id, "/01_input_data/02_crosswalk/", decomp_step, "/", date, "/") #where to save plots and input data and output estimates
if(!(dir.exists(out_dir))) dir.create(out_dir)
bt_path <- paste0("FILEPATH/", bundle_id, "/00_documentation/gbd2020_bundle_tracker.csv") #where is this bundle's bundle tracker saved?
upload_dir <- paste0("FILEPATH/", bundle_id, "/03_review/02_upload/")
path_to_sex_crosswalk <- paste0(upload_dir, date,"_", decomp_step, "sex_split_no_age_split.xlsx")
path_to_dismod_age_data <- paste0(upload_dir, date,"_", decomp_step, "_dismod_age_pattern_data.xlsx")
path_to_final_xw <- paste0(upload_dir, date, "_", decomp_step, "_sex_split_age_split_final_xw.xlsx")

plot <- T
offset <- F
drop_zeros <- T
fix_ones <- F
inlier_pct <- 0.9

## change this as helpful
model_descrip <- "Unoutlering OMN, actually correcting Nepal misextraction, no trimming. sex ratio using GBD2020 extracted cfr data"
cat(date, model_descrip, sep="\n", file=paste0(out_dir, "model_details.txt"))

## don't change these!
sex_xw_descrip <- "sex split, no age split"
dismod_xw_descrip <- "for dismod age pattern model: sex split data, restricted to rows with age bins > 20 years"
final_xw_descrip <- "sex split, age split"


# get bundle tracker to find bundle vers to xw
bundle_tracker <- fread(bt_path)
bv_id <- unique(bundle_tracker[current_best_bv==1, bundle_version_id])
print(paste("Sex splitting on bundle version", bv_id))
cat(paste("Sex splitting on bundle version", bv_id), sep="\n", file=paste0(out_dir, "model_details.txt"), append=T)

## SEX SPLIT --------------------------------------------------
data <- get_bundle_version(bundle_version_id = bv_id, fetch="all")

## within study sex splitting
within_study_ss <- age_sex_split(data) # 11 nids, 46 age-spec rows getting split

## MR BRT sex splitting

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
## Make funnel plot
repl_python()

plots <- import("crosswalk.plots")

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

##applying other outliers
sex_split_no_age_split[nid==435639 & location_name == "Aceh", is_outlier := 1]
sex_split_no_age_split[nid==441618 & ihme_loc_id=="OMN", is_outlier:=0]
sex_split_no_age_split[nid==93684 & ihme_loc_id == "HTI", is_outlier:=1]

## SAVE XW OF SEX SPLIT DATA ---------
# to later apply age splitting to
## remove anything in UK because won't be used by DisMod
sex_split_no_age_split <- sex_split_no_age_split[location_id %in% dismod_locs$location_id] # no rows dropped
# cap standard error at 1, all points that are coming back with se > 1 are outliers (and all have orig mean of 0) - 6 rows
sex_split_no_age_split[ standard_error > 1, standard_error :=1]

# remove Japan pre 1950 rows
sex_split_no_age_split <- sex_split_no_age_split[!(nid ==468522 & year_start == 1940)]
write.xlsx(sex_split_no_age_split, file=path_to_sex_crosswalk, sheetName="extraction", row.names=FALSE, col.names=TRUE)
sex_crosswalk <- save_crosswalk_version(bundle_version_id = bv_id, data_filepath = path_to_sex_crosswalk, description = paste(sex_xw_descrip,model_descrip, sep=" - "))

if(sex_crosswalk$request_status=="Successful"){
  tracker_temp <- data.table(bundle_id = bundle_id,
                             date = date,
                             bundle_version_id = bv_id,
                             crosswalk_id = sex_crosswalk$crosswalk_version_id,
                             is_bulk_outlier = 0,
                             descrip = paste(sex_xw_descrip,model_descrip, sep=" - "),
                             current_best_bv = 1,
                             current_best_xw = 0)
  bundle_tracker <- rbind(bundle_tracker, tracker_temp)
  fwrite(bundle_tracker, bt_path)

}

if(run_new_age_pattern){

  ## SAVE XW OF SEX SPLIT DATA IN >20 year age bins for age pattern dismod model -----------------------------
  ## subset to only <20 year age bins
  dismod_age_data <- copy(sex_split_no_age_split)
  dismod_age_data <- dismod_age_data[, age_band := abs(age_end - age_start)]
  age_spec_subset <- dismod_age_data[age_band < 20]

  # save as .xlsx file which will point to in 'save' call
  openxlsx::write.xlsx(age_spec_subset, file=path_to_dismod_age_data, sheetName="extraction", row.names=FALSE, col.names=TRUE)
  dismod_xw <- save_crosswalk_version(bundle_version_id = bv_id, data_filepath = path_to_dismod_age_data, description = paste(dismod_xw_descrip,model_descrip, sep=" - "))

  if(dismod_xw$request_status=="Successful"){
    tracker_temp <- data.table(bundle_id = bundle_id,
                               date = date,
                               bundle_version_id = bv_id,
                               crosswalk_id = dismod_xw$crosswalk_version_id,
                               is_bulk_outlier = 0,
                               descrip = paste(dismod_xw_descrip,model_descrip, sep=" - "),
                               current_best_bv = 1,
                               current_best_xw = 0)
    bundle_tracker <- fread(bt_path)
    bundle_tracker <- rbind(bundle_tracker, tracker_temp)
    fwrite(bundle_tracker, bt_path)
  }
}

## Now, go launch dismod age pattern model, then proceed with age splitting below

## APPLY AGE SPLIT -------------------------------
# SET OBJECTS
today_date <- gsub("-", "_", Sys.Date())
path_to_final_xw <- paste0(upload_dir, today_date, "_", decomp_step, "_sex_split_age_split_final_xw.xlsx")

# get bundle version with sex split data
bundle_tracker <- fread(bt_path)
ss_xw_row <- bundle_tracker[current_best_bv==1 & descrip %like% "sex split, no age split"]
# find date directory to save final split in - this assumes we are using most recent xw that is sex split
date_folder <- ss_xw_row[nrow(ss_xw_row), date]
# this directory already exists from generating when used mrbrt to sex split and then save this xw, so no need to dir.create
out_dir <- paste0("FILEPATH/", bundle_id, "/01_input_data/02_crosswalk/",
                  decomp_step, "/", date_folder, "/")

# find bv and xw id of the data to be age split
ss_xw_id <- ss_xw_row[nrow(ss_xw_row), crosswalk_id]
bv_id <- ss_xw_row[nrow(ss_xw_row), bundle_version_id]

# age pattern model details
age_meid <- 24317
age_mvid <- 579785 # ten percent trim, correct alg for se for ss points with mean of 0 or 1 #535517 updated SE calc for SS data #513236 (fixed SS algebra error)

## get data to split
ss_data <- get_crosswalk_version(crosswalk_version_id = ss_xw_id) #this is same as the sex split data csv saved above

## split the data. this function appropriately sets crosswalk_parent_seq and clears seq for the rows that are split.
ss_age_split <- apply_age_split(data=ss_data,
                                dismod_meid = age_meid,
                                dismod_mvid = age_mvid,
                                decomp_step_meid = "iterative",
                                decomp_step_pop = "iterative",
                                gbd_round = gbd_round,
                                write_out = T,
                                out_file = paste0(out_dir, today_date, "_sex_split_age_split.xlsx"))

## for rows with a value in crosswalk parent seq, clear the seq column (seq from when saved ss no age split xw) because the parent will be same as it was when saved the sex xw version
ss_age_split[!(is.na(crosswalk_parent_seq)), seq:= NA]

## need to cap standard error at 1 for prevalence or proportion models
if(xw_measure %in% c("prevalence", "proportion")){
  cat(paste("There are: ", nrow(ss_age_split[standard_error>1,]), " rows of data with standard error >1. Capping Standard error at 1 becuase measure is seroprevalence or cfr.\n"))
  ss_age_split[standard_error >1, standard_error:=1]
}
## save as xlsx and then save xw
openxlsx::write.xlsx(ss_age_split, file=path_to_final_xw, sheetName="extraction", row.names=FALSE, col.names=TRUE)
crosswalk_metadata <- save_crosswalk_version(bundle_version_id = bv_id, data_filepath = path_to_final_xw,
                                             description = paste(final_xw_descrip, model_descrip, sep=" - "))
if(crosswalk_metadata$request_status=="Successful"){
  tracker_temp_final <- data.table(bundle_id = bundle_id,
                                   date = date,
                                   bundle_version_id = bv_id,
                                   crosswalk_id = crosswalk_metadata$crosswalk_version_id,
                                   is_bulk_outlier = 0,
                                   descrip = paste(final_xw_descrip,model_descrip, sep=" - "),
                                   current_best_bv = 1,
                                   current_best_xw = 1)
  bundle_tracker <- fread(bt_path)
  # have new best age/sex split xw so unmark old best!
  bundle_tracker[, current_best_xw:=0]
  bundle_tracker <- rbind(bundle_tracker, tracker_temp_final)
  fwrite(bundle_tracker, bt_path)
}

