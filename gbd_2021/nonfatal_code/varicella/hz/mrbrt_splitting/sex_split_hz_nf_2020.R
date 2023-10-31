#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:      USERNAME
# Date:         June 2020
# Purpose:      Sex split HZ incidence and save xw, subset to age specific and save second xw to run DisMod age pattern model on
# Location:     /FILEPATH
# Notes: Run this to sex split and save xw of HZ incidence bundle data before launching age pattern model
#---------------------------------------------------------------------------------------------
## EMPTY THE ENVIRONMENT
rm(list = ls())

## LIBRARIES
pacman::p_load(openxlsx, ggplot2, data.table)
library(crosswalk, lib.loc = "FILEPATH/")
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
acause <- "varicella"
bundle_id <- 50
date <- gsub("-", "_", Sys.Date())
xw_measure <- "incidence"
use_prev_mrbrt <- TRUE
path_to_prev_mrbrt <- "/FILEPATH/2020_06_29/fit1.pkl"
run_new_age_pattern <- TRUE

dismod_locs <- get_location_metadata(location_set_id = 9, gbd_round_id = gbd_round, decomp_step = decomp_step)

out_dir <- paste0("FILEPATH", bundle_id, "/01_input_data/02_crosswalk/", decomp_step, "/", date, "/") #where to save plots and input data and output estimates
if(!(dir.exists(out_dir))) dir.create(out_dir)
bt_path <- paste0("FILEPATH/00_documentation/gbd2020_bundle_tracker.csv") #where is this bundle's bundle tracker saved?
upload_dir <- paste0("FILEPATH/", bundle_id, "/03_review/02_upload/")
path_to_sex_crosswalk <- paste0(upload_dir, date,"_", decomp_step, "sex_split_no_age_split.xlsx")
path_to_dismod_age_data <- paste0(upload_dir, date,"_", decomp_step, "_dismod_age_pattern_data.xlsx")
path_to_final_xw <- paste0(upload_dir, date, "_", decomp_step, "_sex_split_age_split_final_xw.xlsx")

plot <- T
offset <- T
drop_zeros <- F
fix_ones <- T

## change this as helpful
model_descrip <- "Correcting se of 0 or 1 data, incorporation of gamma, MSCM new equation for se of sex split data, 06_29 age pattern."
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
# within the function it will calculate cases and sample size for rows with only mean and se as within study splitting relies on cases and sample size values
dt <- copy(data)
within_study_ss <- age_sex_split(dt)

## MR BRT sex splitting

sex_split <- run_sex_split(dt=within_study_ss,
                           out_dir=out_dir,
                           offset=offset,
                           fix_ones=fix_ones,
                           drop_zeros=drop_zeros,
                           plot=plot,
                           inlier_pct = 0.9,
                           use_prev_mrbrt = T,
                           path_to_prev_mrbrt = path_to_prev_mrbrt)

sex_split_no_age_split <- sex_split$data
fwrite(sex_split_no_age_split, file=paste0(out_dir, "sex_split_data.csv"))

## Make the funnel plot
repl_python()

plots <- import("crosswalk.plots")

plots$funnel_plot(
  cwmodel = sex_results,
  cwdata = formatted_data,
  continuous_variables = list(),
  obs_method = 'Female',
  plot_note = 'Sex splitting funnel plot',
  plots_dir = out_dir,
  file_name = "funnel_plot",
  write_file = TRUE
)


## SAVE XW OF SEX SPLIT DATA ---------
# to later apply age splitting to!
## remove location_id==95 (United Kingdom) because won't be used by DisMod
sex_split_no_age_split <- sex_split_no_age_split[location_id %in% dismod_locs$location_id]
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
    tracker_temp_age <- data.table(bundle_id = bundle_id,
                               date = date,
                               bundle_version_id = bv_id,
                               crosswalk_id = dismod_xw$crosswalk_version_id,
                               is_bulk_outlier = 0,
                               descrip = paste(dismod_xw_descrip,model_descrip, sep=" - "),
                               current_best_bv = 1,
                               current_best_xw = 0)
    bundle_tracker <- fread(bt_path)
    bundle_tracker <- rbind(bundle_tracker, tracker_temp_age)
    fwrite(bundle_tracker, bt_path)
  }
}
## APPLY AGE SPLIT -------------------------------
# SET OBJECTS
today_date <- gsub("-", "_", Sys.Date())
# get bundle version with sex split data
bundle_tracker <- fread(bt_path)
ss_xw_row <- bundle_tracker[current_best_bv==1 & descrip %like% "sex split, no age split"]
# find date directory to save final split in - this assumes we want latest sex split version as one to apply age split to
ss_xw_row <- ss_xw_row[nrow(ss_xw_row),]
date_folder <- ss_xw_row[nrow(ss_xw_row), date]
# this directory already exists from generating when used mrbrt to sex split and then save this xw, so no need to dir.create
out_dir <- paste0("FILEPATH/", bundle_id, "/01_input_data/02_crosswalk/",
                  decomp_step, "/", date_folder, "/")

# find bv and xw id of the data to be age split
ss_xw_id <- ss_xw_row[, crosswalk_id]
bv_id <- ss_xw_row[, bundle_version_id]

# age pattern model details
age_meid <- 24320
age_mvid <- 525059

## get data to split
ss_data <- get_crosswalk_version(crosswalk_version_id = ss_xw_id) #this is same as the sex split data csv saved above

## apply the US outlier
ss_data[nid==282600, is_outlier:=1]

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

## need to cap standard error at 1 for prev or prop models
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


### save bulk outlier
# this data doesn't go into age pattern model or sex weights; just save as bulk outlier because don't need to rerun sex splitting or age pat model
source("/FILEPATH/save_bulk_outlier.R")
bundle_tracker <- fread(bt_path)
ss_xw_row <- bundle_tracker[current_best_bv==1 & descrip %like% "sex split, age split"]
xw_id <- ss_xw_row[nrow(ss_xw_row), crosswalk_id]
bv_id <- ss_xw_row[nrow(ss_xw_row), bundle_version_id]
old_xw <- get_crosswalk_version(xw_id)

#get rows of NID to outlier
to_outlier <- old_xw[nid==282600,]
#subset to the columns needed for save bulk outlier and save it
to_save_outlier <- to_outlier[, c("seq", "is_outlier")]
to_save_outlier[, is_outlier := 1]

file_path <- paste0(upload_dir, date,"_USA_bulk_outlier.xlsx")
descrip <- paste0("outliering USA rows from crosswalk ",xw_id)
openxlsx::write.xlsx(to_save_outlier, file=file_path, sheetName="extraction", row.names=FALSE, col.names=TRUE)


bulk_outlier_metadata <- save_bulk_outlier(crosswalk_version_id = xw_id,
                                        filepath=file_path,
                                        gbd_round_id=gbd_round,
                                        decomp_step = decomp_step,
                                        description = descrip)
if(bulk_outlier_metadata$request_status=="Successful"){
  tracker_temp_final <- data.table(bundle_id = bundle_id,
                                   date = date,
                                   bundle_version_id = bv_id,
                                   crosswalk_id =bulk_outlier_metadata$crosswalk_version_id,
                                   is_bulk_outlier = 1,
                                   descrip = descrip,
                                   current_best_bv = 1,
                                   current_best_xw = 1)
  bundle_tracker <- fread(bt_path)
  # have new best age/sex split xw so unmark old best!
  bundle_tracker[, current_best_xw:=0]
  bundle_tracker <- rbind(bundle_tracker, tracker_temp_final)
  fwrite(bundle_tracker, bt_path)
}
