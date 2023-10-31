#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:       USERNAME
# Date:         June 2020
# Purpose:      Sex split diphtheria cfr and save xw, subset to age specific and save second xw to run DisMod age pattern model on
# Location:     /FILEPATH/sex_split_cfr.R
# Notes: Run this to sex split and save xw of diph cfr bundle data before launching age pattern model
#---------------------------------------------------------------------------------------------
rm(list = ls())

## SET UP FOCAL DRIVES
os <- .Platform$OS.type
if (os=="windows") {
  j <- FILEPATH
  h <- FILEPATH
} else {
  j <- FILEPATH
  h <- paste0(FILEPATH, Sys.info()[7], "/")
}

## LIBRARIES
pacman::p_load(openxlsx, ggplot2, data.table)
library(crosswalk, lib.loc = "FILEPATH")
library(dplyr)
library(stringr)
library(tidyr)
library(boot)
setDTthreads(1)

## SOURCE FUNCTIONS
## central
source("FILEPATH/get_ids.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_population.R")
source('FILEPATH/save_bundle_version.R')
source('FILEPATH/get_bundle_version.R')
source('FILEPATH/save_crosswalk_version.R')
source('FILEPATH/get_crosswalk_version.R')
source("FILEPATH/get_bundle_data.R")

## custom helpers
source('FILEPATH/mrbrt_sex_split.R')
source('FILEPATH/within_study_sex_splitting.R')
source('FILEPATH/apply_age_crosswalk_copy.R')

## SET OBJECTS
gbd_round <- 7
decomp_step <- "iterative"
acause <- "diptheria"
bundle_id <- 407
date <- gsub("-", "_", Sys.Date())
xw_measure <- "proportion"
use_prev_mrbrt <- FALSE
path_to_prev_mrbrt <- NULL
run_new_age_pattern <- FALSE

dismod_locs <- get_location_metadata(location_set_id = 9, gbd_round_id=gbd_round, decomp_step="iterative")

out_dir <- paste0("FILEPATH") #where to save plots and input data and output estimates
if(!(dir.exists(out_dir))) dir.create(out_dir)
bt_path <- paste0("FILEPATH/gbd2020_bundle_tracker.csv") #where is this bundle's bundle tracker saved?
upload_dir <- paste0("FILEPATH")
path_to_sex_crosswalk <- paste0("FILEPATH")
path_to_dismod_age_data <- paste0("FILEPATH")
path_to_final_xw <- paste0("FILEPATH")

plot <- T
offset <- F
drop_zeros <- T
fix_ones <- F
inlier_pct <- 0.9

## change this as helpful
model_descrip <- "applying age pattern from 11/9 to data with 416150 corrections, bundle level outliers, no offset in mrbrt, 10 pct trim"
cat(date, model_descrip, sep="\n", file=paste0(FILEPATH))
cat(paste0("inlier_pct: ", inlier_pct), file=paste0(FILEPATH, "model_details.txt"), append=T)

## don't change these!
sex_xw_descrip    <- "sex split, no age split"
dismod_xw_descrip <- "for dismod age pattern model: sex split data, restricted to rows with age bins > 20 years"
final_xw_descrip  <- "sex split, age split"


# get bundle tracker to find bundle vers to xw
bundle_tracker <- fread(bt_path)
bv_id <- unique(bundle_tracker[current_best_bv==1, bundle_version_id])
print(paste("Sex splitting on bundle version", bv_id))
cat(paste("\nSex splitting on bundle version", bv_id), sep="\n", file=paste0(out_dir, "model_details.txt"), append=T)

## SEX SPLIT --------------------------------------------------
data <- get_bundle_version(bundle_version_id = bv_id, fetch="all")

## within study sex splitting
# within the function it will calculate cases and sample size for rows with only mean and se as within study splitting relies on cases and sample size values
dt <- copy(data)
within_study_ss <- age_sex_split(dt)

if(use_prev_mrbrt == FALSE){

  ### prep claims data to use when generating sex ratio

  cfr_claims <- fread("FILEPATH/diptheria_cfr.csv")
  fwrite(cfr_claims, paste0(out_dir, "clinical_team_cfr_data.csv"))

  # drop rows where cfr==0
  cfr_claims <- cfr_claims[cfr != 0]
  cfr_claims <- cfr_claims[, se := sqrt(cfr * (1/cfr) / cases)]
  # remove duplicated rows
  cfr_claims <- cfr_claims[!duplicated(cfr_claims)]
  # we only want clinical where have male/female pairs because want it to help with sex splitting (not in main diphteria cfr model bundle)
  # make a unique NID to pair rows of data (male and female)
  cfr_claims[, uid := .GRP, by=c("age_group_id", "location_id", "year_start", "year_end", "nid")]
  keep <- cfr_claims[, .N, by = "uid"][N == 2, uid]
  cfr_claims <- cfr_claims[uid %in% keep]

  cfr_claims[, c("source", "icg_name", "icg_measure", "uid") := NULL]
  setnames(cfr_claims, c("sex_id", "se"), c("sex", "standard_error"))
  cfr_claims$sex <- as.character(cfr_claims$sex)
  cfr_claims[sex==1, sex := "Male"]
  cfr_claims[sex==2, sex := "Female"]
  setnames(cfr_claims, c("deaths", "cases", "cfr"), c("cases", "sample_size", "mean"))


  # add ages and location_name for claims data (get set 12 so have under 1 aggregated) because match on location_name, age_start and age_end to find pairs for sex splitting
  ages <- get_age_metadata(age_group_set_id = 12, gbd_round_id = 7)[, c("age_group_years_start", "age_group_years_end", "age_group_id")]
  setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
  under_1 <- data.table(age_start = 0 , age_end = 1, age_group_id = 28)
  ages <- rbind(ages, under_1)
  cfr_claims <- merge(cfr_claims, ages, by="age_group_id")
  cfr_claims$age_group_id <- NULL
  locations <- get_location_metadata(location_set_id = 22, gbd_round_id = gbd_round, decomp_step= decomp_step)
  cfr_claims <- merge(cfr_claims, locations[, c("location_id", "location_name")], by="location_id")

  # gather the extra sex data
  sex_data <- copy(cfr_claims)
  # add columns as needed for sex splitting code
  sex_data[, group_review := 1]
  sex_data[, is_outlier := 0]
  sex_data[, measure := "proportion"]

  ## add identifiers to this additional sex data to remove later so don't get saved in xw for age pattern or main cfr model
  sex_nids <- unique(sex_data$nid)

  ## gather all data for sex splitting
  within_study_ss <- rbind(within_study_ss, sex_data, fill=T)
}


sex_split <- run_sex_split(dt=within_study_ss,
                           out_dir=out_dir,
                           offset=offset,
                           fix_ones=fix_ones,
                           drop_zeros=drop_zeros,
                           plot=plot,
                           inlier_pct = inlier_pct,
                           use_prev_mrbrt = use_prev_mrbrt,
                           path_to_prev_mrbrt = path_to_prev_mrbrt)

#get the sex split data
sex_split_no_age_split <- sex_split$data

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


# remove the extra sex data that we just had to generate the weights
if(use_prev_mrbrt == FALSE) sex_split_no_age_split <- sex_split_no_age_split[!(nid %in% sex_nids),]

# save sex split data!
fwrite(sex_split_no_age_split, file=paste0(out_dir, "sex_split_data.csv")) #this includes any UK locs if present in the data

## SAVE XW OF SEX SPLIT DATA ---------
# to later apply age splitting to!
sex_split_no_age_split <- sex_split_no_age_split[location_id %in% dismod_locs$location_id] # more rows than GBD 19  because not deleting rows that are outliers, just marking as such
# adjust to meet uploader validations
sex_split_no_age_split[is.na(unit_value_as_published), unit_value_as_published := 1]
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

## SAVE XW OF SEX SPLIT DATA IN >20 year age bins for age pattern dismod model -----------------------------
## subset to only <20 year age bins
if(run_new_age_pattern){
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
# pull latest saved ss_xw row, this assumes latest ss xw is the one you want to age split
ss_xw_row <- ss_xw_row[nrow(ss_xw_row),]
# find date directory to save final split in
date_folder <- ss_xw_row[, date]
## dates got messed up for 07.31.20, all are saved in the 07.31.20 folder even those labeled 07.31.20-A
if(date_folder=="07.31.20-A") date_folder <- "2020_07_31"
# this directory already exists from generating when used mrbrt to sex split and then save this xw, so no need to dir.create
out_dir <- paste0(FILEPATH)

# find bv and xw id of the data to be age split
ss_xw_id <- ss_xw_row[, crosswalk_id]
bv_id <- ss_xw_row[, bundle_version_id]

# age pattern model details
age_meid <- 24318
age_mvid <- 578936

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

## for rows with a value in crosswalk parent seq, clear the seq column because the parent will be same as it was when saved the sex xw version
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

## save
source("/FILEPATH/save_bulk_outlier.R")
bundle_tracker <- fread(bt_path)
ss_xw_row <- bundle_tracker[current_best_bv==1 & descrip %like% "sex split, age split"]
xw_id <- ss_xw_row[nrow(ss_xw_row), crosswalk_id]
bv_id <- ss_xw_row[nrow(ss_xw_row), bundle_version_id]
old_xw <- get_crosswalk_version(xw_id)

#get rows of NID to outlier
to_outlier <- old_xw[nid==133384,]
#subset to the columns needed for save bulk outlier and save it
to_save_outlier <- to_outlier[, c("seq", "is_outlier")]
to_save_outlier[, is_outlier := 1]

file_path <- paste0(upload_dir, date,"_assam_bulk_outlier.xlsx")
descrip <- paste0("bulk outliering assam rows from crosswalk ",xw_id)
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
  bundle_tracker[, current_best_xw:=0]
  bundle_tracker <- rbind(bundle_tracker, tracker_temp_final)
  fwrite(bundle_tracker, bt_path)
}



