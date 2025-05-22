#*******************************************************************************
#----HEADER---------------------------------------------------------------------
# TITLE: HERPES ZOSTER INCIDENCE SYSTEMATIC REVIEW SEX AND AGE SPLITTING
# Author:       REDACTED
# Date:         June 2020, December 2023
# Purpose:      SEX AND AGE SPLIT HZ INCIDENCE ESTIMATES
# CONTENTS 
# A: SET UP
# B: SEX SPLIT
# C: SUBSET AGE BINS AND RUN AGE PATTERN MODEL IN DISMOD
# D: AGE SPLIT USING DISMOD AGE PATTERN

#*******************************************************************************
# PART A. SET UP ####
#*******************************************************************************

# Clear environment
rm(list = ls())

# Define local drives
os <- .Platform$OS.type
if (os=="REDACTED"){
  REDACTED
}

# Define global environment
date <- gsub("-", "_", Sys.Date())

# Load MR_BRT and XW modules
Sys.setenv("RETICULATE_PYTHON" = "/FILEPATH/bin/python")
reticulate::use_python("/FILEPATH/mrtool_0.0.1/bin/python") 
library(reticulate)
mr <- import("mrtool")
xwalk <- import("crosswalk")

# Load libraries
pacman::p_load(openxlsx, ggplot2, data.table)
library(dplyr)
library(stringr)
library(tidyr)
library(boot)
library(crosswalk002) #ignore error message
library(mrbrt003) #ignore error message

setDTthreads(1)

# Source central functions
source("/FILEPATH/get_ids.R")
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/get_age_metadata.R")
source("/FILEPATH/get_population.R")
source('/FILEPATH/save_bundle_version.R')
source('/FILEPATH/get_bundle_version.R')
source('/FILEPATH/save_crosswalk_version.R')
source('/FILEPATH/get_crosswalk_version.R')
source("/FILEPATH/get_bundle_data.R")
source("/FILEPATH/get_elmo_ids.R")

# Source custom helpers
source("/FILEPATH/within_study_sex_splitting.R")
source("/FILEPATH/apply_age_crosswalk_copy.R") 
source("/FILEPATH/mrbrt_sex_split.R")
source("/FILEPATH/add_population_cols_fxn.R")

# Set model objects
release_id <- 16
acause <- "varicella"
bundle_id <- 50 #herpes incidence
xw_measure <- "incidence"

# MRBRT settings
use_prev_mrbrt <- FALSE
path_to_prev_mrbrt <- "/FILEPATH/fit1.pkl"
run_new_age_pattern <- FALSE

# Get location data
dismod_locs <- get_location_metadata(location_set_id = 9, release_id = release_id)

# Define directories
out_dir <- paste0(j, "FILEPATH") #where to save plots and input data and output estimates
if(!(dir.exists(out_dir))) dir.create(out_dir)
upload_dir <- paste0(j, "FILEPATH/")
path_to_sex_crosswalk <- paste0(upload_dir, date,"_", release_id, "sex_split_no_age_split.xlsx")
path_to_dismod_age_data <- paste0(upload_dir, date,"_", release_id, "_dismod_age_pattern_data.xlsx")
path_to_final_xw <- paste0(upload_dir, date, "_", release_id, "_sex_split_age_split_final_xw.xlsx")

# Plot settings
plot <- T
offset <- T
drop_zeros <- F
fix_ones <- T

# Model description: change as needed
model_descrip <- "GBD2023 sex - age split hz incidence sr, outliered Qatar, splitting on age pattern 842361" 
cat(date, model_descrip, sep="\n", file=paste0(out_dir, "model_details.txt"))

# Change with caution!
sex_xw_descrip <- "sex split, no age split"
dismod_xw_descrip <- "for dismod age pattern model: sex split data, restricted to rows with age bins < 10 years"
final_xw_descrip <- "sex split, age split"

# Get bundle tracker and bundle version of data to sex and age split
bt_path <- paste0(j, "FILEPATH/gbd2022_bundle_tracker.csv") 
bundle_tracker <- fread(bt_path)

ids_df <- get_elmo_ids(bundle_id = bundle_id) #get version ids for bundle
bv_id <- max(ids_df$bundle_version_id) #select most recent bundle version

#*******************************************************************************
# PART B. SEX SPLIT DATA ####
#*******************************************************************************
print(paste("Sex splitting on bundle version", bv_id))
cat(paste("Sex splitting on bundle version", bv_id), sep="\n", file=paste0(out_dir, "model_details.txt"), append=T)

# Load data
data <- get_bundle_version(bundle_version_id = bv_id, fetch="all")

# SEX SPLITTING

# Within study sex splitting: function will calculate cases and sample size for rows with only mean and se 
# as within study splitting relies on cases and sample size values
dt <- copy(data)
within_study_ss <- age_sex_split(dt)

# Run MR BRT sex splitting
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

# Make funnel plot:
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

# Save sex split data as a crosswalk version. This will be used to later apply
# age splitting based on the age pattern model generated in DisMod

# Remove locations not modelled in DisMod 
sex_split_no_age_split <- sex_split_no_age_split[location_id %in% dismod_locs$location_id]

# Save file
write.xlsx(sex_split_no_age_split, file=path_to_sex_crosswalk, sheetName="extraction", rowNames=FALSE, colNames=TRUE)

# Save as crosswalk version
sex_crosswalk <- save_crosswalk_version(bundle_version_id = bv_id, 
                                        data_filepath = path_to_sex_crosswalk, 
                                        description = paste(sex_xw_descrip,model_descrip, sep=" - "))

# Update bundle tracker
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

#*******************************************************************************
# PART C. SUBSET SEX SPLIT DATA INTO AGE BINS FOR AGE PATTERN MODEL ####
#*******************************************************************************

# Use sex split data to model a new age pattern in DisMOD

if(run_new_age_pattern){
  
# Prepare sex split data for age pattern DisMod model 
dismod_age_data <- copy(sex_split_no_age_split)

# Create age band variable
dismod_age_data <- dismod_age_data[, age_band := abs(age_end - age_start)]

# Subset data only to age bands within defined bin threshold
# NOTE: Prior to GBD2022 data was subset to 20 year age bins
age_spec_subset <- dismod_age_data[age_band <= 10] 
  
# Save age bin subset data
openxlsx::write.xlsx(age_spec_subset, file=path_to_dismod_age_data, sheetName="extraction", rowNames=FALSE, colNames=TRUE)

# Save age subset data as a crosswalk version
dismod_xw <- save_crosswalk_version(bundle_version_id = bv_id, 
                                    data_filepath = path_to_dismod_age_data, 
                                    description = paste(dismod_xw_descrip,model_descrip, sep=" - "))

# Update bundle tracker
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

# STOP: Launch dismod HZ NF age pattern model. When finished proceed with age splitting below. 

#*******************************************************************************
# PART D: APPLY AGE SPLIT ####
#*******************************************************************************

# Set objects
run_date <- gsub("-", "_", Sys.Date())

# get bundle version with sex split data
bundle_tracker <- fread(bt_path)
ss_xw_row <- bundle_tracker[current_best_bv==1 & descrip %like% "sex split, no age split"]

# find date directory to save final split in 
ss_xw_row <- ss_xw_row[nrow(ss_xw_row),]
date_folder <- ss_xw_row[nrow(ss_xw_row), date]

# this directory already exists from generating when used mrbrt to sex split and then save this xw, so no need to dir.create
out_dir <- paste0(j, "FILEPATH", "/")
if(!dir.exists(out_dir)) {dir.create(out_dir, recursive = T)}

# find bv and xw id of the data to be age split
ss_xw_id <- ss_xw_row[, crosswalk_id]
bv_id <- ss_xw_row[, bundle_version_id]

# age pattern model details
age_meid <- 24320
age_mvid <- 842361 #Get model version ID from the age pattern model in EPIVIZ!

# get data to split
ss_data <- get_crosswalk_version(crosswalk_version_id = ss_xw_id) #this is same as the sex split data csv saved above

# Apply age split to data 
# ^NB apply_age_split function sourced from apply_age_crosswalk_copy.R 
ss_age_split <- apply_age_split(data=ss_data,
                                dismod_meid = age_meid,
                                dismod_mvid = age_mvid,
                                release_id_meid = release_id,
                                release_id = release_id,
                                write_out = T,
                                out_file = paste0(out_dir, run_date, "_sex_split_age_split.xlsx"))

# For rows with a value in crosswalk parent seq, clear the seq column (seq from when saved ss no age split xw) because the parent will be same as it was when saved the sex xw version
ss_age_split[!(is.na(crosswalk_parent_seq)), seq:= NA] #if upload still fails, try keeping crosswalk parent seq the same in age split function?

# Need to cap standard error at 1 for prevalence or proportion models
if(xw_measure %in% c("prevalence", "proportion")){
  cat(paste("There are: ", nrow(ss_age_split[standard_error>1,]), " rows of data with standard error >1. Capping Standard error at 1 becuase measure is seroprevalence or cfr.\n"))
  ss_age_split[standard_error >1, standard_error:=1]
}

# Save as xlsx and then save xw
openxlsx::write.xlsx(ss_age_split, file=path_to_final_xw, sheetName="extraction", rowNames=FALSE, colNames=TRUE)

# Save age split data as crosswalk version
crosswalk_metadata <- save_crosswalk_version(bundle_version_id = bv_id, 
                                             data_filepath = path_to_final_xw,
                                             description = paste(final_xw_descrip, model_descrip, sep=" - "))
# Update bundle tracker
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

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# LAUNCH DISMOD HERPES ZOSTER INCIDENCE MODEL USING SEX AND AGE SPLIT DATA!
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

# END SCRIPT! 

#*******************************************************************************
