#*******************************************************************************
#----HEADER---------------------------------------------------------------------
# TITLE: VARICELLA SEROPREVALENCE SYSTEMATIC REVIEW SEX AND AGE SPLITTING
# AUTHORS: REDACTED
# DATE: June 2020, updated January 2024
# PURPOSE:      
# 1. Sex split varicella seroprevalence data. 
# 2. Subset sex specific data to age specific strata and run Dismod age pattern
# 3. Use age pattern to age split data for DisMod non-fatal modeling. 
# SECTIONS:
# A: SET UP
# B: SEX SPLIT
# C: SUBSET AGE BINS AND RUN AGE PATTERN MODEL IN DISMOD
# D: AGE SPLIT USING DISMOD AGE PATTERN
#-------------------------------------------------------------------------------
#*******************************************************************************

message("MRBRT sex splitting only works in singularity 4222!")

#*******************************************************************************
# PART A: SETUP
#*******************************************************************************

## EMPTY THE ENVIRONMENT
rm(list = ls())

## SET UP FOCAL DRIVES
os <- .Platform$OS.type
if (os==REDACTED}
REDACTED
}

## LIBRARIES
## Load MR_BRT and XW modules
Sys.setenv("RETICULATE_PYTHON" = "FILEPATH/python")
reticulate::use_python("/FILEPATH/python") 
library(reticulate)
mr <- import("mrtool")
cw <- import("crosswalk")
xwalk <- import("crosswalk") 

## LIBRARIES
pacman::p_load(openxlsx, ggplot2, data.table)
library(dplyr)
library(stringr)
library(tidyr)
library(boot)
library(crosswalk002)
library(mrbrt003) 
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
source("/FILEPATH/get_elmo_ids.R")

# Source custom helpers
source("/FILEPATH/within_study_sex_splitting.R")
source("/FILEPATH/mrbrt_sex_split.R")
source("/FILEPATH/add_population_cols_fxn.R")
source("/FILEPATH/apply_age_crosswalk_copy.R") 


## SET MRBRT perferences
plot <- T
offset <- F #should not run with offset applied to input data
drop_zeros <- T 
fix_ones <- T
inlier_pct <- 1

## SET OBJECTS
release_id <- 16
acause <- "varicella"
bundle_id <- 49
date <- gsub("-", "_", Sys.Date())
xw_measure <- "prevalence" 

# Get dismod location metadata
dismod_locs <- get_location_metadata(location_set_id = 9, 
                                     release_id = release_id)

# Use previous MRBRT output for age splitting? # set this to true if no changes have been made to bundle
use_prev_mrbrt <- F
path_to_prev_mrbrt <- paste0("/FILEPATH/fit1.pkl")

# Run new age pattern? Set to true if there is new seroprevalence data in the bundle or new outliers!
run_new_age_pattern <- T

# Define directories
out_dir <- paste0(j, "FILEPATH") 
if(!(dir.exists(out_dir))) dir.create(out_dir, recursive = T)
upload_dir <- paste0(j, "FILEPATH", acause, "/", bundle_id, "/03_review/02_upload/")
path_to_sex_crosswalk <- paste0(upload_dir, date,"_", release_id, "sex_split_no_age_split.xlsx")
path_to_dismod_age_data <- paste0(upload_dir, date,"_", release_id, "_dismod_age_pattern_data.xlsx")
path_to_final_xw <- paste0(upload_dir, date, "_", release_id, "_sex_split_age_split_final_xw.xlsx")

## change this as helpful
model_descrip <- "testing updated mrbrt script"
cat(date, model_descrip, sep="\n", file=paste0(out_dir, "model_details.txt"))

## xw descriptions
sex_xw_descrip <- "sex split, no age split"
dismod_xw_descrip <- "for dismod age pattern model: sex split data, restricted to rows with age bins < 10 years"
final_xw_descrip <- "sex split, age split"

# Load bundle tracker to find bundle version for crosswalk
bundle_ids <- get_elmo_ids(bundle_id = bundle_id)
bv_id <- max(bundle_ids$bundle_version_id)

## SEX SPLIT --------------------------------------------------

print(paste("Sex splitting on bundle version", bv_id))
cat(paste("Sex splitting on bundle version", bv_id), sep="\n", file=paste0(out_dir, "model_details.txt"), append=T)

# Get bundle version
data <- get_bundle_version(bundle_version_id = bv_id, fetch="all")

# Fix measure column 
data$measure <- xw_measure

# within study sex splitting
# will calculate cases and sample size for rows with only mean and se as within study splitting relies on cases and sample size values

#remove one row with specificity = pregnacy status because will cause error in matching
data <- data[specificity != "pregnancy status", ]

# Recode specificity
within_study <- data[sex == "Male" | sex == "Female", specificity := "sex"]  # define which rows are the sex-specific vs age-specific rows
within_study[sex == "Both", specificity := "age"]    # assumes no other specificity but sex and age
 

# Keep only most granular rows of data
within_study <- within_study[group_review == 1, ]

# Fix vector types
within_study$mean <- as.numeric(within_study$mean)
within_study$lower <- as.numeric(within_study$lower)
within_study$upper <- as.numeric(within_study$upper)
within_study$sample_size <- as.numeric(within_study$sample_size)
within_study$standard_error <- as.numeric(within_study$standard_error)
within_study$age_start <- as.numeric(within_study$age_start)
within_study$age_end <- as.numeric(within_study$age_end)
within_study$location_id <- as.numeric(within_study$location_id)

# Apply sex split
within_study_ss <- age_sex_split(within_study) 

if(sum(is.na(within_study_ss$mean)) > 0) {
  stop()
  message("You have NAs in your data")
}else{
  print("There are no NAs in your data, continue!")
}

## Run MR-BRT sex split model
sex_split <- run_sex_split(dt=within_study_ss, 
                           out_dir=out_dir, 
                           offset=offset, 
                           fix_ones=fix_ones, 
                           drop_zeros=drop_zeros, 
                           plot=plot,
                           use_prev_mrbrt = use_prev_mrbrt,
                           path_to_prev_mrbrt = path_to_prev_mrbrt,
                           inlier_pct = inlier_pct)

sex_split_no_age_split <- sex_split$data
fwrite(sex_split_no_age_split, file=paste0(out_dir, "sex_split_data.csv")) 

## make funnel plot
repl_python() 

# now type exit in console before running the rest of the code

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

# Keep only Dismod locations
sex_split_no_age_split <- sex_split_no_age_split[location_id %in% dismod_locs$location_id] 

# Save file
openxlsx::write.xlsx(sex_split_no_age_split, file=path_to_sex_crosswalk, sheetName="extraction", rowNames=FALSE, colNames=TRUE)

# Save as a crosswalk version
sex_crosswalk <- save_crosswalk_version(bundle_version_id = bv_id, 
                                        data_filepath = path_to_sex_crosswalk, 
                                        description = paste(sex_xw_descrip,model_descrip, sep=" - "))

cat(paste("New sex split crosswalk id", sex_crosswalk$crosswalk_version_id), sep="\n", file=paste0(out_dir, "model_details.txt"), append=T)

## SAVE XW OF SEX SPLIT DATA IN <10 year age bins for age pattern dismod model -----------------------------
## subset to only <10 year age bins
if(run_new_age_pattern){
  dismod_age_data <- copy(sex_split_no_age_split)
  dismod_age_data <- dismod_age_data[, age_band := abs(age_end - age_start)]
  age_spec_subset <- dismod_age_data[age_band < 10]
}  
  
# if(exclude_vax == F){
  
  # save as .xlsx file 
  openxlsx::write.xlsx(age_spec_subset, file=path_to_dismod_age_data, sheetName="extraction", rowNames=FALSE, colNames=TRUE)
  
  # save as a crosswalk
  dismod_xw <- save_crosswalk_version(bundle_version_id = bv_id, 
                                      data_filepath = path_to_dismod_age_data, 
                                      description = paste(dismod_xw_descrip, model_descrip, sep=" - "))

  cat(paste("New sex split age restricted crosswalk id", dismod_xw$crosswalk_version_id), sep="\n", file=paste0(out_dir, "model_details.txt"), append=T)
  
#*******************************************************************************
# STOP!!! Launch DisMod model in EpiViz. 
# When done proceed with age splitting below
stop()
#*******************************************************************************

## APPLY AGE SPLIT --------------------------------------
# SET OBJECTS
today_date <- gsub("-", "_", Sys.Date())
# generate path to final xw to match today's date - the date we are saving the final xw on
path_to_final_xw <- paste0(upload_dir, today_date, "_", release_id, "_sex_split_age_split_final_xw.xlsx")

bv_id <- bv_id
ss_xw_id <- 44550

# age pattern model details
age_meid <- 24319
age_mvid <- 830725 

## get data to split
ss_data <- get_crosswalk_version(crosswalk_version_id = ss_xw_id) #this is same as the sex split data csv saved above

# NB for varicella age pattern is capped at 65
ss_age_split <- apply_age_split(data=ss_data,
                                dismod_meid = age_meid,
                                dismod_mvid = age_mvid,
                                release_id = release_id, 
                                release_id_meid = release_id, 
                                gbd_round = gbd_round,
                                write_out = T,
                                out_file = paste0(out_dir, today_date, "_sex_split_age_split.xlsx"))

## for rows with a value in crosswalk parent seq, clear the seq column because the parent will be same as it was when saved the sex xw version
ss_age_split[!(is.na(crosswalk_parent_seq)), seq:= NA] 

## need to cap standard error at 1 because this is prevalence
if(xw_measure %in% c("prevalence", "proportion")){
  cat(paste("There are: ", nrow(ss_age_split[standard_error>1,]), " rows of data with standard error >1. Capping Standard error at 1 becuase measure is seroprevalence or cfr.\n"))
  ss_age_split[standard_error >1, standard_error:=1]
}

# ## save as xlsx and then save xw
openxlsx::write.xlsx(ss_age_split, file=path_to_final_xw, sheetName="extraction", rowNames=FALSE, colNames=TRUE)
crosswalk_metadata <- save_crosswalk_version(bundle_version_id = bv_id, data_filepath = path_to_final_xw,
                                             description = paste(final_xw_descrip, model_descrip, sep=" - "))

# Can now launch sex and age specific varicella seroprevalence model in Epiviz. 
#####END SCRIPT####