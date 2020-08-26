####################################################################################################################################################
## Purpose: EMR regression using MR BRT - upload bundle data from step2 version, link step2 seq, add EMR row, create new version for new EMR bundle
## Author: NAME
## Date: August 9, 2019
####################################################################################################################################################

rm(list=ls())

pacman::p_load(data.table, ggplot2, plyr, stringr, openxlsx, gtools)

source("FILEPATH_CENTRAL_FXN/upload_bundle_data.R")
source("FILEPATH_CENTRAL_FXN/save_bundle_version.R")
source("FILEPATH_CENTRAL_FXN/get_bundle_version.R")
source("FILEPATH_CENTRAL_FXN/get_bundle_data.R")

#Fill these variables
bundle_version <- 7481 #bundle version associated with step2 best crosswalk version

extractor <- 'uwnetid' #UW name

path_upload <- 'FILEPATH/2019_step3_bundle_vers_for_emr_upload.xlsx' #upload location for your cause
bundle_id <- 6998 #new bundle ID provided by central comp for EMR method change

decomp_step <- 'step3'

## Get bundle version for step2 best crosswalk version
bundle_version_id <- bundle_version
bundle_version_df <- get_bundle_version(bundle_version_id, export= FALSE) 
bundle_version_dt <- as.data.table(bundle_version_df)

## Alter so that seq variable is called step2_seq and a new, blank seq column is created
setnames(bundle_version_dt, "seq", "step2_seq")
bundle_version_dt$seq <- ''

## Append extra row of dummy EMR data in order to generate EMR seq - create needed columns 
#NID for modeled EMR used below
emr_row = data.table(
  sex = 'Female', age_start = 50 , age_end = 55, location_name = 'Global', location_id = 1, ihme_loc_id = "IDN", 
  year_start = 2014, year_end = 2015, source_type = 'Facility - inpatient', age_demographer = 1, measure = 'mtexcess', 
  unit_type = 'Person', unit_value_as_published = 1, representative_name = 'Nationally representative only', 
  urbanicity_type = 'Unknown', recall_type = 'Not Set', extractor = extractor, is_outlier = 0, mean = 0.1, 
  standard_error = 0.01, uncertainty_type = 'Confidence interval', nid = '416752')

combo_dt <- as.data.table(rbind.fill(bundle_version_dt, emr_row))

# Sometimes clinical data has SE greater than 1, which will cause uploader to fail, so blank these out
combo_dt[standard_error >1, standard_error  := ""]
combo_dt[upper>1, c("upper", "lower", "uncertainty_type_value") := ""]

## Save as .xlsx file to upload location - to be uploaded to new bundle
write.xlsx(combo_dt, path_upload, sheetName = "extraction")

## Upload bundle data
path_to_data <-  path_upload
result <- upload_bundle_data(bundle_id, decomp_step, path_to_data)

## Save new bundle version and get ID
#Save bundle version and get ID
result <- save_bundle_version(bundle_id, decomp_step, include_clinical=FALSE) #Set clinical to false because already included in step2 bundle version 
print(sprintf('Bundle version ID: %s', result$bundle_version_id))

# 12329 - new bundle version pud

#Get bundle data and locate seq for EMR dummy row - use this in following script
bundle_data <- get_bundle_data(bundle_id, decomp_step, export=TRUE) 
print(paste0("seq for EMR dummy row ", bundle_data[nid == '416752',]$seq))

# 61274 - seq for EMR dummy row pud
