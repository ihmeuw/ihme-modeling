####################################################################################################################################################
## Purpose: EMR regression using MR BRT - upload bundle data from step2 version, link step2 seq, add EMR row, create new version for new EMR bundle
## Author: USER
## Date: August 9, 2019
####################################################################################################################################################

rm(list=ls())

pacman::p_load(data.table, ggplot2, plyr, dplyr, stringr, openxlsx, gtools)

source("FILEPATH/upload_bundle_data.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/get_bundle_data.R")


# #Fill these variables
bundle_version <- 2012 #bundle version associated with step2 best crosswalk version
extractor <- USER #UW name
path_upload <-  FILEPATH #upload location for your cause
bundle_id <- na #new bundle ID provided by central comp for EMR method change
decomp_step <- ds


bundle_version <- as.numeric(unlist(vars)[1]) #bundle version associated with step2 best crosswalk version
extractor <- unlist(vars)[2]
bundle_id <- as.numeric(unlist(vars)[3])
path_upload <- unlist(vars)[4]
decomp_step <- unlist(vars)[5]

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
  sex = 'Female', age_start = 50 , age_end = 55, location_name = 'Global', location_id = 1,
  year_start = 2014, year_end = 2015, source_type = 'Facility - inpatient', age_demographer = 1, measure = 'mtexcess', 
  unit_type = 'Person', unit_value_as_published = 1, representative_name = 'Nationally representative only', 
  urbanicity_type = 'Unknown', recall_type = 'Not Set', extractor = extractor, is_outlier = 0, mean = 0.1, 
  standard_error = 0.01, uncertainty_type = 'Confidence interval', nid = '416752')

combo_dt <- as.data.table(rbind.fill(bundle_version_dt, emr_row))

# Sometimes clinical data has SE greater than 1, which will cause uploader to fail, so blank these out
combo_dt$standard_error[combo_dt$standard_error > 1] <- ""

## Save as .xlsx file to upload location - to be uploaded to new bundle
write.xlsx(combo_dt, path_upload, sheetName = "extraction")

# Kept getting error with unreadable characters. Save as CSV, go into file, rename sheet to 'extraction' and save as xlsx.
# write.csv(combo_dt, path_to_data_csv, row.names = FALSE)

## Upload bundle data
path_to_data <-  path_upload
result <- upload_bundle_data(bundle_id, decomp_step, path_to_data)

## Save new bundle version and get ID
#Save bundle version and get ID
result <- save_bundle_version(bundle_id, decomp_step, include_clinical=FALSE) #Set clinical to false because already included in step2 bundle version 
# take note of next bundle id for next script

bv <- get_bundle_version(bundle_version_id = result$bundle_version_id)

print(sprintf('Bundle version ID: %s', result$bundle_version_id))

#Get bundle data and locate seq for EMR dummy row - use this in following script
bundle_data <- get_bundle_data(bundle_id, export=FALSE, decomp_step = decomp_step) 
bundle_data$seq[bundle_data$measure == 'mtexcess']

