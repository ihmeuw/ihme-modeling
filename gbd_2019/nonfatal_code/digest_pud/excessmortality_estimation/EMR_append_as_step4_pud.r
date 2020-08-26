##################################################################################################
## Purpose: Step4 EMR Workflow
## Date: 9/16/19
## Created by: NAME
##################################################################################################

#NAME implementation for PUD

rm(list=ls())

source("FILEPATH_CENTRAL_FXNS/upload_bundle_data.R")
source("FILEPATH_CENTRAL_FXNS/get_bundle_data.R")
source("FILEPATH_CENTRAL_FXNS/save_bundle_version.R")
source("FILEPATH_CENTRAL_FXNS/get_bundle_version.R")
source("FILEPATH_CENTRAL_FXNS/save_crosswalk_version.R")
source("FILEPATH_CENTRAL_FXNS/get_crosswalk_version.R")
source("FILEPATH_CENTRAL_FXNS/validate_input_sheet.R")

bundle_id = 6998 #EMR bundle
path_upload <- 'FILEPATH/2019_step3_to_step4_bundle_data_upload.xlsx' #filepath for uploading to bundle of your cause
path_save <-  'FILEPATH/2019_step4_xw_step3_best_and_step4_new.xlsx' #location to save flat file for crosswalk version from step3 now appended to step4 crosswalk version
crosswalk_version_id_step3 <- 8885 #this is your step3 best crosswalk version (with appended EMR)
crosswalk_version_id_step4 <- 9356 #this is your step4 best crosswalk version (new data)

#Get step3 bundle data from EMR bundle and save to J drive
decomp_step = 'step3'
bundle_df <- get_bundle_data(bundle_id, decomp_step, export=FALSE, merge_id_columns=FALSE) 
bundle_dt <- as.data.table(bundle_df)

## Alter so that seq variable is called step3_seq and a new, blank seq column is created
setnames(bundle_dt, "seq", "step3_seq")
bundle_dt$seq <- ''

# Sometimes clinical data has SE greater than 1, which will cause uploader to fail, so blank these out
bundle_dt[standard_error >1, standard_error  := ""]

## Save as .xlsx file to upload location - to be uploaded to new bundle
write.xlsx(bundle_dt, path_upload, sheetName = "extraction")

## Upload bundle data as step4 data
path_to_data <-  path_upload
decomp_step <- 'step4'
result <- upload_bundle_data(bundle_id, decomp_step, path_to_data)

#Save bundle version and get ID
result <- save_bundle_version(bundle_id, decomp_step, include_clinical=FALSE) #Set clinical to false because step2 clinical data already included in step3 bundle version that just got uploaded as step4 data, and already uploaded step4 clinical data from the other bundle
print(sprintf('Bundle version ID: %s', result$bundle_version_id))

bundle_version_id <- 14396  #FILL THIS IN FROM ABOVE

## Pull in step3 crosswalk version that includes EMR data
xwalk_dt <- get_crosswalk_version(crosswalk_version_id_step3, export=FALSE)

## Set seq correctly for step4 crosswalk version using bundle data for step3 bundle
# First, get_bundle_data you just uploaded to step4
bundle_data <- get_bundle_data(bundle_id, decomp_step = 'step4', export=FALSE) 

# Next, clear seq column in crosswalk version, and re-name crosswalk_parent_seq to step3_seq
xwalk_dt2 <- as.data.table(xwalk_dt)
xwalk_dt2[, "seq"] <- NULL
setnames(xwalk_dt2, "crosswalk_parent_seq", "step3_seq")

xwalk_dt2 <- merge(xwalk_dt2, bundle_data[, c("step3_seq", "seq")], by = "step3_seq")
xwalk_dt2[, "step3_seq"] <- NULL
setnames(xwalk_dt2, "seq", "crosswalk_parent_seq")
xwalk_dt2$seq <- ''

# Sometimes clinical data has SE greater than 1, which will cause uploader to fail, so blank these out
xwalk_dt2[standard_error >1, standard_error  := ""]

## This is the not-yet-resaved crosswalk version with modeled EMR from step3, now with step4 seqs
final_old <- as.data.table(xwalk_dt2)

## Now append this newly created crosswalk version to whatever crosswalk version you have already saved for step4 NEW data, and save
final_new <- get_crosswalk_version(crosswalk_version_id_step4, export=FALSE)
final_combo <- plyr::rbind.fill(final_old, final_new)
write.xlsx(final_combo, path_save, sheetName = "extraction")

## Save a new crosswalk version that has the step3 crosswalk version (with EMR) appended to your step4 crosswalk version of choice
data_filepath <- path_save
description <- 'step4 crosswalk - step3 best crosswalk version (8885) appended to step4 new clinical informatics data crosswalk (9356) version'

result <- save_crosswalk_version(bundle_version_id=bundle_version_id, data_filepath=data_filepath, description=description)

print(sprintf('Request status: %s', result$request_status))
print(sprintf('Request ID: %s', result$request_id))
print(sprintf('Bundle version ID: %s', result$bundle_version_id)) # This doesn't always work - view "result" variable to see id otherwise

#crosswalk_version_id 9623
