# CE - EMR - MR-BRT - bundle prep

###############################################################################
## Purpose: EMR regression using MR BRT - 


rm(list=ls())

pacman::p_load(data.table, ggplot2, dplyr, stringr, openxlsx, gtools)

source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")


#Fill these variables ---------------------------------------------------------
bundle_id <-  ADDRESS
cause_name <- "ADDRESS"
extractor <- 'ADDRESS' 
path_upload <- paste0(FILEPATH)
decomp_step <- 'ADDRESS'
# -----------------------------------------------------------------------------

## Append extra row of dummy EMR data in order to generate EMR seq - create needed columns 
emr_row = data.table(
  sex = 'Female', age_start = 50 , age_end = 55, location_name = 'Global', location_id = 1, ihme_loc_id = "", 
  year_start = 2014, year_end = 2015, source_type = 'FADDRESS', age_demographer = 1, measure = 'mtexcess', 
  unit_type = 'Person', unit_value_as_published = 1, representative_name = 'ADDRESS', 
  urbanicity_type = 'Unknown', recall_type = 'Not Set', extractor = extractor, is_outlier = 1, mean = 0.1, 
  standard_error = 0.01, uncertainty_type = 'Standard error', nid = 'ADDRESS', seq = "", underlying_nid = "", sampling_type = "",
  recall_type_value = "", input_type = "", upper ="", lower = "", effective_sample_size ="", sample_size ="", cases = "",
  design_effect = "", uncertainty_type_value = "") 



## Save as .xlsx file to upload location - to be uploaded to new bundle
write.xlsx(FILEPATH)


## Upload bundle data
path_to_data <-  path_upload
result <- upload_bundle_data(bundle_id, decomp_step, path_to_data, gbd_round_id = ADDRESS)


## Save new bundle version and get ID
result <- save_bundle_version(bundle_id, decomp_step, gbd_round_id = ADDRESS, include_clinical="ADDRESS") 

bundle_data <- get_bundle_data(bundle_id, decomp_step, gbd_round_id = ADDRESS, export=FALSE) 
bundle_data[measure=="mtexcess" & location_id ==1, c("measure", "seq")]

