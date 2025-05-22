###############################################################################
## Purpose: EMR regression using MR BRT -
## Add row of EMR data to bundle data to generate seq for predicted EMR - update for GBD 2023
## Author:
## Date: May 11, 2020
###############################################################################

rm(list=ls())

pacman::p_load(data.table, ggplot2, dplyr, stringr, openxlsx, gtools)

source("FILEPATH/upload_bundle_data.R")
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_version.R")


#Fill these variables ---------------------------------------------------------
bundle_id <-  10060
cause_name <- "_none"
extractor <- 'USERNAME' #UW name
path_upload <- paste0("FILEPATH", cause_name, "/", bundle_id, "FILEPATH", bundle_id, "_bundle_data_emr_row.xlsx")
# decomp_step <- 'iterative'
# -----------------------------------------------------------------------------

## Append extra row of dummy EMR data in order to generate EMR seq - create needed columns
emr_row = data.table(
  seq = numeric(),
  nid = '416752',  # This is the NID for modeled EMR
  underlying_nid = numeric(),
  sex = 'Female',
  age_start = 50 ,
  age_end = 55,
  location_name = 'Global',
  location_id = 1,
  ihme_loc_id = "",
  year_start = 2014,
  year_end = 2015,
  source_type = 'Facility - inpatient',
  age_demographer = 1,
  measure = 'mtexcess',
  mean = 0.1,
  lower = numeric(),
  upper = numeric(),
  standard_error = 0.01,
  cases = numeric(),
  sample_size = numeric(),
  is_outlier = 1,
  unit_type = 'Person',
  unit_value_as_published = 1,
  effective_sample_size = numeric(),
  representative_name = 'Nationally representative only',
  urbanicity_type = 'Unknown',
  recall_type = 'Not Set',
  recall_type_value = character(),
  input_type = character(),
  sampling_type = character(),
  design_effect = character(),
  extractor = extractor,
  uncertainty_type = 'Standard error',
  uncertainty_type_value = numeric()
)


## Save as .xlsx file to upload location - to be uploaded to new bundle
write.xlsx(emr_row, path_upload, sheetName = "extraction")


## Upload bundle data
path_to_data <- path_upload
result <- upload_bundle_data(bundle_id, path_to_data)


## Save new bundle version and get ID
# Save bundle version and get ID
result <- save_bundle_version(bundle_id)  # Set clinical to TRUE to pull in refresh 2 clinical data
print(sprintf('Bundle version ID: %s', result$bundle_version_id))  # Use this bundle version in the next script


#Get bundle data and locate seq for EMR dummy row - use this in next script for all rows of predicted EMR
bundle_data <- get_bundle_data(bundle_id, export=FALSE)
bundle_data[measure=="mtexcess", c("measure", "seq")]
