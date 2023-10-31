#################################################################################
## STGPR_EPI_UPLOAD.R
## Script to streamline epi uploads for ST-GPR bundle, inpatient envelope
#################################################################################

rm(list=ls()) 

library(data.table)
library(openxlsx)

## --------------------------------------------------------------------------------------------------
## READ IN ALL NON-ADJUSTED DATA AND PREP
## Pre-split
pre_splitting <- as.data.table(read.csv("FILEPATH"))

# Prep pre-splitting for rbind upload
pre_splitting <- pre_splitting[, c("sex","age_start","age_end","age_group_id","year_start","year_end","nid",
                                   "location_id","mean","standard_error","sample_size","measure","is_outlier")]
pre_splitting[, orig_age_start := age_start]
pre_splitting[, orig_age_end := age_end]
pre_splitting[, orig_year_start := year_start]
pre_splitting[, orig_year_end := year_end]
pre_splitting[, year_id := year_start]
pre_splitting[, year_end := year_id]
pre_splitting[is.na(age_group_id), age_group_id := 999]
pre_splitting$val <- pre_splitting$mean
pre_splitting$mean <- NULL
pre_splitting$variance <- pre_splitting$standard_error^2
pre_splitting$standard_error <- NULL

# # Rbind
for_bundle_upload <- pre_splitting

# Add other cols
for_bundle_upload[, underlying_nid := NA]
for_bundle_upload[, seq := 1:nrow(for_bundle_upload)]
for_bundle_upload[variance >= 1, variance := 0.999]
for_bundle_upload[, year_id := year_start]

# For sex-splitting & processing
# write.csv(to_split, "FILEPATH")

# Save
write.xlsx(for_bundle_upload, "FILEPATH", sheetName = "extraction")

## --------------------------------------------------------------------------------------------------
## UPLOAD TO BUNDLE
source("FILEPATH/upload_bundle_data.R")
bundle_id <- 7919
decomp_step <- "iterative"
path_to_data <- "FILEPATH"
gbd_round_id <- 7
result <- upload_bundle_data(bundle_id, decomp_step, path_to_data, gbd_round_id)

print(sprintf('Request status: %s', result$request_status))
print(sprintf('Request ID: %s', result$request_id))

## --------------------------------------------------------------------------------------------------
## SAVE BUNDLE VERSION
source("FILEPATH/save_bundle_version.R")
bundle_id <- 7919
decomp_step <- "iterative"
gbd_round_id <- 7
result <- save_bundle_version(bundle_id, decomp_step, gbd_round_id) 

print(sprintf('Request status: %s', result$request_status))
print(sprintf('Request ID: %s', result$request_id))
print(sprintf('Bundle version ID: %s', result$bundle_version_id))

## --------------------------------------------------------------------------------------------------
## PREP FOR XWALK VERSION
xwalk_version <- as.data.table(read.csv("FILEPATH"))
xwalk_version[, year_start := year_id]
xwalk_version[, year_end := year_id]
xwalk_version[, underlying_nid := NA]
xwalk_version[!is.na(crosswalk_parent_seq), seq := NA]
xwalk_version[, unit_value_as_published := 1]

# Write to xlsx
write.xlsx(xwalk_version, "FILEPATH", sheetName = "extraction")

## --------------------------------------------------------------------------------------------------
## SAVE CROSSWALK VERSION
source("FILEPATH/save_crosswalk_version.R")
bundle_version_id <- 34724
data_filepath <- "FILEPATH"
description <- ""
result <- save_crosswalk_version(bundle_version_id, data_filepath, description)
print(sprintf('Request status: %s', result$request_status))
print(sprintf('Request ID: %s', result$request_id))
print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))

## --------------------------------------------------------------------------------------------------
## GET BUNDLE DATA
source("FILEPATH/get_bundle_data.R")
bundle_id <- 7919
decomp_step <- "iterative"
gbd_round_id = 7
bundle_df <- get_bundle_data(bundle_id, decomp_step, gbd_round_id)


