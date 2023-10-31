####################################################################################################
## DISMOD_EPI_UPLOAD_AGE_PATTERN.R
## Script to streamline all uploader processes for age pattern model in DisMod 
####################################################################################################

rm(list=ls())
 
library(data.table)
library(openxlsx)
library(Hmisc)

## -------------------------------------------------------------------------------------------------
## PREP DATA FOR BUNDLE UPLOAD
## Read in pre-split data
pre_splitting <- as.data.table(read.csv("FILEPATH"))

## Keep only necessary columns and prep for validations
pre_splitting$input_type <- NA
dismod_cols <- c("mean","standard_error","upper","lower","sample_size","cases","source_type",
                 "location_id","sex","measure","representative_name","urbanicity_type","uncertainty_type_value",
                 "recall_type","unit_type","unit_value_as_published","nid","year_start","year_end",
                 "age_start","age_end","is_outlier","seq","underlying_nid","input_type","design_effect",
                 "recall_type_value","uncertainty_type","sampling_type","effective_sample_size","age_group_id")

## Drop non-necessary columns
pre_splitting <- pre_splitting[, ..dismod_cols]
pre_splitting$recall_type <- "Period: weeks"
pre_splitting$representative_name <- "Unknown"
pre_splitting$source_type <- "Unidentifiable"
pre_splitting$unit_type <- "Person"
pre_splitting$urbanicity_type <- "Mixed/both"
pre_splitting$recall_type_value <- NA
pre_splitting[age_start == 95, age_group_id := 235]

## Keep data in perfect age groups only
perf_ages <- pre_splitting[!is.na(age_group_id)]
wide_ages <- pre_splitting[is.na(age_group_id)] 

## Save to xlsx for bundle upload
write.xlsx(perf_ages, "FILEPATH", sheetName = "extraction")

## -------------------------------------------------------------------------------------------------
## UPLOAD TO BUNDLE
source("FILEPATH/upload_bundle_data.R")
bundle_id <- 7889
decomp_step <- "iterative"
## To upload data
path_to_data <- "FILEPATH"
gbd_round_id <- 7
result <- upload_bundle_data(bundle_id, decomp_step, path_to_data, gbd_round_id)
print(sprintf("Request status: %s", result$request_status))
print(sprintf("Request ID: %s", result$request_id))

## -------------------------------------------------------------------------------------------------
## SAVE BUNDLE VERSION
source("FILEPATH/save_bundle_version.R")
bd_version <- save_bundle_version(bundle_id, decomp_step, gbd_round_id)
print(sprintf('Request status: %s', bd_version$request_status))
print(sprintf('Request ID: %s', bd_version$request_id))
print(sprintf('Bundle version ID: %s', bd_version$bundle_version_id))

## -------------------------------------------------------------------------------------------------
## PREP POST-XWALK DATA
## Read in data
post_xwalk <- as.data.table(read.csv("FILEPATH"))

# Add extra cols
post_xwalk$input_type <- NA
post_xwalk$mean <- post_xwalk$mean_final
post_xwalk$standard_error <- post_xwalk$se_final
post_xwalk$recall_type <- "Period: weeks"
post_xwalk$representative_name <- "Unknown"
post_xwalk$source_type <- "Unidentifiable"
post_xwalk$unit_type <- "Person"
post_xwalk$urbanicity_type <- "Mixed/both"
post_xwalk$recall_type_value <- NA

# Keep only needed columns 
xwalk_cols <- c("mean","standard_error","upper","lower","sample_size","cases","source_type",
                "location_id","sex","measure","representative_name","urbanicity_type","uncertainty_type_value",
                "recall_type","unit_type","unit_value_as_published","nid","year_start","year_end",
                "age_start","age_end","is_outlier","seq","underlying_nid","input_type","design_effect",
                "recall_type_value","uncertainty_type","sampling_type","effective_sample_size","age_group_id",
                "crosswalk_parent_seq")
post_xwalk <- post_xwalk[, ..xwalk_cols]

# Keep data in age groups only
ages_xwalk <- post_xwalk[!is.na(age_group_id)]
wides_xwalk <- post_xwalk[is.na(age_group_id)]


## Drop locations not estimated 
for_model <- subset(ages_xwalk, location_id %nin% c(4618, 4619, 4620, 4621, 4622, 4623, 4624, 4625, 4626))

## Outliers
for_model <- for_model[mean >= 1.5, is_outlier := 1]
for_model <- for_model[age_start == 0 & mean > 0.7, is_outlier := 1]

for_model <- for_model[!is.na(crosswalk_parent_seq), seq := NA]

## Write to xlsx
write.xlsx(for_model, "FILEPATH", sheetName = "extraction")

## -------------------------------------------------------------------------------------------------
## SAVE CROSSWALK VERSION
source("FILEPATH/save_crosswalk_version.R")
bundle_id <- 7889
decomp_step <- 'iterative'
path_to_data <- "FILEPATH"
gbd_round_id <- 7
bd_ver_id <- 34664
xwalk_version <- save_crosswalk_version(bundle_version_id=bd_ver_id, data_filepath=path_to_data, description="")
print(sprintf('Request status: %s', xwalk_version$request_status))
print(sprintf('Request ID: %s', xwalk_version$request_id))
print(sprintf('Crosswalk version ID: %s', xwalk_version$crosswalk_version_id))

## -------------------------------------------------------------------------------------------------
## GET BUNDLE DATA
source("FILEPATH/get_bundle_data.R")
bundle_id <- 7889
decomp_step <- "iterative"
gbd_round_id = 7
bundle_df <- get_bundle_data(bundle_id, decomp_step, gbd_round_id=gbd_round_id)

## -------------------------------------------------------------------------------------------------
## CLEAR BUNDLE 
clear_bundle <- as.data.table(data.frame(matrix(NA, nrow = 186858, ncol = 34)))
colnames(clear_bundle) <- colnames(bundle_df)
bundle_seqs <- bundle_df$seq
clear_bundle$seq <- bundle_seqs
write.xlsx(clear_bundle, "FILEPATH", sheetName = "extraction")

## -------------------------------------------------------------------------------------------------
## GET BUNDLE VERSION
source("FILEPATH/get_bundle_version.R")
bundle_id <- 7889
decomp_step <- 'iterative'
gbd_round_id = 7
bd_ver_df <- get_bundle_version(27428, fetch = "all")
