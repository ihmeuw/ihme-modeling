###########################################################################
# Description: Save crosswalk version to fit age pattern in DisMod        #
#                                                                         #
###########################################################################
#

library(data.table)
library(openxlsx)
source('FILEPATH/save_bundle_version.R')
source('FILEPATH/get_bundle_version.R')
source('FILEPATH/save_crosswalk_version.R')
source('FILEPATH/get_crosswalk_version.R')
source("FILEPATH/get_bundle_data.R")

### ----------------------- BUNDLE VERSIONS ------------------------------

bundle_id <- "ADDRESS"
decomp_step <- "ADDRESS"
bundle_df <- get_bundle_data(bundle_id)

age_version <- "ADDRESS"
age_version_data <- get_bundle_version(age_version)


# FOLLOWING SECTIONS TBD ORDER OF OPERATIONS

### ----------------------- APPLY BRT COEFFS ------------------------------

# Apply to age spec bundle
# Apply to all-age bundle

### ----------------------- RUN AGE CURVE ------------------------------


age_version_data <- age_version_data[, crosswalk_parent_seq := seq]

# Set years to 2010
age_version_data <- age_version_data[, year_start := 2010]
age_version_data <- age_version_data[, year_end := 2010]

males <- age_version_data[, sex := 'Male']
females <- age_version_data[, sex := 'Female']
final <- rbind(males, females)
final <- final[, seq := '']


# Save out crosswalk version
age_cross_file = 'FILEPATH'
write.xlsx(final, age_cross_file, sheetName = "extraction")
age_cw_version <- save_crosswalk_version("ADDRESS",    data_filepath=age_cross_file, description='DESCRIPTION')




