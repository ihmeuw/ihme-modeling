###################################################################################
## Prep Upper respiratory non-fatal data ##
###################################################################################
library(openxlsx)
library(plyr)
library(ggplot2)

source() # filepaths for upload and download functions

save_crosswalk_version(bundle_version_id = 1376, 
                       data_filepath = "filepath", 
                       description = "Sex split URI data")

uri_data <- get_bundle_data(bundle_id=25, decomp_step="step2", export=F)

df <- subset(uri_data, measure=="prevalence")

t.test(mean ~ cv_dhs_cough_only, data=df)
t.test(mean ~ cv_diag_valid_good, data=df)
t.test(mean ~ cv_mild_uri, data=df)

