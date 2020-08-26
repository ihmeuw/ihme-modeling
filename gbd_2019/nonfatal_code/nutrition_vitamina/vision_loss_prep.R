##########################################################################
### Purpose: This script saves a bundle version of the vision loss bundle, sex-splits it, and then uploads as a crosswalk version
###          it relies on two specific helper functions from run_sex_split.R 

##########################################################################
#
# Bundle = bundle 94 
# Bundle version = 875


if (Sys.info()["sysname"] == "Linux") {
  j_root <- "/home/j/" 
  h_root <- "~/"
  l_root <- "/ihme/limited_use/"
} else { 
  j_root <- "J:/"
  h_root <- "H:/"
  l_root <- "L:/"
}

pacman::p_load(data.table, openxlsx, ggplot2, reshape2)

source("/FILEPATH/save_bundle_version.R")
source("/FILEPATH/get_bundle_data.R")
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/get_bundle_version.R")
source("/FILEPATH/save_crosswalk_version.R")
source("/FILEPATH/sex_split_functions.R")  # two helper functions 


#######################################################################
#  Download bundle data and save a bundle version
#######################################################################
bundle_id <- 94
decomp_step <- 'step2'

bundle_version_id <- 875
bundle_version_df <- get_bundle_version(bundle_version_id)



#########################################################################
# Do the sex split and save
########################################################################

bundle_version_id <- 875
bundle_version_df <- get_bundle_version(bundle_version_id)

nosplit_dt <- bundle_version_df[sex!="Both"]
single_match <- make_sex_match_vision_loss(nosplit_dt)
ratio_mean <- single_match$ratio 
ratio_se <- single_match$ratio_se    

split_data <- to_split_data(bundle_version_df, ratio_mean, ratio_se)
split_data[, upper:= mean+1.96*standard_error]
split_data[, lower:= mean-1.96*standard_error]
split_data[lower < 0, lower:=0]

decomp_step <- "step2"

path_to_data <- paste0("/FILEPATH/",decomp_step,"_sexsplit_data_w_age_aggregation.xlsx")

write.xlsx(split_data, path_to_data, row.names = FALSE, sheetName="extraction")


#######################################################################
# Upload the sex split data as a new crosswalk version
########################################################################

description_of_crosswalk_version <- "Final crosswalk version, data sex split an age aggregated ratio and se"


crosswalk_version <- save_crosswalk_version(bundle_version_id,
                                            path_to_data,
                                            description=description_of_crosswalk_version)

