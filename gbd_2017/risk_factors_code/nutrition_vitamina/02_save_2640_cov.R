## AUTHOR
## GBD 2017
## Purpose: save vitamin a supplementation cov 2640
## Dependencies: need to have run dismod model (2640), marked best, and run interpolate code (dismod_to_cov.py)

# Set environment

rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  jpath <- "FILEPATH"
} else {
  jpath <- "FILEPATH"
}


# source functions 
source(paste0(jpath,"FILEPATH/save_results_covariate.R"))
source(paste0(jpath,"FILEPATH/get_best_model_versions.R"))

# set directories
save_folder_2640 <- "FILEPATH"

# define variables
file_pattern_2640 <- "{location_id}.csv"
cov_id <- 261


############################################################################################
## first: get model version id of input dismod model
# make sure you haven't bested a new model between running the dismod to cov code and saving the results
mvid <- get_best_model_versions(entity="modelable_entity", ids=c(2640), gbd_round_id=5, status='best')$model_version_id
description <- paste0("dismod model ",mvid,"; copied select kenya subnat and 44533")

## second: copy kenya to 8 kenya subnationals for successful upload
kenya_ids <- c(44793:44800)
kenya_national <- read.csv(paste0(save_folder_2640,"180.csv"), stringsAsFactors = FALSE)
for (id in kenya_ids){
  kenya_national$location_id <- id
  write.csv(kenya_national,paste0(save_folder_2640,id,".csv"), row.names = FALSE)
}

# do the same thing for 44533, china w/o hong kong and micau
china_national <- read.csv(paste0(save_folder_2640,"6.csv"), stringsAsFactors = FALSE)
china_national$location_id <- 44533
write.csv(china_national, paste0(save_folder_2640,"44533.csv"), row.names = FALSE)

## step three: save results
save_results_covariate(input_dir = save_folder_2640, input_file_pattern = file_pattern_2640, covariate_id = cov_id, description = description, age_std = TRUE, gbd_round_id = 5, mark_best = TRUE)
