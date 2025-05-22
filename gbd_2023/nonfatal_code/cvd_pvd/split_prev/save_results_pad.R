##############################################################################################################
# SET UP
##############################################################################################################

rm(list=ls())

#Paths and release info
input_dir <- "FILEPATH"
release_id <- 16
bundle_id <- 345
crosswalk_version_id <- 45778 
central <- "FILEPATH"
suppressMessages(source(paste0(central, "get_best_model_versions.R")))
suppressMessages(source(paste0(central, "save_results_epi.R")))

##############################################################################################################
# OBTAIN CURRENT DATE
##############################################################################################################

date <- Sys.Date()
# obtain best model versions to save
df <- get_best_model_versions(entity="modelable_entity",
                              ids=c(1861, 2532),
                              release_id=release_id)

##############################################################################################################
# FORMAT MODEL VERSIONS FOR DESCRIPTION ARGUMENT IN SAVE_RESULTS_EPI
##############################################################################################################

models <- unique(df$model_version)
models <- as.character(models)
models <- paste0(models[1], " and ", models[2])

##############################################################################################################
# SAVE MODEL RESULTS
##############################################################################################################
source("FILEPATH/save_results_epi.R")

asymp_mv_df <- save_results_epi(modelable_entity_id=3118,
                                description=paste0("Upload", date, "; splits from ", models),
                                input_dir=paste0(input_dir, "asymp"),
                                input_file_pattern="{measure_id}_{location_id}.csv",
                                measure_id=c(5, 6),
                                mark_best=T,
                                release_id=release_id,
                                bundle_id = bundle_id,
                                crosswalk_version_id = crosswalk_version_id)

symp_mv_df <- save_results_epi(modelable_entity_id=3234,
                               description=paste0("Upload", date, "; splits from ", models),
                               input_dir=paste0(input_dir, "symp"),
                               input_file_pattern="{measure_id}_{location_id}.csv",
                               measure_id=5,
                               mark_best=T,
                               release_id = release_id,
                               bundle_id = bundle_id,
                               crosswalk_version_id = crosswalk_version_id)

