rm(list=ls())

##working environment 

j<- "FILEPATH"
lib_path <- "FILEPATH"
h<-"FILEPATH"

pacman::p_load(data.table, ggplot2, ggrepel, readr, RMySQL, openxlsx, readxl, stringr, tidyr, plyr, dplyr, dbplyr, Hmisc)

## Source central functions
central_functions <- c("get_bundle_data.R", "save_bundle_version.R", "get_bundle_version.R", "save_crosswalk_version.R", "get_crosswalk_version.R", "save_bulk_outlier.R")
for (fxn in central_functions) {
  source(file.path(lib_path, fxn))
}

#Save copies of Step 4 bundle versions as csvs, if not already done
step4_bvs <- c(16010, 16427, 16430, 14846, 16433, 16436, 10913)
for (version in step4_bvs) {
  get_bundle_version(version, fetch = "all", export = TRUE, transform = FALSE)
}

#Synch bundles for GBD 2020
get_bundle_data(3059, "step2", gbd_round_id = 7, export = TRUE, sync = TRUE)
bundles <- c(6998, 3196, 3197, 7001, 3200, 3201)
for (bundle in bundles) {
  get_bundle_data(bundle, "step2", gbd_round_id = 7, export = TRUE, sync = TRUE)
}

#Make Step 2 bundle version and crosswalk version for GERD
bv_3059_step12_noclin <- save_bundle_version(bundle_id = 3059, decomp_step = "step2", gbd_round_id = 7)
# > print(bv_3059_step12_noclin$bundle_version_id)
# [1] 21812
bv_3059_step12_noclin <- get_bundle_version(bundle_version_id = 21812, fetch = "all", export = TRUE)
bv_3059_step12_empty <- get_bundle_version(bundle_version_id = 21812, fetch = "new", export = TRUE)
step12_xwv <- save_crosswalk_version(21812, paste0(j, "FILEPATH/step2emptyxw.xlsx"))
# appeared as 17282 in EpiViz (forgot to give name)

#Remainder of script was removed for publication of GBD2020 digest_gerd code because it related to preparation of datasets unrelated to digest_gerd; see digest_gastritis and digest_pud for code used to prepare datasets for GBD2020 for those causes