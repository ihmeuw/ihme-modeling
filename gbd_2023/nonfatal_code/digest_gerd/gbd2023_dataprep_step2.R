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
step4_bvs <- c(bvid, bvid, bvid, bvid, bvid, bvid, bvid)
for (version in step4_bvs) {
  get_bundle_version(version, fetch = "all", export = TRUE, transform = FALSE)
}

#Synch bundles for GBD 2020
get_bundle_data(id, "step2", gbd_round_id = 7, export = TRUE, sync = TRUE)
bundles <- c(id, id, id, id, id, id)
for (bundle in bundles) {
  get_bundle_data(bundle, "step2", gbd_round_id = 7, export = TRUE, sync = TRUE)
}

#Make Step 2 bundle version and crosswalk version for GERD
bv_3059_step12_noclin <- save_bundle_version(bundle_id = id, decomp_step = "step2", gbd_round_id = 7)
bv_3059_step12_noclin <- get_bundle_version(bundle_version_id = id, fetch = "all", export = TRUE)
bv_3059_step12_empty <- get_bundle_version(bundle_version_id = id, fetch = "new", export = TRUE)
step12_xwv <- save_crosswalk_version(xwid, paste0(j, "FILEPATH"))