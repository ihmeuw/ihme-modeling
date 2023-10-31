##########################################################################
### Author: USER
### Date: 2020/09/16
### Project: GBD Nonfatal Estimation
##########################################################################

rm(list=ls())

library(pacman)
pacman::p_load(data.table, openxlsx, ggplot2, Hmisc, msm, magrittr, reticulate, plyr) 

## Standard IHME Functions
functions_dir <- "FILEPATH"
functs <- c("get_location_metadata", "get_population","get_age_metadata", 
            "get_ids", "get_outputs","get_draws", "get_cod_data",
            "get_bundle_data", "upload_bundle_data", "get_bundle_version", 
            "save_bundle_version", "get_crosswalk_version", "save_crosswalk_version",
            "save_bulk_outlier")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x, ".R"))))

## Initialize vars
date <- gsub("-", "_", Sys.Date())
crosswalk_version_id <- 30611
gbd_round_id <- 7
decomp_step <- 'iterative'
path_to_data <- paste0("FILEPATH", date,"_iterative_277_bulk_outlier_nids_125545_269696.xlsx")
description <- paste('Crosswalk Version', crosswalk_version_id,  'with Outliers for NID with EMR data, 125545 and 269696')

epil_xwalk <- get_crosswalk_version(crosswalk_version_id)
xw <- copy(epil_xwalk)


red <- xw[nid == 125545 | nid == 269696, ]
red <- red[, c("seq", "is_outlier")]
red[, is_outlier := 1]

write.xlsx(red, path_to_data, sheetName = "extraction")

result <- save_bulk_outlier(
  crosswalk_version_id = crosswalk_version_id,
  gbd_round_id = gbd_round_id,
  decomp_step = decomp_step,
  filepath = path_to_data,
  description = description
)


