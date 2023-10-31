##########################################################################
### Author: USER
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
            "save_bundle_version", "get_crosswalk_version", "save_crosswalk_version", "save_bulk_outlier")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x, ".R"))))

#Variables:
date <- gsub("-", "_", Sys.Date())
crosswalk_version_id <- 31217
path_to_data <- paste0("FILEPATH", date, "iterative_277_bulk_outlier_marketscan.xlsx")
decomp_step <- "iterative"
gbd_round_id <- 7
description <- paste("Crosswalk Version", crosswalk_version_id,  "with Outliers for all marketscan")

xw_data <- get_crosswalk_version(crosswalk_version_id = crosswalk_version_id)
xw_dt <- copy(xw_data)

#NIDs for marketscan
sort(unique(xw_dt[field_citation_value %like% "Truven Health", nid]))

xw_dt[nid %in% c("244369", "244370", "244371", "336847", "336848", "336849", "336850", "408680", "433114"), is_outlier := 1]
xwred_dt <- copy(xw_dt[, c("seq", "is_outlier")])

write.xlsx(xwred_dt, file = path_to_data, sheetName = "extraction")

result <- save_bulk_outlier(
  crosswalk_version_id = crosswalk_version_id,
  gbd_round_id = gbd_round_id,
  decomp_step = decomp_step,
  filepath = path_to_data,
  description = description
)

result
