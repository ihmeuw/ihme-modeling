##########################################################################
### Author: USERNAME
### Date: 09/10/2020
### Project: GBD Nonfatal Estimation
### Purpose: Outlier observations which are possibly causing issue with Epilepsy model
##########################################################################
rm(list=ls())

library(pacman)
pacman::p_load(data.table, openxlsx, ggplot2, Hmisc, msm, magrittr, reticulate, plyr) 

library(crosswalk, lib.loc = "FILEPATH")
library(metafor, lib.loc = "FILEPATH")
library(mortdb, lib = "FILEPATH")

## Standard IHME Functions
functions_dir <- "FILEPATH"
functs <- c("get_location_metadata", "get_population","get_age_metadata", 
            "get_ids", "get_outputs","get_draws", "get_cod_data",
            "get_bundle_data", "upload_bundle_data", "get_bundle_version", 
            "save_bundle_version", "get_crosswalk_version", "save_crosswalk_version",
            "save_bulk_outlier")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x, ".R"))))

#
date <- gsub("-", "_", Sys.Date())
path_to_data <- paste0("FILEPATH", date,"iterative_277_bulk_outlier_marketscan_2000_and_peru.xlsx")
crosswalk_version_id <- 29930
description <- paste('Crosswalk Version', crosswalk_version_id,  'with Outliers for Marketscan 2000 and Peru NID 124520')
xwalk_epi <- get_crosswalk_version(crosswalk_version_id)
xw <- copy(xwalk_epi)

to_drop <- xw[nid %in% c(244369, 124520)]
to_drop <- to_drop[, c("seq", "is_outlier")]
to_drop[, is_outlier := 1]


write.xlsx(to_drop, file = path_to_data, sheetName = "extraction")

save_bulk_outlier(crosswalk_version_id = crosswalk_version_id,
                  gbd_round_id = 7,
                  decomp_step = "iterative",
                  filepath = path_to_data,
                  description = description
)
