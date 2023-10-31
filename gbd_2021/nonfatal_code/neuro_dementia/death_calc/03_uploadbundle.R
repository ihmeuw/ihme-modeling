##########################################################################
### Author: USERNAME
### Project: GBD Nonfatal Estimation
### Purpose: ADD NEW (UNADJUSTED) CSMR ESTIMATES TO BUNDLE
##########################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH" 
  h <- "FILEPATH"
  l <- "FILEPATH"
  functions_dir <- "FILEPATH"
} else { 
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
  functions_dir <- "FILEPATH"
}

pacman::p_load(data.table, openxlsx, ggplot2)
date <- gsub("-", "_", Sys.Date())

# SET OBJECTS -------------------------------------------------------------

functions_dir <- paste0(functions_dir, "FILEPATH")
bid <- ID
save_dir <- paste0("FILEPATH")
upload_dir <- paste0("FILEPATH")

# SOURCE FUNCTIONS --------------------------------------------------------

functs <- c("get_bundle_data.R", "upload_bundle_data.R", "save_bundle_version.R")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x))))

# GET OLD DATA ------------------------------------------------------------

old_data <- get_bundle_data(bundle_id = bid, decomp_step = "step3")

