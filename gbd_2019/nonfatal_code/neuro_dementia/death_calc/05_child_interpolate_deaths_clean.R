##########################################################################
### Project: GBD Nonfatal Estimation
### Purpose: Interpolate Dementia Deaths - Child Script
##########################################################################

rm(list=ls())


if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
  functions_dir <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
  functions_dir <- "FILEPATH"
}

pacman::p_load(data.table, openxlsx, ggplot2, readr, rhdf5)
library(mortcore, lib = "FILEPATH")
date <- gsub("-", "_", Sys.Date())

# SET OBJECTS -------------------------------------------------------------

functions_dir <- paste0("FILEPATH")
draws <- paste0("draw_", 0:999)
meid <- ID

## GET ARGS AND ITEMS
args<-commandArgs(trailingOnly = TRUE)
map_path <-args[1]
save_dir <-args[2]
params <- fread(map_path)
task_id <- Sys.getenv("SGE_TASK_ID")
location <- params[task_num == task_id, location]

# SOURCE FUNCTIONS --------------------------------------------------------

functs <- c("interpolate.R", "get_age_metadata.R")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x))))

# GET AGE -----------------------------------------------------------------

age_dt <- get_age_metadata(12)
age_dt <- age_dt[age_group_years_start >= 40]

# INTERPOLATE AND SAVE ----------------------------------------------------

dt <- interpolate(gbd_id_type = "modelable_entity_id", gbd_id = meid,
                  source = "epi", measure_id = 6, location_id = location,
                  reporting_year_start = 1980, decomp_step = "step3", age_group_id = age_dt[, unique(age_group_id)],
                  sex_id = 1:2)
dt[, `:=` (measure_id = 1, modelable_entity_id = NULL, model_version_id = NULL)]
write.csv(dt, paste0("FILEPATH"), row.names = F)
