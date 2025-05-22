###########################################################################################################
# Interpolate prevalence and incidence hazard and use to produce interpolated incidence - child script
###########################################################################################################

# SET UP

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

pacman::p_load(data.table, openxlsx, ggplot2, readr, rhdf5)
library(mortcore, lib = "FILEPATH")
date <- gsub("-", "_", Sys.Date())



# SET OBJECTS -------------------------------------------------------------
meid <- ID
decomp_step <- 'iterative'
gbd_round <- 7

## GET ARGS AND ITEMS
args <- commandArgs(trailingOnly = TRUE)
date <- args[1]

save_dir <- paste0("FILEPATH")
map_dir <- map_path <- paste0(save_dir, "FILEPATH")

params <- fread(map_path)
task_id <- Sys.getenv("SGE_TASK_ID")
location <- params[task_num == task_id, location]

# SOURCE FUNCTIONS --------------------------------------------------------

functs <- c("interpolate.R", "get_age_metadata.R")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x))))


# GET AGE -----------------------------------------------------------------
age_dt <- get_age_metadata(12, gbd_round_id=7)
age_dt <- age_dt[age_group_years_start >= 40]


# import and compile death as incidence of dementia from DisMod estimates
df_incid <- interpolate(gbd_id_type = "modelable_entity_id", 
                        gbd_id = meid, 
                        location_id = location, 
                        measure_id = 6, 
                        source = "epi",
                        sex_id = c(1,2),
                        reporting_year_start = 1980,
                        reporting_year_end = 2022,
                        status='best',
                        age_group_id = age_dt[, unique(age_group_id)],
                        gbd_round_id = gbd_round,
                        decomp_step = decomp_step)

setDT(df_incid)
df_incid[, `:=` (measure_id = 1, modelable_entity_id = NULL, model_version_id = NULL)]

write.csv(df_incid, paste0(save_dir, location, ".csv"), row.names=F)

