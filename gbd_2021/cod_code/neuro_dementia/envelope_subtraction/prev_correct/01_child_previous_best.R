################################################################################
### Author: USERNAME, updated by USERNAME to pull in 2019 best results
### Project: GBD Nonfatal Estimation
### Purpose: Get 2019 results
################################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH" 
  h <- "FILEPATH"
  l <- "FILEPATH"
} else { 
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
}

pacman::p_load(data.table, openxlsx, ggplot2, readr)
library(mortcore, lib = "FILEPATH")
library(mortdb, lib = "FILEPATH") 
date <- gsub("-", "_", Sys.Date())

# SET OBJECTS -------------------------------------------------------------

functions_dir <- "FILEPATH"
main_dir <- "FILEPATH"
stroke_dir <- paste0(main_dir, "FILEPATH")
tbi_dir <- paste0(main_dir, "FILEPATH")
ds_dir <- paste0(main_dir, "FILEPATH")
pd_dir <- paste0(main_dir, "FILEPATH")
draws <- paste0("draw_", 0:999)

## GET ARGS AND ITEMS
args<-commandArgs(trailingOnly = TRUE)
location <- args[1]

# SOURCE FUNCTIONS --------------------------------------------------------

source(paste0(functions_dir, "get_draws.R"))
source(paste0(functions_dir, "get_age_metadata.R"))


# GET AGES ----------------------------------------------------------------
source(paste0(functions_dir, "get_age_metadata.R"))

age_dt <- get_age_metadata(ID, gbd_round_id=ID)
age_dt <- age_dt[age_group_id >= ID]

# GET MEIDS ---------------------------------------------------------------

tbime_dt <- as.data.table(read.xlsx("FILEPATH"))
tbi_ids <- tbime_dt[!modelable_entity_id == 17394, modelable_entity_id] 
stroke_ids <- c(IDS) 
ds_ids <- ID
park_ids <- ID

# PULL RELEVANT DATA ------------------------------------------------------

tbi <- get_draws(gbd_id_type = rep("modelable_entity_id", length(tbi_ids)), gbd_id = tbi_ids, 
                 source = "epi", measure_id = c(5, 35, 36), location_id = location, sex_id = c(1, 2), 
                 status = "best", metric_id = 3, age_group_id = age_dt[, unique(age_group_id)], 
                 year_id = c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019), gbd_round_id=6, decomp_step="step4")
tbi[, (draws) := lapply(.SD, sum), by = c("age_group_id", "sex_id", "year_id"), .SDcols = draws]
tbi <- unique(tbi, by = c("age_group_id", "sex_id", "year_id"))
tbi[, modelable_entity_id := NULL]
tbi[, measure_id := 5]

stroke <- get_draws(gbd_id_type = rep("modelable_entity_id", length(stroke_ids)), gbd_id = stroke_ids, 
                    source = "epi", measure_id = 5, location_id = location, sex_id = c(1, 2), 
                    status = "best", age_group_id = age_dt[, unique(age_group_id)], gbd_round_id=6, decomp_step="step4")
stroke <- stroke[metric_id == 3]
stroke[, (draws) := lapply(.SD, sum), by = c("age_group_id", "sex_id", "year_id"), .SDcols = draws]
stroke <- unique(stroke, by = c("age_group_id", "sex_id", "year_id"))
stroke[, modelable_entity_id := NULL]

get_normal_prev <- function(id){
  prev <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = id, source = "epi", measure_id = 5,
                    location_id = location, sex_id = c(1, 2), status = "best", metric_id = 3, 
                    age_group_id = age_dt[, unique(age_group_id)], decomp_step = "step4", gbd_round_id = 6)
  prev <- prev[, c("age_group_id", "sex_id", "year_id", draws), with = F]
  setnames(prev, draws, paste0("prev_", 0:999))
  return(prev)
}


ds_prev <- get_normal_prev(id = ds_ids)
ds_prev[age_group_id >= 19, paste0("prev_", 0:999) := 0]
pd_prev <- get_normal_prev(id = park_ids)


# SAVE FILES --------------------------------------------------------------

readr::write_rds(tbi, paste0(tbi_dir, location, ".rds"))
readr::write_rds(stroke, paste0(stroke_dir, location, ".rds"))
readr::write_rds(pd_prev, paste0(pd_dir, location, ".rds"))
readr::write_rds(ds_prev, paste0(ds_dir, location, ".rds"))
