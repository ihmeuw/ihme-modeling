###########################################################
### Author: 
### Date: 10/20/17
### Project: Squeeze Prob/Def Migraine to Both
### Purpose: GBD 2017 Nonfatal Estimation
###########################################################

## SET-UP
rm(list=ls())

if (Sys.info()[1] == "Linux"){
  j_root <- "/home/j/"
  h_root <- "~/"
} else if (Sys.info()[1] == "Windows"){
  j_root <- "J:/"
  h_root <- "H:/"
}

library(pacman, lib.loc = FILEPATH)
pacman::p_load(data.table, ggplot2, readr)

## SET OBJECTS
date <- gsub("-", "_", Sys.Date())
repo_dir <- paste0(h_root, FILEPATH)
squeeze_dir <- paste0(repo_dir, FILEPATH)
functions_dir <- paste0(j_root, FILEPATH)
dir.create(paste0(FILEPATH, date, "/"))
dir.create(paste0(FILEPATH, date, "/"))
dir.create(paste0(FILEPATH, date, "/"))
save_prob_dir <- paste0(FILEPATH, date, "/")
save_def_dir <- paste0(FILEPATH, date, "/")
save_scale_dir <- paste0(FILEPATH, date, "/")
draws <- paste0("draw_", 0:999)

## SOURCE FUNCTIONS
source(paste0(functions_dir, "get_draws.R"))
source(paste0(squeeze_dir, "job_array.R"))
source(paste0(functions_dir, "get_demographics.R"))

## GET TASK INFORMATION
getit <- job.array.child()
print(commandArgs()) 
loc_id <- getit[[1]] # grab the unique PARAMETERS for this task id
loc_id <- as.numeric(loc_id)
print(loc_id)

## GET MAP AND MEs
map_dt <- fread(paste0(repo_dir, "bundle_map.csv"))
prob_me <- map_dt[bundle_name == "prob_migraine_adj", meid]
def_me <- map_dt[bundle_name == "def_migraine_adj", meid]
both_me <- map_dt[bundle_name == "migraine_adj", meid]

## GET IDS
ids <- get_demographics(gbd_team = "epi")
years <- ids$year_id
sexes <- ids$sex_id

## GET DRAWS
draws_dt <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = c(both_me, def_me, prob_me), source = "epi", 
                     measure_id = c(5, 6), location_id = loc_id, year_id = years, age_group_id = c(6:20, 30:32, 235),
                     sex_id = sexes, status = "best")
draws_dt[modelable_entity_id == 1959, type := "both"]
draws_dt[modelable_entity_id == 19684, type := "probable"]
draws_dt[modelable_entity_id == 19685, type := "definite"]

## SQUEEZE
long <- melt(draws_dt, measure.vars = draws, variable.name = "draw")
long[, c("modelable_entity_id", "model_version_id") := NULL]
long <- dcast(long, ... ~ type, value.var = "value")
long[, scale_factor := both/(probable + definite)]
scale_factors <- copy(long)
long[, `:=` (probable = probable * scale_factor,
             definite = definite * scale_factor)]

## SEPERATE AND SAVE
probable <- copy(long)
probable[, c("definite", "both", "scale_factor") := NULL]
probable <- dcast(probable, ... ~ draw, value.var = "probable")
write.csv(probable, paste0(save_prob_dir, loc_id, ".csv"))
definite <- copy(long)
definite[, c("probable", "both", "scale_factor") := NULL]
definite <- dcast(definite, ... ~ draw, value.var = "definite")
write.csv(definite, paste0(save_def_dir, loc_id, ".csv"))

## SAVE SCALE FACTORS
scale_factors[, c("draw") := NULL]
scale_factors[, scale_factor := mean(scale_factor), by = c("age_group_id", "sex_id", "year_id", "measure_id")]
scale_factors <- unique(scale_factors, by = c("age_group_id", "sex_id", "year_id", "measure_id"))
write_rds(scale_factors, paste0(save_scale_dir, loc_id, ".rds"))

