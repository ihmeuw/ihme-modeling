###########################################################
### Author: 
### Date: 10/20/17
### Project: Split Migraine into asym/sym
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
functions_dir <- paste0(j_root, FILEPATH)
headache_dir <- FILEPATH
sev_dir <- paste0(headache_dir, "sev_draws/")
dir.create(paste0(headache_dir, "probable_sym/", date, "/"))
dir.create(paste0(headache_dir, "definite_sym/", date, "/"))
dir.create(paste0(headache_dir, "probable_asym/", date, "/"))
dir.create(paste0(headache_dir, "definite_asym/", date, "/"))
save_probsym_dir <- paste0(headache_dir, "probable_sym/", date, "/")
save_defsym_dir <- paste0(headache_dir, "definite_sym/", date, "/")
save_probasym_dir <- paste0(headache_dir, "probable_asym/", date, "/")
save_defasym_dir <- paste0(headache_dir, "definite_asym/", date, "/")
draws <- paste0("draw_", 0:999)
prob_me <- 20190
def_me <- 20191

## SOURCE FUNCTIONS
source(paste0(functions_dir, "get_draws.R"))
source(paste0(repo_dir, "job_array.R"))
source(paste0(functions_dir, "get_demographics.R"))

## GET TASK INFORMATION
getit <- job.array.child()
print(commandArgs()) 
loc_id <- getit[[1]] # grab the unique PARAMETERS for this task id
loc_id <- as.numeric(loc_id)
print(loc_id)

## GET IDS
ids <- get_demographics(gbd_team = "epi")
years <- ids$year_id
sexes <- ids$sex_id

## GET DRAWS
draws_dt <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = c(def_me, prob_me), source = "epi", 
                      measure_id = c(5, 6), location_id = loc_id, year_id = years, age_group_id = c(6:20, 30:32, 235),
                      sex_id = sexes, status = "best")
draws_dt[, id := 1]

## GET SEVERITY DRAWS AND FORMAT
files <- list.files(sev_dir)
dates <- substr(files, 1, 10)
dates <- gsub("_", "-", dates)
last_date <- dates[which.max(as.POSIXct(dates))]
last_date <- gsub("-", "_", last_date)
timesym <- as.data.table(read_rds(paste0(sev_dir, last_date, ".rds")))
timesym[, id := 1]
def <- copy(timesym[, .(variable, id, timesymdef)])
def <- dcast(def, id ~ variable, value.var = "timesymdef")
setnames(def, draws, paste0("def_", 0:999))
prob <- copy(timesym[, .(variable, id, timesymprob)])
prob <- dcast(prob, id ~ variable, value.var = "timesymprob")
setnames(prob, draws, paste0("prob_", 0:999))

## MERGE AND CALC DEF
def_draws <- merge(draws_dt[modelable_entity_id == def_me], def, by = "id")
def_draws[, id := NULL]
defsym_draws <- copy(def_draws)
defsym_draws[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("def_", x)))]
defsym_draws[, c(paste0("def_", 0:999), "model_version_id", "modelable_entity_id") := NULL]
defasym_draws <- copy(def_draws)
defasym_draws[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * (1 - get(paste0("def_", x))))]
defasym_draws[, c(paste0("def_", 0:999), "model_version_id", "modelable_entity_id") := NULL]

## MERGE AND CALC PROB
prob_draws <- merge(draws_dt[modelable_entity_id == prob_me], prob, by = "id")
prob_draws[, id := NULL]
probsym_draws <- copy(prob_draws)
probsym_draws[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("prob_", x)))]
probsym_draws[, c(paste0("prob_", 0:999), "model_version_id", "modelable_entity_id") := NULL]
probasym_draws <- copy(prob_draws)
probasym_draws[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * (1 - get(paste0("prob_", x))))]
probasym_draws[, c(paste0("prob_", 0:999), "model_version_id", "modelable_entity_id") := NULL]

## SAVE FILES
write.csv(defsym_draws, paste0(save_defsym_dir, loc_id, ".csv"), row.names = F)
write.csv(defasym_draws, paste0(save_defasym_dir, loc_id, ".csv"), row.names = F)
write.csv(probsym_draws, paste0(save_probsym_dir, loc_id, ".csv"), row.names = F)
write.csv(probasym_draws, paste0(save_probasym_dir, loc_id, ".csv"), row.names = F)


