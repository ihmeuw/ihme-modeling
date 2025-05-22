###########################################################
### Project: Squeeze Prob/Def Migraine to Both
### Purpose: GBD 2019 Nonfatal Estimation
###########################################################

## SET-UP
rm(list=ls())

library(pacman, lib.loc = "FILEPATH")
pacman::p_load(data.table, ggplot2, readr)

## SET OBJECTS
dir.create("/FILEPATH/")
dir.create("/FILEPATH/")
dir.create("/FILEPATH/")
draws <- paste0("draw_", 0:999)

step <- "step4"

## SOURCE FUNCTIONS
source(paste0("FILEPATH", "get_draws.R"))
source(paste0("FILEPATH", "job_array.R"))
source(paste0("FILEPATH", "get_demographics.R"))

## GET ARGS AND ITEMS
args<-commandArgs(trailingOnly = TRUE)
map_path <-args[1]
save_dir <-args[2]
print(save_dir)
print(map_path)
params <- fread(map_path)
task_id <- Sys.getenv("SGE_TASK_ID")

print(task_id)
loc_id <- params[task_num == task_id, location]

## GET MAP AND MEs
map_dt <- fread(paste0("FILEPATH"))
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
                     sex_id = sexes, status = "best", decomp_step = step)
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
write.csv(probable, "FILEPATH")

definite <- copy(long)
definite[, c("probable", "both", "scale_factor") := NULL]
definite <- dcast(definite, ... ~ draw, value.var = "definite")
write.csv(definite, "FILEPATH"))

## SAVE SCALE FACTORS
scale_factors[, c("draw") := NULL]
scale_factors[, scale_factor := mean(scale_factor), by = c("age_group_id", "sex_id", "year_id", "measure_id")]
scale_factors <- unique(scale_factors, by = c("age_group_id", "sex_id", "year_id", "measure_id"))
write_rds(scale_factors, "FILEPATH")

