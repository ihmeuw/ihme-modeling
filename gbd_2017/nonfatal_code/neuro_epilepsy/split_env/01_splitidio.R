##########################################################################
## Author: 
## Date: December 4th, 2017
## Purpose: Child Script to Split Envelope into Idiopathic/Secondary
##########################################################################

## LOAD LIBRARIES
library(pacman, lib.loc = FILEPATH)
pacman::p_load(data.table, magrittr, readr)

## SET OBJECTS
repo_dir <- FILEPATH
functions_dir <- FILEPATH
epilepsy_dir <- FILEPATH
regressions_dir <- paste0(epilepsy_dir, "regressions/")
secondary_dir <- paste0(epilepsy_dir, "secondary/")
primary_dir <- paste0(epilepsy_dir, "primary/")
draws <- paste0("draw_", 0:999)
date <- gsub("-", "_", Sys.Date())
keep_cols <- c("age_group_id", "year_id", "sex_id", "measure_id", draws)

## SOURCE FUNCTIONS
source(paste0(repo_dir, "job_array.R"))
source(paste0(functions_dir, "get_demographics.R"))
source(paste0(functions_dir, "get_draws.R"))

## GET TASK INFORMATION
getit <- job.array.child()
print(commandArgs()) 
loc_id <- getit[[1]] # grab the unique PARAMETERS for this task id
loc_id <- as.numeric(loc_id)
print(loc_id)

## GET DEMOGRAPHICS
dems <- get_demographics(gbd_team = "epi")
sexes <- dems$sex_id
ages <- dems$age_group_id
years <- dems$year_id

## GET DRAWS
env_draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = 2403, location_id = loc_id, 
                       measure_id = c(5, 6), status = "best", gbd_round_id = 5, source = "epi", 
                       age_group_id = ages, sex_id = sexes)
setnames(env_draws, paste0("draw_", 0:999), paste0("env_", 0:999))

## GET PROPORTIONS
folders <- list.files(regressions_dir)
dates <- gsub("_", "-", folders)
last_date <- dates[which.max(as.POSIXct(dates))]
last_date <- gsub("-", "_", last_date)
idio_props <- read_rds(paste0(regressions_dir, "/", last_date, "/idio_draws.rds"))

## MERGE AND CALCULATE
total <- merge(env_draws, idio_props, by = c("location_id", "year_id"))
primary <- copy(total)
secondary <- copy(total)
primary[, (draws) := lapply(0:999, function(x) get(paste0("env_", x)) * get(paste0("draw_", x)))]               
secondary[, (draws) := lapply(0:999, function(x) get(paste0("env_", x)) * (1-get(paste0("draw_", x))))]
primary <- primary[, keep_cols, with = F]
secondary <- secondary[, keep_cols, with = F]

## FORMAT AND WRITE
dir.create(paste0(primary_dir, date))
primary_date_dir <- paste0(primary_dir, date, "/")
write.csv(primary, paste0(primary_date_dir, loc_id, ".csv"), row.names = F)

dir.create(paste0(secondary_dir, date))
secondary_date_dir <- paste0(secondary_dir, date, "/")
write.csv(secondary, paste0(secondary_date_dir, loc_id, ".csv"), row.names = F)


