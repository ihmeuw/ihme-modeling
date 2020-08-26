##########################################################################
## Purpose: Child Script to Split Primary into Severe, Not Severe, Treated
##          no fits
##########################################################################

## LOAD LIBRARIES
library(data.table)
library(magrittr)
library(readr)

## SET OBJECTS
decomp_step = "step4"
user <- Sys.info()[['user']]
share_path <- "FILEPATH"
functions_dir <- "FILEPATH"
epilepsy_dir <- "FILEPATH"
regressions_dir <- "FILEPATH"
notsevere_dir <- "FILEPATH"
severe_dir <- "FILEPATH"
tnf_dir <- "FILEPATH"
draws <- paste0("draw_", 0:999)
date <- gsub("-", "_", Sys.Date())
keep_cols <- c("age_group_id", "year_id", "sex_id", "measure_id", "location_id", draws)

## SOURCE FUNCTIONS
source(paste0(share_path, "job_array.R"))
source(paste0(functions_dir, "get_demographics.R"))
source(paste0(functions_dir, "get_draws.R"))

## GET TASK INFORMATION
getit <- job.array.child()
print(commandArgs()) # debugging tool: showing command args
loc_id <- getit[[1]] # grab the unique PARAMETERS for this task id
loc_id <- as.numeric(loc_id)
print(loc_id)

## GET DEMOGRAPHICS
dems <- get_demographics(gbd_team = "epi")
sexes <- dems$sex_id
ages <- dems$age_group_id
years <- dems$year_id

## GET DRAWS
primary_draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = 3025, location_id = loc_id, 
                       measure_id = c(5, 6), status = "best", gbd_round_id = 6, source = "epi", 
                       age_group_id = ages, sex_id = sexes, decomp_step=decomp_step)
setnames(primary_draws, paste0("draw_", 0:999), paste0("primary_", 0:999))

## GET PROPORTIONS
folders <- list.files(regressions_dir)
dates <- gsub("_", "-", folders)
last_date <- dates[which.max(as.POSIXct(dates))]
last_date <- gsub("-", "_", last_date)

severe_props <- read_rds(paste0(regressions_dir, last_date, "/severe_draws.rds"))
tg_props <- read_rds(paste0(regressions_dir, last_date, "/tg_draws.rds"))
tnf_props <- read_rds(paste0(regressions_dir, last_date, "/tnf_draws.rds"))


## MERGE AND CALCULATE
## CALCULATE SEVERE AS PRIMARY * PROPORTION SEVERE
severe <- merge(primary_draws, severe_props, by = c("location_id", "year_id"))
lesssevere <- copy(severe)
severe[, (draws) := lapply(0:999, function(x) get(paste0("primary_", x)) * get(paste0("draw_", x)))]
severe <- severe[, keep_cols, with = F]

## CALCULATE TOTAL NOT SEVERE AS PRIMARY * (1-PROPORTION SEVERE)
lesssevere[, (paste0("lesssevere_", 0:999)) := lapply(0:999, function(x) get(paste0("primary_", x)) * (1-get(paste0("draw_", x))))]
lesssevere_cols <- c("age_group_id", "year_id", "sex_id", "measure_id", "location_id", paste0("lesssevere_", 0:999))
lesssevere <- lesssevere[, lesssevere_cols, with = F]

## CALCULATE TREATED AS NOT SEVERE * (1-TREATMENT GAP)
treated <- merge(lesssevere, tg_props, by = c("location_id", "year_id"))
treated[, (paste0("treated_", 0:999)) := lapply(0:999, function(x) get(paste0("lesssevere_", x)) * (1-get(paste0("draw_", x))))]
treated_cols <- c("age_group_id", "year_id", "sex_id", "measure_id", "location_id", paste0("treated_", 0:999))
treated <- treated[, treated_cols, with = F]

## CALCULATE TREATED WITHOUT FITS (TNF) AS TREATED * PROPORTION TNF
tnf <- merge(treated, tnf_props, by = c("location_id", "year_id"))
tnf[, (draws) := lapply(0:999, function(x) get(paste0("treated_", x)) * get(paste0("draw_", x)))]
tnf <- tnf[, keep_cols, with = F]

## CALCULATE FINAL NOT SEVERE AS TOTAL NOT SEVERE - TNF
lesssevere <- merge(lesssevere, tnf, by = c("location_id", "year_id", "age_group_id", "sex_id", "measure_id"))
lesssevere[, (draws) := lapply(0:999, function(x) get(paste0("lesssevere_", x)) - get(paste0("draw_", x)))]
lesssevere <- lesssevere[, keep_cols, with = F]

## FORMAT AND WRITE
if (!file.exists(paste0(notsevere_dir, date))){dir.create(paste0(notsevere_dir, date))}
notsevere_date_dir <- paste0(notsevere_dir, date, "/")
write.csv(lesssevere, paste0(notsevere_date_dir, loc_id, ".csv"), row.names = F)

if (!file.exists(paste0(severe_dir, date))){dir.create(paste0(severe_dir, date))}
severe_date_dir <- paste0(severe_dir, date, "/")
write.csv(severe, paste0(severe_date_dir, loc_id, ".csv"), row.names = F)

if (!file.exists(paste0(tnf_dir, date))){dir.create(paste0(tnf_dir, date))}
tnf_date_dir <- paste0(tnf_dir, date, "/")
write.csv(tnf, paste0(tnf_date_dir, loc_id, ".csv"), row.names = F)
