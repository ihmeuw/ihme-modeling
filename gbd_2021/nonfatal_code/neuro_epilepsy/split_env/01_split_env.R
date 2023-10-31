# ------------------------------------------------------------------------------
# Author: Emma Nichols, updated by Paul Briant
# Purpose: Child Script to Split Envelope into Primary/Secondary and 
# Primary into Severe, Not Severe, Treated no fits
# ------------------------------------------------------------------------------

user <- Sys.info()[['user']]
date <- gsub("-", "_", Sys.Date())

# ---LOAD LIBRARIES-------------------------------------------------------------

library(data.table)
library(magrittr)
library(readr)
library(mortcore, lib = "/ihme/mortality/shared/r/")

# ---GET TASK INFORMATION-------------------------------------------------------

args <- commandArgs(trailingOnly = TRUE)
env_id <- args[1]
primary_id <- args[2]
secondary_id <- args[3]
severe_id <- args[4]
notsevere_id <- args[5]
tnf_id <- args[6]
gbd_round_id <- args[7]
decomp_step <- args[8]
epilepsy_dir <- args[9]
functions_dir <- args[10]
share_path <- args[11]
map_path <- args[12]

params <- fread(map_path)
task_id <- Sys.getenv("SGE_TASK_ID")
message(paste("Task_id:", task_id))
loc_id <- params[task_num == task_id, location]
loc_id <- as.numeric(loc_id)
message(paste("Location_id:", loc_id))

# ---SET OBJECTS----------------------------------------------------------------

regressions_dir <- paste0(epilepsy_dir, "regressions/")
primary_dir <- paste0(epilepsy_dir, "/", primary_id, "/")
secondary_dir <- paste0(epilepsy_dir, "/", secondary_id, "/")
severe_dir <- paste0(epilepsy_dir, "/", severe_id, "/")
notsevere_dir <- paste0(epilepsy_dir, "/", notsevere_id, "/")
tnf_dir <- paste0(epilepsy_dir, "/", tnf_id, "/")
draws <- paste0("draw_", 0:999)
keep_cols <- c(
  "age_group_id", "year_id", "sex_id", "measure_id", "location_id", draws)

# ---SOURCE FUNCTIONS-----------------------------------------------------------

source(paste0(functions_dir, "get_demographics.R"))
source(paste0(functions_dir, "get_draws.R"))

# SLEEP FOR RANDOM TIME TO HELP FILE SYSTEM ------------------------------------

rtime <- runif(1, min = 0, max = 60)
Sys.sleep(rtime)

# ---GET DEMOGRAPHICS-----------------------------------------------------------

dems <- get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id)
sexes <- dems$sex_id
ages <- dems$age_group_id
years <- dems$year_id

# ---GET IMPAIRMENT ENVELOPE DRAWS----------------------------------------------

message("Getting impairment envelope draws")
env_draws <- get_draws(gbd_id_type = "modelable_entity_id", 
                       gbd_id = env_id, location_id = loc_id, 
                       measure_id = c(5, 6), 
                       status = "best", 
                       gbd_round_id = gbd_round_id, 
                       source = "epi", 
                       age_group_id = ages, 
                       sex_id = sexes, 
                       decomp_step=decomp_step)
setnames(env_draws, paste0("draw_", 0:999), paste0("env_", 0:999))

# ---GET PROPORTIONS------------------------------------------------------------

message("Reading in split proportions")
folders <- list.files(regressions_dir)
dates <- gsub("_", "-", folders)
last_date <- dates[which.max(as.POSIXct(dates))]
last_date <- gsub("-", "_", last_date)
idio_props <- read_rds(
  paste0(regressions_dir, last_date, "/idio_draws.rds"))
severe_props <- read_rds(
  paste0(regressions_dir, last_date, "/severe_draws.rds"))
tg_props <- read_rds(paste0(regressions_dir, last_date, "/tg_draws.rds"))
tnf_props <- read_rds(paste0(regressions_dir, last_date, "/tnf_draws.rds"))

# ---CALCULATE PRIMARY AND SECONDARY EPILEPSY-----------------------------------
# Split out primary (idiopathic) and secondary epilepsy from the epilepsy 
# impairment envelope by multiplying envelope draws by idiopathic proportion 
# draws to create primary epilepsy (env * idio_prop) and multiplying envelope 
# draws by the difference of one and idiopathic proportion draws to create 
# secondary epilepsy (env * (1 - idio_prop)). Secondary epilepsy is not used 
# for any additional computations but needs to be be uploaded because it is 
# part of other causes within the GBD cause hierarchy.
# ------------------------------------------------------------------------------

message("Splitting the impairment envelope into primary and secondary")
total <- merge(env_draws, idio_props, by = c("location_id", "year_id"))
primary <- copy(total)
secondary <- copy(total)
primary[
  , (draws) := lapply(0:999, function(x) get(
    paste0("env_", x)) * get(paste0("draw_", x)))]               
secondary[
  , (draws) := lapply(0:999, function(x) get(
    paste0("env_", x)) * (1-get(paste0("draw_", x))))]
primary <- primary[, keep_cols, with = F]
secondary <- secondary[, keep_cols, with = F]

# Copy primary estimates for calculating estimates for severe, not severe and 
# treated no fits.
primary_draws <- copy(primary)
setnames(primary_draws, paste0("draw_", 0:999), paste0("primary_", 0:999))

# ---CALCULATE SEVERE AS PRIMARY * PROPORTION SEVERE----------------------------

message("Splitting severe from primary")
severe <- merge(primary_draws, severe_props, by = c("location_id", "year_id"))
lesssevere <- copy(severe)
severe[
  , (draws) := lapply(0:999, function(x) get(
    paste0("primary_", x)) * get(paste0("draw_", x)))]
severe <- severe[, keep_cols, with = F]

# ---CALCULATE TOTAL NOT SEVERE AS PRIMARY * (1-PROPORTION SEVERE)--------------

message("Splitting not severe from primary")
lesssevere[
  , (paste0("lesssevere_", 0:999)) := lapply(0:999, function(x) get(
    paste0("primary_", x)) * (1-get(paste0("draw_", x))))]
lesssevere_cols <- c(
  "age_group_id", "year_id", "sex_id", "measure_id", "location_id", paste0(
    "lesssevere_", 0:999))
lesssevere <- lesssevere[, lesssevere_cols, with = F]

# ---CALCULATE TREATED AS NOT SEVERE * (1-TREATMENT GAP)------------------------

message("Calculating treated")
treated <- merge(lesssevere, tg_props, by = c("location_id", "year_id"))
treated[
  , (paste0("treated_", 0:999)) := lapply(0:999, function(x) get(
    paste0("lesssevere_", x)) * (1-get(paste0("draw_", x))))]
treated_cols <- c(
  "age_group_id", "year_id", "sex_id", "measure_id", "location_id", 
  paste0("treated_", 0:999))
treated <- treated[, treated_cols, with = F]

# ---CALCULATE TREATED WITHOUT FITS (TNF) AS TREATED * PROPORTION TNF-----------

message("Calculating treated without fits")
tnf <- merge(treated, tnf_props, by = c("location_id", "year_id"))
tnf[, (draws) := lapply(0:999, function(x) get(paste0("treated_", x)) * get(
  paste0("draw_", x)))]
tnf <- tnf[, keep_cols, with = F]

# ---CALCULATE FINAL NOT SEVERE AS TOTAL NOT SEVERE - TNF-----------------------

message("Calculating final not severe")
lesssevere <- merge(lesssevere, tnf, by = c("location_id", 
                                            "year_id", 
                                            "age_group_id", 
                                            "sex_id", 
                                            "measure_id"))
lesssevere[
  , (draws) := lapply(
    0:999, function(x) get(paste0("lesssevere_", x)) - get(paste0("draw_", x)))]
lesssevere <- lesssevere[, keep_cols, with = F]

# ---FORMAT AND WRITE-----------------------------------------------------------

message("Preparing to output files")
output_dir_list <- c(primary_dir, secondary_dir, severe_dir, notsevere_dir, 
                     tnf_dir)
estimates <- list(primary, secondary, severe, lesssevere, tnf)

if (length(output_dir_list) != length(estimates)) {
  message(length(output_dir_list))
  message(length(estimates))
  stop("Output lists need to be the same length")
}

items = length(output_dir_list)

for (i in 1:items) {
  message(paste("Writing estimates to", output_dir_list[i]))
  output <- paste0(output_dir_list[i], date, "/")
  if (!file.exists(output)){dir.create(output, recursive = T)}
  write.csv(estimates[i], paste0(output, loc_id, ".csv"), row.names = F)
}
