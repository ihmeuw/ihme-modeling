#' [Title: STH Submit Wasting
#' [Notes: Array job for Wasting from Stata Script

#############################################################################################
###'                                [General Set-Up]                                      ###
#############################################################################################

rm(list = ls())

# packages

source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
library(data.table)
library(stringr)
library(readstata13)
library(dplyr)
library(argparse)

# set-up run directory
run_file <- fread(FILEPATH)
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, FILEPATH)
draws_dir <- paste0(run_dir, FILEPATH)
interms_dir <- paste0(run_dir, FILEPATH)

params_dir <- FILEPATH
code_dir   <- FILEPATH

# parrallelize helpers 
source(FILEPATH)
my_shell <- FILEPATH
source(FILEPATH)

#############################################################################################
###'                                     [Qsub Prep]                                      ###
#############################################################################################

ifelse(!dir.exists(paste0(draws_dir, ADDRESS)), dir.create(paste0(draws_dir, ADDRESS)), FALSE)
ifelse(!dir.exists(paste0(draws_dir, ADDRESS)), dir.create(paste0(draws_dir, ADDRESS)), FALSE)
ifelse(!dir.exists(paste0(draws_dir, ADDRESS)), dir.create(paste0(draws_dir, ADDRESS)), FALSE)

demos       <- get_demographics(gbd_team = "epi", gbd_round_id = 6)
locs        <- demos$location_id

all_locs <- copy(locs)
 
no_file <- c()

for (loc in all_locs) {
if (!file.exists(paste0(draws_dir, ADDRESS, loc, ".csv"))) {
  no_file <- c(no_file, loc)
}}
 
locs <- no_file
loc_ids     <- data.table(location_id = locs)

decomp_step <- "step4"

# write out zero file
zero <- gen_zero_draws(modelable_entity_id = 1, location_id = 1, measure_id = 5, metric_id = 3)
fwrite(zero, paste0(interms_dir, FILEPATH))

#' add new locations to grs
grs_hook            <- fread(FILEPATH)
grs_ascar           <- fread(FILEPATH)
grs_trichur         <- fread(FILEPATH)


grs_hook[, restrict_hook := 1]
grs_ascar[, restrict_ascar := 1]           
grs_trichur[, restrict_trichur := 1]    

grs_hook <- grs_hook[, .(location_id, restrict_hook)]
grs_ascar <- grs_ascar[, .(location_id, restrict_ascar)]        
grs_trichur <- grs_trichur[, .(location_id, restrict_trichur)]    

grs_hook    <- distinct(grs_hook)
grs_ascar   <- distinct(grs_ascar)
grs_trichur <- distinct(grs_trichur)  

param_map <- merge(loc_ids, grs_hook, by = "location_id", all.x = TRUE)
param_map <- merge(param_map, grs_ascar, by = "location_id", all.x = TRUE)
param_map <- merge(param_map, grs_trichur, by = "location_id", all.x = TRUE)

param_map[is.na(restrict_trichur), restrict_trichur := 0 ]
param_map[is.na(restrict_ascar), restrict_ascar := 0 ]
param_map[is.na(restrict_hook), restrict_hook := 0 ]

param_map[, decomp_step := decomp_step]

num_jobs  <- nrow(param_map)
fwrite(param_map, paste0(interms_dir, FILEPATH), row.names = F)

#############################################################################################
###'                                        [Qsub]                                        ###
#############################################################################################

qsub(job_name = "STH_Wasting",
     shell    = my_shell,
     code     = paste0(code_dir, FILEPATH),
     args     = list("--param_path", paste0(interms_dir, FILEPATH)),
     project  = ADDRESS,
     m_mem_free = "10G",
     fthread = "3",
     archive = NULL,
     h_rt = "00:00:25:00",
     queue = ADDRESS,
     num_jobs = num_jobs)
