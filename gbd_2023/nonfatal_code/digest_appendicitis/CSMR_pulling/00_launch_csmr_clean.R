###############################################################################
## Purpose: - Pull post-CoDCorrect death counts for by loc/year/sex/age"
###############################################################################
rm(list=ls())

##load in shared functions 
invisible(sapply(list.files("FILEPATH", full.names = T), source))
pacman::p_load(reticulate, data.table, ggplot2, dplyr, stringr, tidyr, DBI, openxlsx, gtools, gridExtra,
               glue)

library(data.table)
#set objects
release_id <- OBJECT
date<-gsub("-","_",Sys.Date())
home_dir <- "FILEPATH"
script <- paste0(" ", home_dir, "01_create_csmr_final.R")

input_version <- paste0("appendicitis_csmr_", date)
save_dir <- paste0("FILEPATH", input_version)
if(!dir.exists(save_dir)) dir.create(save_dir)
save_dir <- paste0("FILEPATH")

shared_functions_dir <- "FILEPATH"
## LOCATIONS
epi_locations <- get_location_metadata(release_id = 16, location_set_id=9)
location_ids <- epi_locations[is_estimate==1 & most_detailed==1]
location_ids <- location_ids[,location_id]


#test two location 
#location_ids <- location_ids[1:2]

## SUBMIT JOBS
shell <- " FILEPATH" 
project <- '-A proj_rgud ' 
slurm_output_dir <- "FILEPATH"
fthreads <- 5 
mem <- 80
q <- " -p long.q "

job_name<- paste0('-J appendicitis_csmr')

for (location in location_ids) {
sys_sub<- paste0('sbatch ', project, ' -c ', fthreads, slurm_output_dir, job_name, q,' --mem=', mem, 'G' )
print(paste(sys_sub, shell, "-s", script, save_dir, location))
system(paste(sys_sub, shell, "-s", script, save_dir, location)) }

files <- list.files(path = save_dir,pattern = ".csv", full.names = TRUE)
length(files)
temp <- lapply(files, fread, sep=",")
dt_all <- rbindlist(temp, fill = TRUE, use.names = TRUE)

drop_cols <- c("cause_id", "measure_id", "metric_id", "run_id")
dt_all[, (drop_cols) := NULL]
dt_all$nid <- 416752 # this is the NID specific for cause specific EMR for appendicitis

write.xlsx(dt_all, "FILEPATH", row.names = FALSE, sheetName = "extraction")