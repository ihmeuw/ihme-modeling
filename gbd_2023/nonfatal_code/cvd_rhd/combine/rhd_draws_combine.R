#Child script to pull draws from RHD models
rm(list=ls())

library(data.table)

source("/FILEPATH/get_draws.R")

################### PATHS AND ARGS #########################################
args <- commandArgs(trailingOnly = TRUE)
task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))

job_params_path <- as.character(args[1])
gbd_release <- as.integer(args[2])
output <- as.character(args[3])

job_params <- fread(job_params_path)
l <- job_params[task_id, location_id]
endem <- job_params[task_id, endemic]

################### GET DATA #########################################
me_id <- ifelse(endem==1, 3075, 3076) #set endemic vs non-endemic

# Pull draws
message("Getting draws for loc ", l, "...")
draws <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=me_id, source="epi", measure_id=c(5, 6), location_id=l, status="best", release_id=gbd_release)
message(" Done getting draws")

# Change ME to 1810 for uploading
draws[, modelable_entity_id:=1810]

# Write out csvs for uploading
message("Writing outputs...")
write.csv(draws[measure_id %in% c(5,6)], file=paste0(output, l, ".csv"), row.names=F)
message(" Done writing outputs")
