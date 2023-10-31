#Child script to pull draws from RHD models
rm(list=ls())

os <- .Platform$OS.type
if (os=="windows") {
  j<- "FILEPATH"
} else {
  j<- "/FILEPATH/"
}

date<-gsub("-", "_", Sys.Date())

library(ggplot2)
library(data.table)
library(openxlsx)
suppressMessages(library(R.utils))

################### PATHS AND ARGS #########################################
args <- commandArgs(trailingOnly = TRUE)
l <- as.integer(args[1])
endem <- as.integer(args[2])

output <- "/FILEPATH/"

sourceDirectory("/FILEPATH/")

################### GET DATA #########################################
me_id <- ifelse(endem==1, 3075, 3076) #set endemic vs non-endemic

message("Getting draws for loc ", l, "...")
draws <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=me_id, source="epi", measure_id=c(5, 6), location_id=l, status="best", decomp_step="iterative", gbd_round_id = 7)
message(" Done getting draws")

draws[, modelable_entity_id:=1810]
message("Writing outputs...")

for(meas in c(5, 6)){
  write.csv(draws[measure_id==meas], file=paste0(output, meas,"_", l, ".csv"), row.names=F)
}

message(" Done writing outputs")

