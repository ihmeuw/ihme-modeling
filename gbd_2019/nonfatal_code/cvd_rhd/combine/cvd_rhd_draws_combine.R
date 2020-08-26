#Child script to pull draws from RHD models
rm(list=ls())

os <- .Platform$OS.type
if (os=="windows") {
  j<- "FILEPATH"
} else {
  j<- "FILEPATH"
}

date<-gsub("-", "_", Sys.Date())

.libPaths("FILEPATH")
library(ggplot2)
library(data.table)
library(openxlsx)
suppressMessages(library(R.utils))

################### PATHS AND ARGS #########################################
args <- commandArgs(trailingOnly = TRUE)
l <- as.integer(args[1])
endem <- as.integer(args[2])

output <- "FILEPATH"

sourceDirectory("FILEPATH")

################### GET DATA #########################################
me_id <- ifelse(endem==1, "VALUE", "VALUE") #set endemic vs non-endemic

message("Getting draws for loc ", l, "...")
draws <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=me_id, source="epi", measure_id=c(5, 6), location_id=l, status="best", decomp_step="step2")
message(" Done getting draws")

draws[, modelable_entity_id:="VALUE"]
message("Writing outputs...")

for(meas in c(5, 6)){
  write.csv(draws[measure_id==meas], file=paste0(output, meas,"_", l, ".csv"), row.names=F)
}

message(" Done writing outputs")

