################################################################################
## Purpose: Prep back-calculated incidence for upload
## Date created: 06/12/17
## Date modified: February 2019
## Author: Austin Carter, aucarter@uw.edu
## Run instructions: launch after aggregation
## Notes:
################################################################################

### Setup
rm(list=ls())
windows <- Sys.info()[1]=="Windows"
root <- ifelse(windows,"J:/","/home/j/")
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
code.dir <- paste0(ifelse(windows, "H:", paste0("/homes/", user)), "/hiv_gbd2019/")

## Packages
library(data.table)

## Arguments
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 0) {
	run.name <- args[1]
	loc <- args[2]
} else {
	run.name <- "200713_yuka"
	loc <- "PAK_53621"
}

### Paths
upload.path <- paste0( "FILEPATH", loc, "_ART_data.csv")
scaled.path <- paste0( "FILEPATH", run.name, "/", loc, ".csv")

### Functions
library(mortdb, lib =  "FILEPATH")

### Tables
loc.table <- data.table(get_locations(hiv_metadata = T))

### Code
scaled.dt <- fread(scaled.path)[, .(year_id, sex_id, age_group_id, run_num, scaled_new_hiv, population)]
scaled.dt[, rate := scaled_new_hiv / population]

upload.dt <- fread(upload.path)
merge.dt <- merge(upload.dt, scaled.dt[, .(year_id, sex_id, age_group_id, run_num, rate)], by = c("year_id", "sex_id", "age_group_id", "run_num"))
merge.dt[, new_hiv := rate]
merge.dt[, rate := NULL]

write.csv(merge.dt, upload.path, row.names = F)



### End