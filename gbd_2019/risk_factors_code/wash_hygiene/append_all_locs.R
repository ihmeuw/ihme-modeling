# Author: NAME
# Date: 2/22/2019
# Purpose: Combine draws for all locations into one file to prep for post-ST-GPR processing [WaSH]

rm(list=ls())

library(data.table)

arg <- commandArgs(trailingOnly = T)
print(arg)

me_name <- as.character(arg[1])
run_id <- as.numeric(arg[2])
me_parent <- as.character(arg[3])
run <- as.character(arg[4])
decomp_step <- as.character(arg[5])

# --------------------------------------------------

# in
input.dir <- file.path("FILEPATH")
files <- list.files(input.dir)

# out
output.dir <- file.path("FILEPATH")
if(!dir.exists(output.dir)) dir.create(output.dir, recursive = TRUE)

# create data.table w/ draws for all locs & save
all_locs <- rbindlist(lapply(file.path(input.dir,files), fread), use.names = TRUE)
write.csv(all_locs, paste0(file.path(output.dir, me_name), ".csv"))
