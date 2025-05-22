#clear memory
rm(list=ls())

library("data.table")
library("ggplot2")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_cause_metadata.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/save_results_risk.R")
source("FILEPATH/csv2objects.R")
source("FILEPATH/submitExperiment.R")


#define args:
user <- Sys.getenv("USER")

config.path    <- "FILEPATH"
config.version <- "config20240817"
config.file <- file.path(config.path, paste0(config.version, ".csv"))
csv2objects(config.file, exclude = ls())


outRoot <- paste0("FILEPATH/FILEPATH", release)
outVersion <- config.version
outdir <- file.path(outRoot, 'temperature', 'pafs', outVersion)
sevdir <- file.path(outdir, 'sevs')


### GET LOCATION META-DATA ###
demog <- get_demographics(gbd_team = 'USERNAME', release_id = release)
loc_ids <- demog$location_id
loc_ids <- loc_ids[order(loc_ids)]

project <- "PROJECT"

arg.list <- list(loc_id = loc_ids, release = release, outdir = sevdir)
name.stub <- "sev"
name.args <- c("loc_id")


# set up run environment
sge.output.dir <- paste0(" -o /FILEPATH/", user, "/FILEPATH/%x.o%j.out -e /FILEPATH/", user, "/FILEPATH/%x.e%j.err ")
r.shell <- "FILEPATH/execRscript.sh"
save.script <- "FILEPATH/sevProcessor.R"

mem <- "10G"
slots <- 8

arg.table <- launch.jobs(arg.list = arg.list, name.stub = name.stub, name.args = name.args, outfile.suffix = "_2022.csv",
                         pause = 1, max.attempts = 1, return = T, relaunch = T)


status <- data.table(expand.grid(location_id = loc_ids, year_id = 1990:2024))
status[, complete := file.exists(file.path(outdir, "sevs", paste0(location_id, "_", year_id, ".csv")))]

table(status$complete)
table(status$year_id, status$complete)
table(status$location_id, status$complete)