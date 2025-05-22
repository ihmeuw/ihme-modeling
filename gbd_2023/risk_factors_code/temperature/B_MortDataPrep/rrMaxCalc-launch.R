#clear memory
rm(list=ls())

library("data.table")
library("dplyr")
source("FILEPATH/csv2objects.R")
source("FILEPATH/submitExperiment.R")

#define args:
config.path    <- "FILEPATH"
config.version <- "config20240817"
config.file <- file.path(config.path, paste0(config.version, ".csv"))

csv2objects(config.file, exclude = ls())

popType <- "annual" # "fixed" or "annual"

missingOnly <- T

user <- Sys.getenv("USER")

version <- config.version
yearList <- 1990:gbd_round_year
zoneList <- 6:28

# set up run environment
project <- "USERNAME"
sge.output.dir <- paste0(" -o /FILEPATH/", user, "/output/%x.o%j -e /FILEPATH/", user, "/errors/%x.e%j ")
r.shell <- "/FILEPATH/execRscript.sh"


slots <- 8
mem <- "32G"
save.script <- paste0("/FILEPATH/rrMaxCalc-newApproach.R")

outdir <- paste0("/FILEPATH/", version, "/rrMax/")
dir.create(outdir, recursive = T)

arg.list <- list(cause = causeList, year = yearList, version = version,
                 popType = popType, zone = zoneList, rrDir = rrDir,
                 tmrelDir = tmrelDir, description = description, outdir = outdir)

name.stub <- "rrMax"
name.args <- c("cause", "zone", "year")

arg.table <- launch.jobs(arg.list = arg.list, name.stub = name.stub, name.args = name.args, outfile.suffix = ".csv",
                         pause = 1, max.attempts = 1, return = T, relaunch = T)


status <- data.table(expand.grid(causeList, 6:28, yearList))
names(status) <- c('cause', 'zone', 'year')

status[, file := paste0(outdir, "rrMax_", cause, "_", zone, "_", year, ".csv")]
status[, status := file.exists(file)]


table(status$status)


### ASSEMBLE FILES AND COMPLETE AGGREGATIONS ###

slots <- 8
mem <- "16G"
save.script <- paste0("FILEPATH/rrMaxCompile.R")

arg.list <- list(cause = causeList, yearStart = 1990, yearEnd = gbd_round_year, zone = zoneList, outdir = outdir)

name.stub <- "rrMax"
name.args <- c("cause", "zone")

arg.table <- launch.jobs(arg.list = arg.list, name.stub = name.stub, name.args = name.args, outfile.suffix = ".csv",
                         pause = 1, max.attempts = 1, return = T, relaunch = T)


status <- data.table(expand.grid(causeList, 6:28))
names(status) <- c('cause', 'zone')
status[, status := file.exists(paste0(outdir, "rrMaxDraws_", cause, "_zone", zone, ".csv"))]

table(status$status)
table(status$zone, status$status)