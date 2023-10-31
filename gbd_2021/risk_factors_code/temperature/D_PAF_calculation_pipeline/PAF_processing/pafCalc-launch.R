## Launches PAF calculations for temperature.
# source("/FILEPATH/pafCalc-launch.R", echo = T)

## SYSTEM SETUP ----------------------------------------------------------
# Clear memory
rm(list=ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]


# Drives 
j <- if (os == "Linux") "/home/j/" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("/homes/", user, "/") else if (os == "Windows") "H:/"


# Base filepaths
work_dir <- paste0(j, '/FILEPATH/')
share_dir <- 'FILEPATH' # only accessible from the cluster
code_dir <- if (os == "Linux") paste0("/FILEPATH/", user, "/") else if (os == "Windows") ""


## LOAD DEPENDENCIES -----------------------------------------------------
library(data.table)
library(ggplot2)
library(argparse)
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/r/get_cause_metadata.R")
source(paste0(code_dir, "FILEPATH/csv2objects.R"))
# Sys.setenv("RETICULATE_PYTHON"='FILEPATH')  # Set the Python interpreter path
# library(jobmonr)
source("submitExperiment.R")




## DEFINE ARGS -----------------------------------------------------------
parser <- ArgumentParser()
parser$add_argument("--year_start", help = "first of years to evaluate",
                    default = 1990, type = "integer")
parser$add_argument("--year_end", help = "last of years to evaluate",
                    default = 2021, type = "integer")
parser$add_argument("--release_id", help = "GBD release ID",
                    default = 10, type = "integer")
parser$add_argument("--proj", help = "project to run on",
                    default = 'gbd', type = "character")
parser$add_argument("--scenario", help = "name of scenario to run",
                    default = 'rcp45', type = "character")
parser$add_argument("--config.path", help = "path to config file",
                    default = '/FILEPATH/config', type = "character")
parser$add_argument("--config.version", help = "name of config file version",
                    default = 'config20220728b', type = "character")
parser$add_argument("--outRoot", help = "root of output path",
                    default = '/FILEPATH/', type = "character")
parser$add_argument("--outVersion", help = "name of output version",
                    default = 'version_15.0', type = "character")
parser$add_argument("--release_id", help = "release ID to run for",
                    default = 10, type = "integer")
parser$add_argument("--config_version", help = "name of config file version",
                    default = 'config20220728b', type = "character")
parser$add_argument("--exp_outdir", help = "output path for era2melt step",
                    default = '/FILEPATH/', type = "character")
parser$add_argument("--rrMax_outRoot", help = "directory to which RRmax results were output",
                    default = "/FILEPATH/", type = "character")
parser$add_argument("--rrDir", help = "folder to pull relative risk samples from",
                    #default = "/FILEPATH/", type = "character")
                    default = "/FILEPATH/", type = "character")
parser$add_argument("--tmrelRoot", help = "folder to pull TMREL inputs from",
                    default = "/FILEPATH/", type = "character")
parser$add_argument("--resume", help = "whether you're resuming an existing pipeline",
                    default = F, type = "logical")
args <- parser$parse_args()
list2env(args, environment()); rm(args)

yearList <- year_start:year_end

config.file <- file.path(config.path, paste0(config.version, ".csv"))
csv2objects(config.file, exclude = ls())

## BODY ------------------------------------------------------------------
# Get location metadata ---------------------------------------
if (proj=="NAME") {
  outdir <- paste0(outRoot, outVersion)

  cod <- fread(cod_source)
  loc_ids <- unique(cod$location_id)

  loc_ids <- loc_ids[order(loc_ids)]
  project <- "NAME"
} else {
  outdir <- paste0(outRoot, outVersion)
  locSet <- 35
  locMeta <- get_location_metadata(location_set_id = locSet, release_id = release_id)
  loc_ids <- locMeta[(is_estimate==1 & most_detailed==1) | level==3 , location_id]
  loc_ids <- loc_ids[order(loc_ids)]
  project <- "NAME"

}

dir.create(outdir, recursive = T)
dir.create(paste0(outdir, "/heat"))
dir.create(paste0(outdir, "/cold"))
dir.create(paste0(outdir, "/sevs"))
dir.create(paste0(outdir, "/sevs/raw"))


arg.list <- list(loc_id = loc_ids, year = yearList, outdir = outdir, proj = proj, scenario = scenario,
                 release_id = release_id, config_version = config_version, exp_outdir = exp_outdir,
                 rrMax_outRoot = rrMax_outRoot, rrDir = rrDir, tmrelRoot = tmrelRoot)
name.stub <- "paf"
name.args <- c("loc_id", "year")


# Set up run environment
sge.output.dir <- paste0(" -o /FILEPATH/", user, "/output/%x.o%j -e /FILEPATH/", user, "/errors/%x.e%j ")
r.shell <- "/FILEPATH/execRscript.sh"
save.script <- paste0(code_dir, "FILEPATH/pafCalc_sevFix.R")
mem <- "70G"
slots <- 8
time <- "00:30:00"


# Launch jobs
arg.table <- launch.jobs(arg.list = arg.list, name.stub = name.stub, name.args = name.args, outfile.suffix = ".csv",
                         pause = 1, max.attempts = 1, return = T, relaunch = T)


# Check output file status
status <- data.table()
for (loc in loc_ids) {
  print(loc)
  for (y in yearList) {
    cat(".")
    status <- rbind(status,
                    data.table(location_id = loc, year_id = y,
                               complete = file.exists(paste0(outdir, "/paf_", loc, "_", y, ".csv"))))
  }
}

table(status$complete)


status <- data.table()
for (loc in loc_ids) {
  print(loc)
  for (y in yearList) {
    cat(".")
    status <- rbind(status,
                    data.table(location_id = loc, year_id = y,
                               heat = file.exists(paste0(outdir, "/heat/", loc, "_", y, ".csv")),
                               cold = file.exists(paste0(outdir, "/cold/", loc, "_", y, ".csv")),
                               sevs = file.exists(paste0(outdir, "/sevs/raw/", loc, "_", y, ".csv"))))
  }
}

table(status$heat)
table(status$year_id, status$complete)
table(status$location_id, status$complete)

