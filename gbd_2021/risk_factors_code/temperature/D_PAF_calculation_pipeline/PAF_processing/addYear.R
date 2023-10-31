## LOAD DEPENDENCIES -----------------------------------------------------
library(data.table)
library(argparse)
library("parallel")


## IMPORT ARGUMENTS ------------------------------------------------------
parser <- ArgumentParser()
parser$add_argument("--paf_version", help = "string giving paf version",
                    default = '15.0', type = "character")
parser$add_argument("--outDir", help = "directory to pull data from and write it to",
                    default = "/FILEPATH/", type = "character")
parser$add_argument("--inyear", help = "year to copy over onto post-estimation years",
                    default = 2021, type = "integer")
args <- parser$parse_args()
list2env(args, environment()); rm(args)


## DEFINE FUNCTIONS ------------------------------------------------------
add.years <- function(infile, inyear, outyears) {
  tmp <- fread(infile)
  for (outyear in outyears) {
    write.csv(tmp[, year_id := outyear], gsub(paste0("_", inyear, ".csv"), paste0("_", outyear, ".csv"), infile))
  }
}

## BODY ------------------------------------------------------------------
for (risk in c("heat", "cold", "sevs")) {
  if (risk == "sevs") {
    input_dir <- paste0(outDir, "version_", paf_version, "/", risk, "/raw")
  } else {
    input_dir <- paste0(outDir, "version_", paf_version, "/", risk)
  }
  files <- list.files(path = input_dir, pattern = sprintf("_%d.csv", inyear), full.names = T)
  
  mclapply(files, function(file) {add.years(file, inyear = inyear, outyears = (inyear+1):(inyear+2))})
}
