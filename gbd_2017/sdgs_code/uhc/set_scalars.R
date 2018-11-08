##########################################################
# Description: Prepare PAF inputs
##########################################################
## DEFINE ROOT AND LIBRARIES
rm(list=ls())
library(argparse)
library(data.table)
library(plyr)
if (Sys.info()[1] == "Linux"){
  j <- "/home/j/"
  h <- paste0("/homes/", Sys.info()[7])
}else if (Sys.info()[1] == "Windows"){
  j <- "J:/"
  h <- "H:/"
}else if (Sys.info()[1] == "Darwin"){
  j <- "/Volumes/snfs/"
  h <- paste0("/Volumes/", Sys.info()[6], "/")
}

##########################################################
## PARSE ARGUMENTS
parser <- ArgumentParser()

parser$add_argument("--prog_dir", help="Program directory",
                    default=paste0(h, "FILEPATH"), type="character")
parser$add_argument("--data_dir", help="Space where results are to be saved",
                    default="FILEPATH", type="character")
parser$add_argument("--lsid", help="Location set",
                    default=35, type="integer")
parser$add_argument("--lid", help="Location id",
                    default=536, type="integer")
parser$add_argument("--yid", help="Year for current job",
                    default=1990, nargs="+", type="integer")
parser$add_argument("--mid", help="Measure",
                    default=1, type="integer")
parser$add_argument("--scale_ceiling", help="Upper limit of PAFs (out of 100)",
                    default=90, type="integer")
args <- parser$parse_args()
list2env(args, .GlobalEnv)
rm(args)

##########################################################
## DEFINE FUNCTIONS
source(paste0(prog_dir, "/utilities.R"))
yids <- c(1990:2017)
##########################################################
## RUN PROGRAM
load(paste0(data_dir, "FILEPATH")) #location file
args <- expand.grid(year_id = yids, location_id = locsdf$location_id[locsdf$most_detailed == 1])
scalardf <- rbindlist(mapply(function(data_dir, yid, lid, mid) {
                                                      print(paste0(lid, ": ", yid))
                                                      load(paste0(data_dir, "FILEPATH"))
                                                      return(inputdf)
                                                    },
                      yid = as.list(args$year_id),
                      lid = as.list(args$location_id),
                      MoreArgs = list(data_dir = data_dir, mid = mid),
                      SIMPLIFY = FALSE))

scalardf <- scalardf[, .(maxpaf = max(paf)), by = .(age_group_id, sex_id, measure_id, cause_id)]
scalardf <- scalardf[, pafscalar := (scale_ceiling / 100) / maxpaf][pafscalar > 1, pafscalar := 1][, maxpaf := NULL]

save(scalardf,
     file = paste0(data_dir, "FILEPATH"))

##########################################################
## END SESSION
quit("no")

##########################################################