###########################################################################
# Description: Prepare location-specific csv files for each MEID          #
#                                                                         #
#                                                                         #
###########################################################################

### ======================= BOILERPLATE ======================= ###

rm(list=ls())
code_root <-"FILEPATH"
data_root <- "FILEPATH"
cause <- "ntd_guinea"

## Define paths 
# Toggle btwn production arg parsing vs interactive development
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
  library(argparse)
  print(commandArgs())
  parser <- ArgumentParser()
  parser$add_argument("--params_dir", type = "character")
  parser$add_argument("--draws_dir", type = "character")
  parser$add_argument("--interms_dir", type = "character")
  parser$add_argument("--logs_dir", type = "character")
  args <- parser$parse_args()
  print(args)
  list2env(args, environment()); rm(args)
  sessionInfo()
} else {
    params_dir  <- "FILEPATH"
    draws_dir   <- "FILEPATH"
    interms_dir <- "FILEPATH"
    logs_dir    <- "FILEPATH"
}

##	Source relevant libraries
library(data.table)
source("FILEPATH/get_demographics.R")

## Define Constants
gbd_round_id <- "ADDRESS"
study_dems <- get_demographics("ADDRESS", release_id = release_id)

### ======================= MAIN EXECUTION ======================= ###

# MEIDS
# "ADDRESS1" - GW incidence
# "ADDRESS2" - Moderate pain and limited mobility due to guinea worm
# "ADDRESS3" - Guinea worm pain due to worm emergence (mild)

## (1) Create subdirectories 
meid_list <- c("ADDRESS1","ADDRESS2","ADDRESS3")

for (sub_dir in meid_list){
  dir.create(file.path(draws_dir, sub_dir))
}


## (2) Define globals for reporting
locations <- study_dems$location_id
ages <- study_dems$age_group_id
years <- study_dems$year_id


## (3) Create zero file template
age_group_id <- ages
year_id <- years
sex_id <- c(1,2)
measure_id <- c(5,6)

nfZeros <- tidyr::crossing(age_group_id, year_id, sex_id, measure_id)
nfZeros <- as.data.table(nfZeros)
nfZeros <- nfZeros[order(measure_id, sex_id, age_group_id, year_id),]
nfZeros[, `:=` (location_id = 0, model_id= 0)]
nfZeros[, paste0("draw_",0:999) := 0]
# Put cols in order: location_id, year_id, sex_id, age_group_id, measure_id, model_id, draw_*
setcolorder(nfZeros, neworder = c("location_id", "year_id", "sex_id", "age_group_id",
                                  "measure_id", "model_id", paste0("draw_",0:999)))


## (4) Import incidence file (MEID "ADDRESS1")
"ADDRESS1" <- fread(file = paste0("FILEPATH/ADDRESS1","_adj.csv"))

# Subset to reporting years
"ADDRESS1" <- "ADDRESS1"[year_id %in% years]

# Get a list of GW locations
gw_locs <- unique("ADDRESS1"$location_id)

# Get a list of non-GW locations
non_gw_locs <- locations[! locations %in% gw_locs]

# Put cols in order:location_id, year_id, sex_id, age_group_id, measure_id, model_id, draw_*
setcolorder("ADDRESS1", neworder = c("location_id", "year_id", "sex_id", "age_group_id",
                                  "measure_id", "model_id", paste0("draw_",0:999)))

# Sort
"ADDRESS1" <- "ADDRESS1"[order(location_id, measure_id, sex_id, age_group_id, year_id),]


## (5) Output csv files for all non-endemic locations, all MEIDs 
for (meid in meid_list){
  nfZeros$model_id<- meid
  for (loc in non_gw_locs){
    nfZeros$location_id <- loc
    write.csv(nfZeros, file=paste0("FILEPATH/"))
  }
}


## (6) OUTPUT CSV FILES FOR ALL ENDEMIC LOCATIONS

## (a) MEID ADDRESS1 - GW incidence
for (loc in gw_locs){
  write.csv("ADDRESS1"[location_id==loc], file=paste0("FILEPATH/ADDRESS1/",loc,".csv"))
}


## (b) MEID "ADDRESS2" - Moderate pain and limited mobility due to guinea worm

meid_"ADDRESS2" <- "ADDRESS1"
meid_"ADDRESS2"$model_id<- "ADDRESS2"

# divide prevalence by 12 - check this is working as expected
meid_"ADDRESS2" <- meid_"ADDRESS2"[measure_id==5, paste0("draw_",0:999) := lapply(0:999, function(x) 
  get(paste0("draw_",x)) * (1/12) )]

# output csv files
for (loc in gw_locs){
  write.csv(meid_"ADDRESS2"[location_id==loc], file=paste0("FILEPATH/ADDRESS2/",loc,".csv"))
}


## (c) MEID "ADDRESS3" - Guinea worm pain due to worm emergence (mild)
meid_"ADDRESS3" <- "ADDRESS1"
meid_"ADDRESS3"$model_id<- "ADDRESS3"

meid_"ADDRESS3" <- meid_"ADDRESS3"[measure_id==5, paste0("draw_",0:999) := lapply(0:999, function(x) 
  get(paste0("draw_",x)) * ((2/12)+(0.3*(9/12))) )]

# output csv files
for (loc in gw_locs){
  write.csv(meid_"ADDRESS3"[location_id==loc], file=paste0("FILEPATH/ADDRESS3/",loc,".csv"))
}