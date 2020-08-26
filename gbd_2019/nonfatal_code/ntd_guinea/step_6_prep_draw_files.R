###########################################################################
# Description: Prepare location-specific csv files for each MEID          #
#                                                                         #
#                                                                         #
###########################################################################

### ======================= BOILERPLATE ======================= ###

rm(list=ls())
code_root <- FILEPATH
data_root <- FILEPATH
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
  params_dir  <- FILEPATH
  draws_dir   <- FILEPATH
  interms_dir <- FILEPATH
  logs_dir    <- FILEPATH
}

##	Source relevant libraries
source(FILEPATH)

## Define Constants
gbd_round_id <- 7
study_dems <- get_demographics("epi", gbd_round_id=gbd_round_id)

### ======================= MAIN EXECUTION ======================= ###

# MEIDS
# ADDRESS - GW incidence
# ADDRESS - Moderate pain and limited mobility due to guinea worm
# ADDRESS - Guinea worm pain due to worm emergence (mild)

## (1) Create subdirectories 
meid_list <- c(ADDRESS, ADDRESS, ADDRESS)

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
nfZeros[, `:=` (location_id = 0, modelable_entity_id = 0)]
nfZeros[, paste0("draw_",0:999) := 0]
# Put cols in order: location_id, year_id, sex_id, age_group_id, measure_id, modelable_entity_id, draw_*
setcolorder(nfZeros, neworder = c("location_id", "year_id", "sex_id", "age_group_id",
                                  "measure_id", "modelable_entity_id", paste0("draw_",0:999)))


## (4) Import incidence file (MEID ADDRESS)
meid_ADDRESS <- fread(file = paste0(interms_dir,FILEPATH))

# Subset to reporting years
meid_ADDRESS <- meid_ADDRESS[year_id %in% years]

# Get a list of GW locations
gw_locs <- unique(meid_ADDRESS$location_id)

# Get a list of non-GW locations
non_gw_locs <- locations[! locations %in% gw_locs]

# Put cols in order
setcolorder(meid_ADDRESS, neworder = c("location_id", "year_id", "sex_id", 
                                       "age_group_id", "measure_id",
                                       "modelable_entity_id", 
                                       paste0("draw_",0:999)))

# Sort
meid_ADDRESS <- meid_ADDRESS[order(location_id, measure_id, sex_id, 
                                   age_group_id, year_id),]


## (5) Output csv files for all non-endemic locations, all MEIDs 
for (meid in meid_list){
    nfZeros$modelable_entity_id <- meid
    for (loc in non_gw_locs){
        nfZeros$location_id <- loc
        write.csv(nfZeros, file=paste0(draws_dir,"/",meid,"/",loc,".csv"))
    }
}


## (6) OUTPUT CSV FILES FOR ALL ENDEMIC LOCATIONS

## (a) MEID ADDRESS - GW incidence
for (loc in gw_locs){
    write.csv(meid_ADDRESS[location_id==loc], file=paste0(draws_dir,FILEPATH,loc,".csv"))
}

## (b) MEID ADDRESS - Moderate pain and limited mobility due to guinea worm
meid_ADDRESS <- meid_ADDRESS
meid_ADDRESS$modelable_entity_id <- ADDRESS

# divide prevalence by 12 (months)
meid_ADDRESS <- meid_ADDRESS[measure_id==5, paste0("draw_",0:999) := lapply(0:999, function(x) 
  get(paste0("draw_",x)) * (1/12) )]

# output csv files
for (loc in gw_locs){
    write.csv(meid_ADDRESS[location_id==loc], file=paste0(draws_dir,FILEPATH,loc,".csv"))
}

## (c) MEID ADDRESS - Guinea worm pain due to worm emergence (mild)
meid_ADDRESS <- meid_ADDRESS
meid_ADDRESS$modelable_entity_id <- ADDRESS

meid_ADDRESS <- meid_ADDRESS[measure_id==5, paste0("draw_",0:999) := lapply(0:999, function(x) 
  get(paste0("draw_",x)) * ((2/12)+(0.3*(9/12))) )]

# output csv files
for (loc in gw_locs){
  write.csv(meid_ADDRESS[location_id==loc], file=paste0(draws_dir,FILEPATH,loc,".csv"))
}
