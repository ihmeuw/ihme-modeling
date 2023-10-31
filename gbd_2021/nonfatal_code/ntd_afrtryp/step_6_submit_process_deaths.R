########################################################################
# Description: Generate csv files of mortality draws by location       #
#                                                                      #
########################################################################

### ========================= BOILERPLATE ========================= ###

rm(list=ls())
data_root <- "FILEPATH"
cause <- "ntd_afrtryp"

# Define paths 
params_dir  <- "FILEPATH"
draws_dir   <- "FILEPATH"
interms_dir <- "FILEPATH"
logs_dir    <- "FILEPATH"

##	Source relevant libraries
source("FILEPATH/get_demographics.R")

## Define Constants
gbd_round_id <- ADDRESS

### ========================= MAIN EXECUTION ========================= ###

## (1) Create deaths subdirectory 
dir.create(paste0(draws_dir, 'FILEPATH'))


## (2) Define globals for reporting
demog <- get_demographics(gbd_team = ADDRESS, gbd_round_id = gbd_round_id)
locations <- demog$location_id
ages <- demog$age_group_id
years <- demog$year_id


## (3) Create zero file template
age_group_id <- ages
year_id <- years
sex_id <- c(1,2)

zeros <- tidyr::crossing(age_group_id, sex_id, year_id)
zeros <- as.data.table(zeros)
zeros[, `:=` (location_id = 0, cause_id = ADDRESS, measure_id = 1)]
zeros[, paste0("draw_",0:999) := 0]
# Put cols in order: location_id, year_id, sex_id, age_group_id, cause_id, measure_id, draw_*
setcolorder(zeros, neworder = c("location_id", "year_id", "sex_id", "age_group_id", "cause_id",
                                "measure_id", paste0("draw_",0:999)))


## (4) Import mortality draws
mort <- readRDS(file = paste0(interms_dir,"FILEPATH"))

# Subset var list
mort <- mort[, c("location_id", "year_id", "sex_id", "age_group_id", paste0("deaths_",0:999))]
setnames(mort, old=paste0("deaths_",0:999), new=paste0("draw_", 0:999))

# Add cause_id and measure_id variables
mort[, `:=` (cause_id = ADDRESS, measure_id = 1)]

# Put cols in order: location_id, year_id, sex_id, age_group_id, cause_id, draw_*
setcolorder(mort, neworder = c("location_id", "year_id", "sex_id", "age_group_id", "cause_id",
                                "measure_id", paste0("draw_",0:999)))

# Get a list of HAT locations
hat_locs <- unique(mort$location_id)

# Get a list of non-HAT locations
non_hat_locs <- locations[! locations %in% hat_locs]


## (5) Output zero files for all non-HAT locations 
for (loc in non_hat_locs){
  zeros$location_id <- loc
  write.csv(zeros, file=paste0(draws_dir,"/deaths/",loc,".csv"))
}


## (6) Output csv files for all locations with HAT
for (loc in hat_locs){
  write.csv(mort[location_id==loc], file=paste0(draws_dir,"/deaths/",loc,".csv"))
}
