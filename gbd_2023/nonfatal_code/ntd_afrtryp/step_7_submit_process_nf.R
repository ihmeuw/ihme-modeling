########################################################################
# Description: Generate files of nf draws by location and MEID     #
#                                                                      #
########################################################################

### ========================= BOILERPLATE ========================= ###

rm(list=ls())
data_root <- "FILEPATH"
cause <- "ntd_afrtryp"

## Define paths 
params_dir  <- "FILEPATH"
draws_dir   <- "FILEPATH"
interms_dir <- "FILEPATH"
logs_dir    <- "FILEPATH"

##	Source relevant libraries
source("FILEPATH/get_demographics.R")

## Define Constants
release_id <- ADDRESS

### ========================= MAIN EXECUTION ========================= ###

# Outcomes
# HAT incidence & prevlanece (both species)
# Skin disfigurement due to species gambiense
# Sleeping sickness due to species gambiense 
# Skin disfigurement due to species rhodesiense
# Sleeping sickness due to species rhodesiense


## (1) Create subdirectories 
meid_list <- c(ADDRESS)

for (sub_dir in meid_list){
  dir.create(file.path(draws_dir, sub_dir))
}


## (2) Define globals for reporting
demog <- get_demographics(gbd_team = ADDRESS, release_id = release_id)
locations <- demog$location_id
ages <- demog$age_group_id
years <- demog$year_id


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


## (4) Import non-fatal estimates
nfDraws <- readRDS(file = paste0(interms_dir,"FILEPATH"))

# Get a list of HAT locations
hat_locs <- unique(nfDraws$location_id)

# Get a list of non-HAT locations
non_hat_locs <- locations[! locations %in% hat_locs]


## (5) Output csv files for all non-endemic locations, all MEIDs 
for (meid in meid_list){
  nfZeros$model_id<- meid
  for (loc in non_hat_locs){
    nfZeros$location_id <- loc
    write.csv(nfZeros, file=paste0(draws_dir,"FILEPATH"))
  }
}

## (6) OUTPUT CSV FILES FOR ALL ENDEMIC LOCATIONS

## (a) HAT incidence & prevlanece (both species)
cols <- c("location_id", "year_id", "sex_id", "age_group_id", "measure_id", paste0("total_g_", 0:999), paste0("total_r_", 0:999))
me_ADDRESS1 <- nfDraws[, .SD, .SDcols=cols]
me_ADDRESS1 <- me_ADDRESS1[, paste0("draw_",0:999) := lapply(0:999, function(x)
  get(paste0("total_g_",x)) +  get(paste0("total_r_",x)) )]
cols_to_drop <- c(paste0("total_g_", 0:999), paste0("total_r_", 0:999))
me_ADDRESS1 <- me_ADDRESS1[, (cols_to_drop) := NULL]
me_ADDRESS1 <- me_ADDRESS1[, model_id:= ADDRESS1]

for (loc in hat_locs){
  write.csv(me_ADDRESS1[location_id==loc], file=paste0(draws_dir,"FILEPATH"))
}

## (b) Skin disfigurement due to species gambiense
cols <- c("location_id", "year_id", "sex_id", "age_group_id", "measure_id", paste0("disf_g_", 0:999))
me_ADDRESS2 <- nfDraws[, .SD, .SDcols=cols]
me_ADDRESS2 <- me_ADDRESS2[, model_id:= me_ADDRESS2]
setnames(me_ADDRESS2, old=paste0("disf_g_",0:999), new=paste0("draw_", 0:999))

for (loc in hat_locs){
  write.csv(me_ADDRESS2[location_id==loc], file=paste0(draws_dir,"FILEPATH"))
}

## (c) Sleeping sickness due to species gambiense
cols <- c("location_id", "year_id", "sex_id", "age_group_id", "measure_id", paste0("sleep_g_", 0:999))
me_ADDRESS3 <- nfDraws[, .SD, .SDcols=cols]
me_ADDRESS3 <- me_ADDRESS3[, model_id:= ADDRESS3]
setnames(me_ADDRESS3, old=paste0("sleep_g_",0:999), new=paste0("draw_", 0:999))

for (loc in hat_locs){
  write.csv(me_ADDRESS3[location_id==loc], file=paste0(draws_dir,"FILEPATH"))
}

## (d) Skin disfigurement due to species rhodesiense
cols <- c("location_id", "year_id", "sex_id", "age_group_id", "measure_id", paste0("disf_r_", 0:999))
me_ADDRESS4 <- nfDraws[, .SD, .SDcols=cols]
me_ADDRESS4 <- me_ADDRESS4[, model_id:= ADDRESS4]
setnames(me_ADDRESS4, old=paste0("disf_r_",0:999), new=paste0("draw_", 0:999))

for (loc in hat_locs){
  write.csv(me_ADDRESS4[location_id==loc], file=paste0(draws_dir,"FILEPATH"))
}

## (e) Sleeping sickness due to species rhodesiense
cols <- c("location_id", "year_id", "sex_id", "age_group_id", "measure_id", paste0("sleep_r_", 0:999))
me_ADDRESS5 <- nfDraws[, .SD, .SDcols=cols]
me_ADDRESS5 <- me_ADDRESS5[, model_id:= ADDRESS5]
setnames(me_ADDRESS5, old=paste0("sleep_r_",0:999), new=paste0("draw_", 0:999))

for (loc in hat_locs){
  write.csv(me_ADDRESS5[location_id==loc], file=paste0(draws_dir,"FILEPATH"))
}