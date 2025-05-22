###Child script for getting total incidence draws and storing them in a format that can be uploaded to custom sequela
###Last updated to accommodate the changes in the MEs from which incidence and prevalence draws are pulled from

#source("FILEPATH")

# set up environment
rm(list=ls())

os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- "FILEPATH"
  j<- "FILEPATH"
  h<-"FILEPATH"
} else  {
  lib_path <- "FILEPATH"
  j<- "FILEPATH"
  h<-"FILEPATH"
}

shell <- "FILEPATH" 

# source central functions
source("FILEPATH/get_draws.R")

# define objects

## objects for causes, locations, directories to send draws to
cause<-commandArgs()[3]
location<-commandArgs()[4]
total_incidence_files<-commandArgs()[5]

## list of ME defs 
pud_total <- 24675
gastritis_total <- 24676

#asymp_pud_no_anemia <- 16219
#asymp_gastritis_no_anemia <- 16235

## (Different MEs from which prevalence draws are pulled from)-----------------------------------------
asymp_pud_no_anemia <- 9314
asymp_gastritis_no_anemia <- 9528

#GBD 2020 iterative best
asymp_pud_version <- 513089 #meid 9314
asymp_gastritis_version <-  513104 #meid 9526
##---------------------------------------------------------------------------------------------------------------------------

## objects for getting draws
age_groups <- c(2:3, 6:20, 30:32, 34, 388, 389, 238, 235)

## get draws for total incidence
total_draws <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=get(paste0(cause,"_total")), source="epi", location_id=location, measure_id=6, age_group_id=age_groups, decomp_step="iterative", gbd_round_id=7)
copy_total_draws <- copy(total_draws)
copy_total_draws <- copy_total_draws[ , modelable_entity_id := get(paste0("asymp_", cause, "_no_anemia"))]
copy_total_draws <- copy_total_draws[ , model_version_id := get(paste0("asymp_", cause, "_version"))]

## to create seqela_draw datasets with missing age groups (2, 3), these have draw values of 0
dummy <- subset(copy_total_draws, age_group_id==2 | age_group_id==3)
dummy$measure_id <- 5
dummy$model_version_id <- 513104
draws <- paste0("draw_", 0:999)
dummy[, (draws) := 0,
       by = c("location_id", "year_id", "age_group_id", "sex_id")]


## put with draws for asymptomatic, no anemia sequelae prevalence (so they will be saved in same model version)
sequela_draws <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=get(paste0("asymp_", cause, "_no_anemia")), source="epi", location_id=location, measure_id=5, age_group_id=age_groups, decomp_step="iterative", gbd_round_id = 7,version_id =get(paste0("asymp_", cause, "_version")))


final_draws <- rbind(copy_total_draws, sequela_draws, dummy)

## these are required for save_results_epi function
final_draws$crosswalk_version_id <-20189
final_draws$bundle_version_id <- 22916 #pud 22913 gastritis 22916

## write draws to files so save results can use them, all locations for a given cause need to go to a single, unique folder
write.csv(final_draws, file.path(total_incidence_files, paste0(location, ".csv")), row.names = F)
