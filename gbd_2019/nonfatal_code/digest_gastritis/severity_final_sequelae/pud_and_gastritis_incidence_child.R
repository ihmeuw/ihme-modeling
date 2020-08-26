###24 July 2018
###Child script for getting total incidence draws and storing them in a format that can be uploaded to custom sequela

#source("FILEPATH/pud_and_gastritis_incidence_child.R")

# set up environment
rm(list=ls())

os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- "FILEPATH_LIB"
  j<- "FILEPATH_J"
  h<-"FILEPATH_H"
} else {
  lib_path <- "FILEPATH_LIB"
  j<- "FILEPATH_J"
  h<-"FILEPATH_H"
}

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

asymp_pud_no_anemia <- 16219
asymp_gastritis_no_anemia <- 16235

#Step 4 bests ( RE submission)
asymp_pud_version <- 479021
asymp_gastritis_version <- 479087 

## objects for getting draws
age_groups <- c(2:20, 30:32, 235)

## get draws for total incidence
total_draws <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=get(paste0(cause,"_total")), source="epi", location_id=location, measure_id=6, age_group_id=age_groups, decomp_step="step4")
copy_total_draws <- copy(total_draws)
copy_total_draws <- copy_total_draws[ , modelable_entity_id := get(paste0("asymp_", cause, "_no_anemia"))]
copy_total_draws <- copy_total_draws[ , model_version_id := get(paste0("asymp_", cause, "_version"))]

## put with draws for asymptomatic, no anemia sequelae prevalence (so they will be saved in same model version)
sequela_draws <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=get(paste0("asymp_", cause, "_no_anemia")), source="epi", location_id=location, measure_id=5, age_group_id=age_groups, decomp_step="step4")

final_draws <- rbind(copy_total_draws, sequela_draws)

## write draws to files so save results can use them, all locations for a given cause need to go to a single, unique folder
write.csv(final_draws, file.path(total_incidence_files, paste0(location, ".csv")), row.names = F)

