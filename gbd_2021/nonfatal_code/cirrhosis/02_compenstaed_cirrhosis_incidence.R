################################################################################################
## Description: Append prevalence of compensated cirrhosis and incidence of total cirrhosis
## for a given location, age, sex, year.
## Output:  Compensated cirrhosis draws  for prevalence and incidence
################################################################################################

# set up environment
rm(list=ls())


if (Sys.info()["sysname"] == "Linux") {
  j <- "/home/j/"
  h <- "~/"
  l <- "/ihme/limited_use/"
} else {
  j <- "J:/"
  h <- "H:/"
  l <- "L:/"
}

pacman::p_load(data.table, openxlsx, ggplot2)
library(gridExtra)
library(grid) 
date <- gsub("-", "_", Sys.Date())
draws <- paste0("draw_", 0:999)

#SOURCE FUNCTIONS 
shared_functions <- FILEPATH

## objects 
cause<-commandArgs()[3]
location<-commandArgs()[4]
total_incidence_files<-commandArgs()[5]
total_cirrhosis <- OBJECT
comp_cirrhosis <- OBJECT
comp_cirrhosis_version <-  OBJECT 
gbd_round_id <- OBJECT
decomp_step <- OBJECT

##---------------------------------------------------------------------------------------------------------------------------

## objects for getting draws
age_groups <- c(2:3, 6:20, 30:32, 34, 388, 389, 238, 235)

## get draws for total incidence
prev_draws <- get_draws(gbd_id_type="modelable_entity_id", 
                        gbd_id=OBJECT, 
                        source="epi", 
                        location_id=location,
                        measure_id=5, 
                        age_group_id=age_groups, 
                        decomp_step=decomp_step, 
                        gbd_round_id=gbd_round_id)
copy_prev_draws <- copy(prev_draws)
copy_prev_draws <- copy_prev_draws[ , modelable_entity_id := get(paste0("comp_", cause))]
copy_prev_draws <- copy_prev_draws[ , model_version_id := get(paste0("comp_", cause, "_version"))]


## put with draws for asymptomatic, no anemia sequelae prevalence (so they will be saved in same model version)
inc_draws <- get_draws(gbd_id_type="modelable_entity_id", 
                       gbd_id=get(paste0("comp_", cause)),
                       source="epi", 
                       location_id=location, 
                       measure_id=6, 
                       age_group_id=age_groups, 
                       decomp_step=decomp_step, 
                       gbd_round_id=gbd_round_id,
                       version_id =get(paste0("comp_", cause, "_version")))

#append prev and inc draws
final_draws <- rbind(copy_prev_draws, inc_draws)

final_draws$crosswalk_version_id <-OBJECT
final_draws$bundle_version_id <- OBJECT 

## write draws to files so save results can use them, all locations for a given cause need to go to a single, unique folder
write.csv(final_draws, file.path(FILEPATH), row.names = F)
