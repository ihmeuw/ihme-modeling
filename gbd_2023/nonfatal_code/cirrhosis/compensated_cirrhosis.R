########################################################################################################################################################
#  Purpose: The purpose of this script is to calculate prevalence and incidence estimates of compensated cirrhosis
#########################################################################################################################################################


pacman::p_load(data.table, openxlsx, ggplot2)
library(gridExtra)
library(grid) 

# source central functions
source("FILEPATH/get_draws.R")

# define objects
out_dir <-OBJECT
rel_id <- OBJECT
loc_id <- OBJECT

total_cirrhosis <- OBJECT
decomp_cirrhosis <- OBJECT
age_groups <- c(2:3, 6:20, 30:32, 34, 388, 389, 238, 235)
draws <- paste0("draw_", 0:999)
date <- gsub("-", "_", Sys.Date())

##---------------------------------------------------------------------------------------------------------------------------
## get draws for incidence from total cirrhosis MEID
inc_draws <- get_draws(gbd_id_type="modelable_entity_id", 
                       gbd_id=total_cirrhosis, 
                       source="epi", 
                       location_id=loc_id, 
                       measure_id=6, 
                       age_group_id=age_groups, 
                       release_id = rel_id)

## drop columns that aren't needed
inc_draws[,grep("mod", colnames(inc_draws)) := NULL]

##---------------------------------------------------------------------------------------------------------------------------
## get draws for prevalence from total cirrhosis MEID and decompensated cirrhosis MEID, and subtract them
total_prev_draws <- get_draws(gbd_id_type="modelable_entity_id", 
                              gbd_id=total_cirrhosis, 
                              source="epi", 
                              location_id=loc_id, 
                              measure_id=5, 
                              age_group_id=age_groups, 
                              release_id = rel_id)

decomp_prev_draws <- get_draws(gbd_id_type="modelable_entity_id", 
                               gbd_id=decomp_cirrhosis, 
                               source="epi", 
                               location_id=loc_id, 
                               measure_id=5, 
                               age_group_id=age_groups, 
                               release_id = rel_id)

## drop columns that aren't needed
total_prev_draws[,grep("mod", colnames(total_prev_draws)) := NULL]
decomp_prev_draws[,grep("mod", colnames(decomp_prev_draws)) := NULL]

## calculate asymptomatic = total - symptomatic
asymp_draws <- copy(total_prev_draws)

for (draw in draws) {
  asymp_draws[,draw := get(draw) - decomp_prev_draws[,get(draw)],with=F]
  asymp_draws[[draw]][asymp_draws[[draw]]<0]=0 # sets any negative draws to zero
}

##---------------------------------------------------------------------------------------------------------------------------
#append prev and inc draws
final_draws <- rbind(inc_draws, asymp_draws)

final_draws$crosswalk_version_id <-OBJECT  
final_draws$bundle_version_id <- OBJECT

## write draws to files so save results can use them, all locations for a given cause need to go to a single, unique folder
write.csv(final_draws, file.path(out_dir, paste0(loc_id, ".csv")), row.names = F)

