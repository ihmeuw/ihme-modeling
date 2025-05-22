################################################################################################################
#     Goal: Bring in two version ids of recently run PAFs, get draws for those. Take the difference and save csv. BY LOCATION
#           Norm_version - shifted_version = final_paf
################################################################################################################



## clear memory
rm(list=ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "J:/"

# Base filepaths

log_dir <- paste0("FILEPATH", user)

## load packages

library(data.table)
library(tidyverse)
library(parallel)

source("FILEPATH/get_best_model_versions.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/save_results_risk.R")
source("FILEPATH/get_elmo_ids.R")

#`````````````````````````````````````````````````````````````````````````````````````````````````````````````


norm_version <-875625 
shifted_version <-875627 
release_id = 16
iron_paf_meid <- 8755
iron_rei_id <- 95
save_dir <- paste0("FILEPATH", norm_version, "_", shifted_version, "/") 
if(!dir.exists(save_dir)){dir.create(save_dir)}

year_id = c(1990:2024)

##

idvars <- c("rei_id", "modelable_entity_id", "location_id", "year_id", "age_group_id", "sex_id", "cause_id",
            "measure_id", "metric_id")

norm_draws <- get_draws(gbd_id_type = "rei_id", 
                        gbd_id = iron_rei_id, 
                        source = "paf", 
                        version_id = norm_version,
                        release_id = release_id)

norm_draws <- melt(norm_draws, id.vars = idvars, value.name = "norm_val", variable.name = "draw" )


shifted_draws <- get_draws(gbd_id_type = "rei_id", 
                           gbd_id = iron_rei_id, 
                           source = "paf", 
                           version_id = shifted_version,
                           release_id = release_id)
                            
shifted_draws <- melt(shifted_draws, id.vars = idvars, value.name = "shifted_val", variable.name = "draw" )


# take the difference -> norm - shifted

draws <- merge(shifted_draws, norm_draws, by = c(idvars, "draw"))
draws[, value:= norm_val - shifted_val]
draws[, `:=` (norm_val=NULL, shifted_val=NULL)]


# check if any are negative -> can't have negative PAFs
neg <- draws[value < 0,]
if(nrow(neg) > 0 ){
  
  print(neg)
  warning("Some draws are negative - this shouldn't happen!!")
  
}

# reshape wide and save
draws <- as.data.table(dcast(draws, ... ~ draw, value.var = "value"))
draws[, `:=` (modelable_entity_id = NULL, metric_id = NULL)]

write.csv(draws, paste0(save_dir, "/", "PAF_difference","_", norm_version, "_", shifted_version,".csv"), row.names = F)



  d <- save_results_risk(input_dir= save_dir,
                         paste0("PAF_difference","_", norm_version, "_", shifted_version,".csv"),
                         modelable_entity_id = iron_paf_meid, 
                         release_id = release_id,  
                         sex_id = 2,
                         n_draws = 250,
                         year_id = year_id,
                         risk_type = "paf",
                         mark_best = T,
                         description = paste0("Custom PAF: difference between PAF versions ", norm_version, " and ", shifted_version, " for direct effect of iron" )
  )
  print(d)
  write.csv(d, paste0(save_dir, "/", d$model_version_id, "_saved.csv"), row.names = F)
  

