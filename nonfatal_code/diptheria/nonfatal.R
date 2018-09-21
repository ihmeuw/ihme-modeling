#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: PART ONE -   Prepare negative binomial regression of diphtheria cases for input into codcorrect
#          PART TWO -   Format for CodCorrect and save results to database
#          PART THREE - Run DisMod model for CFR
#          PART FOUR -  Calculate nonfatal outcomes from mortality
#                       Use mortality to calculate prevalence (prev = mort/cfr*duration)
#                       Calculate incidence from prevalence (inc = prev/duration)
#          PART FIVE -  Format for COMO and save results to database
#***********************************************************************************************************************


#----CONFIG-------------------------------------------------------------------------------------------------------------
### load packages
pacman::p_load(magrittr, foreign, stats, MASS, data.table, rhdf5, plyr, data.table, parallel, dplyr)
if (Sys.info()["sysname"] == "Linux") {
  require(mvtnorm, lib="FILEPATH")
} else { 
  pacman::p_load(mvtnorm)
}
#***********************************************************************************************************************


#----DIRECTORIES--------------------------------------------------------------------------------------------------------
### set objects
acause <- "diptheria"
age_start <- 4
age_end <- 16
cause_id <- 338
me_id <- 1421

### make folders on cluster
cl.death.dir <- file.path("FILEPATH")                                       
dir.create(cl.death.dir, recursive = T)

cl.version.dir <- file.path("FILEPATH")                                              
dir.create(file.path(cl.version.dir), recursive = T)

### directories
home <- file.path(j_root, "FILEPATH")
j.version.dir <- file.path("FILEPATH")
j.version.dir.inputs <- file.path(j.version.dir, "model_inputs")
j.version.dir.logs <- file.path(j.version.dir, "model_logs")
dir.create(j.version.dir.inputs, recursive = T)
dir.create(j.version.dir.logs, recursive = T)

### save description of model run
write.table(description, file.path(j.version.dir, "MODEL_DESCRIPTION.txt"))
#***********************************************************************************************************************


#----FUNCTIONS----------------------------------------------------------------------------------------------------------  
### load shared functions
file.path(j_root, "FILEPATH/get_population.R") %>% source
file.path(j_root, "FILEPATH/get_location_metadata.R") %>% source
file.path(j_root, "FILEPATH/get_covariate_estimates.R") %>% source
file.path(j_root, "FILEPATH/get_envelope.R") %>% source
file.path(j_root, "FILEPATH/get_draws.R") %>% source
#*********************************************************************************************************************** 


########################################################################################################################
##### PART THREE: DisMod NONFATAL RESULTS ##############################################################################
########################################################################################################################


#----GET CFR------------------------------------------------------------------------------------------------------------
### read in results from CFR model in DisMod
cfr_dismod <- get_draws(gbd_id_field="modelable_entity_id", gbd_id=2834, status="best", gbd_round_id=4, source="epi")
cfr_dismod <- subset(cfr_dismod, select=c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", 0:999)))
colnames(cfr_dismod) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("cfr_draw_", 0:999))
#***********************************************************************************************************************


########################################################################################################################
##### PART FOUR: MORTALITY TO NONFATAL #################################################################################
########################################################################################################################


#----PREP---------------------------------------------------------------------------------------------------------------
### bring in duration data
duration <- read.csv(file.path(j_root, "Project/GBD/Causes/mort_to_prev/data/duration_draws.csv"))
duration <- duration[duration$cause==paste0("A05", ".a"), ]
duration$cause <- "A05"
colnames(duration) <- c("cause", paste0("dur_draw_", 0:999))

### prep death data
#deaths <- pred_death_save 
deaths <- get_draws('cause_id', 338, 'codcorrect', location_ids=pop_locs, year_ids=1980:2016, gbd_round_id=4, status="best")                                                               
deaths <- deaths[deaths$measure_id==1, ]
deaths <- subset(deaths, select=c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", 0:999)))
colnames(deaths) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("death_draw_", 0:999))

### bring together variables for nonfatal calculations
predict_nonfatal <- merge(cfr_dismod, deaths, by=c("location_id", "year_id", "age_group_id", "sex_id"))
predict_nonfatal <- merge(predict_nonfatal, pop_env, by=c("location_id", "year_id", "age_group_id", "sex_id"))
predict_nonfatal$cause <- "A05"
predict_nonfatal <- merge(predict_nonfatal, duration, by="cause", all.x=TRUE)
predict_nonfatal <- predict_nonfatal %>% data.frame
#*********************************************************************************************************************** 


#----CALCULATE PREVALENCE-----------------------------------------------------------------------------------------------
### calculate prevalence (mort/cfr*duration)
for (i in 0:999) {
    predict_nonfatal[, paste0("death_rate_", i)] <- ( predict_nonfatal[, paste0("death_draw_", i)] / predict_nonfatal$population )
    predict_nonfatal[, paste0("prev_draw_", i)] <- ( (predict_nonfatal[, paste0("death_rate_", i)] / predict_nonfatal[, paste0("cfr_draw_", i)]) * 
                                                    predict_nonfatal[, paste0("dur_draw_", i)] )
    predict_nonfatal[, paste0("inc_draw_", i)] <- ( predict_nonfatal[, paste0("prev_draw_", i)] / predict_nonfatal[, paste0("dur_draw_", i)])
}

predictions_prev_save <- subset(predict_nonfatal, select=c("location_id", "year_id", "age_group_id", "sex_id",
                                     colnames(predict_nonfatal)[grep("prev_draw_", colnames(predict_nonfatal))]))
predictions_inc_save <- subset(predict_nonfatal, select=c("location_id", "year_id", "age_group_id", "sex_id",
                                     colnames(predict_nonfatal)[grep("inc_draw_", colnames(predict_nonfatal))]))


if (WRITE_FILES == "yes") {
	write.csv(predictions_prev_save, file.path(j.version.dir, "02_prevalence_draws.csv"), row.names=FALSE)
	write.csv(predictions_inc_save, file.path(j.version.dir, "03_incidence_draws.csv"), row.names=FALSE)
}
#***********************************************************************************************************************


########################################################################################################################
##### PART FIVE: FORMAT FOR COMO #######################################################################################
########################################################################################################################


#----SAVE RESULTS-------------------------------------------------------------------------------------------------------
### format prevalence for como
# prevalence, measure_id==5
colnames(predictions_prev_save) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", 0:999))
predictions_prev_save$measure_id <- 5

### format incidence for como
# incidence, measure_id==6
colnames(predictions_inc_save) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", 0:999))
predictions_inc_save$measure_id <- 6

### write nonfatal outcomes to cluster
save_nonfatal <- rbind(predictions_prev_save, predictions_inc_save)
lapply(unique(save_nonfatal$location_id), function(x) write.csv(save_nonfatal[save_nonfatal$location_id==x, ],
                            file.path(cl.version.dir, paste0(x, ".csv")), row.names=FALSE))
print(paste0("nonfatal estimates saved in ", cl.version.dir))
#***********************************************************************************************************************