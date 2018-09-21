#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: PART ONE -   Run CODEm model for fatal outcomes
#          PART TWO -   Run DisMod model for case fatality rate
#          PART THREE - Calculate deaths, prevalence, and incidence from CFR, death rate, and duration
#          PART FOUR -  Format for COMO and save results to database
#          PART FIVE -  Prep motor impairment models
#		       PART SIX -   Upload impairment data through Epi Uploader, run DisMod model
#***********************************************************************************************************************


#----CONFIG-------------------------------------------------------------------------------------------------------------
### load packages
pacman::p_load(magrittr, foreign, stats, MASS, data.table, dplyr, plyr, lme4, reshape2, parallel)
if (Sys.info()["sysname"] == "Linux") {
  library(rhdf5, lib="FILEPATH")
} else { 
  pacman::p_load(rhdf5)
}
#***********************************************************************************************************************


#----DIRECTORIES--------------------------------------------------------------------------------------------------------
### set objects
acause <- "tetanus"
cause_id <- 340
cause <- "A07"

### make folders on cluster
cl.version.dir <- file.path("FILEPATH")                                 
dir.create(file.path(cl.version.dir), recursive = T)

### directories
home <- file.path(j_root, "FILEPATH")
j.version.dir <- file.path(home, "models", custom_version)
dir.create(j.version.dir, recursive = T)

### save description of model run
write.table(description, file.path(j.version.dir, "MODEL_DESCRIPTION.txt"))
#***********************************************************************************************************************


#----FUNCTIONS----------------------------------------------------------------------------------------------------------  
### reading .h5 files
read_block_names <- function(str){
  # Use string splitting to get the column names of a value block
  split_vec <- strsplit(str, "'")[[1]]
  N <- length(split_vec)
  split_vec[seq(from=2, to=N, by=2)]
}
value_blocks <- function(attrib){
  # get all the value block names from teh attributes of an hdf5 table
  grep(glob2rx("values_block_*_kind"), names(attrib), value=TRUE)
}
h5_value_col_names <- function(h5File, key){
  # return a list where each element are the column names of the differnt
  # value block types i.e. float, int, string
  attrib <- h5readAttributes(h5File, paste0(key, "/table/"))
  value_block_names <- value_blocks(attrib)
  nam <- lapply(value_block_names, function(x) read_block_names(attrib[[x]]))
  names(nam) <- gsub("_kind", "", value_block_names)
  nam
}
mat_to_df <- function(mat, mat_names){
  # pytables hdf5 saves values as matrices when more than one column exists 
  # We need to transpose it then apply the names
  if (length(dim(mat)) == 1){
    mat <- matrix(data=mat, nrow=length(mat), ncol=length(mat_names))
  }
  else{
    mat <- t(mat)
  }
  df <- as.data.frame(mat)
  names(df) <- mat_names
  df
}
read_hdf5_table <- function(h5File, key){
  # read in the indices  values as well as the value blocks
  # only indices values may be strings
  data_list <- h5read(h5File, paste0(key, "/table/"), compoundAsDataFrame=F)
  data_value_names <- h5_value_col_names(h5File, key)
  indices <- setdiff(names(data_list), c("index", names(data_value_names)))
  df_index <- data.frame(data_list[indices])
  df_values <- lapply(names(data_value_names), function(x) 
    mat_to_df(data_list[[x]], data_value_names[[x]]))
  do.call(cbind, c(list(df_index), df_values))
}
                      
### load shared functions
file.path(j_root, "FILEPATH/get_location_metadata.R") %>% source
file.path(j_root, "FILEPATH/get_population.R") %>% source
file.path(j_root, "FILEPATH/get_draws.R") %>% source
#*********************************************************************************************************************** 


########################################################################################################################
##### PART ONE: CODEm MODEL FOR FATAL ##################################################################################
########################################################################################################################


#----PULL---------------------------------------------------------------------------------------------------------------
### get location data
locations <- get_location_metadata(gbd_round_id=4, location_set_id=22) ## id=22 is from covariates team, id=9 is from epi
locations <- subset(locations, select=c("location_id", "ihme_loc_id", "location_name", "region_id", "super_region_id", "level", 
                                        "location_type", "parent_id", "super_region_name"))
pop_locs <- unique(locations$location_id)

### pull CodCorrect results
cod_draws <- get_draws('cause_id', 340, 'codcorrect', location_ids=pop_locs, year_ids=1980:2016, gbd_round_id=4)
cod_draws <- cod_draws[cod_draws$measure_id==1, ]

### get population data
population <- get_population(location_id=pop_locs, year_id=1980:2016, age_group_id=-1, sex_id=1:2)

# merge on population
cod_draws <- merge(cod_draws, population, by=c("location_id", "year_id", "age_group_id", "sex_id"), all.x=TRUE)
cod_draws <- subset(cod_draws, select=c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", 0:999), "population"))
colnames(cod_draws) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("death_draw_", 0:999), "pop")
#***********************************************************************************************************************


########################################################################################################################
##### PART TWO: DisMod MODEL FOR CFR ###################################################################################
########################################################################################################################


#----PULL---------------------------------------------------------------------------------------------------------------
#get CFR draws from DisMod model -- (proportion model measure_id=18)
needed_locs <- unique(cod_draws$location_id)
cfr_draws <- get_draws(gbd_id_field="modelable_entity_id", gbd_id=2833, source="epi", location_ids=needed_locs, sex_ids=1:2, status="best")
cfr_draws <- subset(cfr_draws, select=c("location_id", "year_id", "age_group_id", "sex_id", colnames(cfr_draws)[grep("draw", colnames(cfr_draws))]))
cfr_draws <- cfr_draws[cfr_draws$age_group_id %in% c(2:20, 30:32, 235), ]
colnames(cfr_draws) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("cfr_draw_", 0:999))
#***********************************************************************************************************************


########################################################################################################################
##### PART THREE: CALCULATE DEATHS, PREVALENCE, INCIDENCE ##############################################################
########################################################################################################################


#----PREP---------------------------------------------------------------------------------------------------------------
### pull in duration data
duration <- read.csv(file.path(j_root, "FILEPATH"))
duration <- duration[duration$cause==paste0(cause, ".a"), ]
duration <- duration[, colnames(duration) != "cause"]
colnames(duration) <- paste0("dur_draw_", 0:999)

# merge codcorrect death draws with population, cfr draws, and duration draws
death_input <- merge(cfr_draws, cod_draws, by=c("location_id", "year_id", "age_group_id", "sex_id")) %>% data.frame
death_input[, paste0("dur_draw_", 0:999)] <- duration[, paste0("dur_draw_", 0:999)]
#***********************************************************************************************************************


#----CALCULATE----------------------------------------------------------------------------------------------------------
### calculate death rate as deaths / population
### calculate prevalence as death rate / CFR * duration
### calculate incidence as death rate / CFR
for (ii in 0:999) {
    death_input[, paste0("drate_draw_", ii)] <- death_input[, paste0("death_draw_", ii)] / death_input$pop #ulation
    death_input[, paste0("prev_draw_", ii)] <- ( death_input[, paste0("drate_draw_", ii)] / death_input[, paste0("cfr_draw_", ii)] ) *
                                               death_input[, paste0("dur_draw_", ii)]
    death_input[, paste0("inc_draw_", ii)] <- death_input[, paste0("drate_draw_", ii)] / death_input[, paste0("cfr_draw_", ii)]
    death_input[, paste0("case_draw_", ii)] <- death_input[, paste0("inc_draw_", ii)] * death_input$pop
}
#***********************************************************************************************************************


# ----SAVE--------------------------------------------------------------------------------------------------------------
# prevalence
predictions_prev_save <- subset(death_input, select=c("location_id", "year_id", "age_group_id", "sex_id", 
                                                     colnames(death_input)[grep("prev_draw_", colnames(death_input))]))

# incidence
predictions_inc_save <- subset(death_input, select=c("location_id", "year_id", "age_group_id", "sex_id", 
                                                     colnames(death_input)[grep("inc_draw_", colnames(death_input))]))

# cases
predictions_case_save <- subset(death_input, select=c("location_id", "year_id", "age_group_id", "sex_id", 
                                                     colnames(death_input)[grep("case_draw_", colnames(death_input))]))
# write files to j folder
if (WRITE_FILES == "yes") {
	write.csv(predictions_prev_save, file.path(j.version.dir, "01_prev_draws.csv"))
	#collapse_point(acause=acause, input_file=predictions_prev_save, number="02", name="prev", j.direct=j.version.dir)
	write.csv(predictions_inc_save, file.path(j.version.dir, "03_inc_draws.csv"))
	#collapse_point(acause=acause, input_file=predictions_inc_save, number="04", name="inc", j.direct=j.version.dir)
	write.csv(predictions_case_save, file.path(j.version.dir, "05_case_draws.csv"))
	#collapse_point(acause=acause, input_file=predictions_case_save, number="06", name="case", j.direct=j.version.dir)
}
#***********************************************************************************************************************


if (UPLOAD_NONTATAL == "yes") {

########################################################################################################################
##### PART FOUR: FORMAT FOR COMO #######################################################################################
########################################################################################################################


#----SAVE RESULTS-------------------------------------------------------------------------------------------------------
### format prevalence for como, measure_id==5
colnames(predictions_prev_save) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", 0:999))
predictions_prev_save$measure_id <- 5

### format incidence for como, measure_id==6
colnames(predictions_inc_save) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", 0:999))
predictions_inc_save$measure_id <- 6

predictions <- rbind(predictions_prev_save, predictions_inc_save)
predictions <- predictions %>% data.table

### save nonfatal estimates to cluster
lapply(unique(predictions$location_id), function(x) write.csv(predictions[location_id==x],
                  file.path(cl.version.dir, paste0(x, ".csv")), row.names=FALSE))
print(paste0("Nonfatal results saved in ", cl.version.dir))
#***********************************************************************************************************************

}


if (CALCULATE_IMPAIRMENTS == "yes") {

########################################################################################################################
##### PART FIVE: PREP MOTOR IMPAIRMENT MODELS ##########################################################################
########################################################################################################################


#----PREP---------------------------------------------------------------------------------------------------------------
inc_draws <- death_input[death_input$age_group_id <= 3, ]
#***********************************************************************************************************************  


#----MOTOR IMPAIRMENT---------------------------------------------------------------------------------------------------
for (ii in 0:999) {
    inc_draws[, paste0("survival_draw_", ii)] <- inc_draws[, paste0("inc_draw_", ii)] * ( 1 - inc_draws[, paste0("cfr_draw_", ii)] )
    inc_draws[, paste0("mild_draw_", ii)] <- inc_draws[, paste0("survival_draw_", ii)] * 0.11
    inc_draws[, paste0("modsev_draw_", ii)] <- inc_draws[, paste0("survival_draw_", ii)] * 0.07
}

mild_impairment <- subset(inc_draws, select=c("location_id", "year_id", "age_group_id", "sex_id",
                                                     colnames(inc_draws)[grep("mild_draw_", colnames(inc_draws))]))
mod_sev_impairment <- subset(inc_draws, select=c("location_id", "year_id", "age_group_id", "sex_id",
                                                     colnames(inc_draws)[grep("modsev_draw_", colnames(inc_draws))]))
#***********************************************************************************************************************


########################################################################################################################
##### PART SIX: UPLOAD IMPAIRMENTS #####################################################################################
########################################################################################################################


#----PREP---------------------------------------------------------------------------------------------------------------
locations <- get_location_metadata(gbd_round_id=4, location_set_id=22)
locations <- subset(locations, select=c("location_id", "ihme_loc_id", "location_name", "region_id", "super_region_id", 
										"level", "location_type", "parent_id"))
#***********************************************************************************************************************  


#----REFORMAT-----------------------------------------------------------------------------------------------------------
### format results for epi uploader template -- to model in DisMod
# mild impairment
mild_impairment$mean <- rowMeans(subset(mild_impairment, 
                                select=c(colnames(mild_impairment)[grep("mild_draw_", colnames(mild_impairment))])))
mild_impairment$lower <- apply(subset(mild_impairment, select=c(colnames(mild_impairment)[grep("mild_draw_", colnames(mild_impairment))])), 
                                1, function(x) quantile(x, 0.025))
mild_impairment$upper <- apply(subset(mild_impairment, select=c(colnames(mild_impairment)[grep("mild_draw_", colnames(mild_impairment))])), 
                                1, function(x) quantile(x, 0.975))                                                                     
# save results
write.csv(mild_impairment, file.path(j.version.dir, "02_mild_impairment_epi_input_data.csv"), row.names=FALSE) 

# moderate to severe impairment
mod_sev_impairment$mean <- rowMeans(subset(mod_sev_impairment, 
                               select=c(colnames(mod_sev_impairment)[grep("modsev_draw_", colnames(mod_sev_impairment))])))
mod_sev_impairment$lower <- apply(subset(mod_sev_impairment, select=c(colnames(mod_sev_impairment)[grep("modsev_draw_", colnames(mod_sev_impairment))])), 
                               1, function(x) quantile(x, 0.025))
mod_sev_impairment$upper <- apply(subset(mod_sev_impairment, select=c(colnames(mod_sev_impairment)[grep("modsev_draw_", colnames(mod_sev_impairment))])), 
                               1, function(x) quantile(x, 0.975))   
# save results
write.csv(mod_sev_impairment, file.path(j.version.dir, "03_mod_sev_impairment_epi_input_data.csv"), row.names=FALSE)
#***********************************************************************************************************************

}
