#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: PART ONE - Prepare negative binomial regression of diphtheria cases for input into codcorrect
#          PART TWO - Replace modeled estimates with CODEm model for data-rich countries
#          PART THREE - Format for CodCorrect and save results to database
#          PART FOUR - Run DisMod model for CFR
#          PART FIVE - Calculate nonfatal outcomes from mortality
#          PART SIX - Format for COMO and save results to database
#***********************************************************************************************************************


#----CONFIG-------------------------------------------------------------------------------------------------------------
### load packages
pacman::p_load(magrittr, foreign, stats, MASS, data.table, rhdf5, plyr,data.table, parallel, dplyr)
if (Sys.info()["sysname"] == "Linux") {
  require(mvtnorm, lib="FILEPATH")
} else { 
  pacman::p_load(mvtnorm)
}
#***********************************************************************************************************************


#----DIRECTORIES--------------------------------------------------------------------------------------------------------
### set objects
acause <- "varicella"
cause_id <- 342
me_id <- 1440
ages <- c(2:20, 30:32, 235) ## age group early neonatal to 95+

### make folders on cluster
cl.death.dir <- file.path("FILEPATH")                                              
dir.create(cl.death.dir, recursive = T)

cl.version.dir <- file.path("FILEPATH")                                              
dir.create(file.path(cl.version.dir), recursive = T)

### directories
home <- file.path(j_root, "FILEPATH")  
j.version.dir <- file.path(home, "models", custom_version)
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
file.path(j_root, "FILEPATH/get_cod_data.R") %>% source
file.path(j_root, "FILEPATH/get_draws.R") %>% source

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
  # return a list where each element are the column names of the different
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
#***********************************************************************************************************************


if (CALCULATE_NONFATAL == "yes") {
  
  ########################################################################################################################
  ##### PART FOUR: DisMod NONFATAL RESULTS ###############################################################################
  ########################################################################################################################
  
  
  #----GET HAZARDS--------------------------------------------------------------------------------------------------------
  ### varicella DisMod model results
  var_prev <- get_draws(gbd_id_field="modelable_entity_id", gbd_id=1439, status="best", gbd_round_id=4, source="epi", measure_id=5)
  var_inc <- get_draws(gbd_id_field="modelable_entity_id", gbd_id=1439, status="best", gbd_round_id=4, source="epi", measure_id=6)
   
  # prep prevalence draws for incidence rate calculation
  var_prev2 <- subset(var_prev, select=c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", 0:999)))
  colnames(var_prev2) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("prev_draw_", 0:999))
  
  # prep incidence draws for incidence rate calculation
  var_inc2 <- subset(var_inc, select=c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", 0:999)))
  colnames(var_inc2) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("hazard_draw_", 0:999))
  
  # merge incidence and prevalence draws
  var_nonfatal <- merge(var_prev2, var_inc2, by=c("location_id", "year_id", "age_group_id", "sex_id"))
  #*********************************************************************************************************************** 
  
  
  ########################################################################################################################
  ##### PART FIVE: CALCULATE NONFATAL ####################################################################################
  ########################################################################################################################
  
  
  #----INCIDENCE----------------------------------------------------------------------------------------------------------
  var_nonfatal <- var_nonfatal %>% data.frame
  
  ### calculate incidence from hazards
  for (ii in 0:999) {
    var_nonfatal[, paste0("draw_", ii)] <- var_nonfatal[, paste0("hazard_draw_", ii)] * ( 1 - var_nonfatal[, paste0("prev_draw_", ii)] )
  }
  
  # save results
  predictions_inc_save <- subset(var_nonfatal, select=c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", 0:999)))
  
  ### calculate prevalence from incidence
  for (ii in 0:999) {
    var_nonfatal[, paste0("draw_", ii)] <- var_nonfatal[, paste0("draw_", ii)] * (7 / 365.242)
  }
  
  # save results
  predictions_prev_save <- subset(var_nonfatal, select=c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", 0:999)))
  # drop age over 90
  predictions_prev_save <- predictions_prev_save[!predictions_prev_save $age_group_id %in% c(32, 235), ]
  
  if (WRITE_FILES == "yes") {
    write.csv(predictions_prev_save, file.path(j.version.dir, "03_prevalence_draws.csv"), row.names=FALSE)
    write.csv(predictions_inc_save, file.path(j.version.dir, "04_incidence_draws.csv"), row.names=FALSE)
  }
  #*********************************************************************************************************************** 
  
  
  ########################################################################################################################
  ##### PART SIX: FORMAT FOR COMO ########################################################################################
  ########################################################################################################################
  
  
  #----SAVE RESULTS-------------------------------------------------------------------------------------------------------
  ### format nonfatal for como
  # prevalence, measure_id==5
  predictions_prev_save$measure_id <- 5
  # incidence, measure_id==6
  predictions_inc_save$measure_id <- 6
  
  predictions <- rbind(predictions_prev_save, predictions_inc_save)
  predictions <- predictions %>% data.table
  
  lapply(unique(predictions$location_id), function(x) fwrite(predictions[location_id==x],
                                                             file.path(cl.version.dir, paste0(x, ".csv")), row.names=FALSE))
  
  print(paste0("nonfatal estimates saved in ", cl.version.dir))
  #***********************************************************************************************************************
  
}