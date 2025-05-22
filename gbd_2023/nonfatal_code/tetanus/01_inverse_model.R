#----HEADER-------------------------------------------------------------------------------------------------------------
# TITLE: TETANUS MORTALITY TO INCIDENCE MODEL: NON-FATAL OUTCOME ESTIMATION
# Author:  REDACTED
# Date:    February 2017; Spring 2019 edits, January 2024
# Purpose: PART ONE -   Get CODEm model results fatal outcomes
#          PART TWO -   Get Dismod model results for case fatality rate using sex- and age-split data (MR-BRT, out-of-DisMod)
#          PART THREE - Calculate deaths, prevalence, and incidence from CFR, death rate, and duration
#          PART FOUR -  Format for COMO and save results to database
#          PART FIVE -  Prep motor impairment models
#		       PART SIX -   Upload impairment data for later DisMod modeling
#***********************************************************************************************************************


########################################################################################################################
##### START-UP #########################################################################################################
########################################################################################################################


#----CONFIG-------------------------------------------------------------------------------------------------------------
### load packages
pacman::p_load(magrittr, stats, data.table, dplyr, plyr, lme4, reshape2, parallel, foreach, doParallel, reticulate) #MASS and foreign package import here causing errors!
if (Sys.info()["sysname"]=="Linux") {
  library(rhdf5) 
  library(openxlsx) 
} else { 
  pacman::p_load(rhdf5, openxlsx)
}

#set python environment 
use_python('/FILEPATH/python')
gd <- import('get_draws.api')

### set data.table fread threads
setDTthreads(1)
#***********************************************************************************************************************


#----DIRECTORIES--------------------------------------------------------------------------------------------------------
### set objects
acause           <- "tetanus"
cause_id         <- 340
mild_bundle_id   <- 47
modsev_bundle_id <- 48

release_id <- 16
year_end <- 2024

### draw numbers
if (FauxCorrect) {
  draw_nums_gbd  <- 0:99
} else {
  draw_nums_gbd  <- 0:999
}
draw_cols_upload <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", draw_nums_gbd))

### make folders on cluster
cl.version.dir <- file.path("/FILEPATH", custom_version, "draws")                                                
if (!dir.exists(cl.version.dir)) dir.create(cl.version.dir, recursive=TRUE)

### directories
home <- file.path(j_root, "FILEPATH", acause, "00_documentation")
FILEPATH <- file.path(home, "models", custom_version)
if (!dir.exists(FILEPATH)) dir.create(FILEPATH, recursive=TRUE)
FILEPATH.logs <- file.path(FILEPATH, "model_logs")
if (!dir.exists(FILEPATH.logs)) dir.create(FILEPATH.logs, recursive=TRUE)

### save description of model run
if (UPLOAD_NONFATAL=="yes" & CALCULATE_IMPAIRMENTS=="no") add_  <- "NF"
if (CALCULATE_IMPAIRMENTS=="yes" & UPLOAD_NONFATAL=="no") add_  <- "Impairments"
if (UPLOAD_NONFATAL=="yes" & CALCULATE_IMPAIRMENTS=="yes") add_ <- "NF and Impairments"
description <- paste0(add_, " - ", description)
cat(description, file=file.path(FILEPATH, "MODEL_DESCRIPTION.txt"))

#***********************************************************************************************************************


#----FUNCTIONS----------------------------------------------------------------------------------------------------------  
### reading .h5 files
"/FILEPATH/read_hdf5_table.R" %>% source

### custom quantile calculator
"/FILEPATH/collapse_point.R" %>% source

### custom workflow functions
'%!in%' <- function(x,y)!('%in%'(x,y))

### load shared functions
source("/FILEPATH/get_location_metadata.R") 
source("/FILEPATH/get_population.R") 
source("/FILEPATH/get_draws.R")
source("/FILEPATH/get_ids.R")
source("/FILEPATH/get_bundle_data.R") 
source("/FILEPATH/upload_bundle_data.R") 
source('/FILEPATH/save_bundle_version.R')
source('/FILEPATH/get_bundle_version.R')
source('/FILEPATH/save_crosswalk_version.R')
source('/FILEPATH/get_crosswalk_version.R')
source("/FILEPATH/validate_input_sheet.R")
source("/FILEPATH/get_model_results.R")

#*********************************************************************************************************************** 


########################################################################################################################
##### PART ONE: CODEm MODEL FOR FATAL ##################################################################################
########################################################################################################################

#----PULL---------------------------------------------------------------------------------------------------------------
### get location data
locations <- get_location_metadata(release_id = release_id, location_set_id=22)[, 
                                                                               .(location_id, ihme_loc_id, location_name, region_id, super_region_id, level, location_type, parent_id, sort_order)]
pop_locs <- unique(locations[level >= 3, location_id])

# get specific location sets
standard_locations <- get_location_metadata(release_id = release_id, location_set_id=101)$location_id %>% unique
dismod_locs <- get_location_metadata(release_id = release_id, location_set_id=9)$location_id %>% unique

# Define years modelled by pulling draws from Dismod CFR model (sample from a single location) 
available_years <- get_draws(gbd_id_type="modelable_entity_id", 
                               gbd_id=2833, #tetanus CFR
                               source="epi", 
                               location_id=6, 
                               sex_id=1, 
                               age_group_id=6, 
                               release_id = release_id,
                               status="best")[, year_id] %>% unique

# Get fatal draws

if(!codem_estimates){ # If using CodCorrect estimates
  if(scale_codcorrect == FALSE){
    
  cod_draws <- get_draws(gbd_id_type="cause_id", 
                         gbd_id=cause_id, 
                         location_id = pop_locs, 
                         year_id=available_years, 
                         source= "codcorrect", 
                         measure_id=1, 
                         version_id = codcorrect_version,
                         release_id = release_id, 
                         num_workers=20)
  
  } else if (scale_codcorrect == TRUE){
    
      cod_draws <- fread(paste0("/FILEPATH.csv"))
      cod_draws <- dplyr::select(cod_draws, -contains("draw")) # remove original draw columns and rename
      colnames(cod_draws) <- sub("^mod_", "draw_", colnames(cod_draws))
      
    }
    
  message("Pulled death estimates from CodCorrect!")
  
} else if (codem_estimates){ # If using CODEm estimates - 
  
  all_draws <- data.table()
  
  for(mod in unique(codem_versions)){
    
    print(paste0("Getting codem version ", mod))
    
    draws <- get_draws(gbd_id_type="cause_id", 
                         gbd_id=340, 
                         source="codem", 
                         location_id=pop_locs, 
                         year_id=available_years, 
                         release_id = release_id,
                         # sex_id=c(1,2),
                         measure_id=1, 
                         version_id = mod, 
                         num_workers=10)
    
  message(paste0("Pulled death estimates from CODEm for model version ", mod))
  all_draws <- rbind(all_draws, draws)
  
  }
  
  cod_draws <- copy(all_draws)
  rm(all_draws)
  gc(T)
  
}

# save model version
if (!is.null(unique(cod_draws$output_version_id))) vers <- unique(cod_draws$output_version_id) else vers <- custom_version
if(!codem_estimates){
  cat(paste0("CodCorrect results - output version ", vers),
  file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
} else {
  cat(paste0("CODEm results - output version ", vers),
  file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
}

if(scale_codcorrect == TRUE){
  cat(paste0("CodCorrect ", vers, " has been rescaled!"),
      file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
}

### get population data
population <- get_population(location_id=pop_locs, 
                             year_id=1980:year_end, 
                             age_group_id=-1, 
                             sex_id=1:2, 
                             release_id = release_id)

# save model version
cat(paste0("Population - model run ", unique(population$run_id)), 
    file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

# remove additional columns if codem was used
if(codem_estimates) {cod_draws <- cod_draws[, c("measure_id", "model_version_id", "metric_id", "sex_name", "cause_id", "envelope") := NULL]}

# merge on population (if using cod correct results)
if(!codem_estimates) {cod_draws <- merge(cod_draws, population, by=c("location_id", "year_id", "age_group_id", "sex_id"), all.x=TRUE)}

# subset desired columns
cod_draws <- cod_draws[, c(draw_cols_upload, "population"), with=FALSE]

#Fix column names
colnames(cod_draws) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("death_draw_", draw_nums_gbd), "pop") #caution this assumes order of columns is order of colnames!

#***********************************************************************************************************************

########################################################################################################################
##### PART TWO: DisMod MODEL FOR CFR ###################################################################################
########################################################################################################################


#----PULL---------------------------------------------------------------------------------------------------------------
### get CFR draws from DisMod model -- (proportion model measure_id=18)
cfr_draws <- get_draws(gbd_id_type = "modelable_entity_id", 
                       gbd_id = 2833, 
                       source = "epi", 
                       location_id = pop_locs, 
                       sex_id= 1:2,
                       age_group_id=c(2:3, 388, 389, 238, 34, 6:20, 30:32, 235), 
                       release_id = release_id,
                       version_id = cfr_model_version)
                       # status="best"

  message("Pulled CFR draws from Dismod")

# save model version
cat(paste0("Case fatality ratio DisMod model (me_id 2833) - model run ", unique(cfr_draws$model_version_id)),
    file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

# remove excess columns
cfr_draws <- cfr_draws[, draw_cols_upload, with=FALSE]

colnames(cfr_draws) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("cfr_draw_", draw_nums_gbd))
#***********************************************************************************************************************


########################################################################################################################
##### PART THREE: CALCULATE DEATHS, PREVALENCE, INCIDENCE ##############################################################
########################################################################################################################

#----PREP---------------------------------------------------------------------------------------------------------------
### pull in duration data
  duration <- read.csv(file.path(j_root, "FILEPATH/duration_draws.csv")) %>% data.table %>% 
    .[cause=="A07.a"] %>%
    .[, paste0("draw", draw_nums_gbd), with=FALSE]
  
colnames(duration) <- paste0("dur_draw_", draw_nums_gbd)
#NB^ above file is from 2010

# merge death draws with population, cfr draws, and duration draws
death_input <- merge(cfr_draws, cod_draws, by=c("location_id", "year_id", "age_group_id", "sex_id"))
death_input <- death_input[, paste0("dur_draw_", draw_nums_gbd) := duration[, paste0("dur_draw_", draw_nums_gbd), with=FALSE]] 
#***********************************************************************************************************************


#----CALCULATE----------------------------------------------------------------------------------------------------------
### calculate death rate as deaths / population
### calculate prevalence as ( death rate / CFR ) * duration
### calculate incidence as death rate / CFR

death_input <- death_input[, paste0("drate_draw_", 0:999) := lapply(.SD, function(x) x / pop), .SDcols=paste0("death_draw_", 0:999)]

lapply(draw_nums_gbd, function(ii) { 
  death_input <- death_input[, paste0("prev_draw_", ii) := ( get(paste0("drate_draw_", ii)) / get(paste0("cfr_draw_", ii)) ) * get(paste0("dur_draw_", ii))]  #b)
  death_input <- death_input[, paste0("inc_draw_", ii) := get(paste0("drate_draw_", ii)) / get(paste0("cfr_draw_", ii))]  #c)
})

death_input <- death_input[, paste0("case_draw_", 0:999) := lapply(.SD, function(x) x * pop), .SDcols=paste0("inc_draw_", 0:999)]

#***********************************************************************************************************************


# ----SAVE--------------------------------------------------------------------------------------------------------------
### prevalence
predictions_prev_save <- death_input[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("prev_draw_", draw_nums_gbd)), with=FALSE]

### incidence
predictions_inc_save <- death_input[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("inc_draw_", draw_nums_gbd)), with=FALSE]

### case counts
predictions_case_save <- death_input[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("case_draw_", draw_nums_gbd)), with=FALSE]

### save results
if (WRITE_FILES == "yes") {
  fwrite(predictions_prev_save, file.path(FILEPATH, "01_prev_draws.csv"), row.names=FALSE)
  fwrite(predictions_inc_save, file.path(FILEPATH, "02_inc_draws.csv"), row.names=FALSE)
  fwrite(predictions_case_save, file.path(FILEPATH, "03_case_draws.csv"), row.names=FALSE)
}
if (WRITE_FILES == "yes" & CALCULATE_IMPAIRMENTS == "no") {
  fwrite(death_input, file.path(FILEPATH, "04_death_input_obj.csv"), row.names=FALSE)
}
message("Prediction files saved")
#***********************************************************************************************************************


if (UPLOAD_NONFATAL == "yes") {  
  
  ########################################################################################################################
  ##### PART FOUR: FORMAT FOR COMO #######################################################################################
  ########################################################################################################################
  
  
  #----SAVE RESULTS-------------------------------------------------------------------------------------------------------
  ### format prevalence for como, measure_id==5
  colnames(predictions_prev_save) <- draw_cols_upload
  predictions_prev_save[, measure_id := 5]
  
  ### format incidence for como, measure_id==6
  colnames(predictions_inc_save) <- draw_cols_upload
  predictions_inc_save[, measure_id := 6]
  
  # combine prevalence and incidence predictions
  predictions <- rbind(predictions_prev_save, predictions_inc_save)
  
  # clear memory
  rm(predictions_prev_save, predictions_inc_save)
  gc(T)
  
  # save predictions by location id
  lapply(unique(predictions$location_id), function(x) fwrite(predictions[location_id==x],
                                                             file.path(cl.version.dir, paste0(x, ".csv")), row.names=FALSE))
  print(paste0("Nonfatal results saved in ", cl.version.dir))
  
  # save tetanus incidence draws
  job <- paste0("sbatch -J s_epi_", acause, "_1426 --mem 120G -c 10 -C archive -t 10:00:00 -A proj_cov_vpd -p all.q -o /FILEPATH/",
                username, "/%x.o%j",
                " /FILEPATH/execRscript.sh -s ", 
                paste0("/FILEPATH/save_results_wrapper.r"),
                " --year_ids ", paste(unique(predictions$year_id), collapse=","),
                " --type epi",
                " --me_id ", 1425,
                " --input_directory ", cl.version.dir,
                " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
                " --release_id ", release_id, 
                " --best ", mark_model_best,
                " --bundle_id ", bundle_id,
                " --xw_id ", xw_id)
  
  print(job)
  system(job) 
 
  # save severe tetanus draws, modelable_entity_id=1426
  job <- paste0("sbatch -J s_epi_", acause, "_1426 --mem 120G -c 10 -C archive -t 15:00:00 -A proj_cov_vpd -p all.q -o /FILEPATH/",
                username, "/%x.o%j",
                " /FILEPATH/execRscript.sh -s ", 
                paste0("FILEPATH/save_results_wrapper.r"),
                " --year_ids ", paste(unique(predictions$year_id), collapse=","),
                " --type epi",
                " --me_id ", 1426,
                " --input_directory ", cl.version.dir,
                " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
                " --release_id ", release_id, 
                " --best ", mark_model_best,
                " --bundle_id ", bundle_id,
                " --xw_id ", xw_id)

  print(job)
  system(job)
  #***********************************************************************************************************************
  
}

if (CALCULATE_IMPAIRMENTS == "yes") {
  
  ########################################################################################################################
  ##### PART FIVE: PREP MOTOR IMPAIRMENT MODELS ##########################################################################
  ########################################################################################################################
  
  #----PREP---------------------------------------------------------------------------------------------------------------
  inc_draws <- death_input[age_group_id <= 3, ]
  #***********************************************************************************************************************  
  
  
  #----MOTOR IMPAIRMENT---------------------------------------------------------------------------------------------------
  ### calculate incidence of survival = incidence * (1 - cfr)
  ### calculate incidence of mild motor impairment at a proportion of 0.11 (meta-analysis by Theo et al)
  ### calculate incidence of moderate to severe motor impairment at a proportion of 0.07
  
  for (ii in draw_nums_gbd) {  
    inc_draws[, paste0("survival_draw_", ii) := get(paste0("inc_draw_", ii)) * ( 1 - get(paste0("cfr_draw_", ii)) )]
  }
  inc_draws <- inc_draws[, paste0("mild_draw_", draw_nums_gbd) := lapply(.SD, function(x) x * 0.11), .SDcols=paste0("survival_draw_", draw_nums_gbd)]
  inc_draws <- inc_draws[, paste0("modsev_draw_", draw_nums_gbd) := lapply(.SD, function(x) x * 0.07), .SDcols=paste0("survival_draw_", draw_nums_gbd)]
  
  # keep and rename needed columns
  mild_impairment <- inc_draws[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("mild_draw_", draw_nums_gbd)), with=FALSE]
  mod_sev_impairment <- inc_draws[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("modsev_draw_", draw_nums_gbd)), with=FALSE]
  
  ### save results
  if (WRITE_FILES == "yes") {
    fwrite(mild_impairment, file.path(FILEPATH, "04_mild_impairment_draws.csv"), row.names=FALSE)
    fwrite(mod_sev_impairment, file.path(FILEPATH, "05_mod_sev_impairment_draws.csv"), row.names=FALSE)
  }
  #***********************************************************************************************************************
  mod_sev_impairment <- fread(paste0(FILEPATH, "/05_mod_sev_impairment_draws.csv"))
  
  ########################################################################################################################
  ##### PART SIX: UPLOAD IMPAIRMENTS #####################################################################################
  ########################################################################################################################
  
  # Source impairments bundle upload script
  source(paste0("/FILEPATH/tetanus_impairments_bundle_upload.R"))

  ########################################################################################################################
  ########################################################################################################################
  #***********************************************END SCRIPT!************************************************************* 
  ########################################################################################################################
  ########################################################################################################################
}
