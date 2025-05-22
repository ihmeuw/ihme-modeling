#----HEADER-------------------------------------------------------------------------------------------------------------
# Author: REDACTED
# Date:    February 2017, modified Dec 2017 for GBD 2017, modified 9_2023 for GBD2022 by REDACTED
# Path:    /FILEPATH/01_inverse_model.R
# Purpose: PART ONE - Prepare negative binomial regression of varicella deaths for input into CodCorrect
#          PART TWO - Replace modeled estimates with CODEm model for data-rich countries
#          PART THREE - Format for CodCorrect and save results to database
#          PART FOUR - Run DisMod model for CFR
#          PART FIVE - Calculate nonfatal outcomes from mortality
#          PART SIX - Format for COMO and save results to database
#***********************************************************************************************************************

########################################################################################################################
##### START-UP #########################################################################################################
########################################################################################################################


#----CONFIG-------------------------------------------------------------------------------------------------------------
### load packages
pacman::p_load(magrittr, stats, data.table, rhdf5, plyr, data.table, parallel, dplyr, reticulate, ggplot2, tictoc) 
source("/share/code/coverage/functions/load_packages.R")
load_packages("mvtnorm")

### set data.table fread threads
setDTthreads(1)

## Pull in Pandas for HDF5
use_python('/FILEPATH/bin/python')
pandas <- import("pandas")
tables <- import("tables")
#***********************************************************************************************************************

#----DIRECTORIES--------------------------------------------------------------------------------------------------------
### set objects
acause    <- "varicella"
cause_id  <- 342
me_id     <- 1440
ages      <- c(2:3, 388, 389, 238, 34, 6:20, 30:32, 235) ## age group early neonatal to 95+
gbd_round <- 9
release_id <- 16
year_end <- 2024

### draw numbers
draw_nums_gbd    <- 0:999
draw_cols_gbd <- paste0("draw_", draw_nums_gbd)
draw_cols_upload <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", draw_nums_gbd))

### make folders on cluster
# CoD
cl.death.dir <- file.path("/FILEPATH", acause, "mortality", custom_version, "draws")
if (!dir.exists(cl.death.dir) & SAVE_FATAL=="yes") dir.create(cl.death.dir, recursive=TRUE)
# nonfatal/epi
cl.version.dir <- file.path("/FILEPATH", acause, "nonfatal", custom_version, "draws")                                                
if (!dir.exists(cl.version.dir) & CALCULATE_NONFATAL=="yes") dir.create(file.path(cl.version.dir), recursive=TRUE)

### directories
home <- file.path(j_root, "FILEPATH", acause, "00_documentation")
FILEPATH <- file.path(home, "models", custom_version)
FILEPATH.inputs <- file.path(FILEPATH, "model_inputs")
FILEPATH.logs <- file.path(FILEPATH, "model_logs")
if (!dir.exists(FILEPATH.inputs)) dir.create(FILEPATH.inputs, recursive=TRUE)
if (!dir.exists(FILEPATH.logs)) dir.create(FILEPATH.logs, recursive=TRUE)

### save description of model run
# which model components are being uploaded?
if (CALCULATE_NONFATAL=="yes" & SAVE_FATAL=="no") add_  <- "NF"
if (SAVE_FATAL=="yes" & CALCULATE_NONFATAL=="no") add_  <- "CoD"
if (CALCULATE_NONFATAL=="yes" & SAVE_FATAL=="yes") add_ <- "NF and CoD"
# record CODEm data-rich feeder model versions used in CoD hybridization
if (SAVE_FATAL=="yes") description <- paste0("DR M ", male_CODEm_version, ", DR F ", female_CODEm_version, ", ", description)
# save full description to model folder
description <- paste0(add_, " - ", description)
cat(description, file=file.path(FILEPATH, "MODEL_DESCRIPTION.txt"))
#***********************************************************************************************************************


#----FUNCTIONS----------------------------------------------------------------------------------------------------------  
### custom functions
"/FILEPATH/read_hdf5_table.R" %>% source
"/FILEPATH/sql_query.R" %>% source

### load shared functions
file.path("/FILEPATH/get_population.R") %>% source
file.path("/FILEPATH/get_location_metadata.R") %>% source
file.path("/FILEPATH/get_age_metadata.R") %>% source
file.path("/FILEPATH/get_covariate_estimates.R") %>% source
file.path("/FILEPATH/interpolate.R") %>% source
file.path("/FILEPATH/get_envelope.R") %>% source
file.path("/FILEPATH/get_cod_data.R") %>% source
source("/FILEPATH/get_draws.R")
source("/FILEPATH/interpolate.R")

### load custom function for vaccine effect
source(file.path("/FILEPATH/04_varicella_vaccine_impact_modify_estimates_function.R"))
#***********************************************************************************************************************

#----PREP---------------------------------------------------------------------------------------------------------------
### get location data
locations <- get_location_metadata(release_id = release_id, 
                                   location_set_id=22)[, .(location_id, ihme_loc_id, location_name, region_id, super_region_id, level, location_type, parent_id, sort_order)]
pop_locs <- unique(locations[level >= 3, location_id])
standard_locations <- get_location_metadata(release_id = release_id, 
                                            location_set_id=101)$location_id %>% unique

########################################################################################################################
##### PART ONE: COD NEG BIN REGRESSION #################################################################################
########################################################################################################################

if (SAVE_FATAL == "yes") {
  
  # get mortality envelope
  envelope <- get_envelope(location_id = pop_locs, 
                           sex_id = 1:2, 
                           age_group_id = ages, 
                           year_id = 1980:year_end, 
                           with_hiv = 1, 
                           release_id = release_id)
  # save model version
  cat(paste0("Mortality envelope - model run ", unique(envelope$run_id)),
      file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  setnames(envelope, "mean", "envelope")
  envelope <- subset(envelope, select=c("location_id", "year_id", "age_group_id", "sex_id", "envelope"))
  
  # get population data
  population <- get_population(location_id = pop_locs, 
                               sex_id = 1:2, 
                               age_group_id = ages, 
                               year_id = 1980:year_end, 
                               release_id = release_id)
  # save model version
  cat(paste0("Population - model run ", unique(population$run_id)),
      file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  population <- subset(population, select=c("location_id", "year_id", "age_group_id", "sex_id", "population"))
  
  # bring together
  pop_env <- merge(population, envelope, by=c("location_id", "year_id", "age_group_id", "sex_id"))
  
  #get cod data
  cod <- get_cod_data(cause_id = cause_id, 
                      release_id = release_id)
  cod <- cod[, .(location_id, year_id, age_group_id, sex_id, sample_size, cf, nid, refresh_id)]
  
  #restrict years and ages
  cod <- cod[year_id >= 1980 & age_group_id %in% ages & !is.na(cf) & sample_size != 0, ]
  # save model version
  cat(paste0("CoD data - version ", unique(cod$refresh_id)),
      file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  
  if (HAQI_or_HSA == "HSA") {
    
    # covariate: HSA - covariate_id=208, covariate_name_short="health_system_access_capped"
    covariates <- get_covariate_estimates(covariate_id=208, 
                                          year_id=1980:year_end, 
                                          location_id=pop_locs, 
                                          status="best", 
                                          release_id = release_id)[, .(location_id, year_id, mean_value, model_version_id)]
    setnames(covariates, "mean_value", "health")
    
  } else if (HAQI_or_HSA =="HAQI") {
    
    # covariate: HAQ - covariate_id=1099, covariate_name_short="haqi"
    covariates <- get_covariate_estimates(covariate_id=1099, 
                                          year_id=1980:year_end, 
                                          location_id=pop_locs,  
                                          status="best",
                                          release_id = release_id)[, .(location_id, year_id, mean_value, model_version_id)]
    setnames(covariates, "mean_value", "health")
    
  }
  
  ### save covariate versions
  cat(paste0("Covariate ", HAQI_or_HSA, " (CoD) - model version ", unique(covariates$model_version_id)),
      file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  
  ### prep data for regression
  regress <- merge(cod, covariates, by=c("location_id", "year_id"), all.x=TRUE)
  regress <- merge(regress, pop_env, by=c("location_id", "year_id", "age_group_id", "sex_id"), all.x=TRUE)
  
  ### calculate deaths
  regress[, deaths := cf * envelope]
  
  ### replace deaths with 0 if deaths < 0.5
  regress[deaths < 0.5, deaths := 0]
  
  ### drop outliers (cause fractions greater than 99th percentile)
  cf_99 <- quantile(regress$cf, 0.99)
  
  ### make ages into levels
  regress[, age_group_id := as.factor(age_group_id)]
  
  ### using standard locations
  regress <- regress[location_id %in% standard_locations]
  
  ### remove post covid years from regression file due to changes to envelope? 
  if(outlier_covid_years == T){
    regress <- regress[year_id <= 2019, ]
  }
  
  ### save file for reference
  fwrite(regress, file.path(FILEPATH.inputs, "inputs_for_nbreg_CoD.csv"), row.names=FALSE)
  
  message("regression file saved")
  #***********************************************************************************************************************
  
  
  #----NEG BIN MODEL------------------------------------------------------------------------------------------------------
  ### run negative binomial regression, death counts as a function of age, health system, offset by log population to derive rates
  theta = 1 / 4.42503 
  
  GLM <- MASS::glm.nb(deaths ~ health + age_group_id + offset(log(population)),
                      init.theta=theta,
                      data=regress) #log link is default in this function
  
  # save log
  capture.output(summary(GLM), file = file.path(FILEPATH.logs, "log_deaths_nbreg_CoD.txt"), type="output")
  saveRDS(GLM, file = file.path(FILEPATH.logs, "log_deaths_nbreg_CoD.rds"))
  #***********************************************************************************************************************
  
  #----DRAWS--------------------------------------------------------------------------------------------------------------
  set.seed(0311)
  
  ### predict out for all country-year-age-sex
  pred_death <- merge(population, covariates, by=c("year_id", "location_id"))
  N <- length(pred_death$year_id)
  
  # coefficient matrix
  coefmat <- c(coef(GLM))
  names(coefmat)[1] <- "constant"
  names(coefmat) <- paste("b", names(coefmat), sep="_")
  
  if(gbd_round < 7){
    
    coefmat <- matrix(unlist(coefmat), ncol=24, byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coefmat))))) ##ELBR: change so ncol isn't hard coded (error because age grps changed)
    
  } else if (gbd_round >= 7){
    
    coefmat <- matrix(unlist(coefmat), ncol=26, byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coefmat))))) ##ELBR: change so ncol isn't hard coded (error because age grps changed)
    
  }
  
  ### covariance matrix
  vcovmat <- vcov(GLM)
  # create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
  betas <- t(rmvnorm(n=length(draw_nums_gbd), mean=coefmat, sigma=vcovmat)) %>% data.table
  colnames(betas) <- paste0("beta_draw_", draw_nums_gbd)
  
  if(gbd_round < 7){
    betas[, age_group_id := c(NA, NA, 3:20, 30:32, 235)]
  } else if (gbd_round >=7) {
    betas[, age_group_id := c(NA, NA, 3, 6:20, 30:32, 34, 235, 238, 388, 389)] 
    #don't assign column to age grp 2 (smallest age group numerically because it is reference grp in regression)
  }
  
  # merge together predictions with age draws by age_group_id
  pred_death <- merge(pred_death, betas[!is.na(age_group_id), ], by="age_group_id", all.x=TRUE)
  
  ### create draws of dispersion parameter
  alphas <- 1 / exp(rnorm(length(draw_nums_gbd), mean=GLM$theta, sd=GLM$SE.theta))
  
  ### Calculate death predictions from model
  if (GAMMA_EPSILON == "with") {
    
    lapply(draw_nums_gbd, function(draw) {
      
      # set betas
      b0 <- betas[1, ][[paste0("beta_draw_", draw)]]
      b1 <- betas[2, ][[paste0("beta_draw_", draw)]]
      age.fe <- pred_death[[paste0("beta_draw_", draw)]]
      age.fe <- ifelse(is.na(age.fe), 0, age.fe)
      alpha <- alphas[draw + 1]
      # calculate 1000 draws for death counts
      pred_death[, paste0("death_draw_", draw) := rgamma( length(age_group_id), scale=(alpha *
                                                                                         exp( b0 + b1 * health + age.fe ) *
                                                                                         population ),
                                                          shape=(1 / alpha) ) ]
    })
    
  } else if (GAMMA_EPSILON == "without") {
    
    lapply(draw_nums_gbd, function(draw) {
      
      # set betas
      b0 <- betas[1, ][[paste0("beta_draw_", draw)]]
      b1 <- betas[2, ][[paste0("beta_draw_", draw)]]
      age.fe <- pred_death[[paste0("beta_draw_", draw)]]
      age.fe <- ifelse(is.na(age.fe), 0, age.fe)
      alpha <- alphas[draw + 1]
      # calculate 1000 draws for death counts
      pred_death[, paste0("death_draw_", draw) := exp( b0 + b1 * health + age.fe ) * population ]
      
    })
  }
  
  ### save results
  pred_death_save <- pred_death[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("death_draw_", draw_nums_gbd)), with=FALSE]
  if (WRITE_FILES == "yes") {
    fwrite(pred_death_save, file.path(FILEPATH, paste0("01_death_predictions.csv")), row.names=FALSE)
  }
  message("death predictions saved")
  #***********************************************************************************************************************

  if(!is.null(old_custom_vers)){
    
    # read in old results and overwrite latest run results in date version folder
    pred_death_save <- fread(paste0(home,"/models/", old_custom_vers, "/01_death_predictions.csv"))
    if (WRITE_FILES == "yes") {
      fwrite(pred_death_save, file.path(FILEPATH, paste0("01_death_predictions.csv")), row.names=FALSE)
      
      # delete files from fitting the model above because don't accurately reflect the model used to make the predictions because read in predictions from older model-- should go to old date version folder's model fit files
      unlink(file.path(FILEPATH.logs, "log_deaths_nbreg_CoD.rds"))
      unlink(file.path(FILEPATH.logs, "log_deaths_nbreg_CoD.txt"))
      unlink(file.path(FILEPATH.inputs, "inputs_for_nbreg_CoD.csv"))
      
    }
    
    # track which old version was used
    cat(paste0("Using results from previously run model for global locations from custom_version ", old_custom_vers), 
        file=file.path(FILEPATH, "MODEL_DESCRIPTION.txt"), sep = "\n", append = TRUE)
    cat(paste0("Using results from previously run custom model for non-DR locations - date version: ", old_custom_vers),
        file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
    
  }
  
  ########################################################################################################################
  ##### PART TWO: COMBINE CODEm DEATHS ###################################################################################
  ########################################################################################################################
  
  #----COMBINE------------------------------------------------------------------------------------------------------------
  ### read CODEm results for data-rich countries

  cod_M <- data.table(pandas$read_hdf(file.path("/FILEPATH", paste0("draws/deaths_", "male", ".h5")), key="data"))
  cod_F <- data.table(pandas$read_hdf(file.path("/FILEPATH", paste0("draws/deaths_", "female", ".h5")), key="data"))
  
  # save model version
  cat(paste0("Data-rich CODEm feeder model (males) - model version ", male_CODEm_version),
      file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  cat(paste0("Data-rich CODEm feeder model (females) - model version ", female_CODEm_version),
      file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  
  ### combine M/F CODEm results
  cod_DR <- rbind(cod_M, cod_F) %>% .[, draw_cols_upload, with=FALSE]
  cod_DR <- cod_DR[age_group_id %in% ages, ]
  
  # Clear memory 
  rm(cod_M, cod_F)
  gc(T)
  
  ### hybridize CoD results
  data_rich <- unique(cod_DR$location_id)
  colnames(pred_death_save) <- draw_cols_upload
  split_deaths_glb <- pred_death_save[!location_id %in% data_rich, ]
  split_deaths_hyb <- rbind(split_deaths_glb, cod_DR)
  
  ### save results
  if (WRITE_FILES == "yes") {
    fwrite(split_deaths_hyb, file.path(FILEPATH, "02_death_predictions_hybridized.csv"), row.names=FALSE)
  }
  message("hybrid death predictions saved")
  #***********************************************************************************************************************
  
  ########################################################################################################################
  ##### PART THREE: FORMAT FOR CODCORRECT ################################################################################
  ########################################################################################################################
  
  #----SAVE RESULTS-------------------------------------------------------------------------------------------------------
  # assign measure_id
  split_deaths_hyb[, measure_id := 1]
  
  #write separate csv files of death predictions for each location id
  lapply(unique(split_deaths_hyb$location_id), function(x) fwrite(split_deaths_hyb[location_id==x, ],
                                                                  file.path(cl.death.dir, paste0(x, ".csv")), row.names=FALSE))
  print(paste0("death draws saved in ", cl.death.dir))
  
  # format job for slurm
  job <- paste0("sbatch -J s_cod_", acause, " --mem 100G -c 4 -C archive -t 24:00:00 -A ", cluster_proj, " -p all.q -o FILEPATH ", 
                paste0("/FILEPATH/save_results_wrapper.r"),
                # " --year_ids ", paste(1980:year_end, collapse=","),
                " --year_ids ", paste(unique(split_deaths_hyb$year_id), collapse=","),
                " --type cod",
                " --me_id ", cause_id,
                " --input_directory ", cl.death.dir,
                " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
                " --best ", mark_model_best,
                " --release_id ", release_id)
  
print(job)
system(job)

  #*********************************************************************************************************************** 
  
}


if (CALCULATE_NONFATAL == "yes") {
  
  ########################################################################################################################
  ##### PART FOUR: DisMod NONFATAL RESULTS ###############################################################################
  ########################################################################################################################

  if(apply_vaccine_adjustment == FALSE){
    
  # Get seroprevalence and incidence hazard draws from dismod
  
  var_nf   <- get_draws(gbd_id_type="modelable_entity_id", 
                        gbd_id=1439, 
                        # status="best", 
                        version_id = dismod_seroprevalence_version, 
                        release_id = release_id,
                        source="epi", 
                        measure_id=c(5, 6))
  
  message("Varicella seroprevalence draws loaded")
  
  # divide data into separate incidence and prevelance draws
  var_inc  <- var_nf[measure_id==6, ]
  var_prev <- var_nf[measure_id==5, ]
  
  # save model version
  cat(paste0("Varicella DisMod model (me_id 1439) for prevalence and incidence - model version ", unique(var_prev$model_version_id)), 
      file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  
  # prep prevalence draws for incidence rate calculation
  var_prev2 <- var_prev[, draw_cols_upload, with=FALSE]
  colnames(var_prev2) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("prev_draw_", draw_nums_gbd))
  
  # prep incidence draws for incidence rate calculation
  var_inc2 <- var_inc[, draw_cols_upload, with=FALSE]
  colnames(var_inc2) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("hazard_draw_", draw_nums_gbd))
  
  # merge incidence and prevalence draws
  var_nonfatal <- merge(var_prev2, var_inc2, by=c("location_id", "year_id", "age_group_id", "sex_id"))
  
  # remove large draw file and clear memory
  rm(var_nf)
  gc(T)
  #*********************************************************************************************************************** 

  ########################################################################################################################
  ##### PART FIVE: CALCULATE NONFATAL ####################################################################################
  ########################################################################################################################
  
  #----INCIDENCE----------------------------------------------------------------------------------------------------------
  
  ### calculate incidence from hazards
  # draw_cols_gbd <- paste0("draw_", draw_nums_gbd)
  var_nonfatal[, (draw_cols_gbd) := lapply(draw_nums_gbd, function(ii) get(paste0("hazard_draw_", ii)) * ( 1 - get(paste0("prev_draw_", ii)) ) )]
  predictions_inc_save <- subset(var_nonfatal, select=draw_cols_upload)
  
  ### calculate prevalence from incidence by multiplying incidence by duration
  var_nonfatal[, (draw_cols_gbd) := lapply(draw_nums_gbd, function(ii) get(paste0("draw_", ii)) * (7 / 365.242) )]
  predictions_prev_save <- subset(var_nonfatal, select=draw_cols_upload)
  
  # set prevalence in subset of age groups
  predictions_prev_save <- predictions_prev_save[age_group_id %in% c(32, 33, 235, 164), (draw_cols_gbd) := lapply(draw_nums_gbd, function(ii) get(paste0("draw_", ii)) * 0 )]
  
  if (WRITE_FILES == "yes") {
    
    fwrite(predictions_prev_save, file.path(FILEPATH, "03_prevalence_draws.csv"), row.names=FALSE)
    fwrite(predictions_inc_save, file.path(FILEPATH, "04_incidence_draws.csv"), row.names=FALSE)
    
  }
  message("Prediction files saved!")
  
  #*********************************************************************************************************************** 
  
  } else if (apply_vaccine_adjustment == TRUE) {
    
    # Run vaccine adjustment function: # NB expect this function to take at least 1 hour due to interpolation
    predictions_inc_save <- prep_vaccine() 
    
    # Calculate prevalence 
    predictions_prev_save <- copy(predictions_inc_save)
    
    # Select columns to transform
    draw_cols <- grep("draw", colnames(predictions_prev_save))
    
    # Create new column labels
    draw_cols_gbd_prev <- paste0("prev_", draw_cols_gbd) # columns renamed otherwise calculation did not work
    
    # Calculate prevalence
    predictions_prev_save <- predictions_prev_save[, (draw_cols_gbd_prev) := lapply(.SD, function(x) x * (7 / 365.242)), .SDcols = draw_cols]
    
    # Keep prevalence columns
    predictions_prev_save <- dplyr::select(predictions_prev_save, location_id, year_id, age_group_id, sex_id, contains("prev"))
    
    # Rename columns
    colnames(predictions_prev_save) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", draw_nums_gbd))
    
    # set prevalence in subset of age groups
    predictions_prev_save <- predictions_prev_save[age_group_id %in% c(32, 33, 235, 164), (draw_cols_gbd) := lapply(draw_nums_gbd, function(ii) get(paste0("draw_", ii)) * 0 )]
    
  if (WRITE_FILES == "yes") {
    
    fwrite(predictions_prev_save, file.path(FILEPATH, "03_prevalence_draws.csv"), row.names=FALSE)
    fwrite(predictions_inc_save, file.path(FILEPATH, "04_incidence_draws.csv"), row.names=FALSE)
    
  }
  message("Prediction files saved!")
    
  }

  ########################################################################################################################
  ##### PART SIX: FORMAT FOR COMO ########################################################################################
  ########################################################################################################################
  
  #----SAVE RESULTS-------------------------------------------------------------------------------------------------------

  # prevalence, measure_id==5
  predictions_prev_save[, measure_id := 5]
  
  # incidence, measure_id==6
  predictions_inc_save[, measure_id := 6]
  
  #bind together
  predictions <- rbind(predictions_prev_save, predictions_inc_save)
  
  # Clear memory
  rm(predictions_prev_save, predictions_inc_save)
  gc(T)
  
  # save to /share directory by location id
  lapply(unique(predictions$location_id), function(x) fwrite(predictions[location_id==x],
                                                             file.path(cl.version.dir, paste0(x, ".csv")), row.names=FALSE))
  print(paste0("nonfatal estimates saved in ", cl.version.dir))
  
  # Format job for cluster
  job_nf <- paste0("sbatch REDACTED", 
                   paste0("/FILEPATH/save_results_wrapper.r"),
                   " --type epi",
                   " --me_id ", me_id,
                   " --year_ids ", paste(unique(predictions$year_id), collapse=","),
                   " --input_directory ", cl.version.dir,
                   " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
                   " --best ", mark_model_best,
                   " --release_id ", release_id,
                   " --xw_id ", vari_xw,
                   " --bundle_id ", bundle_id)
  
  system(job_nf); 
  print(job_nf)
  #***********************************************************************************************************************
  
}

