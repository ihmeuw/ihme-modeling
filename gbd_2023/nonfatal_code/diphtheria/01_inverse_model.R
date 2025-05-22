#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: Diphtheria fatal and non fatal estimation for GBD
# Author:  REDACTED
# Date:    February 2017, 
# Revised: November 2023, for GBD 2023 by REDACTED
# CONTENTS:SETUP
#          PART ONE -   Prepare negative binomial regression of diphtheria deaths for input into codcorrect
#          PART TWO -   Format for CodCorrect and save results to database
#          PART THREE - Run DisMod model for CFR
#          PART FOUR -  Calculate nonfatal outcomes from mortality
#                       Use mortality to calculate prevalence (prev = mort/cfr*duration)
#                       Calculate incidence from prevalence (inc = prev/duration)
#                       Fix nonfatal estimates so that predictions are never less than case notifications
#          PART FIVE -  Format for COMO and save results to database
#***********************************************************************************************************************

########################################################################################################################
##### START-UP #########################################################################################################
########################################################################################################################


#----CONFIG-------------------------------------------------------------------------------------------------------------
### load packages
pacman::p_load(magrittr,stats, data.table, rhdf5, plyr, 
               data.table, parallel, dplyr, lme4, tictoc) 
source("/FILEPATH/load_packages.R")
load_packages("mvtnorm")

library(reticulate)
reticulate::use_condaenv("/FILEPATH/gbd_env")
pandas <- reticulate::import("pandas")
#***********************************************************************************************************************

#----DIRECTORIES--------------------------------------------------------------------------------------------------------
### set objects
acause    <- "diptheria"
age_start <- 6
age_end   <- 16
cause_id  <- 338
me_id     <- 1421
release_id <- 16
gbd_round <- 9
year_end <- 2024

### draw numbers
draw_nums_gbd    <- 0:999
draw_cols_upload <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", draw_nums_gbd))

### make folders on cluster
# CoD
FILEPATH <- file.path("/FILEPATH", acause, "mortality", custom_version, "draws")
if (!dir.exists(FILEPATH)) dir.create(FILEPATH, recursive=TRUE)
# nonfatal/epi
cl.version.dir <- file.path("/FILEPATH", acause, "nonfatal", custom_version, "draws")
if (!dir.exists(cl.version.dir)) dir.create(cl.version.dir, recursive=TRUE)

### directories
home <- file.path(j_root, "FILEPATH", acause, "00_documentation")
FILEPATH <- file.path(home, "models", custom_version)
FILEPATH <- file.path(FILEPATH, "model_inputs")
FILEPATH.logs <- file.path(FILEPATH, "model_logs")
if (!dir.exists(FILEPATH)) dir.create(FILEPATH, recursive=TRUE)
if (!dir.exists(FILEPATH.logs)) dir.create(FILEPATH.logs, recursive=TRUE)

### save description of model run
# which model components are being uploaded?
if (CALCULATE_NONFATAL=="yes" & CALCULATE_FATAL=="no") add_  <- "NF"
if (CALCULATE_FATAL=="yes" & CALCULATE_NONFATAL=="no") add_  <- "CoD"
if (CALCULATE_NONFATAL=="yes" & CALCULATE_FATAL=="yes") add_ <- "NF and CoD"
# record CODEm data-rich feeder model versions used in CoD hybridization
if (CALCULATE_FATAL=="yes") description <- paste0("DR M ", male_CODEm_version, ", DR F ", female_CODEm_version, ", ", description)
# save full description to model folder
description <- paste0(add_, " - ", description)
cat(description, file=file.path(FILEPATH, "MODEL_DESCRIPTION.txt"))
#***********************************************************************************************************************

#----FUNCTIONS----------------------------------------------------------------------------------------------------------  
### load custom functions
source("/FILEPATH/sql_query.R")
source("/FILEPATH/read_hdf5_table.R")

### load shared functions
source("/FILEPATH/get_population.R")
source("/FILEPATH/get_location_metadata.R") 
source("/FILEPATH/get_covariate_estimates.R") 
source("/FILEPATH/get_envelope.R") 
source("/FILEPATH/get_draws.R")
source("/FILEPATH/get_cod_data.R") 
source("/FILEPATH/get_crosswalk_version.R") 
source("/FILEPATH/interpolate.R")

#*********************************************************************************************************************** 

#----PREP---------------------------------------------------------------------------------------------------------------
### get location data
locations <- get_location_metadata(location_set_id=22, release_id = release_id)[, 
                                                                                .(location_id, ihme_loc_id, location_name, location_ascii_name, region_id, super_region_id, level, location_type, parent_id, super_region_name, sort_order)]
pop_locs <- unique(locations[level >= 3, location_id])
standard_locations <- get_location_metadata(release_id = release_id, location_set_id=101)$location_id %>% unique

# Get mortality envelope
envelope <- get_envelope(location_id = pop_locs, 
                         sex_id= 1:2, 
                         age_group_id = c(age_start:age_end, 388, 389, 238, 34), 
                         with_hiv = 1, 
                         year_id = 1980:year_end, 
                         release_id = release_id)

# Keep desired columns
envelope <- envelope[, .(location_id, year_id, age_group_id, sex_id, mean, run_id)]  
setnames(envelope, "mean", "envelope")

# Get population data
population <- get_population(release_id = release_id, 
                             location_id = pop_locs, 
                             year_id= 1980:year_end, 
                             age_group_id = c(age_start:age_end, 388, 389, 238, 34), 
                             sex_id = 1:2)
# Keep desired columns
population <- population[, .(location_id, year_id, age_group_id, sex_id, population, run_id)]

# save model version
cat(paste0("Mortality envelope - model run ", unique(envelope$run_id)),
    file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
cat(paste0("Population - model run ", unique(population$run_id)), 
    file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

# Combine mortality envelope with population data 
pop_env <- merge(population, envelope, by=c("location_id", "year_id", "age_group_id", "sex_id"))
message("Mortality and population data have been merged")

########################################################################################################################
##### PART ONE: COD NEG BIN REGRESSION #################################################################################
########################################################################################################################

#######################################
if (CALCULATE_FATAL == "yes") {
  #######################################
  
  ### Get covariates
  
  # covariate: DTP3_coverage_prop, covariate_id=32 or 5 year lagged proportion covered, covariate id = 2308
  if(covid_inclusive_vax == F){
    if (use_lagged_covs) {
      if(is.null(custom_coverage)){
        covar <- get_covariate_estimates(covariate_id=2308, year_id=1980:year_end, location_id=pop_locs,
                                         release_id = release_id)[, .(location_id, year_id, mean_value, model_version_id)]
      } else {
        
        covar <- get_covariate_estimates(covariate_id=2308, year_id=1980:year_end, location_id=pop_locs, model_version_id = custom_coverage,
                                         release_id = release_id )[, .(location_id, year_id, mean_value, model_version_id)]
      }
      
    } else {
      covar <- get_covariate_estimates(covariate_id=32, year_id=1980:year_end, location_id=pop_locs,
                                       release_id = release_id)[, .(location_id, year_id, mean_value, model_version_id)]
    }
  } else if (covid_inclusive_vax == T) {
    covar <- get_covariate_estimates(covariate_id=2441, year_id=1980:year_end, location_id=pop_locs,
                                     release_id = release_id)[, .(location_id, year_id, mean_value, model_version_id)]
  }
  
  setnames(covar, "mean_value",  "DTP3_coverage_prop")
  
  if (fatal_fit == "add_HAQI") {
    haqi <- get_covariate_estimates(covariate_id=1099, year_id=1980:year_end, location_id=pop_locs,
                                    release_id = release_id)[, .(location_id, year_id, mean_value, model_version_id)]
    setnames(haqi, c("mean_value", "model_version_id"), c("HAQI", "model_version_id_haqi"))
    covar <- merge(covar, haqi, by=c("location_id", "year_id"))
  }
  
  ### save covariate versions
  cat(paste0("Covariate DTP3_coverage_prop (CoD) - model version ", unique(covar$model_version_id)),
      file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  if (fatal_fit=="add_HAQI") {
    cat(paste0("Covariate HAQI (CoD) - model version ", unique(haqi$model_version_id_haqi)),
        file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  }
  
  # Get cod data
  raw <- get_cod_data(cause_id = cause_id, 
                      release_id = release_id)
  raw <- raw[, .(nid, location_id, year_id, age_group_id, sex_id, cf_corr, sample_size, refresh_id)] 
  
  # save model version
  cat(paste0("CoD data - version ", unique(raw$refresh_id)),
      file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  
  # Merge COD data with mortality and population
  raw <- merge(raw, pop_env, by=c("location_id", "year_id", "age_group_id", "sex_id"), all.x=TRUE)
  
  # calculate death counts
  raw[, deaths := cf_corr * envelope]
  
  ### round to whole number 
  raw[deaths < 0.5, deaths := 0]
  
  ### inform model with national data only
  raw <- raw[location_id %in% locations[level==3, location_id], ]
  
  ### drop outliers (cause fraction greather than 99th percentile)
  cf_999 <- quantile(raw$cf_corr, 0.999)
  raw <- raw[cf_corr <= cf_999, ]
  
  ### make ages into levels
  raw[, age_group_id := factor(age_group_id, levels=c(388, 389, 238, 34, 6:16))]
  
  ### Merge death data with covariates for regression
  regress <- merge(raw, covar, by=c("location_id", "year_id"))
  regress <- regress[location_id %in% standard_locations] # should not drop anything because standard locations drops some subnationals, but we have already subset to only national locations
  
  # Merge again with locations to include location name and super region (do we need these?)
  regress <- merge(regress, locations[, .(location_id, location_name, super_region_id)], by = "location_id")
  
  if(outlier_covid_years == T){
    regress <- regress[year_id <= 2019, ]
  }
  
  ### save file for reference
  fwrite(regress, file.path("FILEPATH"), row.names=FALSE)
  message("Regression input file saved")
  #***********************************************************************************************************************
  
  #----NEG BIN MODEL------------------------------------------------------------------------------------------------------
  ### save input data and run negative binomial regression
  if(fatal_res == FALSE){
    
    if(weights == "sample_size"){
      
      GLM <- MASS::glm.nb(deaths ~ DTP3_coverage_prop + age_group_id + offset(log(envelope)) + super_region_id, data=regress, weights = sample_size)
      
    } else if (weights == "data_star") {
      
      ## assign weights to locations by star rating, use country files because regress only contains national level data
      weight_table <- data.table(stars = c(1, 2, 3, 4, 5), weight = c(0.25, 0.25, 0.25, 1, 1))
      location_stars <- rbind(fread(file.path(j_root, "FILEPATH/locations_four_plus_stars.csv")), 
                              fread(file.path(j_root, "FILEPATH/countries_less_than_four_stars.csv")))
      location_stars_weights <- merge(location_stars, weight_table, by="stars")
      
      regress <- merge(regress, location_stars_weights[,.(location_id, stars, weight)], by="location_id")
      
      GLM <- MASS::glm.nb(deaths ~ DTP3_coverage_prop + age_group_id + offset(log(envelope)) + super_region_id, data=regress, weights = weight)
      
    } else if (weights == "none") {
      
      if(fatal_fit=="sr_FE"){
        regress <- merge(regress, locations[,.(location_id, super_region_id)], by="location_id")
        regress[, super_region_id := as.factor(super_region_id)]
        GLM <- MASS::glm.nb(deaths ~ DTP3_coverage_prop + age_group_id + offset(log(envelope)) + super_region_id, data=regress)
        
      } else if (fatal_fit=="add_HAQI"){
        
        GLM <- MASS::glm.nb(deaths ~ DTP3_coverage_prop + HAQI + age_group_id + offset(log(envelope)), data=regress)
        
      } else if (fatal_fit=="normal"){
        
        GLM <- MASS::glm.nb(deaths ~ DTP3_coverage_prop + age_group_id + offset(log(envelope)), data=regress)
        
      }
    }
    
    ### save input for reference
    fwrite(regress, file.path(FILEPATH, "inputs_for_nbreg_COD.csv"), row.names=FALSE)
    
  } else {
    
    regress <- merge(regress, locations[,.(location_id, super_region_id, region_id, ihme_loc_id, super_region_name)], by="location_id")
    regress[, country := substr(ihme_loc_id, 1, 3)][, ihme_loc_id := NULL]
    
    # save input 
    fwrite(regress, file.path(FILEPATH, "inputs_for_nbreg_COD.csv"), row.names=FALSE)
    
    if(fatal_fit=="normal"){
      tic()
      GLM <- MASS::glmer.nb(deaths ~ DTP3_coverage_prop + age_group_id + offset(log(envelope)) + (1|super_region_name), 
                            data=regress, verbose = TRUE)
      toc()
    } else if(fatal_fit=="add_HAQI") {
      tic()
      GLM <- glmer.nb(deaths ~ DTP3_coverage_prop + HAQI + age_group_id + offset(log(envelope)) + (1|country), 
                      data=regress, verbose=TRUE)
      toc()
    }
    
  }
  
  # save log
  capture.output(summary(GLM), file = file.path(FILEPATH.logs, "log_deaths_nbreg.txt"), type="output")
  saveRDS(GLM, file = file.path(FILEPATH.logs, "log_deaths_nbreg.rds"))
  message("GLM file saved")
  #***********************************************************************************************************************
  
  #----DRAWS--------------------------------------------------------------------------------------------------------------
  ### set random seed
  set.seed(0311)
  
  ### prep prediction dataset
  pred_death <- merge(pop_env, covar, by=c("year_id", "location_id"), all.x=TRUE) 
  N <- nrow(pred_death)
  
  ### 1000 draws for uncertainty
  # coefficient matrix
  
  if(fatal_res==FALSE){
    
    coefmat <- c(coef(GLM))
    names(coefmat)[1] <- "constant"
    names(coefmat) <- paste("b", names(coefmat), sep = "_")
    
    if(fatal_fit=="add_HAQI" | fatal_fit=="normal"){
      pred_death[, age_group_id:= as.factor(age_group_id)]
      pred_death[, mean_death_pred := predict(GLM, newdata=pred_death, allow.new.levels=TRUE, type="response")]
      fwrite(pred_death, file=file.path(FILEPATH, "mean_death_predictions.csv")) 
      
      
    } else if(fatal_fit=="sr_FE") {
      # merge on SR info and convert class so can get point predictions
      pred_death <- merge(pred_death, locations[,.(location_id, super_region_id)], by="location_id")
      pred_death[, super_region_id := as.factor(super_region_id)]
      pred_death[, age_group_id:= as.factor(age_group_id)]
      pred_death[, mean_death_pred := predict(GLM, newdata=pred_death, allow.new.levels=TRUE, type="response")]
      fwrite(pred_death, file=file.path(FILEPATH, "mean_death_predictions.csv"))
      # convert super_region_id back to numeric for creating draws
      pred_death[, super_region_id := as.numeric(as.character(super_region_id))]
    }
    
    # convert back to numeric and proceed with calculating draws
    pred_death[, age_group_id := as.numeric(as.character(age_group_id))]
    
    
  } else if (fatal_res==TRUE){
    
    coefmat <- fixef(GLM)
    names(coefmat)[1] <- "constant"
    names(coefmat) <- paste("b", names(coefmat), sep = "_")
    
    country_res <- data.table(country = rownames(ranef(GLM)[["country"]]), country_re = unlist(ranef(GLM)[["country"]]))
    region_res <- data.table(region_id = as.numeric(rownames(ranef(GLM)[["region_id"]])), region_re = unlist(ranef(GLM)[["region_id"]]))
    #sr_res <- data.table(super_region_id = as.numeric(rownames(ranef(GLM)[["super_region_id"]])), sr_re = unlist(ranef(GLM)[["super_region_id"]]))
    sr_res <- data.table(super_region_name = (rownames(ranef(GLM)[["super_region_name"]])), sr_re = unlist(ranef(GLM)[["super_region_name"]]))
    
    # merge on location info so can predict out using REs
    pred_death <- merge(pred_death, locations[,.(location_id, ihme_loc_id, super_region_id, region_id, super_region_name)], by="location_id")
    pred_death[, country := substr(ihme_loc_id,1,3)]
    
    # merge on res
    pred_death <- merge(pred_death, country_res, by="country", all.x=TRUE)
    pred_death <- merge(pred_death, region_res, by="region_id", all.x=TRUE)
    #pred_death <- merge(pred_death, sr_res, by="super_region_id")
    pred_death <- merge(pred_death, sr_res, by="super_region_name")
    
    # set RE to 0 for locations with no RE (ie not in input data)
    pred_death[is.na(country_re), country_re := 0]
    pred_death[is.na(region_re), region_re := 0]
    pred_death[is.na(sr_re), region_re := 0]
    
    # convert classes and predict out mean from model
    pred_death[, age_group_id:= as.factor(age_group_id)]
    pred_death[, super_region_id := as.factor(super_region_id)]
    pred_death[, mean_death_pred := predict(GLM, newdata=pred_death, allow.new.levels=TRUE, type="response")]
    fwrite(pred_death, file=file.path(FILEPATH, "mean_death_predictions_w_re.csv"))
    
    # temporary stop - compare models fit with different weights before running full script.
    stop()
    # convert back to numeric and proceed with calculating draws
    pred_death[, age_group_id := as.numeric(as.character(age_group_id))]
  }
  
  #get coefficient matrix
  if (gbd_round < 7) {
    
    coefmat <- matrix(unlist(coefmat), ncol=14, byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coefmat)))))
    
  } else if (gbd_round >= 7){
    if(fatal_fit=="normal"){
      coefmat <- matrix(unlist(coefmat), ncol=16, byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coefmat)))))
    } else if(fatal_fit=="add_HAQI"){
      coefmat <- matrix(unlist(coefmat), ncol=17, byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coefmat)))))
    } else if (fatal_fit=="sr_FE"){
      #ELBR: increase ncol by 2 because new u5 age grps in gbd 2020, october increase by 6 more because added super_region fixed effects to model
      coefmat <- matrix(unlist(coefmat), ncol=22, byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coefmat)))))
    }
    
  }
  
  ### covariance matrix
  vcovmat <- vcov(GLM)
  
  # create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
  betas <- t(rmvnorm(n=length(draw_nums_gbd), mean=coefmat, sigma=as.matrix(vcovmat))) %>% data.table 
  colnames(betas) <- paste0("beta_draw_", draw_nums_gbd)
  
  if(gbd_round < 7){
    betas[, age_group_id := c(NA, NA, 5:16)] #2 NAs because excluding the vacc coverage beta and intercept from column assignment
  } else if (gbd_round >= 7) {
    #betas[, age_group_id := c(NA, NA, 389, 238, 34, 6:16)] #NAs because excluding the vacc coverage beta and intercept
    betas[, variable:= names(coefficients(GLM))]
    age_group_betas <- betas[variable %like% "age_group_id", !"variable"]
    colnames(age_group_betas) <- paste0("age_beta_draw_", draw_nums_gbd)
    age_group_betas[, age_group_id := c(389, 238, 34, 6:16)] 
    
    # if have SR FE's need to pull out the draws of those additional betas
    if(fatal_fit=="sr_FE"){
      sr_betas <- betas[variable %like% "super_region_id", !"variable"]
      colnames(sr_betas) <- paste0("sr_beta_draw_", draw_nums_gbd)
      sr_betas[, super_region_id := c(31, 64, 103, 137, 158, 166)] 
    }
    
  }
  
  # merge together predictions with age draws by age_group_id and super_region_draws by super_region_id & fill in betas for reference age group and reference super region with 0s
  pred_death <- merge(pred_death, age_group_betas[!is.na(age_group_id), ], by="age_group_id", all.x=TRUE)
  pred_death[age_group_id == 388 & is.na(age_beta_draw_0), paste0("age_beta_draw_", draw_nums_gbd) := 0]   #ELBR: not sure why this line is here, not there for varicella... actually accounted for lower in varicella, try to make this code match that later
  
  if(fatal_fit=="sr_FE"){
    # merge on SR FE beta draws
    pred_death <- merge(pred_death, sr_betas[!is.na(super_region_id), ], by="super_region_id", all.x=TRUE)
    pred_death[super_region_id==4 & is.na(sr_beta_draw_0), paste0("sr_beta_draw_", draw_nums_gbd) := 0]
  }
  
  ### create draws of disperion parameter
  if(fatal_res == TRUE){
    # different functions to fit w or w/o REs so different syntax to extract the dispersion parameter
    alphas <- 1 / exp(rnorm(1000, mean=getME(GLM, "glmer.nb.theta"), sd=attr(theta.ml(regress$deaths, fitted(GLM), limit=30), "SE")))
  } else {
    alphas <- 1 / exp(rnorm(1000, mean=GLM$theta, sd=GLM$SE.theta))
  }
  
  if (GAMMA_EPSILON == "with") {
    lapply(draw_nums_gbd, function (draw) {
      
      # set intercept and coverage betas
      b0 <- betas[1, ][[paste0("beta_draw_", draw)]] # intercept
      b1 <- betas[2, ][[paste0("beta_draw_", draw)]] # coverage beta
      
      # if have HAQI covariate, need to get the draws beta
      if(fatal_fit=="add_HAQI") b2 <- betas[variable=="HAQI", ][[paste0("beta_draw_", draw)]]
      # age fixed effects
      age.fe <- pred_death[[paste0("age_beta_draw_", draw)]]
      # get draws of SR FE betas if fitting those
      if(fatal_fit=="sr_FE") sr.fe <- pred_death[[paste0("sr_beta_draw_", draw)]]
      
      alpha <- alphas[draw + 1]
      
      # calculate 1000 draws
      if(fatal_res == FALSE){
        if(fatal_fit=="sr_FE"){
          pred_death[, paste0("death_draw_", draw) := rgamma( N, scale=(alpha * exp( b0 + (b1 * DTP3_coverage_prop) + age.fe + sr.fe) * envelope ),
                                                              shape=(1 / alpha) ) ]
        } else if (fatal_fit=="add_HAQI"){
          pred_death[, paste0("death_draw_", draw) := rgamma( N, scale=(alpha * exp( b0 + (b1 * DTP3_coverage_prop) + (b2 * HAQI) + age.fe) * envelope ),
                                                              shape=(1 / alpha) ) ]
        } else if (fatal_fit=="normal"){
          pred_death[, paste0("death_draw_", draw) := rgamma( N, scale=(alpha * exp( b0 + (b1 * DTP3_coverage_prop) + age.fe) * envelope ),
                                                              shape=(1 / alpha) ) ]
        }
        
      } else if (fatal_res == TRUE){
        pred_death[, paste0("death_draw_", draw) := rgamma( N, scale=(alpha * exp( b0 + (b1 * DTP3_coverage_prop) + age.fe + country_re + region_re) * envelope ),
                                                            shape=(1 / alpha) ) ]
      }
      
    })
    
  } else if (GAMMA_EPSILON == "without") {
    lapply(draw_nums_gbd, function (draw) {
      # set betas
      b0 <- betas[1, ][[paste0("beta_draw_", draw)]]
      b1 <- betas[2, ][[paste0("beta_draw_", draw)]]
      # age fixed effects
      age.fe <- pred_death[[paste0("beta_draw_", draw)]]
      alpha <- alphas[draw + 1]
      # calculate 1000 draws
      pred_death[, paste0("death_draw_", draw) := exp( b0 + (b1 * DTP3_coverage_prop) + age.fe ) * envelope ]
    })
  }
  
  ### save results
  pred_death_save <- pred_death[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("death_draw_", draw_nums_gbd)), with=FALSE]
  if (WRITE_FILES == "yes") {
    fwrite(pred_death_save, file.path(FILEPATH, paste0("01_death_predictions_from_model.csv")), row.names=FALSE)
  }
  message("death prediction file saved")
  #***********************************************************************************************************************
  
  if(!is.null(old_custom_vers)){
    
    # read in old results and overwrite latest run results in date version folder
    pred_death_save <- fread(paste0(FILEPATH, "/01_death_predictions_from_model.csv"))
    if (WRITE_FILES == "yes") {
      fwrite(pred_death_save, file.path(FILEPATH, paste0("01_death_predictions_from_model.csv")), row.names=FALSE)
      
      pred_death <- fread(paste0(FILEPATH, old_custom_vers, "/mean_death_predictions.csv"))
      fwrite(pred_death, file=file.path(FILEPATH, "mean_death_predictions.csv"))
      
      # delete files from fitting the model above because don't acurately reflect the model used to make the predictions because read in predictions from older model-- should go to old date version folder's model fit files
      unlink(file.path(FILEPATH.logs, "log_deaths_nbreg.rds"))
      unlink(file.path(FILEPATH.logs, "log_deaths_nbreg.txt"))
      unlink(file.path(FILEPATH, "inputs_for_nbreg.csv"))
      
    }
    
    # track which old version was used
    cat(paste0("Using results from previously run model for global locations from custom_version ", old_custom_vers), 
        file=file.path(FILEPATH, "MODEL_DESCRIPTION.txt"), sep = "\n", append = TRUE)
    cat(paste0("Using results from previously run custom model for non-DR locations - date version: ", old_custom_vers),
        file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  }
  
  #----HYBRIDIZE----------------------------------------------------------------------------------------------------------
  
  ### read CODEm results for data-rich countries
  cod_M <- data.table(pandas$read_hdf(file.path("/FILEPATH", male_CODEm_version, paste0("draws/deaths_", "male", ".h5")), key="data"))
  cod_F <- data.table(pandas$read_hdf(file.path("/FILEPATH", female_CODEm_version, paste0("draws/deaths_", "female", ".h5")), key="data"))
  
  # save model version
  cat(paste0("Data-rich CODEm feeder model (males) - model version ", male_CODEm_version), 
      file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  cat(paste0("Data-rich CODEm feeder model (females) - model version ", female_CODEm_version), 
      file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  
  # combine M/F CODEm results
  cod_DR <- rbind(cod_M, cod_F, fill=TRUE)
  cod_DR <- cod_DR[, draw_cols_upload, with=FALSE]
  
  rm(cod_M, cod_F)
  gc(T)
  
  # hybridize data-rich and custom models
  data_rich <- unique(cod_DR$location_id)
  deaths_glb <- pred_death_save[!location_id %in% data_rich, ]
  colnames(deaths_glb) <- draw_cols_upload
  deaths_hyb <- rbind(deaths_glb, cod_DR)
  
  # keep only needed age groups
  deaths_hyb <- deaths_hyb[age_group_id %in% c(age_start:age_end, 388, 389, 238, 34), ]
  pred_death_save <- copy(deaths_hyb) 
  
  if (WRITE_FILES == "yes") {
    fwrite(pred_death_save, file.path(FILEPATH, paste0("02_hybridized_death_predictions_from_model.csv")), row.names=FALSE)
  }
  message("hybridized death prediction file saved")
  #***********************************************************************************************************************
  
  ########################################################################################################################
  ##### PART TWO: FORMAT FOR CODCORRECT ##################################################################################
  ########################################################################################################################
  
  #----SAVE RESULTS-------------------------------------------------------------------------------------------------------
  ### save to share directory for upload
  colnames(pred_death_save) <- draw_cols_upload
  pred_death_save[, measure_id := 1]
  
  lapply(unique(pred_death_save$location_id), function(x) write.csv(pred_death_save[location_id==x, ], file.path(FILEPATH, paste0(x, ".csv")), row.names=FALSE))
  
  print(paste0("death draws saved in ", FILEPATH))
  
  ### save_results 
  
  job <- paste0("sbatch -J s_cod_", acause, " --mem 100G -c 5 -C archive -t 8:00:00 -A proj_cov_vpd -p all.q -o /FILEPATH/",
                username, "/%x.o%j",
                " /FILEPATH/execRscript.sh -s ", 
                paste0("/FILEPATH/", username, "/FILEPATH/save_results_wrapper.r"),
                " --year_ids ", paste(unique(pred_death_save$year_id), collapse=","),
                " --type cod",
                " --me_id ", cause_id,
                " --input_directory ", FILEPATH,
                " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
                " --best ", mark_model_best,
                " --release_id ", release_id)
  
  print(job)
  system(job) 
  #*********************************************************************************************************************** 
  
}


#######################################
if (CALCULATE_NONFATAL == "yes") {
  #######################################
  
  if (FauxCorrect) {
    ### draw numbers
    draw_nums_gbd    <- 0:99
    draw_cols_upload <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", draw_nums_gbd))
  }
  ########################################################################################################################
  ##### PART THREE: DisMod NONFATAL RESULTS ##############################################################################
  ########################################################################################################################
  
  #----GET CFR------------------------------------------------------------------------------------------------------------
  
  # Get CFR draws: default is just DISMOD modeling years
  if(interpolate_cfr == FALSE){
    
    cfr_dismod <- get_draws(gbd_id_type="modelable_entity_id",
                            gbd_id=2834,
                            # status="best",
                            version_id = cfr_model_version,
                            source="epi",
                            release_id = release_id)
    
  } else if (interpolate_cfr == TRUE) {
    
    cfr_dismod <- data.table() # create empty data frame
    years <- seq(1990, 2024) # indicate years to loop through, this makes the process faster
    
    # Loop through each year to get interpolated CFR results
    for(year in years){
      print(paste0("Getting CFR results for ", year))
      
      cfr_draws <- interpolate(
        gbd_id_type = "modelable_entity_id",
        gbd_id = 2834, #diphtheria dismod cfr model
        source = "epi",
        measure_id = 18, #proportion
        location_id = "all",
        sex_id = c(1,2),
        release_id = release_id,
        reporting_year_start = 1990,
        reporting_year_end = 2024, 
        version_id = cfr_model_version)
      
      cfr_dismod <- rbind(cfr_dismod, cfr_draws)
      
    }
  }
  
  message("CFR model draws imported")
  
  # save cfr model version metadata
  cat(paste0("Case fatality ratio DisMod model (me_id 2834) - model run ", unique(cfr_dismod$model_version_id)),
      file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  
  # remove excess columns
  cfr_dismod <- cfr_dismod[, draw_cols_upload, with=FALSE]
  
  # fix column names
  colnames(cfr_dismod) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("cfr_draw_", draw_nums_gbd))
  #***********************************************************************************************************************
  
  
  ########################################################################################################################
  ##### PART FOUR: MORTALITY TO NONFATAL #################################################################################
  ########################################################################################################################
  
  
  #----PREP---------------------------------------------------------------------------------------------------------------
  ### Load duration draw data 
  if (FauxCorrect) {
    ### Need to downsample appropriately from 1000 to 100
    custom_file <- paste0(j_root, "FILEPATH/duration_draws.csv")
    # Source downsample wrapper
    source_python("/FILEPATH/test_dsample_py.py")
    # Call python function written in sourced .py
    duration <- downsample_draws(custom_file, 100L, cause_sub="A05.a")
    colnames(duration) <- c("cause", paste0("dur_draw_", draw_nums_gbd))
  } else {
    duration <- read.csv(file.path(j_root, "FILEPATH/duration_draws.csv")) %>% data.table
    duration <- duration[cause=="A05.a", ]
    colnames(duration) <- c("cause", paste0("dur_draw_", draw_nums_gbd))
  }
  
  # Pull in death draws from either codem, cod correct, or death draws made in fatal portion of the script above
  if (fatal_source == "CC") {
    if(scale_cod_correct == FALSE){
      deaths <- get_draws("cause_id", 
                          cause_id, 
                          "codcorrect", 
                          location_id = pop_locs, 
                          year_id = unique(cfr_dismod$year_id), 
                          measure_id = 1, 
                          version_id = codcorrect_version,
                          release_id = release_id)
      
    } else if (scale_cod_correct == TRUE){
      deaths <- fread(paste0("/FILEPATH.csv"))
      deaths <- dplyr::select(deaths, -contains("draw")) 
      colnames(deaths) <- sub("^mod_", "draw_", colnames(deaths))
    }
    
    print("Pulled from CodCorrect and moving on!")
    
  } else if (fatal_source == "CODEm" & exists("pred_death_save")){
    deaths <- copy(pred_death_save) # if using custom CoD model estimates and just ran fatal estimation (ie pred_death_save exists), use those results rather than pulling from database because new fatal modeled results will not have had time to upload yet 
    print("Copied custom CoD model raw estimates from this model run and moving on!")
    
  } else if (fatal_source == "CODEm" & !exists("pred_death_save")){
    
    deaths <- data.table()
    
    for(mod in codem_models){
      draws_temp <- get_draws("cause_id", 
                              cause_id, 
                              "codem", 
                              location_id=pop_locs, 
                              year_id=unique(cfr_dismod$year_id), 
                              # sex_id=c(1,2),
                              measure_id=1, 
                              version_id = mod, 
                              # status = codem_version, 
                              release_id = release_id, 
                              num_workers=10)
      deaths <- rbind(deaths, draws_temp)
      message(paste0("Got draws for model version ", mod))
    }
    
    print("Pulled from custom CoDem model raw estimates and moving on!")
    
  }
  
  # save model version of Codcorrect or CoD model estimates
  if (!is.null(unique(deaths$version_id))) vers <- unique(deaths$version_id) else vers <- codem_models
  
  if(fatal_source == "CODEm" & !(exists("pred_death_save"))){
    # cat(paste0("Custom fatal estimates pulled from CODEm upload - date pulled ", vers, " release_id ", release_id, "status ", codem_version),
    cat(paste0("Custom fatal estimates pulled from CODEm upload - date pulled ", vers, " release_id ", release_id, "status ", codem_models),  
        file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
    
  } else if (fatal_source == "CODEm" & exists("pred_death_save")){
    cat(paste0("Using fatal estimates calculated in this model run, "), custom_version)
    
  } else {
    cat(paste0("CodCorrect results - output version ", vers),
        file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  }
  
  if(CALCULATE_FATAL == "yes"){
    if(scale_cod_correct == TRUE){
      cat(paste0("CodCorrect ", vers, " has been rescaled!"),
          file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
    }
  }
  
  # remove excess columns
  deaths <- deaths[, draw_cols_upload, with=FALSE]
  colnames(deaths) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("death_draw_", draw_nums_gbd))
  
  # merge deaths and CFR draws
  predict_nonfatal <- merge(cfr_dismod, deaths, by=c("location_id", "year_id", "age_group_id", "sex_id"))
  
  # merge with population and mortality envelope data 
  predict_nonfatal <- merge(predict_nonfatal, pop_env, by=c("location_id", "year_id", "age_group_id", "sex_id")) %>% data.table
  
  # Merge with duration data draws
  predict_nonfatal <- merge(predict_nonfatal[, cause := "A05.a"], duration, by="cause", all.x=TRUE)
  
  # save non fatal prediction file
  if (WRITE_FILES == "yes") {
    fwrite(predict_nonfatal, file.path(FILEPATH, paste0("02_non_fatal_predictions.csv")), row.names=FALSE)
    message("Non fatal predictions have been saved!")
  }
  
  # Remove files and clear memory 
  rm(envelope)
  rm(population)
  rm(pop_env)
  gc(T)
  
  #----RUN CASE NOTIFICATIONS ADJUSTMENT------------------------------------------
  
  if(run_cn_adjustment){
    
    # Read JRF [for now from flat file, fix bundle issues and then import from bundle]
    jrf <- fread("/FILEPATH/diphtheria_jrf_2023.csv")
    setDT(jrf)
    
    # Rename columns
    setnames(jrf, c("Period", "SpatialDimValueCode", "FactValueNumeric"), c("year_id", "ihme_loc_id", "cases"))
    
    # Keep desire columns
    jrf <- jrf[, .(year_id, ihme_loc_id, cases)]
    
    # Merge with locations
    jrf <- merge(jrf, locations[, .(ihme_loc_id, location_name, location_id)], by = "ihme_loc_id", all.x = T)
    
    # Keep only location years in the prediction data frame
    jfr <- jrf[location_id %in% predict_nonfatal$location_id & year_id %in% predict_nonfatal$year_id, ]
    # NB above line means we are only adjusting adjustment to GBD prediction years! 
    
    # copy predict_nonfatal in preparation for adjustment
    readjust <- copy(predict_nonfatal)
    
    # Calculate incidence rate as deaths/population/cfr
    lapply(draw_nums_gbd, function (i) {
      readjust[, paste0("inc_draw_", i) := ((get(paste0("death_draw_", i)) / population) / get(paste0("cfr_draw_", i)))]
    })
    
    # Calculate row means of incidence rate draws
    inc_cols_to_average <- grep("inc_draw", names(readjust))
    readjust <- readjust[, mean_inc_rate := rowMeans(.SD), .SDcols = inc_cols_to_average]
    
    # Calculate case numbers by multiplying incidence rate * population
    readjust[, mean_predicted_cases := mean_inc_rate * population]
    
    # Calculate mean deaths
    death_cols_to_average <- grep("death_draw", names(readjust))
    readjust <- readjust[, mean_deaths := rowMeans(.SD), .SDcols = death_cols_to_average] 
    #this is already a count so no need to multiple by population
    
    #Keep columns to aggregate
    readjust_2 <- readjust[, .(location_id, year_id, mean_inc_rate, mean_predicted_cases, mean_deaths, population)]
    
    # Sum total cases and deaths for location and year (ie drop sex and age specificity in order to merge with WHO case notifications)
    readjust_2[, total_predicted_cases := sum(mean_predicted_cases), by = c("location_id", "year_id")]
    readjust_2[, total_predicted_deaths := sum(mean_deaths), by = c("location_id", "year_id")]
    
    # Keep aggregated rows
    readjust_2 <- readjust_2[, .(location_id, year_id, total_predicted_cases, total_predicted_deaths)] %>% unique
    
    # Merge case notifications with incidence predictions
    merged_cases <- merge(jrf[year_id %in% unique(readjust_2$year_id), .(location_id, location_name, year_id, cases)],
                          readjust_2, by = c("location_id", "year_id"), all.x = T)
    
    # Determine whether notified cases exceed predicted cases
    merged_cases[, exceed := ifelse(cases > total_predicted_cases, 1, 0)]
    
    # Subset location-years where notifications exceed predicted cases
    underestimated <- merged_cases[exceed == 1, ]
    under_locs <- unique(underestimated$location_id)
    under_year <- unique(underestimated$year_id)
    
    # Calculate a scalar by dividing notifications by cases from model 
    underestimated[, notif_model_ratio := cases/total_predicted_cases]
    underestimated[, abs_diff := cases - total_predicted_cases]
    
    # Check coding
    if(sum(underestimated$notif_model_ratio <= 1) > 0){
      print("Something is wrong with the math, the ratio must be greater than 1")
    } else {
      print("All ratios are greater than 1, proceed with readjustment")
    }  
    
    # Merge underestimated cases with locations to add ihme loc id variable
    underestimated <- merge(underestimated, locations[, .(location_id, ihme_loc_id)], by = "location_id", all.x = T)
    setnames(underestimated, "cases", "case_notifications")
    
    # Keep a copy of underestimated locations
    fwrite(underestimated, paste0(FILEPATH.logs, "/location_years_for_incidence_floor_adjustment.csv"))
    
    # Merge file to be readjusted with locations to allow merge by ihme_loc_id after removing subnational postfix
    readjust <- merge(readjust, locations[, .(location_id, ihme_loc_id)], by = "location_id", all.x = T)
    
    #remove subnationals post fix to apply the adjustment to all subnationals such that the national will add up
    # [NB this assumes same scalar should be applied to all subnationals for affected location years]
    readjust[, ihme_loc_id := gsub("_.*", "", ihme_loc_id)]
    
    # Merge underestimated location years with all predictions
    readjust <- merge(readjust, underestimated[, .(ihme_loc_id, notif_model_ratio, year_id)], 
                      by = c("ihme_loc_id", "year_id"), all.x = T)
    
    # Convert the NA ratios for location years where readjustment is not needed to 1
    readjust[, notif_model_ratio := ifelse(is.na(notif_model_ratio), 1, notif_model_ratio)]
    
    # Check that no NAs remain in the data
    if(sum(is.na(readjust)) > 0) {
      print("STOP! There are missing data!")
      stop()
    }else{
      print("Success! There are no missing data")
    }
    
    # Subset incidence columns to correct
    cols_to_correct <- grep("inc_draw", names(readjust), value = TRUE)
    
    # Apply correction factor to incidence draws
    readjust[, (cols_to_correct) := lapply(.SD, function(x) x * notif_model_ratio), .SDcols = cols_to_correct]
    
    # # Recalculate death draws to align with corrected incidence so that deaths/cases == CFR [ie keeping CFR and adjusted cases as constants]
    # Death adjsutment is done as a shock and this part of the code is not run! 
    # lapply(draw_nums_gbd, function (i) {
    # readjust[, paste0("death_draw_", i) := get(paste0("cfr_draw_", i)) * get(paste0("inc_draw_", i)) * population]
    # })
    # message("Death draws adjusted") 
    
    # Calculate prevalence by multiplying incidence * duration
    lapply(draw_nums_gbd, function (i) {
      readjust[, paste0("prev_draw_", i) := get(paste0("inc_draw_", i)) * get(paste0("dur_draw_", i))]
    })
    
    message("Prevalence draws calculated")
    
    # Save readjusted file
    if (WRITE_FILES == "yes") {
      fwrite(readjust, file.path(FILEPATH, paste0("025_non_fatal_predictions_inc_floor_adjusted.csv")), row.names=FALSE)
    }
    message("Adjusted predictions file saved!")
    
    ### Subset data 
    predictions_prev_save <- readjust[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("prev_draw_", draw_nums_gbd)), with=FALSE]
    predictions_inc_save <- readjust[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("inc_draw_", draw_nums_gbd)), with=FALSE]
    # predictions_death_save <- readjust[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("death_draw_", draw_nums_gbd)), with=FALSE]
    
  } else if (run_cn_adjustment == FALSE) {
    # Run code without incidence floor
    
    ### calculate prevalence (mort/cfr*duration) and incidence (prevalence/cfr)
    lapply(draw_nums_gbd, function (i) {
      predict_nonfatal[, paste0("prev_draw_", i) := ( (get(paste0("death_draw_", i)) / population) / get(paste0("cfr_draw_", i)) ) * get(paste0("dur_draw_", i))]
      predict_nonfatal[, paste0("inc_draw_", i) := get(paste0("prev_draw_", i)) / get(paste0("dur_draw_", i))]
    })
    
    ### keep needed columns
    predictions_prev_save <- predict_nonfatal[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("prev_draw_", draw_nums_gbd)), with=FALSE]
    predictions_inc_save <- predict_nonfatal[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("inc_draw_", draw_nums_gbd)), with=FALSE]
  }
  
  # Clear memory
  rm(predict_nonfatal)
  rm(readjust)
  rm(readjust_2)
  gc(T)
  #***********************************************************************************************************************
  
  ########################################################################################################################
  ##### PART FIVE: FORMAT FOR COMO #######################################################################################
  ########################################################################################################################
  
  #----SAVE RESULTS-------------------------------------------------------------------------------------------------------
  ### format prevalence for como, prevalence measure_id==5
  colnames(predictions_prev_save) <- draw_cols_upload
  predictions_prev_save[, measure_id := 5]
  
  ### format incidence for como, incidence measure_id==6
  colnames(predictions_inc_save) <- draw_cols_upload
  predictions_inc_save[, measure_id := 6]
  
  ### Bind prevalence and incidence data and save
  save_nonfatal <- rbind(predictions_prev_save, predictions_inc_save)
  
  if(WRITE_FILES == "yes"){
    fwrite(save_nonfatal, paste0(FILEPATH, "/03_incidence_and_prevalence_predictions.csv"))
  }
  
  # split nonfatal estimates by location in preparation to upload to cluster
  lapply(unique(save_nonfatal$location_id), function(x) fwrite(save_nonfatal[location_id==x, ],
                                                               file.path(cl.version.dir, paste0(x, ".csv")), row.names=FALSE))
  print(paste0("nonfatal estimates saved in ", cl.version.dir))
  
  ### upload results to central database
  job_nf <- paste0("sbatch -J s_epi_", acause, " --mem 125G -c 10 -C archive -t 12:00:00 -A proj_cov_vpd -p all.q -o /FILEPATH/",
                   username, "/%x.o%j", 
                   " /FILEPATH/execRscript.sh -s ", 
                   paste0("/FILEPATH/", username, "/FILEPATH/save_results_wrapper.r"),
                   " --year_ids ", paste(unique(save_nonfatal$year_id), collapse=","),
                   " --type epi",
                   " --me_id ", me_id,
                   " --input_directory ", cl.version.dir,
                   " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
                   " --best ", mark_model_best,
                   " --bundle_id ", bundle_id,
                   " --xw_id ", xw_id,
                   " --release_id ", release_id)
  
  print(job_nf)
  system(job_nf)
  
  #***********************************************************************************************************************
  
}

