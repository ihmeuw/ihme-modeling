#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:  USERNAME
# Date:    February 2017, modified Dec 2017 for GBD 2017
# Path:    FILEPATH
# Purpose: PART ONE -   Prepare negative binomial regression of diphtheria cases for input into codcorrect
#          PART TWO -   Format for CodCorrect and save results to database
#          PART THREE - Run DisMod model for CFR
#          PART FOUR -  Calculate nonfatal outcomes from mortality
#                       Use mortality to calculate prevalence (prev = mort/cfr*duration)
#                       Calculate incidence from prevalence (inc = prev/duration)
#          PART FIVE -  Format for COMO and save results to database
#***********************************************************************************************************************


########################################################################################################################
##### START-UP #########################################################################################################
########################################################################################################################


#----CONFIG-------------------------------------------------------------------------------------------------------------
### load packages
pacman::p_load(magrittr, foreign, stats, MASS, data.table, rhdf5, plyr, data.table, parallel, dplyr, reticulate, lme4)
library(tictoc, lib.loc = FILEPATH)
source(".../load_packages.R")
load_packages("mvtnorm")

## Pull in Pandas for HDF5 -- new 02.11.19 from Nafis
pandas <- import("pandas")
#***********************************************************************************************************************


#----DIRECTORIES--------------------------------------------------------------------------------------------------------
### set objects
acause    <- "diptheria"
age_start <- 6
age_end   <- 16
cause_id  <- 338
me_id     <- 1421
gbd_round <- 7
year_end  <- if (gbd_round %in% c(3, 4, 5)) gbd_round + 2012 else if (gbd_round==6) 2019 else if (gbd_round==7) 2022

### draw numbers
draw_nums_gbd    <- 0:999
draw_cols_upload <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", draw_nums_gbd))

### make folders on cluster
# CoD
cl.death.dir <- file.path("/share/epi/vpds", acause, "mortality", custom_version, "draws")
if (!dir.exists(cl.death.dir)) dir.create(cl.death.dir, recursive=TRUE)
# nonfatal/epi
cl.version.dir <- file.path("/share/epi/vpds", acause, "nonfatal", custom_version, "draws")
if (!dir.exists(cl.version.dir)) dir.create(cl.version.dir, recursive=TRUE)

### directories
home <- file.path(j_root, "WORK/12_bundle", acause, "00_documentation")
j.version.dir <- file.path(home, "models", custom_version)
j.version.dir.inputs <- file.path(j.version.dir, "model_inputs")
j.version.dir.logs <- file.path(j.version.dir, "model_logs")
if (!dir.exists(j.version.dir.inputs)) dir.create(j.version.dir.inputs, recursive=TRUE)
if (!dir.exists(j.version.dir.logs)) dir.create(j.version.dir.logs, recursive=TRUE)

### save description of model run
# which model components are being uploaded?
if (CALCULATE_NONFATAL=="yes" & CALCULATE_FATAL=="no") add_  <- "NF"
if (CALCULATE_FATAL=="yes" & CALCULATE_NONFATAL=="no") add_  <- "CoD"
if (CALCULATE_NONFATAL=="yes" & CALCULATE_FATAL=="yes") add_ <- "NF and CoD"
# record CODEm data-rich feeder model versions used in CoD hybridization
if (CALCULATE_FATAL=="yes") description <- paste0("DR M ", male_CODEm_version, ", DR F ", female_CODEm_version, ", ", description)
# save full description to model folder
description <- paste0(add_, " - ", description)
cat(description, file=file.path(j.version.dir, "MODEL_DESCRIPTION.txt"))
#***********************************************************************************************************************


#----FUNCTIONS----------------------------------------------------------------------------------------------------------
### load custom functions
source("FILEPATH/sql_query.R")
source("FALEPATH/read_hdf5_table.R")

### load shared functions
source("FILEPATH/get_population.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_envelope.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_cod_data.R")
#***********************************************************************************************************************


########################################################################################################################
##### PART ONE: COD NEG BIN REGRESSION #################################################################################
########################################################################################################################


#----PREP---------------------------------------------------------------------------------------------------------------
### get location data
locations <- get_location_metadata(gbd_round_id=gbd_round, location_set_id=22, decomp_step = decomp_step)[, ## id=22 is from covariates team, id=9 is from epi
                                                                               .(location_id, ihme_loc_id, location_name, location_ascii_name, region_id, super_region_id, level, location_type, parent_id, super_region_name, sort_order)]
pop_locs <- unique(locations[level >= 3, location_id])
standard_locations <- get_location_metadata(gbd_round_id=gbd_round, location_set_id=101, decomp_step = decomp_step)$location_id %>% unique

### pop_env
# get envelope
if (gbd_round == 7){
  envelope <- get_envelope(location_id=pop_locs, sex_id=1:2, age_group_id=c(age_start:age_end, 388, 389, 238, 34), with_hiv=0, year_id=1980:year_end, gbd_round_id=gbd_round, decomp_step=fatal_decomp_step)[,
                                                                                                                                                                                    .(location_id, year_id, age_group_id, sex_id, mean, run_id)]
} else if (gbd_round==6) {
  envelope <- get_envelope(location_id=pop_locs, sex_id=1:2, age_group_id=c(age_start:age_end), with_hiv=0, year_id=1980:year_end, gbd_round_id=gbd_round, decomp_step=decomp_step)[,
                                                                                                                                                                                    .(location_id, year_id, age_group_id, sex_id, mean, run_id)]
} else { envelope <- get_envelope(location_id=pop_locs, sex_id=1:2, age_group_id=c(age_start:age_end), with_hiv=0, year_id=1980:year_end)[,
                                                                                                                                          .(location_id, year_id, age_group_id, sex_id, mean, run_id)]
}
setnames(envelope, "mean", "envelope")
# get population data
if (gbd_round ==7){
  population <- get_population(location_id=pop_locs, year_id=1980:year_end, age_group_id=c(age_start:age_end, 388, 389, 238, 34), sex_id=1:2, gbd_round_id=gbd_round, decomp_step=fatal_decomp_step)[,
                                                                                                                                                                            .(location_id, year_id, age_group_id, sex_id, population, run_id)]
} else if (gbd_round==6) {
  population <- get_population(location_id=pop_locs, year_id=1980:year_end, age_group_id=c(age_start:age_end), sex_id=1:2, gbd_round_id=gbd_round, decomp_step=decomp_step)[,
                                                                                                                                                                            .(location_id, year_id, age_group_id, sex_id, population, run_id)]
} else { population <- get_population(location_id=pop_locs, year_id=1980:year_end, age_group_id=c(age_start:age_end), sex_id=1:2)[,
                                                                                                                                  .(location_id, year_id, age_group_id, sex_id, population, run_id)]
}
# save model version
cat(paste0("Mortality envelope - model run ", unique(envelope$run_id)),
    file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
cat(paste0("Population - model run ", unique(population$run_id)),
    file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
# bring together
pop_env <- merge(population, envelope, by=c("location_id", "year_id", "age_group_id", "sex_id"))

#######################################
if (CALCULATE_FATAL == "yes") {
#######################################

  ### covariates
  # covariate: DTP3_coverage_prop, covariate_id=32
  if (use_lagged_covs) {
    if(is.null(custom_coverage)){
      covar <- get_covariate_estimates(covariate_id=2308, year_id=1980:year_end, location_id=pop_locs,
                                       gbd_round_id=gbd_round, decomp_step=fatal_decomp_step)[, .(location_id, year_id, mean_value, model_version_id)]
    } else {

      covar <- get_covariate_estimates(covariate_id=2308, year_id=1980:year_end, location_id=pop_locs, model_version_id = custom_coverage,
                                       gbd_round_id=gbd_round, decomp_step=fatal_decomp_step)[, .(location_id, year_id, mean_value, model_version_id)]
    }

  } else {
    if (decomp) {
      if(is.null(custom_coverage)){
        covar <- get_covariate_estimates(covariate_id=32, year_id=1980:year_end, location_id=pop_locs, gbd_round_id=gbd_round,
                                         gbd_round_id=gbd_round, decomp_step=fatal_decomp_step)[, .(location_id, year_id, mean_value, model_version_id)]
      } else {
        covar <- get_covariate_estimates(covariate_id=32, year_id=1980:year_end, location_id=pop_locs, gbd_round_id=gbd_round, model_version_id = custom_coverage,
                                         gbd_round_id=gbd_round, decomp_step=fatal_decomp_step)[, .(location_id, year_id, mean_value, model_version_id)]
      }

    } else {
      covar <- get_covariate_estimates(covariate_id=32, year_id=1980:year_end, location_id=pop_locs,
                                       gbd_round_id=gbd_round)[, .(location_id, year_id, mean_value, model_version_id)]
    }
  }
  setnames(covar, "mean_value",  "DTP3_coverage_prop")

  if (fatal_fit=="add_HAQI") {
    haqi <- get_covariate_estimates(covariate_id=1099, year_id=1980:year_end, location_id=pop_locs,
                                    gbd_round_id=gbd_round, decomp_step=fatal_decomp_step)[, .(location_id, year_id, mean_value, model_version_id)]
    setnames(haqi, c("mean_value", "model_version_id"), c("HAQI", "model_version_id_haqi"))
    covar <- merge(covar, haqi, by=c("location_id", "year_id"))
  }

  ### save covariate versions
  cat(paste0("Covariate DTP3_coverage_prop (CoD) - model version ", unique(covar$model_version_id)),
      file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  if (fatal_fit=="add_HAQI") {
    cat(paste0("Covariate HAQI (CoD) - model version ", unique(haqi$model_version_id_haqi)),
        file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  }

  ### raw
  # get raw data
  if (decomp) {
    raw <- get_cod_data(cause_id=cause_id, gbd_round_id=gbd_round, decomp_step=fatal_decomp_step) %>%
      setnames(., "year", "year_id") %>% .[, .(nid, location_id, location_name, year_id, age_group_id, sex, cf_corr, sample_size, description)] %>% setnames(., "sex", "sex_id")
  } else {
    raw <- get_cod_data(cause_id=cause_id, gbd_round_id=gbd_round) %>%
      setnames(., "year", "year_id") %>% .[acause=="diptheria", .(nid, location_id, location_name, year_id, age_group_id, sex, cf_corr, sample_size, description)] %>% setnames(., "sex", "sex_id")
  }
  if(gbd_round >= 7){
    raw <- raw[!is.na(cf_corr) & year_id >= 1980 & sample_size != 0 & age_group_id %in% c(age_start:age_end, 388, 389, 238, 34), ]
  } else if (gbd_round < 7) {
    raw <- raw[!is.na(cf_corr) & year_id >= 1980 & sample_size != 0 & age_group_id %in% c(age_start:age_end), ]
  }
  # save model version
  cat(paste0("CoD data - version ", unique(raw$description)),
      file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  # add on mortality envelope
  raw <- merge(raw, pop_env, by=c("location_id", "year_id", "age_group_id", "sex_id"), all.x=TRUE)
  # calculate death counts
  raw[, deaths := cf_corr * envelope]

  ### round to whole number (counts!)
  raw[deaths < 0.5, deaths := 0]

  ### inform model with national data only
  raw <- raw[location_id %in% locations[level==3, location_id], ]

  ### drop outliers (cf greather than 99th percentile)
  cf_999 <- quantile(raw$cf_corr, 0.999)
  raw <- raw[cf_corr <= cf_999, ]

  ### make ages into levels
  raw[, age_group_id := factor(age_group_id, levels=c(388, 389, 238, 34, 6:16))]

  ### bring together variables for regression
  regress <- merge(raw, covar, by=c("location_id", "year_id"))
  regress <- regress[location_id %in% standard_locations] # should not drop anything because standard locations drops some subnationals, but we have already subset to only national locations

  ### save file for reference
  fwrite(regress, file.path(j.version.dir.inputs, "inputs_for_nbreg_COD.csv"), row.names=FALSE)
  #***********************************************************************************************************************


  #----NEG BIN MODEL------------------------------------------------------------------------------------------------------
  ### save input data and run negative binomial regression
  if(fatal_res == FALSE){

    if(weights == "sample_size"){

      GLM <- glm.nb(deaths ~ DTP3_coverage_prop + age_group_id + offset(log(envelope)) + super_region_id, data=regress, weights=sample_size)

    } else if (weights == "data_star") {

      ## assign weights to locations by star rating, use country files because regress only contains national level data
      weight_table <- data.table(stars = c(1, 2, 3, 4, 5), weight = c(0.25, 0.25, 0.25, 1, 1))
      location_stars <- rbind(fread(file.path(j_root, ".../locations_four_plus_stars.csv")),
                         fread(file.path(j_root, ".../countries_less_than_four_stars.csv")))
      location_stars_weights <- merge(location_stars, weight_table, by="stars")

      regress <- merge(regress, location_stars_weights[,.(location_id, stars, weight)], by="location_id")

      GLM <- glm.nb(deaths ~ DTP3_coverage_prop + age_group_id + offset(log(envelope)) + super_region_id, data=regress, weights = weight)

    } else if (weights == "none") {

      if(fatal_fit=="sr_FE"){
        regress <- merge(regress, locations[,.(location_id, super_region_id)], by="location_id")
        regress[, super_region_id := as.factor(super_region_id)]
        GLM <- glm.nb(deaths ~ DTP3_coverage_prop + age_group_id + offset(log(envelope)) + super_region_id, data=regress)

      } else if (fatal_fit=="add_HAQI"){

        GLM <- glm.nb(deaths ~ DTP3_coverage_prop + HAQI + age_group_id + offset(log(envelope)), data=regress)

      } else if (fatal_fit=="normal"){

        GLM <- glm.nb(deaths ~ DTP3_coverage_prop + age_group_id + offset(log(envelope)), data=regress)

      }
    }

    ### save input for reference
    fwrite(regress, file.path(j.version.dir.inputs, "inputs_for_nbreg_COD.csv"), row.names=FALSE)

  } else {

    regress <- merge(regress, locations[,.(location_id, super_region_id, region_id, ihme_loc_id, super_region_name)], by="location_id")
    regress[, country := substr(ihme_loc_id, 1, 3)][, ihme_loc_id := NULL]

    # save input
    fwrite(regress, file.path(j.version.dir.inputs, "inputs_for_nbreg_COD.csv"), row.names=FALSE)

    if(fatal_fit=="normal"){
      tic()
      GLM <- glmer.nb(deaths ~ DTP3_coverage_prop + age_group_id + offset(log(envelope)) + (1|super_region_name),
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
  capture.output(summary(GLM), file = file.path(j.version.dir.logs, "log_deaths_nbreg.txt"), type="output")
  saveRDS(GLM, file = file.path(j.version.dir.logs, "log_deaths_nbreg.rds"))
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
      fwrite(pred_death, file=file.path(j.version.dir, "mean_death_predictions.csv"))
    } else if(fatal_fit=="sr_FE") {
      # merge on SR info and convert class so can get point predictions
      pred_death <- merge(pred_death, locations[,.(location_id, super_region_id)], by="location_id")
      pred_death[, super_region_id := as.factor(super_region_id)]
      pred_death[, age_group_id:= as.factor(age_group_id)]
      pred_death[, mean_death_pred := predict(GLM, newdata=pred_death, allow.new.levels=TRUE, type="response")]
      fwrite(pred_death, file=file.path(j.version.dir, "mean_death_predictions.csv"))
      # convert sr_id back to numeric for creating draws
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
    sr_res <- data.table(super_region_name = (rownames(ranef(GLM)[["super_region_name"]])), sr_re = unlist(ranef(GLM)[["super_region_name"]]))

    # merge on location info so can predict out using REs
    pred_death <- merge(pred_death, locations[,.(location_id, ihme_loc_id, super_region_id, region_id, super_region_name)], by="location_id")
    pred_death[, country := substr(ihme_loc_id,1,3)]

    # merge on res
    pred_death <- merge(pred_death, country_res, by="country", all.x=TRUE)
    pred_death <- merge(pred_death, region_res, by="region_id", all.x=TRUE)
    pred_death <- merge(pred_death, sr_res, by="super_region_name")

    # set RE to 0 for locations with no RE (ie not in input data)
    pred_death[is.na(country_re), country_re := 0]
    pred_death[is.na(region_re), region_re := 0]
    pred_death[is.na(sr_re), region_re := 0]

    # convert classes and predict out mean from model
    pred_death[, age_group_id:= as.factor(age_group_id)]
    pred_death[, super_region_id := as.factor(super_region_id)]
    pred_death[, mean_death_pred := predict(GLM, newdata=pred_death, allow.new.levels=TRUE, type="response")]
    fwrite(pred_death, file=file.path(j.version.dir, "mean_death_predictions_w_re.csv"))

    # temporary stop - compare models fit with different weights before running full script.
    stop()
    # convert back to numeric and proceed with calculating draws
    pred_death[, age_group_id := as.numeric(as.character(age_group_id))]
  }

  if (gbd_round < 7) {

    coefmat <- matrix(unlist(coefmat), ncol=14, byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coefmat)))))

  } else if (gbd_round >= 7){
    if(fatal_fit=="normal"){
      coefmat <- matrix(unlist(coefmat), ncol=16, byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coefmat)))))
    } else if(fatal_fit=="add_HAQI"){
      coefmat <- matrix(unlist(coefmat), ncol=17, byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coefmat)))))
    } else if (fatal_fit=="sr_FE"){
      #increase ncol by 2 because new u5 age grps in gbd 2020, increase by 6 more because added super_region fixed effects to model
      coefmat <- matrix(unlist(coefmat), ncol=22, byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coefmat)))))
    }


  }

  ### covariance matrix
  vcovmat <- vcov(GLM)
  # create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
  betas <- t(rmvnorm(n=length(draw_nums_gbd), mean=coefmat, sigma=as.matrix(vcovmat))) %>% data.table #ELBR: had to add as.matrix because was getting error about vcovmat being dpoMatrix
  colnames(betas) <- paste0("beta_draw_", draw_nums_gbd)
  if(gbd_round < 7){
    betas[, age_group_id := c(NA, NA, 5:16)] #2 NAs because excluding the vacc coverage beta and intercept from column assignment
  } else if (gbd_round >= 7) {
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
  pred_death[age_group_id==388 & is.na(age_beta_draw_0), paste0("age_beta_draw_", draw_nums_gbd) := 0]

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
    fwrite(pred_death_save, file.path(j.version.dir, paste0("01_death_predictions_from_model.csv")), row.names=FALSE)
  }
  #***********************************************************************************************************************

  #----HYBRIDIZE----------------------------------------------------------------------------------------------------------
  ### read CODEm COD results for data-rich countries

  cod_M <- data.table(pandas$read_hdf(file.path("/FILEPATH/", paste0("draws/deaths_", "male", ".h5")), key="data"))
  cod_F <- data.table(pandas$read_hdf(file.path("/FILEPATH/", paste0("draws/deaths_", "female", ".h5")), key="data"))


  # save model version
  cat(paste0("Data-rich CODEm feeder model (males) - model version ", male_CODEm_version),
      file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  cat(paste0("Data-rich CODEm feeder model (females) - model version ", female_CODEm_version),
      file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

  # combine M/F CODEm results
  cod_DR <- rbind(cod_M, cod_F, fill=TRUE)
  cod_DR <- cod_DR[, draw_cols_upload, with=FALSE]

  # hybridize data-rich and custom models
  data_rich <- unique(cod_DR$location_id)
  deaths_glb <- pred_death_save[!location_id %in% data_rich, ]
  colnames(deaths_glb) <- draw_cols_upload
  deaths_hyb <- rbind(deaths_glb, cod_DR)

  # keep only needed age groups
  deaths_hyb <- deaths_hyb[age_group_id %in% c(age_start:age_end, 388, 389, 238, 34), ]
  pred_death_save <- copy(deaths_hyb)
  #***********************************************************************************************************************


  ########################################################################################################################
  ##### PART TWO: FORMAT FOR CODCORRECT ##################################################################################
  ########################################################################################################################


  #----SAVE RESULTS-------------------------------------------------------------------------------------------------------
  ### save to share directory for upload
  colnames(pred_death_save) <- draw_cols_upload
  pred_death_save[, measure_id := 1]
  lapply(unique(pred_death_save$location_id), function(x) write.csv(pred_death_save[location_id==x, ], file.path(cl.death.dir, paste0(x, ".csv")), row.names=FALSE))
  print(paste0("death draws saved in ", cl.death.dir))

  ### save_results
  job <- paste0("qsub -N s_cod_", acause, " -l m_mem_free=100G -l fthread=5 -l archive -l h_rt=8:00:00 -P proj_cov_vpd -q all.q -o .../sgeoutput/",
                username, " -e /FILEPATH/", username,
                " FILEPATH/execRscript.sh -i FILEPATH/ihme_rstudio_3631.img -s FILEPATH/save_results_wrapper.r",
                #" --args",
                " --type cod",
                " --me_id ", cause_id,
                " --input_directory ", cl.death.dir,
                " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
                " --best ", mark_model_best,
                " --gbd_round ", gbd_round)
  if (decomp) job <- paste0(job, " --decomp_step ", fatal_decomp_step)
  system(job); print(job)
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
  ### read in results from CFR model in DisMod

  # Need to downsample if FauxCorrect run on only 100 draws
  if (decomp & FauxCorrect) {
    cfr_dismod <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=2834, status="best", gbd_round_id=gbd_round, source="epi", decomp_step=decomp_step, downsample = TRUE, n_draws=100)
  } else if (decomp & !FauxCorrect) {

    if (decomp_step=="step4" & gbd_round == 6) {
      cfr_dismod <- gd$get_draws(gbd_id_type="modelable_entity_id", gbd_id=as.integer(2834), version_id=as.integer(475868), gbd_round_id=6, source="epi",
                                 decomp_step="iterative", num_workers=as.integer(10)) %>% data.table
    } else {
      cfr_dismod <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=2834, status="best", gbd_round_id=gbd_round, source="epi", decomp_step=decomp_step)
    }

  } else if (!decomp){
    cfr_dismod <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=2834, status="best", gbd_round_id=gbd_round, source="epi")

  }

  # save model version
  cat(paste0("Case fatality ratio DisMod model (me_id 2834) - model run ", unique(cfr_dismod$model_version_id)),
      file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

  # remove excess columns
  cfr_dismod <- cfr_dismod[, draw_cols_upload, with=FALSE]

  colnames(cfr_dismod) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("cfr_draw_", draw_nums_gbd))
  #***********************************************************************************************************************


  ########################################################################################################################
  ##### PART FOUR: MORTALITY TO NONFATAL #################################################################################
  ########################################################################################################################


  #----PREP---------------------------------------------------------------------------------------------------------------
  ### bring in duration data
  if (FauxCorrect) {
    ### Need to downsample appropriately from 1000 to 100 here, too, and custom outputs so need CC .py function
    custom_file <- paste0(j_root, "FILEPATH/duration_draws.csv")
    # Source downsample wrapper
    source_python("FILEPATH/test_dsample_py.py")
    # Call python function written in sourced .py
    duration <- downsample_draws(custom_file, 100L, cause_sub="A05.a")
    colnames(duration) <- c("cause", paste0("dur_draw_", draw_nums_gbd))
  } else {
    duration <- read.csv(file.path(j_root, "FILEPATH/duration_draws.csv")) %>% data.table
    duration <- duration[cause=="A05.a", ]
    colnames(duration) <- c("cause", paste0("dur_draw_", draw_nums_gbd))
  }

  ### prep death data
  if (decomp & FauxCorrect) {  # Note that version=5 is hardcoded for GBD 2019 Step 2!
    deaths <- get_draws("cause_id", cause_id, "fauxcorrect", location_id=pop_locs, year_id=unique(cfr_dismod$year_id), gbd_round_id=gbd_round,
                        measure_id=1, version=compare_version, decomp_step=fatal_decomp_step)
  } else if (decomp & !FauxCorrect & !codem_estimates) {
    deaths <- get_draws("cause_id", cause_id, "codcorrect", location_id=pop_locs, year_id=unique(cfr_dismod$year_id), gbd_round_id=gbd_round,
                        measure_id=1, version_id = "best", decomp_step=fatal_decomp_step)


  } else if (!decomp){
    deaths <- get_draws("cause_id", cause_id, source = "codcorrect", location_id=pop_locs, year_id=unique(cfr_dismod$year_id), gbd_round_id=gbd_round,
                        measure_id=1, status="latest")
  } else if (decomp & codem_estimates){
    deaths <- get_draws("cause_id", cause_id, "codem", location_id=pop_locs, year_id=unique(cfr_dismod$year_id),
                        gbd_round_id=gbd_round, sex_id=c(1,2),
                        measure_id=1, status=compare_version, decomp_step=fatal_decomp_step, num_workers=8)
    print("AYOOO pulled from custom CoD model raw estimates and moving on!")
  }

  # save model version
  if (!is.null(unique(deaths$output_version_id))) vers <- unique(deaths$output_version_id) else vers <- custom_version
  if(codem_estimates){
    cat(paste0("Custom fatal estimates pulled from CODEm upload - date pulled ", vers, " decomp_step ", fatal_decomp_step, "status ", compare_version),
        file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

  } else {
    cat(paste0("CodCorrect results - output version ", vers),
        file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  }

  # remove excess columns
  deaths <- deaths[, draw_cols_upload, with=FALSE]
  colnames(deaths) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("death_draw_", draw_nums_gbd))

  ### bring together variables for nonfatal calculations
  predict_nonfatal <- merge(cfr_dismod, deaths, by=c("location_id", "year_id", "age_group_id", "sex_id"))
  predict_nonfatal <- merge(predict_nonfatal, pop_env, by=c("location_id", "year_id", "age_group_id", "sex_id")) %>% data.table
  predict_nonfatal <- merge(predict_nonfatal[, cause := "A05.a"], duration, by="cause", all.x=TRUE)
  #***********************************************************************************************************************


  #----CALCULATE PREVALENCE-----------------------------------------------------------------------------------------------
  ### calculate prevalence (mort/cfr*duration)
  lapply(draw_nums_gbd, function (i) {
    predict_nonfatal[, paste0("prev_draw_", i) := ( (get(paste0("death_draw_", i)) / population) / get(paste0("cfr_draw_", i)) ) * get(paste0("dur_draw_", i))]
    predict_nonfatal[, paste0("inc_draw_", i) := get(paste0("prev_draw_", i)) / get(paste0("dur_draw_", i))]
  })

  ### keep needed columns
  predictions_prev_save <- predict_nonfatal[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("prev_draw_", draw_nums_gbd)), with=FALSE]
  predictions_inc_save <- predict_nonfatal[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("inc_draw_", draw_nums_gbd)), with=FALSE]
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

  ### write nonfatal outcomes to cluster
  save_nonfatal <- rbind(predictions_prev_save, predictions_inc_save)
  lapply(unique(save_nonfatal$location_id), function(x) fwrite(save_nonfatal[location_id==x, ],
                                                               file.path(cl.version.dir, paste0(x, ".csv")), row.names=FALSE))
  print(paste0("nonfatal estimates saved in ", cl.version.dir))

  ### upload results to db
  job <- paste0("qsub -N s_epi_", acause, " -l m_mem_free=125G -l fthread=10 -l archive -l h_rt=12:00:00 -P proj_cov_vpd -q all.q -o FILEPATH/sgeoutput/",
                username, " -e /FILEPATH/", username,
                " FILEPATH/execRscript.sh -i FILEPATH/ihme_rstudio_3631.img -s FILEPATH/save_results_wrapper.r",
                " --type epi",
                " --me_id ", me_id,
                " --year_ids ", paste(unique(save_nonfatal$year_id), collapse=","),
                " --input_directory ", cl.version.dir,
                " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
                " --best ", mark_model_best,
                " --bundle_id ", bundle_id,
                " --xw_id ", xw_id,
                " --gbd_round ", gbd_round)

  if (decomp) job <- paste0(job, " --decomp_step ", decomp_step)
  if (FauxCorrect) job <- paste0(job, " --draws ", length(colnames(save_nonfatal)[grepl("draw_", colnames(save_nonfatal))]))
  system(job); print(job)
  #***********************************************************************************************************************

}
