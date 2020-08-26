#----HEADER-------------------------------------------------------------------------------------------------------------
# Author: "USERNAME"
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
pacman::p_load(magrittr, foreign, stats, MASS, data.table, rhdf5, plyr, data.table, parallel, dplyr, reticulate) # "USERNAME": added reticulate package 02.11.19
source(""FILEPATH"load_packages.R")
load_packages("mvtnorm")

pandas <- import("pandas")
use_python('"FILEPATH"python')
gd <- import('get_draws.api')
#***********************************************************************************************************************


#----DIRECTORIES--------------------------------------------------------------------------------------------------------
### set objects
acause    <- "varicella"
cause_id  <- 342
me_id     <- 1440
ages      <- c(2:20, 30:32, 235) ## age group early neonatal to 95+
gbd_round <- 6
year_end  <- if (gbd_round %in% c(3, 4, 5)) gbd_round + 2012 else if (gbd_round==6) 2019

### draw numbers
draw_nums_gbd    <- 0:999
draw_cols_upload <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", draw_nums_gbd))

### make folders on cluster
# CoD
cl.death.dir <- file.path(""FILEPATH)
if (!dir.exists(cl.death.dir) & SAVE_FATAL=="yes") dir.create(cl.death.dir, recursive=TRUE)
# nonfatal/epi
cl.version.dir <- file.path(""FILEPATH"")                                                
if (!dir.exists(cl.version.dir) & CALCULATE_NONFATAL=="yes") dir.create(file.path(cl.version.dir), recursive=TRUE)

### directories
home <- file.path(FILEPATH)
j.version.dir <- file.path(home, "models", custom_version)
j.version.dir.inputs <- file.path(j.version.dir, "model_inputs")
j.version.dir.logs <- file.path(j.version.dir, "model_logs")
if (!dir.exists(j.version.dir.inputs)) dir.create(j.version.dir.inputs, recursive=TRUE)
if (!dir.exists(j.version.dir.logs)) dir.create(j.version.dir.logs, recursive=TRUE)

### save description of model run
# which model components are being uploaded?
if (CALCULATE_NONFATAL=="yes" & SAVE_FATAL=="no") add_  <- "NF"
if (SAVE_FATAL=="yes" & CALCULATE_NONFATAL=="no") add_  <- "CoD"
if (CALCULATE_NONFATAL=="yes" & SAVE_FATAL=="yes") add_ <- "NF and CoD"
# record CODEm data-rich feeder model versions used in CoD hybridization
if (SAVE_FATAL=="yes") description <- paste0("DR M ", male_CODEm_version, ", DR F ", female_CODEm_version, ", ", description)
# save full description to model folder
description <- paste0(add_, " - ", description)
cat(description, file=file.path(j.version.dir, "MODEL_DESCRIPTION.txt"))
#***********************************************************************************************************************


#----FUNCTIONS----------------------------------------------------------------------------------------------------------  
### custom functions
""FILEPATH"read_hdf5_table.R" %>% source
""FILEPATH"sql_query.R" %>% source

### load shared functions
file.path(""FILEPATH"get_population.R") %>% source
file.path(""FILEPATH"get_location_metadata.R") %>% source
file.path(""FILEPATH"get_covariate_estimates.R") %>% source
file.path(""FILEPATH"get_envelope.R") %>% source
file.path(""FILEPATH"get_cod_data.R") %>% source
source(""FILEPATH"get_draws.R")
#***********************************************************************************************************************


########################################################################################################################
##### PART ONE: COD NEG BIN REGRESSION #################################################################################
########################################################################################################################


#----PREP---------------------------------------------------------------------------------------------------------------
### get location data
locations <- get_location_metadata(gbd_round_id=gbd_round, location_set_id=22)[, ## id=22 is from covariates team, id=9 is from epi
                          .(location_id, ihme_loc_id, location_name, region_id, super_region_id, level, location_type, parent_id, sort_order)]
pop_locs <- unique(locations[level >= 3, location_id])
standard_locations <- get_location_metadata(gbd_round_id=gbd_round, location_set_id=101)$location_id %>% unique

if (SAVE_FATAL == "yes") {
  
### pop_env
# get envelope
if (decomp) {
  envelope <- get_envelope(location_id=pop_locs, sex_id=1:2, age_group_id=ages, year_id=1980:year_end, with_hiv=0, gbd_round_id=gbd_round, decomp_step=decomp_step)
} else {
  envelope <- get_envelope(location_id=pop_locs, sex_id=1:2, age_group_id=ages, year_id=1980:year_end, with_hiv=0, gbd_round_id=gbd_round)
}
# save model version
cat(paste0("Mortality envelope - model run ", unique(envelope$run_id)),
    file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
setnames(envelope, "mean", "envelope")
envelope <- subset(envelope, select=c("location_id", "year_id", "age_group_id", "sex_id", "envelope"))
# get population data
if (decomp) {
  if (pop_run_id==145) {
    population <- get_population(location_id=pop_locs, sex_id=1:2, age_group_id=ages, year_id=1980:year_end, gbd_round_id=gbd_round, decomp_step=decomp_step, run_id=145)
  } else {
    population <- get_population(location_id=pop_locs, sex_id=1:2, age_group_id=ages, year_id=1980:year_end, gbd_round_id=gbd_round, decomp_step=decomp_step)
  }
} else {
  population <- get_population(location_id=pop_locs, sex_id=1:2, age_group_id=ages, year_id=1980:year_end, gbd_round_id=gbd_round)
}
# save model version
cat(paste0("Population - model run ", unique(population$run_id)),
    file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
population <- subset(population, select=c("location_id", "year_id", "age_group_id", "sex_id", "population"))
# bring together
pop_env <- merge(population, envelope, by=c("location_id", "year_id", "age_group_id", "sex_id"))

### get cod data (querying directly is faster than central function wrapper)
if (decomp) {
  cod <- get_cod_data(cause_id=cause_id, gbd_round_id=gbd_round, decomp_step=decomp_step)[, .(location_id, year, age_group_id, sex, sample_size, cf, description)] %>% setnames(., "sex", "sex_id") %>% setnames(., "year", "year_id")
} else {
  cod <- get_cod_data(cause_id=cause_id)[, .(location_id, year, age_group_id, sex, sample_size, cf, description)] %>% setnames(., "sex", "sex_id") %>% setnames(., "year", "year_id")
}
cod <- cod[year_id >= 1980 & age_group_id %in% ages & !is.na(cf) & sample_size != 0, ]
# save model version
cat(paste0("CoD data - version ", unique(cod$description)),
    file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

if (HAQI_or_HSA == "HSA") {

  # covariate: HSA - covariate_id=208, covariate_name_short="health_system_access_capped"
  if (decomp) covariates <- get_covariate_estimates(covariate_id=208, year_id=1980:year_end, location_id=pop_locs, status="best", gbd_round_id=gbd_round, decomp_step=decomp_step)[, .(location_id, year_id, mean_value, model_version_id)]
  else covariates <- get_covariate_estimates(covariate_id=208, year_id=1980:year_end, location_id=pop_locs, status="best")[, .(location_id, year_id, mean_value, model_version_id)]
  setnames(covariates, "mean_value", "health")

} else if (HAQI_or_HSA =="HAQI") {

  # covariate: HAQ - covariate_id=1099, covariate_name_short="haqi"
  if (decomp) covariates <- get_covariate_estimates(covariate_id=1099, year_id=1980:year_end, location_id=pop_locs, status="best", gbd_round_id=gbd_round, decomp_step=decomp_step)[, .(location_id, year_id, mean_value, model_version_id)]
  else covariates <- get_covariate_estimates(covariate_id=1099, year_id=1980:year_end, location_id=pop_locs, status="best")[, .(location_id, year_id, mean_value, model_version_id)]
  setnames(covariates, "mean_value", "health")

}

### save covariate versions
cat(paste0("Covariate ", HAQI_or_HSA, " (CoD) - model version ", unique(covariates$model_version_id)),
    file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

### prep data for regression
regress <- merge(cod, covariates, by=c("location_id", "year_id"), all.x=TRUE)
regress <- merge(regress, pop_env, by=c("location_id", "year_id", "age_group_id", "sex_id"), all.x=TRUE)

### calculate deaths
regress[, deaths := cf * envelope]

### replace deaths with 0 if deaths < 0.5
regress[deaths < 0.5, deaths := 0]

### drop outliers (cf greather than 99th percentile)
cf_99 <- quantile(regress$cf, 0.99)

### make ages into levels
regress[, age_group_id := as.factor(age_group_id)]

### using standard locations
regress <- regress[location_id %in% standard_locations]

### save file for reference
fwrite(regress, file.path(j.version.dir.inputs, "inputs_for_nbreg_CoD.csv"), row.names=FALSE)
#***********************************************************************************************************************


#----NEG BIN MODEL------------------------------------------------------------------------------------------------------
### run negative binomial regression
theta = 1 / 4.42503
GLM <- glm.nb(deaths ~ health + age_group_id + offset(log(population)), init.theta=theta, data=regress)

# save log
capture.output(summary(GLM), file = file.path(j.version.dir.logs, "log_deaths_nbreg_CoD.txt"), type="output")
#***********************************************************************************************************************


#----DRAWS--------------------------------------------------------------------------------------------------------------
set.seed(0311)

### predict out for all country-year-age-sex
pred_death <- merge(population, covariates, by=c("year_id", "location_id"))
N <- length(pred_death$year_id)

### 1000 draws for uncertainty
# coefficient matrix
coefmat <- c(coef(GLM))
names(coefmat)[1] <- "constant"
names(coefmat) <- paste("b", names(coefmat), sep="_")
coefmat <- matrix(unlist(coefmat), ncol=24, byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coefmat)))))

### covariance matrix
vcovmat <- vcov(GLM)
# create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
betas <- t(rmvnorm(n=length(draw_nums_gbd), mean=coefmat, sigma=vcovmat)) %>% data.table
colnames(betas) <- paste0("beta_draw_", draw_nums_gbd)
betas[, age_group_id := c(NA, NA, 3:20, 30:32, 235)]
# merge together predictions with age draws by age_group_id
pred_death <- merge(pred_death, betas[!is.na(age_group_id), ], by="age_group_id", all.x=TRUE)

### create draws of disperion parameter
alphas <- 1 / exp(rnorm(length(draw_nums_gbd), mean=GLM$theta, sd=GLM$SE.theta))

### calculate out death predictions from model
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
  fwrite(pred_death_save, file.path(j.version.dir, paste0("01_death_predictions.csv")), row.names=FALSE)
}
#***********************************************************************************************************************


########################################################################################################################
##### PART TWO: COMBINE CODEm DEATHS ###################################################################################
########################################################################################################################


#----COMBINE------------------------------------------------------------------------------------------------------------
### read CODEm COD results for data-rich countries

cod_M <- data.table(pandas$read_hdf(file.path(""FILEPATH"")), key="data"))
cod_F <- data.table(pandas$read_hdf(file.path(""FILEPATH"")), key="data"))


# save model version
cat(paste0("Data-rich CODEm feeder model (males) - model version ", male_CODEm_version),
    file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
cat(paste0("Data-rich CODEm feeder model (females) - model version ", female_CODEm_version),
    file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

### combine M/F CODEm results
cod_DR <- rbind(cod_M, cod_F) %>% .[, draw_cols_upload, with=FALSE]
cod_DR <- cod_DR[age_group_id %in% ages, ]

### hybridize CoD results
data_rich <- unique(cod_DR$location_id)
colnames(pred_death_save) <- draw_cols_upload
split_deaths_glb <- pred_death_save[!location_id %in% data_rich, ]
split_deaths_hyb <- rbind(split_deaths_glb, cod_DR)

### save results
if (WRITE_FILES == "yes") {
  fwrite(split_deaths_hyb, file.path(j.version.dir, "02_death_predictions_hybridized.csv"), row.names=FALSE)
}
#***********************************************************************************************************************

########################################################################################################################
##### PART THREE: FORMAT FOR CODCORRECT ################################################################################
########################################################################################################################


#----SAVE RESULTS-------------------------------------------------------------------------------------------------------
### save to share directory for upload
split_deaths_hyb[, measure_id := 1]

lapply(unique(split_deaths_hyb$location_id), function(x) fwrite(split_deaths_hyb[location_id==x, ],
                                                                   file.path(cl.death.dir, paste0(x, ".csv")), row.names=FALSE))
print(paste0("death draws saved in ", cl.death.dir))

### save_results
job <- paste0("qsub -N s_cod_", acause, " -l m_mem_free=100G -l fthread=4 -l archive -l h_rt=8:00:00 -P proj_cov_vpd -q all.q -o "FILEPATH"", 
              username, " -e "FILEPATH"", username,
              " "FILEPATH"save_results_wrapper.r",
              " --args",
              " --type cod",
              " --me_id ", cause_id,
              " --input_directory ", cl.death.dir,
              " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
              " --best ", mark_model_best, 
              " --gbd_round ", gbd_round)

if (decomp) job <- paste0(job, " --decomp_step ", decomp_step)
system(job); print(job)
#*********************************************************************************************************************** 
  
}


if (CALCULATE_NONFATAL == "yes") {
  
########################################################################################################################
##### PART FOUR: DisMod NONFATAL RESULTS ###############################################################################
########################################################################################################################



#----GET HAZARDS--------------------------------------------------------------------------------------------------------
### varicella DisMod model results
if (decomp) {
  if (decomp_step=="step2") {  
    source_python(""FILEPATH"get_python_draws.py")
    var_nf   <- data.table(python_draws(gbd_id_type="modelable_entity_id", gbd_id=1439, status="best", gbd_round_id=gbd_round, source="epi", decomp_step=decomp_step))  # measure_id is hardcoded in python fxn
  } else if (decomp_step=="step4") {
    var_nf <- gd$get_draws(gbd_id_type="modelable_entity_id", gbd_id=1439, status="best", gbd_round_id=6, source="epi", decomp_step="step4", measure_id=c(5, 6)) %>% data.table
    
  } else {
    var_nf   <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=1439, status="best", gbd_round_id=gbd_round, source="epi", measure_id=c(5, 6), decomp_step=decomp_step)
  }
} else {
  var_nf   <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=1439, status="best", gbd_round_id=gbd_round, source="epi", measure_id=c(5, 6))
}
var_inc  <- var_nf[measure_id==6, ]
var_prev <- var_nf[measure_id==5, ]
# save model version
cat(paste0("Varicella DisMod model (me_id 1439) for prevalence and incidence - model version ", unique(var_prev$model_version_id)), 
    file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

### prep prevalence draws for incidence rate calculation
var_prev2 <- var_prev[, draw_cols_upload, with=FALSE]
colnames(var_prev2) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("prev_draw_", draw_nums_gbd))

### prep incidence draws for incidence rate calculation
var_inc2 <- var_inc[, draw_cols_upload, with=FALSE]
colnames(var_inc2) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("hazard_draw_", draw_nums_gbd))

### merge incidence and prevalence draws
var_nonfatal <- merge(var_prev2, var_inc2, by=c("location_id", "year_id", "age_group_id", "sex_id"))
#*********************************************************************************************************************** 


########################################################################################################################
##### PART FIVE: CALCULATE NONFATAL ####################################################################################
########################################################################################################################


#----INCIDENCE----------------------------------------------------------------------------------------------------------
### calculate incidence from hazards
draw_cols_gbd <- paste0("draw_", draw_nums_gbd)
var_nonfatal[, (draw_cols_gbd) := lapply(draw_nums_gbd, function(ii) get(paste0("hazard_draw_", ii)) * ( 1 - get(paste0("prev_draw_", ii)) ) )]
predictions_inc_save <- subset(var_nonfatal, select=draw_cols_upload)

### calculate prevalence from incidence
var_nonfatal[, (draw_cols_gbd) := lapply(draw_nums_gbd, function(ii) get(paste0("draw_", ii)) * (7 / 365.242) )]
predictions_prev_save <- subset(var_nonfatal, select=draw_cols_upload)
### force age over 90 to 0 for squareness
predictions_prev_save <- predictions_prev_save[age_group_id %in% c(32, 33, 235, 164), (draw_cols_gbd) := lapply(draw_nums_gbd, function(ii) get(paste0("draw_", ii)) * 0 )]



if (WRITE_FILES == "yes") {
  fwrite(predictions_prev_save, file.path(j.version.dir, "03_prevalence_draws.csv"), row.names=FALSE)
  fwrite(predictions_inc_save, file.path(j.version.dir, "04_incidence_draws.csv"), row.names=FALSE)
}
#*********************************************************************************************************************** 


########################################################################################################################
##### PART SIX: FORMAT FOR COMO ########################################################################################
########################################################################################################################


#----SAVE RESULTS-------------------------------------------------------------------------------------------------------
### format nonfatal for como
# prevalence, measure_id==5
predictions_prev_save[, measure_id := 5]
# incidence, measure_id==6
predictions_inc_save[, measure_id := 6]
predictions <- rbind(predictions_prev_save, predictions_inc_save)

### save flat files
lapply(unique(predictions$location_id), function(x) fwrite(predictions[location_id==x],
                                                           file.path(cl.version.dir, paste0(x, ".csv")), row.names=FALSE))
print(paste0("nonfatal estimates saved in ", cl.version.dir))

### upload results, verify logs in right folder
job <- paste0("qsub -N s_epi_", acause, " -l m_mem_free=125G -l fthread=10 -l archive -l h_rt=12:00:00 -P proj_cov_vpd -q all.q -o "FILEPATH"", 
              username, " -e "FILEPATH"", username,
              " "FILEPATH"save_results_wrapper.r",
              " --args",
              " --type epi",
              " --me_id ", me_id,
              " --year_ids ", paste(unique(predictions$year_id), collapse=","),
              " --input_directory ", cl.version.dir,
              " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
              " --best ", mark_model_best, 
              " --gbd_round ", gbd_round)

if (decomp) job <- paste0(job, " --decomp_step ", decomp_step)
system(job); print(job)
#***********************************************************************************************************************
  
}
