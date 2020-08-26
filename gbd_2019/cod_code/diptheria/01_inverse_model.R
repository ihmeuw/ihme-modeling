#----HEADER-------------------------------------------------------------------------------------------------------------
# Author: "USERNAME"
# Date:    February 2017, modified Dec 2017 for GBD 2017
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
pacman::p_load(magrittr, foreign, stats, MASS, data.table, rhdf5, plyr, data.table, parallel, dplyr, reticulate) 
source(""FILEPATH"load_packages.R")   
load_packages("mvtnorm")

use_python('"FILEPATH"python')
gd <- import('get_draws.api')

pandas <- import("pandas")
#***********************************************************************************************************************


#----DIRECTORIES--------------------------------------------------------------------------------------------------------
### set objects
acause    <- "diptheria"
age_start <- 4
age_end   <- 16
cause_id  <- 338
me_id     <- 1421
gbd_round <- 6
year_end  <- if (gbd_round %in% c(3, 4, 5)) gbd_round + 2012 else if (gbd_round==6) 2019

### draw numbers
draw_nums_gbd    <- 0:999
draw_cols_upload <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", draw_nums_gbd))

### make folders on cluster
# CoD
cl.death.dir <- file.path(""FILEPATH, "draws")
if (!dir.exists(cl.death.dir)) dir.create(cl.death.dir, recursive=TRUE)
# nonfatal/epi
cl.version.dir <- file.path(""FILEPATH, "draws")
if (!dir.exists(cl.version.dir)) dir.create(cl.version.dir, recursive=TRUE)

### directories
home <- file.path(FILEPATH)
j.version.dir <- FILEPATH
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
source(""FILEPATH"sql_query.R")
source(""FILEPATH"read_hdf5_table.R")

### load shared functions
source(""FILEPATH"get_population.R")
source(""FILEPATH"get_location_metadata.R") 
source(""FILEPATH"get_covariate_estimates.R") 
source(""FILEPATH"get_envelope.R") 
source(""FILEPATH"get_draws.R")
source(""FILEPATH"get_cod_data.R") 
#*********************************************************************************************************************** 


########################################################################################################################
##### PART ONE: COD NEG BIN REGRESSION #################################################################################
########################################################################################################################


#----PREP---------------------------------------------------------------------------------------------------------------
### get location data
locations <- get_location_metadata(gbd_round_id=gbd_round, location_set_id=22)[, ## id=22 is from covariates team, id=9 is from epi
                                                                               .(location_id, ihme_loc_id, location_name, location_ascii_name, region_id, super_region_id, level, location_type, parent_id, super_region_name, sort_order)]
pop_locs <- unique(locations[level >= 3, location_id])
standard_locations <- get_location_metadata(gbd_round_id=gbd_round, location_set_id=101)$location_id %>% unique

### pop_env
# get envelope
if (decomp) {
  envelope <- get_envelope(location_id=pop_locs, sex_id=1:2, age_group_id=c(age_start:age_end), with_hiv=0, year_id=1980:year_end, gbd_round_id=gbd_round, decomp_step=decomp_step)[, 
                                                                                                                                                                                    .(location_id, year_id, age_group_id, sex_id, mean, run_id)]
} else { envelope <- get_envelope(location_id=pop_locs, sex_id=1:2, age_group_id=c(age_start:age_end), with_hiv=0, year_id=1980:year_end)[, 
                                                                                                                                          .(location_id, year_id, age_group_id, sex_id, mean, run_id)]
}
setnames(envelope, "mean", "envelope")
# get population data
if (decomp) {
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
    covar <- get_covariate_estimates(covariate_id=2308, year_id=1980:year_end, location_id=pop_locs,
                                     gbd_round_id=gbd_round, decomp_step=decomp_step)[, .(location_id, year_id, mean_value, model_version_id)]
  } else {
    if (decomp) {
      covar <- get_covariate_estimates(covariate_id=32, year_id=1980:year_end, location_id=pop_locs, gbd_round_id=gbd_round,
                                       gbd_round_id=gbd_round, decomp_step=decomp_step)[, .(location_id, year_id, mean_value, model_version_id)]
    } else {
      covar <- get_covariate_estimates(covariate_id=32, year_id=1980:year_end, location_id=pop_locs,
                                       gbd_round_id=gbd_round)[, .(location_id, year_id, mean_value, model_version_id)]
    }
  }
  setnames(covar, "mean_value",  "DTP3_coverage_prop")
  
  if (fatal_fit=="add_HAQI") {
    haqi <- get_covariate_estimates(covariate_id=1099, year_id=1980:year_end, location_id=pop_locs,
                                    gbd_round_id=gbd_round, decomp_step=decomp_step)[, .(location_id, year_id, mean_value, model_version_id)]
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
  if (decomp) {
    raw <- get_cod_data(cause_id=cause_id, gbd_round_id=gbd_round, decomp_step=decomp_step) %>%
      setnames(., "year", "year_id") %>% .[acause=="diptheria", .(nid, location_id, location_name, year_id, age_group_id, sex, cf_corr, sample_size, description)] %>% setnames(., "sex", "sex_id")
  } else {
    raw <- get_cod_data(cause_id=cause_id, gbd_round_id=gbd_round) %>%
      setnames(., "year", "year_id") %>% .[acause=="diptheria", .(nid, location_id, location_name, year_id, age_group_id, sex, cf_corr, sample_size, description)] %>% setnames(., "sex", "sex_id")
  }
  raw <- raw[!is.na(cf_corr) & year_id >= 1980 & sample_size != 0 & age_group_id %in% c(age_start:age_end), ]
  # save model version
  cat(paste0("CoD data - version ", unique(raw$description)),
      file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  # add on mortality envelope
  raw <- merge(raw, pop_env, by=c("location_id", "year_id", "age_group_id", "sex_id"), all.x=TRUE)
  # calculate death counts
  raw[, deaths := cf_corr * envelope]
  
  ### round to whole number (counts model)
  raw[deaths < 0.5, deaths := 0]
  
  ### inform model with national data only
  raw <- raw[location_id %in% locations[level==3, location_id], ]
  
  ### drop outliers
  cf_999 <- quantile(raw$cf_corr, 0.999)
  raw <- raw[cf_corr <= cf_999, ]
  
  ### make ages into levels
  raw[, age_group_id := as.factor(age_group_id)]
  
  ### bring together variables for regression
  regress <- merge(raw, covar, by=c("location_id", "year_id"))
  regress <- regress[location_id %in% standard_locations]
  
  ### save file for reference
  fwrite(regress, FILEPATH, row.names=FALSE)
  #***********************************************************************************************************************
  
  
  #----NEG BIN MODEL------------------------------------------------------------------------------------------------------
  ### run negative binomial regression
  GLM <- glm.nb(deaths ~ DTP3_coverage_prop + age_group_id + offset(log(envelope)), data=regress)
  
  # save log
  capture.output(summary(GLM), file = FILEPATH, type="output")
  #***********************************************************************************************************************
  
  
  #----DRAWS--------------------------------------------------------------------------------------------------------------
  ### set random seed
  set.seed(0311)
  
  ### prep prediction dataset
  pred_death <- merge(pop_env, covar, by=c("year_id", "location_id"), all.x=TRUE)
  N <- nrow(pred_death)
  
  ### 1000 draws for uncertainty
  # coefficient matrix
  coefmat <- c(coef(GLM))
  names(coefmat)[1] <- "constant"
  names(coefmat) <- paste("b", names(coefmat), sep = "_")
  coefmat <- matrix(unlist(coefmat), ncol=14, byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coefmat)))))
  
  ### covariance matrix
  vcovmat <- vcov(GLM)
  # create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
  betas <- t(rmvnorm(n=length(draw_nums_gbd), mean=coefmat, sigma=vcovmat)) %>% data.table
  colnames(betas) <- paste0("beta_draw_", draw_nums_gbd)
  betas[, age_group_id := c(NA, NA, 5:16)]
  # merge together predictions with age draws by age_group_id
  pred_death <- merge(pred_death, betas[!is.na(age_group_id), ], by="age_group_id", all.x=TRUE)
  pred_death[age_group_id==4 & is.na(beta_draw_0), paste0("beta_draw_", draw_nums_gbd) := 0]
  
  ### create draws of disperion parameter
  alphas <- 1 / exp(rnorm(1000, mean=GLM$theta, sd=GLM$SE.theta))
  
  if (GAMMA_EPSILON == "with") {
    lapply(draw_nums_gbd, function (draw) {
      # set betas
      b0 <- betas[1, ][[paste0("beta_draw_", draw)]]
      b1 <- betas[2, ][[paste0("beta_draw_", draw)]]
      # age fixed effects
      age.fe <- pred_death[[paste0("beta_draw_", draw)]]
      alpha <- alphas[draw + 1]
      # calculate 1000 draws
      pred_death[, paste0("death_draw_", draw) := rgamma( N, scale=(alpha * exp( b0 + (b1 * DTP3_coverage_prop) + age.fe ) * envelope ),
                                                          shape=(1 / alpha) ) ]
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
    fwrite(pred_death_save, FILEPATH), row.names=FALSE)
  }
  #***********************************************************************************************************************
  
  
  #----HYBRIDIZE----------------------------------------------------------------------------------------------------------
  ### read CODEm COD results for data-rich countries
  cod_M <- data.table(pandas$read_hdf(file.path(""FILEPATH"deaths_", "male", ".h5")), key="data"))
cod_F <- data.table(pandas$read_hdf(file.path(""FILEPATH"deaths_", "female", ".h5")), key="data"))


# save model version
cat(paste0("Data-rich CODEm feeder model (males) - model version ", male_CODEm_version), 
    file=FILEPATH, sep="\n", append=TRUE)
cat(paste0("Data-rich CODEm feeder model (females) - model version ", female_CODEm_version), 
    file=FILEPATH, sep="\n", append=TRUE)

# combine M/F CODEm results
cod_DR <- rbind(cod_M, cod_F, fill=TRUE)
cod_DR <- cod_DR[, draw_cols_upload, with=FALSE]

# hybridize data-rich and custom models
data_rich <- unique(cod_DR$location_id)
deaths_glb <- pred_death_save[!location_id %in% data_rich, ]
colnames(deaths_glb) <- draw_cols_upload
deaths_hyb <- rbind(deaths_glb, cod_DR)

# keep only needed age groups
deaths_hyb <- deaths_hyb[age_group_id %in% c(age_start:age_end), ]
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
job <- paste0("qsub -N s_cod_", acause, " -l m_mem_free=100G -l fthread=5 -l archive -l h_rt=8:00:00 -P proj_cov_vpd -q all.q -o "FILEPATH"", 
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
    
    if (decomp_step=="step4") {
      cfr_dismod <- gd$get_draws(gbd_id_type="modelable_entity_id", gbd_id=as.integer(2834), version_id=as.integer(473450), gbd_round_id=6, source="epi", 
                                 decomp_step="iterative", num_workers=as.integer(10)) %>% data.table
    } else {
      cfr_dismod <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=2834, status="best", gbd_round_id=gbd_round, source="epi", decomp_step=decomp_step)
    }
    
  } else {
    cfr_dismod <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=2834, status="best", gbd_round_id=gbd_round, source="epi")
    
  }
  
  # save model version
  cat(paste0("Case fatality ratio DisMod model (me_id 2834) - model run ", unique(cfr_dismod$model_version_id)),
      file=FILEPATH, sep="\n", append=TRUE)
  
  # remove excess columns
  if (decomp_step=="step4") {
    cfr_dismod <- cfr_dismod[, draw_cols_upload, with=FALSE]  
  } else {
    cfr_dismod <- cfr_dismod[, draw_cols_upload, with=FALSE]
  }
  
  colnames(cfr_dismod) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("cfr_draw_", draw_nums_gbd))
  #***********************************************************************************************************************
  
  
  ########################################################################################################################
  ##### PART FOUR: MORTALITY TO NONFATAL #################################################################################
  ########################################################################################################################
  
  
  #----PREP---------------------------------------------------------------------------------------------------------------
  ### bring in duration data
  if (FauxCorrect) {
    ### Downsample
    custom_file <- paste0(j_root, ""FILEPATH"duration_draws.csv")
    # Source downsample wrapper
    source_python(""FILEPATH"test_dsample_py.py")
    # Call python function written in sourced .py
    duration <- downsample_draws(custom_file, 100L, cause_sub="A05.a")
    colnames(duration) <- c("cause", paste0("dur_draw_", draw_nums_gbd))
  } else {
    duration <- read.csv(file.path(j_root, ""FILEPATH"duration_draws.csv")) %>% data.table
    duration <- duration[cause=="A05.a", ]
    colnames(duration) <- c("cause", paste0("dur_draw_", draw_nums_gbd))
  }
  
  ### prep death data
  if (decomp & FauxCorrect) {  
    deaths <- get_draws("cause_id", cause_id, "fauxcorrect", location_id=pop_locs, year_id=unique(cfr_dismod$year_id), gbd_round_id=gbd_round,
                        measure_id=1, version=5, decomp_step=decomp_step)
  } else if (decomp & !FauxCorrect) {
    
    deaths <- gd$get_draws(gbd_id_type="cause_id", gbd_id=as.integer(cause_id), location_id=as.integer(pop_locs), year_id=as.integer(unique(cfr_dismod$year_id)), 
                           source="codcorrect", measure_id=as.integer(1), version_id=as.integer(99), gbd_round_id=6, decomp_step="step4", num_workers=as.integer(10))
    print("AYOOO pulled from CodCorrect and moving on!")
    deaths <- data.table(deaths)
    
    
  } else {
    deaths <- get_draws("cause_id", cause_id, "codcorrect", location_id=pop_locs, year_id=unique(cfr_dismod$year_id), gbd_round_id=gbd_round,
                        measure_id=1, status="latest")
  }
  
  # save model version
  if (!is.null(unique(deaths$output_version_id))) vers <- unique(deaths$output_version_id) else vers <- custom_version
  cat(paste0("CodCorrect results - output version ", vers),
      file=FILEPATH, sep="\n", append=TRUE)
  
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
                                                               file.path(FILEPATH, paste0(x, ".csv")), row.names=FALSE))
  print(paste0("nonfatal estimates saved in ", FILEPATH))
  
  ### upload results to db
  job <- paste0("qsub -N s_epi_", acause, " -l m_mem_free=125G -l fthread=10 -l archive -l h_rt=12:00:00 -P proj_cov_vpd -q all.q -o "FILEPATH"", 
                username, " -e "FILEPATH"", username,
                " "FILEPATH"save_results_wrapper.r",
                " --args",
                " --type epi",
                " --me_id ", me_id,
                " --year_ids ", paste(unique(save_nonfatal$year_id), collapse=","),
                " --input_directory ", FILEPATH,
                " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
                " --best ", mark_model_best, 
                " --gbd_round ", gbd_round)
  
  if (decomp) job <- paste0(job, " --decomp_step ", decomp_step)
  if (FauxCorrect) job <- paste0(job, " --draws ", length(colnames(save_nonfatal)[grepl("draw_", colnames(save_nonfatal))]))
  system(job); print(job)
  #***********************************************************************************************************************
  
}