#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:  REDACTED
# Date:    2017, 2023 
# Purpose: Model pertussis fatal and nonfatal outcomes for GBD
#          PART ONE -    Incidence natural history model
#          PART TWO -    Rescale case estimates
#          PART THREE -  Age-sex split cases
#          PART FOUR -   Calculate prevalence and incidence from split case estimates
#          PART FIVE -   Format for COMO and save results to the database
#          PART SIX -    Model fatal outcomes with negative binomial model
#          PART SEVEN -  Calculate deaths from CFR
#          PART EIGHT -  Rescale death estimates
#          PART NINE -   Age-sex split deaths
#          PART TEN -    Replace modeled death estimates in select locations with CODEm data-rich feeder model
#          PART ELEVEN - Format for codcorrect and save results to the database
#***********************************************************************************************************************


########################################################################################################################
##### START-UP #########################################################################################################
########################################################################################################################


#----CONFIG-------------------------------------------------------------------------------------------------------------
### load packages
pacman::p_load(magrittr, stats, data.table, plyr, dplyr, lme4, parallel, reticulate)
if (Sys.info()["sysname"] == "Linux") {
  library(rhdf5)  
  library(mvtnorm) 
} else { 
  pacman::p_load(mvtnorm, rhdf5)
}

setDTthreads(1)
library(reticulate)
reticulate::use_condaenv("/FILEPATH/envs/gbd_env")
pandas <- import("pandas")
#***********************************************************************************************************************


#----DIRECTORIES--------------------------------------------------------------------------------------------------------
### set objects
acause    <- "whooping"
age_start <- 6             ## pre 2020, age_start = 4, age group with smallest age group id is 6 now
age_end   <- 16            ## age 55-59 years
a         <- 3             ## birth cohort years before 1980
cause_id  <- 339

# if(covid_adjustment) me_id <- 26945 else 
me_id <- 1424 
gbd_round <- 9
release_id <- 16
year_end <- 2024 #change this for each GBD round

### draw numbers
draw_nums_gbd    <- 0:999
draw_cols_gbd    <- paste0("draw_", draw_nums_gbd)
draw_cols_upload <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", draw_nums_gbd))

### make folders on cluster
# mortality
cl.death.dir <- file.path("/FILEPATH", "mortality", custom_version, "draws") 
if (!dir.exists(cl.death.dir) & CALCULATE_COD == "yes") dir.create(cl.death.dir, recursive=TRUE)
# nonfatal
cl.version.dir <- file.path("/FILEPATH", "nonfatal", custom_version, "draws")
if (!dir.exists(cl.version.dir) & CALCULATE_NONFATAL == "yes") dir.create(file.path(cl.version.dir), recursive=TRUE)

### directories
home <- file.path(j_root, "FILEPATH", acause, "00_documentation")
FILEPATH <- file.path(home, "models", custom_version)
FILEPATH.inputs <- file.path(FILEPATH, "model_inputs")
FILEPATH.logs <- file.path(FILEPATH, "model_logs")
if (!dir.exists(FILEPATH.inputs)) dir.create(FILEPATH.inputs, recursive=TRUE)
if (!dir.exists(FILEPATH.logs)) dir.create(FILEPATH.logs, recursive=TRUE)

### save description of model run
if (CALCULATE_NONFATAL=="yes" & CALCULATE_COD=="no") add_  <- "NF"
if (CALCULATE_COD=="yes" & CALCULATE_NONFATAL=="no") add_  <- "CoD"
if (CALCULATE_NONFATAL=="yes" & CALCULATE_COD=="yes") add_ <- "NF and CoD"

# record CODEm data-rich feeder model versions used in CoD hybridization
if (CALCULATE_COD=="yes") description <- paste0("DR M ", male_CODEm_version, ", DR F ", female_CODEm_version, ", ", description)
# save full description to model folder
description <- paste0(add_, " - ", description)
cat(description, file=file.path(FILEPATH, "MODEL_DESCRIPTION.txt"))
#***********************************************************************************************************************

#----FUNCTIONS----------------------------------------------------------------------------------------------------------  
### reading .h5 files
"/FILEPATH/read_hdf5_table.R" %>% source
"/FILEPATH/collapse_point.R" %>% source

### load shared functions
source("/FILEPATH/get_population.R")
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/get_covariate_estimates.R") 
source("/FILEPATH/get_envelope.R") 
source("/FILEPATH/get_bundle_data.R") 
source("/FILEPATH/get_bundle_version.R")

### load personal functions
"/FILEPATH/read_excel.R" %>% source
"/FILEPATH/rake.R" %>% source
"/FILEPATH/collapse_point.R" %>% source
source(file.path("/FILEPATH/sex_age_weight_split.R"))
#***********************************************************************************************************************

########################################################################################################################
##### PART ONE: INCIDENCE NAT HIST MODEL ###############################################################################
########################################################################################################################


#----PREP---------------------------------------------------------------------------------------------------------------
### get location data
locations <- get_location_metadata(release_id = release_id, 
                                   location_set_id=22)[, .(location_id, ihme_loc_id, location_name, location_ascii_name, region_id, super_region_id, level, location_type, parent_id, sort_order)]
pop_locs <- unique(locations[level >= 3, location_id])
standard_locations <- get_location_metadata(release_id = release_id, 
                                            location_set_id=101)$location_id %>% unique

### get population 
population <- get_population(location_id=pop_locs, 
                             year_id=(1980-a):year_end, 
                             age_group_id=c(2:3, 388, 389, 238, 34, 6:20, 30:32, 235), 
                             status="best", 
                             sex_id=1:3, 
                             release_id = release_id)

#merge population with locations
population <- merge(population, locations[, .(location_id, ihme_loc_id)], by="location_id", all.x=TRUE)
# save model version
cat(paste0("Population - model run ", unique(population$run_id)), file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

# collapse by location year for under 1 ages NB incidence is calculated in under 1 year olds but then multiplied by the 1 year old population to get case counts
sum_pop <- population[age_group_id %in% c(2,3,388,389) & sex_id==3, ] %>% .[, pop := sum(population),
                                                                            by=c("ihme_loc_id", "location_id", "year_id")] %>% .[, .(ihme_loc_id, location_id, year_id, pop)] %>% unique


sum_pop[, year_id := year_id + a]
sum_pop <- sum_pop[year_id %in% 1980:year_end, ]

### GET DTP3 COVARIATES : DTP3_coverage_prop 5 year lagged 
if(use_covid_inclusive_vax == F){
if(is.null(custom_coverage)){
  if (use_lagged_covs) {
    covar <- get_covariate_estimates(covariate_id=2308, 
                                     year_id=1980:year_end, 
                                     location_id=pop_locs, 
                                     release_id = release_id)[, .(location_id, year_id, mean_value, model_version_id)]
  } else {
    covar <- get_covariate_estimates(covariate_id=32, 
                                     year_id=1980:year_end, 
                                     location_id=pop_locs)[, .(location_id, year_id, mean_value, model_version_id)]
  }
  
} else if (!is.null(custom_coverage)){
  if (use_lagged_covs) {
    covar <- get_covariate_estimates(covariate_id=2308, 
                                     model_version_id = custom_coverage, 
                                     year_id=1980:year_end, 
                                     location_id=pop_locs, 
                                     release_id = release_id)[, .(location_id, year_id, mean_value, model_version_id)]
  } else {
    covar <- get_covariate_estimates(covariate_id=32, 
                                     model_version_id = custom_coverage, 
                                     year_id=1980:year_end, location_id=pop_locs)[, .(location_id, year_id, mean_value, model_version_id)]
  }
}
}else if (use_covid_inclusive_vax == TRUE) {
  covar <- get_covariate_estimates(covariate_id=2441, 
                                    year_id=1980:year_end, 
                                    location_id=pop_locs, 
                                    release_id = release_id)[, .(location_id, year_id, mean_value, model_version_id)]
}

# Fix column name
setnames(covar, "mean_value", "DTP3")

# save covariate model version id
cat(paste0("Covariate DTP3 (NF) - model version ", unique(covar$model_version_id)), file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

if(use_mask_covariate == T) {
  masks <- get_covariate_estimates(covariate_id= 2537, #proportion of people reporting using a mask when leaving home 
                                       release_id = release_id)
      setnames(masks, "mean_value", "mask_use")
      
      # Make post 2022 values = 0
      masks[, mask_use := ifelse(year_id >=2022, 0, mask_use)]
      
      # Create no masking variable in order to log transform, this will make the regression RE align with direction of predictions. 
      masks[, no_mask := 1 - mask_use]
      masks[, ln_no_mask := log(no_mask)]
      
      # Print covariate model version id 
      cat(paste0("Covariate masking - model version ", unique(masks$model_version_id)), file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
      
      # Keep desired columns
      masks <- masks[, .(location_id, year_id, mask_use, no_mask, ln_no_mask)]
      
      # merge with vaccine estimates
      covar <- merge(covar, masks, by = c("location_id", "year_id"), all.x = T)
}


# Get case notifications from bundle: NB bundle contains only JRF data
case_notif <- get_bundle_version(bundle_version_id = incidence_bundle_version)
setDT(case_notif)
setnames(case_notif, "val", "cases")

# Merge case notifications with location metadata and keep desired columns
case_notif <- merge(case_notif, locations[, .(location_id, ihme_loc_id)], by = "location_id", all.x = T)
case_notif <- case_notif[, .(nid, location_id, ihme_loc_id, year_id, cases)]

# Drop 1980 and 1981 data, almost no locations reporting
if(drop_early_jrf){
  cases <- case_notif[!year_id %in% c(1980, 1981), ]
}

### function to prep additional literature / expert data
prep_regression <- function(...) {
  
  # prep US territories up to 2013
  US_terr <- fread(file.path(home, "FILEPATH/", "US_territories.csv"))
  US_terr <- US_terr[, c("ihme_loc_id", "year_start", "cases", "nid"), with=FALSE]
  setnames(US_terr, "year_start", "year_id")
  US_terr <- na.omit(US_terr)
  
  ### prep pertussis historical data from UK 
  UK <- fread(file.path(home, "FILEPATH/", "UK_Aust_JPN_population_for_agesexsplit.csv"))[iso3=="XEW"]
  setnames(UK, c("iso3", "year"), c("ihme_loc_id", "year_id"))
  UK[ihme_loc_id=="XEW", ihme_loc_id := "GBR"]
  # collapse UK pop by year
  UK[, ukpop := sum(pop2) * 100000, by=c("ihme_loc_id", "year_id")]
  UK_pop <- UK[, .(ihme_loc_id, year_id, ukpop)] %>% unique
  UK_pop[, year_id := year_id + a]
  
  # pull together more historical data from UK 1970 - 2008
  pne <- fread(file.path(j_root, "FILEPATH/Pertussis_Notification_Extraction.csv"))[iso3=="XEW"]
  names(pne) <- tolower(names(pne))
  setnames(pne, c("iso3", "year"), c("ihme_loc_id", "year_id"))
  pne[, notifications := as.integer(notifications)]
  # GBR 1940+
  pne1940 <- foreign::read.dta(file.path(j_root, "FILEPATH/DTP3_pre_1940.dta")) %>% as.data.table
  setnames(pne1940, c("iso3", "year"), c("ihme_loc_id", "year_id"))
  
  # replace missing cases with notification data
  historical <- rbind(pne, pne1940, fill = T)[, nid := 261647] 
  
  # Bind historical data to case notification data
  historical <- rbind(historical, cases, fill = T) 
  historical[is.na(cases), cases := notifications]
  historical <- historical[, c("year_id", "cases", "ihme_loc_id", "vacc_rate", "nid"), with=FALSE]
  historical[ihme_loc_id=="XEW", ihme_loc_id := "GBR"]
  historical[year_id <= 1923, vacc_rate := 0]
  historical <- rbind(historical, US_terr, fill = T)
  fwrite(historical, file.path(FILEPATH.inputs, paste0("incidence_data_inputs.csv")), row.names=FALSE)
  
  # collapse duplicates
  regr <- historical[, .(cases=max(cases, na.rm=TRUE), vacc_rate=min(vacc_rate, na.rm=TRUE)), by=c("ihme_loc_id", "year_id", "nid")] #ELBR- only relevant when using historical GBR data as have some overlap in 1970s and 1980s overlap with JRF

  # add on populations
  regr <- merge(regr, sum_pop[, c("ihme_loc_id", "year_id", "location_id", "pop")], by=c("ihme_loc_id", "year_id"), all.x=TRUE)
  regr <- merge(regr, UK_pop, by=c("ihme_loc_id", "year_id"), all.x=TRUE) #ELBR: only relevant when use early year GBR data
  regr[is.na(pop), pop := ukpop]
  # fix Great Britain
  regr[ihme_loc_id=="GBR", location_id := 95]
  
  # Remove data with missing population
  if (nrow(regr[is.na(pop)]) > 0) print(paste0("dropping rows with missing population data (", nrow(regr[is.na(pop)]), ")"))
  regr <- regr[!is.na(pop)]
  regr[is.infinite(vacc_rate), vacc_rate := NA] #ELBR: will only already have vacc_rate column if using historical GBR data
  
  # Merge with covariates
  regr <- merge(regr, covar, by=c("location_id", "year_id"), all.x=TRUE)
  regr[is.na(vacc_rate), vacc_rate := DTP3]
  #  regr[, vacc_rate := DTP3] # ELBR: if not using early GBR data, will need to create vacc_rate column
  
  if (nrow(regr[is.na(vacc_rate)]) > 0) print(paste0("dropping rows with missing vaccine coverage data (", nrow(regr[is.na(vacc_rate)]), ")"))
  regr <- regr[!is.na(vacc_rate)]
  
  # Convert variables to correct scale
  regr[, pop := round(pop, 0)]
  regr[, incidence := (cases / pop) * 100000] # this were the case numbers are divided by the population under 1
  regr[, ln_inc := log(incidence)]
  regr[is.infinite(ln_inc), ln_inc := NA] 
  regr[, ln_vacc := log(vacc_rate)]
  regr[, ln_unvacc := log(1 - vacc_rate)]
  
  # drop if incidence greater than 20,000 cases/100,000 pop 
  regr <- regr[incidence <= 20000, ] 
  
  return(regr)
  
}

### prepare nonfatal regression input
regr <- prep_regression() 
regr <- regr[location_id %in% standard_locations]

# save input for reference
fwrite(regr, file.path(FILEPATH.inputs, paste0("incidence_regression_input.csv")), row.names=FALSE)
#***********************************************************************************************************************

if(run_for_rrs){
  #model with coverage (not ln(1-coverage)) as covariate so can calculate rr as exp(beta)
  
  if(custom_version == "09.17.20"){
    
    regr <- fread("/FILEPATH/incidence_regression_input.csv")
  }
  source(paste0("/FILEPATH/rr_custom_incidence_model.R"))
  model_for_rr_beta <- fit_model_for_rrs(acause = acause)
  
}

#----MIXED EFFECTS MODEL------------------------------------------------------------------------------------------------
### run mixed effects regression model

if(use_mask_covariate == F){
  
me_model <- lmer(ln_inc ~ ln_unvacc + (1 | ihme_loc_id), data=regr)

}else{
  
  me_model <- lmer(ln_inc ~ ln_unvacc + ln_no_mask + (1 | ihme_loc_id), data=regr)
}

# save log: summary of model and RDS of model object
capture.output(summary(me_model), file = file.path(FILEPATH.logs, "log_incidence_mereg.txt"), type="output")
saveRDS(me_model, file=file.path(FILEPATH.logs, "log_incidence_mereg.RDS"))

#***********************************************************************************************************************

#----DRAWS--------------------------------------------------------------------------------------------------------------
set.seed(0311)

### prep prediction frame
draws <- merge(covar, sum_pop, by=c("location_id", "year_id"), all.x=TRUE)
draws[, ln_unvacc := log(1 - DTP3)]
N <- nrow(draws)

# Get random effects from mixed effects model
reffect <- data.frame(ranef(me_model)[[1]])
setnames(reffect, "X.Intercept.", "random_effect")
setorder(reffect, -random_effect) 

# Format to save 
reffect$ihme_loc_id <- rownames(reffect)
rownames(reffect) <- NULL
setDT(reffect)
fwrite(reffect, paste0(FILEPATH.logs, "/random_effect_table.csv"))

# Choose which underreporting scaling approach to use:
if (scalar_method == "predictions"){

# Choose which random effect to use

if(which_re == "CHE"){
# standard random effect set to Switzerland, where the pertussis monitoring system is thought to capture a large percentage of cases
if (CHE_RE=="gbd2017") {
  reffect_MOD <- 4.639529  
} else {
  reffect_MOD <- reffect$random_effect[reffect$ihme_loc_id=="CHE"]
}
cat(paste0("CHE random effect used (NF) =", reffect_MOD), file=file.path(FILEPATH.logs, "log_incidence_mereg.txt"), sep="\n", append=TRUE)

} else if (which_re == "drq"){
  # get data rich location assignment 
  stars <- fread("/FILEPATH/stars_by_iso3_time_window.csv")
  setDT(stars)
  
  #keep star rating used in COD models
  stars <- stars[time_window == "full_time_series" & stars >=4, ]
  
  # identify data rich locations in regression file
  star_locs <- unique(regr$ihme_loc_id[regr$ihme_loc_id%in%unique(stars$ihme_loc_id)])
  
  # Select locations in random effect frame that are data-rich locations
  reffect_dq <- reffect[ihme_loc_id %in% star_locs, ]
  
  # calculate mean of star location random effect
  reffect_MOD <- mean(reffect_dq$random_effect)
  
  # Record RE
  cat(paste0("Random effect for mean COD data rich locations ", reffect_MOD), file=file.path(FILEPATH.logs, "log_incidence_mereg.txt"), sep="\n", append=TRUE)

} else if (which_re == "top_5"){
  
  reffect_MOD <- mean(head(reffect$random_effect, 5))
  # Record RE
  cat(paste0("Random effect for top 5 locations ", reffect_MOD), file=file.path(FILEPATH.logs, "log_incidence_mereg.txt"), sep="\n", append=TRUE)
  
} else if (scalar_method == "incidence") { 
  source("/FILEPATH/re_incidence_adjustment.R") 
}
}

# Create the coefficient matrix

if(use_mask_covariate == F){
  
# Extract ln_unvaccinated coefficient from model
beta1 <- coef(me_model)[[1]] %>% data.frame
beta1 <- unique(beta1$ln_unvacc)

# Extract fixed effects from model
beta0 <- fixef(me_model)[[1]] %>% data.frame

# Create a dataframe with beta0 (fixed effect), beta1 (ln_unvaccinated) and reference random effect
coeff <- cbind(beta0, beta1, reffect_MOD)
colnames(coeff) <- c("constant", "b_unvax", "b_reffect_MOD")

# remove RE term to create the coefficient matrix
coefmat <- coeff[, !colnames(coeff)=="b_reffect_MOD"]
coefmat <- matrix(unlist(coefmat), ncol=2, byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coefmat)))))

# covariance matrix
vcovs <- vcov(me_model) 
vcovlist <- c(vcovs[1,1], vcovs[1,2], vcovs[2,1], vcovs[2,2])
vcovmat <- matrix(vcovlist, ncol=2, byrow=TRUE)

# create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
betadraws <- rmvnorm(n=1000, mean=coefmat, sigma=vcovmat)

# add random effect to coefficient draws
betadraws <- cbind(betadraws, rep(reffect_MOD, 1000))

# transpose coefficient matrix
betas <- t(betadraws)

### generate draws of the prediction using coefficient draws
draw_nums <- 1:1000
draw_cols <- paste0("case_draw_", draw_nums_gbd)
invisible(
  draws[, (draw_cols) := lapply(draw_nums, function(x) { exp( betas[1, x] +
                                                                ( betas[2, x] * ln_unvacc ) +
                                                                ( betas[3, x] ) ) *
      ( pop / 100000 ) } )]
)
}else{
  betas <- coef(me_model)[[1]] %>% data.frame
  beta1 <- unique(betas$ln_unvacc)
  beta2 <- unique(betas$ln_no_mask)
  
  # Extract fixed effects from model
  beta0 <- fixef(me_model)[[1]] %>% data.frame
  
  # Create a dataframe with beta0 (fixed effect), beta1 (ln_unvaccinated) and random effect
  coeff <- cbind(beta0, beta1, beta2, reffect_MOD)
  colnames(coeff) <- c("constant", "b_unvax", "b_no_mask", "b_reffect_MOD")
  
  # remove CHE RE term to create the coefficient matrix
  coefmat <- coeff[, !colnames(coeff)=="b_reffect_MOD"]
  coefmat <- matrix(unlist(coefmat), ncol=3, byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coefmat)))))
  
  # covariance matrix
  vcovs <- vcov(me_model) 
  # vcovlist <- c(vcovs[1,1], vcovs[1,2], vcovs[2,1], vcovs[2,2])
  vcovmat <- matrix(vcovs, ncol= length(coefmat), byrow=TRUE)
  
  # create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
  betadraws <- rmvnorm(n=1000, mean=coefmat, sigma=vcovmat)
  
  # add random effect of CHE to coefficient draws
  betadraws <- cbind(betadraws, rep(reffect_MOD, 1000))
  # betadraws <- cbind(betadraws, rep(mean_hc_re, 1000))
  
  # transpose coefficient matrix
  betas <- t(betadraws)
  
  ### generate draws of the prediction using coefficient draws
  draw_nums <- 1:1000
  draw_cols <- paste0("case_draw_", draw_nums_gbd)
  invisible(
    draws[, (draw_cols) := lapply(draw_nums, function(x) { exp( betas[1, x] +
                                                                  ( betas[2, x] * ln_unvacc ) +
                                                                  ( betas[3, x] * ln_no_mask ) +
                                                                  ( betas[4, x] ) ) *
        ( pop / 100000 ) } )]
  )
}
#***********************************************************************************************************************

# Correct incidence in locations where predictions are lower than case notifications

# Create mean of draws
mean_draws <- draws[, mean_draw := rowMeans(.SD), .SDcols = draw_cols]

# Keep needed columns and merge with notifications
mean_draws <- dplyr::select(mean_draws, -contains("case_draw"))

# Merge with regression file
mean_draws <- merge(mean_draws, regr[, .(location_id, year_id, cases)], by = c("location_id", "year_id"), all.y = T)

# Create a dataframe for underestimated parent locations
under <- mean_draws[mean_draw < cases, ] 

# Create a location year column
under[, location_year := paste0(ihme_loc_id, "_", year_id) ]
unique(under$location_year)

# Create scalar 
under[, scalar := cases/mean_draw]
under[, abs_diff := cases - mean_draw]

# Save files for records of which locations were adjusted
if(WRITE_FILES == "yes"){
  fwrite(under, file.path(FILEPATH.logs, "case_notification_rescaled_loc_years.csv"), row.names=FALSE)
}

# identify locations in draws including all sunbationals that need to be readjusted
draws[, country_code := substr(ihme_loc_id, 1, 3)]

# create location_year column
draws[, location_year := paste0(country_code, "_", year_id)]

# mark rows to replace with mean of case notifications
draws[, replace_draws := ifelse(location_year %in% under$location_year, 1, 0)] 

# Merge the scalar with the draws
draws_temp <- merge(draws, under[, .(location_year, scalar)], by = "location_year", all.x = T)
draws_temp[, scalar := ifelse(is.na(scalar), 1, scalar)]

# Multiple draws by scalar
draws_temp[, (draw_cols) := lapply(.SD, function(x) x * scalar), .SDcols = draw_cols]

# Calculate mean of draws
mean_draws_2 <- draws_temp[, mean_draw := rowMeans(.SD), .SDcols = draw_cols]

# Keep needed columns and merge with notifications
mean_draws_2 <- dplyr::select(mean_draws_2, -contains("case_draw"))

# Merge with regression file
mean_draws_2 <- merge(mean_draws_2, regr[, .(location_id, year_id, cases)], by = c("location_id", "year_id"), all.y = T)

# Create a dataframe for underestimated parent locations with at least 1 case fewer predicted than notified
under_2 <-  mean_draws_2[mean_draw < cases - 1, ] #transformation applied correctly

if(nrow(under_2 > 0)){
  message("Stop! your scalar was not applied correctly")
  stop()
}else{
  print("Proceed there are no estimates lower than notifications!")
}

# Rename file
draws <- copy(draws_temp)

#----SAVE---------------------------------------------------------------------------------------------------------------
# save results
save_case_draws <- draws[, c("location_id", "year_id", draw_cols), with=FALSE]

if (WRITE_FILES == "yes") {
  write.csv(save_case_draws, file.path(FILEPATH, "01_case_predictions_from_model.csv"), row.names=FALSE)
}
#***********************************************************************************************************************

########################################################################################################################
##### PART TWO: RESCALE CASES ##########################################################################################
########################################################################################################################

#----RESCALE------------------------------------------------------------------------------------------------------------
### run custom rescale function
rescaled_cases <- rake(save_case_draws, measure="case_draw_")

### save results
if (WRITE_FILES == "yes") {
  
  fwrite(rescaled_cases, file.path(FILEPATH, "02_case_predictions_rescaled.csv"), row.names=FALSE)
  
  # make collapsed file and save that for location specific case vetting
  rescaled_cases_collapsed <- collapse_point(rescaled_cases, draws_name="case_draw")
  fwrite(rescaled_cases_collapsed, file.path(FILEPATH, "025_case_predictions_rescaled_collapsed.csv"), row.names=FALSE)

  message("Rescaled cases saved")

  }
#***********************************************************************************************************************


# If running following code with existing rescaled estimates uncomment this ##
# rescaled_cases <- fread("/FILEPATH/02_case_predictions_rescaled.csv")

if (CALCULATE_NONFATAL == "yes") {
  
  
  ########################################################################################################################
  ##### PART THREE: CASES AGE-SEX SPLIT ##################################################################################
  ########################################################################################################################
  
  
  #----SPLIT--------------------------------------------------------------------------------------------------------------
  ### split pertussis cases by age/sex pattern from CoD database
  split_cases <- age_sex_split(cause_id=cause_id, input_file=rescaled_cases, measure="case")
  
  ### save split draws
  if (WRITE_FILES == "yes") {
    
    fwrite(split_cases, file.path(FILEPATH, "03_case_predictions_split.csv"), row.names=FALSE)
    
    # make collapsed file and save that for location/age/sex specific case vetting
    split_cases_collapsed <- collapse_point(split_cases[age_group_id %in% c(2, 3, 388, 389, 238, 34)], draws_name="split_case_draw")  # this is just the u5s
    fwrite(split_cases_collapsed, file.path(FILEPATH, "035_case_predictions_split_collapsed_u5.csv"), row.names=FALSE)
  
    message("Split cases saved")
    }
  #***********************************************************************************************************************
  
  ########################################################################################################################
  ##### PART FOUR: CASES TO PREVALENCE AND INCIDENCE #####################################################################
  ########################################################################################################################
  
  # DATA CHECK 
    if(sum(is.na(split_cases > 0 ))){
      message("STOP! there are NAs in your data!")
    }else{
      print("Continue, there are no NAs in your data")
    }
    
  #----PREVALENCE---------------------------------------------------------------------------------------------------------
  ### convert cases to prevalence, duration of 50 days
  invisible( split_cases[, (draw_cols_gbd) := lapply( draw_nums_gbd, function(x) (get(paste0("split_case_draw_", x)) * (50 / 365)) / population )] )
  predictions_prev_save <- split_cases[, c("location_id", "year_id", "age_group_id", "sex_id", draw_cols_gbd), with=FALSE]
  
  # Make sure prevalence does not exceed 1
  check <- dplyr::select(predictions_prev_save, contains("draw"))
  if(sum(check > 1)){
    message("Prevalence exceeds 1 in your data!")
    stop()
  }else{
    print("Prevalence is <1 in all draws, proceed")
  }
  #***********************************************************************************************************************
  
  #----INCIDENCE---------------------------------------------------------------------------------------------------------
  ### convert cases to incidence rate
  invisible(split_cases[, (draw_cols_gbd) := lapply( draw_nums_gbd, function(x) get(paste0("split_case_draw_", x)) / population )] )
  predictions_inc_save <- split_cases[, c("location_id", "year_id", "age_group_id", "sex_id", draw_cols_gbd), with=FALSE]
  
  #***********************************************************************************************************************
  
  ########################################################################################################################
  ##### PART FIVE: FORMAT FOR COMO #######################################################################################
  ########################################################################################################################
  
  #----SAVE RESULTS-------------------------------------------------------------------------------------------------------
  ### format prevalence for como
  predictions_prev_save[, measure_id := 5]
  
  ### format incidence for como
  predictions_inc_save[, measure_id := 6]
  
  ### save flat files for upload
  save_nonfatal <- rbind(predictions_prev_save, predictions_inc_save)
  save_nonfatal <- save_nonfatal[age_group_id %in% c(age_start:age_end, 388, 389, 238, 34), ] # ER: added new 2020 age groups
  
  if(WRITE_FILES == "yes"){
    fwrite(save_nonfatal, file.path(FILEPATH, "04_save_nonfatal.csv"), row.names=FALSE)
  }
  message("non fatal predictions file saved, proceed with separating file by location id")
  
  # Save each location id as separate csv file
  invisible( lapply(unique(save_nonfatal$location_id), function(x) fwrite(save_nonfatal[location_id==x, ], 
                                                                          file.path(cl.version.dir, paste0(x, ".csv")), row.names = FALSE)) )
  print(paste0("nonfatal estimates saved in ", cl.version.dir))
  
  # save pertussis incidence draws, modelable_entity_id=1424
  # for slurm
  job_nf <- paste0("sbatch -J s_epi_", acause, " --mem 150G -c 5 -C archive -t 10:00:00 -A ", cluster_proj, " -p all.q -o /FILEPATH/",
                   username, "/%x.o%j",
                   " /FILEPATH/execRscript.sh -s ", 
                   paste0("/FILEPATH/save_results_wrapper.r"),
                   " --year_ids ", paste(unique(save_nonfatal$year_id), collapse=","),
                   " --type epi",
                   " --me_id ", me_id,
                   " --input_directory ", cl.version.dir,
                   " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
                   " --best ", mark_model_best,
                   " --release_id ", release_id, 
                   " --xw_id ", incidence_xw_id,
                   " --bundle_id ", incidence_bundle_id)
  
  print(job_nf)
  system(job_nf)
  #***********************************************************************************************************************
  
}

if (CALCULATE_COD == "yes") {
  
  ########################################################################################################################
  ##### PART SIX: MODEL CFR ##############################################################################################
  ########################################################################################################################
  
  #----PREP---------------------------------------------------------------------------------------------------------------
  ### GET COVARIATES
  
    # LDI: covariate id 57, Lag distributed income per capita 
    ldi <- get_covariate_estimates(covariate_id=57,
                                   year_id=1980:year_end, 
                                   location_id=pop_locs, 
                                   release_id = release_id)
    ldi[, ln_ldi := log(mean_value)]
    setnames(ldi, "mean_value", "ldi")
    # save model version
    cat(paste0("Covariate LDI (CoD) - model version ", unique(ldi$model_version_id)), file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
    
    # MCI: covariate_id 208, Maternal care and immunization
    mci <- get_covariate_estimates(covariate_id=208, 
                                   year_id=1980:year_end, 
                                   location_id=pop_locs, 
                                   release_id = release_id) 
    setnames(mci, "mean_value", "health") #bad variable name needs to change! 
    # save model version
    cat(paste0("Covariate MCI capped (CoD) - model version ", unique(mci$model_version_id)), file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
    
    # SEV: covariate id 1230, Age-standardized SEV for Child underweight 
    sev <- get_covariate_estimates(covariate_id=1230, 
                                       year_id=1980:year_end, 
                                       location_id=pop_locs, 
                                       release_id = release_id)
    sev[, ln_mal := log(mean_value)]
    # save model version
    cat(paste0("Covariate SEV (CoD) - model version ", unique(sev$model_version_id)), file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
    
    # HAQI: covariate_id=1099, healthcare access and quality index
    haqi <- get_covariate_estimates(covariate_id=1099, 
                                    year_id=1980:year_end, 
                                    location_id=pop_locs, 
                                    release_id = release_id) 
    setnames(haqi, "mean_value", "HAQI")
    # save model version
    cat(paste0("Covariate HAQI (CoD) - model version ", unique(haqi$model_version_id)), file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
    
    # merge covariates
    cfr_covar <- merge(mci[, c("location_id", "year_id", "health"), with=FALSE], ldi[, c("location_id", "year_id", "ln_ldi", "ldi"), with=FALSE], by=c("location_id", "year_id"), all.x=TRUE)
    cfr_covar <- merge(cfr_covar, sev[, c("location_id", "year_id", "ln_mal"), with=FALSE], by=c("location_id", "year_id"), all.x=TRUE)
    cfr_covar <- merge(cfr_covar, haqi[, c("location_id", "year_id", "HAQI"), with=FALSE], by=c("location_id", "year_id"), all.x=TRUE)
    
  ### GET CFR DATA
    
    if(which_cfr_data == "GBD2023"){
    
    # get cfr data from bundle
    cfr_extractions <- get_bundle_version(bundle_version_id = cfr_bundle_version_id)
    
    # Fix column names for clarity
    setnames(cfr_extractions, c("cases", "sample_size"), c("deaths", "cases"))

    # average year_start and year_end to make year_id variable
    cfr_extractions[, year_id := round((year_start + year_end) / 2, 0)]

    # Extract non aggregated rows 
    cfr_extractions <- cfr_extractions[group_review == 1, ]
    
    # Remove studies conducted exlusively in hospitals 
    cfr_extractions <- cfr_extractions[cv_hospital == 0 | cv_hospital == 2, ] # exclude studies conducted exclusively in hospitals
    
    # Remove all rows with CFR > 0.5 as was done in GBD2021
    cfr_extractions <- cfr_extractions[mean < 0.5, ]

    # do not have any cases that are a fraction of 1
    cfr_extractions[!is.na(cases) & cases < 1 & cases > 0, cases := 1] 
    
    # aggregate by location-year
    cfr_extractions <- cfr_extractions[, sum_cases := sum(cases), by = c("nid", "location_id", "year_id")]
    cfr_extractions <- cfr_extractions[, sum_deaths := sum(deaths), by = c("nid", "location_id", "year_id")]

    # collapse to aggregated rows
    cfr_data_collapsed <- cfr_extractions[, 
                      .(location_id, location_name, year_id, nid, field_citation_value, sum_cases, sum_deaths)] %>% unique() 
    
    # Create CFR variable from aggregated deaths and cases
    cfr_data_collapsed[, cfr := sum_deaths/sum_cases]
    
    # Rename columns
    setnames(cfr_data_collapsed, c("sum_cases", "sum_deaths"), c("cases", "deaths"))
  
    # merge cfr data with covariates
    cfr_data <- merge(cfr_data_collapsed, cfr_covar, by=c("location_id", "year_id"), all.x=TRUE)
    
    # Keep standard locations only
    cfr_data <- cfr_data[location_id %in% standard_locations] 
  
    # record bundle version
    cat(paste0("CFR bundle version ID: ", cfr_bundle_version_id), file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append = T)
    
    # Save CFR regression inputs
    fwrite(cfr_data, file.path(FILEPATH.inputs, "cfr_regression_input.csv"), row.names=FALSE)
  
    
    } else if (which_cfr_data == "GBD2021"){
      
      prep_cfr_data <- function(...) {
        
        ### read in CFR data - GBD 2010 literature review
        cfr <- fread(file.path(home, "FILEPATH/cfr_data_gbd2010.csv")) 
        # prep cfr
        setnames(cfr, c("NID", "Country ISO3 Code", "Parameter Value", "numerator_number_of_cases_with_the_condition", "effective_sample_size", "mid_point_year_of_data_collection", "Age Start", "Age End", "Year Start", "Year End"),
                 c("nid", "ihme_loc_id", "cfr", "deaths", "cases", "year_id", "age_start", "age_end", "year_start", "year_end"))
        # drop outliers
        cfr[, ignore := as.numeric(ignore)]
        cfr[is.na(ignore), ignore := 0]
        if (nrow(cfr[ignore==1, ]) > 0) print(paste0("dropping outliers (", nrow(cfr[ignore==1, ]), " rows)"))
        cfr <- cfr[ignore != 1, ]
        cfr <- cfr[!(ihme_loc_id=="UGA" & year_start==1951), ]
        # cleanup
        cfr[year_id < 1980, year_id := 1980]
        cfr <- cfr[, c("nid", "ihme_loc_id", "year_id", "age_start", "age_end", "deaths", "cases", "cfr"), with=FALSE]
        
        # prep cfr_expert - additional CFR data from experts - GBD 2013 update
        cfr_expert <- fread(file.path(home, "FILEPATH/cfr_data_gbd2013.csv")) 
        setnames(cfr_expert, c("iso3", "mean", "numerator", "denominator", "year_start"), c("ihme_loc_id", "cfr", "deaths", "cases", "year_id"))
        cfr_expert <- cfr_expert[, c("nid", "ihme_loc_id", "year_id", "age_start", "age_end", "deaths", "cases", "cfr"), with=FALSE]
        
        # add in new CFR data from 2016 literature review
        cfr_2016 <- fread(file.path(home, "FILEPATH/cfr_GBD2016_extraction.csv"))
        setnames(cfr_2016, c("cases", "sample_size"), c("deaths", "cases"))
        cfr_2016[, cfr := deaths / cases]
        cfr_2016[, year_id := round((year_start + year_end) / 2, 0)]
        cfr_2016 <- cfr_2016[, c("nid", "ihme_loc_id", "year_id", "age_start", "age_end", "deaths", "cases", "cfr"), with=FALSE]
        
        # merge CFR data
        cfr_temp <- bind_rows(cfr, cfr_expert, cfr_2016)
        cfr_temp[, cfr := NULL]
        cfr_data <- copy(cfr_temp) #otherwise get shallow copy warning
        cfr_data[, cfr := deaths / cases]
        cfr_data <- cfr_data[cfr <= 0.5, ] #removing nid with CFR > 0.5?
        cfr_data[!is.na(cases) & cases < 1 & cases > 0, cases := 1]
        
        # merge with locations
        cfr_data <- merge(cfr_data, locations[, .(location_id, ihme_loc_id)], by = "ihme_loc_id")
        cfr_data <- cfr_data[location_id %in% standard_locations]
        
        # Return dataframe
        return(cfr_data)
      
    }
    
      # Run function to get CFR data
      cfr_data <- prep_cfr_data()
      
      # Merge with covariates
      cfr_data <- merge(cfr_data, cfr_covar, by=c("location_id", "year_id"), all.x=TRUE)
      
      # Round deaths to whole numbers 
      cfr_data[, deaths := round(deaths, 0)]
      
      # Save CFR regression input
      fwrite(cfr_data, file.path(FILEPATH.inputs, "cfr_regression_input.csv"), row.names=FALSE)
    }
    
  #***********************************************************************************************************************
  
  #----CFR REGRESSION-----------------------------------------------------------------------------------------------------
  
  ## Run regression
  if(which_cfr_model == "logit"){
    
  # Create CFR variable for logit transformation 
  cfr_data[, cfr := deaths/cases]
  cfr_data[, offset_cfr := ifelse(cfr == 0, min(cfr_data$cfr[cfr_data$cfr!=0])/2, cfr)]
  cfr_data[, logit_cfr := arm::logit(offset_cfr)]
  
  nb_model <- glm(logit_cfr ~ HAQI, data = cfr_data) 
  
  # save log
  capture.output(summary(nb_model), file = file.path(FILEPATH.logs, "log_cfr_negbin.txt"), type="output")
  saveRDS(nb_model, file = file.path(FILEPATH.logs, "log_cfr_negbin.RDS"))
  
} else if (which_cfr_model == "linear") {

  if (HAQI_or_HSA == "use_HSA") {
    
    nb_model <- MASS::glm.nb(deaths ~ ln_ldi + health + offset(log(cases)), data=cfr_data)
    # save log
    capture.output(summary(nb_model), file = file.path(FILEPATH.logs, "log_cfr_negbin.txt"), type="output")
    saveRDS(nb_model, file = file.path(FILEPATH.logs, "log_cfr_negbin.RDS"))
    
  } else if (HAQI_or_HSA == "use_HAQI") {
    
    nb_model <- MASS::glm.nb(deaths ~ ln_ldi + HAQI + offset(log(cases)), data=cfr_data)
    # save log
    capture.output(summary(nb_model), file = file.path(FILEPATH.logs, "log_cfr_negbin.txt"), type="output")
    saveRDS(nb_model, file = file.path(FILEPATH.logs, "log_cfr_negbin.RDS"))
    
  } else if (HAQI_or_HSA == "HAQI_only") {
    
    nb_model <- MASS::glm.nb(deaths ~ HAQI + offset(log(cases)), data=cfr_data)
    # save log
    capture.output(summary(nb_model), file = file.path(FILEPATH.logs, "log_cfr_negbin.txt"), type="output")
    saveRDS(nb_model, file = file.path(FILEPATH.logs, "log_cfr_negbin.RDS"))

  } else if (HAQI_or_HSA == "LDI_only") {
    
    nb_model <- MASS::glm.nb(deaths ~ ln_ldi + offset(log(cases)), data=cfr_data)
    # save log
    capture.output(summary(nb_model), file = file.path(FILEPATH.logs, "log_cfr_negbin.txt"), type="output")
    saveRDS(nb_model, file = file.path(FILEPATH.logs, "log_cfr_negbin.RDS"))
    
  }
}
  #***********************************************************************************************************************
  
  #----DRAWS--------------------------------------------------------------------------------------------------------------
  set.seed(0311)
  
  ### predict out for all country-year-age-sex
  pred_CFR <- merge(sum_pop, cfr_covar, by=c("location_id", "year_id"))
  N <- nrow(pred_CFR)
  
  ### Get coefficient matrix
  coefmat <- c(coef(nb_model))
  names(coefmat)[1] <- "constant"
  names(coefmat) <- paste("b", names(coefmat), sep = "_")
  coefmat <- matrix(unlist(coefmat), ncol=length(coefmat), byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coefmat)))))
  
  # covariance matrix
  vcovmat <- vcov(nb_model)
  
  # create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
  betadraws <- rmvnorm(n=1000, mean=coefmat, sigma=vcovmat)
  betas <- t(betadraws)
  
  ### estimate deaths
  cfr_draw_cols <- paste0("cfr_draw_", draw_nums_gbd)
  
  if(which_cfr_model == "logit"){
    
    #Add covariate toggles here as below! 
    pred_CFR[, (cfr_draw_cols) := lapply(draw_nums_gbd, function(i) exp( betas[1, (i + 1)] +
                                                                             ( betas[2, (i + 1)] * HAQI ))
    )]
    
    
  } else if (which_cfr_model == "linear") {
    
  # create draws of disperion parameter
  alphas <- 1 / exp(rnorm(1000, mean=nb_model$theta, sd=nb_model$SE.theta))
  
  # generate draws of the prediction using coefficient draws
  if (HAQI_or_HSA == "use_HSA") {
    if (GAMMA_EPSILON == "without") {
      pred_CFR[, (cfr_draw_cols) := lapply(draw_nums_gbd, function(i) exp( betas[1, (i + 1)] +
                                                                             ( betas[2, (i + 1)] * ln_ldi ) +
                                                                             ( betas[3, (i + 1)] * health ) )
      )]
    } else if (GAMMA_EPSILON == "with") {
      pred_CFR[, (cfr_draw_cols) := lapply(draw_nums_gbd, function(i) rgamma( N,
                                                                              scale=(alphas[i + 1] *
                                                                                       exp( betas[1, (i + 1)] +
                                                                                              ( betas[2, (i + 1)] * ln_ldi ) +
                                                                                              ( betas[3, (i + 1)] * health ) )), shape=(1 / alphas[i + 1]) )
      )]
    }
  } else if (HAQI_or_HSA == "use_HAQI") {
    if (GAMMA_EPSILON == "without") {
      pred_CFR[, (cfr_draw_cols) := lapply(draw_nums_gbd, function(i) exp( betas[1, (i + 1)] +
                                                                             ( betas[2, (i + 1)] * ln_ldi ) +
                                                                             ( betas[3, (i + 1)] * HAQI ) )
      )]
    } else if (GAMMA_EPSILON == "with") {
      pred_CFR[, (cfr_draw_cols) := lapply(draw_nums_gbd, function(i) rgamma( N,
                                                                              scale=(alphas[i + 1] *
                                                                                       exp( betas[1, (i + 1)] +
                                                                                              ( betas[2, (i + 1)] * ln_ldi ) +
                                                                                              ( betas[3, (i + 1)] * HAQI ) )), shape=(1 / alphas[i + 1]) )
      )]
    }
  } else if (HAQI_or_HSA == "HAQI_only") {
    if (GAMMA_EPSILON == "without") {
      pred_CFR[, (cfr_draw_cols) := lapply(draw_nums_gbd, function(i) exp( betas[1, (i + 1)] +
                                                                             ( betas[2, (i + 1)] * HAQI ) )
      )]
    } else if (GAMMA_EPSILON == "with") {
      pred_CFR[, (cfr_draw_cols) := lapply(draw_nums_gbd, function(i) rgamma( N,
                                                                              scale=(alphas[i + 1] *
                                                                                       exp( betas[1, (i + 1)] +
                                                                                              ( betas[2, (i + 1)] * HAQI ) )), shape=(1 / alphas[i + 1]) )
      )]
    }
  }
  
  }
  # save results
  CFR_draws_save <- pred_CFR[, c("location_id", "year_id", cfr_draw_cols), with=FALSE]
  if (WRITE_FILES == "yes") {
    fwrite(CFR_draws_save, file.path(FILEPATH, paste0("04_cfr_model_draws.csv")), row.names=FALSE)
    message("CFR draws saved")
  }

  #***********************************************************************************************************************
  
  
  ########################################################################################################################
  ##### PART SEVEN: CALCULATE DEATHS #####################################################################################
  ########################################################################################################################
  
  #if using prior incidence estimates to calculate deaths import here
  if(CALCULATE_NONFATAL == "no"){
  rescaled_cases <- fread(file.path("/FILEPATH",
                                    non_fatal_run_date,"02_case_predictions_rescaled.csv"))
  } else {
    message("Nonfatal estimates are in global environment")
  }
  
  #----CALCULATE DEATHS---------------------------------------------------------------------------------------------------
  ### merge cases and cfr
  predictions_deaths <- merge(rescaled_cases, CFR_draws_save, by=c("location_id", "year_id"), all.x=TRUE)
  
  ### calculate deaths from cases and CFR
  death_draw_cols <- paste0("death_draw_", draw_nums_gbd)
  predictions_deaths[, (death_draw_cols) := lapply(draw_nums_gbd, function(ii) get(paste0("cfr_draw_", ii)) * get(paste0("case_draw_", ii)) )]
  
  ### save results
  predictions_deaths_save <- predictions_deaths[, c("location_id", "year_id", death_draw_cols), with=FALSE]
  
  if (WRITE_FILES == "yes") {
    fwrite(predictions_deaths_save, file.path(FILEPATH, "05_death_predictions_from_model.csv"), row.names=FALSE)
    message("Death predictions from model saved")
  }
  
  #***********************************************************************************************************************
  
  ########################################################################################################################
  ##### PART EIGHT: RESCALE DEATHS #######################################################################################
  ########################################################################################################################
  
  #----RESCALE------------------------------------------------------------------------------------------------------------
  ### run custom rescale function
  rescaled_deaths <- rake(predictions_deaths_save, measure="death_draw_")
  
  ### save results
  if (WRITE_FILES == "yes") {
    fwrite(rescaled_deaths, file.path(FILEPATH, "06_death_predictions_rescaled.csv"), row.names=FALSE)
    message("Rescaled deaths have been saved")
  }
  
  #***********************************************************************************************************************
  
 
  ########################################################################################################################
  ##### PART NINE: DEATHS AGE-SEX SPLIT ##################################################################################
  ########################################################################################################################
  
  
  #----SPLIT--------------------------------------------------------------------------------------------------------------
  ### age-sex split using demographic pattern in cause of death data
  split_deaths <- age_sex_split(cause_id=cause_id, input_file=rescaled_deaths, measure="death")
  
  ### save split draws
  split_deaths_save <- split_deaths[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("split_death_draw_", 0:999)), with=FALSE]
  
  if (WRITE_FILES == "yes") {
    fwrite(split_deaths_save, file.path(FILEPATH, "07_death_predictions_split.csv"), row.names=FALSE)
    message("Split deaths have been saved")
  }
  
  #***********************************************************************************************************************
  
  ### GBD 2023 reupload with final Envelope:
  # split_deaths_save <- fread("/FILEPATH/07_death_predictions_split.csv")
  
  #######################################################################################################################
  ##### PART TEN: COMBINE CODEm DEATHS ##################################################################################
  ########################################################################################################################
  
  #----COMBINE------------------------------------------------------------------------------------------------------------
  ### read CODEm COD results for data-rich countries
  cod_M <- data.table(pandas$read_hdf(file.path("/FILEPATH", paste0("draws/deaths_", "male", ".h5")), key="data"))
  cod_F <- data.table(pandas$read_hdf(file.path("/FILEPATH", paste0("draws/deaths_", "female", ".h5")), key="data"))

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
  
  # Hybridize data-rich and custom models
  data_rich <- unique(cod_DR$location_id)
  split_deaths_glb <- split_deaths_save[!location_id %in% data_rich, ]
  colnames(split_deaths_glb) <- draw_cols_upload
  split_deaths_hyb <- rbind(split_deaths_glb, cod_DR)
  
  # Keep only needed age groups
  split_deaths_hyb <- split_deaths_hyb[age_group_id %in% c(age_start:age_end, 388, 389, 238, 34), ] 
  
  #***********************************************************************************************************************
  
  ########################################################################################################################
  ##### PART ELEVEN: FORMAT FOR CODCORRECT ###############################################################################
  ########################################################################################################################
  
  #----SAVE RESULTS-------------------------------------------------------------------------------------------------------
  ### format for codcorrect
  split_deaths_hyb[, measure_id := 1]
  
  if(WRITE_FILES == "yes"){
    fwrite(split_deaths_hyb, file.path(FILEPATH, "08_death_predictions_hybrid.csv"), row.names=FALSE)
    message("Hybridized deaths saved")
  }
  
  ### Save prediction by location to the shared directory
  lapply(unique(split_deaths_hyb$location_id), function(x) fwrite(split_deaths_hyb[location_id==x, ], file.path(cl.death.dir, paste0(x, ".csv")), row.names=FALSE)) 
  print(paste0("death draws saved in ", cl.death.dir)) 
  
  ### save results to central data base 
  job <- paste0("sbatch -J s_cod_", acause, " --mem 100G -c 5 -C archive -t 24:00:00 -A proj_cov_vpd -p all.q -o /FILEPATH/",
                username, "/%x.o%j",
                " /FILEPATH/execRscript.sh -s ", 
                paste0("/FILEPATH/save_results_wrapper.r"),
                # " --year_ids ", paste(1980:year_end, collapse=","),
                " --year_ids ", paste(unique(split_deaths_hyb$year_id), collapse=","),
                " --type cod",
                " --me_id ", cause_id,
                " --input_directory ", cl.death.dir,
                " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
                " --best ", mark_model_best,
                # " --gbd_round ", gbd_round,
                " --release_id ", release_id)
  print(job)
  system(job)
  #***********************************************************************************************************************
}

