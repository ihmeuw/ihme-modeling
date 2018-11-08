#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: Model measles fatal and nonfatal outcomes for GBD
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
pacman::p_load(magrittr, foreign, stats, MASS, dplyr, plyr, lme4, reshape2, parallel, rhdf5)
source("FILEPATH")
load_packages(c("data.table", "mvtnorm")) # "pscl"

### set data.table fread threads to 1
setDTthreads(1)
#***********************************************************************************************************************


#----DIRECTORIES--------------------------------------------------------------------------------------------------------
### set objects
acause    <- "measles"
age_start <- 4  #2           ## age "early neonatal"
age_end   <- 16              ## age 55-59 years
a         <- 3               ## birth cohort years before 1980
cause_id  <- 341
me_id     <- 1436
gbd_round <- 5
year_end  <- gbd_round + 2012

### draw numbers
draw_nums_gbd    <- 0:999
draw_cols_gbd    <- paste0("draw_", draw_nums_gbd)
draw_cols_upload <- c("location_id", "year_id", "age_group_id", "sex_id", draw_cols_gbd)

### make folders on cluster
# mortality
cl.death.dir <- file.path("FILEPATH", acause, "mortality", custom_version, "draws") 
if (!dir.exists(cl.death.dir) & CALCULATE_COD=="yes") dir.create(cl.death.dir, recursive=TRUE)
# nonfatal
cl.version.dir <- file.path("FILEPATH", acause, "nonfatal", custom_version, "draws")
if (!dir.exists(cl.version.dir) & CALCULATE_NONFATAL=="yes") dir.create(file.path(cl.version.dir), recursive=TRUE)
# shocks
cl.shocks.dir <- file.path("FILEPATH", acause, "nonfatal", custom_version, "shocks_adjustment")
if (!dir.exists(cl.shocks.dir) & run_shocks_adjustment) dir.create(file.path(cl.shocks.dir), recursive=TRUE)

### directories
home <- file.path(j_root, "FILEPATH", acause, "00_documentation")
j.version.dir <- file.path(home, "models", custom_version)
j.version.dir.inputs <- file.path(j.version.dir, "model_inputs")
j.version.dir.logs <- file.path(j.version.dir, "model_logs")
if (!dir.exists(j.version.dir.inputs)) dir.create(j.version.dir.inputs, recursive=TRUE)
if (!dir.exists(j.version.dir.logs)) dir.create(j.version.dir.logs, recursive=TRUE)

### save description of model run
# which model components are being uploaded?
if (CALCULATE_NONFATAL=="yes" & CALCULATE_COD=="no") add_  <- "NF"
if (CALCULATE_COD=="yes" & CALCULATE_NONFATAL=="no") add_  <- "CoD"
if (CALCULATE_NONFATAL=="yes" & CALCULATE_COD=="yes") add_ <- "NF and CoD"
# record CODEm data-rich feeder model versions used in CoD hybridization
if (CALCULATE_COD=="yes") description <- paste0("DR M ", male_CODEm_version, ", DR F ", female_CODEm_version, ", ", description)
# save full description to model folder
description <- paste0(add_, " - ", description)
cat(description, file=file.path(j.version.dir, "MODEL_DESCRIPTION.txt"))
#***********************************************************************************************************************


#----FUNCTIONS----------------------------------------------------------------------------------------------------------  
### load shared functions
file.path(j_root, "FILEPATH/get_population.R") %>% source
file.path(j_root, "FILEPATH/get_envelope.R") %>% source
file.path(j_root, "FILEPATH/get_location_metadata.R") %>% source
file.path(j_root, "FILEPATH/get_covariate_estimates.R") %>% source
file.path(j_root, "FILEPATH/get_cod_data.R") %>% source

### load personal functions
"FILEPATH/read_hdf5_table.R" %>% source
"FILEPATH/read_excel.R" %>% source
"FILEPATH/rake.R" %>% source
"FILEPATH/collapse_point.R" %>% source
"FILEPATH/sql_query.R" %>% source
file.path(home, "FILEPATH/age_sex_split.R") %>% source
#*********************************************************************************************************************** 


########################################################################################################################
##### PART ONE: INCIDENCE NAT HIST MODEL ###############################################################################
########################################################################################################################


#----PREP---------------------------------------------------------------------------------------------------------------
### get location data
locations <- get_location_metadata(gbd_round_id=gbd_round, location_set_id=22)[, 
                 .(location_id, ihme_loc_id, location_name, location_ascii_name, region_id, super_region_id, super_region_name, level, location_type, parent_id, sort_order)]
pop_locs <- unique(locations[level >= 3, location_id])

### read population file
# get population for birth cohort at average age of notification by year
population <- get_population(location_id=pop_locs, year_id=(1980-a):year_end, age_group_id=c(2:20, 30:32, 235), status="best", 
                             sex_id=1:3, gbd_round_id=gbd_round)
population_young <- population[age_group_id %in% c(2:4) & sex_id==3, ]
population_young <- population_young[, year_id := year_id + a] %>% .[year_id <= year_end, ]
# collapse by location-year
sum_pop_young <- population_young[, pop := sum(population), by=c("location_id", "year_id")] %>% .[, .(location_id, year_id, pop)] %>% unique
# save model version
cat(paste0("Population - model run ", unique(population$run_id)), 
    file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

### prep SIA data
prep_sia <- function(...) {
  
  ### correct SIA coverage
  source("FILEPATH/adjust_sia_data.R")
  SIA <- adjust_sia_data()
  setnames(SIA, "corrected_target_coverage", "supp")
  
  # don't want coverage >100%; if campaign targeted entire population rather than just <15, this would be possible in our measure
  SIA[supp > 1, supp := 0.99]
  
  # apply SIAs in Sudan pre-2011 to South Sudan
  SIA_s_sudan <- subset(SIA, location_id==522 & year_id < 2011)
  SIA_s_sudan$location_id <- 435
  SIA <- rbind(SIA, SIA_s_sudan)
  
  # apply SIAs to subnational units
  SIA_sub <- copy(SIA)
  # make df of unique subnational locations 
  loc_subs <- locations[level > 3, c("location_id", "parent_id")]
  # subset SIAs to only admin0 locations with subnational units
  SIA_sub <- SIA_sub[SIA_sub$location_id %in% unique(loc_subs$parent_id), ]
  setnames(SIA_sub, "location_id", "parent_id")
  
  # level 1
  subs_level1 <- join(SIA_sub, loc_subs, by="parent_id", type="inner")
  
  # level 2
  subs_level2 <- subs_level1[, .(location_id, year_id, supp)]
  setnames(subs_level2, "location_id", "parent_id")
  subs_level2 <- join(subs_level2, loc_subs, by="parent_id", type="inner")
  subs_level2 <- subs_level2[!is.na(location_id), ]
  
  # level 3
  subs_level3 <- subs_level2[, .(location_id, year_id, supp)]
  setnames(subs_level3, "location_id", "parent_id")
  subs_level3 <- join(subs_level3, loc_subs, by="parent_id", type="inner")
  subs_level3 <- subs_level3[!is.na(location_id), ]
  
  # bring together SIAs for all subnational levels
  SIA_subs <- bind_rows(subs_level1, subs_level2, subs_level3)[, parent_id := NULL]
  
  # add subnational SIAs to national SIA df
  SIA <- bind_rows(SIA, SIA_subs)
  SIA <- SIA[!duplicated(SIA[, c("location_id", "year_id")]), ]
  
  # add lag to SIA, 1 to 5 years
  for (i in 1:5) {
    assign(paste("lag", i, sep="_"), copy(SIA))
    get(paste("lag", i, sep="_"))[, year_id := year_id + i]
    setnames(get(paste("lag", i, sep="_")), "supp", paste0("supp_", i))
  }
  
  # bring lags together
  SIAs <- join_all(list(SIA, lag_1, lag_2, lag_3, lag_4, lag_5), by=c("location_id", "year_id"), type="full") %>% as.data.table
  
  # make missing SIAs 0
  for (i in 1:5) {
    SIAs[is.na(get(paste0("supp_", i))), paste0("supp_", i) := 0]
    SIAs[get(paste0("supp_", i))==0, paste0("supp_", i) := 0.000000001]
  }
  
  return(SIAs)
  
}

### prep incidence (case notification) data
prep_case_notifs <- function(add_subnationals=FALSE) {
  
  # get WHO case notification data, downloaded from http://www.who.int/immunization/monitoring_surveillance/data/en/
  case_notif <- read_excel("FILEPATH/incidence_series.xls", sheet="Measles") %>% data.table
  case_notif[, c("WHO_REGION", "Cname", "Disease") := NULL]
  setnames(case_notif, "ISO_code", "ihme_loc_id")
  case_notif <- melt(case_notif, variable.name="year_id", value.name="cases")
  case_notif[, year_id := year_id %>% as.character %>% as.integer]
  case_notif[, nid := 83133]
  
  # get supplement
  case_notif_supp <- fread("FILEPATH/measles_incidence_supplement.csv")
  case_notif_supp_who <- read_excel("FILEPATH/measlesreportedcasesbycountry.xls", sheet="WEB", skip=3) %>% data.table
  case_notif_supp_who <- case_notif_supp_who[, c(2:3, 16)]
  colnames(case_notif_supp_who) <- c("location_name", "ihme_loc_id", "cases")
  case_notif_supp_who[, cases := as.numeric(cases)]
  case_notif_supp_who <- case_notif_supp_who[!is.na(ihme_loc_id)]
  case_notif_supp_who[, year_id := 2017]
  case_notif_supp_who[, nid := 83133]
  case_notif_supp <- case_notif_supp[!(year_id==2017 & ihme_loc_id %in% case_notif_supp_who$ihme_loc_id), ]
  #case_notif <- rbind(case_notif, case_notif_supp, case_notif_supp_who, fill=TRUE)[, location_name := NULL] # don't add - JRF has been updated through 2017
  
  # bring together and clean
  case_notif <- merge(case_notif, locations[, .(location_id, ihme_loc_id)], by="ihme_loc_id", all.x=TRUE) 
  
  if (add_subnationals) {
    
    ### add subnational case notifications that we trust
    # Japan
    JPN_data_subnational <- list.files(file.path(j_root, "FILEPATH/INFECTIOUS_DISEASE_WEEKLY_REPORT"), 
                                       full.names=TRUE, pattern=".csv", ignore.case=TRUE)
    JPN <- lapply(JPN_data_subnational, function(x) { 
           dt <- read.csv(x, skip=3) %>% as.data.table
           colnums <- grep("Measles", colnames(dt)) + 1
           dt <- dt[-1, c(1, colnums), with=FALSE]
           colnames(dt) <- c("location_ascii_name", "cases")
           dt[, cases := cases %>% as.integer]
           dt[, year_id := substring(x, 98, 101) %>% as.integer] 
    }) %>% rbindlist(., fill=TRUE)
    JPN <- JPN[location_ascii_name != "Total No."]
    JPN_nids <- data.table(year_id=c(2012, 2013, 2014, 2015, 2016), nid=c(205877, 206136, 206141, 257236, 282062))
    JPN <- merge(JPN, JPN_nids, by="year_id", all.x=TRUE)
    JPN <- merge(JPN, locations[, .(location_id, location_ascii_name, ihme_loc_id)], by="location_ascii_name", all.x=TRUE) %>% .[, .(nid, ihme_loc_id, location_id, year_id, cases)]
    # United States
    USA <- read_excel("FILEPATH/US_subnational_notifications.xlsx") %>% data.table %>% .[, .(nid, ihme_loc_id, location_id, year_id, cases)]
    # add to dataset
    case_notif <- bind_rows(case_notif, JPN, USA)
    
  }
  
  # drop unmapped locations
  drop.locs <- case_notif[!(ihme_loc_id %in% locations$ihme_loc_id)]$ihme_loc_id %>% unique
  if (length(drop.locs) > 0 ) {
    print(paste0("unmapped locations in WHO dataset (dropping all): ", toString(drop.locs)))
    case_notif <- case_notif[!(ihme_loc_id %in% drop.locs)]
  }
  
  # remove missing rows
  case_notif <- case_notif[!is.na(cases), .(nid, location_id, year_id, cases)]
  
  return(case_notif)
  
}

### prepare covariates
prep_covs <- function(...) {
  
  ### get updated covariates
  ### covariate: measles_vacc_cov_prop, covariate_id=75
  mcv1 <- get_covariate_estimates(covariate_id=75, gbd_round_id=gbd_round)
  mcv1[, ln_unvacc := log(1 - mean_value)]
  setnames(mcv1, "mean_value", "mean_mcv1")
  # save model version
  cat(paste0("Covariate MCV1 (NF) - model version ", unique(mcv1$model_version_id)), file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  #remove excess columns
  mcv1 <- mcv1[, .(location_id, year_id, ln_unvacc, mean_mcv1)]
  
  ### covariate: covariate_name_short="measles_vacc_cov_prop_2", covariate_id=1108
  mcv2 <- get_covariate_estimates(covariate_id=1108, gbd_round_id=gbd_round)
  mcv2[, ln_unvacc_mcv2 := log(1 - mean_value)]
  setnames(mcv2, "mean_value", "mean_mcv2")
  # save model version
  cat(paste0("Covariate MCV2 (NF) - model version ", unique(mcv2$model_version_id)), file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  #remove excess columns
  mcv2 <- mcv2[, .(location_id, year_id, ln_unvacc_mcv2, mean_mcv2)]
  
  ### merge together MCV1 and MCV2
  covs_both <- merge(mcv1, mcv2, by=c("location_id", "year_id"), all.x=TRUE) 
  
  if (MCV_lags == "yes") {
    
    # calculate proportion of people receiving ONLY mcv1 versus mcv2
    covs_both[, ln_unvacc_mcv1_only := log( 1 - (mean_mcv1 - mean_mcv2) )]
    covs_both[, ln_unvacc_mcv2_only := log(1 - mean_mcv2)]
    
    # lags on MCV coverage
    for (i in 1:5) {
      assign(paste0("mcv_lag_", i), subset(covs_both, select=c("location_id", "year_id", "ln_unvacc", "ln_unvacc_mcv2")))
      setnames(get(paste0("mcv_lag_", i)), "ln_unvacc", paste0("mcv1_lag_", i))
      setnames(get(paste0("mcv_lag_", i)), "ln_unvacc_mcv2", paste0("mcv2_lag_", i))
      get(paste0("mcv_lag_", i))[, year_id := year_id + i]
      assign(paste0("mcv_lag_", i), get(paste0("mcv_lag_", i))[, c("location_id", "year_id", paste0("mcv1_lag_", i), paste0("mcv2_lag_", i)), with=FALSE])
      # fill in missingness
      get(paste0("mcv_lag_", i))[is.na(get(paste0("mcv1_lag_", i))), paste0("mcv1_lag_", i) := log(1)]
      get(paste0("mcv_lag_", i))[is.na(get(paste0("mcv2_lag_", i))), paste0("mcv2_lag_", i) := log(1)]
    }
    
    covs_both <- merge(covs_both, mcv_lag_1, by=c("location_id", "year_id"), all.x=TRUE)
    covs_both <- merge(covs_both, mcv_lag_2, by=c("location_id", "year_id"), all.x=TRUE)
    covs_both <- merge(covs_both, mcv_lag_3, by=c("location_id", "year_id"), all.x=TRUE)
    covs_both <- merge(covs_both, mcv_lag_4, by=c("location_id", "year_id"), all.x=TRUE)
    covs <- merge(covs_both, mcv_lag_5, by=c("location_id", "year_id"), all.x=TRUE)
    
  } else { 
    
    covs <- copy(covs_both[!is.na(ln_unvacc_mcv2)])
    
  }
  
  return(covs)
  
}

### combine input data for regression
prep_regression <- function(...) {
  
  # merge on location_id, region, and super region
  regress <- merge(case_notif, locations[, .(location_id, ihme_loc_id, region_id, super_region_id)], by="location_id", all.x=TRUE)
  # merge on population (NOTE: this is JUST the population of the birth cohort at the average age of notification--our fix for underreporting)
  regress <- join(regress, sum_pop_young, by=c("location_id", "year_id"), type="inner")
  # merge on ihme covariates and WHO SIA data
  regress <- join(regress, covs, by=c("location_id", "year_id"), type="inner")
  regress <- join(regress, SIAs, by=c("location_id", "year_id"), type="left")
  
  # fix missing lags
  regress[is.na(supp), supp := 0]
  for (i in 1:5) {
    regress[is.na(get(paste0("supp_", i))), paste0("supp_", i) := 0.000000001]
    regress[get(paste0("supp_", i))==0, paste0("supp_", i) := 0.000000001]
  }
  
  ### set up regression
  # incidence rate generated from the population just for the 1 year birth cohort at average age of notification
  regress[, inc_rate := (cases / pop) * 100000]
  # because of the 1-year population, some unrealisticly high incidence rates generated for places with good notification and outbreaks 
  # drop these high outliers that suggest >95% of the population of the entire country got measles
  regress <- regress[inc_rate <= 95000, ]
  # outlier PNG outbreaks
  regress <- regress[!(ihme_loc_id=="PNG" & year_id %in% c(2013:2015)), ]
  
  ### generate transformed variables
  # proportion of population unvaccinated
  #regress[inc_rate==0, inc_rate := 0.000000001]
  regress[, ln_inc := log(inc_rate)]
  regress <- do.call(data.table, lapply(regress, function(x) replace(x, is.infinite(x), NA))) 
  
  # set up factors for regression
  regress[, super_region_id := as.factor(super_region_id)]
  regress[, region_id := as.factor(region_id)]
  regress[, location_id := as.factor(location_id)]
  
  ### add herd immunity covariate
  regress$herd_imm_95_mcv1 <- 0
  regress[mean_mcv1 >= 0.95, herd_imm_95_mcv1 := 1]
  
  regress$herd_imm_95_mcv2 <- 0
  regress[mean_mcv2 >= 0.95, herd_imm_95_mcv2 := 1]
  
  # we found older data to be less reliable - we tested cutting off data at 1980, 1985, 1990, and 1995 and found 1995 gave most expected 
  # relationship between vaccination and incidence
  regress <- regress[year_id >= 1995, ]
  
  return(regress)
  
}

### prepare regression input data
covs <- prep_covs()
SIAs <- prep_sia()
case_notif <- prep_case_notifs()
regress <- prep_regression()

# ### test continuous herd immunity after 90% coverage
# regress[, herd_imm_cont_mcv1 := 0]
# regress[mean_mcv1 >= 0.90, herd_imm_cont_mcv1 := (mean_mcv1 - 0.90) * 100]
# regress[, herd_imm_cont_mcv2 := 0]
# regress[mean_mcv2 >= 0.90, herd_imm_cont_mcv2 := (mean_mcv2 - 0.90) * 100]
# model <- as.formula(paste0("ln_inc ~ ln_unvacc + ln_unvacc_mcv2 + herd_imm_cont_mcv1 + herd_imm_cont_mcv2 + supp_1 + supp_2 + supp_3 + supp_4 + supp_5 + (1 | super_region_id) + (1 | region_id) + (1 | location_id)"))
# summary(lmer(model, data=regress))

# save model input
write.csv(regress, file.path(j.version.dir.inputs, "case_regression_input.csv"), row.names=FALSE)
#***********************************************************************************************************************


#----MIXED EFFECTS MODEL------------------------------------------------------------------------------------------------
### run mixed effects regression model
if (MCV1_or_MCV2 == "use_MCV2") {
  
  me_model <- lmer(ln_inc ~ ln_unvacc + ln_unvacc_mcv2 + supp_1 + supp_2 + supp_3 + supp_4 + supp_5 + 
                     (1 | super_region_id) + (1 | region_id) + (1 | location_id), data=regress)
  
} else if (MCV1_or_MCV2 == "MCV1_only") {
  
  me_model <- lmer(ln_inc ~ ln_unvacc + supp_1 + supp_2 + supp_3 + supp_4 + supp_5 + 
                     (1 | super_region_id) + (1 | region_id) + (1 | location_id), data=regress)
  
}

### save log
capture.output(summary(me_model), file = file.path(j.version.dir.logs, "log_incidence_mereg.txt"), type="output")
#***********************************************************************************************************************  


#----DRAWS--------------------------------------------------------------------------------------------------------------
### prep prediction data
# merge ihme_loc_id, population (NOTE: this is JUST the population of the birth cohort at the average age of notification--our 
# fix for underreporting), ihme covariates, and WHO SIA data
draws_nonfatal <- merge(covs, locations[, .(location_id, ihme_loc_id, region_id, super_region_id)], by="location_id", all.x=TRUE)
draws_nonfatal <- merge(draws_nonfatal, sum_pop_young, by=c("location_id", "year_id"), all.x=TRUE)
draws_nonfatal <- merge(draws_nonfatal, SIAs[, c("location_id", "year_id", paste0("supp_", 1:5))], by=c("location_id", "year_id"), all.x=TRUE)

# fix missing lags
for (i in 1:5) {
  draws_nonfatal[is.na(get(paste0("supp_", i))), paste0("supp_", i) := 0]
}

### set options
if (MCV1_or_MCV2 == "use_MCV2") {
  cols <- c("constant", "b_ln_unvacc", "b_ln_unvacc_mcv2", "b_lag_1", "b_lag_2", "b_lag_3", "b_lag_4", "b_lag_5")
} else if (MCV1_or_MCV2 == "MCV1_only") {
  cols <- c("constant", "b_ln_unvacc", "b_lag_1", "b_lag_2", "b_lag_3", "b_lag_4", "b_lag_5")
}
### calculate draws of case counts - 1000 draws for uncertainty
# coefficient matrix
coeff <- cbind(fixef(me_model)[[1]] %>% data.table, coef(me_model)[[1]] %>% data.table %>% .[, "(Intercept)" := NULL] %>% .[1, ])
colnames(coeff) <- cols
coefmat <- matrix(unlist(coeff), ncol=length(cols), byrow=TRUE, dimnames=list(c("coef"), c(as.vector(cols))))

# generate a standard random effect
# set such that 95% of unvaccinated individuals will get measles (95% attack rate)
# given 0% vaccinated, the combination of the RE and the constant will lead to an incidence of 95,000/100,000
standard_RE <- log(95000) - coefmat["coef", "constant"]

# covariance matrix
vcovmat <- vcov(me_model)
vcovlist <- NULL
for (ii in 1:length(cols)) {
  if (MCV1_or_MCV2 == "use_MCV2") { vcovlist_a <- c(vcovmat[ii, 1], vcovmat[ii, 2], vcovmat[ii, 3], vcovmat[ii, 4], vcovmat[ii, 5], vcovmat[ii, 6], vcovmat[ii, 7], vcovmat[ii, 8])
  } else if (MCV1_or_MCV2 == "MCV1_only") { vcovlist_a <- c(vcovmat[ii, 1], vcovmat[ii, 2], vcovmat[ii, 3], vcovmat[ii, 4], vcovmat[ii, 5], vcovmat[ii, 6], vcovmat[ii, 7]) }
  vcovlist <- c(vcovlist, vcovlist_a)
}
vcovmat2 <- matrix(vcovlist, ncol=length(cols), byrow=TRUE)

# create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
betadraws <- rmvnorm(n=1000, mean=coefmat, sigma=vcovmat2)

# transpose coefficient matrix
betas <- t(betadraws)

### calculate cases from model coefficients
draw_nums <- 1:1000
draw_cols <- paste0("case_draw_", draw_nums_gbd)
if (MCV1_or_MCV2 == "use_MCV2") {
  invisible(
  draws_nonfatal[, (draw_cols) := lapply(draw_nums, function(x) { exp( betas[1, x] +
                                                                ( betas[2, x] * ln_unvacc ) +
                                                                ( betas[3, x] * ln_unvacc_mcv2 ) + 
                                                                ( betas[4, x] * supp_1 ) +
                                                                ( betas[5, x] * supp_2 ) +
                                                                ( betas[6, x] * supp_3 ) +
                                                                ( betas[7, x] * supp_4 ) +
                                                                ( betas[8, x] * supp_5 ) +
                                                                standard_RE ) *
      ( pop / 100000 ) } )] )
} else if (MCV1_or_MCV2 == "MCV1_only") {
  invisible(
  draws_nonfatal[, (draw_cols) := lapply(draw_nums, function(x) { exp( betas[1, x] +
                                                                ( betas[2, x] * ln_unvacc ) +
                                                                ( betas[3, x] * supp_1 ) +
                                                                ( betas[4, x] * supp_2 ) +
                                                                ( betas[5, x] * supp_3 ) +
                                                                ( betas[6, x] * supp_4 ) +
                                                                ( betas[7, x] * supp_5 ) +
                                                                standard_RE ) *
      ( pop / 100000 ) } )] )
}

### save results
draws_nonfatal <- draws_nonfatal[, c("location_id", "year_id", draw_cols), with=FALSE]
if (WRITE_FILES == "yes") {
  write.csv(draws_nonfatal, file.path(j.version.dir, "01_case_predictions_from_model.csv"), row.names=FALSE)
}
#***********************************************************************************************************************


#----FIX MODEL ESTIMATES------------------------------------------------------------------------------------------------
### get population 
pop_u5 <- population[year_id %in% c(1980:year_end) & age_group_id %in% c(2:5) & sex_id==3, ]
pop_u5 <- pop_u5[, pop := sum(population), by=c("location_id", "year_id")] %>% .[, .(location_id, year_id, pop)] %>% unique

### PART A - countries where we trust case notifications
### assume cases reported well in WHO case notifications in some super regions -- use model for the others
# keep only countries from 3 super regions: high income, eastern europe/central asia, latin america/caribbean
sr_64_31_103 <- locations[super_region_id %in% c(64, 31, 103), location_id]
case_notif <- prep_case_notifs(add_subnationals=TRUE)
# ignore GBD locations in these super regions that do not have case notifications (i.e. BMU, GRL, PRI, VIR, and 
# other high-come subnationals levels)
# this way will include subnational case notifications (instead of just level==3)
trusted_notifications <- sr_64_31_103[sr_64_31_103 %in% unique(case_notif$location_id)]

### model case notifications in ST-GPR to fix missingness in WHO reporting data
if (run_case_notification_smoothing) { 
  source(file.path(j_root, "FILEPATH/est_shocks.r"))
  case_notification_smoothing()
}

### pull case notifications
adjusted_notifications_q <- lapply(list.files("FILEPATH/case_notifications", full.names=TRUE), fread) %>% 
  rbindlist %>% .[, .(location_id, year_id, combined)] %>% setnames(., "combined", "cases")

### generate error
# calculate incidence rate from case notification and population data
adjusted_notifications <- merge(adjusted_notifications_q, pop_u5, by=c("location_id", "year_id"), all.x=TRUE)
adjusted_notifications <- merge(adjusted_notifications, locations[, .(location_id, region_id, super_region_id)], by="location_id", all.x=TRUE)
invisible(adjusted_notifications[, inc_rate := cases / pop])
# fix Mongolia 2017
invisible(adjusted_notifications[location_id==38 & year_id==year_end, inc_rate := adjusted_notifications[location_id==38 & year_id==2014, inc_rate]])
# fix weirdly high ST-GPR est in 2017
wide <- dcast.data.table(adjusted_notifications_q, value.var="cases", location_id ~ year_id)
colnames(wide) <- c("location_id", paste0("x_", 1980:year_end))
weird_locs <- wide[x_2017 > x_2016 & x_2017 > x_2015 & x_2017 > x_2014 & x_2017 > 5, location_id]
weird_locs <- weird_locs[!weird_locs %in% case_notif[year_id==2017, location_id]]
invisible(lapply(weird_locs, function(x) adjusted_notifications[location_id==x & year_id==year_end, inc_rate := round(mean(adjusted_notifications[location_id==x & year_id %in% c((year_end - 3):(year_end - 1)), inc_rate]), 0)]))
# calculate standard error
adjusted_notifications[, SE := sqrt( (inc_rate * (1 - inc_rate)) / pop )]
# calculate region and super-region average error for country-years with 0 cases reported
notif_error_reg <- ddply(adjusted_notifications, c("region_id", "year_id"), summarise, reg_SE=mean(SE, na.rm=TRUE))
notif_error_sr <- ddply(adjusted_notifications, c("super_region_id", "year_id"), summarise, sr_SE=mean(SE, na.rm=TRUE))
# add on region and super-region mean error, replace with these errors if country-level error is zero
adjusted_notifications <- merge(adjusted_notifications, notif_error_reg, by=c("region_id", "year_id"), all.x=TRUE)
adjusted_notifications <- merge(adjusted_notifications, notif_error_sr, by=c("super_region_id", "year_id"), all.x=TRUE)
invisible(adjusted_notifications[SE==0 | is.na(SE), SE := reg_SE])
invisible(adjusted_notifications[SE==0 | is.na(SE), SE := sr_SE])
# if region and super region error is still zero, calculate non-zero SE 
adjusted_notifications[SE==0, SE := sqrt( ((1 / pop) * inc_rate * (1 - inc_rate)) + ((1 / (4 * (pop ^ 2))) * ((qnorm(0.975)) ^ 2)) ) ]

# generate 1000 draws of incidence from error term
#while(any(inc_draws > 0)) {inc_draws <- rnorm(n=1000 * length(adjusted_notifications$inc_rate), mean=adjusted_notifications$inc_rate, sd=adjusted_notifications$SE)}
inc_draws <- rnorm(n=1000 * length(adjusted_notifications$inc_rate), mean=adjusted_notifications$inc_rate, sd=adjusted_notifications$SE) %>% matrix(., ncol=1000, byrow=FALSE) %>% as.data.table
colnames(inc_draws) <- paste0("inc_draw_", draw_nums_gbd)
adjusted_notifications <- cbind(adjusted_notifications, inc_draws)

### calculate cases from incidence and population
invisible(adjusted_notifications[, (draw_cols) := lapply(draw_nums_gbd, function(x) get(paste0("inc_draw_", x)) * pop)])
invisible(adjusted_notifications <- adjusted_notifications[, c("location_id", "year_id", draw_cols), with=FALSE])

### floor draws at 0
invisible( lapply(draw_nums_gbd, function(x) combined_case_draws[get(paste0("case_draw_", x)) < 0, paste0("case_draw_", x) := 0]) )

### save results
if (WRITE_FILES == "yes") {
  write.csv(combined_case_draws, file.path(j.version.dir, "02_add_case_notifs_and_shocks.csv"), row.names=FALSE)  
}
#*********************************************************************************************************************** 


########################################################################################################################
##### PART TWO: RESCALE CASES ##########################################################################################
########################################################################################################################


#----RESCALE------------------------------------------------------------------------------------------------------------
### run custom rescaling function
rescaled_cases <- rake(combined_case_draws)

### save results
if (WRITE_FILES == "yes") {
  write.csv(rescaled_cases, file.path(j.version.dir, "03_case_predictions_rescaled.csv"), row.names=FALSE)
}
#*********************************************************************************************************************** 


if (CALCULATE_NONFATAL == "yes") {
########################################################################################################################
##### PART THREE: CASES AGE-SEX SPLIT ##################################################################################
########################################################################################################################


#----SPLIT--------------------------------------------------------------------------------------------------------------
### split measles cases by age/sex pattern from CoD database
split_cases <- age_sex_split(acause=acause, input_file=rescaled_cases, measure="case")

### save results
if (WRITE_FILES == "yes") {
  write.csv(split_cases, file.path(j.version.dir, "04_case_predictions_split.csv"), row.names=FALSE)
}
#*********************************************************************************************************************** 


########################################################################################################################
##### PART FOUR: CASES TO PREVALENCE AND INCIDENCE #####################################################################
########################################################################################################################


#----PREVALENCE AND INCIDENCE-------------------------------------------------------------------------------------------
### convert cases to prevalence, assuming duration of 10 days, and incidence
# prep data
predictions_prev <- copy(split_cases) %>% .[, measure_id := 5]
predictions_inc <- copy(split_cases) %>% .[, measure_id := 6]
rm(split_cases)
# calculate prevalence
invisible( predictions_prev[, (draw_cols_gbd) := lapply( draw_nums_gbd, function(x) (get(paste0("split_case_draw_", x)) * (10 / 365)) / population )] )
predictions_prev <- predictions_prev[, c(draw_cols_upload, "measure_id"), with=FALSE]
# calculate incidence
invisible( predictions_inc[, (draw_cols_gbd) := lapply( draw_nums_gbd, function(x) get(paste0("split_case_draw_", x)) / population )] )
predictions_inc <- predictions_inc[, c(draw_cols_upload, "measure_id"), with=FALSE]
#*********************************************************************************************************************** 


########################################################################################################################
##### PART FIVE: FORMAT FOR COMO #######################################################################################
########################################################################################################################


#----SAVE RESULTS-------------------------------------------------------------------------------------------------------
### format for como, (prevalence measure_id==5, incidence measure_id==6)
predictions <- rbind(predictions_prev, predictions_inc)
predictions <- predictions[age_group_id %in% c(age_start:age_end), ]

### save to /share directory
lapply(unique(predictions$location_id), function(x) write.csv(predictions[location_id==x, ],
                                                              file.path(cl.version.dir, paste0(x, ".csv")), row.names=FALSE))
print(paste0("nonfatal estimates saved in ", cl.version.dir))

### save measles nonfatal draws to the database, modelable_entity_id=1436
job_nf <- paste0("qsub -N s_epi_", acause, " -pe multi_slot 40 -P ", cluster_proj, " -o FILEPATH/", username, " -e FILEPATH/", username,
                 " FILEPATH/r_shell.sh FILEPATH/save_results_wrapper.r",
                 " --args",
                 " --type epi",
                 " --me_id ", me_id, 
                 " --input_directory ", cl.version.dir,
                 " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
                 " --best ", mark_model_best)
system(job_nf); print(job_nf)
#***********************************************************************************************************************
}


if (CALCULATE_COD == "yes") {
########################################################################################################################
##### PART Six: MODEL CFR ##############################################################################################
########################################################################################################################


#----PREP---------------------------------------------------------------------------------------------------------------
### function: prep covariates (for use later)
prep_covariates <- function(...) {
  
  ### get covariates
  # under-2 malnutrition, covariate_name_short="underweight_prop_waz_under_2sd" covariate id 66; now using wasting covariate "wasting_prop_whz_under_2sd" id 1070
  mal_cov <- get_covariate_estimates(covariate_id=66, year_id=1980:year_end, location_id=pop_locs, gbd_round_id=gbd_round)
  mal_cov[, ln_mal := log(mean_value)] # + 1e-6)]
  setnames(mal_cov, "mean_value", "mal")
  # save model version
  cat(paste0("Covariate malnutrition, covariate id 66 (CoD) - model version ", unique(mal_cov$model_version_id)), file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  # remove excess columns
  covariates <- mal_cov[, c("location_id", "year_id", "mal", "ln_mal"), with=FALSE]
  
  # LDI, covariate_name_short="LDI_pc"
  ldi_cov <- get_covariate_estimates(covariate_id=57, year_id=1980:year_end, location_id=pop_locs, gbd_round_id=gbd_round)
  ldi_cov[, ln_LDI := log(mean_value)]
  setnames(ldi_cov, "mean_value", "LDI")
  # save model version
  cat(paste0("Covariate LDI (CoD) - model version ", unique(ldi_cov$model_version_id)), file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  # remove excess columns
  ldi_cov <- ldi_cov[, c("location_id", "year_id", "ln_LDI", "LDI"), with=FALSE]
  covariates <- merge(ldi_cov, covariates, by=c("location_id", "year_id"), all.x=TRUE)
  
  # healthcare access and quality index, covariate_name_short="haqi"
  haqi <- get_covariate_estimates(covariate_id=1099, year_id=1980:year_end, location_id=pop_locs, gbd_round_id=gbd_round)
  haqi[, HAQI := mean_value / 100] #setnames(haqi, "mean_value", "HAQI")
  haqi[, ln_HAQI := log(HAQI)]
  # save model version
  cat(paste0("Covariate HAQ (CoD) - model version ", unique(haqi$model_version_id)), file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  # remove excess columns
  haqi <- haqi[, c("location_id", "year_id", "HAQI", "ln_HAQI"), with=FALSE]
  covariates <- merge(haqi, covariates, by=c("location_id", "year_id"), all.x=TRUE)
  
  # socio-demographic index, covariate_name_short="sdi"
  sdi <- get_covariate_estimates(covariate_id=881, year_id=1980:year_end, location_id=pop_locs, gbd_round_id=gbd_round)
  # save model version
  cat(paste0("Covariate SDI (CoD) - model version ", unique(sdi$model_version_id)), file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  # remove excess columns
  setnames(sdi, "mean_value", "SDI")
  sdi <- sdi[, c("location_id", "year_id", "SDI"), with=FALSE]
  covariates <- merge(sdi, covariates, by=c("location_id", "year_id"), all.x=TRUE)
  
  return(covariates)
  
}
  
### function: prep CoD regression
prep_cod <- function(add_vital_registration=TRUE) {
  
  ### get mortality envelope
  mortality_envelope <- get_envelope(location_id=pop_locs, year_id=1980:year_end, age_group_id=22, sex_id=1:2, gbd_round_id=gbd_round)
  setnames(mortality_envelope, "mean", "mortality_envelope")
  
  ### get CoD data
  # download raw (uncorrected) COD data from the database
  cod <- get_cod_data(cause_id=cause_id) %>% setnames(., "year", "year_id")
  # save model version
  cat(paste0("CoD data - version ", unique(cod$description)), 
      file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  cod <- cod[acause=="measles" & age_group_id %in% 22 & !is.na(cf_corr) & sample_size != 0, ] # c(age_start:age_end)
  cod <- merge(cod, mortality_envelope[, .(location_id, year_id, age_group_id, sex_id, mortality_envelope)], by=c("location_id", "year_id", "age_group_id", "sex_id"))
  cod[, deaths := cf_corr * mortality_envelope]
  cod_sum <- cod[, .(deaths=sum(deaths), sample_size=sum(sample_size)), by=c("location_id", "year_id")]
  # merge on locations
  cod_sum <- merge(cod_sum, locations[, .(location_id, ihme_loc_id, super_region_id)], by="location_id", all.x=TRUE)
  
  ### get WHO notifications
  case_notif <- prep_case_notifs()
  WHO <- merge(case_notif, cod_sum[, .(location_id, year_id, deaths)], by=c("location_id", "year_id"))
  # remove instances with zero cases
  WHO <- WHO[cases > 0, ]
  # round deaths
  WHO[, deaths := round(deaths, 0)]
  # merge on locations
  WHO <- merge(WHO[, .(nid, location_id, year_id, cases)], locations[, .(location_id, ihme_loc_id, super_region_id)], by="location_id", all.x=TRUE)
  WHO <- WHO[super_region_id %in% c(64, 31, 103) & !is.na(year_id), .(ihme_loc_id, year_id, deaths, cases, nid)]
  WHO[, source := "implied_cfr"]
  
  ### just keep 4 and 5 star countries (defined by "well-defined" vital registration data)
  cod_loc_include <- file.path(j_root, "FILEPATH/locations_four_plus_stars.csv") %>% fread
  print(paste0("dropping locations in SR 64, 31, and 103 without well-defined vital registration: ", paste(WHO[!ihme_loc_id %in% cod_loc_include$ihme_loc_id, ihme_loc_id] %>% unique, collapse=", ")))
  WHO <- WHO[ihme_loc_id %in% cod_loc_include$ihme_loc_id]
  
  ### get CFR literature data
  cfr_lit <- read_excel(file.path(home, "FILEPATH/measles_cfr_extraction.xlsx"), sheet="extraction") %>% as.data.table
  cfr_lit[, hospital := hospital %>% as.integer]
  cfr_lit[, rural := rural %>% as.integer]
  cfr_lit[, outbreak := outbreak %>% as.integer]
  setnames(cfr_lit, c("cases", "sample_size"), c("deaths", "cases"))
  cfr_lit[is.na(midpointyear), midpointyear := floor((year_start + year_end) / 2)]
  cfr_lit[is.na(group_review), group_review := 0]
  cfr_lit[is.na(is_outlier), is_outlier := 0]
  cfr_lit[is.na(ignore), ignore := 0]
  cfr_lit <- cfr_lit[group_review != 1 & ignore != 1, .(nid, ihme_loc_id, year_start, year_end, midpointyear, age_start, age_end, cases, deaths, hospital, outbreak, rural, ignore, group_review, is_outlier)] %>% unique
  setnames(cfr_lit, "midpointyear", "year_id")
  cfr_lit[, source := "extracted_cfr"]
  
  ### bring together all CFR data
  if (add_vital_registration) all_cfr <- rbind(cfr_lit, WHO, fill=TRUE) else all_cfr <- copy(cfr_lit)
  all_cfr[ihme_loc_id=="HKG", "ihme_loc_id" := "CHN_354"]
  all_cfr <- merge(all_cfr, locations[, .(location_id, ihme_loc_id, super_region_id)], by="ihme_loc_id", all.x=TRUE)
  
  ### drop outliers
  all_cfr[, cfr := deaths / cases]
  all_cfr[is.na(is_outlier), is_outlier := 0]
  all_cfr[(ihme_loc_id=="BEL" & year_start==2009) | 
            (ihme_loc_id=="BEL" & year_start==2010) |
            (ihme_loc_id=="GNB" & year_start==1979 & year_end==1982 & age_start==0) | 
            cfr > 0.1 |
            (cfr > 0.025 & super_region_id==64) |
            # drop data from hill tribes or ethnic minorities
            (ihme_loc_id=="THA" & year_start==1984) |
            (ihme_loc_id=="ETH" & year_start==1981) |
            (ihme_loc_id=="IND" & year_start==1991) |
            (ihme_loc_id=="IND" & year_start==1992 & cfr > 0.15) |
            (ihme_loc_id=="GNB" & year_start==1979) |
            # drop data from Senegal, which are from studies conducted in Ibel village (a remote village with difficult transportation), 
            # and an unspecified rural area (data collected by lay interviewers and no information on case definition)
            (ihme_loc_id=="SEN" & year_start==1983) |
            (ihme_loc_id=="SEN" & year_start==1985), 
          is_outlier := 1]
  all_cfr <- all_cfr[is_outlier != 1, ]
  
  ### fix, merge on covariates
  all_cfr[is.na(hospital), hospital := 0]
  all_cfr[is.na(outbreak), outbreak := 0]
  all_cfr[is.na(rural), rural := 0]
  all_cfr <- unique(all_cfr[, .(nid, location_id, ihme_loc_id, year_id, age_start, age_end, cases, deaths, hospital, outbreak, rural, source)])
  all_cfr <- all_cfr[year_id >= 1980, ] # some years are missing in expert extractions??
  all_cfr <- merge(all_cfr, covariates, by=c("location_id", "year_id"), all.x=TRUE)
  all_cfr[, ihme_loc_id := as.factor(ihme_loc_id)]
  
  return(all_cfr)
  
}

### prep CoD regression
covariates <- prep_covariates()
all_cfr <- prep_cod(add_vital_registration=include_VR_data_in_CFR)

### save regression input
write.csv(all_cfr, file.path(j.version.dir.inputs, "cfr_regression_input.csv"), row.names=FALSE)
#*********************************************************************************************************************** 


#----MODEL CFR----------------------------------------------------------------------------------------------------------
### set theta using output from GBD2015
theta <- exp(1 / -.3633593)  ## ln_mal only

### mixed effects neg binomial regression
### model specifications
# covariates
if (which_covariate=="only_HAQI") covar1 <- "HAQI"
if (which_covariate=="only_SDI") covar1 <- "SDI"
if (which_covariate=="only_LDI") covar1 <- "LDI"
if (which_covariate=="only_ln_LDI") covar1 <- "ln_LDI"
if (which_covariate=="use_HAQI") { covar1 <- "ln_mal"; covar2 <- "HAQI" }
if (which_covariate=="use_LDI") { covar1 <- "HAQI"; covar2 <- "ln_LDI" }
# random effects
res <- as.formula(~ 1 | ihme_loc_id)
# formula
if (exists("covar2")) formula <- paste0("deaths ~ ", covar1, " + ", covar2, " + hospital + outbreak + rural + offset(log(cases))") else
  formula <- paste0("deaths ~ ", covar1, " + hospital + outbreak + rural + offset(log(cases))")
cfr_model <- MASS::glmmPQL(as.formula(formula), random=res, family=negative.binomial(theta=theta, link=log), data=all_cfr)

### save log
saveRDS(cfr_model, file.path(j.version.dir.logs, "cfr_model_menegbin.rds"))
capture.output(summary(cfr_model), file=file.path(j.version.dir.logs, "log_cfr_menegbin_summary.txt"), type="output")
#*********************************************************************************************************************** 


#----DRAWS--------------------------------------------------------------------------------------------------------------
set.seed(0311)

### predict out for all country-year-age-sex
pred_CFR <- merge(covariates, sum_pop_young, by=c("location_id", "year_id"), all.x=TRUE)
pred_CFR <- merge(pred_CFR, locations[, .(location_id, ihme_loc_id, super_region_id, parent_id, location_type, level)], 
                  by="location_id", all.x=TRUE)
N <- nrow(pred_CFR)

### calculate location and super region random effects
reffect_cfr <- ranef(cfr_model)[1] %>% data.frame
colnames(reffect_cfr) <- "RE_loc"
reffect_cfr$ihme_loc_id <- gsub(".*/", "", rownames(reffect_cfr))
reffect_cfr <- reffect_cfr[, c("ihme_loc_id", "RE_loc")]
# collapse super region / region REs
reffect_cfr <- merge(reffect_cfr, locations[, .(ihme_loc_id, super_region_id, region_id)], by="ihme_loc_id", all.x=TRUE) %>% data.table
super_region_re <- ddply(reffect_cfr, c("super_region_id"), summarise, RE_sr=mean(RE_loc) ) %>% data.table
region_re <- ddply(reffect_cfr, c("region_id"), summarise, RE_reg=mean(RE_loc) ) %>% data.table
reffect_cfr[, c("super_region_id", "region_id") := NULL]
# create full dataset of location reffects
reffect_cfr <- rbind(reffect_cfr, data.table(ihme_loc_id=pred_CFR[, ihme_loc_id] %>% unique %>% .[!. %in% reffect_cfr$ihme_loc_id]), fill=TRUE)
reffect_cfr <- merge(reffect_cfr, locations[, .(location_id, ihme_loc_id, super_region_id, region_id, parent_id, location_type, level)], by="ihme_loc_id", all.x=TRUE)
# if national location is missing random effect, fill with region RE
reffect_cfr <- merge(reffect_cfr, region_re, by="region_id", all.x=TRUE)
reffect_cfr[is.na(RE_loc) & level==3, RE_loc := RE_reg]
# if national location is still missing random effect, fill with super region RE
reffect_cfr <- merge(reffect_cfr, super_region_re, by="super_region_id", all.x=TRUE)
reffect_cfr[is.na(RE_loc) & level==3, RE_loc := RE_sr]
# keep just needed cols
reffect_cfr <- reffect_cfr[, .(location_id, RE_loc, level, parent_id)]

### add on same RE as parent for subnationals
for (LEVEL in 4:max(locations$level)) {
  reffect_subnats <- reffect_cfr[, .(location_id, RE_loc)] %>% setnames(., c("location_id", "RE_loc"), c("parent_id", "RE_parent"))
  reffect_cfr <- merge(reffect_cfr, reffect_subnats, by="parent_id", all.x=TRUE)
  reffect_cfr <- reffect_cfr[level==LEVEL & is.na(RE_loc), RE_loc := RE_parent]
  reffect_cfr <- reffect_cfr[, .(location_id, RE_loc, level, parent_id)]
}

### merge on to prediction dataset
pred_CFR <- merge(pred_CFR, reffect_cfr[, .(location_id, RE_loc)], by="location_id", all.x=TRUE)

### 1000 draws for uncertainty
# keep only intercept and ln_mal coefficient
# coefficient matrix
# coefmat <- c(fixef(cfr_model))
coefmat <- c(fixef(cfr_model)) # $cond #(add this if TMB model)
names(coefmat)[1] <- "constant"
names(coefmat) <- paste("b", names(coefmat), sep = "_")
coefmat <- matrix(unlist(coefmat), ncol=length(coefmat), byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coefmat)))))
if (!exists("covar2")) length <- 2 else if (exists("covar2")) length <- 3
coefmat <- coefmat[1, 1:length]

# covariance matrix
vcovmat <- vcov(cfr_model) # $cond #(add this if TMB model)
vcovmat <- vcovmat[1:length, 1:length]

# create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
betadraws <- rmvnorm(n=1000, mean=coefmat, sigma=vcovmat)
betas <- t(betadraws)

# zero inflation parameter
#zi <- rnorm(n=1000, mean=fixef(cfr_model)$zi, sd=sqrt(vcov(cfr_model)$zi))

### estimate deaths at the draw level
cfr_draw_cols <- paste0("cfr_draw_", draw_nums_gbd)
# generate draws of the prediction using coefficient draws
if (exists("covar2")) { pred_CFR[, (cfr_draw_cols) := lapply(draw_nums_gbd, function(i) exp( betas[1, (i + 1)] + 
                                                                                            ( betas[2, (i + 1)] * get(covar1) ) +
                                                                                            ( betas[3, (i + 1)] * get(covar2) ) +
                                                                                              RE_loc ) )] }
if (!exists("covar2")) { pred_CFR[, (cfr_draw_cols) := lapply(draw_nums_gbd, function(i) exp( betas[1, (i + 1)] + 
                                                                                            ( betas[2, (i + 1)] * get(covar1) ) +
                                                                                            #zi[(i + 1)]  +
                                                                                            #fixef(cfr_model)$zi +
                                                                                              RE_loc ) )] }

### save results
CFR_draws_save <- pred_CFR[, c("location_id", "year_id", cfr_draw_cols), with=FALSE]
if (WRITE_FILES == "yes") {
  write.csv(CFR_draws_save, file.path(j.version.dir, paste0("05_cfr_model_draws.csv")), row.names=FALSE)
}
#*********************************************************************************************************************** 


########################################################################################################################
##### PART SEVEN: DEATHS ###############################################################################################
########################################################################################################################


#----CALCULATE DEATHS---------------------------------------------------------------------------------------------------
### merge together data
predictions_deaths <- merge(rescaled_cases, CFR_draws_save, by=c("location_id", "year_id"))

### calculate deaths from cases and CFR
death_draw_cols <- paste0("death_draw_", draw_nums_gbd)
predictions_deaths[, (death_draw_cols) := lapply(draw_nums_gbd, function(ii) get(paste0("cfr_draw_", ii)) * get(paste0("case_draw_", ii)) )]

### save results
predictions_deaths_save <- predictions_deaths[, c("location_id", "year_id", death_draw_cols), with=FALSE]
if (WRITE_FILES == "yes") {
  write.csv(predictions_deaths_save, file.path(j.version.dir, "06_death_predictions_from_model.csv"), row.names=FALSE)
}
#*********************************************************************************************************************** 


########################################################################################################################
##### PART EIGHT: RESCALE DEATHS #######################################################################################
########################################################################################################################


#----RESCALE------------------------------------------------------------------------------------------------------------
### run custom rescale function
invisible( lapply(death_draw_cols, function(x) predictions_deaths_save[location_id %in% locations[level > 3, location_id] & get(x)==0, (x) := 1e-10]) )
rescaled_deaths <- rake(predictions_deaths_save, measure="death_draw_")

### save results
if (WRITE_FILES == "yes") {
  write.csv(rescaled_deaths, file.path(j.version.dir, "07_death_predictions_rescaled.csv"), row.names=FALSE)
}
#***********************************************************************************************************************


########################################################################################################################
##### PART NINE: SPLIT DEATHS ##########################################################################################
########################################################################################################################


#----SPLIT--------------------------------------------------------------------------------------------------------------
### split using age pattern from cause of death data
split_deaths <- age_sex_split(acause=acause, input_file=rescaled_deaths, measure="death")
#*********************************************************************************************************************** 


#----SAVE---------------------------------------------------------------------------------------------------------------
### save split draws
split_deaths_save <- split_deaths[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("split_death_draw_", draw_nums_gbd)), with=FALSE]
if (WRITE_FILES == "yes") {
  write.csv(split_deaths_save, file.path(j.version.dir, "08_death_predictions_split.csv"), row.names=FALSE)
}
#*********************************************************************************************************************** 


########################################################################################################################
##### PART TEN: COMBINE CODEm DEATHS ##################################################################################
########################################################################################################################


#----COMBINE------------------------------------------------------------------------------------------------------------
### read CODEm COD results for data-rich countries
cod_M <- read_hdf5_table(file.path("FILEPATH"), key="data")
cod_F <- read_hdf5_table(file.path("FILEPATH"), key="data")
# save model version
cat(paste0("Data-rich CODEm feeder model (males) - model version ", male_CODEm_version), 
    file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
cat(paste0("Data-rich CODEm feeder model (females) - model version ", female_CODEm_version), 
    file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

# combine M/F CODEm results
cod_DR <- rbind(cod_M, cod_F, fill=TRUE)
cod_DR <- cod_DR[, draw_cols_upload, with=FALSE]

# use model for USA and MEX
MEX_USA_subnats <- locations[parent_id %in% c(102, 130), location_id]
cod_DR <- cod_DR[!location_id %in% c(102, 130, MEX_USA_subnats), ]

# hybridize data-rich and custom models
data_rich <- unique(cod_DR$location_id)
split_deaths_glb <- split_deaths_save[!location_id %in% data_rich, ]
colnames(split_deaths_glb) <- draw_cols_upload
split_deaths_hyb <- rbind(split_deaths_glb, cod_DR)

# keep only needed age groups
split_deaths_hyb <- split_deaths_hyb[age_group_id %in% c(age_start:age_end), ]
#***********************************************************************************************************************


########################################################################################################################
##### PART ELEVEN: FORMAT FOR CODCORRECT ###############################################################################
########################################################################################################################


#----SAVE RESULTS-------------------------------------------------------------------------------------------------------
### format for codcorrect
split_deaths_hyb[, measure_id := 1]

### save to share directory for upload
lapply(unique(split_deaths_hyb$location_id), function(x) write.csv(split_deaths_hyb[location_id==x, ],
                                                              file.path(cl.death.dir, paste0(x, ".csv")), row.names=FALSE))
print(paste0("death draws saved in ", cl.death.dir))

### save_results
job <- paste0("qsub -N s_cod_", acause, " -pe multi_slot 48 -P ", cluster_proj, " -o FILEPATH/", username, " -e FILEPATH/", username,
              " FILEPATH/r_shell.sh FILEPATH/save_results_wrapper.r",
              " --args",
              " --type cod",
              " --me_id ", cause_id, 
              " --input_directory ", cl.death.dir,
              " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
              " --best ", mark_model_best)
system(job); print(job)
#***********************************************************************************************************************  
}