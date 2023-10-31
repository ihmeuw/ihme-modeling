#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:  
# Date:    Februrary 2017
# Path:    FILEPATH
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
pacman::p_load(magrittr, foreign, stats, MASS, dplyr, plyr, lme4, reshape2, parallel, rhdf5, reticulate, arm, msm, ggplot2) 

library(optimx, lib.loc = FILEPATH)
#load mrbrt for mrbrt cfr model
if(cfr_model_type=="mrbrt"){
  mrbrt_helper_dir <- FILEPATH
  library(mrbrt001, lib.loc = mrbrt_helper_dir)
}


source(FIELPATH)
load_packages(c("data.table", "mvtnorm"))

## Pull in Pandas for HDF5
pandas <- import("pandas")

### set data.table fread threads to 1
setDTthreads(1)
#***********************************************************************************************************************


#----DIRECTORIES--------------------------------------------------------------------------------------------------------
### set objects
acause    <- "measles"
age_start <- 6  
age_end   <- 17              
a         <- 3               ## birth cohort years before 1980
cause_id  <- 341
me_id     <- 1436
gbd_round <- 7
release_id <- 9
year_end  <- if (gbd_round %in% c(3, 4, 5)) gbd_round + 2012 else if (gbd_round==6) 2019 else if (gbd_round==7) 2022
year_end_data <- 2019      ##new variable in GDB 2020 to account for fact we are generating estimates for years beyound last year with data

### draw numbers
draw_nums_gbd    <- 0:999
draw_cols_gbd    <- paste0("draw_", draw_nums_gbd)
draw_cols_upload <- c("location_id", "year_id", "age_group_id", "sex_id", draw_cols_gbd)

### make folders on cluster
# mortality
cl.death.dir <- FILEPATH
if (!dir.exists(cl.death.dir) & CALCULATE_COD=="yes") dir.create(cl.death.dir, recursive=TRUE)
# nonfatal
cl.version.dir <- FILEPATH
if (!dir.exists(cl.version.dir) & CALCULATE_NONFATAL=="yes") dir.create(file.path(cl.version.dir), recursive=TRUE)
# shocks
cl.shocks.dir <- FILEPATH
if (!dir.exists(cl.shocks.dir) & run_shocks_adjustment) dir.create(file.path(cl.shocks.dir), recursive=TRUE)

### directories
home <- FILEPATH
j.version.dir <- FILEPATH
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
source("FILEPATH/get_population.R")
source("FILEPATH/get_envelope.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_cod_data.R")

### load personal functions
"FILEPATH/read_hdf5_table-copy.R" %>% source 
"/FILEPATH/read_excel.R" %>% source
"/FILEPATH/rake.R" %>% source
"/FILEPATH/collapse_point.R" %>% source
"/FILEPATH/sql_query.R" %>% source
source(file.path("/FILEPATH/age_sex_split.R"))
source(file.path("/FILEPATH/calc_oos_error.R"))
source(paste0("/FILEPATH/adjust_sia_data.R"))
weighted.rmse <- function(actual, predicted, weight){
  sqrt(sum((predicted-actual)^2*weight)/sum(weight))
}
weighted.mae <- function(actual, predicted, weight){
  sum(abs(predicted-actual)*weight)/sum(weight)
}
rmse <- function(predicted, actual, remove_na){
  sqrt(mean((predicted - actual)^2, na.rm = remove_na))
}
mae <- function(predicted, actual, remove_na){
  mean(abs(predicted-actual), na.rm = remove_na)
}
#***********************************************************************************************************************


########################################################################################################################
##### PART ONE: INCIDENCE NAT HIST MODEL ###############################################################################
########################################################################################################################


#----PREP---------------------------------------------------------------------------------------------------------------
### get location data
locations <- get_location_metadata(gbd_round_id=gbd_round, location_set_id=22, 
                                   decomp_step=decomp_step)[,.(location_id, ihme_loc_id, location_name,
                                                                location_ascii_name, region_id, super_region_id,
                                                                super_region_name, level, location_type,
                                                                parent_id, sort_order)]
pop_locs <- unique(locations[level >= 3, location_id])

# This set of locations has select subnational locations which are used by modeling tools to minimize impact on regression coefficients
standard_locations <- get_location_metadata(gbd_round_id=gbd_round, location_set_id=101, decomp_step=decomp_step)$location_id %>% unique

# get population for birth cohort at average age of notification by year
if (gbd_round == 7) {
  population <- get_population(location_id=pop_locs, year_id=(1980-a):year_end, age_group_id=c(2:3, 388, 389, 238, 34, 6:20, 30:32, 235), 
                               status="best", sex_id=1:3, gbd_round_id=gbd_round, decomp_step=decomp_step)
  population_young <- population[age_group_id %in% c(2, 3, 388, 389) & sex_id==3, ]
} else if (gbd_round==6) {
  population <- get_population(location_id=pop_locs, year_id=(1980-a):year_end, age_group_id=c(2:20, 30:32, 235), status="best",
                               sex_id=1:3, gbd_round_id=gbd_round, decomp_step=decomp_step)
  population_young <- population[age_group_id %in% c(2, 3, 4) & sex_id==3, ]
} else { 
  population <- get_population(location_id=pop_locs, year_id=(1980-a):year_end, age_group_id=c(2:20, 30:32, 235), status="best",
                                      sex_id=1:3, gbd_round_id=gbd_round)
  population_young <- population[age_group_id %in% c(2, 3, 4) & sex_id==3, ]
}
population_young <- population_young[, year_id := year_id + a] %>% .[year_id <= year_end, ]
# collapse by location-year
sum_pop_young <- population_young[, pop := sum(population), by=c("location_id", "year_id")] %>% .[, .(location_id, year_id, pop)] %>% unique
# save model version
cat(paste0("Population - model run ", unique(population$run_id)),
    file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

### prep SIA data
prep_sia <- function(...) {
  
  ### correct SIA coverage
  SIA <- adjust_sia_data()
  setnames(SIA, "corrected_target_coverage", "supp")
  
  # don't want coverage >100%
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
  
  # add lag to SIA
  for (i in 1:sia_lag) {
    assign(paste("lag", i, sep="_"), copy(SIA))
    get(paste("lag", i, sep="_"))[, year_id := year_id + i]
    setnames(get(paste("lag", i, sep="_")), "supp", paste0("supp_", i))
  }
  
  # bring lags together
  if(sia_lag==5){
    SIAs <- join_all(list(SIA, lag_1, lag_2, lag_3, lag_4, lag_5), by=c("location_id", "year_id"), type="full") %>% as.data.table
  } else if (sia_lag==7) {
    SIAs <- join_all(list(SIA, lag_1, lag_2, lag_3, lag_4, lag_5, lag_6, lag_7), by=c("location_id", "year_id"), type="full") %>% as.data.table
  } else if (sia_lag==10) {
    SIAs <- join_all(list(SIA, lag_1, lag_2, lag_3, lag_4, lag_5, lag_6, lag_7, lag_8, lag_9, lag_10), by=c("location_id", "year_id"), type="full") %>% as.data.table
  }
  
  # make missing SIAs 0 and offset
  for (i in 1:sia_lag) {
    SIAs[is.na(get(paste0("supp_", i))), paste0("supp_", i) := 0]
    SIAs[get(paste0("supp_", i))==0, paste0("supp_", i) := 0.000000001]
  }
  
  return(SIAs)
  
}

### prep incidence (case notification) data
prep_case_notifs <- function(add_subnationals=FALSE) {
  
  # get WHO case notification data, downloaded from ADDRESS
  case_notif <- read_excel("FIELPATH", sheet="Measles") %>% data.table
  case_notif[, c("WHO_REGION", "Cname", "Disease") := NULL]
  setnames(case_notif, "ISO_code", "ihme_loc_id")
  case_notif <- melt(case_notif, variable.name="year_id", value.name="cases", id.vars = "ihme_loc_id")
  case_notif[, year_id := year_id %>% as.character %>% as.integer]
  case_notif[, nid := 83133]
  
  #include 2019 numbers as reported in the most recent supp if 2019 is blank in case_notif
  #calc 2019 annual incident cases
  case_notif_supp <- read_excel("FIELPATH", sheet="WEB") %>% data.table
  setnames(case_notif_supp, c("ISO3", "Year"), c("ihme_loc_id", "year_id"))
  case_notif_supp_2019 <- case_notif_supp[year_id==2019]
  case_notif_supp_2019 <- case_notif_supp_2019[, c("January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December") := 
                                                 lapply(.SD, as.numeric), .SDcols = c("January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December")]
  case_notif_supp_2019 <- case_notif_supp_2019[, cases :=  rowSums(.SD), .SDcols = c("January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December")]
  case_notif_supp_2019 <- case_notif_supp_2019[, c("ihme_loc_id", "year_id", "cases")]
  setnames(case_notif_supp_2019, "cases", "cases_supp")
  
  case_notif <- case_notif[year_id==2019 & is.na(cases), add_supp := 1]
  case_notif <- merge(case_notif, case_notif_supp_2019, by=c("ihme_loc_id", "year_id"), all.x=TRUE)
  case_notif <- case_notif[add_supp==1, cases := cases_supp]
  case_notif <- case_notif[add_supp ==1, nid := 419891][, c("add_supp", "cases_supp") := NULL]
  
  # bring together and clean
  case_notif <- merge(case_notif, locations[, .(location_id, ihme_loc_id)], by="ihme_loc_id", all.x=TRUE)
  
  
  if (add_subnationals) {
    
    ### add subnational case notifications that we trust
    # Japan
    JPN_data_subnational <- list.files(file.path(j_root, "DATA/JPN/INFECTIOUS_DISEASE_WEEKLY_REPORT"),
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
    if (decomp) {JPN <- JPN[year_id %in% c(2012, 2013, 2014, 2015, 2016, 2017, 2018)]}
    JPN_nids <- data.table(year_id=c(2012, 2013, 2014, 2015, 2016, 2017, 2018), nid=c(205877, 206136, 206141, 257236, 282062, 335319, 373828))
    JPN <- merge(JPN, JPN_nids, by="year_id", all.x=TRUE)
    JPN <- merge(JPN, locations[, .(location_id, location_ascii_name, ihme_loc_id)], by="location_ascii_name", all.x=TRUE) %>% .[, .(nid, ihme_loc_id, location_id, year_id, cases)]
    # United States
    USA <- read_excel("/home/j/WORK/12_bundle/measles/00_documentation/data/extraction/US_subnational_notifications_gbd2020.xlsx") %>% data.table %>% .[, .(nid, ihme_loc_id, location_id, year_id, cases)]
    # add to dataset
    case_notif <- bind_rows(case_notif, JPN, USA)
    
    if(new_subnat_data){
      BRA       <- fread("FILEPATH/BRA_subnational_notifications_gbd2020.csv")[year_id != 2017] 
      early_JPN <- fread("FILEPATH/JPN_subnational_notifications_gbd2020.csv") #data from years before the JPN data that is read in above
      GBR       <- fread("FILEPATH/GBR_subnational_notifications_gbd2020.csv")
      IDN       <- fread("FILEPATH/IDN_subnational_notifications_gbd2020.csv")
      ITA       <- fread("FILEPATH/ITA_subnational_notifications_gbd2020.csv") 
      POL       <- fread("FILEPATH/POL_subnational_notifications_gbd2020.csv")
      ZAF       <- fread("FILEPATH/ZAF_subnational_notifications_gbd2020.csv")
      
      new_subnat <- rbind(BRA, early_JPN, GBR, IDN, ITA, POL, ZAF)
      setnames(new_subnat, "NID", "nid")
      new_subnat <- merge(new_subnat, locations[, .(location_id, ihme_loc_id)], by="ihme_loc_id", all.x=T) %>% .[, .(nid, ihme_loc_id, location_id, year_id, cases)]
      
      case_notif <- bind_rows(case_notif, new_subnat)
    }
    
    
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
  if(is.null(custom_mcv1_coverage)){
    
    if (use_lagged_covs) {
      mcv1 <- get_covariate_estimates(covariate_id=2309, gbd_round_id=gbd_round, decomp_step=decomp_step)
    } else {
      if (decomp) {
        mcv1 <- get_covariate_estimates(covariate_id=75, gbd_round_id=gbd_round, decomp_step=decomp_step) 
      }
      else {
        mcv1 <- get_covariate_estimates(covariate_id=75, gbd_round_id=gbd_round)
      }
    }
    
  } else {
    
    if (use_lagged_covs) {
      mcv1 <- get_covariate_estimates(covariate_id=2309, model_version_id = custom_mcv1_coverage, release_id = release_id)
    } else {
      if (decomp) {
        mcv1 <- get_covariate_estimates(covariate_id=75, gbd_round_id=gbd_round, decomp_step=decomp_step,  model_version_id = custom_mcv1_coverage) 
      }
      else {
        mcv1 <- get_covariate_estimates(covariate_id=75, gbd_round_id=gbd_round,  model_version_id = custom_mcv1_coverage)
      }
    }
    
  }
 
  mcv1[, ln_unvacc := log(1 - mean_value)]
  setnames(mcv1, "mean_value", "mean_mcv1")
  # save model version
  cat(paste0("Covariate MCV1 (NF) - model version ", unique(mcv1$model_version_id)), file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  #remove excess columns
  mcv1 <- mcv1[, .(location_id, year_id, ln_unvacc, mean_mcv1)]
  
  ### covariate: covariate_name_short="measles_vacc_cov_prop_2", covariate_id=1108
  if(is.null(custom_mcv2_coverage)){
    
    if (use_lagged_covs) {
      mcv2 <- get_covariate_estimates(covariate_id=2310, gbd_round_id=gbd_round, decomp_step=decomp_step)
    } else {
      if (decomp) mcv2 <- get_covariate_estimates(covariate_id=1108, gbd_round_id=gbd_round, decomp_step=decomp_step) else mcv2 <- get_covariate_estimates(covariate_id=1108, gbd_round_id=gbd_round)
    }
    
  } else {
    
    if (use_lagged_covs) {
      mcv2 <- get_covariate_estimates(covariate_id=2310, model_version_id = custom_mcv2_coverage, release_id = release_id)
    } else {
      if (decomp) mcv2 <- get_covariate_estimates(covariate_id=1108, gbd_round_id=gbd_round, decomp_step=decomp_step, model_version_id = custom_mcv2_coverage) else mcv2 <- get_covariate_estimates(covariate_id=1108, gbd_round_id=gbd_round, model_version_id = custom_mcv2_coverage)
    }
    
  }
  
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
  # merge on population 
  regress <- join(regress, sum_pop_young, by=c("location_id", "year_id"), type="inner")
  # merge on ihme covariates and WHO SIA data
  regress <- join(regress, covs, by=c("location_id", "year_id"), type="inner")
  regress <- join(regress, SIAs, by=c("location_id", "year_id"), type="left")
  
  # fix missing lags
  regress[is.na(supp), supp := 0]
  for (i in 1:sia_lag) {
    regress[is.na(get(paste0("supp_", i))), paste0("supp_", i) := 0.000000001]
    regress[get(paste0("supp_", i))==0, paste0("supp_", i) := 0.000000001]
  }
  
  if (ln_sia) {
    # Calculate ln_unvacc equivalent for SIAs 
    for (i in 1:sia_lag) {
      regress[, paste0("ln_unvacc_supp_", i) := log(1-get(paste0("supp_", i)))]
    }
    
  }
  
  ### set up regression
  # incidence rate generated from the population just for the 1 year birth cohort at average age of notification
  regress[, inc_rate := (cases / pop) * 100000]
  regress <- regress[inc_rate <= 95000, ]
  # outlier PNG outbreaks
  regress <- regress[!(ihme_loc_id=="PNG" & year_id %in% c(2013:2015)), ]
  
  ### generate transformed variables
  # proportion of population unvaccinated
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
  
  regress <- regress[year_id >= 1995, ]  
  
  return(regress)
  
}

### prepare regression input data
covs <- prep_covs()
SIAs <- prep_sia()
case_notif <- prep_case_notifs()
regress <- prep_regression()
regress <- regress[location_id %in% standard_locations]

# prep for folds if doing OOS validation
if (!run_oos & n_folds != 0) {
  message("Resetting n_folds to 0 since run_oos = FALSE.")
  message("If you meant to run an out-of-sample model, please check your options.")
  n_folds <- 0
}
# Generate folds (holdouts)
# holding out by ihme_loc_id
if (run_oos == TRUE) {
  
  # Assign each country to a fold
  
  # Generate a table of the unique ihme_loc_ids
  holdout_link_table <- data.table(ihme_loc_id = unique(regress$ihme_loc_id))
  
  # Generate folds by taking random numbers for each row and dividing into quantiles
  # This ensures that groups are random and as equally sized as possible
  random_draws <- rnorm(nrow(holdout_link_table))
  quantiles <- quantile(random_draws, 0:n_folds/n_folds)
  
  which_folds <- cut(random_draws, quantiles, include.lowest = TRUE)
  levels(which_folds) <- 1:n_folds
  
  # Assign these to the ihme_loc_ids
  holdout_link_table[, fold_id := as.numeric(which_folds)]
  
  # Merge with input data set
  regress <- merge(regress, holdout_link_table, by = "ihme_loc_id")
  
  # Create id column to later identify the location years in a given fold
  regress[, id := paste0(location_id, year_id)]
  
}


# save model input
fwrite(regress, file.path(j.version.dir.inputs, "case_regression_input.csv"), row.names=FALSE)
#***********************************************************************************************************************

# If running to calculate RR's, call the function to fit model with unlagged coverage, save summary of model and then function will automatically STOP modeling.
if(run_for_rrs){
  #model with coverage (not ln(1-coverage)) as covariate so can calculate rr as exp(beta)
  if(custom_version == "09.17.20"){
    # on this run date wanted to calculate RR's using GBD2019 best covariate versions
    regress <- fread("FIELPATH/case_regression_input.csv")
  }
  source(paste0("FILEPATH/rr_custom_incidence_model.R"))
  model_for_rr_beta <- fit_model_for_rrs(acause = acause)

}

#----MIXED EFFECTS MODEL------------------------------------------------------------------------------------------------
for (fold in 0:n_folds) {
  
  # Set up your input data set
  if (fold != 0) {
    
    # Subset data to only the data in the fold
    regress_fold <-  regress[fold_id!=fold,]
    
  } else {
    
    # Use all of the input data
    regress_fold <- copy(regress)
  }
  
  ### scale regression covariates
  if (scale_to_converge) {
    ## save the means
    mean_u1 <- mean(regress_fold$ln_unvacc)
    mean_u2 <- mean(regress_fold$ln_unvacc_mcv2)

    mean_s1 <- mean(regress_fold$supp_1)
    mean_s2 <- mean(regress_fold$supp_2)
    mean_s3 <- mean(regress_fold$supp_3)
    mean_s4 <- mean(regress_fold$supp_4)
    mean_s5 <- mean(regress_fold$supp_5)
    
    ## save the standard devs
    sd_u1 <- sd(regress_fold$ln_unvacc)
    sd_u2 <- sd(regress_fold$ln_unvacc_mcv2)
    sd_s1 <- sd(regress_fold$supp_1)
    sd_s2 <- sd(regress_fold$supp_2)
    sd_s3 <- sd(regress_fold$supp_3)
    sd_s4 <- sd(regress_fold$supp_4)
    sd_s5 <- sd(regress_fold$supp_5)
    
    ## manually scale
    regress_fold[, ln_unvacc := (ln_unvacc - mean_u1)/sd_u1]
    regress_fold[, ln_unvacc_mcv2 := (ln_unvacc_mcv2 - mean_u2)/sd_u2]
    regress_fold[, supp_1 := (supp_1-mean_s1)/sd_s1]
    regress_fold[, supp_2 := (supp_2-mean_s2)/sd_s2]
    regress_fold[, supp_3 := (supp_3-mean_s3)/sd_s3]
    regress_fold[, supp_4 := (supp_4-mean_s4)/sd_s4]
    regress_fold[, supp_5 := (supp_5-mean_s5)/sd_s5]
  }
  
  ### run mixed effects regression model
  if (MCV1_or_MCV2 == "use_MCV2") {
    if(sia_lag==5){
      me_model <- lmer(ln_inc ~ ln_unvacc + ln_unvacc_mcv2 + supp_1 + supp_2 + supp_3 + supp_4 + supp_5 +
                         (1 | super_region_id) + (1 | region_id) + (1 | location_id), data=regress_fold,
                       control = lmerControl(optimizer = "nloptwrap", calc.derivs = FALSE))
    } else if (sia_lag==7){
      me_model <- lmer(ln_inc ~ ln_unvacc + ln_unvacc_mcv2 + supp_1 + supp_2 + supp_3 + supp_4 + supp_5 + supp_6 + supp_7 +
                         (1 | super_region_id) + (1 | region_id) + (1 | location_id), 
                       data=regress_fold,control = lmerControl(optimizer = "nloptwrap", calc.derivs = FALSE))
      
    } else if (sia_lag==10){
      me_model <- lmer(ln_inc ~ ln_unvacc + ln_unvacc_mcv2 + supp_1 + supp_2 + supp_3 + supp_4 + supp_5 + supp_6 + supp_7 + supp_8 + supp_9 + supp_10 + 
                         (1 | super_region_id) + (1 | region_id) + (1 | location_id), 
                       data=regress_fold,control = lmerControl(optimizer = "nloptwrap", calc.derivs = FALSE))
      
    }
   
  
    
    if (ln_sia) {
      if(sia_lag==5){
        me_model <- lmer(ln_inc ~ ln_unvacc + ln_unvacc_mcv2 + ln_unvacc_supp_1 + ln_unvacc_supp_2 + ln_unvacc_supp_3 + ln_unvacc_supp_4 + ln_unvacc_supp_5 +
                           (1 | super_region_id) + (1 | region_id) + (1 | location_id), data=regress_fold)
      } else if(sia_lag==7){
        me_model <- lmer(ln_inc ~ ln_unvacc + ln_unvacc_mcv2 + ln_unvacc_supp_1 + ln_unvacc_supp_2 + ln_unvacc_supp_3 + ln_unvacc_supp_4 + ln_unvacc_supp_5 +
                           ln_unvacc_supp_6 + ln_unvacc_supp_7 +
                           (1 | super_region_id) + (1 | region_id) + (1 | location_id), data=regress_fold)
      } else if (sia_lag==10){
        me_model <- lmer(ln_inc ~ ln_unvacc + ln_unvacc_mcv2 + ln_unvacc_supp_1 + ln_unvacc_supp_2 + ln_unvacc_supp_3 + ln_unvacc_supp_4 + ln_unvacc_supp_5 +
                           ln_unvacc_supp_6+ ln_unvacc_supp_7 + ln_unvacc_supp_8 + ln_unvacc_supp_9 + ln_unvacc_supp_10 +
                           (1 | super_region_id) + (1 | region_id) + (1 | location_id), data=regress_fold)
      }
      
    }
    
  } else if (MCV1_or_MCV2 == "MCV1_only") {
    
    me_model <- lmer(ln_inc ~ ln_unvacc + supp_1 + supp_2 + supp_3 + supp_4 + supp_5 +
                       (1 | super_region_id) + (1 | region_id) + (1 | location_id), data=regress_fold)
    
  }
  
  ### save summary of object and model object and error
  if(fold==0){
    
    capture.output(summary(me_model), file=file.path(j.version.dir.logs, "log_incidence_mereg.txt"), type="output") 
    saveRDS(me_model, file=file.path(j.version.dir.logs, "log_incidence_mereg.rds"))
    
  } else {
    
    capture.output(summary(me_model), file=file.path(j.version.dir.logs, paste0("log_incidence_mereg_", fold, ".txt")), type="output")
    saveRDS(me_model, file=file.path(j.version.dir.logs, paste0("log_incidence_mereg_", fold, ".rds")))
    
  }

  #calculate and save in sample fit stats for model
  regress_fold[, obs:=exp(regress_fold$ln_inc)]
  regress_fold[!(is.na(obs)), predicted := exp(predict(me_model, regress_fold[!(is.na(obs)),], type="response"))]
  inc_rmse <- rmse(regress_fold$predicted, regress_fold$obs, remove_na=T)
  inc_mae <- mae(regress_fold$predicted, regress_fold$obs, remove_na=T)
  if(fold==0) cat(c("\nRMSE: ", inc_rmse, "\nMAE: ", inc_mae), file=file.path(j.version.dir.logs, "log_incidence_mereg.txt"), append=TRUE) else cat(c("\nRMSE: ", inc_rmse, "\nMAE: ", inc_mae), file=file.path(j.version.dir.logs, paste0("log_incidence_mereg_", fold, ".txt")), append=TRUE)

#***********************************************************************************************************************


#----DRAWS--------------------------------------------------------------------------------------------------------------
  set.seed(0311)  
  ### prep prediction data
  # merge ihme_loc_id, population, ihme covariates, and WHO SIA data
  draws_nonfatal <- merge(covs, locations[, .(location_id, ihme_loc_id, region_id, super_region_id)], by="location_id", all.x=TRUE)
  draws_nonfatal <- merge(draws_nonfatal, sum_pop_young, by=c("location_id", "year_id"), all.x=TRUE)
  draws_nonfatal <- merge(draws_nonfatal, SIAs[, c("location_id", "year_id", paste0("supp_", 1:sia_lag)), with=F], by=c("location_id", "year_id"), all.x=TRUE)
  
  if(fold!=0){
    # subset to location years in draws_nonfatal that were in fold
    draws_nonfatal[, id := paste0(location_id, year_id)]
    draws_nonfatal <- draws_nonfatal[id %in% regress[fold_id == fold, id], ]
    
  }
  # fix missing lags
  for (i in 1:sia_lag) {
    if (ln_sia) {
      draws_nonfatal[is.na(get(paste0("supp_", i))), paste0("supp_", i) := 0.000000001]
      
    } else {
      draws_nonfatal[is.na(get(paste0("supp_", i))), paste0("supp_", i) := 0]
    }
  }
  
  if (ln_sia) {
    # Calculate ln_unvacc equivalent for SIAs
    for (i in 1:sia_lag) {
      draws_nonfatal[, paste0("ln_unvacc_supp_", i) := log(1-get(paste0("supp_", i)))]
    }
  }
  
  
  if (scale_to_converge) {
    ## manually scale
    draws_nonfatal[, ln_unvacc := (ln_unvacc - mean_u1)/sd_u1]
    draws_nonfatal[, ln_unvacc_mcv2 := (ln_unvacc_mcv2 - mean_u2)/sd_u2]
    draws_nonfatal[, supp_1 := (supp_1-mean_s1)/sd_s1]
    draws_nonfatal[, supp_2 := (supp_2-mean_s2)/sd_s2]
    draws_nonfatal[, supp_3 := (supp_3-mean_s3)/sd_s3]
    draws_nonfatal[, supp_4 := (supp_4-mean_s4)/sd_s4]
    draws_nonfatal[, supp_5 := (supp_5-mean_s5)/sd_s5]
  }
  
  
  ### set options
  if (MCV1_or_MCV2 == "use_MCV2") {
    if(sia_lag==5){
      cols <- c("constant", "b_ln_unvacc", "b_ln_unvacc_mcv2", "b_lag_1", "b_lag_2", "b_lag_3", "b_lag_4", "b_lag_5")
    } else if(sia_lag==7){
      cols <- c("constant", "b_ln_unvacc", "b_ln_unvacc_mcv2", "b_lag_1", "b_lag_2", "b_lag_3", "b_lag_4", "b_lag_5", "b_lag_6", "b_lag_7")
    } else if(sia_lag==10){
      cols <- c("constant", "b_ln_unvacc", "b_ln_unvacc_mcv2", "b_lag_1", "b_lag_2", "b_lag_3", "b_lag_4", "b_lag_5", "b_lag_6", "b_lag_7", "b_lag_8", "b_lag_9", "b_lag_10")
    }
  } else if (MCV1_or_MCV2 == "MCV1_only") {
    if(sia_lag==5){
      cols <- c("constant", "b_ln_unvacc", "b_lag_1", "b_lag_2", "b_lag_3", "b_lag_4", "b_lag_5")
    } else if(sia_lag==7){
      cols <- c("constant", "b_ln_unvacc", "b_lag_1", "b_lag_2", "b_lag_3", "b_lag_4", "b_lag_5", "b_lag_6", "b_lag_7")
    } else if(sia_lag==10){
      cols <- c("constant", "b_ln_unvacc", "b_lag_1", "b_lag_2", "b_lag_3", "b_lag_4", "b_lag_5", "b_lag_6", "b_lag_7", "b_lag_8", "b_lag_9", "b_lag_10")
    }
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
    if (MCV1_or_MCV2 == "use_MCV2") { 
      if(sia_lag == 5){
        vcovlist_a <- c(vcovmat[ii, 1], vcovmat[ii, 2], vcovmat[ii, 3], vcovmat[ii, 4], vcovmat[ii, 5], vcovmat[ii, 6], vcovmat[ii, 7], vcovmat[ii, 8])
      } else if (sia_lag == 7){
        vcovlist_a <- c(vcovmat[ii, 1], vcovmat[ii, 2], vcovmat[ii, 3], vcovmat[ii, 4], vcovmat[ii, 5], vcovmat[ii, 6], vcovmat[ii, 7], vcovmat[ii, 8], vcovmat[ii, 9], vcovmat[ii,10])
      } else if (sia_lag == 10){
        vcovlist_a <- c(vcovmat[ii, 1], vcovmat[ii, 2], vcovmat[ii, 3], vcovmat[ii, 4], vcovmat[ii, 5], vcovmat[ii, 6], vcovmat[ii, 7], vcovmat[ii, 8], vcovmat[ii, 9], vcovmat[ii,10], vcovmat[ii,11], vcovmat[ii,12], vcovmat[ii,13])
      }
    } else if (MCV1_or_MCV2 == "MCV1_only") { 
      vcovlist_a <- c(vcovmat[ii, 1], vcovmat[ii, 2], vcovmat[ii, 3], vcovmat[ii, 4], vcovmat[ii, 5], vcovmat[ii, 6], vcovmat[ii, 7]) 
      }
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
  
  ### If running model on full data, predict out draws as well as in sample point estimate
  if((run_oos == TRUE & fold == 0) | run_oos == FALSE){
    
    #if running OOS calculate in sample point predictions for calculating is and oos error later
    if (run_oos == T) draws_nonfatal[, is_mean := (((exp(predict(me_model, newdata=draws_nonfatal, allow.new.levels = TRUE, type="response")))/100000)*pop)]
    
    #predict draws
    if (MCV1_or_MCV2 == "use_MCV2") {
      if(sia_lag==5){
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
        
      } else if (sia_lag==7){
        invisible(
          draws_nonfatal[, (draw_cols) := lapply(draw_nums, function(x) { exp( betas[1, x] +
                                                                                 ( betas[2, x] * ln_unvacc ) +
                                                                                 ( betas[3, x] * ln_unvacc_mcv2 ) +
                                                                                 ( betas[4, x] * supp_1 ) +
                                                                                 ( betas[5, x] * supp_2 ) +
                                                                                 ( betas[6, x] * supp_3 ) +
                                                                                 ( betas[7, x] * supp_4 ) +
                                                                                 ( betas[8, x] * supp_5 ) +
                                                                                 ( betas[9, x] * supp_6 ) +
                                                                                 ( betas[10, x] * supp_7 ) +
                                                                                 standard_RE ) *
              ( pop / 100000 ) } )] )
      } else if (sia_lag==10){
        invisible(
          draws_nonfatal[, (draw_cols) := lapply(draw_nums, function(x) { exp( betas[1, x] +
                                                                                 ( betas[2, x] * ln_unvacc ) +
                                                                                 ( betas[3, x] * ln_unvacc_mcv2 ) +
                                                                                 ( betas[4, x] * supp_1 ) +
                                                                                 ( betas[5, x] * supp_2 ) +
                                                                                 ( betas[6, x] * supp_3 ) +
                                                                                 ( betas[7, x] * supp_4 ) +
                                                                                 ( betas[8, x] * supp_5 ) +
                                                                                 ( betas[9, x] * supp_6 ) +
                                                                                 ( betas[10, x] * supp_7 ) +
                                                                                 ( betas[11, x] * supp_8 ) +
                                                                                 ( betas[12, x] * supp_9 ) +
                                                                                 ( betas[13, x] * supp_10 ) +
                                                                                 standard_RE ) *
              ( pop / 100000 ) } )] )
      }
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
    
    #subset to important columns before saving
    if(fold==0 & run_oos == T) draws_nonfatal <- draws_nonfatal[, c("location_id", "year_id", "is_mean", draw_cols), with=FALSE]
    if(run_oos == F) draws_nonfatal <- draws_nonfatal[, c("location_id", "year_id", draw_cols), with=FALSE]
  } else if (run_oos == TRUE & fold!=0){
    
    draws_nonfatal[, oos_mean := (((exp(predict(me_model, newdata=draws_nonfatal, allow.new.levels = TRUE, type="response")))/100000)*pop)]

  }
    
  ### save results
  if (WRITE_FILES == "yes") {
    if(fold==0) fwrite(draws_nonfatal, file.path(j.version.dir, "01_case_predictions_from_model.csv"), row.names=FALSE) else fwrite(draws_nonfatal, file.path(j.version.dir, paste0("01_case_predictions_from_model_", fold, ".csv")), row.names=FALSE)
  }
  
}

### Calculate oos error and then proceed with draws_nonfatal from full model if running OOS model by reading in draws_nonfatal made on full sample
if(run_oos == T){
  calculate_oos_error(preds.dir = j.version.dir, input.data.dir = j.version.dir.inputs, output.dir = j.version.dir.logs, folds = n.folds)
  draws_nonfatal <- fread(file.path(j.version.dir, "01_case_predictions_from_model.csv"))
  #drop in sample prediction column created for model validation before proceeding with model
  draws_nonfatal[, is_mean:=NULL]
}

### read in original GBD 2019 Step 4 01_case_predictions and save to active j.version.dir for consistency
if (late_2019_ii) {
  draws_nonfatal <- fread("FILEPATH/01_case_predictions_from_model.csv")
  fwrite(draws_nonfatal, file.path(j.version.dir, "01_case_predictions_from_model.csv"), row.names=FALSE)
}
#***********************************************************************************************************************



#----FIX MODEL ESTIMATES------------------------------------------------------------------------------------------------
### get population
if (gbd_round > 6) { 
  pop_u5 <- population[year_id %in% c(1980:year_end) & age_group_id %in% c(2, 3, 388, 389, 238, 34) & sex_id==3, ]
  pop_u5 <- pop_u5[, pop := sum(population), by=c("location_id", "year_id")] %>% .[, .(location_id, year_id, pop)] %>% unique
} else {
  pop_u5 <- population[year_id %in% c(1980:year_end) & age_group_id %in% c(2:5) & sex_id==3, ]
  pop_u5 <- pop_u5[, pop := sum(population), by=c("location_id", "year_id")] %>% .[, .(location_id, year_id, pop)] %>% unique
}

### PART A - countries where we trust case notifications
### assume cases reported well in WHO case notifications in some super regions -- use model for the others
# keep only countries from 3 super regions: high income, eastern europe/central asia, latin america/caribbean
sr_64_31_103 <- locations[super_region_id %in% c(64, 31, 103), location_id]
case_notif <- prep_case_notifs(add_subnationals=TRUE)
# ignore GBD locations in these super regions that do not have case notifications (i.e. BMU, GRL, PRI, VIR, and
# other high-come subnationals levels)
# include subnational case notifications (instead of just level==3)
trusted_notifications <- sr_64_31_103[sr_64_31_103 %in% unique(case_notif$location_id)]

# add on add'l elimination locations to trusted_notif
elimination_locs <- data.table(ihme_loc_id = elimination_locs)
elimination_locs <- merge(elimination_locs, locations[, c("location_id", "ihme_loc_id")], by="ihme_loc_id")
elimination_locs <- elimination_locs[, location_id]
# only keep locs for which we have CN data
elimination_locs <- elimination_locs[elimination_locs %in% unique(case_notif$location_id)]
trusted_notifications <- c(trusted_notifications, elimination_locs)
trusted_notifications <- unique(trusted_notifications)

### model case notifications in ST-GPR to fix missingness in WHO reporting data
if (run_case_notification_smoothing) {
  source(file.path("/FILEPATH/est_shocks_copy.R"))
  case_notification_smoothing()
}

### pull case notifications from st-gpr
message("prepping to read in ST-GPR results")
st_gpr_runs <- grep("case_notifications", list.dirs("FILEPATH"), value = TRUE)
if(st_gpr_version == "latest"){
  st_gpr_paths <- file.info(st_gpr_runs)
  st_gpr_path <- rownames(st_gpr_paths)[which.max(st_gpr_paths$mtime)]
} else {
  st_gpr_path <- grep(st_gpr_version, st_gpr_runs, value=T)
}
adjusted_notifications_q <- lapply(list.files(st_gpr_path, full.names = T), fread) %>% 
  rbindlist %>% .[, .(location_id, year_id, combined)] %>% setnames(., "combined", "cases")
message("read in ST-GPR results")

#######################################################################
# HERE NEED TO ADD IN TOGGLE TO SWAP IN ALL REPLACING REPORTED CASES! #
#######################################################################
### if late_2019, need to swap in actual 2019 annualized reported cases
### for NZL, ARG, COL, VEN, BRA, USA (WSM -- group 3 -- comes later)
if (late_2019) {
  if(gbd_round==6){
    annualized_cases <- fread("/FILEPATH/2019_outbreak_locs_prepped.csv")
    setnames(annualized_cases, "annualized_report", "cases")
  } else if (gbd_round==7){
    annualized_cases <- fread("/FILEPATH/2020_outbreak_locs_prepped.csv")
    annualized_cases <- annualized_cases[!(location_id %in% c(527,570,532,544,569, 4657))]
    # add level to identify when to replace which locations
    annualized_cases <- merge(annualized_cases, locations[,.(location_id, level)], by="location_id")
    setnames(annualized_cases, "corrected_cases", "cases")
    
  }
  
  replacements <- unique(annualized_cases$location_id)
  removed_to_replace <- adjusted_notifications_q[!(location_id %in% replacements & year_id==2020), ]
  adjusted_notifications_q <- rbind(removed_to_replace, annualized_cases[, .(location_id, year_id, cases)])

}

usa_cases <- fread("/FILEPATH/2019_outbreak_locs_usa_subs_prepped.csv")
setnames(usa_cases, "case_report", "cases")
usa_replacements <- unique(usa_cases$location_id)
removed_to_replace <- adjusted_notifications_q[!(location_id %in% usa_replacements & year_id==2019), ]
adjusted_notifications_q <- rbind(removed_to_replace, usa_cases[, .(location_id, year_id, cases)])

bmu <- data.table(location_id = 305, year_id = year_start:year_end, cases = 0)
adjusted_notifications_q <- rbind(adjusted_notifications_q, bmu) 
#######################################################################



### generate error
# calculate incidence rate from case notification and population data
adjusted_notifications <- merge(adjusted_notifications_q, pop_u5, by=c("location_id", "year_id"), all.x=TRUE)
adjusted_notifications <- merge(adjusted_notifications, locations[, .(location_id, region_id, super_region_id)], by="location_id", all.x=TRUE)
invisible(adjusted_notifications[, inc_rate := cases / pop])

# Adjust high ST-GPR fits
wide <- dcast.data.table(adjusted_notifications_q, value.var="cases", location_id ~ year_id)

colnames(wide) <- c("location_id", paste0("x_", 1980:year_end))

weird_locs <- as.data.table(wide[x_2020 > (2*x_2019) & x_2020 > (2*x_2018) & x_2020 > (2*x_2017) & x_2020 > (2*x_2016) & x_2020 > 5, location_id]) 
weird_locs$year <- 2020
#if the weird locs getting tagged because of actual reported number of cases, remove it from weird locs 
weird_locs <- weird_locs[!(weird_locs[[1]] %in% case_notif[year_id==2020, location_id])]
### remove any weird locs that were flagged but are actually new outbreaks
if (late_2019) weird_locs <- weird_locs[!(weird_locs[[1]] %in% c(replacements))]
#proceed with identifying locations that ST-GPR predicted high in future years
add_on <- as.data.table(wide[x_2021 > 2*x_2019 & x_2021 > 2*x_2018 & x_2021 > 2*x_2017 & x_2021 > 2*x_2016 & x_2021 > 5, location_id]) 
add_on$year <- 2021
weird_locs <- rbind(weird_locs, add_on)
add_on <- as.data.table(wide[x_2022 > 2*x_2019 & x_2022 > 2*x_2018 & x_2022 > 2*x_2017 & x_2022 > 2*x_2016 & x_2022 > 5, location_id])
add_on$year <- 2022
weird_locs <- rbind(weird_locs, add_on)
#
weird_locs <- set_names(weird_locs, c("location_id", "year_id"))
fwrite(weird_locs, file.path(j.version.dir.logs, "weird_locs.csv"))


year_end <- as.integer(year_end)                                    

# adjust preds for weird locs that were ID'd
for(i in 1:nrow(weird_locs)){
  loc_to_adjust <- weird_locs[i, location_id]
  year_to_adjust <- weird_locs[i, year_id]
  adjusted_notifications[location_id==loc_to_adjust & year_id==year_to_adjust, 
                         inc_rate := round(mean(adjusted_notifications[location_id== loc_to_adjust & year_id %in% c((2020 - 3):(2020 - 1)), inc_rate]), 0)]
}

# floor inc at 0
adjusted_notifications[inc_rate < 0, inc_rate := 0]

# use binom instead of rnorm
if(trusted_case_distrib == "binom"){
  # check for disagreement between subnational sum and national cases when all zeros in subnationals
  subnat_sums <- merge(adjusted_notifications, locations[,.(location_id, parent_id)], by = "location_id")[,.(subnat_sum = sum(cases)),.(parent_id, year_id)]
  setnames(subnat_sums, "parent_id", "location_id")
  subnat_sums <- merge(subnat_sums, adjusted_notifications[,.(location_id, year_id, cases)], by = c("location_id", "year_id"))
  if(nrow(subnat_sums[subnat_sum == 0 & cases > 0 & location_id != 95]) > 0) stop(paste0("You have ",  nrow(subnat_sums[subnat_sum == 0 & cases > 1 & location_id != 95]), " location-years where the sum of the subnat cases is 0 and the parent is greater than 1."))

  # simulate incidence draws
  inc_draws <- rbinom(n=1000 * length(adjusted_notifications$inc_rate), prob=adjusted_notifications$inc_rate, size=as.integer(adjusted_notifications$pop)) %>% matrix(., ncol=1000, byrow=FALSE) %>% as.data.table
  colnames(inc_draws) <- paste0("inc_draw_", draw_nums_gbd)
  adjusted_notifications <- cbind(adjusted_notifications, inc_draws)
  inc_draws_names <- paste0("inc_draw_", draw_nums_gbd)
  adjusted_notifications[, (inc_draws_names) := lapply(.SD, function(x) x/pop), .SDcols = inc_draws_names]
} else if (trusted_case_distrib == "norm"){
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
  inc_draws <- rnorm(n=1000 * length(adjusted_notifications$inc_rate), mean=adjusted_notifications$inc_rate, sd=adjusted_notifications$SE) %>% matrix(., ncol=1000, byrow=FALSE) %>% as.data.table
  adjusted_notifications <- cbind(adjusted_notifications, inc_draws)
}

### calculate cases from incidence and population
invisible(adjusted_notifications[, (draw_cols) := lapply(draw_nums_gbd, function(x) get(paste0("inc_draw_", x)) * pop)])
invisible(adjusted_notifications <- adjusted_notifications[, c("location_id", "year_id", draw_cols), with=FALSE])

if (run_shocks_adjustment) {
  ### PART B - countries where we do not trust case notifications but want to adjust for shocks (i.e. outbreaks),
  ### trust the trend in case notifications but not the absolute value
  # get list of countries with case notifications but are not in the 3 trusted super regions
  untrusted_locations <- case_notif[!location_id %in% trusted_notifications, location_id] %>% unique
  # get draws from modeled case notifications where we don't trust case notifications, to adjust for underreporting and shocks
  shocks_cases <- adjusted_notifications[location_id %in% untrusted_locations, ]
  # run ST-GPR shocks adjustment (smoothed difference between model case notifications through time)
  source(file.path("/FILEPATH/est_shocks.r"))
  shocks_adjustment()
  
  # get results
  shocks_difference <- lapply(list.files(cl.shocks.dir, full.names=TRUE), fread) %>% rbindlist %>%
    .[, .(location_id, year_id, mean)] %>% setnames(., "mean", "shock")
  # apply modeled difference to modeled draws
  shocks_cases <- merge(shocks_cases, shocks_difference, by=c("location_id", "year_id"), all.x=TRUE)
  shocks_cases[, (draw_cols) := lapply(draw_nums_gbd, function(x) get(paste0("case_draw_", x)) + shock)]
  shocks_cases <- shocks_cases[, c("location_id", "year_id", draw_cols), with=FALSE]
  
  ### bring together modeled case draws and notification case draws
  combined_case_draws <- bind_rows(shocks_cases[location_id %in% untrusted_locations, ],
                                   adjusted_notifications[location_id %in% trusted_notifications, ],
                                   ### PART C - use model estimates for countries with no case notification data
                                   draws_nonfatal[!location_id %in% c(untrusted_locations, trusted_notifications), ])
} else {
  combined_case_draws <- bind_rows(adjusted_notifications[location_id %in% trusted_notifications | location_id == 305, ], 
                                   draws_nonfatal[!location_id %in% trusted_notifications & !location_id == 305, ])
  
  #######################################################################
  ###### ADD IN OUTBREAKS IN NON TRUSTED CN national locations!!!! ######
  #######################################################################
  if (late_2019) {
    # grp 3 shocks locations
    wsm_2019 <- adjusted_notifications[location_id==27 & year_id==2019]
    combined_case_draws <- combined_case_draws[!(location_id==27 & year_id==2019)]
    combined_case_draws <- rbind(combined_case_draws, wsm_2019)
    grp_3_shock_locs <- annualized_cases[!(location_id %in% trusted_notifications) & level==3 & year_id==2020, location_id]
    grp_3_shocks <- adjusted_notifications[location_id %in% grp_3_shock_locs & year_id==2020]
    combined_case_draws <- combined_case_draws[!(location_id %in% grp_3_shock_locs & year_id==2020)]
    combined_case_draws <- rbind(combined_case_draws, grp_3_shocks)
  }
  #######################################################################
  
  
  
  
}

##### NF COVID ADJUSTMENT ######################
if(covid_adjustment){
  # save to covid adjusted ME
  me_id <- 26944
  
  # replace 2020 draws with covid inclusive 2020 draws
  # still use for 2020 from combined_case_draws to get relative proportions by which to split trusted loc nat'l into subnats during raking
  covid_case_draws <- fread(adjusted_case_draws_path)[, cause_id := NULL][, year_id := 2020]
  setnames(covid_case_draws, grep("draw", names(covid_case_draws), value = T), draw_cols)
  adjusted_case_draws <- combined_case_draws[!(location_id %in% covid_case_draws$location_id & year_id == 2020)]
  adjusted_case_draws <- rbind(adjusted_case_draws, covid_case_draws)
  combined_case_draws <- copy(adjusted_case_draws)
  
}

### save results
if (WRITE_FILES == "yes") {
  fwrite(combined_case_draws, file.path(j.version.dir, "02_add_case_notifs_and_shocks.csv"), row.names=FALSE)
  
  if (run_shocks_adjustment) {
    ### save for plotting
    # get mean of results and steps
    modeled_draws_q <- collapse_point(draws_nonfatal) %>% .[, .(location_id, year_id, mean)] %>% setnames(., "mean", "model_cases")
    notifs <- copy(adjusted_notifications_q[location_id %in% unique(case_notif$location_id), ]) %>% setnames(., "cases", "filled_case_notifications")
    final_cases_q <- collapse_point(combined_case_draws) %>% .[, .(location_id, year_id, mean)] %>% setnames(., "mean", "final_cases")
    gbd_2016 <- fread(file.path(home, "FILEPATH/02_case_predictions_fixed_for_rescaling.csv")) %>% collapse_point %>%
      .[, .(location_id, year_id, mean)] %>% setnames(., "mean", "gbd_2016")
    # bring together
    save_cases_to_plot <- merge(modeled_draws_q, notifs, by=c("location_id", "year_id"), all.x=TRUE)
    save_cases_to_plot <- merge(save_cases_to_plot, final_cases_q, by=c("location_id", "year_id"), all.x=TRUE)
    save_cases_to_plot <- merge(save_cases_to_plot, gbd_2016, by=c("location_id", "year_id"), all.x=TRUE)
    save_cases_to_plot <- merge(save_cases_to_plot, shocks_difference, by=c("location_id", "year_id"), all.x=TRUE)
    # add location tags
    save_cases_to_plot[location_id %in% trusted_notifications, location_source := "final model using filled case notifications"]
    save_cases_to_plot[location_id %in% untrusted_locations, location_source := "final model using shocks adjustment of case notifications"]
    save_cases_to_plot[!location_id %in% c(untrusted_locations, trusted_notifications), location_source := "no case notifications available"]
    write.csv(save_cases_to_plot, file.path(j.version.dir.logs, "diagnostic_case_notifs_and_shocks.csv"), row.names=FALSE)
    
    ### plot case model diagnostics
    source(file.path("FILEPATH/diagnostics.r", echo=TRUE))
  }
  
}


### Add the 2019 case diffs to original Step 4 draws_nonfatal and case_notifs
if (late_2019_ii) {
  # save original Step 4 draws in current j.version.dir for consistency
  combined_case_draws <- fread("FILEPATH/02_add_case_notifs_and_shocks.csv")
  fwrite(combined_case_draws, file.path(j.version.dir, "02_add_case_notifs_and_shocks_pre_correction.csv"), row.names = FALSE)
  
  #  read in the outbreak cases
  annualized_cases <- fread("/FILEPATH/2019_outbreak_locs_prepped.csv")
  setnames(annualized_cases, "annualized_report", "cases")
  replacements <- unique(annualized_cases$location_id)
  
  # subset out loc/year/draws of loc_ids in 'replacements' to add differences
  add_correction <- combined_case_draws[location_id %in% replacements & year_id==2019]
  combined_case_draws <- combined_case_draws[!(location_id %in% replacements & year_id==2019)]
  
  # take the opposite of the current 'estimated_minus_annualized'
  addition_factor <- annualized_cases[, .(location_id, year_id, estimated_minus_annualized)][, addition_factor := estimated_minus_annualized*(-1)]
  
  # add addition_factor to every corresponding draw
  add_correction <- merge(add_correction, addition_factor[, .(location_id, year_id, addition_factor)], by=c("location_id", "year_id"), all.x=T)
  draw_cols <- names(add_correction)[grepl("case_draw_[0-9]*", names(add_correction))]
  add_correction[, (draw_cols) := .SD + addition_factor, .SDcols = draw_cols][, addition_factor := NULL]
  
  # add back in the corrected draws to main 'combined_case_draws' df
  combined_case_draws <- rbind(combined_case_draws, add_correction)
  
  # save as original '02_xxx_xxx.csv' file name
  if (WRITE_FILES) fwrite(combined_case_draws, file.path(j.version.dir, "02_add_case_notifs_and_shocks.csv"), row.names=FALSE)
  
}

rm(draws_nonfatal)
gc(TRUE)
#***********************************************************************************************************************


########################################################################################################################
##### PART TWO: RESCALE CASES ##########################################################################################
########################################################################################################################


#----RESCALE------------------------------------------------------------------------------------------------------------
### run custom rescaling function
rescaled_cases <- rake(combined_case_draws)

rm(combined_case_draws)

if(trusted_case_distrib == "binom"){
  # aggregate subnats so that low case counts with NAs have draw-wise consistency between subnats and parent
  rescaled_cases <- merge(rescaled_cases, locations[,.(location_id, parent_id)])
  rescaled_cases[, parentyear := paste0(parent_id, year_id)]
  rescaled_cases[, locyear := paste0(location_id, year_id)]
  
  is_NA <- apply(rescaled_cases, 1, function(x) any(is.na(x))) #only subnats will have any NA draws made in raking
  parentyears_to_agg <- unique(rescaled_cases[is_NA ,.(parentyear)])
  # identify which locyears have nonzero natl draws (ie nonzero natl cases) but had NAs (ie have all 0s for subnat draws)
  subnat_sums[, parentyear := paste0(location_id, year_id)]
  parentyears_to_agg <- parentyears_to_agg[!(parentyear %in% subnat_sums[cases == 0, parentyear])]
  
  # reset rows with all 0 draws that became NA in raking to 0
  rescaled_cases[,(draw_cols) := lapply(.SD, function(x) ifelse(is.na(x), 0, x)), .SDcols=draw_cols] 
  
  # aggregate subnats to parent
  agg <-  rescaled_cases[parentyear %in% parentyears_to_agg$parentyear]
  agg <- agg[, (draw_cols) := sum(.SD), .SDcols = draw_cols, by = "parentyear"]
  keep_cols <- c(draw_cols, "parent_id", "year_id", "parentyear")
  agg <- unique(agg[, keep_cols, with = FALSE], by = c("parentyear"))
  setnames(agg, c("parent_id", "parentyear"), c("location_id", "locyear"))
  
  # replace national draws with aggregated draws
  rescaled_cases <- rescaled_cases[!(locyear %in% agg$locyear)]
  rescaled_cases[, parent_id := NULL]
  rescaled_cases[, parentyear := NULL]
  rescaled_cases <- rbind(rescaled_cases, agg)
  rescaled_cases[, locyear := NULL]
  
}

gc(T)

if(late_2019){
  ## account for outbreaks in subnational locations in countries where don't have case counts at the national level and for all subnationals
  # identify location ids that need to be replaced
  subnats_replacement_locs <- annualized_cases[!(location_id %in% trusted_notifications) & level > 3]
  # identify parent country that will also need to be adjusted
  subnats_replacement_locs <- merge(subnats_replacement_locs, locations[,.(location_id, parent_id)], by="location_id")
  # calculate mean case est for these locs and parents after raking, merge on to report and calc diff, add to parent loc and resimulate draws for parent 
  subnat_mean_est <- rescaled_cases[(location_id %in% subnats_replacement_locs$location_id) & year_id == 2020] %>% .[,.(subnat_mean_est = rowMeans(.SD, na.rm=T), location_id = location_id), .SDcols=paste0("case_draw_", 0:999)]
  parent_mean_est <- rescaled_cases[location_id %in% subnats_replacement_locs$parent_id & year_id == 2020, .(parent_mean_est = rowMeans(.SD, na.rm=T), parent_id = location_id), .SDcols=paste0("case_draw_", 0:999)]
  # compare report to est and adjust parent accordingly
  subnats_replacement_locs <- merge(subnats_replacement_locs, subnat_mean_est, by="location_id")
  subnats_replacement_locs <- merge(subnats_replacement_locs, parent_mean_est, by="parent_id")
  subnats_replacement_locs[, report_minus_est := cases - subnat_mean_est]
  subnats_replacement_locs[, corrected_parent_cases := parent_mean_est + report_minus_est]
  
  # simulate draws (as did with stgpr results of new parent value to use as replacement), no need to calc SE for R/SR because no locations with 0 cases/) SE in this outbreak set
  new_parent_cases <- data.table(location_id = subnats_replacement_locs$parent_id, year_id = 2020, cases = subnats_replacement_locs$corrected_parent_cases)
  new_parent_cases <- merge(new_parent_cases, pop_u5, by=c("location_id", "year_id"), all.x=TRUE)
  invisible(new_parent_cases[, inc_rate := cases / pop])
  new_parent_cases[, SE := sqrt( (inc_rate * (1 - inc_rate)) / pop )]
  new_parent_inc_draws <- rnorm(n=1000 * length(new_parent_cases$inc_rate), mean=new_parent_cases$inc_rate, sd=new_parent_cases$SE) %>% matrix(., ncol=1000, byrow=FALSE) %>% as.data.table
  colnames(new_parent_inc_draws) <- paste0("inc_draw_", draw_nums_gbd)
  new_parent_cases <- cbind(new_parent_cases, new_parent_inc_draws)
  
  ### calculate cases from incidence and population
  invisible(new_parent_cases[, (draw_cols) := lapply(draw_nums_gbd, function(x) get(paste0("inc_draw_", x)) * pop)])
  invisible(new_parent_cases <- new_parent_cases[, c("location_id", "year_id", draw_cols), with=FALSE])
  # already have draws for of the reported cases for these subnat locations in adjusted_notifications, just didn't get subbed in to complete cases before
  subnat_replacements <- adjusted_notifications[location_id %in% subnats_replacement_locs$location_id]
  parent_subnat_replacements <- rbind(new_parent_cases, subnat_replacements)
  
  # replace subnat/parent draws in main case draw dt
  rescaled_cases <- rescaled_cases[!(location_id %in% parent_subnat_replacements$location_id & year_id == 2020)]
  rescaled_cases <- rbind(rescaled_cases, parent_subnat_replacements)
  
}

### save results
if (WRITE_FILES == "yes") {
  if(cap_draws){
    fwrite(rescaled_cases, file.path(j.version.dir, "03_case_predictions_rescaled_uncapped.csv"), row.names=FALSE) # if capping draws, save this uncapped version now for posterity, official rescaled is saved below!
  } else {
    fwrite(rescaled_cases, file.path(j.version.dir, "03_case_predictions_rescaled.csv"), row.names=FALSE) # if not capping draws, save this as final rescaled file
  }
}
#***********************************************************************************************************************


if (CALCULATE_NONFATAL == "yes") {
  ########################################################################################################################
  ##### PART THREE: CASES AGE-SEX SPLIT ##################################################################################
  ########################################################################################################################
  
  
  #----SPLIT--------------------------------------------------------------------------------------------------------------
  ### split measles cases by age/sex pattern from CoD database
  split_cases <- age_sex_split(cause_id = cause_id, input_file=rescaled_cases, measure="case")
  
  ### save results
  if (WRITE_FILES == "yes") {
    fwrite(split_cases, file.path(j.version.dir, "04_case_predictions_split.csv"), row.names=FALSE)
  }
  #***********************************************************************************************************************
  
  
  ########################################################################################################################
  ##### PART FOUR: CASES TO PREVALENCE AND INCIDENCE #####################################################################
  ########################################################################################################################
  
  
  #----PREVALENCE AND INCIDENCE-------------------------------------------------------------------------------------------
  ### convert cases to prevalence, assuming duration of 10 days, and incidence
  if(cap_draws){
    # prep data
    predictions_inc <- copy(split_cases) %>% .[, measure_id := 6]
    rm(split_cases)
    gc(TRUE)
    
    # calculate incidence
    invisible( predictions_inc[, (draw_cols_gbd) := lapply( draw_nums_gbd, function(x) get(paste0("split_case_draw_", x)) / population)] )
    message("calculated age and sex specific incidence rate for saving")
    predictions_inc <- predictions_inc[, c(draw_cols_upload, "measure_id"), with=FALSE]
    message("subset age and sex specific incidence rate to relevant columns")
    
    # cap incidence
    predictions_inc[, (draw_cols_gbd) := lapply(.SD, function(x) ifelse(x > 0.9, 0.9, x)), .SDcols=draw_cols_gbd] 
    message("capped incidence")
    
    # calculate prevalence by multiplying incidence by 10/365 (assuming 10 day duration)
    predictions_prev <- copy(predictions_inc) %>% .[, measure_id := 5]
    invisible( predictions_prev[, (draw_cols_gbd) := lapply(draw_nums_gbd, function(x) (get(paste0("draw_", x)) * (10 / 365)))] )
    message("calculated age and sex specific prevalence from capped incidence")
    predictions_prev <- predictions_prev[, c(draw_cols_upload, "measure_id"), with=FALSE]
    message("subset age and sex spec prevalence to relevant columns")
    
    # to maintain consistency b/w NF and fatal results, recalculate all age both sex case draws from capped incidence
    rescaled_cases <- merge(predictions_inc, population, by=c("sex_id", "age_group_id", "location_id", "year_id"))
    message("merged on pop to backcalculate cases from capped inc")
    rescaled_cases[,(draw_cols) := lapply(draw_nums_gbd, function(x) (get(paste0("draw_", x)) * population))]
    message("backcalculated age and sex specific cases from capped inc")
    rescaled_cases <- rescaled_cases[, c("location_id", "year_id", draw_cols), with=FALSE]
    message("subset backcalculated cases to relevant columns")
    rescaled_cases <- rescaled_cases[, lapply(.SD, sum), by=c("year_id", "location_id"), .SDcols=draw_cols]
    message("summing backcalculated cases over age and sex to get all age both sex cases that are consistent with capped incidence")
    
    # overwrite rescaled cases from uncapped incidence to rescaled cases from capped incidence
    if (WRITE_FILES == "yes") {
      fwrite(rescaled_cases, file.path(j.version.dir, "03_case_predictions_rescaled.csv"), row.names=FALSE)
    }
    
  } else {
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
  }
  
  #***********************************************************************************************************************
  
  ########################################################################################################################
  ##### PART FIVE: FORMAT FOR COMO #######################################################################################
  ########################################################################################################################
  
  
  #----SAVE RESULTS-------------------------------------------------------------------------------------------------------
  ### format for como, (prevalence measure_id==5, incidence measure_id==6)
  predictions <- rbind(predictions_prev, predictions_inc)
  predictions <- predictions[age_group_id %in% c(age_start:age_end, 389, 238, 34), ]
  
  rm(predictions_prev)
  rm(predictions_inc)
  
  # Garbage collect
  gc(TRUE)
  
  
  ### save to /share directory
  system.time(lapply(unique(predictions$location_id), function(x) fwrite(predictions[location_id==x, ],
                                                                         file.path(cl.version.dir, paste0(x, ".csv")), row.names=FALSE)))
  print(paste0("nonfatal estimates saved in ", cl.version.dir))
  
  rm(predictions)
  gc(TRUE)
  
  ### save measles nonfatal draws to the database, modelable_entity_id=1436
  job_nf <- paste0("qsub -N s_epi_", acause, " -l m_mem_free=100G -l fthread=5 -l archive -l h_rt=8:00:00 -P ", cluster_proj, " -q QUEUE -o FILEPATH",
                   username, " -e FILEPATH", username,
                   " FILEPATH -i FILEPATH -s FILEPATH",
                   " --type epi",
                   " --me_id ", me_id,
                   " --input_directory ", cl.version.dir,
                   " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
                   " --best ", mark_model_best,
                   " --xw_id ", nf_xw_id,
                   " --bundle_id ", nf_bundle_id)
  if (decomp) job_nf <- paste0(job_nf, " --decomp_step ", decomp_step)
  system(job_nf)
  print(job_nf)
  #***********************************************************************************************************************
}


if (CALCULATE_COD == "yes") {
  
  if(covid_adjustment){
    #qsub to fatal adjustment script
    script <- paste0("FILEPATH/01b_fatal_covid_adjustment.R")
    args <- paste(nocovid_codcorrect_vers, nocovid_custom_version, custom_version, fatal_decomp_step, gbd_round, release_id)
    mem <- "-l m_mem_free=100G"
    fthread <- "-l fthread=5"
    runtime <- "-l h_rt=00:20:00"
    archive <- "-l archive=TRUE"
    jname <- paste0("-N ", custom_version, "01b_fatal_covid_adjustment")
    
    system(paste("qsub",jname,mem,fthread,runtime,archive,project,"-q QUEUE",sge.output.dir,rshell,
                 "-i FILEPATH","-s",script,args))
    message("qsubbed fatal covid adjustment job. Stopping 01_natural_history_model script -- CoD team handles fatal COVID adjustment from the scalars produced in 01b script.")
    stop()
  }
  
  ########################################################################################################################
  ##### PART Six: MODEL CFR ##############################################################################################
  ########################################################################################################################
  
  
  #----PREP---------------------------------------------------------------------------------------------------------------
  ### function: prep covariates (for use later)
  prep_covariates <- function(...) {
    
    if (decomp) {
      mal_cov <- get_covariate_estimates(covariate_id=1230, year_id=1980:year_end, location_id=pop_locs, gbd_round_id=gbd_round, decomp_step=fatal_decomp_step)
    } else {
      mal_cov <- get_covariate_estimates(covariate_id=1230, year_id=1980:year_end, location_id=pop_locs, gbd_round_id=gbd_round)
    }
    mal_cov[, ln_mal := log(mean_value)] # + 1e-6)]
    setnames(mal_cov, "mean_value", "mal")
    # save model version
    cat(paste0("Covariate malnutrition, covariate id 1230 (CoD) - model version ", unique(mal_cov$model_version_id)), file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
    # remove excess columns
    covariates <- mal_cov[, c("location_id", "year_id", "mal", "ln_mal"), with=FALSE]
    
    
    # LDI, covariate_name_short="LDI_pc"
    if (decomp) {
      ldi_cov <- get_covariate_estimates(covariate_id=57, year_id=1980:year_end, location_id=pop_locs, gbd_round_id=gbd_round, decomp_step=fatal_decomp_step)
    } else {
      ldi_cov <- get_covariate_estimates(covariate_id=57, year_id=1980:year_end, location_id=pop_locs, gbd_round_id=gbd_round)
    }
    ldi_cov[, ln_LDI := log(mean_value)]
    setnames(ldi_cov, "mean_value", "LDI")
    # save model version
    cat(paste0("Covariate LDI (CoD) - model version ", unique(ldi_cov$model_version_id)), file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
    # remove excess columns
    ldi_cov <- ldi_cov[, c("location_id", "year_id", "ln_LDI", "LDI"), with=FALSE]
    covariates <- merge(ldi_cov, covariates, by=c("location_id", "year_id"), all.x=TRUE)
    
    # healthcare access and quality index, covariate_name_short="haqi"
    if (decomp) {
      haqi <- get_covariate_estimates(covariate_id=1099, year_id=1980:year_end, location_id=pop_locs, gbd_round_id=gbd_round, decomp_step=fatal_decomp_step)
    } else {
      haqi <- get_covariate_estimates(covariate_id=1099, year_id=1980:year_end, location_id=pop_locs, gbd_round_id=gbd_round)
    }
    haqi[, HAQI := mean_value / 100] #setnames(haqi, "mean_value", "HAQI")
    haqi[, ln_HAQI := log(HAQI)]
    # save model version
    cat(paste0("Covariate HAQ (CoD) - model version ", unique(haqi$model_version_id)), file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
    # remove excess columns
    haqi <- haqi[, c("location_id", "year_id", "HAQI", "ln_HAQI"), with=FALSE]
    covariates <- merge(haqi, covariates, by=c("location_id", "year_id"), all.x=TRUE)
    
    # socio-demographic index, covariate_name_short="sdi"
    if (decomp) {
      if(manual_sdi){
        locations_2 <- get_location_metadata(gbd_round_id=6, location_set_id=22, 
                                           decomp_step="step4")[,.(location_id, ihme_loc_id, location_name,
                                                                       location_ascii_name, region_id, super_region_id,
                                                                       super_region_name, level, location_type,
                                                                       parent_id, sort_order)]
        pop_locs_2 <- unique(locations_2[level >= 3, location_id]) 
        sdi <- get_covariate_estimates(covariate_id=881, year_id=1980:2019, location_id=pop_locs_2, gbd_round_id=6, decomp_step="step4", model_version_id = 30492)
      } else {
        sdi <- get_covariate_estimates(covariate_id=881, year_id=1980:year_end, location_id=pop_locs, gbd_round_id=gbd_round, decomp_step=fatal_decomp_step)
      }
    } else {
      sdi <- get_covariate_estimates(covariate_id=881, year_id=1980:year_end, location_id=pop_locs, gbd_round_id=gbd_round)
    }
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
    if (decomp) {
      mortality_envelope <- get_envelope(location_id=pop_locs, year_id=1980:year_end, age_group_id=22, sex_id=1:2, gbd_round_id=gbd_round, decomp_step=fatal_decomp_step)
    } else {
      mortality_envelope <- get_envelope(location_id=pop_locs, year_id=1980:year_end, age_group_id=22, sex_id=1:2, gbd_round_id=gbd_round)
    }
    setnames(mortality_envelope, "mean", "mortality_envelope")
    
    ### get CoD data
    # download raw (uncorrected) COD data from the database
    if (add_vital_registration) {
      if (decomp) {
        cod <- get_cod_data(cause_id=cause_id, decomp_step=fatal_decomp_step)
      } else {
        cod <- get_cod_data(cause_id=cause_id)
      }
      setnames(cod, "year", "year_id")
      setnames(cod, "sex", "sex_id")
      # save model version
      cat(paste0("CoD data - version ", unique(cod$description)),
          file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
      cod <- cod[acause=="measles" & age_group_id %in% 22 & !is.na(cf_corr) & sample_size != 0, ]
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
    }
    
    ### get CFR literature data
    cfr_lit <- read_excel(file.path(home, "FILEPATH/measles_cfr_2020_updated_locs_and_study_cov_07.04.xlsx"), sheet="extraction") %>% as.data.table 
    cfr_lit[, hospital := hospital %>% as.integer]
    cfr_lit[, rural := rural %>% as.integer]
    cfr_lit[, outbreak := outbreak %>% as.integer]
    setnames(cfr_lit, c("cases", "sample_size"), c("deaths", "cases"))
    cfr_lit[is.na(midpointyear), midpointyear := floor((year_start + year_end) / 2)]
    cfr_lit[is.na(group_review), group_review := 0]
    cfr_lit[is.na(is_outlier), is_outlier := 0]
    cfr_lit[is.na(ignore), ignore := 0]
    cfr_lit <- cfr_lit[group_review != 1 & ignore != 1, .(nid, ihme_loc_id, year_start, year_end, midpointyear, age_start, age_end, 
                                                          cases, deaths, hospital, outbreak, rural, ignore, group_review, is_outlier, population_level)] %>% unique
    
    setnames(cfr_lit, "midpointyear", "year_id")
    cfr_lit[, source := "extracted_cfr"]
    
    ### bring together all CFR data
    if (add_vital_registration) all_cfr <- rbind(cfr_lit, WHO, fill=TRUE) else all_cfr <- copy(cfr_lit)
    all_cfr[ihme_loc_id=="HKG", "ihme_loc_id" := "CHN_354"]
    all_cfr <- merge(all_cfr, locations[, .(location_id, ihme_loc_id, super_region_id)], by="ihme_loc_id", all.x=TRUE)
    
    if(collapsed_cfr) {
      #sum together any age specific data by year_start, year_end, nid, location_id
      all_cfr[, total_deaths := sum(deaths), by=c("nid", "year_id", "location_id", "hospital", "outbreak", "rural")]
      all_cfr[, total_cases := sum(cases), by=c("nid", "year_id", "location_id", "hospital", "outbreak", "rural")]
      all_cfr[, collapsed_age_start := min(age_start), by=c("nid", "year_id", "location_id", "hospital", "outbreak", "rural")]
      all_cfr[, collapsed_age_end := max(age_end), by=c("nid", "year_id", "location_id", "hospital", "outbreak", "rural")]
      
      #fix column names
      all_cfr <- unique(all_cfr, by=c("nid", "year_id", "location_id", "hospital", "outbreak", "rural", "total_deaths", "total_cases", "collapsed_age_start", "collapsed_age_end"))
      all_cfr[, c("age_start", "age_end", "deaths", "cases") := NULL]
      setnames(all_cfr, c("total_deaths", "total_cases", "collapsed_age_start", "collapsed_age_end"), c("deaths", "cases", "age_start", "age_end"))
    }  
    
    
    ### drop outliers
    all_cfr[, cfr := deaths / cases]
    all_cfr[is.na(is_outlier), is_outlier := 0]
    all_cfr[(ihme_loc_id=="BEL" & year_start==2009) |
              (ihme_loc_id=="BEL" & year_start==2010) |
              (ihme_loc_id=="GNB" & year_start==1979 & year_end==1982 & age_start==0) |
              # drop data from hill tribes or ethnic minorities
              (ihme_loc_id=="THA" & year_start==1984) |
              (ihme_loc_id=="ETH" & year_start==1981) |
              (ihme_loc_id=="IND" & year_start==1991) |
              (ihme_loc_id=="IND" & year_start==1992 & cfr > 0.15) |
              (ihme_loc_id=="GNB" & year_start==1979) |
              (ihme_loc_id=="SEN" & year_start==1983) |
              (ihme_loc_id=="SEN" & year_start==1985),
            is_outlier := 1]
    # drop population level rows that are from untrusted CN or non DR locations
    if (pop_level_lit_cfr == FALSE){
      data_rich <- paste0(j, "FILEPATH/locations_four_plus_stars.csv") %>% fread
      sr_64_31_103 <- locations[super_region_id %in% c(64, 31, 103),ihme_loc_id]
      
      print(paste0("dropping population level data from any location without trusted notifs (outside of SR 64, 31, and 103) or without well-defined vital registration (non DR locs): ", 
                   paste(all_cfr[(population_level==1 ) & (!(ihme_loc_id %in% data_rich$ihme_loc_id) | !(ihme_loc_id %in% sr_64_31_103)) , ihme_loc_id] %>% unique, collapse=", ")))
      
      all_cfr <- all_cfr[((population_level==1 ) & (!(ihme_loc_id %in% data_rich$ihme_loc_id) | !(ihme_loc_id %in% sr_64_31_103))), is_outlier := 1]
    }
    
    all_cfr <- all_cfr[is_outlier != 1, ]
    
    ## if using 2019 locs, replace subnats with national level
    if(use_2019_cfr_locs){ 
      all_cfr[nid %in% c(141692, 334517, 334515, 131287, 334686, 415358, 415364, 141693, 141688) & ihme_loc_id %like% "NGA_", c("ihme_loc_id", "location_id") := list("NGA", 214)]
      all_cfr[nid %in% c(141695, 141696, 334513, 275568) & ihme_loc_id %like% "PAK_", c("ihme_loc_id", "location_id") := list("PAK", 165)]
      all_cfr[nid %in% c(141713, 141714, 131283) & ihme_loc_id %like% "ZAF_", c("ihme_loc_id", "location_id") := list("ZAF", 196)]
      
    }
    
    ### fix, merge on covariates
    all_cfr[is.na(hospital), hospital := 0]
    all_cfr[is.na(outbreak), outbreak := 0]
    all_cfr[is.na(rural), rural := 0]
    all_cfr <- unique(all_cfr[, .(nid, location_id, ihme_loc_id, year_id, age_start, age_end, cases, deaths, hospital, outbreak, rural, source, population_level)])
    all_cfr <- all_cfr[year_id >= 1980, ] # some years are missing in expert extractions??
    all_cfr <- merge(all_cfr, covariates, by=c("location_id", "year_id"), all.x=TRUE)
    all_cfr[, ihme_loc_id := as.factor(ihme_loc_id)]
    
    return(all_cfr)
    
  }
  
  ### prep CoD regression
  covariates <- prep_covariates()
  all_cfr <- prep_cod(add_vital_registration=include_VR_data_in_CFR)
  all_cfr <- all_cfr[location_id %in% standard_locations]
  
  
  #***********************************************************************************************************************
  
  
  #----MODEL CFR----------------------------------------------------------------------------------------------------------
  ### set theta for neg bin cfr model using output from GBD2015 
  theta <- exp(1 / -.3633593)  

  ### mixed effects neg binomial regression
  ### model specifications
  # covariates
  if (which_covariate=="only_HAQI") covar1 <- "HAQI"
  if (which_covariate=="only_SDI") covar1 <- "SDI"
  if (which_covariate=="only_LDI") covar1 <- "LDI"
  if (which_covariate=="only_ln_LDI") covar1 <- "ln_LDI"
  if (which_covariate=="use_HAQI") { covar1 <- "ln_mal"; covar2 <- "HAQI" }
  if (which_covariate=="use_LDI") { covar1 <- "HAQI"; covar2 <- "ln_LDI" }
  
  ## run cfr model
  if(cfr_model_type=="negbin"){
    # random effects on location
    res <- as.formula(~ 1 | ihme_loc_id)
    
    # formula
    if (exists("covar2")) formula <- paste0("deaths ~ ", covar1, " + ", covar2, " + hospital + outbreak + rural + offset(log(cases))") else
      formula <- paste0("deaths ~ ", covar1, " + hospital + outbreak + rural + offset(log(cases))")
    
    # weights and model
    if (cfr_weights== "cases") {
      all_cfr[, weight := cases]
      ### save regression input
      fwrite(all_cfr, file.path(j.version.dir.inputs, "cfr_regression_input.csv"), row.names=FALSE)
      
      
      cfr_model <- MASS::glmmPQL(as.formula(formula),
                                 random=res,
                                 family=negative.binomial(theta=theta, link=log),
                                 data=all_cfr,
                                 weights=weight)
    } else if (cfr_weights=="median"){
      # For population level rows going in to model, weight is median cases in non-population level rows.
      all_cfr[population_level==1, weight := median(all_cfr[population_level!=1, cases])]
      # Non-population level rows have weight equal to cases
      all_cfr[is.na(weight), weight := cases]
      
      ### save regression input
      fwrite(all_cfr, file.path(j.version.dir.inputs, "cfr_regression_input.csv"), row.names=FALSE)
      
      cfr_model <- MASS::glmmPQL(as.formula(formula),
                                 random=res,
                                 family=negative.binomial(theta=theta, link=log),
                                 data=all_cfr,
                                 weights=weight)
    } else if (cfr_weights == "none") {
      ### save regression input
      fwrite(all_cfr, file.path(j.version.dir.inputs, "cfr_regression_input.csv"), row.names=FALSE)
      
      #no weights
      cfr_model <- MASS::glmmPQL(as.formula(formula),
                                 random=res,
                                 family=negative.binomial(theta=theta, link=log),
                                 data=all_cfr)
    }
    
    ## calculate errors
    all_cfr[, predicted_cfr := predict(cfr_model, type="response")]
    all_cfr[, observed_cfr := deaths/cases]
    rmse <- weighted.rmse(actual = all_cfr$observed_cfr, predicted = all_cfr$predicted_cfr, weight = all_cfr$cases)
    mae <- weighted.mae(actual = all_cfr$observed_cfr, predicted = all_cfr$predicted_cfr, weight= all_cfr$cases)
    fwrite(data.table(wmae = mae, wrmse = rmse), file= file.path(j.version.dir.logs, "cfr_model_error.csv"))
    
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
    # keep only intercept and fixed effect coefficient
    coefmat <- c(fixef(cfr_model)) 
    names(coefmat)[1] <- "constant"
    names(coefmat) <- paste("b", names(coefmat), sep = "_")
    coefmat <- matrix(unlist(coefmat), ncol=length(coefmat), byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coefmat)))))
    if (!exists("covar2")) length <- 2 else if (exists("covar2")) length <- 3
    coefmat <- coefmat[1, 1:length]
    
    # covariance matrix
    vcovmat <- vcov(cfr_model) 
    vcovmat <- vcovmat[1:length, 1:length]
    
    # create 1000 draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
    betadraws <- rmvnorm(n=1000, mean=coefmat, sigma=vcovmat)
    betas <- t(betadraws)
    
    
    ### estimate deaths at the draw level
    cfr_draw_cols <- paste0("cfr_draw_", draw_nums_gbd)
    # generate draws of the prediction using coefficient draws
    if (exists("covar2")) { pred_CFR[, (cfr_draw_cols) := lapply(draw_nums_gbd, function(i) exp( betas[1, (i + 1)] +
                                                                                                   ( betas[2, (i + 1)] * get(covar1) ) +
                                                                                                   ( betas[3, (i + 1)] * get(covar2) ) +
                                                                                                   RE_loc ) )] }
    if (!exists("covar2")) { pred_CFR[, (cfr_draw_cols) := lapply(draw_nums_gbd, function(i) exp( betas[1, (i + 1)] +
                                                                                                    ( betas[2, (i + 1)] * get(covar1) ) +
                                                                                                    RE_loc ) )] }
    
    ### save results
    CFR_draws_save <- pred_CFR[, c("location_id", "year_id", cfr_draw_cols), with=FALSE]
    if (WRITE_FILES == "yes") {
      fwrite(CFR_draws_save, file.path(j.version.dir, paste0("05_cfr_model_draws.csv")), row.names=FALSE)
    }
    
    
  } else if (cfr_model_type=="mrbrt"){
    
    all_cfr[, cfr:=deaths/cases]
    #offset any 0 cfr so not dropped from logit space model
    all_cfr[cfr==0, cfr := .00001]
    #calc se
    z <- qnorm(0.975)
    all_cfr[, cfr_se := sqrt((deaths/cases)*(1-(deaths/cases))/cases + z^2/(4*cases^2))]
    all_cfr[, ldiff := logit(cfr)] 
    #must be data frame for delta method to work!
    all_cfr <- as.data.frame(all_cfr)
    all_cfr$ldiff_se <- sapply(1:nrow(all_cfr), function(i) {
      mean_i <- all_cfr[i, "cfr"]
      se_i <- all_cfr[i, "cfr_se"]
      deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
    })
    
    ### save regression input
    fwrite(all_cfr, file.path(j.version.dir.inputs, "cfr_regression_input.csv"), row.names=FALSE)

    #prep input for mrbrt
    dt2 <- all_cfr[,c("ldiff", "ldiff_se","location_id", "rural", "outbreak", "hospital", "SDI", "nid", "cases", "deaths")]
    message(paste("There are", nrow(dt2[is.infinite(dt2$ldiff) | is.nan(dt2$ldiff),]), "rows with nan or inf logit(cfr). Dropping these rows from cfr input. To avoid this, make sure all the zero cfrs in input data are being offset."))
    dt2 <- dt2[!is.infinite(dt2$ldiff) & !is.nan(dt2$ldiff),]
    dt2$rural <- as.integer(dt2$rural)
    dt2$outbreak <- as.integer(dt2$outbreak)
    dt2$hospital <- as.integer(dt2$hospital)
    dt2$nid <- as.character(dt2$nid)
    dt2$location_id <- as.factor(dt2$location_id)
    message(paste("There are", sum(!complete.cases(dt2)), "not complete cases in mrbrt input data. These rows will be dropped. If > 0, STOP and make sure you aren't missing covariate values for any row."))
    dt2 <- dt2[complete.cases(dt2),]
    
    ## prep mrdata object of input data
    dat1 <- MRData()
    dat1$load_df(data = dt2,  col_obs = "ldiff", col_obs_se = "ldiff_se", col_study_id = "location_id",
                 col_covs = list("rural", "outbreak", "hospital", "SDI"))
    
    ## RUN MRBRT
    mod1 <- MRBRT(data = dat1,
                  inlier_pct=inlier_pct,
                  cov_models = list(
                    LinearCovModel("intercept", use_re = TRUE), #random effect on location_id
                    LinearCovModel("rural"),
                    LinearCovModel("outbreak"),
                    LinearCovModel("hospital"),
                    LinearCovModel("SDI"))
    )
    
    
  
    mod1$fit_model(inner_print_level = 5L, inner_max_iter = 100L)
    
    # pull out and save re by location_id and betas
    re <- mod1$re_soln
    df_re <- data.frame(location_id=as.integer(names(mod1$re_soln)), re=unlist(re))
    fwrite(df_re, file=file.path(j.version.dir.logs, "mrbrt_res_by_loc.csv"))
    betas <- data.frame(covariate= mod1$cov_names, betas = mod1$beta_soln)
    fwrite(betas, file=file.path(j.version.dir.logs, "mrbrt_betas.csv"))
    
    ## save model object
    py_save_object(object = mod1, filename = file.path(j.version.dir.logs, "fit1.pkl"), pickle = "dill")
    
   
    #get beta and gamma samples
    n_samples <- 1000L
    samples1 <- mod1$sample_soln(sample_size = n_samples)
    
    
    ##calculate error
    dt2$predicted <- mod1$predict(data=dat1, predict_for_study = TRUE, sort_by_data_id=TRUE)
    rmse_mrbrt <- weighted.rmse(actual = invlogit(dt2$ldiff), predicted = invlogit(dt2$predicted), weight = dt2$cases)
    mae_mrbrt <- weighted.mae(actual = invlogit(dt2$ldiff), predicted = invlogit(dt2$predicted), weight= dt2$cases)
    fwrite(data.table(wmae_mrbrt = mae_mrbrt, wrmse_mrbrt = rmse_mrbrt), file= file.path(j.version.dir.logs, "cfr_model_error.csv"))
    
    ## make and save diagnostic plots of fit
    df_sim1_tmp <- dat1$to_df()
    df_sim1_tmp$not_trimmed <- as.factor(mod1$w_soln) # 0 if trimmed, 1 if not
    # test predicting out on RE vs not
    df_sim1_tmp$pred_re <- mod1$predict(data = dat1, predict_for_study = TRUE)
    df_sim1_tmp$pred_no_re <- mod1$predict(data = dat1, predict_for_study = FALSE)
    
    #add ihme_loc_id and SR info and save
    setnames(df_sim1_tmp, "study_id", "location_id")
    df_sim1_tmp <- merge(df_sim1_tmp, locations[,c("location_id", "ihme_loc_id", "super_region_name")], by="location_id")
    df_sim1_tmp <- as.data.table(df_sim1_tmp)
    fwrite(df_sim1_tmp, file=file.path(j.version.dir.logs, "mrbrt_input_trimmed.csv"))
    
    #plot observed vs predicted with color for trimming
    pdf(file = file.path(j.version.dir.logs, "measlescfr_mrbrt_diagnostic_plots.pdf"), 
        height =6,
        width = 15, 
        onefile = TRUE)
    
    print(ggplot(data=df_sim1_tmp) + geom_point(aes(x=invlogit(obs), y=invlogit(pred_re), color=super_region_name), shape=df_sim1_tmp$not_trimmed) + labs(y="predicted cfr", x="observed cfr") +
      ggtitle(label=paste0("Measles CFR MR-BRT observed vs predicted \n inlier_pct = ", inlier_pct)) + coord_equal() + geom_abline(slope=1, intercept=0, color="black"))
    
    #residual vs fitted plot
    df_sim1_tmp$residual <- df_sim1_tmp$pred_re-df_sim1_tmp$obs
    print(plot(df_sim1_tmp$residual ~ df_sim1_tmp$pred_re, xlab="fitted", ylab="residual", main=paste0("Residual vs fitted; inlier_pct = ", inlier_pct)) + abline(0,0))
    
    dev.off()
    
    ## PREDICT OUT
    length <- nrow(covariates)
    # assume covariates are 0 when predicting out
    prediction_frame <- data.frame(rural = rep(0, length), outbreak = rep(0, length), hospital = rep(0,length), 
                                   SDI = covariates$SDI, location_id = as.character(covariates$location_id))
    ## make mrdata of prediction data
    data_pred <- MRData()
    data_pred$load_df(data=prediction_frame,
                      col_covs=list("hospital", "rural", "outbreak", "SDI"), col_study_id = "location_id")
   
    ## get point predictions using fixed and random effect, convert out of logit space
    prediction_frame_temp<- data_pred$to_df()
    prediction_frame_temp$pred_incl_re <- mod1$predict(data=data_pred, predict_for_study = TRUE)
    prediction_frame_temp$pred_no_re <- mod1$predict(data=data_pred, predict_for_study = FALSE)
    prediction_frame_temp$linear_pred_re <- invlogit(prediction_frame_temp$pred_incl_re)
    prediction_frame_temp$linear_pred_no_re <- invlogit(prediction_frame_temp$pred_no_re)
    
    ## GET DRAWS USING FIXED AND RANDOM EFFECT with NO sharing of re over space!
    # these predictions automatically predict out on the RE because knows from samples that model included RE
    draws2 <- mod1$create_draws(
      data=data_pred,
      beta_samples = samples1[[1]],
      gamma_samples = samples1[[2]],
      random_study = FALSE # FALSE means don't include between location heterogeneity in uncertainty
    )
    
    
   linear_draws <- as.data.table(invlogit(draws2))
   cfr_draw_cols <- paste0("cfr_draw_", draw_nums_gbd)
   setnames(linear_draws, c(1:1000), cfr_draw_cols)
   
   
   final_draws_w_re <- copy(as.data.table(prediction_frame_temp))
   
   #get the lower and upper CI from draws
   final_draws_w_re$pred_lo <- apply(linear_draws, 1, function(x) quantile(x, 0.025))
   final_draws_w_re$pred_hi <- apply(linear_draws, 1, function(x) quantile(x, 0.975))
   final_draws_w_re$pred_mean <- rowMeans(linear_draws)
   final_draws_w_re$pred_median <- apply(linear_draws, 1, function(x) quantile(x, 0.5))
   
   setnames(final_draws_w_re, "study_id", "location_id")
   final_draws_w_re[, location_id := as.integer(location_id)]
   #now merge sdi from central functions so we can match estimates to the location year (and age, sex if covar is age, sex specific)!
   final_draws_w_re <- merge(covariates[, c("location_id", "SDI", "year_id")], final_draws_w_re, by=c("location_id", "SDI"))
   
   #save summary of draws
   fwrite(final_draws_w_re, file=paste0(j.version.dir.logs, "mrbrt_preds_summarized.csv"))
   ## merge on draws
   final_draws_w_re <- cbind(final_draws_w_re, linear_draws)
   
   ### save draws
   CFR_draws_save <- final_draws_w_re[, c("location_id", "year_id", cfr_draw_cols), with=FALSE]
   if (WRITE_FILES == "yes") {
     fwrite(CFR_draws_save, file.path(j.version.dir, paste0("05_cfr_model_draws.csv")), row.names=FALSE)
   }
  }
  
  ### pull in exactly what was used in original GBD 2019 model or pre-2020 shock data GBD 2020 model 
  if (gbd_round ==6 & (late_2019_ii | late_2019)) {
    CFR_draws_save <- fread("FILEPATH/05_cfr_model_draws.csv")
    # save original Step 4 into current j.version.dir for consistency/reproducibility
    fwrite(CFR_draws_save, file.path(j.version.dir, paste0("05_cfr_model_draws.csv")), row.names=FALSE)
  } else if (gbd_round == 7 & (late_2019_ii | late_2019)) {
    CFR_draws_save <- fread("/FILEPATH/05_cfr_model_draws.csv")
    # save last best into current j.version.dir for consistency/reproducibility
    fwrite(CFR_draws_save, file.path(j.version.dir, paste0("05_cfr_model_draws.csv")), row.names=FALSE)
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
    fwrite(predictions_deaths_save, file.path(j.version.dir, "06_death_predictions_from_model.csv"), row.names=FALSE)
  }
  #***********************************************************************************************************************
  
  
  ########################################################################################################################
  ##### PART EIGHT: RESCALE DEATHS #######################################################################################
  ########################################################################################################################
  
  
  #----RESCALE------------------------------------------------------------------------------------------------------------
  ### run custom rescale function
  invisible( lapply(death_draw_cols , function(x) predictions_deaths_save[location_id %in% locations[level > 3, location_id] & get(x)==0, (x) := 1e-10]) )
  rescaled_deaths <- rake(predictions_deaths_save, measure="death_draw_")
  
  ### save results
  if (WRITE_FILES == "yes") {
    fwrite(rescaled_deaths, file.path(j.version.dir, "07_death_predictions_rescaled.csv"), row.names=FALSE)
  }
  #***********************************************************************************************************************
  
  
  ########################################################################################################################
  ##### PART NINE: SPLIT DEATHS ##########################################################################################
  ########################################################################################################################
  
  
  #----SPLIT--------------------------------------------------------------------------------------------------------------
  ### split using age pattern from cause of death data
  split_deaths <- age_sex_split(cause_id = cause_id, input_file=rescaled_deaths, measure="death")
  #***********************************************************************************************************************
  
  
  #----SAVE---------------------------------------------------------------------------------------------------------------
  ### save split draws
  split_deaths_save <- split_deaths[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("split_death_draw_", draw_nums_gbd)), with=FALSE]
  rm(split_deaths)
  gc(T)
  
  
  if (WRITE_FILES == "yes") {
    fwrite(split_deaths_save, file.path(j.version.dir, "08_death_predictions_split.csv"), row.names=FALSE)
  }
  #***********************************************************************************************************************
  
  ########################################################################################################################
  ##### PART TEN: COMBINE CODEm DEATHS ##################################################################################
  ########################################################################################################################
  
  
  #----COMBINE------------------------------------------------------------------------------------------------------------
  ### read CODEm COD results for data-rich countries
  cod_M <- data.table(pandas$read_hdf(file.path("/ihme/codem/data", acause, male_CODEm_version, paste0("draws/deaths_", "male", ".h5")), key="data"))
  cod_F <- data.table(pandas$read_hdf(file.path("/ihme/codem/data", acause, female_CODEm_version, paste0("draws/deaths_", "female", ".h5")), key="data"))
  
  # save model version
  cat(paste0("Data-rich CODEm feeder model (males) - model version ", male_CODEm_version),
      file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  cat(paste0("Data-rich CODEm feeder model (females) - model version ", female_CODEm_version),
      file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  
  # combine M/F CODEm results
  cod_DR <- rbind(cod_M, cod_F, fill=TRUE)
  rm(cod_M)
  rm(cod_F)
  gc(T)
  
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
  split_deaths_hyb <- split_deaths_hyb[age_group_id %in% c(age_start:age_end, 389, 238, 34), ]
  #***********************************************************************************************************************
  
  
  ########################################################################################################################
  ##### PART TEN POINT FIVE (NEW Step 4 GBD 2019): FORCE DEATHS TO 0 WHERE TRUSTED CASE NOTIFS 0 #########################
  ########################################################################################################################
  
  
  #----FORCE CODEM OR CALCULATED DEATHS TO 0 FOR TRUSTED LOCS WITH 0 CASES PREDICTED------------------------------------------------------------------------------
  # Read in the smothed, trusted adjusted case notifications (close to 0 rounded down to 0)
  st_gpr_runs <- grep("case_notifications", list.dirs("/share/epi/vpds/measles/nonfatal"), value = TRUE)
  if(st_gpr_version == "latest"){
    st_gpr_paths <- file.info(st_gpr_runs)
    st_gpr_path <- rownames(st_gpr_paths)[which.max(st_gpr_paths$mtime)]
  } else {
    st_gpr_path <- grep(st_gpr_version, st_gpr_runs, value=T)
  }
  adjusted_notifications_q <- lapply(list.files(st_gpr_path, full.names = T), fread) %>% 
    rbindlist %>% .[, .(location_id, year_id, combined)] %>% setnames(., "combined", "cases")
  
  ### if late_2019, need to swap in actual 2019 annualized reported cases
  ### for NZL, ARG, COL, VEN, BRA, USA (WSM -- group 3 -- comes later)
  if (late_2019) {
    if(gbd_round==6){
      annualized_cases <- fread("FILEPATH/2019_outbreak_locs_prepped.csv")
      setnames(annualized_cases, "annualized_report", "cases")
    } else if (gbd_round==7){
      annualized_cases <- fread("/FILEPATH/2020_outbreak_locs_prepped.csv")
      setnames(annualized_cases, "corrected_cases", "cases")
    }
    
    replacements <- unique(annualized_cases$location_id)
    removed_to_replace <- adjusted_notifications_q[!(location_id %in% replacements & year_id==2020), ]
    adjusted_notifications_q <- rbind(removed_to_replace, annualized_cases[, .(location_id, year_id, cases)])
  }
  #######################################################################
  
  
  # subset down to trusted case notification super region locations
  trust_locs <- adjusted_notifications_q[location_id %in% unique(locations[super_region_id %in% c(31, 64, 103), location_id])]
  ## if there are GBD subnationals in these regions not with direct case notifs, append them on and assign parent cases number...
  # merge on parent_id and level....
  trust_locs <- merge(trust_locs, locations[, c("location_id", "ihme_loc_id", "parent_id", "level")], all.x=TRUE)

  # Subset to just location-years where cases are 0 -- these are locations where forcing deaths to 0
  zero_cases <- trust_locs[cases==0]
  
  # Use this list to force deaths to 0 in these location-years for all age groups, both sexes
  # Implement here on this working data set because combined model & CODEm so captures trusted loc if it is DR or not and has 0 cases
  ## merge on zero cases (gets applied to all ages/sexes within location-year)
  split_deaths_hyb <- merge(split_deaths_hyb, zero_cases, by=c("location_id", "year_id"), all.x = TRUE)
  ## cases either 0 or NA, so
  cols <- grep("draw_", names(split_deaths_hyb), value=TRUE)
  split_deaths_hyb <- split_deaths_hyb[cases==0, (cols) := 0]
  split_deaths_hyb <- split_deaths_hyb[, cases := NULL]
  
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
  job <- paste0("qsub -N s_cod_", acause, " -l m_mem_free=100G -l fthread=5 -l archive -l h_rt=8:00:00 -P proj_cov_vpd -q QUEUE -o FILEPATH",
                username, " -e FILEPATH", username,
                " FILEPATH -i FILEPATH -s FILEPATH", 
                " --type cod",
                " --me_id ", cause_id,
                " --input_directory ", cl.death.dir,
                " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
                " --best ", mark_model_best)
  if (decomp) job <- paste0(job, " --decomp_step ", fatal_decomp_step)
  system(job)
  print(job)
  #***********************************************************************************************************************
}

