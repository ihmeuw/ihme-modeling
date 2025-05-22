#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:  REDACTED
# Date:    Februrary 2017, updated 2023 by REDACTED
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
pacman::p_load(magrittr, stats, dplyr, plyr, lme4, reshape2, parallel, rhdf5, msm, ggplot2) 
library(optimx, lib.loc = '/FILEPATH')

#load mrbrt for mrbrt cfr model
if(cfr_model_type=="mrbrt"){
  mrbrt_helper_dir <- "/FILEPATH/"
  library(mrbrt001, lib.loc = mrbrt_helper_dir)
}

source("/FILEPATH.R")
load_packages(c("data.table", "mvtnorm"))

# Get STGPR functions, NB this must be done here to set python environement
source("/FILEPATH/stgpr/api/public.R")

# Get pandas to read in HDF files
pandas <- reticulate::import("pandas")

### set data.table threads
setDTthreads(1)
#***********************************************************************************************************************


#----DIRECTORIES--------------------------------------------------------------------------------------------------------
### set objects
acause    <- "measles"
age_start <- 6  #2           
age_end   <- 17              
a         <- 3  ## birth cohort years before 1980
cause_id  <- 341
me_id <- 1436

release_id <- 16
gbd_round <- 9
year_end  <- 2024 # Last year of prediction for this cycle
year_end_data <- 2023 # Last year of JRF data, including monthly reports

### draw numbers
draw_nums_gbd    <- 0:999 
draw_cols_gbd    <- paste0("draw_", draw_nums_gbd)
draw_cols_upload <- c("location_id", "year_id", "age_group_id", "sex_id", draw_cols_gbd)

### make folders on cluster
# mortality
cl.death.dir <- file.path("/FILEPATH", acause, "mortality", custom_version, "draws")
if (!dir.exists(cl.death.dir) & CALCULATE_COD=="yes") dir.create(cl.death.dir, recursive=TRUE)
# nonfatal
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
if (CALCULATE_NONFATAL=="yes" & CALCULATE_COD=="no") add_  <- "NF"
if (CALCULATE_COD=="yes" & CALCULATE_NONFATAL=="no") add_  <- "CoD"
if (CALCULATE_NONFATAL=="yes" & CALCULATE_COD=="yes") add_ <- "NF and CoD"
# record CODEm data-rich feeder model versions used in CoD hybridization
if (CALCULATE_COD=="yes") description <- paste0("DR M ", male_CODEm_version, ", DR F ", female_CODEm_version, ", ", description)
# save full description to model folder
description <- paste0(add_, " - ", description)
cat(description, file=file.path(FILEPATH, "MODEL_DESCRIPTION.txt"), append = T)
#***********************************************************************************************************************

#----FUNCTIONS----------------------------------------------------------------------------------------------------------
### load shared functions
source("/FILEPATH/get_population.R")
source("/FILEPATH/get_envelope.R")
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/get_covariate_estimates.R")
source("/FILEPATH/get_cod_data.R")
source("/FILEPATH/get_bundle_data.R")
source("/FILEPATH/get_bundle_version.R")

### load personal functions
"/FILEPATH/read_hdf5_table-copy.R" %>% source 
"/FILEPATH/read_excel.R" %>% source
"/FILEPATH/rake.R" %>% source
"/FILEPATH/collapse_point.R" %>% source
"/FILEPATH/sql_query.R" %>% source
source(file.path("/FILEPATH/sex_age_weight_split.R"))
source(file.path("/FILEPATH/calc_oos_error.R"))

# measures of error
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

#----PREP---------------------------------------------------------------------------------------------------------------
### get location data
locations <- get_location_metadata(release_id = release_id, location_set_id=22)[,.(location_id, ihme_loc_id, location_name,
                                                                                   location_ascii_name, region_id, region_name, super_region_id,
                                                                                   super_region_name, level, location_type,
                                                                                   parent_id, sort_order)]
pop_locs <- unique(locations[level >= 3, location_id])

#This set of locations has select subnational locations which are used by modeling tools to minimize impact on regression coefficients
standard_locations <- get_location_metadata(location_set_id=101, 
                                            release_id=release_id)$location_id %>% unique

# get population for birth cohort at average age of notification by year
population <- get_population(location_id=pop_locs, 
                             year_id=(1980-a):year_end, 
                             age_group_id=c(2:3, 388, 389, 238, 34, 6:20, 30:32, 235), 
                             status="best", 
                             sex_id=1:3, 
                             release_id = release_id)

# Subset to under 1 population and create age cohort effect
population_young <- population[age_group_id %in% c(2, 3, 388, 389) & sex_id==3, ] # under 1 population
population_young <- population_young[, year_id := year_id + a] %>% .[year_id <= year_end, ]

# collapse by location-year
sum_pop_young <- population_young[, pop := sum(population), by=c("location_id", "year_id")] %>% .[, .(location_id, year_id, pop)] %>% unique

# create an under 5 population, to be used in model estimate correction
pop_u5 <- population[year_id %in% c(1980:year_end) & age_group_id %in% c(2, 3, 388, 389, 238, 34) & sex_id==3, ]
pop_u5 <- pop_u5[, pop := sum(population), by=c("location_id", "year_id")] %>% .[, .(location_id, year_id, pop)] %>% unique

# save model version
cat(paste0("Population - model run ", unique(population$run_id)),
    file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

########################################################################################################################
##### PART ONE: INCIDENCE NAT HIST MODEL ###############################################################################
########################################################################################################################

### Import supplementary immunization activity data
SIA <- get_bundle_version(bundle_version_id = sia_bundle_version)
setnames(SIA, "val", "supp")

# apply SIAs to subnational units: make df of unique subnational locations
loc_subs <- locations[level > 3, c("location_id", "parent_id")]

# subset SIAs to only admin0 locations with subnational units
SIA_sub <- SIA[SIA$location_id %in% unique(loc_subs$parent_id), ]
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

# add subnational SIAs to national SIA dataframe
SIA <- bind_rows(SIA, SIA_subs)
SIA <- SIA[!duplicated(SIA[, c("location_id", "year_id")]), ]

# add lag to SIA, 1 to sia_lag (as set in 00_launch) years
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

# Fix NAs in SIA data
for (i in 1:sia_lag) {
  SIAs[is.na(get(paste0("supp_", i))), paste0("supp_", i) := 0]
  SIAs[get(paste0("supp_", i))==0, paste0("supp_", i) := 0.000000001]
}

### Get case notifications
# ^NB! JRF UPDATED JULY 15 ANNUALLY, WITH EVERY GBD ROUND DOWNLOAD NEW JRF FROM:
# https://immunizationdata.who.int/pages/incidence/measles.html?GROUP=Countries&YEAR=

# Read in case notifications
case_notif <- fread("/FILEPATH/measles_jrf_2023.csv")
setDT(case_notif)
case_notif <- case_notif[, .(SpatialDimValueCode, Period, FactValueNumeric)]
setnames(case_notif, c("SpatialDimValueCode", "Period", "FactValueNumeric"), c("ihme_loc_id", "year_id", "cases"))

#merge with locations
case_notif <- merge(case_notif, locations[, .(ihme_loc_id, location_id)], by="ihme_loc_id", all.x=TRUE)

# Assign nid value
case_notif[, nid := 530127]

# Read in subnational notification data
BRA       <- fread("/FILEPATH/BRA_subnational_notifications_gbd2022.csv")
JPN       <- fread("/FILEPATH/JPN_subnational_notifications_gbd2022.csv")
GBR       <- fread("/FILEPATH/GBR_subnational_notifications_gbd2022.csv")
IDN       <- fread("/FILEPATH/IDN_subnational_notifications_gbd2022.csv")
ITA       <- fread("/FILEPATH/ITA_subnational_notifications_gbd2022.csv")
POL       <- fread("/FILEPATH/POL_subnational_notifications_gbd2022.csv")
ZAF       <- fread("/FILEPATH/ZAF_subnational_notifications_gbd2022.csv")
USA       <- fread("/FILEPATH/US_subnational_notifications_gbd2022.csv")

new_subnat <- rbind(BRA, JPN, GBR, IDN, ITA, POL, ZAF, USA, fill = T) %>% data.table()
new_subnat <- new_subnat[, .(nid, ihme_loc_id, year_id, cases)]

# Merge subnationals with locations to fill in missing location_ids
new_subnat <- merge(new_subnat, locations[, .(ihme_loc_id, location_id)], by = "ihme_loc_id", all.x = T)

# Remove data with no cases
new_subnat <- new_subnat[!is.na(cases), ]

# Bind national case notifications with subnational notifications
case_notif <- bind_rows(case_notif, new_subnat)

if(include_monthly_cases == T){
# https://www.who.int/teams/immunization-vaccines-and-biologicals/immunization-analy[…]illance/monitoring/provisional-monthly-measles-and-rubella-data

# Load monthly case notification from smoothing bundle
monthly_cases <- get_bundle_data(bundle_id = cn_smoothing_bundle_id)

# Keep only monthly data by subseting only to year id beyond last year of case notifications
monthly_cases <- monthly_cases[year_id == max(case_notif$year_id) + 1,
                               .(nid, year_id, ihme_loc_id, location_id, cases)]

monthly_locations <-locations$location_name[locations$location_id %in% unique(monthly_cases$location_id)]
cat(monthly_locations, file = paste0(FILEPATH.logs, "/monthly_case_notification_locations.txt"))

# Join case notifs to monthly aggregates for last year in case notifs
case_notif <- rbind(case_notif, monthly_cases)

}

# Make corrections for outbreak data, numbers different from JRF in trusted locations

if(include_outbreaks){

  case_notif[year_id == 2022 & location_id == locations$location_id[locations$location_name == "Brazil"], cases := 127]
  case_notif[year_id == 2023 & location_id == locations$location_id[locations$location_name == "Azerbaijan"], cases := 13728]
  case_notif[year_id == 2023 & location_id == locations$location_id[locations$location_name == "Kazakhstan"], cases := 15111]

}

# drop un-mapped locations
drop.locs <- case_notif[!(location_id %in% locations$location_id)]$ihme_loc_id %>% unique
if (length(drop.locs) > 0 ) {
  print(paste0("unmapped locations in WHO dataset (dropping all): ", toString(drop.locs)))
  case_notif <- case_notif[!(ihme_loc_id %in% drop.locs)]
}

# remove rows missing case notifications
case_notif <- case_notif[!is.na(cases), .(nid, location_id, year_id, cases)]

# save file
fwrite(case_notif, file.path(FILEPATH.inputs, "case_notifications_with_subnational_data.csv"), row.names=FALSE)

### prepare immunization and masking covariates
prep_covs <- function(...) {
  
  if(covid_inclusive_vax == F){ 
  
  ### get updated measles vaccine coverate covariates
    
  if(is.null(custom_mcv1_coverage)){
    
    if (use_lagged_covs) {
      mcv1 <- get_covariate_estimates(covariate_id=2309, release_id = release_id)
    }  else {
        mcv1 <- get_covariate_estimates(covariate_id=75, release_id = release_id)
      }
    
  } else {
    
    if (use_lagged_covs) {
      mcv1 <- get_covariate_estimates(covariate_id=2309, model_version_id = custom_mcv1_coverage, release_id = release_id)
    } else {
        mcv1 <- get_covariate_estimates(covariate_id=75, release_id = release_id,  model_version_id = custom_mcv1_coverage)
      }
    }
    
  # Calculate log 1- vaccinated
  mcv1[, ln_unvacc := log(1 - mean_value)]
  
  # Rename variables
  setnames(mcv1, "mean_value", "mean_mcv1")
  
  # save model version
  cat(paste0("Covariate MCV1 (NF) - model version ", unique(mcv1$model_version_id)), file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  
  #remove excess columns
  mcv1 <- mcv1[, .(location_id, year_id, ln_unvacc, mean_mcv1)]
  
  ### Get MCV2 covariate:
  
  if(is.null(custom_mcv2_coverage)){
    
    if (use_lagged_covs) {
      mcv2 <- get_covariate_estimates(covariate_id=2310, release_id = release_id)
    } else {
      if (decomp) mcv2 <- get_covariate_estimates(covariate_id=1108, release_id = release_id) else mcv2 <- get_covariate_estimates(covariate_id=1108, release_id = release_id)
    }
    
  } else {
    
    if (use_lagged_covs) {
      mcv2 <- get_covariate_estimates(covariate_id=2310, model_version_id = custom_mcv2_coverage, release_id = release_id)
    } else {
      if (decomp) mcv2 <- get_covariate_estimates(covariate_id=1108, release_id = release_id, model_version_id = custom_mcv2_coverage) else mcv2 <- get_covariate_estimates(covariate_id=1108, release_id = release_id, model_version_id = custom_mcv2_coverage)
    }
    
  }
  
  mcv2[, ln_unvacc_mcv2 := log(1 - mean_value)]
  setnames(mcv2, "mean_value", "mean_mcv2")
  # save model version
  cat(paste0("Covariate MCV2 (NF) - model version ", unique(mcv2$model_version_id)), file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
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
      
      # fill in missing values
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
  
  # Offset zero values for mean coverage
  covs <- covs[, `:=` (offset_mean_mcv1 = if_else(mean_mcv1 == 0, 0.000000001, mean_mcv1), 
                       offset_mean_mcv2 = if_else(mean_mcv2 == 0, 0.000000001, mean_mcv2))] 

  return(covs)
  
  # Or run function with COVID inclusive vaccine estimates
  
  } else if (covid_inclusive_vax == T){
    
    ### get lagged mcv 1 covid inclusive vaccine covariate results
    mcv1 <- get_covariate_estimates(covariate_id= 2442, 
                                        release_id = release_id)
    
    mcv1[, ln_unvacc := log(1 - mean_value)]
    setnames(mcv1, "mean_value", "mean_mcv1")
   
    # save model version
    cat(paste0("Covariate MCV1 (NF) - model version ", unique(mcv1$model_version_id)), file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
    #remove excess columns
    mcv1 <- mcv1[, .(location_id, year_id, ln_unvacc, mean_mcv1)]
    
    ### get lagged mcv 2 covid inclusive vaccine covariate results
    mcv2 <- get_covariate_estimates(covariate_id = 2443, 
                                    release_id = release_id)
    
    mcv2[, ln_unvacc_mcv2 := log(1 - mean_value)]
    setnames(mcv2, "mean_value", "mean_mcv2")
   
     # save model version
    cat(paste0("Covariate MCV2 (NF) - model version ", unique(mcv2$model_version_id)), file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
    #remove excess columns
    mcv2 <- mcv2[, .(location_id, year_id, ln_unvacc_mcv2, mean_mcv2)]
    
    # merge
    covs_both <- merge(mcv1, mcv2, by=c("location_id", "year_id"), all.x=TRUE)
    
    # remove missing data
    covs <- copy(covs_both[!is.na(ln_unvacc_mcv2)])
    
    # Create offsets
    covs <- covs[, `:=` (offset_mean_mcv1 = if_else(mean_mcv1 == 0, min(covs$mean_mcv1[covs$mean_mcv1!=0]/2), mean_mcv1), 
                         offset_mean_mcv2 = if_else(mean_mcv2 == 0, min(covs$mean_mcv2[covs$mean_mcv2!=0]/2), mean_mcv2))] 
    
    if(add_masks == T) {
      
      #Get mask covariate
      masks <- get_covariate_estimates(covariate_id= 2537, #proportion of people reporting using a mask when leaving home 
                                       release_id = release_id)
      setnames(masks, "mean_value", "mask_use")
      cat(paste0("Covariate masking - model version ", unique(masks$model_version_id)), file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
      
      # Make post 2022 values = 0 (NB all values before 2020 are zero in covariate data)
      masks[, mask_use := ifelse(year_id >=2022, 0, mask_use)]
      
      # Create no masking variable in order to align with direction of predictions. 
      masks[, no_mask := 1 - mask_use]
      masks[, ln_no_mask := log(no_mask)]
      
      # Keep desired columns
      masks <- masks[, .(location_id, year_id, mask_use, no_mask, ln_no_mask)]
      
      # Merge covariates
      covid_covs <- copy(masks)
      covs <- merge(covs, covid_covs, by = c("location_id", "year_id"), all.x = T)
    }
    return(covs)
  }
}

### combine input data for regression
prep_regression <- function(...) {
  
  # merge case notifications on location_id, region, and super region
  regress <- merge(case_notif, locations[, .(location_id, location_name, ihme_loc_id, region_id, super_region_id)], by="location_id", all.x=TRUE) 
  
  # merge on population (NOTE: this is the population of the birth cohort at the average age of notification--our way to address underreporting)
  regress <- join(regress, sum_pop_young, by=c("location_id", "year_id"), type="inner")
  
  # merge with covariates and WHO SIA data
  regress <- join(regress, covs, by=c("location_id", "year_id"), type="inner")
  regress <- join(regress, SIAs, by=c("location_id", "year_id"), type="left")
  
  # fix missing lagged SIA
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
  
  ### Copy regression file to avoid shallow copy warning
  regress <- copy(regress)
  
  # Calculate incidence rates from the population of the 1 year birth cohort at average age of notification
  regress[, inc_rate := (cases / pop) * 100000]
  
  # because of the 1-year population, some unrealisticaly high incidence rates generated for places with good notification and outbreaks
  # drop these high outliers that suggest >95% of the population of the entire country got measles
  regress <- regress[inc_rate <= 95000, ]
  
  # Remove PNG outbreaks
  regress <- regress[!(ihme_loc_id=="PNG" & year_id %in% c(2013:2015)), ]
  
  ### Log transform incidence rate
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
  
  # As of GBD2017 data pre 1995 is removed
  regress <- regress[year_id >= 1995, ]  
  
  return(regress)
  
}

### prepare regression input data
covs <- prep_covs()
regress <- prep_regression() 
regress <- regress[location_id %in% standard_locations] #this is where most subnationals are dropped from the regression model!

# prep for folds if doing OOS validation
if (!run_oos & n_folds != 0) {
  message("Resetting n_folds to 0 since run_oos = FALSE.")
  message("If you meant to run an out-of-sample model, please check your options.")
  n_folds <- 0
}

# Generate folds (holdouts)
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
fwrite(regress, file.path(FILEPATH.inputs, "case_regression_input.csv"), row.names=FALSE)

#***********************************************************************************************************************

# If running to calculate relative risks, call function to fit model with vaccine coverage rather than 1-coverage. 
# Save summary of model and then function will automatically STOP modeling. Relative risk is exp(beta).

if(run_for_rrs){
 
  if(custom_version == "09.17.20"){
    regress <- fread("/FILEPATH/case_regression_input.csv")
  }
  
  source(paste0("/FILEPATH/rr_custom_incidence_model.R"))
  model_for_rr_beta <- fit_model_for_rrs(acause = acause)
  
}

#****************************************************************************************

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
  
}  

### scale regression covariates
if (scale_to_converge) {
  ## save the means
  #mean_i  <- mean(regress$ln_inc)  # << this is what predicting, so don't need to scale!
  mean_u1 <- mean(regress_fold$ln_unvacc)
  mean_u2 <- mean(regress_fold$ln_unvacc_mcv2)
  #mean_s  <- mean(regress$supp)
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

#-------------------------------------------------------------------------
### run mixed effects regression model

if(add_masks == F){
if (MCV1_or_MCV2 == "use_MCV2"){
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
  
} else if (MCV1_or_MCV2 == "composite") { 
  me_model<- lmer(ln_inc ~ final_mcv1_sia + final_mcv2_sia + (1 | super_region_id) + (1 | region_id) + (1 | location_id),
                  data=regress_fold, control = lmerControl(optimizer = "nloptwrap", calc.derivs = FALSE))
  
}
### save summary of object and model object and error
if(fold==0){
  
  capture.output(summary(me_model), file=file.path(FILEPATH.logs, "log_incidence_mereg.txt"), type="output") 
  saveRDS(me_model, file=file.path(FILEPATH.logs, "log_incidence_mereg.rds"))
  
} else {
  
  capture.output(summary(me_model), file=file.path(FILEPATH.logs, paste0("log_incidence_mereg_", fold, ".txt")), type="output")
  saveRDS(me_model, file=file.path(FILEPATH.logs, paste0("log_incidence_mereg_", fold, ".rds")))
  
}
} else {
  # with masks
  me_model <- lmer(ln_inc ~ ln_unvacc + ln_unvacc_mcv2 + supp_1 + supp_2 + supp_3 + supp_4 + supp_5 + ln_no_mask + #mobility +
                     (1 | super_region_id) + (1 | region_id) + (1 | location_id), data=regress_fold,
                   control = lmerControl(optimizer = "nloptwrap", calc.derivs = FALSE))

 if(fold==0){
    
    capture.output(summary(me_model), file=file.path(FILEPATH.logs, "log_incidence_mereg.txt"), type="output") 
    saveRDS(me_model, file=file.path(FILEPATH.logs, "log_incidence_mereg.rds"))
    
  } else {
    
    capture.output(summary(me_model), file=file.path(FILEPATH.logs, paste0("log_incidence_mereg_", fold, ".txt")), type="output")
    saveRDS(me_model, file=file.path(FILEPATH.logs, paste0("log_incidence_mereg_", fold, ".rds")))
    
  }

}

#calculate and save sample fit statistics for model
regress_fold[, obs:=exp(regress_fold$ln_inc)]
regress_fold[!(is.na(obs)), predicted := exp(predict(me_model, regress_fold[!(is.na(obs)),], type="response"))]
inc_rmse <- rmse(regress_fold$predicted, regress_fold$obs, remove_na=T)
inc_mae <- mae(regress_fold$predicted, regress_fold$obs, remove_na=T)

#add these in to report
if(fold==0) cat(c("\nRMSE: ", inc_rmse, "\nMAE: ", inc_mae), file=file.path(FILEPATH.logs, "log_incidence_mereg.txt"), append=TRUE) else cat(c("\nRMSE: ", inc_rmse, "\nMAE: ", inc_mae), file=file.path(FILEPATH.logs, paste0("log_incidence_mereg_", fold, ".txt")), append=TRUE)

#***********************************************************************************************************************

#----PREDICTIONS--------------------------------------------------------------------------------------------------------------
set.seed(0311)  

# merge ihme_loc_id, population (NOTE: this is JUST the population of the birth cohort at the average age of notification)
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

# Is the masking covariate included in the model? Add extra columns
if(add_masks){
  cols <- c(cols, "b_no_masks")
}

# Get coefficient matrix
coeff <- cbind(fixef(me_model)[[1]] %>% data.table, coef(me_model)[[1]] %>% data.table %>% .[, "(Intercept)" := NULL] %>% .[1, ])
colnames(coeff) <- cols
coefmat <- matrix(unlist(coeff), ncol=length(cols), byrow=TRUE, dimnames=list(c("coef"), c(as.vector(cols))))

# generate a standard random effect set such that 95% of unvaccinated individuals will get measles (95% attack rate)
# given 0% vaccinated, the combination of the RE and the constant will lead to an incidence of 95,000/100,000
standard_RE <- log(95000) - coefmat["coef", "constant"]

# covariance matrix
vcovmat <- vcov(me_model)
vcovlist <- NULL

if(add_masks == F){
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
} else {
for (ii in 1:length(cols)) {
# vcovlist_a <- c(vcovmat[ii, 1], vcovmat[ii, 2], vcovmat[ii, 3], vcovmat[ii, 4], vcovmat[ii, 5], vcovmat[ii, 6], vcovmat[ii, 7], vcovmat[ii, 8], vcovmat[ii, 9]), vcovmat[ii,10])
  vcovlist_a <- c(vcovmat[ii, 1], vcovmat[ii, 2], vcovmat[ii, 3], vcovmat[ii, 4], vcovmat[ii, 5], vcovmat[ii, 6], vcovmat[ii, 7], vcovmat[ii, 8], vcovmat[ii, 9])
  vcovlist <- c(vcovlist, vcovlist_a)
}
}

vcovmat2 <- matrix(vcovlist, ncol=length(cols), byrow=TRUE)

# Get samples from the distribution of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
betadraws <- rmvnorm(n=1000, mean=coefmat, sigma=vcovmat2)

# transpose coefficient matrix
betas <- t(betadraws)

### calculate cases from model coefficients
draw_nums <- 1:1000
draw_cols <- paste0("case_draw_", draw_nums_gbd)

### If running model on full data, predict out draws as well as in-sample point estimate

if(!add_masks){
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
}

if(add_masks){
  #predict draws
      invisible(draws_nonfatal[, (draw_cols) := lapply(draw_nums, function(x) { exp( betas[1, x] +
                                                                               ( betas[2, x] * ln_unvacc ) +
                                                                               ( betas[3, x] * ln_unvacc_mcv2 ) +
                                                                               ( betas[4, x] * supp_1 ) +
                                                                               ( betas[5, x] * supp_2 ) +
                                                                               ( betas[6, x] * supp_3 ) +
                                                                               ( betas[7, x] * supp_4 ) +
                                                                               ( betas[8, x] * supp_5 ) +
                                                                               ( betas[9, x] * ln_no_mask ) +
                                                                               standard_RE ) *
            ( pop / 100000 ) } )] )
  draws_nonfatal <- draws_nonfatal[, c("location_id", "year_id", draw_cols), with=FALSE]
  }


### save results
if (WRITE_FILES == "yes") {
  
  if(fold==0) fwrite(draws_nonfatal, file.path(FILEPATH, "01_case_predictions_from_model.csv"), row.names=FALSE) 
  else fwrite(draws_nonfatal, file.path(FILEPATH, paste0("01_case_predictions_from_model_", fold, ".csv")), row.names=FALSE)
  
}

### Calculate oos error and then proceed with draws_nonfatal from full model if running OOS model by reading in draws_nonfatal made on full sample
if(run_oos == T){
  
  calculate_oos_error(preds.dir = FILEPATH, input.data.dir = FILEPATH.inputs, output.dir = FILEPATH.logs, folds = n.folds)
  draws_nonfatal <- fread(file.path(FILEPATH, "01_case_predictions_from_model.csv"))
  
  #drop in sample prediction column created for model validation before proceeding with model
  draws_nonfatal[, is_mean:=NULL]
}

#***********************************************************************************************************************

#----FIX MODEL ESTIMATES------------------------------------------------------------------------------------------------

################################################################################
# Step 1 - identify locations with trusted case notifications and/or elimination locs
################################################################################

# SR 64 = High-income 31 = Central Europe, Eastern Europe, and Central Asia 103 = Latin America and Caribbean
sr_64_31_103 <- locations[super_region_id %in% c(64, 31, 103), location_id]

# ignore GBD locations in trusted super regions that do not have case notifications (i.e. BMU, GRL, PRI, VIR, and
# other high-income subnationals levels), this way will include subnational case notifications (instead of just national)
trusted_notification_locs <- sr_64_31_103[sr_64_31_103 %in% unique(case_notif$location_id)]

# add on add'l elimination locations to trusted_notif 
elimination_loc_ids <- locations$location_id[locations$ihme_loc_id %in% elimination_locs]

# Combine with trusted notification superregions
trusted_notification_locs <- c(trusted_notification_locs, elimination_loc_ids) %>% unique()

################################################################################
# Step 2: Get complete case notifications from STGPR model 
################################################################################

st_gpr_estimates <- get_estimates(st_gpr_version, entity = "gpr") 

# Make copy to align with prior nomencalature 
adjusted_notifications_q <- copy(st_gpr_estimates)

# Create a variable to compare year on year changes for the last 3 years of modeling
adjusted_notifications_q <- adjusted_notifications_q[, c("delta_1", "delta_2") := list(
  val[year_id == year_end_data] / val[year_id == year_end_data-1],
  val[year_id == year_end_data+1] / val[year_id == year_end_data]), by = location_id]

# Subset locations where ratio of last yeary versus penultimate years was greater than the 95% percentile
to_fix <- adjusted_notifications_q[delta_2 >= quantile(adjusted_notifications_q$delta_2, 0.95), ] 

# Identify locations for which prediction year (ie year for which STGPR has no data) needs to be adjusted
stgpr_to_fix <- locations$location_name[locations$location_id %in% unique(to_fix$location_id)]

# Replace last year estimates in extreme locations with penultimate year estimates multiplied by a correction factor
corr_fac <- quantile(adjusted_notifications_q$delta_2, 0.95) #Use the rate of change of the 95% percentile 

# Extract relevant estimates
change <- adjusted_notifications_q[location_id %in% unique(to_fix$location_id) & year_id > 2022, .(location_id, year_id, val)]

# Make wider dataframe 
change_wide <- dcast(change, location_id ~ year_id, value.var = "val")
setDT(change_wide)

#calculate new val for 2024
change_wide[, val := `2023` * corr_fac]

# Reformat for binding with adjusted notifications
change_wide <- change_wide[, .(location_id, val)]
change_wide[, year_id := 2024]

# Remove affected 2024 locations from adjusted case notifications and rbind corrected estimates 
adjusted_notifications_q <- adjusted_notifications_q[!(location_id %in% unique(change_wide$location_id) & year_id == 2024), ]

# Bind replacement values 
adjusted_notifications_q <- rbind(adjusted_notifications_q, change_wide, fill = T) 

# subtract ST-GPR offset manually
offset <- 9.893e-7 #From stgpr viz
adjusted_notifications_q <- adjusted_notifications_q[, mean := val - offset]

# Merge estimates with population
adjusted_notifications_q <- merge(adjusted_notifications_q, pop_u5, by=c("location_id", "year_id"), all.x=TRUE)
adjusted_notifications_q <- adjusted_notifications_q[!is.na(pop), ]

# Multiple rate by population to get case numbers
adjusted_notifications_q[,  mean := mean * pop]

# Keep Macao and Hong Kong, also keep all UK subnationals other than Wales, we do not have any data for these locations!
uk_sub_locs <- locations$location_id[locations$parent_id == 95]
missing_locs <- c(uk_sub_locs, 354, 361)
keep_to_bind <- adjusted_notifications_q[location_id %in% missing_locs, ]
keep_to_bind <- keep_to_bind[!location_id == locations$location_id[locations$location_name == "Wales"]]

# Use old model results 

# Read in old stgpr draws 
folder_path <- "/FILEPATH/case_notifications_06.09.21"
file_list <- list.files(path = folder_path, pattern = "*.csv", full.names = TRUE)

# Read and combine all CSV files into one data.table
stgpr_old <- rbindlist(lapply(file_list, fread))

# Create a copy for modification
stgpr_old_mod <- copy(stgpr_old)

stgpr_old_mod <- stgpr_old_mod[, c("combined", "nid", "pop", "case_notifications") := NULL]
setnames(stgpr_old_mod, "st_gpr_output", "mean") # in est shocks copy, the st_gpr_output has already been offset and multiplied by population!

# Keep needed years for hybridization
stgpr_old_mod <- stgpr_old_mod[year_id <= 2021, ]
stgpr_new <- adjusted_notifications_q[year_id > 2021, ]

# Combine
adjusted_notifications_q <- rbind(stgpr_old_mod, stgpr_new, fill = T)

# Make square dataframe of notifications to enable fill
notif_square <- CJ(location_id=unique(case_notif$location_id), year_id=1980:year_end) %>% as.data.table 

# Merge with case notifications
notifs <- merge(notif_square, case_notif, by=c("location_id", "year_id"), all.x=TRUE)
notifs <- unique(notifs, by = c("location_id", "year_id")) 

# Merge notifications with stgpr output
adjusted_notifications_q <- merge(notifs, adjusted_notifications_q, by=c("location_id", "year_id"), all.x=TRUE)

# Get HKG and Macao and missing UK subnationals back in dataframe
adjusted_notifications_q <- rbind(adjusted_notifications_q, keep_to_bind, fill = T) %>% unique()

# Fix column names
adjusted_notifications_q[, notifications := cases] # rename jrf case notifications 

# Replace missing case notifications with stgpr output: 
adjusted_notifications_q[is.na(notifications), notifications := round(mean, 0)] # NB mean = stgpr estimates

# Fix variable names
setnames(adjusted_notifications_q, c("cases", "mean", "notifications"), c("who_case_notifications", "st_gpr_output", "combined"))

### save filled case notifications by location_id
stgpr_save_dir <- paste0("/FILEPATH", custom_version, "/")

if (!dir.exists(stgpr_save_dir)) dir.create(stgpr_save_dir, recursive = T)
fwrite(adjusted_notifications_q, paste0(stgpr_save_dir, "smoothed_notifs.csv"))

message("saved combined ST-GPR results")

# Rename variables
setnames(adjusted_notifications_q, "combined", "cases") 

# Data check: make sure that no post-fill cases are lower than who case notifications
if(sum(adjusted_notifications_q$cases < adjusted_notifications_q$who_case_notifications, na.rm = T)>0){
  stop()
  print("Your final case estimates are lower than notified cases!")
}else{
  print("No cases are lower than notifications, proceed!")
}

# Remove missing data and keep desired columns
adjusted_notifications_q <- adjusted_notifications_q[!is.na(cases), .(location_id, year_id, cases)]

if(sum(is.na(adjusted_notifications_q) > 0)){ 
  message("Stop! Your data contains NAs")
}else{
  print("Proceed, there are no NAs in your data")
}

################################################################################
# Step 3: Fix subnationals and calculate error for adjusted notifications
################################################################################

# calculate incidence rate
adjusted_notifications <- merge(adjusted_notifications_q, pop_u5, by=c("location_id", "year_id"), all.x=TRUE)
adjusted_notifications <- merge(adjusted_notifications, locations[, .(location_id, region_id, super_region_id)], by="location_id", all.x=TRUE)
invisible(adjusted_notifications[, inc_rate := cases / pop])

# floor draws at 0 - this fixes any stgpr results that are < 0 from st-gpr estimate < offset
adjusted_notifications[inc_rate < 0, inc_rate := 0]

# Align subnational with national estimates
if(trusted_case_distrib == "binom"){
  
  # make sure no locs with subnat notifications where sum(subnat) = 0 and national>1 
  subnat_sums <- merge(adjusted_notifications, locations[,.(location_id, parent_id)], by = "location_id")[,.(subnat_sum = sum(cases)),.(parent_id, year_id)]
  setnames(subnat_sums, "parent_id", "location_id")
  subnat_sums <- merge(subnat_sums, adjusted_notifications[,.(location_id, year_id, cases)], by = c("location_id", "year_id"))
  
  # check data
  if(nrow(subnat_sums[subnat_sum == 0 & cases > 0]) > 0){ 
    message(paste0("You have ",  
            nrow(subnat_sums[subnat_sum == 0 & cases > 1]), " location-years where the sum of the subnat cases is 0 and the parent is greater than 1."))
  } else {
    print("Proceed! Subnational sums are never zero when parent is greater than 1")
  }
  
  # generate draws of cases, calculate incidence rate to match result when used rnorm
  inc_draws <- rbinom(n=1000 * length(adjusted_notifications$inc_rate), 
                      prob = adjusted_notifications$inc_rate, 
                      size=as.integer(adjusted_notifications$pop)) %>% matrix(., ncol=1000, byrow=FALSE) %>% as.data.table
  
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

################################################################################
# Step 4: Fix draws in non-trusted locations where estimates are lower than case notifications
################################################################################

# Calculate row means of draws
cols_to_average <- grep("draw", names(draws_nonfatal))

# Compute means of draws
draw_means <- draws_nonfatal[, mean_draw := rowMeans(.SD), .SDcols = cols_to_average]
draw_means <- draw_means[, .(location_id, year_id, mean_draw)]

# Merge with case notifications
test <- merge(case_notif, draw_means, by = c("location_id", "year_id"), all.x = T)

# Identify location_years where the case notifications exceed model estimates in 
# "non trusted" locations
test <- test[!location_id %in% trusted_notification_locs, ]
test <- test[cases > mean_draw, ] 

# Merge with locations
test <- merge(test, locations[, .(location_id, location_name, level)], by = "location_id", all.x = T)

# Check if subnationals are modelled in underestimated locations
mod_sub <- locations[parent_id %in% unique(test$location_id), ]
if(nrow(mod_sub) == 0){
  message("There are no subnationals to adjust")
}else{
  message("Stop! code to adjust subnationals is pending")
}

# Create location year variable for locations to adjust
test[, location_year := paste(location_id, year_id, sep = "_")]

# Merge test with population
test <- merge(test, pop_u5, by = c("location_id", "year_id"))
test[, inc_rate := cases / pop]

# subset data for saving
test_to_save <- test[, .(nid, location_id, location_name, year_id, cases, mean_draw, pop)]
test_to_save[, abs_diff := cases - mean_draw]

if(WRITE_FILES == "yes"){
  fwrite(test_to_save, paste0(FILEPATH.logs, "/loc_years_for_case_notif_floor_adjustment.csv"))
}

if(trusted_case_distrib == "binom"){ 
  
  # generate draws of cases, calculate incidence rate to match result when used rnorm
  inc_draws_nt <- rbinom(n=1000 * length(test$inc_rate), 
                      prob = test$inc_rate, 
                      size=as.integer(test$pop)) %>% matrix(., ncol=1000, byrow=FALSE) %>% as.data.table
  colnames(inc_draws_nt) <- paste0("inc_draw_", draw_nums_gbd)
  
  # Column bind to combine with new draws
  test <- cbind(test, inc_draws_nt)
  inc_draws_names <- paste0("inc_draw_", draw_nums_gbd)
  test[, (inc_draws_names) := lapply(.SD, function(x) x/pop), .SDcols = inc_draws_names]
  
} else if (trusted_case_distrib == "norm"){
  
  # calculate standard error
  test[, SE := sqrt( (inc_rate * (1 - inc_rate)) / pop )]  
  
  # calculate region and super-region average error for country-years with 0 cases reported
  notif_error_reg <- ddply(test, c("region_id", "year_id"), summarise, reg_SE=mean(SE, na.rm=TRUE))
  notif_error_sr <- ddply(test, c("super_region_id", "year_id"), summarise, sr_SE=mean(SE, na.rm=TRUE))
  
  # add on region and super-region mean error, replace with these errors if country-level error is zero
  test <- merge(test, notif_error_reg, by=c("region_id", "year_id"), all.x=TRUE)
  test <- merge(test, notif_error_sr, by=c("super_region_id", "year_id"), all.x=TRUE)
  invisible(test[SE==0 | is.na(SE), SE := reg_SE])
  invisible(test[SE==0 | is.na(SE), SE := sr_SE])
  
  # if region and super region error is still zero, calculate non-zero SE
  test[SE==0, SE := sqrt( ((1 / pop) * inc_rate * (1 - inc_rate)) + ((1 / (4 * (pop ^ 2))) * ((qnorm(0.975)) ^ 2)) ) ]
  
  # generate 1000 draws of incidence from error term
  inc_draws <- rnorm(n=1000 * length(test$inc_rate), mean=test$inc_rate, sd=test$SE) %>% matrix(., ncol=1000, byrow=FALSE) %>% as.data.table
  test <- cbind(test, inc_draws)
}

# calculate cases from incidence and population, keep needed columns
test[, (draw_cols) := lapply(draw_nums_gbd, function(x) get(paste0("inc_draw_", x)) * pop)]
adjusted_draws <- test[, c("location_id", "year_id", draw_cols), with=FALSE]

# Create location year variable in draws_non_fatal
draws_nonfatal[, location_year := paste(location_id, year_id, sep = "_")]

# Remove underestimated location_years from draws_nonfatal
removed_to_replace_nt <- draws_nonfatal[!location_year %in% test$location_year, ]

#keep needed columns
removed_to_replace_nt <- removed_to_replace_nt[, c("location_year", "mean_draw") := NULL]

# Bind adjusted draws with rest of estimates
draws_nonfatal <- rbind(removed_to_replace_nt, adjusted_draws)

# Recalculate mean of draws to make sure adjustment has been implemented correctly
draw_means <- draws_nonfatal[, mean_draw := rowMeans(.SD), .SDcols = cols_to_average]

# compute standard deviation of draws
draw_means[, sd_draw := apply(.SD, 1, sd), .SDcols = cols_to_average] 
draw_means <- draw_means[, .(location_id, year_id, mean_draw, sd_draw)]

# Merge with case notifications
test_2 <- merge(case_notif, draw_means, by = c("location_id", "year_id"), all.x = T)

# Identify location_years where the case notifications exceed mean +/- SD
test_2 <- test_2[!location_id %in% trusted_notification_locs, ]
if(nrow(test_2[cases > mean_draw + sd_draw, ]) > 0 ){
  message("Incidence floor adjustment was not successful")
}else{
  print("Proceed, the incidence floor adjustment has been completed")
}

################################################################################
# Step 5: Combine trusted and not-trusted adjusted case estimates and save
################################################################################

# Remove excess columns
draws_nonfatal <- draws_nonfatal[, c("mean_draw", "sd_draw") := NULL]

# Combine case draws: modeled estimates in "non trusted" locations and case notifications + fill in trusted locations
combined_case_draws <- rbind(adjusted_notifications[location_id %in% trusted_notification_locs, ], 
                       draws_nonfatal[!location_id %in% trusted_notification_locs, ])
  
### save results
if (WRITE_FILES == "yes") {
  
  fwrite(combined_case_draws, file.path(FILEPATH, "02_add_case_notifs_and_shocks.csv"), row.names=FALSE)
  
}
# Remove data to clear memory
rm(draws_nonfatal)
gc(TRUE)
#***********************************************************************************************************************

########################################################################################################################
##### PART TWO: RESCALE CASES ##########################################################################################
########################################################################################################################

#----RESCALE------------------------------------------------------------------------------------------------------------

### run custom rescaling function: assures that subnational estimates match national estimates! 
rescaled_cases <- rake(combined_case_draws)

# Remove data to clear memory
rm(combined_case_draws)

# Fix subnational locations where rescaling process produced wrong counts
if(trusted_case_distrib == "binom"){
  
  # aggregate subnats for low, nonzero case count years 
  rescaled_cases <- merge(rescaled_cases, locations[,.(location_id, parent_id)])
  rescaled_cases[, parentyear := paste0(parent_id, year_id)]
  rescaled_cases[, locyear := paste0(location_id, year_id)]
  
  is_NA <- apply(rescaled_cases, 1, function(x) any(is.na(x))) #locs with NA draws are subnats where the NAs were made in raking
  parentyears_to_agg <- unique(rescaled_cases[is_NA ,.(parentyear)])
  
  # identify which locyears have nonzero natl draws but had NAs (ie 0s) for subnat draws)
  subnat_sums[, parentyear := paste0(location_id, year_id)]
  parentyears_to_agg <- parentyears_to_agg[!(parentyear %in% subnat_sums[cases == 0, parentyear])] # 19 rows on 10.03.22 run
  
  # reset rows with all 0 draws that became NA in raking to 0
  rescaled_cases[,(draw_cols) := lapply(.SD, function(x) ifelse(is.na(x), 0, x)), .SDcols=draw_cols] 
  
  # aggregate subnationals to parent
  agg <-  rescaled_cases[parentyear %in% parentyears_to_agg$parentyear]
  agg <- agg[, (draw_cols) := lapply(.SD, sum), .SDcols = draw_cols, by = "parentyear"]
  keep_cols <- c(draw_cols, "parent_id", "year_id", "parentyear")
  agg <- unique(agg[, keep_cols, with = FALSE], by = c("parentyear"))
  setnames(agg, c("parent_id", "parentyear"), c("location_id", "locyear"))
  
  # replace national draws with aggregated draws
  rescaled_cases <- rescaled_cases[!(locyear %in% agg$locyear)]
  rescaled_cases[, parent_id := NULL]
  rescaled_cases[, parentyear := NULL]
  rescaled_cases <- rbind(rescaled_cases, agg)
  # rescaled_cases <- rbind(rescaled_cases, agg, fill = T)
  rescaled_cases[, locyear := NULL]
  
}

# Check to make sure there are no NAs in the rescaled cases
if(sum(is.na(rescaled_cases > 0))){
  message("there are NAs in your data, stop!")
}else{
  print("Proceed! There are no NAs in your data")
}

gc(T)

### save results
if (WRITE_FILES == "yes") {
  if(cap_draws){
    fwrite(rescaled_cases, file.path(FILEPATH, "03_case_predictions_rescaled_uncapped.csv"), row.names=FALSE) # if capping draws, save this uncapped version now for posterity, official rescaled is saved below!
  } else {
    fwrite(rescaled_cases, file.path(FILEPATH, "03_case_predictions_rescaled.csv"), row.names=FALSE) # if not capping draws, save this as final rescaled file
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
  
  # check that there are no missing values in split cases
  if(sum(is.na(split_cases > 0))){
    message("there are NAs in your data, stop!")
  }else{
    print("There are no NAs in your data")
  }

### save results
  if (WRITE_FILES == "yes") {
    fwrite(split_cases, file.path(FILEPATH, "04_case_predictions_split.csv"), row.names=FALSE)
  }
  #***********************************************************************************************************************
  
  #######################################################################################################################
  ##### PART FOUR: CASES TO PREVALENCE AND INCIDENCE #####################################################################
  ########################################################################################################################
  
  
  #----PREVALENCE AND INCIDENCE-------------------------------------------------------------------------------------------
  ### convert case counts to incidence rates and prevalence
  
  if(cap_draws){
    # prep data
    predictions_inc <- copy(split_cases) %>% .[, measure_id := 6]
    rm(split_cases)
    gc(TRUE)
    
    # calculate incidence
    invisible(predictions_inc[, (draw_cols_gbd) := lapply( draw_nums_gbd, function(x) get(paste0("split_case_draw_", x)) / population)] )
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
    
    # to maintain consistency between non-fatal and fatal results, recalculate all age both sex case draws from capped incidence
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
      fwrite(rescaled_cases, file.path(FILEPATH, "03_case_predictions_rescaled.csv"), row.names=FALSE)
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
  
  ### save to  directory
  system.time(lapply(unique(predictions$location_id), function(x) fwrite(predictions[location_id==x, ],
                                                                         file.path(cl.version.dir, paste0(x, ".csv")), row.names=FALSE)))
  print(paste0("nonfatal estimates saved in ", cl.version.dir))
  
  ## save measles nonfatal draws to the database, modelable_entity_id=1436
  # for slurm
  job_nf <- paste0("FILEPATH", 
                   paste0("FILEPATH/save_results_wrapper.r"), 
                   " --type epi",
                   " --me_id ", me_id,
                   " --input_directory ", cl.version.dir, 
                   " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
                   " --best ", mark_model_best,
                   " --year_ids ", paste(1980:year_end, collapse=","),
                   " --xw_id ", nf_xw_id,
                   " --bundle_id ", nf_bundle_id, 
                   " --release_id ", release_id)
  print(job_nf)
  system(job_nf)
  #***********************************************************************************************************************
}

if (CALCULATE_COD == "yes") {
  
  ########################################################################################################################
  ##### PART Six: MODEL CFR ##############################################################################################
  ########################################################################################################################
  
  
  #----PREP---------------------------------------------------------------------------------------------------------------
  ### function: prep covariates for cfr neg bin model

  prep_covariates <- function(...) {
    mal_cov <- get_covariate_estimates(covariate_id=1230, 
                                       year_id=1980:year_end, 
                                       location_id=pop_locs, 
                                       release_id = release_id)
    mal_cov[, ln_mal := log(mean_value)] # + 1e-6)]
    setnames(mal_cov, "mean_value", "mal")
    
    # save model version
    cat(paste0("Covariate malnutrition, covariate id 1230 (CoD) - model version ", unique(mal_cov$model_version_id)), file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
    # remove excess columns
    covariates <- mal_cov[, c("location_id", "year_id", "mal", "ln_mal"), with=FALSE]
    
    # LDI, covariate_name_short="LDI_pc"
    ldi_cov <- get_covariate_estimates(covariate_id=57, 
                                       year_id=1980:year_end, 
                                       location_id=pop_locs, 
                                       release_id = release_id)
    ldi_cov[, ln_LDI := log(mean_value)]
    setnames(ldi_cov, "mean_value", "LDI")
    
    # save model version
    cat(paste0("Covariate LDI (CoD) - model version ", unique(ldi_cov$model_version_id)), file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
    # remove excess columns
    ldi_cov <- ldi_cov[, c("location_id", "year_id", "ln_LDI", "LDI"), with=FALSE]
    covariates <- merge(ldi_cov, covariates, by=c("location_id", "year_id"), all.x=TRUE)
    
    # healthcare access and quality index, covariate_name_short="haqi"
    haqi <- get_covariate_estimates(covariate_id=1099, 
                                    year_id=1980:year_end, 
                                    location_id=pop_locs, 
                                    release_id = release_id)
    haqi[, HAQI := mean_value / 100] #setnames(haqi, "mean_value", "HAQI")
    haqi[, ln_HAQI := log(HAQI)]
    
    # save model version
    cat(paste0("Covariate HAQ (CoD) - model version ", unique(haqi$model_version_id)), file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
    # remove excess columns
    haqi <- haqi[, c("location_id", "year_id", "HAQI", "ln_HAQI"), with=FALSE]
    covariates <- merge(haqi, covariates, by=c("location_id", "year_id"), all.x=TRUE)
    
    # socio-demographic index, covariate_name_short="sdi"
    sdi <- get_covariate_estimates(covariate_id=881, year_id=1980:year_end, location_id=pop_locs, release_id = release_id)
    
    # save model version
    cat(paste0("Covariate SDI (CoD) - model version ", unique(sdi$model_version_id)), file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
    # remove excess columns
    setnames(sdi, "mean_value", "SDI")
    sdi <- sdi[, c("location_id", "year_id", "SDI"), with=FALSE]
    covariates <- merge(sdi, covariates, by=c("location_id", "year_id"), all.x=TRUE)
    
    return(covariates)
    
  }
  
### function: add vital registration data to CFR regression

  add_VR <- function(add_vital_registration=TRUE) {
    
    mortality_envelope <- get_envelope(location_id=pop_locs, 
                                       year_id=1980:year_end, 
                                       age_group_id=22, 
                                       sex_id=1:2, 
                                       release_id = release_id)
    
    setnames(mortality_envelope, "mean", "mortality_envelope")
    
    ### get CoD data
    if (add_vital_registration) {
      cod <- get_cod_data(cause_id=cause_id, 
                         release_id = release_id)
    }
    
    # save model version
    cat(paste0("CoD data - version ", unique(cod$description)),
        file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

    # Keep all age data
    cod <- cod[age_group_id %in% 22 & !is.na(cf_corr) & sample_size != 0, ]
    
    # Merge with mortality envelope 
    cod_env <- merge(cod, mortality_envelope[, .(location_id, year_id, age_group_id, sex_id, mortality_envelope)], by=c("location_id", "year_id", "age_group_id", "sex_id"))
    
    # Calculate deaths
    cod_env[, deaths := cf_corr * mortality_envelope]
    
    # Sum by location-year
    cod_sum <- cod_env[, .(deaths=sum(deaths), sample_size=sum(sample_size)), by=c("location_id", "year_id")]
    
    # merge with locations metadata
    cod_sum <- merge(cod_sum, locations[, .(location_id, ihme_loc_id, super_region_id)], by="location_id", all.x=TRUE)

    ### get WHO notifications
    if(CALCULATE_NONFATAL == "no"){
      case_notif <- fread(paste0(j_root, "/FILEPATH/case_notifications_with_subnational_data.csv"))
    }else{
      message("Case notificaitons are in global environment!")
    }
   
    # Merge case notifications with cod data
    WHO <- merge(case_notif, cod_sum[, .(location_id, year_id, deaths)], by=c("location_id", "year_id"))
    # remove instances with zero cases
    WHO <- WHO[cases > 0, ]
    # round deaths
    WHO[, deaths := round(deaths, 0)]
    # merge on locations
    WHO <- merge(WHO, locations[, .(location_id, ihme_loc_id, super_region_id)], by="location_id", all.x=TRUE)
    WHO <- WHO[super_region_id %in% c(64, 31, 103) & !is.na(year_id), .(ihme_loc_id, year_id, deaths, cases, nid)]
    WHO[, source := "implied_cfr"]
    
    ### just keep 4 and 5 star countries with "well-defined" vital registration data
    cod_loc_include <- file.path(j_root, "FILEPATH/locations_four_plus_stars.csv") %>% fread
    print(paste0("dropping locations in SR 64, 31, and 103 without well-defined vital registration: ", paste(WHO[!ihme_loc_id %in% cod_loc_include$ihme_loc_id, ihme_loc_id] %>% unique, collapse=", ")))
    WHO <- WHO[ihme_loc_id %in% cod_loc_include$ihme_loc_id]
    
  }

if(!sex_age_spec_cfr){
    
  cfr <- get_bundle_version(bundle_version_id = cfr_bundle_version)

  #Keep most detailed rows (group_review == 1)
  cfr <- cfr[group_review == 1, ] 
  
  # create year id variable
  cfr[, year_id := floor((year_start + year_end) / 2)]
  cfr[, source := "extracted_cfr"]
  
  # Create cfr variable
  setnames(cfr, c("cases", "sample_size"), c("deaths", "cases"))
  cfr[, cfr := deaths / cases] 
  
  # Make copy
  all_cfr <- copy(cfr)

  if(collapsed_cfr) {
  #sum together any age specific data by year_start, year_end, nid, location_id
  all_cfr[, total_deaths := sum(deaths), by=c("nid", "year_id", "location_id", "cv_hospital", "cv_outbreak", "rural")]
  all_cfr[, total_cases := sum(cases), by=c("nid", "year_id", "location_id", "cv_hospital", "cv_outbreak", "rural")]
  all_cfr[, collapsed_age_start := min(age_start), by=c("nid", "year_id", "location_id", "cv_hospital", "cv_outbreak", "rural")]
  all_cfr[, collapsed_age_end := max(age_end), by=c("nid", "year_id", "location_id", "cv_hospital", "cv_outbreak", "rural")]
  
  #fix column names
  all_cfr <- unique(all_cfr, by=c("nid", "year_id", "location_id", "cv_hospital", "cv_outbreak", "rural", "total_deaths", "total_cases", "collapsed_age_start", "collapsed_age_end"))
  all_cfr[, c("age_start", "age_end", "deaths", "cases") := NULL]
  setnames(all_cfr, c("total_deaths", "total_cases", "collapsed_age_start", "collapsed_age_end"), c("deaths", "cases", "age_start", "age_end"))
  }  

  # Do you want to drop population level data from non-trusted locations?
  if (pop_level_lit_cfr == FALSE){
  
  # Define locations to drip
  locs_id_to_keep <- c(data_rich$location_id, sr_64_31_103)
  locs_to_drop <- all_cfr$location_id[!all_cfr$location_id %in% locs_id_to_keep] %>% unique()
  ihme_locs_to_drop <- locations$ihme_loc_id[locations$location_id %in% locs_to_drop]
  
  print(paste0("Outliering population level data from any location without trusted notifs (outside of SR 64, 31, and 103) or without well-defined vital registration (non DR locs): ",
               paste(all_cfr[(cv_population_revised == 0  & ihme_loc_id %in% ihme_locs_to_drop), ihme_loc_id] %>% unique, collapse=", ")))
  
  all_cfr <- all_cfr[(cv_population_revised == 0  & ihme_loc_id %in% ihme_locs_to_drop), is_outlier := 1] # 
  
  }

  # Keep unique rows from desired columns for regression
  all_cfr <- unique(all_cfr[, .(nid, location_id, ihme_loc_id, year_id, age_start, age_end, cases, deaths, cv_hospital_revised, cv_outbreak, rural, source, cv_population_revised, is_outlier)])
  
  # Remove pre 1980 studies
  all_cfr <- all_cfr[year_id >= 1980, ] 
  
  # remove all outliers
  all_cfr <- all_cfr[is_outlier != 1, ] 
  }

  ### prep covariates for regression
  covariates <- prep_covariates()
  
  # Include VR data in CFR regression? 
  if(include_VR_data_in_CFR){
    
    add_cod <- prep_cod()
    all_cfr <- rbind(all_cfr, add_cod, fill=TRUE) 
    
  } else {
    
    all_cfr <- copy(all_cfr)
    
  }

  # Merge CFR data with covariates
  all_cfr <- merge(all_cfr, covariates, by=c("location_id", "year_id"), all.x=TRUE)
  all_cfr[, ihme_loc_id := as.factor(ihme_loc_id)]
  all_cfr <- all_cfr[location_id %in% standard_locations]

#***********************************************************************************************************************

#----MODEL CFR----------------------------------------------------------------------------------------------------------
### set theta for neg bin cfr model using output from GBD2015 
theta <- exp(1 / -.3633593) 

### Set model covariates
if (which_covariate=="only_HAQI") covar1 <- "HAQI"
if (which_covariate=="only_SDI") covar1 <- "SDI"
if (which_covariate=="only_LDI") covar1 <- "LDI"
if (which_covariate=="only_ln_LDI") covar1 <- "ln_LDI"
if (which_covariate=="use_HAQI") { covar1 <- "ln_mal"; covar2 <- "HAQI" }
if (which_covariate=="use_LDI") { covar1 <- "HAQI"; covar2 <- "ln_LDI" }

## run cfr model
if(cfr_model_type=="negbin"){
   
  if(cfr_use_re == T){ 
  # random effects
  res <- as.formula(~ 1 | ihme_loc_id)
  
  # formula
  if (exists("covar2")) formula <- paste0("deaths ~ ", covar1, " + ", covar2, " + cv_hospital + cv_outbreak + rural + offset(log(cases))") else
    formula <- paste0("deaths ~ ", covar1, " + cv_hospital_revised + cv_outbreak + rural + offset(log(cases))")
  
  # weights and model
  if (cfr_weights== "cases") {
    all_cfr[, weight := cases]
    
    ### save regression input
    fwrite(all_cfr, file.path(FILEPATH.inputs, "cfr_regression_input.csv"), row.names=FALSE)
    
    cfr_model <- MASS::glmmPQL(as.formula(formula),
                               random=res,
                               family=negative.binomial(theta=theta, link=log),
                               data=all_cfr,
                               weights=weight)
    
  } else if (cfr_weights=="median"){
    # For population level rows going in to model, weight is median cases in non population level rows.
    all_cfr[population == 1, weight := median(all_cfr[population !=1, cases])]
    # Non population level rows have weight equal to cases
    all_cfr[is.na(weight), weight := cases]
    
    ### save regression input
    fwrite(all_cfr, file.path(FILEPATH.inputs, "cfr_regression_input.csv"), row.names=FALSE)
    
    cfr_model <- MASS::glmmPQL(as.formula(formula),
                               random=res,
                               family=negative.binomial(theta=theta, link=log),
                               data=all_cfr,
                               weights=weight) 
    
  } else if (cfr_weights == "none") {
    
    ### save regression input
    fwrite(all_cfr, file.path(FILEPATH.inputs, "cfr_regression_input.csv"), row.names=FALSE)
    
    #no weights
    cfr_model <- MASS::glmmPQL(as.formula(formula),
                               random=res,
                               family=negative.binomial(theta=theta, link=log),
                               data=all_cfr)
  }
  
} else if (cfr_use_re == FALSE){
  
  # formula
  if (exists("covar2")) formula <- paste0("deaths ~ ", covar1, " + ", covar2, " + cv_hospital + cv_outbreak + rural + offset(log(cases))") else
    formula <- paste0("deaths ~ ", covar1, " + cv_hospital_revised + cv_outbreak + rural + offset(log(cases))")
  
  # save regression inputs
  fwrite(all_cfr, file.path(FILEPATH.inputs, "cfr_regression_input.csv"), row.names=FALSE)
  
  # Model with no random effects
  cfr_model <- MASS::glm.nb(formula, data=all_cfr)

}  

  ## calculate errors
  all_cfr[, predicted_cfr := predict(cfr_model, type="response")]
  all_cfr[, observed_cfr := deaths/cases]
  rmse <- weighted.rmse(actual = all_cfr$observed_cfr, predicted = all_cfr$predicted_cfr, weight = all_cfr$cases)
  mae <- weighted.mae(actual = all_cfr$observed_cfr, predicted = all_cfr$predicted_cfr, weight= all_cfr$cases)
  fwrite(data.table(wmae = mae, wrmse = rmse), file= file.path(FILEPATH.logs, "cfr_model_error.csv"))
  
  ### save log
  saveRDS(cfr_model, file.path(FILEPATH.logs, "cfr_model_menegbin.rds"))
  capture.output(summary(cfr_model), file=file.path(FILEPATH.logs, "log_cfr_menegbin_summary.txt"), type="output")
  #***********************************************************************************************************************
  
  #----DRAWS--------------------------------------------------------------------------------------------------------------
  set.seed(0311)
  
  ### Merge data with covariates and locations
  pred_CFR <- merge(covariates, sum_pop_young, by=c("location_id", "year_id"), all.x=TRUE)
  pred_CFR <- merge(pred_CFR, locations[, .(location_id, ihme_loc_id, super_region_id, parent_id, location_type, level)],
                    by="location_id", all.x=TRUE)
  
  # Calculate draws
  N <- nrow(pred_CFR)
  
  if(cfr_use_re == T){
    
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
  
  ### Create coefficient and covariance matrix
  coefmat <- c(fixef(cfr_model)) 
  names(coefmat)[1] <- "constant"
  names(coefmat) <- paste("b", names(coefmat), sep = "_")
  coefmat <- matrix(unlist(coefmat), ncol=length(coefmat), byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coefmat)))))
  if (!exists("covar2")) length <- 2 else if (exists("covar2")) length <- 3
  coefmat <- coefmat[1, 1:length]
  
  } else if (cfr_use_re == FALSE) {
    
  coefmat <- c(coef(cfr_model)) 
  names(coefmat)[1] <- "constant"
  names(coefmat) <- paste("b", names(coefmat), sep = "_")
  coefmat <- matrix(unlist(coefmat), ncol=length(coefmat), byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coefmat)))))
  if (!exists("covar2")) length <- 2 else if (exists("covar2")) length <- 3
  coefmat <- coefmat[1, 1:length]
  
  }
  
  # covariance matrix
  vcovmat <- vcov(cfr_model) # $cond #(add this if TMB model)
  vcovmat <- vcovmat[1:length, 1:length]
  
  # create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
  betadraws <- rmvnorm(n=1000, mean=coefmat, sigma=vcovmat)
  betas <- t(betadraws)
  
  ### Get samples 
  cfr_draw_cols <- paste0("cfr_draw_", draw_nums_gbd)
  
  # generate draws of the prediction using coefficient draws
  
  if(cfr_use_re == T){
  if (exists("covar2")) { pred_CFR[, (cfr_draw_cols) := lapply(draw_nums_gbd, function(i) exp( betas[1, (i + 1)] +
                                                                                                 ( betas[2, (i + 1)] * get(covar1) ) +
                                                                                                 ( betas[3, (i + 1)] * get(covar2) ) +
                                                                                                 RE_loc ) )] }
  if (!exists("covar2")) { pred_CFR[, (cfr_draw_cols) := lapply(draw_nums_gbd, function(i) exp( betas[1, (i + 1)] +
                                                                                                  ( betas[2, (i + 1)] * get(covar1) ) +
                                                                                                  #zi[(i + 1)]  +
                                                                                                  #fixef(cfr_model)$zi +
                                                                                                  RE_loc ) )] }
  
  } else if (cfr_use_re == F) {
  #without RE model
  if (!exists("covar2")) { pred_CFR[, (cfr_draw_cols) := lapply(draw_nums_gbd, function(i) exp( betas[1, (i + 1)] +
                                                                                                  ( betas[2, (i + 1)] * get(covar1) )
                                                                                                  #zi[(i + 1)]  +
                                                                                                  #fixef(cfr_model)$zi +
                                                                                                  # RE_loc
                                                                                                ))] }

  
  ### save results
  CFR_draws_save <- pred_CFR[, c("location_id", "year_id", cfr_draw_cols), with=FALSE]
  if (WRITE_FILES == "yes") {
    fwrite(CFR_draws_save, file.path(FILEPATH, paste0("05_cfr_model_draws.csv")), row.names=FALSE)
  }

  
} else if (cfr_model_type=="mrbrt"){
  
  all_cfr[, cfr := deaths/cases]
  
  #offset any 0 cfr so it doesn't get dropped from logit model
  all_cfr[cfr==0, cfr := .00001]
  
  #calc standard error as epi uploader does
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
  fwrite(all_cfr, file.path(FILEPATH.inputs, "cfr_regression_input.csv"), row.names=FALSE)
  
  #prep input for mrbrt
  dt2 <- all_cfr[,c("ldiff", "ldiff_se","location_id", "rural", "cv_outbreak", "cv_hospital", "SDI", "nid", "cases", "deaths")] #when accidentally was using as.data.table here (even though dt2 is already a dt, rows were getting added?? WHY?)
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
  
  ## RUN MRBRT! For some documentation run: py_help(LinearCovModel), py_help(MRBRT)
  mod1 <- MRBRT(data = dat1,
                inlier_pct=inlier_pct, # 5 pct outliering
                cov_models = list(
                  LinearCovModel("intercept", use_re = TRUE), #random effect on location_id
                  LinearCovModel("rural"),
                  LinearCovModel("outbreak"),
                  LinearCovModel("hospital"),
                  LinearCovModel("SDI"))
  )
  
  
  mod1$fit_model(inner_print_level = 5L, inner_max_iter = 100L)
  
  # pull out and save betans and location_id random effect
  re <- mod1$re_soln
  df_re <- data.frame(location_id=as.integer(names(mod1$re_soln)), re=unlist(re))
  fwrite(df_re, file=file.path(FILEPATH.logs, "mrbrt_res_by_loc.csv"))
  betas <- data.frame(covariate= mod1$cov_names, betas = mod1$beta_soln)
  fwrite(betas, file=file.path(FILEPATH.logs, "mrbrt_betas.csv"))
  
  ## save model object
  py_save_object(object = mod1, filename = file.path(FILEPATH.logs, "fit1.pkl"), pickle = "dill")
  
  #get beta and gamma samples
  n_samples <- 1000L
  samples1 <- mod1$sample_soln(sample_size = n_samples)
  
  ##calculate error
  dt2$predicted <- mod1$predict(data=dat1, predict_for_study = TRUE, sort_by_data_id=TRUE)
  rmse_mrbrt <- weighted.rmse(actual = invlogit(dt2$ldiff), predicted = invlogit(dt2$predicted), weight = dt2$cases)
  mae_mrbrt <- weighted.mae(actual = invlogit(dt2$ldiff), predicted = invlogit(dt2$predicted), weight= dt2$cases)
  fwrite(data.table(wmae_mrbrt = mae_mrbrt, wrmse_mrbrt = rmse_mrbrt), file= file.path(FILEPATH.logs, "cfr_model_error.csv"))
  
  ## make and save diagnostic plots of fit
  df_sim1_tmp <- dat1$to_df()
  df_sim1_tmp$not_trimmed <- as.factor(mod1$w_soln) # 0 if trimmed, 1 if not
  df_sim1_tmp$pred_re <- mod1$predict(data = dat1, predict_for_study = TRUE)
  df_sim1_tmp$pred_no_re <- mod1$predict(data = dat1, predict_for_study = FALSE)
  
  #add ihme_loc_id and superregion and save
  setnames(df_sim1_tmp, "study_id", "location_id")
  df_sim1_tmp <- merge(df_sim1_tmp, locations[,c("location_id", "ihme_loc_id", "super_region_name")], by="location_id")
  df_sim1_tmp <- as.data.table(df_sim1_tmp)
  fwrite(df_sim1_tmp, file=file.path(FILEPATH.logs, "mrbrt_input_trimmed.csv"))
  
  #plot observed vs predicted with color for trimming
  pdf(file = file.path(FILEPATH.logs, "measlescfr_mrbrt_diagnostic_plots.pdf"), 
      height =6,
      width = 15, 
      onefile = TRUE)
  
  print(ggplot(data=df_sim1_tmp) + 
          geom_point(aes(x=invlogit(obs), 
                         y=invlogit(pred_re), 
                         color=super_region_name), 
                     shape=df_sim1_tmp$not_trimmed) + 
          labs(y="predicted cfr", x="observed cfr") +
          ggtitle(label=paste0("Measles CFR MR-BRT observed vs predicted \n inlier_pct = ", inlier_pct)) 
        + coord_equal() + geom_abline(slope=1, intercept=0, color="black"))
  
  #residual vs fitted plot
  df_sim1_tmp$residual <- df_sim1_tmp$pred_re-df_sim1_tmp$obs
  print(plot(df_sim1_tmp$residual ~ df_sim1_tmp$pred_re, xlab="fitted", ylab="residual", main=paste0("Residual vs fitted; inlier_pct = ", inlier_pct)) + abline(0,0))
  
  dev.off()
  
  ## PREDICT OUT
  length <- nrow(covariates)
  # assume covariates are 0 when predicting out
  prediction_frame <- data.frame(rural = rep(0, length), outbreak = rep(0, length), hospital = rep(0,length), 
                                 SDI = covariates$SDI, location_id = as.character(covariates$location_id))
  
  ## make prediction data frame
  data_pred <- MRData()
  data_pred$load_df(data=prediction_frame,
                    col_covs=list("hospital", "rural", "outbreak", "SDI"), col_study_id = "location_id")
  
  ## get point predictions using fixed and random effect, convert out of logit scale
  prediction_frame_temp<- data_pred$to_df()
  prediction_frame_temp$pred_incl_re <- mod1$predict(data=data_pred, predict_for_study = TRUE)
  prediction_frame_temp$pred_no_re <- mod1$predict(data=data_pred, predict_for_study = FALSE)
  prediction_frame_temp$linear_pred_re <- invlogit(prediction_frame_temp$pred_incl_re)
  prediction_frame_temp$linear_pred_no_re <- invlogit(prediction_frame_temp$pred_no_re)
  
  ## GET DRAWS USING FIXED AND RANDOM EFFECT with NO sharing of random effect over space!
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
  fwrite(final_draws_w_re, file=paste0(FILEPATH.logs, "mrbrt_preds_summarized.csv"))
  ## merge on draws
  final_draws_w_re <- cbind(final_draws_w_re, linear_draws)
  
  ### save draws
  CFR_draws_save <- final_draws_w_re[, c("location_id", "year_id", cfr_draw_cols), with=FALSE]
  if (WRITE_FILES == "yes") {
    fwrite(CFR_draws_save, file.path(FILEPATH, paste0("05_cfr_model_draws.csv")), row.names=FALSE)
  }
}
}

########################################################################################################################
##### PART SEVEN: DEATHS ###############################################################################################
########################################################################################################################

#----CALCULATE DEATHS---------------------------------------------------------------------------------------------------

if(CALCULATE_NONFATAL == "no"){ 

rescaled_cases <- fread(paste0("/FILEPATH", "/03_case_predictions_rescaled.csv"))
}else{
  print("Rescaled cases already in global environment")
}

# merge rescaled cases with cfr draws
predictions_deaths <- merge(rescaled_cases, CFR_draws_save, by=c("location_id", "year_id"))

### calculate deaths from cases and CFR
death_draw_cols <- paste0("death_draw_", draw_nums_gbd)
predictions_deaths[, (death_draw_cols) := lapply(draw_nums_gbd, function(ii) get(paste0("cfr_draw_", ii)) * get(paste0("case_draw_", ii)) )]

### save results
predictions_deaths_save <- predictions_deaths[, c("location_id", "year_id", death_draw_cols), with=FALSE]
if (WRITE_FILES == "yes") {
  fwrite(predictions_deaths_save, file.path(FILEPATH, "06_death_predictions_from_model.csv"), row.names=FALSE)
}

#***********************************************************************************************************************

########################################################################################################################
##### PART EIGHT: RESCALE DEATHS #######################################################################################
########################################################################################################################

#----RESCALE------------------------------------------------------------------------------------------------------------
### run custom rescale function
invisible(lapply(death_draw_cols , function(x) predictions_deaths_save[location_id %in% locations[level > 3, location_id] & get(x)==0, (x) := 1e-10]) )
rescaled_deaths <- rake(predictions_deaths_save, measure="death_draw_")

### save results
if (WRITE_FILES == "yes") {
  fwrite(rescaled_deaths, file.path(FILEPATH, "07_death_predictions_rescaled.csv"), row.names=FALSE)
  message("rescaled deaths saved")
}
#***********************************************************************************************************************

########################################################################################################################
##### PART NINE: SPLIT DEATHS ##########################################################################################
########################################################################################################################

#----SPLIT--------------------------------------------------------------------------------------------------------------
### split using sex and age pattern from cause of death data
split_deaths <- age_sex_split(cause_id = cause_id, input_file=rescaled_deaths, measure="death")
#***********************************************************************************************************************

#----SAVE---------------------------------------------------------------------------------------------------------------
### save split draws
split_deaths_save <- split_deaths[, c("location_id", "year_id", "age_group_id", "sex_id", paste0("split_death_draw_", draw_nums_gbd)), with=FALSE]
rm(split_deaths)

gc(T)


if (WRITE_FILES == "yes") {
  fwrite(split_deaths_save, file.path(FILEPATH, "08_death_predictions_split.csv"), row.names=FALSE)
message("split deaths saved")
}

#***********************************************************************************************************************

# Read in file when just uploading for final envelope
# GBD 2023 UPLOAD WITH FINAL ENVELOPE
# split_deaths_save <- fread("/FILEPATH/08_death_predictions_split.csv")

########################################################################################################################
##### PART TEN: COMBINE CODEm DEATHS ##################################################################################
########################################################################################################################


#----COMBINE------------------------------------------------------------------------------------------------------------
### read CODEm COD results for data-rich countries
pandas <- reticulate::import("pandas") 
cod_M <- data.table(pandas$read_hdf(file.path("FILEPATH/deaths_", "male", ".h5")), key="data")
cod_F <- data.table(pandas$read_hdf(file.path("FILEPATH/deaths_", "female", ".h5")), key="data")

# save model version
cat(paste0("Data-rich CODEm feeder model (males) - model version ", male_CODEm_version),
    file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
cat(paste0("Data-rich CODEm feeder model (females) - model version ", female_CODEm_version),
    file=file.path(FILEPATH.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

# combine M/F CODEm results
cod_DR <- rbind(cod_M, cod_F, fill=TRUE)
rm(cod_M)
rm(cod_F)
gc(T)

cod_DR <- cod_DR[, draw_cols_upload, with=FALSE]

# use model for USA and MEX
MEX_USA_subnats <- locations[parent_id %in% c(102, 130), location_id]
cod_DR <- cod_DR[!location_id %in% c(102, 130, MEX_USA_subnats), ]

# Create a data rich frame with modeled ages
split_deaths_dr <- cod_DR[age_group_id %in% c(age_start:age_end, 389, 238, 34), ]

# Remove data rich locations from modelled estimates
split_deaths_glb <- split_deaths_save[!location_id %in% split_deaths_dr$location_id, ]

# Add column names and bind data
colnames(split_deaths_glb) <- draw_cols_upload

# Bind global and datarich estimates
split_deaths_hyb <- rbind(split_deaths_glb, split_deaths_dr)
split_deaths_hyb <- split_deaths_hyb[age_group_id %in% c(age_start:age_end, 389, 238, 34), ] 
#***********************************************************************************************************************

########################################################################################################################
##### PART TEN POINT FIVE (NEW Step 4 GBD 2019): FORCE DEATHS TO 0 WHERE TRUSTED CASE NOTIFS 0 #########################
########################################################################################################################


#----FORCE CODEM OR CALCULATED DEATHS TO 0------------------------------------------------------------------------------
# # Read in the smoothed, trusted adjusted case notifications 

# Get the smoothed notifications to align 0 cases to 0 deaths
if(CALCULATE_NONFATAL == "no"){
  adjusted_notifications_q <- fread(paste0("/FILEPATH", "/smoothed_notifs.csv"))
}

setnames(adjusted_notifications_q, "combined", "cases")
adjusted_notifications_q <- adjusted_notifications_q[, .(location_id, year_id, cases)]

# subset down to trusted case notification super region locations
trust_locs <- adjusted_notifications_q[location_id %in% unique(locations[super_region_id %in% c(31, 64, 103), location_id])]

## if there are GBD subnationals in these regions without append them on and assign parent cases number...
trust_locs <- merge(trust_locs, locations[, c("location_id", "ihme_loc_id", "parent_id", "level")], all.x=TRUE)

# Subset to just location-years where cases are 0 -- these are locations where we are forcing deaths to 0
zero_cases <- trust_locs[cases==0, ]

## merge hybridized deaths with  zero cases (gets applied to all ages/sexes within location-year)
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

# Remove extra columns
split_deaths_hyb <- split_deaths_hyb[, c("level", "parent_id", "ihme_loc_id") := NULL]

# # Data check: make sure there are no NAs in your data
if(sum(is.na(split_deaths_hyb) > 0)){
  message("There are NAs in your data! STOP!")
  stop()
}else{
  print("There are no NAs in your data, continue with save.")
}

#save split_deaths_hyb
if (WRITE_FILES == "yes") {
  fwrite(split_deaths_hyb, file.path(FILEPATH, "09_hybridized_split_deaths.csv"), row.names=FALSE)
  message("Hybridized split deaths saved")
}

### save to share directory for upload
lapply(unique(split_deaths_hyb$location_id), function(x) write.csv(split_deaths_hyb[location_id==x, ],
                                                                   file.path(cl.death.dir, paste0(x, ".csv")), row.names=FALSE))
print(paste0("death draws saved in ", cl.death.dir))

### save_results for slurm
job <- paste0("FILEPATH", 
              paste0("/FILEPATH/save_results_wrapper.r"),
              " --year_ids ", paste(1980:year_end, collapse=","),
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


