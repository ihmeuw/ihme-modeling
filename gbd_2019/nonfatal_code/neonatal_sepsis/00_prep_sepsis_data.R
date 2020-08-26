##########################################################################################
### Project: Neonatal Sepsis and Other Neonatal Infections
### Purpose: Process and prep sepsis data, conduct age/sex splitting, and apply crosswalks
###########################################################################################

rm(list=ls())
os <- .Platform$OS.type

pacman::p_load(ggplot2, data.table, magrittr, dplyr, splitstackshape, msm, openxlsx, tidyr, matrixStats)

## Set options
bundle <- "neonatal_sepsis" # Which cause are you running? 
decomp_step <- "" # Current decomp step ('step1','step2','step3','step4','iterative')
prep_cfr_emr <- F # Do you need to re-prep CFR data to EMR? (T/F)
new_bundle_version <- T # Do you need a new bundle version ID to be created? (T/F)
xwalk_version_description <- "" # Description for crosswalk version upload

## Source central functions
setwd("FILEPATH")
functions <- c("get_covariate_estimates","get_location_metadata","get_bundle_data","upload_bundle_data","save_bundle_version","get_bundle_version",
               "save_crosswalk_version","get_model_results","get_demographics","get_ids","get_population","interpolate","get_draws")
for (i in functions) source(paste0(i,".R"))
  

## Source MR-BRT functions
setwd("FILEPATH")
functions <- c("run_mr_brt_function","cov_info_function","check_for_outputs_function","load_mr_brt_outputs_function","predict_mr_brt_function",
               "check_for_preds_function","load_mr_brt_preds_function","plot_mr_brt_function")
for (i in functions) source(paste0(i,".R"))


## User-defined functions
setwd("FILEPATH")
# Prep CFR to EMR and upload
source("01_prep_emr.R")
# Prep clinical data from prevalence to incidence
source("02_prep_clinical_data.R")
# Sex-Split
source("03_sex_split.R")
# Age-Split
source("04_age_split.R")
source("helpers/divide_data.R")
source("helpers/functions_agesex_split.R")
# Crosswalk
source("05_crosswalk.R")

###############
##### RUN #####
###############

## EMR prep
if (prep_cfr_emr) prep_emr(bundle, decomp_step)

## Save bundle version
if (new_bundle_version){
  # load bundle data
  bundle_version <- save_bundle_version(92, decomp_step, include_clinical=TRUE)  ## If you want to create new bundle version ID
  # write out bundle version id and save
  df <- fread("FILEPATH/bundle_versions.csv")
  df[bundle==bundle]$is_best <- 0
  dt   <- data.frame("bundle"            = bundle,
                     "bundle_version_id" = bundle_version$bundle_version_id,
                     "note"              = xwalk_version_description,
                     "is_best"           = 1)
  df <- rbind(df,dt,fill=T)
  write.csv(df,"FILEPATH/bundle_versions.csv", row.names=F)
  # get bundle version
  data <- get_bundle_version(bundle_version$bundle_version_id) 
} else {
  bundle_version <- fread("FILEPATH/bundle_versions.csv")[bundle=="neonatal_sepsis"&is_best==1] ## If you want to use existing bundle version ID
  data <- get_bundle_version(bundle_version$bundle_version_id) 
}

## Hospital data prep
data <- prep_clinical_data(data,prevalence=FALSE)

## Sex Split
data <- sex_split(data)

## Age split data
data[measure=="incidence"&age_end==0.999,age_end:=0.07671233] #setting to only split within the neonatal age range
data[measure=="incidence"&age_end==0.0762130,age_end:=0.0761233]
data <- age_split(data)

#identify crosswalk matches
df <- copy(data)
df <- prep_data(df)
df <- match_data(df)

#run MRBRT for crosswalk
df <- prep_mrbrt(df)
fit_mrbrt(df,"sepsis_combined_incidence_outliers_test")

#Apply crosswalks to data
data <- apply_mrbrt(data,"sepsis_combined_incidence_outliers_test")

## Outliers
income <- get_location_metadata(26, gbd_round_id=5)
ids <- income[region_name=="World Bank High Income"]$ihme_loc_id
locs <- fread("FILEPATH/locs.csv")[,.(location_id,ihme_loc_id)]
data$ihme_loc_id <- NULL
data <- merge(data,locs,by="location_id",all.x=T)
data[measure=="incidence" & location_id %in% c(4856,43923,43887,4862,43929,43893),is_outlier:=1]
data[measure=="incidence" & age_start==0 & mean > 5.2, is_outlier := 1]
data[measure=="incidence" & age_start==0.01917808 & mean > 1.7, is_outlier := 1]
data[, loc_id := substr(ihme_loc_id,1,3)]
data[measure=="incidence" & age_start==0 & loc_id %in% ids & mean > 0.52, is_outlier := 1]
data[measure=="incidence" & age_start==0.01917808 & mean >0.17, is_outlier := 1]
data[ihme_loc_id %like% "USA" & year_start==2015, is_outlier := 1]

## Write-out
write.xlsx(data,paste0("FILEPATH"), sheetName="extraction")

#upload split data to CW version in DB
save_crosswalk_version(bundle_version$bundle_version_id, 
                       data_filepath=paste0("FILEPATH"), 
                       description=xwalk_version_description)
