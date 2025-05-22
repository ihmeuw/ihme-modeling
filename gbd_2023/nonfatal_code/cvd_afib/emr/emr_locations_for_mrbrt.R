##################################
# AFIB EMR LOCATION SELECTION CODE
# Author: USERNAME
# Purpose: Choose locations for MR-BRT EMR calculation and prediction based on the following criteria: 
#         1.) Ranking of 4 or 5 stars for quality of VR data. 
#         2.) Prevalence data available from the literature included in Dismod estimation. 
#         3.) Prevalence rate >= .005
#         4.) CSMR >= .00002
# Date last updated: DATE
#################################

## Section zero: Load libraries and central functions

source("FILEPATH/get_bundle_version.R")
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_model_results.R")

library(openxlsx)
library(ggplot2)
library(plyr)
library(ggrepel)
library(ggplot2)

'%ni%' <- Negate('%in%')
date <- gsub("-", "_", Sys.Date())

## Section one: Loading/formatting/merging quality of VR data: 

loc_meta <- get_location_metadata(location_set_id = 35, release_id = 16)

## retrieve list of 4-star CoD locations
vr_stars <- data.table(read.xlsx('FILEPATH/cod_decomp4_stars.xlsx'))
vr_stars <- data.table(merge(vr_stars[,c('ihme_loc_id','stars')],loc_meta[,c('location_id','location_name','parent_id','ihme_loc_id')], by='ihme_loc_id'))

vr_stars <- vr_stars[stars >= 4] #these locations are eligable for use in MR-BRT. 

## Section two: Prevalence data from literature available - change rounds/step to reflect current modeling framework. 

afib_data <- get_bundle_data(bundle_id = 119)
afib_prev <- afib_data[measure=='prevalence']
afib_prev[grep('Truven',field_citation_value), cv_inpatient := 1] #set Truven to cv_inpatient 
afib_prev[grep('Poland',field_citation_value), cv_inpatient := 1] #set Poland NHF claims to cv_inpatient 
afib_prev <- afib_prev[cv_inpatient != 1] #remove non-reference sources - These locations are eligable for use in MR-BRT.  

## Section three: Prevalence rate >= .005 - change rounds/step to reflect current modeling framework.

afib_model <- get_model_results('epi', 1859, decomp_step='iterative', release_id = 16, status='best', 
                                age_group_id = 22, year_id = 2023, measure_id = 5)

afib_model_prev <- afib_model[mean >= .005] #only allow locations that are above this threshold.

## Section four: CSMR rate >= .00002 - get from uncorrected CODEm model. 

afib_cod_model_all <- get_model_results('cod', 500, gbd_round_id=5, age_group_id = 22, year_id = 2017, model_version_id = c('407906','407909'))
afib_cod_model <- afib_cod_model_all[mean_death_rate >= .00002]

## Find locations that meet all criteria

vr_locs <- unique(vr_stars[,location_id])
afib_data_locs <- unique(afib_prev[,location_id])
afib_model_prev_locs <- unique(afib_model_prev[,location_id])
afib_cod_model_locs  <- unique(afib_cod_model[,location_id])

candidate_locs <- Reduce(intersect, list(vr_locs,afib_data_locs,afib_model_prev_locs,afib_cod_model_locs))





