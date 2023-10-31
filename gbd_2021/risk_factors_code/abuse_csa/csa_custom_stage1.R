########################################################################################################################
# 
# Author: AUTHOR
# Purpose: Custom Stage 1 models for CSA prevalence
#
########################################################################################################################

rm(list=ls())

# set-up environment -----------------------------------------------------------------------------------------------------------------------------------------------

library(data.table)
library(dplyr)
library(openxlsx)
library(ggplot2)
library(stringr)
library(lme4)
library(crosswalk, lib.loc = "FILEPATH")
library(locfit)

source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/get_crosswalk_version.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/upload_bundle_data.R")

locs <- get_location_metadata(22)
ages <- get_age_metadata(age_group_set_id=19, gbd_round_id = 7)

# Load in data for modeling ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

mdata <- get_crosswalk_version(33290)
fdata <- get_crosswalk_version(33287)

#bind m and f together
data <- rbind(mdata, fdata, fill=T)
orig <- copy(data)

#subset to only columns we need to fit the regression
data <- data[is_outlier==0]
data <- data[, c('nid', 'location_id', 'ihme_loc_id', 'sex', 'year_id', 'age_group_id', 'val', 'variance')]
data[, sex_id:=ifelse(sex=='Female', 2, 1)]
data <- merge(data, locs[, c('location_id', 'super_region_id', 'region_id', 'level')], by=c('location_id'))

#create se before logit transform
data[, se:=sqrt(variance)]

#logit transform the data using the delta transform package
logit_vals <- as.data.table(delta_transform(mean=data$val, sd=data$se, transformation='linear_to_logit'))

#bind back onto main data table
data <- cbind(data, logit_vals)

# Linear regression
model <- lmer(formula = mean_logit ~  as.factor(age_group_id) + as.factor(sex_id) + (1 | super_region_id/region_id/location_id), 
              data = data, 
              control = lmerControl(optimizer = 'bobyqa'), 
              REML = T)

summary(model) %>% print

#create prediction data frame --------------------------------------------------------------------------------------------------------------------------------------------

stgpr_out <- fread('FILEPATH')
stgpr_out <- stgpr_out[variable=='Stage 1']
m_stgpr <- copy(stgpr_out)
stgpr_out[, c('gpr_lower', 'gpr_upper', 'variable', 'mean'):=NULL]

#merge on location information used in the lmer model (ids rather than names)
stage1 <- merge(stgpr_out, locs[, c('location_id','region_id', 'super_region_id')], 
                by='location_id', allow.cartesian = T)

#duplicate so a location/year/age value for each sex_id
stage1_fem <- copy(stage1)
stage1_fem[, `:=` (sex_id=2, sex='Females')]

#bind together
stage1 <- rbind(stage1, stage1_fem, fill=T)

#predict using the lmer model
stage1[, custom_stage1_predictions := expit(predict(model,
                                                    stage1,
                                                    re.form=NA,
                                                    allow.new.levels=TRUE))]

version <- 'global_sex_trend_NO_predict_re_bothsex_crosswalk_norperp_nosexualdebut'

#save stage1
write.csv(stage1, paste0('FILEPATH'))

setnames(stage1, 'custom_stage1_predictions', 'cv_custom_stage_1')

#write sex specific stage 1
write.csv(stage1[sex_id==1], paste0('FILEPATH', version, '_male_model_predictions.csv'))
write.csv(stage1[sex_id==2], paste0('FILEPATH', version, '_female_model_predictions.csv'))

