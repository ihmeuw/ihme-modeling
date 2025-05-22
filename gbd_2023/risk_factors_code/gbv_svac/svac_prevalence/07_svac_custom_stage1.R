##########################################################################################################################################
# 
# Purpose: Custom Stage 1 models for SVAC prevalence
#
##########################################################################################################################################

rm(list=ls())

#release id
rid <- 16

#crosswalk versions
csa_xwid <- 'XW ID'

#se inflation factor
inflate <- 2

#version name for saving
stage_version <- paste0('global_sex_trend_xwid_', csa_xwid, '_inflation_', inflate)


##### 0. SET UP ##########################################################################################################################

#libraries + central functions
pacman::p_load(data.table, dplyr, openxlsx, ggplot2, stringr, lme4, reticulate)
invisible(sapply(list.files("FILEPATH", full.names = T), source))
reticulate::use_python("FILEPATH")
library(crosswalk002)
cw <- import("crosswalk")
library(locfit)

#metadata
locs <- get_location_metadata(22, release_id = rid)
ages <- get_age_metadata(age_group_set_id = 24, release_id = rid)


##### 1. LOAD + FORMAT DATA ##############################################################################################################

#get data
data <- fread(paste0('FILEPATH/all_xw-', csa_xwid, '_predicted_inputs_inflated_by_', inflate,'.csv'))
orig <- copy(data)

#subset to only columns needed for the regression
data <- data[is_outlier==0]
data <- data[, c('nid', 'location_id', 'ihme_loc_id', 'sex', 'year_id', 'age_group_id', 'val', 'variance')]

#sex and location metadata
data[, sex_id:=ifelse(sex=='Female', 2, 1)]
data$location_id <- as.numeric(data$location_id)
data <- merge(data, locs[, c('location_id', 'super_region_id', 'region_id', 'level')], by=c('location_id'))

#create se before logit transformation
data[, se := sqrt(variance)]

#logit transform the data using the delta transform package
logit_vals <- as.data.table(delta_transform(mean=data$val, sd=data$se, transformation='linear_to_logit'))

#bind back onto main data table
data <- cbind(data, logit_vals)



##### 2. MODEL DATA ######################################################################################################################

# Linear regression: Sex indicator; Sex by region indicator; Nested region effects: (1 | level_1/level_2/level_3)

model <- lmer(formula = mean_logit ~  as.factor(age_group_id) + as.factor(sex_id) + (1 | super_region_id/region_id/location_id), 
              data = data, 
              control = lmerControl(optimizer = 'bobyqa'), 
              REML = T)

summary(model) %>% print

#save final model object
saveRDS(model, paste0('FILEPATH/custom_stage1_lmer_xwid_', csa_xwid, '_inflated_by_', inflate, '.RDS'))



##### 3. PREDICT FROM MODEL ##############################################################################################################

#create prediction data frame for every location-year-age-sex combinations
#note: for the gbd, we usually model one year past the GBD Round (eg, GBD 2022 is modeled through 2023, only for it to later be cut off)
stgpr_out <- expand.grid(location_id = locs[level >= 3]$location_id, 
                         year_id = 1980:2024,
                         age_group_id = c(7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 32, 235),
                         sex_id = c(1, 2)) %>% as.data.table()

#merge on location information used in the lmer model (ids rather than names)
stage1 <- merge(stgpr_out, locs[, c('location_id','region_id', 'super_region_id')], 
                by='location_id', allow.cartesian = T)

#fix sex
stage1[, sex := ifelse(sex_id == 2, 'Female', 'Male')]

#predict using the lmer model
stage1[, custom_stage1_predictions := expit(predict(model,
                                                    stage1,
                                                    re.form=NA,
                                                    allow.new.levels=TRUE))]

#save stage1
write.csv(stage1, paste0('FILEPATH/', stage_version, '_model_predictions.csv'), row.names = F)

#rename
setnames(stage1, 'custom_stage1_predictions', 'cv_custom_stage_1')

#drop extra info
stage1[, c('sex', 'location_name', 'region_name', 'super_region_name') := NULL]

#write sex specific stage 1
write.csv(stage1[sex_id==1], paste0('FILEPATH/', stage_version, '_male_model_predictions.csv'), row.names = F)
write.csv(stage1[sex_id==2], paste0('FILEPATH/', stage_version, '_female_model_predictions.csv'), row.names = F)



