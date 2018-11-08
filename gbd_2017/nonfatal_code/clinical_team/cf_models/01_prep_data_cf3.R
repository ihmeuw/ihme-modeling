
rm(list = ls())

source('filepath')
source('filepath')
source('filepath')
source('filepath')
source('filepath')

library(readstata13)
library(ggplot2)
library(tidyr)
library(lme4)
library(tibble)
library(plyr)
library(RMySQL)
library(readr)


## Background information
locs <- get_location_metadata(location_set_id = 35, gbd_round = 5)
regions <- locs[, c('location_id', 'region_id', 'super_region_id')]
locs <- locs[, c('location_id', 'location_name')]

ages <- fread('filepath')
ages$V1 <- NULL


## Outpatient
env_df <- get_model_results()

env_df <- env_df[, c('location_id', 'year_id', 'age_group_id', 'sex_id', 'mean')]
env_df <- merge(env_df, ages, by = 'age_group_id', all.x = TRUE)
colnames(env_df)[5] <- 'envelope_mean'
env_df <- env_df[age_group_id == 164, age_start := 0]
env_df <- env_df[age_group_id == 164, age_end := 0]
env_df[age_group_id == 235, age_start := 95]
env_df[age_group_id == 235, age_end := 99]

ip_env <- get_model_results()

ip_env <- ip_env[, c('location_id', 'year_id', 'age_group_id', 'sex_id', 'mean')]
ip_env <- merge(ip_env, ages, by = 'age_group_id', all.x = TRUE)
colnames(ip_env)[5] <- 'envelope_mean'
ip_env <- ip_env[age_group_id == 164, age_start := 0]
ip_env <- ip_env[age_group_id == 164, age_end := 0]
ip_env[age_group_id == 235, age_start := 95]
ip_env[age_group_id == 235, age_end := 99]

interp_under1_env <- function(env_df){
  env_df <- env_df[, c('location_id', 'year_id', 'age_group_id', 'sex_id', 'envelope_mean')]
  env_df <- merge(env_df, ages, by = 'age_group_id', all.x = TRUE)
  colnames(env_df)[5] <- 'envelope_mean'
  env_df <- env_df[age_group_id == 164, age_start := 0]
  env_df <- env_df[age_group_id == 164, age_end := 0]
  
  under_one_env <- env_df[(env_df$age_group_id == 2) |
                            (env_df$age_group_id == 3) |
                            (env_df$age_group_id == 4)]
  # get years that are present in under one envelope
  years <- unique(under_one_env$year_id)
  # get location_ids that are present in the envelope
  loc_list <- unique(under_one_env$location_id)
  # get population data for under one years old age groups
  under_one_pop <- get_population(age_group_id=c(2:4), location_id=loc_list,
                                  sex_id=c(1,2), year_id=years)
  # under_one_pop = get_population(age_group_id=[2, 3, 4], location_id=-1,
  #                                sex_id=[1, 2], year_id=years)
  # Merge population data onto under one envelope
  under_one_env <- merge(under_one_env, under_one_pop, by = c('location_id', 'sex_id', 'age_group_id', 'year_id'))
  under_one_env$envelope_mean <- under_one_env$envelope_mean*under_one_env$population
  
  ## aggregate to just under_one age group
  under_one_env <- under_one_env[, lapply(.SD, sum), by = c('location_id', 'sex_id', 'year_id'),
                                 .SD = c('envelope_mean', 'population')]
  ## divide by pop to get actual value
  under_one_env$envelope_mean <- under_one_env$envelope_mean/under_one_env$population
  
  ## ad age group id
  under_one_env$age_group_id <- 28
  under_one_env$age_start <- 0
  under_one_env$age_end <- 1
  under_one_env$population <- NULL
  
  ## add back to envelope and replace age_group_ids 2, 3, 4
  env_df <- env_df[age_group_id != 2 & age_group_id != 3 & age_group_id != 4]
  env_df <- rbind(env_df, under_one_env)
}
env_df <- interp_under1_env(env_df)
ip_env <- interp_under1_env(ip_env)
setnames(ip_env, 'envelope_mean', 'ip_envelope')

env_df <- merge(env_df, ip_env, by = c('age_group_id', 'location_id', 'year_id', 'sex_id', 'age_start', 'age_end'))

#################### HAQI and SDI and outpatient cost ##########################
## get haqi covariate estimates:
## haqi is subnational so that's good
haqi <- get_covariate_estimates(1099)
sdi <- get_covariate_estimates(881)

haqi <- haqi[, c('location_id', 'year_id', 'mean_value')]
sdi <- sdi[, c('location_id', 'year_id', 'mean_value')]

colnames(haqi)[3] <- 'haqi_mean'
colnames(sdi)[3] <- 'sdi_mean'

## Outpatient cost
ldi <- get_covariate_estimates(57)
ldi <- ldi[, c('location_id', 'year_id', 'mean_value')]
op_cost <- fread('filepath')
op_cost <- op_cost[, c('location_id', 'year_id', 'op_cost')]
op_cost[year_id == 2016, year_id := 2017]

cost_cov <- merge(ldi, op_cost, by = c('location_id', 'year_id'))
cost_cov$cost_over_ldi <- cost_cov$op_cost/cost_cov$mean_value

## Merge outpatient, haqi, sdi, cost_cov taogether
data <- merge(haqi, sdi, by = c('location_id', 'year_id'))
## This merge loses subnational data for covariates
data <- merge(data, cost_cov, by = c('location_id', 'year_id'))
## merge envelope on (envelope from dismod)
data <- merge(data, env_df, by = c('location_id', 'year_id'))
colnames(data)[5] <- 'ldi'

data <- as.data.table(data)
## We don't have birth cf's at this age group, just 0-1
data <- data[age_group_id != 164]
data <-  data[age_group_id != 33]
data[age_group_id == 235, age_start := 95]
data[age_group_id == 235, age_end := 99]


nat <- fread('filepath')
nat <- nat[, c('sex', 'age_start', 'bundle_id', 'indv_cf', 'incidence', 'prevalence')]
colnames(nat)[1] <- 'sex_id'

usa <- data[location_id == 102]
usa[age_group_id == 235, age_start := 95][age_group_id == 235, age_end := 99]
model <- merge(nat, usa, by = c('sex_id', 'age_start'), allow.cartesian = T)


model <- model[year_id == 2010]

#############################################################################################
## Taiwan
taiwan <- fread("filepath") ## Prep uses this one
taiwan <- taiwan[, c('bundle_id', 'sex', 'age_ihmec', 'correction1', 'correction2', 'correction3')]
setnames(taiwan, c('correction1', 'correction2', 'correction3'), c('indv_cf', 'incidence', 'prevalence'))
taiwan <- as.data.table(taiwan)
colnames(taiwan)[3] <- 'age_end'
taiwan$age_end <- as.numeric(taiwan$age_end)
colnames(taiwan)[2] <- 'sex_id'
taiwan <- taiwan[age_end == 0, age_end := 1][age_end == 95, age_end := 99]
taiwan$bundle_id <- as.character(taiwan$bundle_id)
taiwan <- taiwan[bundle_id > 0]
taiwan$bundle_id <- as.numeric(taiwan$bundle_id)


## Taiwan envelope
tai_env <- fread('filepath')
colnames(tai_env)[1] <- 'op_envelope'

## re-put on envelope to covariate data
taiwan_data <- data[location_id == 8]
taiwan_data$envelope_mean <- NULL

taiwan_data <- merge(taiwan_data, tai_env, by = c('year_id', 'sex_id', 'age_start', 'age_end', 'location_id'))

## Merge with CF3
taiwan$bundle_id <- as.character(taiwan$bundle_id)
taiwan <- taiwan[bundle_id > 0]
taiwan$bundle_id <- as.numeric(taiwan$bundle_id)

taiwan[age_end == 0.99, age_end := 1]

test <- merge(taiwan, taiwan_data, by = c('sex_id', 'age_end'))
## This is ready to be modeled at national level
setnames(model, 'envelope_mean', 'op_envelope')
prep <- rbind(test, model)
prep[location_id == 8 & sex_id == 1, source := 'taiwan men']
prep[location_id == 8 & sex_id == 2, source := 'taiwan women']
prep[location_id != 8 & sex_id == 1, source := 'USA men']
prep[location_id != 8 & sex_id == 2, source := 'USA women']
prep[, year_id := 2010]


## Get bundle names
buns <- loadBundles(unique(prep$bundle_id))
buns <- buns[, c('bundle_id', 'bundle_name')]
prep <- merge(prep, buns, by = 'bundle_id')

## get rid of NAs
prep <- prep[!is.na(prep$prevalence), ]

## Write to csv. This is data for claims
write_csv(prep, 'filepath') ## Just download for the model
