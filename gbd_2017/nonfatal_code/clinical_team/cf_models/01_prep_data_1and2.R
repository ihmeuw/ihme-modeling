
rm(list = ls())

library(data.table)
library(stringr)
library(readr)
library(magrittr)


source('filepath')
source('filepath')
source('filepath')
source('filepath')


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


ages <- fread('filepath')
ages$V1 <- NULL

data_dir <- paste0(filepath)

usa_hcup <- fread(paste0(filepath)) %>%
  setnames('sex', 'sex_id') %>%
  .[, .(sex_id, age_start, bundle_id, indv_cf, incidence)] %>%
  .[, location_id := 102]


nzl <- fread(paste0(data_dir, 'filepath')) %>%
  setnames('sex', 'sex_id') %>%
  .[, .(sex_id, age_start, bundle_id, indv_cf, incidence)] %>%
  .[, location_id := 72]

phl <- fread(paste0(data_dir, 'filepath')) %>%
  setnames('sex', 'sex_id') %>%
  .[, .(sex_id, age_start, bundle_id, indv_cf, incidence)] %>%
  .[, location_id := 16]

ms_data <- fread('filepath') %>%
  setnames('sex', 'sex_id') %>%
  .[, .(sex_id, age_start, bundle_id, indv_cf, incidence)] %>%
  .[, location_id := 102]

all_data <- rbind(usa_hcup, nzl, phl, ms_data) %>%
  setnames(c('indv_cf', 'incidence'), c('cf1', 'cf2')) %>%
  .[, year_id := 2010] %>%
  merge(ages[! age_group_id %in% c(1, 2, 21)], by = 'age_start') %>%
  .[age_start == 95, age_group_id := 235]

## Get envelope
env_df <- data_extract_function() %>%
  .[, .(location_id, year_id, age_group_id, sex_id, mean)] %>%
  setnames('mean', 'envelope_mean') %>%
  interp_under1_env() %>%
  setnames('envelope_mean', 'ip_envelope') %>%
  .[age_group_id == 235, age_start := 95] %>%
  .[age_group_id == 235, age_end := 99]

op_env <- get_model_results(data_extract_function()) %>%
  .[, .(location_id, year_id, age_group_id, sex_id, mean)] %>%
  setnames('mean', 'envelope_mean') %>%
  interp_under1_env() %>%
  setnames('envelope_mean', 'op_envelope') %>%
  .[age_group_id == 235, age_start := 95] %>%
  .[age_group_id == 235, age_end := 99]

env_df <- merge(env_df, op_env, by = c('location_id', 'year_id', 'age_group_id', 'sex_id', 'age_start', 'age_end'))


## Merge on
all_data[age_group_id == 28, age_end := 1]
cfs <- merge(all_data, env_df, by = c('location_id', 'year_id', 'age_group_id', 'sex_id', 'age_start', 'age_end'), all.x = T)

taiwan <- as.data.table(read.csv("filepath")) %>%
  .[, c('bundle_id', 'sex', 'age_ihmec', 'correction1', 'correction2')] %>%
  setnames(c('age_ihmec', 'correction1', 'correction2', 'sex'), c('age_end', 'cf1', 'cf2', 'sex_id')) 

taiwan[, age_end := as.numeric(age_end)][age_end == 0, age_end := 1][age_end == 95, age_end := 99]
taiwan$bundle_id <- as.character(taiwan$bundle_id)
taiwan <- taiwan[bundle_id > 0]
taiwan$bundle_id <- as.numeric(taiwan$bundle_id)

## Taiwan inpatient envelope
tai_env <- fread('filepath')
colnames(tai_env)[1] <- 'ip_envelope'
tai_op <- fread('filepath')
colnames(tai_op)[1] <- 'op_envelope'
tai_env <- merge(tai_env, tai_op[, c('op_envelope', 'sex_id', 'age_start', 'age_end')])

taiwan <- merge(taiwan, tai_env, by = c('sex_id', 'age_end'))
taiwan <- merge(taiwan, ages, by = c('age_start', 'age_end'), all.x = T)
taiwan[age_end == 1, age_group_id := 28]
taiwan[age_start == 95, age_group_id := 235]
taiwan$location_id <- 8
taiwan$year_id <- 2010

prep_data <- rbind(cfs, taiwan, fill = TRUE)

## Get covariates
haqi <- get_covariate_estimates(1099)
sdi <- get_covariate_estimates(881)

haqi <- haqi[, c('location_id', 'year_id', 'mean_value')]
sdi <- sdi[, c('location_id', 'year_id', 'mean_value')]

colnames(haqi)[3] <- 'haqi_mean'
colnames(sdi)[3] <- 'sdi_mean'

covs <- merge(haqi, sdi, by = c('location_id', 'year_id'))

prep_data$cf1 <- as.numeric(as.character(prep_data$cf1))
prep_data$cf2 <- as.numeric(as.character(prep_data$cf2))
prep_data <- merge(prep_data, covs, by = c('location_id', 'year_id'))

write_csv(prep_data, 'filepath')
