# dismod only produces draws for estimation years, so we must interpolate
# each location will get interpolated draws for 1990 - 2017 for all age groups and sexes
# which can then be saved with the save_results function

rm(list=ls())

source('interpolate.R')
library(data.table)
library(readr)

date <- Sys.Date()
date <- gsub('([[:punct:]])|\\s+','_', date)
write_path <- FILEPATH
new_dir <- dir.create(path = write_path)

location <- commandArgs()[4]

###### Useful functions ####################################
# Not a db utility
interp_under1_env <- function(env_df){
  ages <- get_age_metadata(12, gbd_round_id = 5) %>% 
    setnames(c('age_group_years_start', 'age_group_years_end'), c('age_start', 'age_end'))
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
                                  sex_id=c(1,2), year_id=years, gbd_round_id= 5)
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
  env_df[, age_group_weight_value := NULL]
  env_df <- rbind(env_df, under_one_env)
}

### Now run ###
df <- interpolate('modelable_entity_id', gbd_id = 19797, version_id = 246617, source = 'epi', measure_id = 19, sex_id = c(1,2), 
                                            gbd_round_id= 5, reporting_year_start = 1990, location_id = location,
                                            reporting_year_end = 2017)
setnames(df, 'year_id', 'year_start')
df <- melt(df, id.vars = c('age_group_id', 'location_id', 'year_start', 'sex_id'), 
                        measure.vars = patterns('draw_')) 
df[, envelope_mean := mean(value), by = c('age_group_id', 'location_id', 'year_start', 'sex_id')] # Get mean of the draws
df[, value := NULL][, variable := NULL]
setnames(df, 'year_start', 'year_id')
df <- interp_under1_env(df)
df <- unique(df) %>% setnames('year_id', 'year_start')
df <- df[age_group_id != 164]


file_name <- paste0(write_path, location, '.csv')

write.csv(df, file = file_name, row.names = FALSE)