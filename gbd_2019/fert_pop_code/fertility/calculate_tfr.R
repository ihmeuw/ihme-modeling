##############################
## Purpose: Aggregate to TFR
##############################
sessionInfo()
rm(list=ls())


## libraries
library(data.table)
library(readr)
library(haven)

if (interactive()){
  username <- USERNAME
  root <- FILEPATH
  version <-
  loc <-
  gbd_year <-
  year_start <-
  year_end <-
} else {
  task_id <- as.integer(Sys.getenv('SGE_TASK_ID'))
  username <- USERNAME
  param <- fread(FILEPATH)
  root <- FILEPATH
  version <- param[task_id, version]
  loc <- param[task_id, model_locs]
  gbd_year <- param[task_id, gbd_year]
  year_start <- param[task_id, year_start]
  year_end <- param[task_id, year_end]
}


## set directories
fert_function_dir <- FILEPATH

base_dir <- FILEPATH
jbase <- FILEPATH
j_data_dir <- FILEPATH
j_results_dir <- FILEPATH
final_results_dir <- FILEPATH

## functions

## set up ids for assertion checks
birth_id_vars <- list(ihme_loc_id = loc,
                      year_id = year_start:year_end,
                      sex_id = 1:3, draw = 0:999)
mean_birth_id_vars <- list(ihme_loc_id = loc, year_id = year_start:year_end, sex_id = 1:3)
mean_tfr_id_vars <- list(ihme_loc_id = loc, year = (year_start+0.5):(year_end+0.5))
tfr_id_vars <- list(ihme_loc_id = loc,  year = (year_start+0.5):(year_end+0.5), sim = 0:999)
srb_1_id_vars <- list(ihme_loc_id = loc, year_id = seq(year_start, year_end, 1))


## set model locs, get loc and age maps
model_locs <- data.table(get_locations(level='estimate', gbd_year = gbd_year))
model_locs <- model_locs$ihme_loc_id

all_locs <- data.table(get_locations(level='all', gbd_year = gbd_year))
sdi_locs <- data.table(get_locations(gbd_type = 'sdi', gbd_year = 2017))
sdi_locs <- sdi_locs[is_estimate != 1]
agg_locs <- all_locs[is_estimate != 1]
agg_loc_ids <- c(agg_locs$location_id, sdi_locs$location_id)

age_map <- data.table(get_age_map())[,.(age_group_id, age_group_name_short)]
setnames(age_map, 'age_group_name_short', 'age')

loc_map <- data.table(get_locations(level='all'))[,.(ihme_loc_id, location_id)]
sdi_map <- data.table(get_locations(gbd_type='sdi', gbd_year = 2017))[,.(ihme_loc_id, location_id, is_estimate)]
sdi_map <- sdi_map[is_estimate != 1][,is_estimate := NULL]
loc_map <- rbind(loc_map, sdi_map)

## read in ASFR data

missing_files <- c()
asfr <- list()
for(model_age in seq(10, 50, 5)){
  cat(paste0(model_age, '\n')); flush.console()
  file <- paste0(final_results_dir,'age_', model_age, '/gpr_', loc, '_', model_age, '_sim.csv')
  if(file.exists(file)){
    asfr[[paste0(model_age)]] <- fread(file)
    asfr[[paste0(model_age)]]$V1 <- NULL
    asfr[[paste0(model_age)]]$age <- model_age
  } else {
    missing_files <- c(missing_files, file)
  }
}
if(length(missing_files) >0 ) stop('missing GPR files')
asfr <- rbindlist(asfr, use.names = T)

## reading in pop
pop <- fread(paste0(jbase, '/pop_input.csv'))

##############
## TFR
#############

tfr_draws <- copy(asfr)
tfr_draws <- tfr_draws[,.(val = sum(val)), by = c('ihme_loc_id', 'year', 'sim')]
tfr_draws[,val := val * 5]

## get mean level tfr
tfr_sum <- copy(tfr_draws)
tfr_sum <- tfr_sum[,.(mean = mean(val), lower = quantile(val, 0.025), upper = quantile(val, 0.975)),
                   by = c('ihme_loc_id', 'year')]

assertable::assert_ids(tfr_draws, id_vars = tfr_id_vars)
assertable::assert_values(tfr_draws, colnames='val', test='gte', test_val=0)

assertable::assert_ids(tfr_sum, id_vars = mean_tfr_id_vars)
assertable::assert_values(tfr_sum, colnames='val', test='gte', test_val=0)

## save
write.csv(tfr_draws, paste0(final_results_dir, 'age_tfr/gpr_', loc, '_tfr_sim.csv'), row.names = F)
write.csv(tfr_sum, paste0(final_results_dir, 'age_tfr/gpr_', loc, '_tfr.csv'), row.names = F)
