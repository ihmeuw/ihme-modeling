###########################################################
## Purpose: Split total births data into 5 year age groups
###########################################################

sessionInfo()
rm(list=ls())
library(data.table)
library(readr)


if (interactive()){
  username <- USERNAME
  root <- FILEPATH
  version <-
  loc <-
  gbd_year <-
  year_start <-
  year_end <-
} else {
  username <- USERNAME
  root <- FILEPATH
  task_id <- as.integer(Sys.getenv('SGE_TASK_ID'))
  param <- fread(FILEPATH)
  loc <- param[task_id, locs]
  version <- param[task_id, version]
  gbd_year <- param[task_id, gbd_year]
  year_start <- param[task_id, gbd_year]
  year_end <- param[task_id, gbd_year]
}

print(loc)
## set directories
birth_dir <- FILEPATH
asfr_dir <- FILEPATH
jbase <- FILEPATH
data_dir <- FILEPATH
j_data_dir <- FILEPATH

## read in data

total_births <- data.table(readRDS(paste0(birth_dir, 'total_births.RDs')))
pop <- fread(paste0(jbase, 'pop_input.csv'))
input_data <- readRDS(paste0(j_data_dir, 'asfr.RDs'))
input_data <- input_data[ihme_loc_id == loc]

loop1asfr <- list()
for(age in seq(15,45, 5)){
  loop1asfr[[paste0(age)]] <- fread(paste0(asfr_dir, 'age_', age, '/gpr_', loc, '_', age, '.csv'))
  loop1asfr[[paste0(age)]]$age <- age
}
loop1asfr <- rbindlist(loop1asfr)

## getting age map
age_map <- data.table(get_age_map())[,.(age = age_group_name_short, age_group_id)]
age_map[,age := as.numeric(age)]

loc_map <- data.table(get_locations(level='all', gbd_year = gbd_year))

## data prep/processing
total_births <- total_births[fmeasid == 4][age_group_id == 22][,.(nid, source, source_type, ihme_loc_id, year_id, birth_data = val, pvid = date_added)]
total_births <- total_births[ihme_loc_id == loc]
total_births[, year_id := as.numeric(year_id)][,birth_data := as.numeric(birth_data)]

pop[,pvid := NULL]
pop <- merge(pop, age_map, all.x = T, by = 'age_group_id')
pop <- merge(pop, loc_map[,.(ihme_loc_id, location_id)], all.x = T, by = 'location_id')

loop1asfr[,year_id := floor(year)][,year := NULL][,lower := NULL][,upper := NULL]

## need to split total births to maternal age specific births
calculate_asfr_data <- function(asfr, population, total_birth_data){
  data <- merge(asfr, population, all.x = T, by = c('age', 'year_id', 'ihme_loc_id'))

  ## first get ratio of predicted age specific births to predicted total births
  data[,mean := mean * population]

  total <- data[,.(model_total_births = sum(mean)), by = c('year_id', 'ihme_loc_id')]
  data <- merge(data, total, by = c('year_id', 'ihme_loc_id'), all=T)
  data[,age_prop := mean/model_total_births]

  ## apply ratio to total births data
  data <- merge(data, total_birth_data, by = c('ihme_loc_id', 'year_id'), all.y = T, allow.cartesian = T)
  data[,birth_data := birth_data * age_prop]

  ## now get into asfr space by dividing pop
  data[,val := birth_data/ population]

  ## calculate variance
  data[,var := (val * (1-val)) / birth_data]

  ## format to match the data being read in to model
  data[,fmeasid := 2]
  data[,update := NA]
  data <- data[,.(nid, id = source, source_type, location_id, ihme_loc_id, year_id, age_group_id, fmeasid, val, var, update, pvid)]

  return(data)
}

split_asfr <- calculate_asfr_data(asfr = loop1asfr, population = pop, total_birth_data = total_births)

## get actual completeness
if(gbd_year == 2019) runID <- 281
if(gbd_year == 2020) runID <- 290
ddm_version <- get_proc_version(model_name = 'ddm', model_type = 'estimate',
                                run_id = runID)

completeness <- get_mort_outputs(model_name = 'ddm', model_type = 'estimate', estimate_stage_ids = 11, run_id = ddm_version)
completeness[mean >= 0.95, complete := 1]
completeness[mean < 0.95, complete := 0]

split_asfr <- merge(split_asfr, completeness[source_type_id == 1, .(ihme_loc_id, year_id, complete)], by=c('ihme_loc_id', 'year_id'), all.x=T)
split_asfr[, source_type := ifelse(complete == 1, 'vr_complete_tb', 'vr_incomplete_tb')]
split_asfr[, source_type := as.character(source_type)]
split_asfr[is.na(source_type), source_type := 'vr_incomplete_tb']
split_asfr[, complete := NULL]

if(loc == 'EGY'){
  split_asfr[ihme_loc_id == 'EGY' & year_id > 2000, source_type := 'vr_complete_tb']
}
if(loc == 'CHN_44533'){
  split_asfr[ihme_loc_id == 'CHN_44533', source_type := 'vr_complete_tb']
}

if(loc == 'THA') split_asfr[ihme_loc_id == 'THA', source_type := 'vr_complete_tb']
if(loc == 'CHN_44533') split_asfr[id == 'national health statistics yearbook', source_type :='vr_complete']
if(loc == 'IND') split_asfr[id == 'migration_addition', source_type :='vr_complete_tb']


write.csv(split_asfr, paste0(data_dir, 'total_births/', loc, 'split_births.csv'), row.names = F)



