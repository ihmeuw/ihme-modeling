###########################################################
## Purpose: Split total births in England and Wales
###########################################################

sessionInfo()
rm(list=ls())
library(data.table)


if (interactive()){
  username <- USERNAME
  root <- FILEPATH
  version <-
  gbd_year <-
  year_start <-
  year_end <-
} else {
  username <- USERNAME
  root <- FILEPATH
  version <- commandArgs(trailingOnly = T)[1]
  gbd_year <- commandArgs(trailingOnly = T)[2]
  year_start <- commandArgs(trailingOnly = T)[3]
  year_end <- commandArgs(trailingOnly = T)[4]
}

locs <- c('GBR_4749', 'GBR_4636')
print(locs)
pvid <- as.character(Sys.Date()) %>% gsub('-', '', .)

## set directories

in_dir <- FILEPATH
asfr_dir <- FILEPATH
jbase <- FILEPATH
data_dir <- FILEPATH
j_data_dir <- FILEPATH

## read in data

historic_locs <- data.table(readRDS(paste0(in_dir, 'mpidr_reg.RDS')))
pop <- fread(paste0(jbase, 'pop_input.csv'))
input_data <- readRDS(paste0(j_data_dir, 'asfr.RDs'))

loop1asfr <- list()
for(age in seq(15,45, 5)){
  for(loc in locs){
    loop1asfr[[paste0(age, loc)]] <- fread(paste0(asfr_dir, 'age_', age, '/gpr_', loc, '_', age, '.csv'))
    loop1asfr[[paste0(age, loc)]]$age <- age
  }
}
loop1asfr <- rbindlist(loop1asfr)
loop1asfr[,year_id := floor(year)][,year:= NULL]

## getting age map
age_map <- data.table(get_age_map())[,.(age = age_group_name_short, age_group_id)]
age_map[,age := as.numeric(age)]

loc_map <- data.table(get_locations(level='all', gbd_year = gbd_year))


pop <- merge(pop, loc_map[,.(location_id, ihme_loc_id)], all.x=T, by = 'location_id')
pop <- pop[ihme_loc_id %in% locs]
pop <- merge(pop, age_map, by = 'age_group_id', all.x =T)

historic_locs <- historic_locs[ihme_loc_id == 'GBR_4749_4636']
historic_locs[,location_id := 47494636]
historic_locs <- unique(historic_locs)

split_historic_locs <- function(asfr, population, historic_loc_data, loc_map){

  ## get ratio of England and Wales births to sum
  births <- merge(asfr[,.(ihme_loc_id, year_id, age, mean)], population[,.(ihme_loc_id, year_id, age, population)],
                  by = c('ihme_loc_id', 'year_id', 'age'), all.x = T)
  births[,births := mean * population]
  births[,agg_births := sum(births), by = c('year_id', 'age')]
  births[,prop := births/agg_births]

  ## apply that ratio to the actual england + wales births to get split births
  split <- merge(births, historic_loc_data[,.(nid, source, source_type, year_id, age_group_id, age= age_start, val)],
                 by = c('year_id', 'age'))
  split[,births := val * prop]
  split[,asfr := births / population]

  split <- merge(split, loc_map[,.(ihme_loc_id, location_id)], by = 'ihme_loc_id', all.x=T)
  split[,fmeasid := 2]
  split[,pvid := pvid]

  ## calculate variance
  split[,var := (asfr * (1-asfr)) / births]


  split <- split[,.(nid, id = source, source_type,location_id, ihme_loc_id, year_id, age_group_id, fmeasid, val = asfr, var, pvid)]

  return(split)
}

## run the function to split locs
split_data <- split_historic_locs(asfr = loop1asfr, population = pop, historic_loc_data = historic_locs, loc_map = loc_map)


## save file
write.csv(split_data, paste0(data_dir, 'historic_locs/', paste0(locs, collapse = '_'), 'split_loc.csv'), row.names = F)


