##############################
## Purpose: Split historic locations
## Details: Split total births into age specific information
#############################

library(data.table)
library(lme4)
library(arm)
library(assertable)
library(readr)
library(splines)
library(mortdb, lib = "FILEPATH")
library(mortcore, lib = "FILEPATH")
sessionInfo()
rm(list=ls())

if (interactive()){
  user <- Sys.getenv('USER')
  version_id <- x
  gbd_year <- x
  year_start <- x
  year_end <- x
  loc <- ''
  split_version <- x
} else {
  user <- Sys.getenv('USER')
  parser <- argparse::ArgumentParser()
  parser$add_argument('--version_id', type = 'character')
  parser$add_argument('--gbd_year', type = 'integer')
  parser$add_argument('--year_start', type = 'integer')
  parser$add_argument('--year_end', type = 'integer')
  parser$add_argument('--loc', type = 'character')
  parser$add_argument('--split_version', type = 'integer')
  args <- parser$parse_args()
  version_id <- args$version_id
  gbd_year <- args$gbd_year
  year_start <- args$year_start
  year_end <- args$year_end
  loc <- args$loc
  split_version <- args$split_version
}


locs <- c('GBR_4749', 'GBR_4636')

input_dir <- paste0("FILEPATH")
gpr_dir <- paste0("FILEPATH")
output_dir <- paste0("FILEPATH")
hfc_dir <- 'FILEPATH'

if (loc == 'GBR_4749_4636') current_locs <- c('GBR_4749', 'GBR_4636')

historic_locs <- data.table(readRDS(paste0("FILEPATH")))
pop <- fread(paste0("FILEPATH"))
input_data <- fread(paste0("FILEPATH"))

loop1 <- assertable::import_files(list.files(gpr_dir, full.names = T, pattern = paste0("FILEPATH")), FUN = fread)
loop1 <- loop1
loop1[,year_id := floor(year)]
loop1[,age := as.numeric(age)]

## getting age map
age_map <- fread(paste0("FILEPATH"))
age_map[,age := as.numeric(age_group_name_short)]
loc_map <- fread(paste0("FILEPATH"))


## subset to historical locations
pop <- merge(pop, age_map, by = "age_group_id", all.x =T)

historic_locs <- historic_locs[ihme_loc_id == "GBR_4749_4636"]
historic_locs[,location_id := 47494636]
historic_locs <- unique(historic_locs) 

## splitting historic locs
split_historic_locs <- function(asfr, population, historic_loc_data, loc_map){
  
  ## get ratio of England and Wales births to sum
  births <- merge(asfr[,.(ihme_loc_id, year_id, age, mean)], population[,.(ihme_loc_id, year_id, age, pop)],
                  by = c("ihme_loc_id", "year_id", "age"), all.x = T)
  births[,births := mean * pop]
  births[,agg_births := sum(births), by = c("year_id", "age")]
  births[,prop := births/agg_births]
  
  ## apply that ratio to the actual england + wales births to get split births
  split <- merge(births, historic_loc_data[,.(nid, source, source_type, year_id, age_group_id, age= age_start, val)],
                 by = c("year_id", "age"))
  split[,births := val * prop]
  split[,asfr := births / pop]
  
  ## keeping variables that we need and standardizing
  split <- merge(split, loc_map[,.(ihme_loc_id, location_id)], by = "ihme_loc_id", all.x=T)
  split[,fmeasid := 2]

  ## calculate variance
  split[,var := (asfr * (1-asfr)) / births]
  
  
  split <- split[,.(nid, id = source, source_type,location_id, ihme_loc_id, year_id, age, age_group_id, fmeasid, val = asfr, var)]
  
  return(split)
}

## run the function to split locs
split_data <- split_historic_locs(asfr = loop1, population = pop, historic_loc_data = historic_locs, loc_map = loc_map)


## attribute vr as complete and incomplete correctly
split_data[grepl("GBR", ihme_loc_id), source_name := "VR_complete"]
split_data[,source_type_id := 1]

setnames(split_data, c('val', 'var'), c('mean', 'variance'))
split_data[,method_name := 'Dir-Unadj']
split_data[,method_id := 1]
split_data[,underlying_nid := NA]
split_data[,reference := 0]
split_data[,outlier := 0]
split_data[,dem_measure_id := 2]
split_data[,id := NULL]
split_data[,fmeasid := NULL]
split_data[,source_type := NULL]
split_data[,sample_size := NA]

## Store in database to pull with outliering
upload_tb <- split_data[,.(location_id, year_id, age_group_id, nid, underlying_nid, source_type_id, method_id, mean, variance,
                            sample_size, outlier, reference, dem_measure_id)]
upload_tb[,viz_year := year_id + 0.5]
upload_tb[is.na(outlier), outlier := 0]
readr::write_csv(upload_tb, paste0("FILEPATH"))
upload_results(paste0("FILEPATH"), 'asfr', 'data', run_id = split_version, enforce_new = F, 
               send_slack = F)

send_slack_message(message=paste0('*Loop 1 finished: version ', version_id, 
                                  '*'), channel='#f', 
                   botname='FertBot', icon=':hatching_chick:')
