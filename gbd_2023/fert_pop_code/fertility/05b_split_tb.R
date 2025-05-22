##############################
## Purpose: Split TB data
## Details: Age-split TB based on loop 1 estimated
###############################

library(data.table)
library(readr)
library(mortdb)
library(mortcore)
rm(list=ls())

if (interactive()){
  user <- Sys.getenv('USER')
  version_id <- 'Run id'
  gbd_year <- 2020
  year_start <- 1950
  year_end <- 2022
  loc <- 'input location'
  split_version <- 70
} else {
  user <- Sys.getenv('USER')
  parser <- argparse::ArgumentParser()
  parser$add_argument('--version_id', type = 'character')
  parser$add_argument('--gbd_year', type = 'integer')
  parser$add_argument('--year_start', type = 'integer')
  parser$add_argument('--year_end', type = 'integer')
  parser$add_argument('--split_version', type = 'integer')
  parser$add_argument('--loc', type = 'character')
  args <- parser$parse_args()
  version_id <- args$version_id
  gbd_year <- args$gbd_year
  year_start <- args$year_start
  year_end <- args$year_end
  split_version <- args$split_version
  loc <- args$loc
}

print(commandArgs(trailingOnly = T))

input_dir <- "FILEPATH"
gpr_dir <- "FILEPATH"
output_dir <- "FILEPATH"

input_tb <- fread(paste0(input_dir, 'input_tb.csv'))[ihme_loc_id == loc]
input_pop <- fread (paste0(input_dir, 'input_pop.csv'))[ihme_loc_id == loc]
input_pop[, age := (age_group_id-5)*5]

###############################################
#########  Calculate Loop 1 Births   ##################################################################################################
###############################################

loop1 <- assertable::import_files(list.files(gpr_dir, , pattern=paste0('gpr_', loc, '_[0-9]+_sim.csv'), full.names=T), 
                                  FUN=fread)[,.(ihme_loc_id, sim, year, fert, age)]
loop1 <- loop1[age %in% 15:45]
loop1[, year_id := floor(as.numeric(year))]
loop1 <- merge(loop1, input_pop[ihme_loc_id == loc, .(year_id, ihme_loc_id, age_group_id, age, pop)], 
               by=c('year_id', 'ihme_loc_id', 'age'))
loop1[, births := fert*pop]
loop1[, total_births := sum(births), by=c('sim', 'year')]
loop1[, prop := births/total_births]

loop1_props <- loop1[,.(year_id, ihme_loc_id, age, age_group_id, sim, prop)]

###############################################
############  Split Total Births   ####################################################################################################
###############################################

input_tb[, age_group_id := NULL]
split <- merge(input_tb, loop1_props, by=c('year_id', 'ihme_loc_id'), allow.cartesian = T)
split[, split_births := mean * prop]
split <- merge(split, input_pop[,.(year_id, ihme_loc_id, age_group_id, age, pop)], 
               by=c('year_id', 'ihme_loc_id', 'age', 'age_group_id'), allow.cartesian = T)
split[, split_asfr := split_births/pop]
split <- split[, .(val = mean(split_asfr), var = var(split_asfr)), by=c('ihme_loc_id', 'year_id', 'nid', 'age', 'age_group_id')]

###############################################
##########   Merge onto Input Data   ##################################################################################################
###############################################

setnames(split, c('val', 'var'), c('mean', 'variance'))

clean_split <- merge(split, input_tb[,.(location_id, year_id, nid, underlying_nid, source_type_id, source_name, sample_size, outlier, 
                                        reference)], by=c('year_id', 'nid'), allow.cartesian = T)
clean_split[,method_id := 24]
clean_split[,method_name := 'TB-split']
clean_split[, dem_measure_id := 2]

## Store in database to pull with outliering
upload_tb <- clean_split[,.(location_id, year_id, age_group_id, nid, underlying_nid, source_type_id, method_id, mean, variance,
                            sample_size, outlier, reference, dem_measure_id)]
upload_tb[,viz_year := year_id + 0.5]
upload_tb[is.na(outlier), outlier := 0]
readr::write_csv(upload_tb, paste0(output_dir, loc, '_input_tb_for_upload.csv'))

if(nrow(upload_tb) > 0) {
  
  upload_results(paste0(output_dir, loc, '_input_tb_for_upload.csv'), 'asfr', 'data', run_id = split_version, enforce_new = F,
                 send_slack = F)
  
}