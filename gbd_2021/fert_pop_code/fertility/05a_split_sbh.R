##############################
## Purpose: Split SBH data
## Details: Parallelized by location
##          For each survey, calculate survey parity
##          Calculate loop 1 estimated parity
##          Calculate scalar to get age split SBH 
###############################

library(data.table)
library(readr)
library(mortdb, lib = "FILEPATH")
library(mortcore, lib = "FILEPATH")
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
  parser$add_argument('--split_version', type = 'integer')
  parser$add_argument('--loc', type ='character')
  args <- parser$parse_args()
  version_id <- args$version_id
  gbd_year <- args$gbd_year
  year_start <- args$year_start
  year_end <- args$year_end
  split_version <- args$split_version
  loc <- args$loc
}

input_dir <- paste0("FILEPATH")
gpr_dir <- paste0("FILEPATH")
output_dir <- paste0("FILEPATH")


###############################################
##############   Read in Data   #######################################################################################################
###############################################

input_sbh <- fread(paste0("FILEPATH"))[ihme_loc_id == loc]

loop1 <- assertable::import_files(list.files(gpr_dir, pattern=paste0("FILEPATH"), full.names=T), FUN=fread)

## Format cohorts
loop1[, year_id := floor(year)]
loop1[, cohort := year_id - age]

loop1_wide <- dcast(loop1, ihme_loc_id + sim + cohort  ~ age, value.var='fert')
loop1_wide[,`10` := 0]
loop1_wide[,`50` := 0]

###############################################
#########   Calculate Loop1 Parity   ##################################################################################################
###############################################

## Get single-year cohort ASFR estimates by taking weighted average
loop1_single <- as.data.table(expand.grid(age = 10:54, year_id = 1950:year_end, sim = 0:999, ihme_loc_id = loc))
loop1_single[, cohort := year_id - age][,year_id := NULL]
loop1_single <- loop1_single[cohort >= 1940]
loop1_single <- merge(loop1_single, loop1_wide, by=c('ihme_loc_id', 'sim', 'cohort'), allow.cartesian = T)

loop1_single[, uweight := (age%%5)*0.2]
loop1_single[, lweight := 1 - uweight]

loop1_single[, lage := as.character((age%/%5)*5)]
loop1_single[, uage := as.character((age%/%5)*5+5)]

loop1_single[, lval := .SD[[paste(lage)]], by=lage]
loop1_single[, uval := .SD[[paste(uage)]], by=uage]
loop1_single[is.na(uval), uval := 0]

loop1_single[, val := lweight*lval + uweight*uval]

loop1_single[, cpar := cumsum(val), by=c('cohort', 'sim')]
loop1_single <- loop1_single[age %in% seq(15, 45, 5)]

loop1_single[, year_id := cohort + age]

###############################################
##########   Calculate SBH Parity   ##################################################################################################
###############################################

input_sbh[, svage := (age_group_id-5)*5]
input_sbh[, cohort := year_id - svage]
setnames(input_sbh, 'year_id', 'sv_year')

split <- merge(loop1_single[,.(cohort, sim, ihme_loc_id, age, year_id, val, cpar)], input_sbh[,.(ihme_loc_id, age_group_id, nid, mean, 
               variance, svage, cohort, sv_year)], by=c('cohort', 'ihme_loc_id'), allow.cartesian = T)

split <- split[year_id <= sv_year]
split[,maxage := max(age), by=c('cohort', 'nid', 'sv_year' )]

## Calculate scalar
scalar <- unique(split[age == maxage, .(nid, cohort, sv_year, sim, mean, cpar)])
scalar[, scalar := mean/cpar]

split <- merge(split, scalar[,.(cohort, sv_year, nid, sim, scalar)], by=c('cohort', 'sv_year', 'nid', 'sim'))
split[, splitval := val*scalar]
split <- split[, .(val = mean(splitval), var = var(splitval)), by=c('cohort', 'ihme_loc_id', 'year_id', 'sv_year', 'nid', 'age', 'age_group_id')]


###############################################
##########   Merge onto Input Data   ##################################################################################################
###############################################

setnames(split, c('val', 'var'), c('mean', 'variance'))
split[, dem_measure_id := 2]
split[, cohort := NULL]



setnames(input_sbh, c('svage'), c('age'))
## Merge on source metadata
clean_split <- merge(split, unique(input_sbh[,.(location_id, nid, underlying_nid, source_type_id, source_name, method_id, 
                                              method_name, sv_year)]), by=c('nid', 'sv_year'), all.x=T)
clean_split <- merge(clean_split, input_sbh[,.(age, nid, sample_size, outlier, reference, sv_year)], 
                     by=c('nid', 'age', 'sv_year'))
clean_split[,age_group_id := (age/5)+5]
clean_split[,method_id := 22]
clean_split[,method_name := 'SBH-split']
clean_split[,sv_year := NULL]

## Store in database to pull with outliering
upload_sbh <- clean_split[,.(location_id, year_id, age_group_id, nid, underlying_nid, source_type_id, method_id, mean, variance,
               sample_size, outlier, reference, dem_measure_id)]
upload_sbh[,viz_year := year_id + 0.5]
upload_sbh[is.na(outlier), outlier := 0]
readr::write_csv(upload_sbh, paste0("FILEPATH"))

if(nrow(upload_sbh) > 0) {
  
  upload_results(
    paste0("FILEPATH"), 
    'asfr', 
    'data', 
    run_id = split_version, 
    enforce_new = F,
    send_slack = F
  )
  
}