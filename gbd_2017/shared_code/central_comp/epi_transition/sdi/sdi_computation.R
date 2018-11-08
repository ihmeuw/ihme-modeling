##########################################################
# Author: USERNAME
# Date: 20 December 2016
# Description: Computes full time series of Socio-demographic Index 
##########################################################

rm(list=ls())
if (Sys.info()[1] == "Linux"){
  j <- "FILEPATH"
  h <- "FILEPATH"
  c <- "FILEPATH"
  
} else if (Sys.info()[1] == "Windows"){
  j <- "FILEPATH"
  h <- "FILEPATH"
  c <- "FILEPATH"
}

## LOAD DEPENDENCIES
source(paste0(c, "helpers/primer.R"))


#####################################################
## INPUT ARGUMENTS
#####################################################

## PARSE ARGS
parser <- ArgumentParser()
parser$add_argument("--data_dir", help = "Site where data will be stored",
                    default = "FILEPATH", type = "character")
parser$add_argument("--vid", help = "SDI Version",
                    default = 9, type = "integer")
parser$add_argument("--desc", help = "Version Description",
                    default = "Refresh with SDI Quintile aggregates", type = "character")
args <- parser$parse_args()
list2env(args, environment()); rm(args)

# SDI VERSIONING
replace <- T

# DATABASE
gbdrid <- 5
locsetid <- 22
yids <- 1950:2017

# SDI INPUT SCALES

tfr_floor <- 0
tfr_ceiling <- 3
ldi_floor <- 250 #1 # so that sets scale properly in log space - check with Ryan, Emm, or Chris about this
ldi_ceiling <- 60000
edu_floor <- 0
edu_ceiling <- 17

# MISCELLANEOUS
tfu25 <- F

if(!tfu25){
  
  tfr_floor <- 1.5
  tfr_ceiling <- 8
  
}


####################################################
## FUNCTIONS
####################################################

compute_index <- function(x, floor, ceiling) {
  
  ind <- (x - floor)/(ceiling-floor)
  
  return(ind)
}


####################################################
## LOAD INPUTS
####################################################

## LOAD INPUTS
locsdf <- get_location_metadata(location_set_id = locsetid, gbd_round_id = gbdrid)
popdf <- get_population(age_group_id = 22, location_id = locsdf$location_id, year_id = max(yids),
                        sex_id = 3, location_set_id = locsetid, gbd_round_id = gbdrid)[, .(pop_version_id = run_id, location_id, year_id, population)]

# (1) LDI per capita
ldidf <- get_covariate_estimates(covariate_id = 57, location_id = locsdf$location_id, location_set_id = locsetid, gbd_round_id = gbdrid)[, .(ldi_version_id = model_version_id, location_id, year_id, log_ldi_pc = log(mean_value))]

# (2) Education years per capita (over age 15, under is maternal education)
edudf <- get_covariate_estimates(covariate_id = 1975, location_id = locsdf$location_id, location_set_id = locsetid, age_group_id = 22, year_id = yids, sex_id = 3, gbd_round_id = gbdrid)[, .(edu_version_id = model_version_id, location_id, year_id, edu_yrs_pc = mean_value)]

# (3) Total fertility rate - standard vs TFR25
fert_agids <- if(tfu25) 7:9 else 8:14

fertdf <- get_covariate_estimates(covariate_id = 13, location_id = locsdf$location_id, location_set_id = locsetid, age_group_id = fert_agids, sex_id = 2, year_id = yids, gbd_round_id = gbdrid)[, .(fert_version_id = model_version_id, location_id, year_id, age_group_id, sex_id, asfr = mean_value)]
tfrdf <- fertdf[, .(tfr = 5*sum(asfr)), by = .(fert_version_id, location_id, year_id)]

####################################################
## COMPUTE SDI
####################################################

## COMPILE INPUTS
  inputdf <- Reduce(function(x, y) merge(x, y, by = c('location_id', 'year_id'), all.x = T), list(ldidf, edudf, tfrdf))
  inputdf[, stage := 'raw_input']
  
  metadb <- fread(paste0(data_dir, "/version_metadata.csv"))
  metadf <- inputdf[, grep("version", names(inputdf), value = T), with = F] %>% unique
  metadf[, c('tfr_floor', 'tfr_ceiling', 'edu_floor', 'edu_ceiling', 'ldi_floor', 'ldi_ceiling') := .(tfr_floor, tfr_ceiling, edu_floor, edu_ceiling, ldi_floor, ldi_ceiling)]
  metadf[, sdi_version_id := vid][, note := sprintf('%s; linked to fert v%d, edu v%d, ldi v%d' ,desc, fert_version_id, edu_version_id, ldi_version_id)]
  
## CAP INPUTS
  inputdf[, grep('version', names(inputdf), value = T) := NULL]
  raw_input <- copy(inputdf)
  
  inputdf[log_ldi_pc > log(ldi_ceiling), log_ldi_pc := log(ldi_ceiling)]
  inputdf[log_ldi_pc < log(ldi_floor), log_ldi_pc := log(ldi_floor)]
      
  inputdf[edu_yrs_pc > edu_ceiling, edu_yrs_pc := edu_ceiling]
  inputdf[edu_yrs_pc < edu_floor, edu_yrs_pc := edu_floor]
      
  inputdf[tfr > tfr_ceiling, tfr := tfr_ceiling]
  inputdf[tfr < tfr_floor, tfr := tfr_floor]
      
  assert_values(inputdf, colnames = c('log_ldi_pc', 'edu_yrs_pc', 'tfr'), test = 'not_na')
  
  # Save capped stage
  inputdf[, stage := 'capped_input']
  capped_input <- copy(inputdf)
  
## SCALE INPUTS
  indexdf <- copy(inputdf)
  indexdf[, log_ldi_pc := compute_index(log_ldi_pc, log(ldi_floor), log(ldi_ceiling))]
  indexdf[, edu_yrs_pc := compute_index(edu_yrs_pc, edu_floor, edu_ceiling)]
  indexdf[, tfr := 1 - compute_index(tfr, tfr_floor, tfr_ceiling)]
  indexdf[, stage := 'scaled_input']
  
  floors <- function(x) {
      if (x < .005) {
          x <- .005
      }
      return(x)
  }
  
  indexdf <- indexdf[, lapply(.SD, floors), by = .(location_id, year_id, stage)] # settings floor to avoid SDI of 0
  scaled_input <- copy(indexdf)

## COMPUTE SDI
  indexdf[, sdi := (log_ldi_pc*tfr*edu_yrs_pc)^(1/3)]

##############################################################
## GENERATE QUINTILES
##############################################################

  ## Use only national template, populations greater than 1 million (Decision made for GBD 2016)
  national_lids <- locsdf[level == 3, location_id]
  adequate_pops <- popdf[year_id == max(yids) & population >= 10^6, location_id]
  
  quintile_cutdf <- indexdf[year_id == max(yids) & location_id %in% national_lids & location_id %in% adequate_pops]
  quintile_cuts <- quantile(quintile_cutdf$sdi, probs = seq(0,1,.2))[2:5]
  quintile_cuts <- c(0, quintile_cuts, 1) # allows us to capture small locs outside the bounds of the evaluated locations
  
  quintiledf <- indexdf[year_id == max(yids) & location_id %in% locsdf[level >=3, location_id],
                        .(location_id, year_id, sdi)]
  quintiledf[, sdi_quintile := cut(sdi, breaks = quintile_cuts, labels = c("Low SDI", "Low-middle SDI", "Middle SDI", "High-middle SDI", "High SDI"))]
  
  parent_pops <- popdf[location_id %in% locsdf[level == 3 & most_detailed == 0, location_id]]
  parent_quintiles <- merge(parent_pops[, !c('pop_version_id', 'year_id')], quintiledf[, !'sdi_version_id'], by = 'location_id')
  parent_quintiles <- merge(parent_quintiles[, !c('year_id', 'sdi')], locsdf[, .(location_id, parent_ihmelid = ihme_loc_id)], by = 'location_id')
  setnames(parent_quintiles, c('location_id', 'sdi_quintile', 'population'), c('parent_id', 'parent_quintile', 'parent_population'))
  
  quintiledf <- merge(quintiledf, locsdf[, .(location_id, parent_ihmelid = substr(ihme_loc_id, 1, 3))], by = 'location_id')
  quintiledf <- merge(quintiledf, parent_quintiles[, !'parent_id'], by = 'parent_ihmelid', all.x = T)
  quintiledf[!is.na(parent_quintile) & parent_population < 2e8, sdi_quintile := parent_quintile]
  quintiledf[, grep('parent', names(quintiledf), value = T) := NULL]
  
###############################################################
## GENERATE TIME SERIES FOR QUINTILES
###############################################################
  
  ldipop <- get_mort_outputs(model_name = 'population', model_type = 'estimate', 
                             location_ids = locsdf$location_id, age_group_ids = 22,
                             sex_ids = 3, year_ids = yids, run_id = unique(popdf$pop_version_id))[, .(location_id, year_id, ldi_pop = mean)]
  edupop <- get_mort_outputs(model_name = 'population', model_type = 'estimate', 
                              location_ids = locsdf$location_id, age_group_ids = c(8:20, 30:32, 235),
                              sex_ids = 3, year_ids = yids, run_id = unique(popdf$pop_version_id))[, .(edu_pop = sum(mean)), by = .(location_id, year_id)]
  fertpop <- get_mort_outputs(model_name = 'population', model_type = 'estimate', 
                             location_ids = locsdf$location_id, age_group_ids = fert_agids,
                             sex_ids = 2, year_ids = yids, run_id = unique(popdf$pop_version_id))[, .(fert_pop = sum(mean)), by = .(location_id, year_id, age_group_id)]
  
  aggldi <- merge(ldidf, ldipop, by = c('location_id', 'year_id')) %>% merge(., quintiledf[location_id %in% locsdf[most_detailed == T]$location_id, .(location_id, sdi_quintile)], by = 'location_id') %>%
    .[, .(log_ldi_pc = log(weighted.mean(exp(log_ldi_pc), w = ldi_pop))), by = .(sdi_quintile, year_id)]
  aggedu <- merge(edudf, edupop, by = c('location_id', 'year_id')) %>% merge(., quintiledf[location_id %in% locsdf[most_detailed == T]$location_id, .(location_id, sdi_quintile)], by = 'location_id') %>%
    .[, .(edu_yrs_pc = weighted.mean(edu_yrs_pc, w = edu_pop)), by = .(sdi_quintile, year_id)]
  aggtfr <- merge(fertdf, fertpop, by = c('location_id', 'year_id', 'age_group_id')) %>% merge(., quintiledf[location_id %in% locsdf[most_detailed == T]$location_id, .(location_id, sdi_quintile)], by = 'location_id') %>%
    .[, .(asfr = weighted.mean(asfr, w = fert_pop)), by = .(sdi_quintile, year_id, age_group_id)] %>% .[, .(tfr = 5*sum(asfr)), by = .(sdi_quintile, year_id)]
  
  aggdf <- Reduce(function(x, y) merge(x, y, by = c('sdi_quintile', 'year_id'), all.x = T), list(aggldi, aggedu, aggtfr))
  
  aggdf[log_ldi_pc > log(ldi_ceiling), log_ldi_pc := log(ldi_ceiling)]
  aggdf[log_ldi_pc < log(ldi_floor), log_ldi_pc := log(ldi_floor)]
  
  aggdf[edu_yrs_pc > edu_ceiling, edu_yrs_pc := edu_ceiling]
  aggdf[edu_yrs_pc < edu_floor, edu_yrs_pc := edu_floor]
  
  aggdf[tfr > tfr_ceiling, tfr := tfr_ceiling]
  aggdf[tfr < tfr_floor, tfr := tfr_floor]
  
  aggdf[, log_ldi_pc := compute_index(log_ldi_pc, log(ldi_floor), log(ldi_ceiling))]
  aggdf[, edu_yrs_pc := compute_index(edu_yrs_pc, edu_floor, edu_ceiling)]
  aggdf[, tfr := 1 - compute_index(tfr, tfr_floor, tfr_ceiling)]
  
  aggdf <- aggdf[, lapply(.SD, floors), by = .(sdi_quintile, year_id)]
  aggdf[, sdi := (log_ldi_pc*tfr*edu_yrs_pc)^(1/3)]
  
  agglocsdf <- get_location_metadata(location_set_id = 40)[level == 0, .(sdi_quintile = location_name, location_id)]
  
  aggdf <- merge(aggdf, agglocsdf, by = 'sdi_quintile')
  
  agginput <- aggdf[, !c('sdi_quintile', 'sdi')][, stage := 'scaled_input']
  aggsdi <- aggdf[, c('location_id', 'year_id', 'sdi')][, stage := 'output']
  
  
###############################################################
## COMBINE, VALIDATE, AND SAVE
###############################################################

## COMBINE
  inputs <- rbind(raw_input, capped_input, scaled_input, agginput, use.names = T, fill = T)
  sdi <- indexdf[, .(location_id, year_id, sdi, stage = 'output')]
  
  saveresults <- rbind(inputs, sdi, aggsdi, use.names = T, fill = T)
  saveresults <- melt(saveresults, id.vars = c('location_id', 'year_id', 'stage'),
                      variable.name = 'measure', value.name = 'val')[!is.na(val)]
  
  quintile_cuts <- data.table(sdi_quintile = c("Low SDI", "Low-middle SDI", "Middle SDI", "High-middle SDI", "High SDI"),
                              lower_bound = quintile_cuts[1:5],
                              upper_bound = quintile_cuts[2:6])
  quintiles <- quintiledf[, !c('year_id', 'sdi'), with = F]
  
## STANDARDIZE

  lapply(list(saveresults, quintile_cuts, quintiles), function(df, vid) df[, sdi_version_id := vid], vid = vid)

## CLEAN
  
  setcolorder(saveresults, c('sdi_version_id','location_id', 'year_id', 'stage', 'measure', 'val'))
  saveresults <- saveresults[order(location_id, year_id, stage, measure)]
  
  setcolorder(quintiles, c('sdi_version_id', 'location_id', 'sdi_quintile'))
  setcolorder(quintile_cuts, c('sdi_version_id', 'sdi_quintile', 'lower_bound', 'upper_bound'))
    
## VALIDATE
  
  assert_ids(saveresults[stage %in% c('scaled_input', 'output')], id_vars = list(location_id = c(locsdf$location_id, agglocsdf$location_id),
                                         year_id = yids,
                                         measure = c('log_ldi_pc', 'edu_yrs_pc', 'tfr', 'sdi')))
  
## SAVE
  setwd(paste0(data_dir, vid))
  saveRDS(saveresults, 'sdi_inputs_values.RDS')
  saveRDS(quintile_cuts, 'sdi_quintile_breaks.RDS')
  saveRDS(quintiles, 'sdi_quintiles.RDS')
  
  if(replace) metadb <- metadb[sdi_version_id != vid]
  if(nrow(metadb[sdi_version_id == vid]) > 0) stop("This SDI version has already been generated. Increment SDI version id")
  write.csv(rbind(metadb, metadf, use.names = T, fill = T), paste0(data_dir, "/version_metadata.csv"), row.names = F)

