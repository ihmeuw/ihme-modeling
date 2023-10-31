##############################
## Purpose: Prep for model
## Details: Read in and prepare input data for modeling
##          Generate parent child relationships
##          Get completeness for VR data
#############################

rm(list=ls())
library(data.table)
library(readr)
library(boot)
library(assertable)
library(mortdb, lib = 'FILEPATH')
library(mortcore, lib = 'FILEPATH')

if (interactive()){
  user <- Sys.getenv('USER')
  version_id <- x
  gbd_year <- x
  year_start <- x
  year_end <- x
  loop <- x
  decomp_step <- ''
  split_version <- ''
  pop_est_id <- x
  pop_syr_id <- x
  ddm_est_id <- x
  birth_est_id <- x
} else {
  user <- Sys.getenv('USER')
  parser <- argparse::ArgumentParser()
  parser$add_argument('--version_id', type = 'character')
  parser$add_argument('--gbd_year', type = 'integer')
  parser$add_argument('--year_start', type = 'integer')
  parser$add_argument('--year_end', type = 'integer')
  parser$add_argument('--loop', type = 'integer')
  parser$add_argument('--decomp_step', type = 'character')
  parser$add_argument('--split_version', type = 'character')
  parser$add_argument('--pop_est_id', type = 'integer')
  parser$add_argument('--pop_syr_id', type = 'integer')
  parser$add_argument('--ddm_est_id', type = 'integer')
  parser$add_argument('--birth_est_id', type = 'integer')
  parser$add_argument('--code_dir', type = 'character')
  args <- parser$parse_args()
  version_id <- args$version_id
  gbd_year <- args$gbd_year
  year_start <- args$year_start
  year_end <- args$year_end
  loop <- args$loop
  decomp_step <- args$decomp_step
  split_version <- args$split_version
  pop_est_id <- args$pop_est_id
  pop_syr_id <- args$pop_syr_id
  ddm_est_id <- args$ddm_est_id
  birth_est_id <- args$birth_est_id
  code_dir <- args$code_dir
}
print(commandArgs(trailingOnly = T))

gbd_round <- mortdb::get_gbd_round(gbd_year = gbd_year)

input_dir <- paste0("FILEPATH")
output_dir <- paste0("FILEPATH")
loop1_dir <- paste0("FILEPATH")

model_locs <- fread(paste0("FILEPATH"))

## Source central functions
source('FILEPATH/get_covariate_estimates.R')

## Read in input data
if (loop == 1) {
  best <- get_best_versions(model_name = 'asfr data', 
                            gbd_year = gbd_year)
  inputs <- get_mort_outputs(model_name = 'asfr', 
                             model_type = 'data', 
                             gbd_year = gbd_year, 
                             run_id = best, 
                             outlier_run_id = 'active')
  input_asfr <- inputs[dem_measure_id == 2]
  input_births <- inputs[dem_measure_id == 1]
  input_pop <- mortdb::get_mort_outputs(model_name = 'population', 
                                        model_type = 'estimate', 
                                        sex_ids = 2, 
                                        age_group_ids = 7:15, 
                                        run_id = pop_est_id)
  input_pop_single <- mortdb::get_mort_outputs(model_name = 'population single year', 
                                               model_type = 'estimate', 
                                               sex_ids = 2, 
                                               age_group_ids = 58:102,
                                               run_id = pop_syr_id)
  input_sbh <- inputs[dem_measure_id == 5]
  input_tb <- inputs[dem_measure_id == 4] 
  
  sbh_locs <- data.table(ihme_loc_id = unique(input_sbh$ihme_loc_id[(input_sbh$ihme_loc_id) %in% model_locs$ihme_loc_id]))[!is.na(ihme_loc_id)]
  tb_locs <- data.table(ihme_loc_id = unique(input_tb$ihme_loc_id[(input_tb$ihme_loc_id) %in% model_locs$ihme_loc_id]))[!is.na(ihme_loc_id)]
  
  setnames(input_asfr, 'mean', 'asfr_data')
  setnames(input_births, 'mean', 'births')
  setnames(input_pop, 'mean', 'pop')
  
  ## Calculate ASFR from births
  input_births <- merge(input_births, input_pop[sex_id == 2 & age_group_id %in% 7:15,], 
                        by=c('ihme_loc_id', 'location_id', 'year_id', 'age_group_id'))
  input_births[, asfr_data := births/pop]
  
  input_data <- rbind(input_asfr[,.(nid, underlying_nid, year_id, location_id, 
                                    ihme_loc_id, age_group_id, asfr_data, 
                                    variance, sample_size, outlier, reference, 
                                    dem_measure_id, source_type_id, source_name,
                                    method_id, method_name)], 
                      input_births[,.(nid, underlying_nid, year_id, location_id, 
                                      ihme_loc_id, age_group_id, asfr_data, 
                                      variance, sample_size, outlier, reference, 
                                      dem_measure_id, source_type_id, source_name,
                                      method_id, method_name)])
  
  ## Generate parent child relationship for ASFR and pop
  relations <- get_proc_lineage("asfr", "estimate", run_id = version_id)
  
  if(nrow(relations) == 0) {
    
    mortdb::gen_parent_child(
      child_process = 'asfr estimate', 
      child_id = version_id, 
      parent_runs = list(
        'population estimate' = pop_est_id,
        'population single year estimate' = pop_syr_id,
        'asfr data' = best,
        'ddm estimate' = ddm_est_id,
        'birth estimate' = birth_est_id
      )
    )
    
  }
  
} else {
  input_data <- get_mort_outputs(model_name = 'asfr', model_type = 'data', 
                                 gbd_year = gbd_year, run_id = split_version, 
                                 outlier_run_id = 'active')
  
  setnames(input_data, 'mean', 'asfr_data')
  input_data <- input_data[,.(nid, underlying_nid, year_id, location_id, 
                              ihme_loc_id, age_group_id, asfr_data, variance, 
                              sample_size, outlier, reference, dem_measure_id, 
                              source_type_id, source_name, method_id, method_name)]
  input_data[, age_group_id := as.numeric(age_group_id)]
}

input_data[is.na(outlier), outlier := 0]

## Merge on metadata
loc_map <- get_locations(level = 'all')
age_map <- get_age_map(type = 'all')

input_data[, age := NULL]

input_data <- merge(input_data, loc_map[,.(location_id, region_id, 
                                           super_region_id, location_name, 
                                           region_name, super_region_name)], 
                    by='location_id')
input_data <- merge(input_data, age_map[,.(age_group_id, age_group_name_short)], 
                    by='age_group_id')

setnames(input_data, 'age_group_name_short', 'age')

if (loop == 1) {
  input_data[, lower_bound := min(asfr_data), by='age_group_id']
  input_data[, upper_bound := quantile(asfr_data, 0.993), by='age_group_id']
} else {
  bounds <- fread(paste0("FILEPATH"))
  bounds[, age := as.character(age)]
  input_data <- merge(input_data, bounds, by='age')
}

## Bounded logit transformation
input_data[asfr_data >= upper_bound, asfr_data := upper_bound - 0.000001]
input_data[asfr_data <= lower_bound, asfr_data := lower_bound + 0.000001]
input_data[,logit_asfr_data := boot::logit((asfr_data - lower_bound) / (upper_bound - lower_bound))]

## Outlier points near bounds
input_data[asfr_data >= upper_bound - 0.000001, outlier := 1]
input_data[round(asfr_data, 5) == 0, outlier := 1]

## Unoutlier bounded old-young data
source(paste0(code_dir, "/00b_unoutlier_out_of_bounds_data.R"))

logit_bounds <- unique(input_data[,.(age, lower_bound, upper_bound)])
readr::write_csv(logit_bounds, paste0("FILEPATH"))

input_data[, lower_bound := NULL]
input_data[, upper_bound := NULL]

## Get completeness from DDM
completeness <- mortdb::get_mort_outputs(model_name = 'ddm', model_type = 'estimate', 
                                         estimate_stage_ids = 11, gbd_year = gbd_year, 
                                         run_id= ddm_est_id)
setnames(completeness, 'mean', 'completeness')
completeness <- completeness[,c('year_id', 'location_id', 'ihme_loc_id', 'completeness', 'source_type_id')]

max_comp <- copy(completeness)
max_comp[, max_yr := max(year_id), by = c("location_id", "source_type_id")]
max_comp <- max_comp[year_id == max_yr][, max_yr := NULL]
for(yr in (unique(max_comp$year_id) + 1):year_end) {
  
  temp <- copy(max_comp)
  temp[, year_id := yr]
  completeness <- rbind(temp, completeness)
  
}

# Merge completeness
input_data <- merge(
  input_data, 
  completeness, 
  by=c('year_id', 'location_id', 'ihme_loc_id', 'source_type_id'), 
  all.x=T
)

# Categorize VR completeness
input_data[source_type_id == 1, source_name := ifelse(completeness >= 0.95, 'VR_complete', 'VR_incomplete')]

## If no completeness estimate, mark as incomplete
input_data[source_type_id == 1 & is.na(source_name), source_name := 'VR_incomplete']

## Make exception for RUS VR close to completeness threshold
input_data[grepl('RUS', ihme_loc_id) & 
             grepl('VR', source_name) & (year_id >= 2017),
           source_name := 'VR_complete']

## Make exception for UKR subnats. We do not want to use RUS sources for completeness
input_data[grepl("UKR_", ihme_loc_id) & nid == 376031, source_name := 'VR_complete']

## Make an exception for IRN subs (TB values are comp, but there are no ddm values for non-TB)
input_data[grepl("IRN_", ihme_loc_id) & year_id >= gbd_year & grepl('VR', source_name), source_name := 'VR_complete']

# Custom comp re-assignment: CHN VR sources
input_data[ihme_loc_id == 'CHN_44533' & nid %in% c(415956, 329653), source_name := 'VR_complete']
input_data[ihme_loc_id == 'CHN_44533' & method_name == 'TB-split' & year_id > 2010, source_name := 'VR_complete']

# Custom re-assignment: 
for(loc in c('NOR', 'GBR', 'JPN', 'ISL')) {
  
  input_data[grepl(loc, ihme_loc_id) & grepl('VR', source_name), source_name := 'VR_complete']
  
}

input_data$completeness <- NULL

## Get covariates - female education
if (loop == 1) {
  fem_edu <- get_covariate_estimates(gbd_round_id = gbd_round, covariate_id = 33, 
                                     location_id = -1, year_id = year_start:year_end, 
                                     age_group_id = 7:15, sex_id = 2, 
                                     decomp_step = decomp_step)
  setnames(fem_edu, 'mean_value', 'fem_edu')

  # set external map relations
  fem_edu_id <- unique(fem_edu$model_version_id)
  
  ext_relations <- get_external_input_map("asfr estimate", run_id = version_id)
  if(nrow(ext_relations) == 0) {
    
    gen_external_input_map(
      process_name = 'asfr estimate', 
      run_id = version_id,
      external_input_versions = list('mean_edu' = fem_edu_id)
    )
    
  }
  
  ## Format covariates
  fem_edu <- merge(fem_edu, loc_map[,.(location_id, ihme_loc_id)], 
                   by='location_id')
  fem_edu <- merge(fem_edu, age_map[,.(age_group_id, age_group_name_short)], 
                   by='age_group_id')
  
  setnames(fem_edu, 'age_group_name_short', 'age')
} else {
  fem_edu <- fread(paste0("FILEPATH"))
}

input_data <- merge(input_data, fem_edu[,.(location_id, year_id, age_group_id, fem_edu)], 
                    by=c('location_id', 'year_id', 'age_group_id'), all.x=T)

## Save all inputs, avoid future DB calls
if (loop == 1) {
  readr::write_csv(input_data, paste0("FILEPATH"))
  readr::write_csv(input_pop, paste0("FILEPATH"))
  readr::write_csv(input_pop_single, paste0("FILEPATH"))
  readr::write_csv(input_sbh, paste0("FILEPATH"))
  readr::write_csv(input_tb, paste0("FILEPATH"))
  readr::write_csv(fem_edu, paste0("FILEPATH"))
  readr::write_csv(loc_map, paste0("FILEPATH"))
  readr::write_csv(age_map, paste0("FILEPATH"))
  readr::write_csv(sbh_locs, paste0("FILEPATH"))
  readr::write_csv(tb_locs, paste0("FILEPATH"))
} else {
  l1_input_data <- fread(paste0("FILEPATH"))
  input_data <- rbind(l1_input_data, input_data)
  readr::write_csv(input_data, paste0("FILEPATH"))
}
