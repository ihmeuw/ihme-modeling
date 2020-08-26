##############################
## Purpose: Prepare for GPR
##############################

sessionInfo()
rm(list=ls())
library(data.table)
library(haven)
library(lme4)
library(ggplot2)
library(splines)
library(parallel)
library(magrittr)


if (interactive()){
  username <- USERNAME
  root <- FILEPATH
  model_age <-
  version <-
  loop <-
  gbd_year <-
  year_start <-
  year_end <-
} else {
  username <- USERNAME
  root <- FILEPATH
  model_age <- commandArgs(trailingOnly = T)[1]
  version <- commandArgs(trailingOnly = T)[2]
  loop <- commandArgs(trailingOnly = T)[3]
  gbd_year <- commandArgs(trailingOnly = T)[4]
  year_start <- commandArgs(trailingOnly = T)[5]
  year_end <- commandArgs(trailingOnly = T)[6]
}



## set directories
base_dir <- FILEPATH
jbase <- FILEPATH
out_dir <- FILEPATH
param_dir <- FILEPATH
diagnostic_dir <- FILEPATH
j_data_dir <- FILEPATH
total_births_dir <- FILEPATH

## location maps
reg_map <- data.table(get_locations(level='estimate', gbd_year = gbd_year))[,.(location_id, ihme_loc_id, region_name, super_region_name)]
nat_locs <- data.table(get_locations(level='estimate', gbd_year = gbd_year))
nat_locs <- nat_locs[level==3| ihme_loc_id %in% c('CHN_44533', 'CHN_361', 'CHN_354',
                                                  'GBR_433', 'GBR_434', 'GBR_4636', 'GBR_4749')]
nat_locs <- nat_locs$ihme_loc_id

super_regs <- c('Sub-Saharan Africa', 'High-income', 'others', 'Central Europe, Eastern Europe, and Central Asia')

locations <- get_locations(level = 'estimate', gbd_year = gbd_year)
targets = data.table(locations)
gbd_standard <- get_locations(gbd_type = 'standard_modeling', level = 'all')
mortality_locations <- get_locations(gbd_type = 'ap_old', level = 'estimate')
national_parents <- mortality_locations[level == 4, parent_id]
standard_with_metadata <- mortality_locations[location_id %in% unique(c(gbd_standard$location_id, national_parents, 44533))]

targets[location_id %in% standard_with_metadata$location_id, primary := T]
targets[is.na(primary), primary := F]
targets[, secondary := T]

pop <- fread(paste0(jbase, version, '/inputs/pop_input.csv'))

##################
## Read in Data
##################


## stage 1 and 2 predictions
st_pred <- list()
missing_files <- c()
for(super_reg in super_regs){
  file <- paste0(out_dir, 'stage2_results_age', model_age, '_reg_', super_reg,  '.csv')
  if(file.exists(file)) st_pred[[paste0(super_reg)]] <- fread(file) else missing_files <- c(missing_files, file)
}

if(length(missing_files) > 0) stop('missing files')
st_pred <- rbindlist(st_pred, fill =T)

## data density
dd <- list()
missing_files <- c()
for(super_reg in super_regs){
  file <- paste0(out_dir, 'data_den_age', model_age, '_reg_', super_reg, '.csv')
  if(file.exists(file)) dd[[paste0(super_reg)]] <- fread(file) else missing_files <- c(missing_files, file)
}

if(length(missing_files) > 0) stop('missing files')
dd <- rbindlist(dd)

## parameters
params <- list()
missing_files <- c()

for(super_reg in super_regs){
  file <- paste0(param_dir, 'asfr_params_age_', model_age,'_', super_reg, '.txt')
  if(file.exists(file)) params[[paste0(super_reg)]] <- fread(file) else missing_files <- c(missing_files)
}

if(length(missing_files) > 0) stop('missing files')
params <- rbindlist(params)
write.csv(params, paste0(param_dir, 'asfr_params_age_', model_age, '.txt'))

## logit bounds
bounds <- fread(paste0('/share/fertilitypop/fertility/modeling/', version, '/loop1/results/stage1and2/logit_bounds.csv'))
setnames(bounds, c('lower_bound', 'upper_bound'), c('lower_logit', 'upper_logit'))
bounds <- bounds[age == model_age, .(lower_logit, upper_logit)]

## input file for variance
samp_var <- readRDS(paste0(j_data_dir, 'asfr.RDs'))
samp_var[,9:10] <- NULL


# ##################
# ## Prep for GPR
# ##################
#
## create mean squared error
gpr_input <- copy(st_pred)

## get low data locs for later
low_data_locs <- copy(dd)
low_data_locs <- low_data_locs[dd<5]
low_data_locs <- low_data_locs$ihme_loc_id

gpr_input <- merge(gpr_input, reg_map[, .(location_id, region_name, super_region_name)], by='location_id', all.x=T)

mse <- copy(gpr_input)
mse <- mse[year_id >=1990]
mse[, diff := stage2_pred - stage1_pred_no_re]

vr_only <- merge(gpr_input[grepl('vr', source_type), .N, by=ihme_loc_id], gpr_input[!is.na(source_type), .N, by=ihme_loc_id], by='ihme_loc_id')

high_locs <- locations[super_region_name == "High-income"]
high_locs <- high_locs[, .(ihme_loc_id)]

vr_only <- merge(vr_only, high_locs, by = "ihme_loc_id")
vr_only <- vr_only[N.x >= 40 & N.x == N.y, ihme_loc_id]

mse[ihme_loc_id %in% vr_only & !is.na(logit_asfr_data), diff := stage2_pred - logit_asfr_data]

setkey(mse, ihme_loc_id)
mse <- mse[ihme_loc_id %in% nat_locs,.(mse=stats::var(diff)), by=key(mse)]  #only using national locs to compute mse


good_vr_locs <- dd[dd>=50]$ihme_loc_id
mse <- mse[!grepl('_', mse)]
global_mse <- mean(mse$mse)

gpr_input[, mse := global_mse]
jpn_mse <- mse[ihme_loc_id == 'JPN', mse]
gpr_input[ihme_loc_id == 'JPN', mse := jpn_mse]

mse_vr <- mse[ihme_loc_id %in% vr_only]
setnames(mse_vr, "mse", "mse_vr")
gpr_input <- merge(gpr_input, mse_vr, by="ihme_loc_id", all.x=T)
gpr_input <- gpr_input[!is.na(mse_vr), mse := mse_vr]
gpr_input <- gpr_input[, mse_vr := NULL]

## creating data variance
nat_var <- copy(gpr_input)
nat_var[,diff := logit_asfr_data - stage2_pred]
nat_var[outlier == 1, diff := NA]
setkey(nat_var, ihme_loc_id)
nat_var <- nat_var[,.(year_id=year_id, data_var=var(diff, na.rm=T)), by=key(nat_var)]
setkey(nat_var, NULL)
nat_var <- unique(nat_var)

gpr_input <- merge(gpr_input, nat_var, by=c('year_id', 'ihme_loc_id'))

## for locations in with low data, use the maximum data variance in their region as their data variance
gpr_input[,max_region_var := max(data_var, na.rm = T), by='region_name']
gpr_input[ihme_loc_id %in% low_data_locs, data_var := max_region_var]
gpr_input[,max_region_var := NULL]

vr_only <- merge(gpr_input[grepl('vr', source_type), .N, by=ihme_loc_id], gpr_input[!is.na(source_type), .N, by=ihme_loc_id], by='ihme_loc_id')
vr_only <- vr_only[N.x >= 40 & N.x == N.y, ihme_loc_id]
samp_var[, c('lower_logit', 'upper_logit') := bounds]
samp_var <- merge(samp_var, pop, by=c('age_group_id', 'location_id', 'year_id'))
samp_var[(ihme_loc_id %in% vr_only) | ihme_loc_id == "RUS_44965", logit_samp_var := (val*(1-val)/population) * (((1/(val-lower_logit)) + (1/(upper_logit-val)))^2)]

gpr_input <- merge(gpr_input,
                   samp_var[,.(ihme_loc_id, nid, id, source_type, location_id, year_id, age_group_id, logit_samp_var)],
                   by=c('year_id', 'ihme_loc_id', 'id', 'source_type', 'location_id', 'age_group_id', 'nid'), all.x=T)
gpr_input[!is.na(logit_samp_var), data_var := logit_samp_var]
gpr_input[,logit_samp_var := NULL]

if(loop==2) gpr_input[ihme_loc_id == 'CHN_44533' & source_type == 'survey_unknown_recall_tab', data_var := gpr_input[ihme_loc_id == 'CHN_44533' & nid == 415956, data_var/0.005]]

if(nrow(gpr_input[is.na(data_var)])>0) stop('missing data varaince')
if(nrow(gpr_input[is.na(mse)])>0) stop('missing mse')

gpr_input[outlier == 1, logit_asfr_data := NA]
gpr_input[,data:=0]
gpr_input[!is.na(logit_asfr_data), data:=1]

gpr_input[,category := 'other']

gpr_input <- gpr_input[,.(ihme_loc_id, year_id, region_name, age, nid, asfr_data, logit_asfr_data, adjusted_logit_asfr_data, stage1_pred = stage1_pred_no_re, stage1_pred_no_re, stage2_pred, mse, data_var, data, category, id, outlier, source_type, adjustment_factor)]

if(loop==2)gpr_input[ihme_loc_id == 'CHN_44533' & year_id %in% 2016:2018, stage2_pred := logit_asfr_data]

## saving for GPR
write.csv(gpr_input, paste0(out_dir, 'age_', model_age, '_results.csv'), row.names=F)
