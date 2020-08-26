###########################################################
## Purpose: Fit model for for 10-14 and 50-54 age groups
###########################################################
sessionInfo()
rm(list=ls())


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

print(version)

## libraries
library(data.table)
library(haven)
library(lme4)
library(ggplot2)
library(splines)
library(parallel)
library(magrittr)
library(MASS)
library(bindrcpp)
library(glue)
library(mvtnorm)

## set global options
draws <-F

## set directories
mort_function_dir <- FILEPATH
central_function_dir <- FILEPATH
fert_function_dir <- FILEPATH

base_dir <- FILEPATH

jbase <- FILEPATH
data_dir <- FILEPATH
out_dir <- FILEPATH
param_dir <- FILEPATH
j_data_dir <- FILEPATH
diagnostic_dir <- FILEPATH
j_results_dir <- FILEPATH
map_dir <- FILEPATH
final_results_dir <- paste0(base_dir, 'results/gpr/')
unraked_dir <- paste0(base_dir, 'results/gpr/unraked/')

## creating directories
dir.create(paste0(base_dir, 'results/gpr/age_10'))
dir.create(paste0(base_dir, 'results/gpr/age_50'))
dir.create(paste0(unraked_dir, '/age_10'))
dir.create(paste0(unraked_dir, '/age_50'))


## set model locs, get loc and age maps
model_locs <- data.table(get_locations(level='estimate', gbd_year = gbd_year))
model_locs <- model_locs$ihme_loc_id

age_map <- data.table(get_age_map())[,.(age_group_id, age_group_name_short)]
setnames(age_map, 'age_group_name_short', 'age')

reg_map <- data.table(get_locations(level='estimate'))[,.(region_name, super_region_name, ihme_loc_id)]

parents <- unique(substr(grep('_', model_locs, value = T),1 ,3))
parents_and_childs <- model_locs[grepl(paste(parents, collapse = '|'), model_locs)]
parents_and_childs <- parents_and_childs[!parents_and_childs %in% c('CHN_354', 'CHN_361')]
no_child <- model_locs[!model_locs %in% parents_and_childs]
parents[parents == 'CHN'] <- 'CHN_44533'



## read in data/mapping files
input_data <- readRDS(paste0(j_data_dir, 'asfr.RDs'))
input_data <- merge(input_data, age_map, all.x =T, by = 'age_group_id')
outliers <- fread(paste0(FILEPATH, 'outliered_sources.csv'))
asfr_cov <- fread(paste0(j_results_dir, 'compiled_summary_gpr.csv'))

outliers[,outlier := 1]
outliers <- outliers[,.(nid, ihme_loc_id, age, year_id, entire_timeseries, drop, outlier)]

yearly <- outliers[!is.na(year_id)]
setnames(yearly, 'outlier', 'outlier1')
input_data[,nid := as.character(nid)]
input_data[,age := as.numeric(age)]
input_data <- merge(input_data, yearly, by= c('ihme_loc_id', 'nid', 'year_id', 'age'), all.x=T)
input_data[, entire_timeseries := NULL]
input_data[is.na(drop), drop := 0]
input_data <- input_data[drop != 1]
input_data <- input_data[,drop := NULL]

timeseries <- outliers[entire_timeseries ==1]
timeseries[,year_id := NULL]

input_data <- merge(input_data, timeseries, by = c('ihme_loc_id', 'nid', 'age'), all.x = T)
input_data[outlier1==1, outlier := 1]
input_data[,outlier1 := NULL]
input_data[is.na(outlier), outlier :=0]
input_data[is.na(drop), drop := 0]
input_data <- input_data[drop != 1]
input_data[,entire_timeseries := NULL]
input_data <- input_data[,drop := NULL]


## young ages first
young_asfr <- copy(input_data)
young_asfr <- young_asfr[age == 10 | age == 15]

## reshape to get ratio
young_asfr[, age := paste0('asfr_', age)]
young_asfr <- young_asfr[!(nid == 253019 & year_id == 1990)]
young_asfr <- young_asfr[ihme_loc_id == 'MHL', val := max(val), by = c('nid', 'id', 'source_type', 'ihme_loc_id', 'year_id', 'outlier', 'age')]
young_asfr <- unique(young_asfr,  by = c('nid', 'id', 'source_type', 'ihme_loc_id', 'year_id', 'outlier', 'age'))
young_asfr <- dcast.data.table(young_asfr, nid + id + source_type + ihme_loc_id + year_id + outlier ~ age, value.var = 'val')
young_asfr <- young_asfr[!is.na(asfr_10) & !is.na(asfr_15)]

young_asfr[, ratio := asfr_10 / asfr_15]
young_asfr[,log_ratio := log(ratio)]
young_asfr[,log_asfr_15 := log(asfr_15)]

## merge on region, superregion for random effects test
young_asfr <- merge(young_asfr, reg_map, all.x = T, by = 'ihme_loc_id')

###################
## Model Fitting
###################

## young ASFR
form <-  paste0('log_ratio ~ log_asfr_15 + (1|super_region_name) + (1|region_name) + (1|ihme_loc_id)')

mod <- lmer(formula=as.formula(form), data=young_asfr[outlier == 0])

## ASFR 50-54
old_asfr <- copy(input_data)
old_asfr <- old_asfr[age == 45 | age == 50]

old_asfr[, age := paste0('asfr_', age)]
old_asfr <- old_asfr[ihme_loc_id == 'MHL', val := max(val), by = c('nid', 'id', 'source_type', 'ihme_loc_id', 'year_id', 'outlier', 'age')]
old_asfr <- unique(old_asfr,  by = c('nid', 'id', 'source_type', 'ihme_loc_id', 'year_id', 'outlier', 'age'))

old_asfr <- dcast.data.table(old_asfr, nid + id + source_type + ihme_loc_id + year_id + outlier ~ age, value.var = 'val')
old_asfr <- old_asfr[!is.na(asfr_45) & !is.na(asfr_50)]
old_asfr[,log_ratio := log(asfr_50 / asfr_45)]

mod2 <- lm(form = 'log_ratio ~ 1', data = old_asfr[outlier ==0])

save.image(paste0(out_dir, 'young_old_asfr_workspace.RData'))

