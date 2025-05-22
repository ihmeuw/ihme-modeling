##############################
## Purpose: Fit the ASFR 10-14 and 50-54 models
## Details: Fit hierarchical linear model to ratio of 10-15 ASFR and 15-19 ASFR
##          Apply an average ratio of 50-54 ASFR and 45-49 ASFR to all locations
##############################

library(data.table)
library(argparse)
library(haven)
library(lme4)
library(ggplot2)
library(splines)
library(parallel)
library(magrittr)
library(MASS)
library(glue)
library(mvtnorm)
library(mortdb)
library(mortcore)

sessionInfo()
rm(list=ls())


if (interactive()){
  username <- Sys.getenv('USER')
  root <- 'FILEPATH'
  version <- 'Run id'
  gbd_year <- 2020
  year_start <- 1950
  year_end <- 2022
} else {
  username <- Sys.getenv('USER')
  root <- '/home/j/'
  parser <- argparse::ArgumentParser()
  parser$add_argument('--version_id', type = 'character')
  parser$add_argument('--gbd_year', type = 'integer')
  parser$add_argument('--year_start', type = 'integer')
  parser$add_argument('--year_end', type = 'integer')
  args <- parser$parse_args()
  version_id <- args$version_id
  gbd_year <- args$gbd_year
  year_start <- args$year_start
  year_end <- args$year_end
}

print(version_id)

## set global options
draws <- FALSE

## set directories
input_dir <- "FILEPATH"
gpr_dir <- "FILEPATH"
out_dir <- "FILEPATH"

system(paste0('mkdir ', out_dir))

## set model locs, get loc and age maps
model_locs <- fread(paste0(input_dir, 'model_locs.csv'))
model_locs <- model_locs$ihme_loc_id

age_map <- fread(paste0(input_dir, 'age_map.csv'))
setnames(age_map, 'age_group_name_short', 'age')

reg_map <- fread(paste0(input_dir, 'loc_map.csv'))

## getting locations by whether it's a subnational or parent of a subnational
parents <- unique(substr(grep('_', model_locs, value = T),1 ,3))
parents_and_childs <- model_locs[grepl(paste(parents, collapse = '|'), model_locs)]
parents_and_childs <- parents_and_childs[!parents_and_childs %in% c('CHN_354', 'CHN_361')]
no_child <- model_locs[!model_locs %in% parents_and_childs]
parents[parents == 'CHN'] <- 'CHN_44533'

## read in data/mapping files
input_data <- fread(paste0(input_dir, 'input_data.csv'))
deprior_data <- readRDS('FILEPATH/tabs_dem_measure_id_2_all.RDS')
asfr_cov <- assertable::import_files(list.files(gpr_dir, full.names = T, pattern = 'gpr_[A-Z]{3}[_0-9A-Z]*.csv'), FUN=fread)

# prep dropped age 15 and 45 data from data prep (some sources are deprioritized
## and we want to bring them back to calculate ratios of the adjacent age
## groups, so they can be used in the old and young models)
deprior_data <- deprior_data[age_group_id == 8 | age_group_id == 14]
deprior_data <- deprior_data[!(ihme_loc_id == 'FSM' & year_id == 2000 & 
                                 nid == 268219 & source_type == 'census_tab')]
deprior_data <- unique(deprior_data[, .(ihme_loc_id, year_id, 
                                        nid = as.character(nid),
                                        age_group_id, val)])
deprior_data <- dcast(deprior_data, ...~age_group_id, value.var = "val")
setnames(deprior_data, c("8", "14"), c("deprior_asfr_15", "deprior_asfr_45"))

young_deprior_data <- deprior_data[!is.na(deprior_asfr_15)][, deprior_asfr_45 := NULL]
old_deprior_data <- deprior_data[!is.na(deprior_asfr_45)][, deprior_asfr_15 := NULL]

input_data[,nid := as.character(nid)]
input_data[,age := as.numeric(age)]

## young ages first
young_asfr <- copy(input_data)
young_asfr <- young_asfr[age == 10 | age == 15]

## reshape to get ratio
young_asfr[, age := paste0('asfr_', age)]
young_asfr <- young_asfr[!(nid == 253019 & year_id == 1990)]
young_asfr <- young_asfr[ihme_loc_id == 'MHL', asfr_data := max(asfr_data), by = c('nid', 'method_name', 'source_name', 'ihme_loc_id', 'year_id', 'outlier', 'age')]
young_asfr <- unique(young_asfr,  by = c('nid', 'source_name', 'method_name', 'ihme_loc_id', 'year_id', 'outlier', 'age'))
young_asfr <- dcast.data.table(young_asfr, nid + source_name + method_name + ihme_loc_id + year_id + outlier ~ age, value.var = 'asfr_data')

young_asfr <- merge(young_asfr, 
                    young_deprior_data, 
                    by = c('ihme_loc_id', 'year_id', 'nid'),
                    all.x = T)
young_asfr[is.na(asfr_15), asfr_15 := deprior_asfr_15]
young_asfr[, deprior_asfr_15 := NULL]

young_asfr <- young_asfr[!is.na(asfr_10) & !is.na(asfr_15)]

young_asfr[, ratio := asfr_10 / asfr_15]
young_asfr[,log_ratio := log(ratio)]
young_asfr[,log_asfr_15 := log(asfr_15)]

young_asfr <- young_asfr[asfr_10 <= 0.000001, outlier := 1]

## merge on region, super region for random effects test
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

## reshape to get ratio
old_asfr[, age := paste0('asfr_', age)]
old_asfr <- old_asfr[ihme_loc_id == 'MHL', asfr_data := max(asfr_data), by = c('nid', 'source_name', 'method_name', 'ihme_loc_id', 'year_id', 'outlier', 'age')]
old_asfr <- unique(old_asfr,  by = c('nid', 'source_name', 'method_name', 'ihme_loc_id', 'year_id', 'outlier', 'age'))

old_asfr <- dcast.data.table(old_asfr, nid + source_name + method_name + ihme_loc_id + year_id + outlier ~ age, value.var = 'asfr_data')

old_asfr <- merge(old_asfr, 
                  old_deprior_data, 
                  by = c('ihme_loc_id', 'year_id', 'nid'),
                  all.x = T)
old_asfr[is.na(asfr_45), asfr_45 := deprior_asfr_45]
old_asfr[, deprior_asfr_45 := NULL]

old_asfr <- old_asfr[!is.na(asfr_45) & asfr_45 != 0 & !is.na(asfr_50)]
old_asfr[,log_ratio := log(asfr_50 / asfr_45)]

## apply average ratio to all ages, but do it with a model to get variance/covariance from it
mod2 <- lm(form = 'log_ratio ~ 1', data = old_asfr[outlier == 0])

## saving model outputs to read into next step
save.image(paste0(out_dir, 'young_old_asfr_workspace.RData'))

## save model inputs
old_asfr[, c("asfr_45", "log_ratio") := NULL]
old_asfr[, age := 50]
setnames(old_asfr, "asfr_50", "asfr_data")

young_asfr[, age := 10]
setnames(young_asfr, "asfr_10", "asfr_data")
young_cols <- names(old_asfr)
young_asfr <- young_asfr[, ..young_cols]

old_young_asfr <- rbind(old_asfr, young_asfr)

readr::write_csv(old_young_asfr, paste0(input_dir, "old_young_model_data.csv"))
