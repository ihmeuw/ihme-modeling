library(pacman)
library(data.table)
library(lme4)
library(arm)
library(assertable)
library(readr)
library(splines)
library(mortdb, lib = 'FILEPATH')
library(mortcore, lib = 'FILEPATH')
rm(list=ls())

if (interactive()){
  user <- Sys.getenv('USER')
  version_id <- x
  gbd_year <- x
  year_start <- x
  year_end <- x
  loop <- x
  model_age <- x
} else {
  user <- Sys.getenv('USER')
  parser <- argparse::ArgumentParser()
  parser$add_argument('--version_id', type = 'character')
  parser$add_argument('--gbd_year', type = 'integer')
  parser$add_argument('--year_start', type = 'integer')
  parser$add_argument('--year_end', type = 'integer')
  parser$add_argument('--loop', type = 'integer')
  parser$add_argument('--model_age', type = 'integer')
  args <- parser$parse_args()
  version_id <- args$version_id
  gbd_year <- args$gbd_year
  year_start <- args$year_start
  year_end <- args$year_end
  loop <- args$loop
  model_age <- args$model_age
}

source(paste0('FILEPATH/shared/space_time.R'))

gbd_round <- mortdb::get_gbd_round(gbd_year = gbd_year)

base_dir <- paste0("FILEPATH")
input_dir <- paste0("FILEPATH")
output_dir <- paste0("FILEPATH")
gpr_dir <- paste0("FILEPATH")
loop1_dir <- paste0("FILEPATH")


## Read in model_locs
all_locs <- fread(paste0("FILEPATH"))[is_estimate == 1]
if (model_age == 20) {
  model_locs <- all_locs[,ihme_loc_id]
} else {
  model_locs <- fread(paste0("FILEPATH"))[,ihme_loc_id]
}

## Read in input data
input_data <- fread(paste0("FILEPATH"))
data <- input_data[age == model_age]


if (model_age == 10) {
  assertable::check_files(paste0("FILEPATH"), 
                          folder = gpr_dir, continual = F, display_pct = 0)
  asfr_adj <- assertable::import_files(list.files(gpr_dir, full.names = T, 
                                                  pattern = "FILEPATH"), FUN = fread)
  setnames(asfr_adj, 'mean', 'asfr_adj')
  asfr_adj[, year_id := floor(year)]
  data <- merge(data, asfr_adj[,.(year_id, ihme_loc_id, asfr_adj)], 
                by=c('year_id', 'ihme_loc_id'))
} else if (model_age == 50) {
  assertable::check_files(paste0("FILEPATH"), 
                          folder = gpr_dir, continual = F, display_pct = 0)
  asfr_adj <- assertable::import_files(list.files(gpr_dir, full.names = T, 
                                                  pattern = "FILEPATH"), FUN = fread)
  setnames(asfr_adj, 'mean', 'asfr_adj')
  asfr_adj[, year_id := floor(year)]
  data <- merge(data, asfr_adj[,.(year_id, ihme_loc_id, asfr_adj)],
                by=c('year_id', 'ihme_loc_id'))
}

data[!super_region_name %in% c('Central Europe, Eastern Europe, and Central Asia', 
                               'High-income','Sub-Saharan Africa'), 
     super_region_name := 'Others']

###############################################
##############   Model Fitting   ###############################################
###############################################

## Create variable to uniquely identify sources
data[,loc_source := paste0(ihme_loc_id,  '_', nid)]

## First fit model on standard locations
std_locs <- get_locations(gbd_type = 'standard_modeling', level = 'all', gbd_year = gbd_year)
nat_parents <- all_locs[level == 4, parent_id]
std_locs <- all_locs[location_id %in% unique(c(std_locs$location_id, nat_parents, 44533))]

std_data <- data[location_id %in% std_locs[,location_id]]

## Model form
form <- 'logit_asfr_data ~ fem_edu + asfr_adj'

#fit model on standard locations to get fixed effects
std_mod <- lm(formula = as.formula(form), data=std_data[outlier == 0])

#subtract coefficients from standard locations model, fit just a random intercept model to get REs
data[, pred := predict(std_mod, newdata = data)]

data[, adjustment := 0]
data[, adjusted_asfr_data := logit_asfr_data]

readr::write_csv(data, paste0("FILEPATH"))

###############################################
#################   Predict   ##################################################
###############################################

pred <- data.table(expand.grid(year_id = year_start:year_end, 
                               ihme_loc_id = model_locs, 
                               age = as.integer(as.character(model_age))))
fem_edu <- fread(paste0(loop1_dir, 'fem_edu.csv'))
pred <- merge(pred, fem_edu, by=c('age', 'year_id', 'ihme_loc_id'), all.x=T)
pred <- merge(pred, all_locs[,.(ihme_loc_id, super_region_name)], by='ihme_loc_id')
if (model_age != 20) pred <- merge(pred, asfr_adj[,.(year_id, ihme_loc_id, asfr_adj)], 
                                   by=c('year_id', 'ihme_loc_id'))

if(nrow(pred[is.na(fem_edu)])>0) {
  stop(paste('Missing female education covariate for the following locations:', 
              paste(pred[is.na(fem_edu), unique(ihme_loc_id)], collapse=', ')))
}

## Make predictions
pred[, stage1_pred := predict(std_mod, newdata = pred)]

## Save stage 1 predictions
readr::write_csv(pred, paste0("FILEPATH"))


## Get births for VR density
births <- get_mort_outputs('birth', 'estimate', gbd_year = gbd_year, sex_ids = 3, age_group_ids = model_age/5 + 5)

## Calculate data density 
vr_complete <- unique(data[outlier != 1 & source_name == 'VR_complete', .(ihme_loc_id, nid, year_id)])
vr_incomplete <- unique(data[outlier != 1 & source_name == 'VR_incomplete', .(ihme_loc_id, nid, year_id)])

## Count number of sources instead of source years for non-vr
cbh <- unique(data[outlier != 1 & method_name == 'CBH', .(ihme_loc_id, nid)])
sbh <- unique(data[outlier != 1 & grepl('SBH', method_name), .(ihme_loc_id, nid)])
other <- unique(data[outlier != 1 & !grepl('VR', source_name) & method_name != 'CBH' & method_name != 'SBH', 
                     .(ihme_loc_id, nid, year_id)])

## Weight VR years with fewer than 100 births lower
vr_complete <- merge(vr_complete, 
                     births[,.(ihme_loc_id, year_id, age_group_id, mean)], 
                     by=c('ihme_loc_id', 'year_id'), all.x=T)
vr_complete[, dd := mean/100]
vr_complete[dd > 1, dd:=1]

vr_incomplete <- merge(vr_incomplete, 
                       births[,.(ihme_loc_id, year_id, age_group_id, mean)], 
                       by=c('ihme_loc_id', 'year_id'), all.x=T)
vr_incomplete[, dd := mean/100]
vr_incomplete[dd > 1, dd:=1]

vr_complete <- vr_complete[, .(vr_complete=sum(dd)), by=ihme_loc_id]
vr_incomplete <- vr_incomplete[, .(vr_incomplete=sum(dd)), by=ihme_loc_id]
cbh <- cbh[, .(cbh=.N), by=ihme_loc_id]
sbh <- sbh[, .(sbh=.N), by=ihme_loc_id]
other <- other[, .(other=.N), by=ihme_loc_id]

no_data <- data.table('ihme_loc_id' = unique(pred[,ihme_loc_id])[!unique(pred[,ihme_loc_id]) %in% unique(data[outlier == 0, ihme_loc_id])])

params <- Reduce(function(x, y) merge(x, y, by='ihme_loc_id', all=T), 
                 list(vr_complete, vr_incomplete, cbh, sbh, other, no_data))

params[is.na(params)] <- 0

## Use equation: data_density = complete_vr + 2*cbh + 0.25*sbh + 0.5*incomplete_vr
params[, dd := vr_complete + (2*cbh) + (0.25*sbh) + (0.5*vr_incomplete) + (1*other)]

## Choose parameters based on dd
params[dd>=50, c('zeta', 'beta', 'scale') := .(0.99, 500, 5)]
params[dd<50 & dd>=30, c('zeta', 'beta', 'scale') := .(0.90, 100, 10)]
params[dd<30 & dd>=20, c('zeta', 'beta', 'scale') := .(0.80, 20, 15)]
params[dd<20 & dd>=10, c('zeta', 'beta', 'scale') := .(0.70, 15, 15)]
params[dd<10, c('zeta', 'beta', 'scale') := .(0.60, 10, 15)]

params[,use_super_reg := F]
params[ihme_loc_id == 'PRK', use_super_reg := T]

## Checks
assertable::assert_ids(params, list('ihme_loc_id'=model_locs))
assertable::assert_values(params, c('scale', 'zeta', 'dd', 'beta'), 
                          test='not_na', test_val = NA)

readr::write_csv(params, paste0(output_dir, 'age_', model_age, '_params.csv'))

###############################################
#########   Space-Time Smoothing   #############################################
###############################################

st_data <- merge(pred, data[,.(loc_source, source_name, method_name, year_id, 
                               ihme_loc_id, location_id, age_group_id, nid, 
                               asfr_data, sample_size, outlier, reference, 
                               logit_asfr_data, adjustment, adjusted_asfr_data)],
                 by = c('year_id', 'age_group_id', 'location_id', 'ihme_loc_id'), 
                 all=T)
## Get residuals
st_data[, resid := stage1_pred - adjusted_asfr_data]
st_data[outlier == 1, resid := NA]
st_data[,.(resid = mean(resid, na.rm=T)), by=c('ihme_loc_id', 'year_id', 'age')]

st_data <- merge(st_data, all_locs[,.(ihme_loc_id, region_id, super_region_id)], 
                 by='ihme_loc_id')


smoothed <- data.table()
for (i in 1:nrow(params)){
  st_pred <- space_time_beta(st_data, loc = params$ihme_loc_id[i], 
                             beta = params$beta[i], zeta = params$zeta[i], 
                             min_year = year_start, max_year = year_end, 
                             loc_map_path = paste0(loop1_dir, 'loc_map.csv'), 
                             use_super_reg = params$use_super_reg[i],
                             regression = F, data_density = params$dd[i])
  smoothed <- rbind(smoothed, st_pred)
}

setnames(smoothed, c('resid.V1', 'year'), c('pred2_resid', 'year_id'))

pred <- merge(pred, smoothed, by=c('ihme_loc_id', 'year_id'))
pred[, stage2_pred := stage1_pred - pred2_resid]

## Checks
assertable::assert_values(pred, 'stage2_pred', test='not_na', test_val = NA)
assertable::assert_ids(pred, list('year_id' = year_start:year_end, 
                                  'ihme_loc_id' = model_locs), assert_combos=T)

## Save stage 2 predictions
readr::write_csv(pred, paste0("FILEPATH"))


###############################################
#############   Prep GPR Input   ###############################################
###############################################

## Fix this
data[,category := 'other']
data[source_name == 'VR_complete', category := 'vr_unbiased']
data[source_name == 'VR_incomplete', category := 'vr_other']

## Get bounds
bounds <- fread(paste0("FILEPATH"))[age == model_age]
data[,c('lower_bound', 'upper_bound') := bounds[,.(lower_bound, upper_bound)]]

## Calculate data variance
data[, variance := sample_size*asfr_data*(1-asfr_data)]
data[, logit_variance := (asfr_data*(1-asfr_data)/sample_size) * (((1/(asfr_data-lower_bound)) + (1/(upper_bound-asfr_data)))^2)] 

data[, adjusted_asfr_data := logit_asfr_data]
data[, adjustment := 0]
gpr_input <- merge(data[,.(ihme_loc_id, year_id, age, region_name, 
                           super_region_name, nid, asfr_data, logit_asfr_data, 
                           adjusted_asfr_data, variance, logit_variance, 
                           category, source_name, adjustment, outlier)], 
                   pred[,.(ihme_loc_id, year_id, age, stage1_pred, stage2_pred)],
                   by=c('ihme_loc_id', 'year_id', 'age'), all=T)
gpr_input[outlier == 1, asfr_data := NA]
gpr_input[, data := 0]
gpr_input[!is.na(asfr_data), data := 1]

## Calculate MSE
gpr_input[, diff := stage2_pred - stage1_pred]
vr_only <- merge(gpr_input[grepl('VR', source_name), .N, by=ihme_loc_id], 
                 gpr_input[!is.na(source_name), .N, by=ihme_loc_id], 
                 by='ihme_loc_id')
vr_only <- vr_only[N.x >= 40 & N.x == N.y, ihme_loc_id]
gpr_input[ihme_loc_id %in% vr_only & !is.na(logit_asfr_data), 
          diff := stage2_pred - logit_asfr_data]
gpr_input[outlier == 1, diff := NA]
gpr_input[!grepl('_', ihme_loc_id) & year_id >= 1990, mse := var(diff, na.rm=T), 
          by='ihme_loc_id']

global_mse <- gpr_input[,mean(mse, na.rm=T)]
jpn_mse <- gpr_input[ihme_loc_id == 'JPN', mean(mse, na.rm=T)]
gpr_input[,mse := global_mse]
gpr_input[ihme_loc_id == 'JPN', mse := jpn_mse]

## Creating data variance
nat_var <- copy(gpr_input)
nat_var[,diff := logit_asfr_data - stage2_pred]
nat_var[outlier == 1, diff := NA]
setkey(nat_var, ihme_loc_id)
nat_var <- nat_var[,.(year_id=year_id, data_var=var(diff, na.rm=T)), 
                   by=key(nat_var)]
setkey(nat_var, NULL)
nat_var <- unique(nat_var)
gpr_input <- merge(gpr_input, nat_var, by=c('year_id', 'ihme_loc_id'))
gpr_input[, logit_variance := data_var]
gpr_input[ihme_loc_id == 'CHN_44533' & nid %in% c(426273, 426273, 353530, 353533, 
                                                  357371, 415956, 415956)
          & year_id > 2010, data_var := data_var/1000]

## get low data locs
low_data_locs <- params[dd<5]
low_data_locs <- low_data_locs$ihme_loc_id

## for locations in with low data, use the maximum data variance in their *super* 
## region as their data variance this is different from other age groups which 
## use region b/c there's more data
gpr_input[,max_region_var := max(logit_variance, na.rm = T), by='super_region_name']
gpr_input[ihme_loc_id %in% low_data_locs, logit_variance := max_region_var]

gpr_input[,max_region_var := NULL]

assertable::assert_values(gpr_input, 'mse', test='not_na')
assertable::assert_values(gpr_input[data==1], 'logit_variance', test='not_na')

gpr_input <- merge(gpr_input, params[,.(ihme_loc_id, scale)], by='ihme_loc_id')

readr::write_csv(gpr_input, paste0("FILEPATH"))

