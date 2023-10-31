##############################
## Purpose: Space-time smoothing
## Details: Calculate data density
##          Choose space-time parameters 
##          Smooth predictions
###############################

library(data.table)
library(mortdb, lib = 'FILEPATH')
library(mortcore, lib = 'FILEPATH')

if (interactive()){
  user <- Sys.getenv('USER')
  version_id <- x
  gbd_year <- x
  year_start <- x
  year_end <- x
  loop <- x
  model_age <- x
  birth_est_id <- x
} else {
  user <- Sys.getenv('USER')
  parser <- argparse::ArgumentParser()
  parser$add_argument('--version_id', type = 'character')
  parser$add_argument('--gbd_year', type = 'integer')
  parser$add_argument('--year_start', type = 'integer')
  parser$add_argument('--year_end', type = 'integer')
  parser$add_argument('--birth_est_id', type = 'integer')
  parser$add_argument('--loop', type = 'integer')
  parser$add_argument('--code_dir', type = 'character')
  parser$add_argument('--model_age', type = 'integer')
  args <- parser$parse_args()
  version_id <- args$version_id
  gbd_year <- args$gbd_year
  year_start <- args$year_start
  year_end <- args$year_end
  birth_est_id <- args$birth_est_id
  loop <- args$loop
  code_dir <- args$code_dir
  model_age <- args$model_age
}

source(paste0(code_dir, '/shared/space_time.R'))

gbd_round <- mortdb::get_gbd_round(gbd_year = gbd_year)

input_dir <- paste0("FILEPATH")
stage1_dir <- paste0("FILEPATH")
output_dir <- paste0("FILEPATH")
loop1_dir <- paste0("FILEPATH")

data <- assertable::import_files(list.files("FILEPATH"), full.names = T, FUN=fread)
pred <- assertable::import_files(list.files("FILEPATH"), full.names = T, 
                                 FUN=fread)
loc_map <- fread(paste0("FILEPATH"))
model_locs <- fread(paste0("FILEPATH"))

st_data <- merge(pred, data[,.(loc_source, source_name, method_name, year_id, ihme_loc_id, location_id, region_id, age_group_id, nid, 
                               asfr_data, sample_size, outlier, reference, logit_asfr_data, adjustment, adjusted_asfr_data)],
                 by = c('year_id', 'age_group_id', 'location_id', 'ihme_loc_id'), all=T)
## Get residuals
st_data[, resid := stage1_pred - adjusted_asfr_data]
st_data[outlier == 1, resid := NA]
st_data <- st_data[,.(resid = mean(resid, na.rm=T)), by=c('ihme_loc_id', 'year_id', 'age')]

###############################################
#########   Parameter Selection   #####################################################################################################
###############################################

## Get births for VR density
births <- get_mort_outputs('birth', 'estimate', run_id = birth_est_id, sex_ids = 3, age_group_ids = model_age/5 + 5)

## Assuming 2019 equals 2020
prev_gbd_year <- get_gbd_year(gbd_round = gbd_round - 1)
prev_births <- births[year_id == prev_gbd_year]
current_births <- copy(prev_births)
current_births <- current_births[, year_id := gbd_year - 1]
future_births1 <- copy(prev_births)
future_births1 <- future_births1[, year_id := gbd_year]

births <- rbind(births, current_births, future_births1)

## Identify missing locations due to round location map differences
new_locs <- setdiff(loc_map$ihme_loc_id, unique(births$ihme_loc_id))
common_locs <- unique(substr(new_locs, 1, 4))

## Calculate data density 
vr_complete <- unique(data[outlier != 1 & source_name == 'VR_complete', .(ihme_loc_id, nid, year_id)])
vr_incomplete <- unique(data[outlier != 1 & source_name == 'VR_incomplete', .(ihme_loc_id, nid, year_id)])

## Count number of sources instead of source years for non-vr
cbh <- unique(data[outlier != 1 & method_name == 'CBH', .(ihme_loc_id, nid)])
sbh <- unique(data[outlier != 1 & grepl('SBH', method_name), .(ihme_loc_id, nid)])
other <- unique(data[outlier != 1 & !grepl('VR', source_name) & method_name != 'CBH' &  !grepl('SBH', method_name), 
                     .(ihme_loc_id, nid)])

## Weight VR years with fewer than 100 births lower
vr_complete <- merge(vr_complete, births[,.(ihme_loc_id, year_id, age_group_id, mean)], by=c('ihme_loc_id', 'year_id'), all.x=T)
vr_complete[, dd := mean/100]
vr_complete[dd > 1, dd:=1]

vr_incomplete <- merge(vr_incomplete, births[,.(ihme_loc_id, year_id, age_group_id, mean)], by=c('ihme_loc_id', 'year_id'), all.x=T)
vr_incomplete[, dd := mean/100]
vr_incomplete[dd > 1, dd:=1]

vr_complete <- vr_complete[, .(vr_complete=sum(dd)), by=ihme_loc_id]
vr_incomplete <- vr_incomplete[, .(vr_incomplete=sum(dd)), by=ihme_loc_id]
cbh <- cbh[, .(cbh=.N), by=ihme_loc_id]
sbh <- sbh[, .(sbh=.N), by=ihme_loc_id]
other <- other[, .(other=.N), by=ihme_loc_id]

no_data <- data.table('ihme_loc_id' = unique(pred[,ihme_loc_id])[!unique(pred[,ihme_loc_id]) %in% unique(data[outlier == 0, ihme_loc_id])])

params <- Reduce(function(x, y) merge(x, y, by='ihme_loc_id', all=T), list(vr_complete, vr_incomplete, cbh, sbh, other, no_data))

params[is.na(params)] <- 0

## Use this equation: data_density = complete_vr + 2*cbh + 0.25*sbh + 0.5*incomplete_vr
params[, dd := vr_complete + (2*cbh) + (0.25*sbh) + (0.5*vr_incomplete) + (1*other)]

## Use average of common subnationals to replace dd for new subnationals
common_ids <- setdiff(names(params), "ihme_loc_id")
common_params <- params[ihme_loc_id %like% common_locs & !ihme_loc_id %in% new_locs]
common_params[, ihme_loc_id := substr(ihme_loc_id, 1, 4)]
common_params <- common_params[, lapply(.SD, mean), .SDcols = common_ids, by = "ihme_loc_id"]

new_params <- data.table()
for(loc in new_locs) {
  temp <- common_params[ihme_loc_id == substr(loc, 1, 4)]
  temp[, ihme_loc_id := loc]
  new_params <- rbind(temp, new_params)
}

params <- params[!ihme_loc_id %in% new_locs]
params <- rbind(params, new_params)

## Choose parameters based on dd
params[dd>=50, c('zeta', 'beta', 'scale') := .(0.99, 500, 5)]
params[dd<50 & dd>=30, c('zeta', 'beta', 'scale') := .(0.90, 100, 10)]
params[dd<30 & dd>=20, c('zeta', 'beta', 'scale') := .(0.80, 20, 15)]
params[dd<20 & dd>=10, c('zeta', 'beta', 'scale') := .(0.70, 15, 15)]
params[dd<10, c('zeta', 'beta', 'scale') := .(0.60, 10, 15)]

#############################
## Custom parameter choosing -- MOVE TO FLAT FILE?#####################################################################################
##############################

custom_params <- fread(paste0("FILEPATH"))
params[match(custom_params[!is.na(zeta), ihme_loc_id], params[,ihme_loc_id])]$zeta <- custom_params[!is.na(zeta), zeta]
params[match(custom_params[!is.na(beta), ihme_loc_id], params[,ihme_loc_id])]$beta <- custom_params[!is.na(beta), beta]
params[match(custom_params[!is.na(scale), ihme_loc_id], params[,ihme_loc_id])]$scale <- custom_params[!is.na(scale), scale]

params[grepl("USA_", ihme_loc_id), beta := beta * 1.5]
params[grepl("GBR_", ihme_loc_id) & beta < 400, beta := beta * 2]

params[, use_super_reg := (ihme_loc_id == 'PRK')]

st_data <- merge(st_data, loc_map[,.(ihme_loc_id, super_region_id, region_id)], by='ihme_loc_id')

## Checks
assertable::assert_ids(params, list('ihme_loc_id'=model_locs$ihme_loc_id))
assertable::assert_values(params, c('scale', 'zeta', 'dd', 'beta'), test='not_na', test_val = NA)

readr::write_csv(params, paste0("FILEPATH"))

###############################################
#########   Space-Time Smoothing   ####################################################################################################
###############################################

smoothed <- data.table()
for (i in 1:nrow(params)){
  st_pred <- space_time_beta(st_data, loc = params$ihme_loc_id[i], beta = params$beta[i], zeta = params$zeta[i], min_year = year_start,
                             max_year = year_end, loc_map_path = paste0("FILEPATH"), use_super_reg = params$use_super_reg[i],
                             regression = F, data_density = params$dd[i])
  smoothed <- rbind(smoothed, st_pred)
}

setnames(smoothed, c('resid.V1', 'year'), c('pred2_resid', 'year_id'))

pred <- merge(pred, smoothed, by=c('ihme_loc_id', 'year_id'))
pred[, stage2_pred := stage1_pred - pred2_resid]

## Checks
assertable::assert_values(pred, 'stage2_pred', test='not_na', test_val = NA)
assertable::assert_ids(pred, list('year_id'=year_start:year_end, 'ihme_loc_id'=model_locs$ihme_loc_id), assert_combos=T)

## Save stage 2 predictions
readr::write_csv(pred, paste0("FILEPATH"))

###############################################
#############   Prep GPR Input   ######################################################################################################
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


gpr_input <- merge(data[,.(ihme_loc_id, year_id, age, region_name, super_region_name, nid, asfr_data, logit_asfr_data, adjusted_asfr_data, variance, 
                           logit_variance, category, source_name, adjustment, outlier)], 
                   pred[,.(ihme_loc_id, year_id, age, stage1_pred, stage2_pred)], by=c('ihme_loc_id', 'year_id', 'age'), all=T)
gpr_input[outlier == 1, asfr_data := NA]
gpr_input[, data := 0]
gpr_input[!is.na(asfr_data), data := 1]

## Calculate MSE
gpr_input[, diff := stage2_pred - stage1_pred]
vr_only <- merge(gpr_input[grepl('VR', source_name), .N, by=ihme_loc_id], 
                 gpr_input[!is.na(source_name), .N, by=ihme_loc_id], by='ihme_loc_id')
vr_only <- vr_only[N.x >= 40 & N.x == N.y, ihme_loc_id]
gpr_input[ihme_loc_id %in% vr_only & !is.na(logit_asfr_data), diff := stage2_pred - logit_asfr_data]
gpr_input[outlier == 1, diff := NA]
gpr_input[!grepl('_', ihme_loc_id) & year_id >= 1990, mse := var(diff, na.rm=T), by='ihme_loc_id']
global_mse <- gpr_input[,mean(mse, na.rm=T)]
jpn_mse <- gpr_input[ihme_loc_id == 'JPN', mean(mse, na.rm=T)]
gpr_input[,mse := global_mse/2]
gpr_input[ihme_loc_id == 'JPN', mse := jpn_mse]

## Creating data variance
nat_var <- copy(gpr_input)
nat_var[,diff := logit_asfr_data - stage2_pred]
nat_var[outlier == 1, diff := NA]
setkey(nat_var, ihme_loc_id)
nat_var <- nat_var[,.(year_id=year_id, data_var=var(diff, na.rm=T)), by=key(nat_var)]
setkey(nat_var, NULL)
nat_var <- unique(nat_var)
gpr_input <- merge(gpr_input, nat_var, by=c('year_id', 'ihme_loc_id'))
gpr_input[, logit_variance := data_var]
gpr_input[ihme_loc_id == 'CHN_44533' & nid %in% c(426273, 426273, 353530, 353533, 357371, 415956, 415956)
          & year_id > 2010, data_var := data_var/0.05]

## get low data locs
low_data_locs <- params[dd<5]
low_data_locs <- low_data_locs$ihme_loc_id

## for locations in with low data, use the maximum data variance in their region as their data variance
gpr_input[,max_region_var := max(logit_variance, na.rm = T), by='region_name']
gpr_input[ihme_loc_id %in% low_data_locs, logit_variance := max_region_var]

## old/young: if no regional variance, use super region
gpr_input[,super_region_var := max(logit_variance, na.rm=T), by='super_region_name']
gpr_input[ihme_loc_id %in% low_data_locs & logit_variance == -Inf, logit_variance := super_region_var]

gpr_input[,super_region_var := NULL]
gpr_input[,max_region_var := NULL]

assertable::assert_values(gpr_input, 'mse', test='not_na')
assertable::assert_values(gpr_input[data==1], 'logit_variance', test='not_na')

gpr_input <- merge(gpr_input, params[,.(ihme_loc_id, scale)], by='ihme_loc_id')

readr::write_csv(gpr_input, paste0("FILEPATH"))

