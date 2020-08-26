##############################
## Purpose: First stage regression, second stage space-time smoothing. Data Processing and Bias Adjustment.
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
library(broom)
library(readr)
library(boot)


if (interactive()){
  username <- USERNAME
  root <- FILEPATH
  model_age <-
  version <-
  loop <-
  super_reg <-
  gbd_year <-
  year_start <-
  year_end <-
} else {
  username <- USERNAME
  root <- FILEPATH
  model_age <- commandArgs(trailingOnly = T)[1]
  version <- commandArgs(trailingOnly = T)[2]
  loop <- commandArgs(trailingOnly = T)[3]
  super_reg <- commandArgs(trailingOnly = T)[4]
  gbd_year <- commandArgs(trailingOnly = T)[5]
  year_start <- commandArgs(trailingOnly = T)[6]
  year_end <- commandArgs(trailingOnly = T)[7]
}


if(super_reg == 'Africa') super_reg <- 'Sub-Saharan Africa'
if(super_reg == 'central_europe_eastern_europe_central_asia') super_reg <- 'Central Europe, Eastern Europe, and Central Asia'
super_reg1 <- super_reg
if(super_reg == 'others') super_reg <- c('Southeast Asia, East Asia, and Oceania', 'Latin America and Caribbean', 'North Africa and Middle East', 'South Asia')
print(super_reg)


## set directories
fert_function_dir <- FILEPATH

base_dir <- FILEPATH

jbase <- FILEPATH
data_dir <-FILEPATH
out_dir <- FILEPATH
param_dir <- FILEPATH
j_data_dir <- FILEPATH
diagnostic_dir <- FILEPATH
j_results_dir <-FILEPATH

source(paste0(fert_function_dir, 'space_time.r'))
source(paste0(fert_function_dir, 'identify_outliers.R'))
source(paste0(fert_function_dir, 'deduplicate_input_data.R'))

inv_logit <- function(x){
  return(exp(x)/(1+exp(x)))
}


## set model locs
locations <- get_locations(level = 'estimate', gbd_year = gbd_year)
targets = data.table(locations)
gbd_standard <- get_locations(gbd_type = 'standard_modeling', level = 'all', gbd_year = gbd_year)
mortality_locations <- get_locations(gbd_type = 'ap_old', level = 'estimate', gbd_year = gbd_year)
national_parents <- mortality_locations[level == 4, parent_id]
standard_with_metadata <- mortality_locations[location_id %in% unique(c(gbd_standard$location_id, national_parents, 44533))]

targets[location_id %in% standard_with_metadata$location_id, primary := T]
targets[is.na(primary), primary := F]
targets[, secondary := T]

model_locs <- data.table(get_locations(level='estimate', gbd_year = gbd_year))
model_locs <- model_locs[!grepl('SAU_', ihme_loc_id)]
model_locs <- model_locs[super_region_name %in% super_reg]

model_locs <- model_locs$location_id

loc_map <- data.table(get_locations(level='estimate', gbd_year = gbd_year))
loc_map <- loc_map[!grepl('SAU_', ihme_loc_id)]
nat_locs <- copy(loc_map)
reg_map <- copy(loc_map)
loc_map <- loc_map[,.(location_id, ihme_loc_id)]
nat_locs <- nat_locs[level==3| ihme_loc_id %in% c('CHN_44533', 'CHN_361', 'CHN_354',
                                                  'GBR_433', 'GBR_434', 'GBR_4636', 'GBR_4749')]
nat_locs <- nat_locs[!ihme_loc_id %in% c('CHN', 'GBR')]
nat_locs <- nat_locs$ihme_loc_id
reg_map <- reg_map[,.(location_id, ihme_loc_id, region_name, super_region_name)]

age_map <- data.table(get_age_map())[,.(age_group_id, age_group_name_short)]
setnames(age_map, 'age_group_name_short', 'age')


## read in data/mapping files
input_data <- readRDS(paste0(FILEPATH, '/input_data.RDS'))

## get DDM version for completeness designation
ddm_version <- get_proc_version(model_name = 'ddm', model_type = 'estimate',
                                run_id = runID)

completeness <- get_mort_outputs(model_name = 'ddm', model_type = 'estimate', estimate_stage_ids = 11, run_id = ddm_version)
completeness[mean >= 0.95, complete := 1]
completeness[mean < 0.95, complete := 0]

missing_completeness <- input_data[source_type == 'vr', .(ihme_loc_id, year_id)]

missing_completeness <- merge(missing_completeness, completeness[source=='VR',.(ihme_loc_id, year_id, complete)], by=c('ihme_loc_id', 'year_id'))
missing_completeness[, loc_year := paste0(ihme_loc_id, '_', year_id)]


input_data <- input_data[age == model_age]

pop <- fread(paste0(FILEPATH, '/pop_input.csv'))
female_edu <- fread(paste0(FILEPATH, '/fedu_cov_input.csv'))
births <- fread(paste0(FILEPATH, '/dd_births.csv'))

## merge on covarites
female_edu <- female_edu[,.(location_id, year_id, sex_id, age_group_id, mean_value)]
setnames(female_edu, c('mean_value'), c('female_edu_yrs'))
female_edu <- female_edu[sex_id==2]
female_edu <- merge(female_edu, age_map, by='age_group_id')
input_data <- merge(input_data, female_edu, by=c('location_id', 'year_id', 'age'), all.x=T)

## merge on asfr 20
if(model_age !=20){
  asfr20 <-  tryCatch({
    fread(paste0(base_dir, 'results/gpr/compiled_summary_gpr.csv'))
  }, error = function(e) {
    print('Re-trying file with 30 second rest')
    Sys.sleep(30)
    fread(paste0(base_dir, 'results/gpr/compiled_summary_gpr.csv'))
  })


  asfr20 <- asfr20[age == 20]
  asfr20[,year_id := floor(year)]
  asfr20 <- merge(asfr20, loc_map, all.x=T, by='ihme_loc_id')
  asfr20 <- asfr20[,.(location_id, year_id, mean)]
  asfr20[,mean := logit(mean)]

  if(nrow(asfr20[is.na(mean)])>0) stop('Missing covariate values for ASFR age 20')

  setnames(asfr20, 'mean', 'asfr20')
  input_data <- merge(input_data, asfr20, all.x=T, by=c('location_id', 'year_id'))

}


#################
## Stage 1 Model
################

## designate the internal 'reference source' for the regression aka it's absorbed in the intercept
other_sources <- unique(input_data$source_type[input_data$source_type != 'vr_complete'])
input_data[,source_type:=factor(source_type,levels=c('vr_complete', other_sources),ordered=FALSE)]

input_data[ihme_loc_id == 'BRA' & grepl('vr', source_type) & year_id %in% 1970:1990, source_type := 'vr_incomplete_1970_1990']
input_data[ihme_loc_id == 'CHN_44533' & year_id < 1981 & grepl('vr', source_type), source_type := 'vr_incomplete_tb']
input_data[ihme_loc_id == 'IND' & age %in% 20:45 & source_type == 'srs' & year_id < 2000, source_type := 'srs_pre2000']
input_data[ihme_loc_id == 'IND' & age == 20 & grepl('cbh', source_type) & year_id < 2000, source_type := paste(source_type, 'pre2000', sep='_')]

## concatenate to get a loc_source RE
input_data[grepl('vr', source_type), loc_source := paste(location_id, source_type, sep='_')]
input_data[!grepl('vr', source_type),loc_source := paste(id, location_id, source_type, sep='_')]
input_data[ihme_loc_id == 'BRA' & grepl('vr', source_type), loc_source := ifelse(year_id <= 1993, paste(loc_source, 'pre1993', sep='_'), paste(loc_source, 'post1993', sep='_'))]
input_data[ihme_loc_id == 'COL' & grepl('vr', source_type), loc_source := ifelse(year_id %in% 1970:1990, paste(loc_source, '1970_1990', sep='_'), loc_source)]

knotsdf <- data.table(region_name = c(rep('High-income', 5), rep('Sub-Saharan Africa', 5), rep('others', 5), rep('Central Europe, Eastern Europe, and Central Asia', 5)), age = rep(seq(25, 45, 5), 4),
                      knot = c(NA, -2.25, -2, -2.25, -2.25, -1.75, -1.25, -1.3, -1.5, -1.75, -1.5, -1.3, -1.3, -2, -2.5, -1.5, -2, -1.75, -1.75, -2))
knots <- knotsdf[age == model_age & region_name == super_reg1, knot]

if(length(knots) == 0) knots <- NULL
if(length(knots) != 0){
  if(is.na(knots)) knots <- NULL
}

input_data <- merge(input_data ,reg_map, by=c('ihme_loc_id', 'location_id'), all.x=T)
input_data <- input_data[super_region_name %in% super_reg]

## implement standard locations
std_data <- input_data[location_id %in% targets[primary==T, location_id]]
input_data <- input_data[location_id %in% targets[secondary==T, location_id]]

if(model_age == 20){
  form <-  paste0('logit_asfr_data ~ female_edu_yrs  + (1|loc_source)')
} else if (model_age != 20 & super_reg1== 'High-income' ) {
  form <-  paste0('logit_asfr_data ~  bs(asfr20, degree = 1, knots = ', paste(knots, collapse = ', '), ') + (1|loc_source)')
} else {
  form <-  paste0('logit_asfr_data ~ female_edu_yrs +  bs(asfr20, degree = 1, knots = ', paste(knots, collapse = ', '), ') + (1|loc_source)')
}

## fit model on standard locations to get fixed effects
std_mod <- lmer(formula=as.formula(form), data=std_data[outlier == 0])

## subtract coefficients from standard locations model, fit just a random intercept model to get REs
input_data[, pred := predict(std_mod, newdata = input_data, re.form = ~0, allow.new.levels = T )]
std_intercept <- fixef(std_mod)[1]
input_data[, logit_asfr_data_2 := logit_asfr_data - pred + std_intercept]

re_mod <- lmer(logit_asfr_data_2 ~ 0 + (1|loc_source), data=input_data[outlier == 0])

locsource_randint <- as.data.frame(ranef(re_mod))
write_csv(locsource_randint, paste0(diagnostic_dir, super_reg1, model_age, '_locsource_randint.csv'))


spline_mod <- grepl('bs', form)


######################
## Data Adjustment
######################

## now extract REs by loc_source
loc_source_re <- data.frame(ranef(re_mod)$loc_source)
loc_source_re$loc_source <- rownames(loc_source_re)
loc_source_re <- data.table(loc_source_re)
setnames(loc_source_re, 'X.Intercept.', 'loc_source_re')

ref_loc_source_re <- copy(loc_source_re)
setnames(ref_loc_source_re, c('loc_source_re'), c('ref_loc_source_re'))

input_data[ihme_loc_id == 'PHL' & grepl('vr', source_type), source_type := 'vr_incomplete']
input_data[ihme_loc_id == 'PAN' & source_type == 'vr_incomplete' & year_id > 1975, source_type := 'vr_complete']
input_data[ihme_loc_id == 'THA' & source_type == 'vr_incomplete' & year_id > 1966, source_type := 'vr_complete']
input_data[ihme_loc_id == 'COL' & source_type == 'vr_complete', source_type := 'vr_incomplete']
input_data[ihme_loc_id == 'NIC' & source_type == 'vr_incomplete' & year_id > 1995, source_type := 'vr_complete']

input_data[ihme_loc_id == 'PRY' & year_id > 2010 & source_type == 'vr_incomplete', source_type := 'vr_complete']
input_data[ihme_loc_id == 'CHN_44533' & nid %in% c(415956, 329653), source_type := 'vr_complete']

## designate reference sources
all_sources <- unique(input_data[,.(ihme_loc_id, location_id, source_type, outlier)])
all_sources <- all_sources[outlier == 0]
ref_source_list <- copy(all_sources)
complete_vr_locs <- ref_source_list[grepl('vr_complete', source_type)]$ihme_loc_id

## choose vr_complete when it's there
ref_source_list <- ref_source_list[(grepl('vr_complete', source_type) & ihme_loc_id %in% complete_vr_locs) | !ihme_loc_id %in% complete_vr_locs]

## chose all CBH sources by default if no vr_complete
ref_source_list <- ref_source_list[grepl('vr_complete', source_type) | grepl('cbh', source_type)]

ref_source_list <- ref_source_list[!ihme_loc_id %in% c('BFA', 'KEN', 'IDN', 'CIV', 'COG')]
if(model_age == 45) ref_source_list <- ref_source_list[!ihme_loc_id %in% c('TUR')]

## if locations that have data don't have a reference source designation from above, make all sources
## 'reference', i.e. agnostic by reference source
data_locs <- unique(input_data$ihme_loc_id[!is.na(input_data$asfr_data)])
missing_locs <- data_locs[!data_locs %in% unique(ref_source_list$ihme_loc_id)]
agnostic_locs <- all_sources[ihme_loc_id %in% missing_locs]
ref_source_list <- rbind(ref_source_list, agnostic_locs)


##########################################
#### custom reference source designations
##########################################

ref_source_list[location_id == 139, source_type := 'other_survey_cbh']

# as per Rafael
if(loop == 1) ref_source_list[ihme_loc_id == 'MEX', source_type := 'dhs_cbh_tab']
if(loop == 2){
  ref_source_list[ihme_loc_id == 'MEX', source_type := 'mics_sbh']
  ref_source_list[grepl('MEX_', source_type), source_type := 'other_survey_sbh']
}
ref_source_list[ihme_loc_id == 'CIV', source_type := 'dhs_cbh']
ref_source_list[ihme_loc_id == 'BGD', source_type := 'dhs_cbh']

## convenience function to add reference sources
add_ref_source <- function(data, ihme_loc_ids, source_types, subnat = F){
  if(subnat == F) {
    add <- unique(data[ihme_loc_id %in% ihme_loc_ids & source_type %in% source_types, .(ihme_loc_id, location_id, source_type, outlier)])
  } else if (subnat == T){
    add <- unique(data[grepl(ihme_loc_ids, ihme_loc_id) & source_type %in% source_types, .(ihme_loc_id, location_id, source_type, outlier)])
  }
  return(rbind(ref_source_list, add))
}

ref_source_list <- add_ref_source(input_data, c('GIN'), 'census_tab')
ref_source_list <- add_ref_source(input_data, c('CIV', 'MOZ', 'NAM', 'STP'),
                                  'census_tab_sbh')

ref_source_list <- add_ref_source(input_data, 'BGD', c('vr_incomplete', 'vr_incomplete_tb', 'wfs_cbh'))
ref_source_list <- add_ref_source(input_data, 'BRA', 'other_survey_sbh')

ref_source_list <- add_ref_source(input_data, 'CHN_44533', c('census_tab', 'survey_unknown_recall_tab'))
ref_source_list <- add_ref_source(input_data, 'CMR', 'wfs_sbh')
ref_source_list <- add_ref_source(input_data, 'CPV', 'dhs_sbh')
ref_source_list[ihme_loc_id == 'DZA', source_type := 'mics_cbh']
ref_source_list <- add_ref_source(input_data, 'DZA', 'dyb_other')
ref_source_list <- add_ref_source(input_data, 'FJI', c('vr_incomplete', 'vr_incomplete_tb'))
ref_source_list <- add_ref_source(input_data, 'GEO', c('vr_incomplete', 'vr_incomplete_tb'))


ref_source_list <- add_ref_source(input_data, 'IND', c('sample_registration', 'srs', 'srs_pre2000'), subnat = T)
ref_source_list <- ref_source_list[!(ihme_loc_id == 'IND' & grepl('vr|sbh|cbh', source_type))]

ref_source_list <- ref_source_list[!(ihme_loc_id %in% c('IND_43894', 'IND_43930', 'IND_4863', 'IND_4863', 'IND_43895', 'IND_43903') & source_type %in% c('sample_registration', 'srs'))]
ref_source_list <- add_ref_source(input_data, c('IND_44538', 'IND_44539','IND_44540'), 'other_survey_cbh')


if(model_age == 45) ref_source_list <- ref_source_list[!(ihme_loc_id == 'TUR' & source_type == 'dyb_other')]
ref_source_list <- add_ref_source(input_data, 'KAZ', c('vr_incomplete', 'vr_incomplete_tb'))
ref_source_list <- ref_source_list[!(ihme_loc_id == 'KEN' & source_type == 'vr_incomplete_tb')]
ref_source_list <- add_ref_source(input_data, 'KOR', c('other_survey_recall', 'wfs_cbh', 'wfs_sbh'))
ref_source_list[ihme_loc_id == 'MAR', source_type := 'dhs_cbh']
ref_source_list[ihme_loc_id == 'MDG', source_type := 'dhs_cbh']
ref_source_list <- add_ref_source(input_data, 'MDA', c('vr_incomplete', 'vr_incomplete_tb'))
if(model_age != 45) ref_source_list <- add_ref_source(input_data, 'MDV', c('dhs_cbh', 'dhs_sbh'))
ref_source_list <- add_ref_source(input_data, 'MNG', c('stat', 'vr_incomplete', 'vr_incomplete_tb'))
if(model_age == 45) ref_source_list <- add_ref_source(input_data, 'MNG', c('vr_incomplete','vr_incomplete_tb'))
ref_source_list <- add_ref_source(input_data, 'JAM',c('vr_incomplete','vr_incomplete_tb'))
if(model_age >= 20) ref_source_list <- add_ref_source(input_data, 'LKA', 'dyb_other')
ref_source_list <- add_ref_source(input_data, 'NIC', c('census_sbh', 'dhs_sbh', 'other_survey_sbh', 'rhs_sbh'))
ref_source_list <- add_ref_source(input_data, 'NPL', 'mics_sbh')
ref_source_list <- ref_source_list[!(ihme_loc_id == 'NGA' & source_type == 'mis_cbh')]
ref_source_list <- ref_source_list[!(ihme_loc_id == 'PAK' & source_type == 'wfs_cbh' )]
ref_source_list <- add_ref_source(input_data, 'PAN', c('vr_incomplete','vr_incomplete_tb'))
ref_source_list <- ref_source_list[!(ihme_loc_id == 'PER' & source_type == 'other_survey_cbh')]
ref_source_list <- ref_source_list[!(ihme_loc_id == 'PHL' & source_type %in% c('vr_complete', 'survey_unknown_recall_tab'))]
ref_source_list <- add_ref_source(input_data, 'PHL', c('dhs_cbh', 'survey_unknown_recall_tab'))
ref_source_list <- add_ref_source(input_data, 'PHL', 'wfs_cbh')
if(loop ==2) ref_source_list <- ref_source_list[!(ihme_loc_id == 'PRY' & source_type == 'vr_incomplete')]
ref_source_list <- add_ref_source(input_data, 'RWA', c('mis_sbh', 'dhs_sbh', 'wfs_sbh', 'census_sbh', 'census_tab_sbh', 'other_survey_sbh'))
ref_source_list <- add_ref_source(input_data, 'SDN', 'dhs_sbh')
ref_source_list <- add_ref_source(input_data, 'SLV', c('vr_incomplete','vr_incomplete_tb'))
ref_source_list <- add_ref_source(input_data, 'STP', c('dhs_cbh', 'mics_cbh'))
ref_source_list <- add_ref_source(input_data, 'TCD', 'mics_sbh')
ref_source_list <- add_ref_source(input_data, 'TGO', 'dhs_sbh')
ref_source_list <- add_ref_source(input_data, 'TLS', 'dhs_sbh')
ref_source_list <- add_ref_source(input_data, 'TUN', c('vr_incomplete','vr_incomplete_tb'))
ref_source_list <- ref_source_list[!(ihme_loc_id == 'TUR' & source_type == 'other_survey_cbh')]
ref_source_list <- add_ref_source(input_data, 'TUR', c('vr_incomplete','vr_incomplete_tb'))
ref_source_list <- add_ref_source(input_data, 'TTO', c('vr_incomplete','vr_incomplete_tb', 'other_survey_sbh'))
ref_source_list <- add_ref_source(input_data, 'UKR', c('vr_incomplete','vr_incomplete_tb'))
ref_source_list <- add_ref_source(input_data, 'YEM', 'dhs_sbh')
ref_source_list <- add_ref_source(input_data, 'VNM', 'mics_sbh')
ref_source_list <- add_ref_source(input_data, 'KGZ', c('vr_incomplete', 'vr_incomplete_tb'))
ref_source_list <- add_ref_source(input_data, 'PRY', c('dhs_cbh', 'mics_cbh', 'rhs_cbh', 'wfs_cbh'))
ref_source_list <- add_ref_source(input_data, 'LBY', c('vr_incomplete_tb'))

idn_sub <- paste0('IDN_', c(4710,4711,4712,4714,4716,4720,4721,4722,4723,4724,4726,4727,4728,4730,4731,4735,4736,4739,4740,4741,4742))
ref_source_list <- ref_source_list[!(ihme_loc_id %in% idn_sub & !source_type %in% c('dhs_cbh', 'wfs_cbh'))]
ref_source_list <- ref_source_list[!(ihme_loc_id == 'KIR' & source_type == 'vr_incomplete_tb')]
ref_source_list <- ref_source_list[!(ihme_loc_id == 'ECU' & grepl('sbh', source_type))]
ref_source_list <- add_ref_source(input_data, 'ECU', c('cbh', 'dhs_cbh', 'wfs_cbh', 'rhs_cbh'))
ref_source_list <- ref_source_list[!(grepl('CHN', ihme_loc_id) & grepl('incomplete', source_type))]
ref_source_list <- add_ref_source(input_data, 'LKA', c('vr_complete', 'vr'))
ref_source_list <- ref_source_list[!(ihme_loc_id == 'LKA' & grepl('cbh', source_type))]
if(model_age != 45) ref_source_list <- ref_source_list[!(ihme_loc_id == 'AZE' & grepl('incomplete', source_type))]
ref_source_list <- ref_source_list[!(ihme_loc_id == 'ARM' & grepl('incomplete', source_type))]
ref_source_list <- ref_source_list[!(ihme_loc_id == 'MNG' & grepl('incomplete', source_type))]
ref_source_list <- add_ref_source(input_data, 'SRB', 'vr_incomplete')
ref_source_list <- ref_source_list[!(ihme_loc_id == 'ECU' & grepl('vr', source_type))]
ref_source_list <- ref_source_list[!(ihme_loc_id == 'SLV' & grepl('incomplete', source_type))]
ref_source_list <- add_ref_source(input_data, ref_source_list[grepl('MEX_', ihme_loc_id) & source_type == 'vr_complete', ihme_loc_id], 'vr_incomplete')
ref_source_list <- add_ref_source(input_data, 'EGY', 'dhs_cbh')
ref_source_list <- ref_source_list[!(ihme_loc_id == 'EGY' & source_type != 'dhs_cbh')]
ref_source_list <- ref_source_list[!(ihme_loc_id == 'SAU' & grepl('tb', source_type))]
ref_source_list <- ref_source_list[!(ihme_loc_id == 'AND' & source_type == 'vr_complete_tb')]
ref_source_list <- ref_source_list[!(ihme_loc_id == 'COG' & grepl('sbh', source_type))]
ref_source_list <- ref_source_list[!(ihme_loc_id == 'COD' & !grepl('cbh', source_type))]
ref_source_list <- ref_source_list[!(ihme_loc_id == 'ZMB' & source_type != 'dhs_cbh')]
ref_source_list <- add_ref_source(input_data, 'NIC', 'vr_complete')
ref_source_list <- add_ref_source(input_data, 'VEN', 'wfs_cbh')
ref_source_list <- ref_source_list[!(ihme_loc_id == 'GMA' & source_type != 'dhs_cbh')]
ref_source_list <- ref_source_list[!(ihme_loc_id == 'LBR' & source_type != 'dhs_cbh')]
ref_source_list <- ref_source_list[!(ihme_loc_id == 'CHN_44533' & source_type == 'cbh')]

ref_source_list <- add_ref_source(input_data, 'MDG', 'mics_cbh')
ref_source_list <- add_ref_source(input_data, 'NER', 'census_tab')
ref_source_list <- ref_source_list[!(ihme_loc_id == 'CHN_44533' & source_type == 'vr_incomplete_tb')]
ref_source_list <- ref_source_list[!(ihme_loc_id == 'CHN_44533' & source_type == 'vr_complete_tb')]
if(model_age == 45) ref_source_list <- ref_source_list[!(ihme_loc_id == 'BRN' & source_type == 'vr_complete_tb')]
ref_source_list <- add_ref_source(input_data, 'GBR_44788', 'vr_incomplete')
ref_source_list <- add_ref_source(input_data, 'THA', c('cesnsus_sbh', 'census_tab_sbh', 'dhs_sbh', 'mics_sbh', 'other_survey_sbh', 'wfs_sbh'))
ref_source_list <- add_ref_source(input_data, 'BRA_4759', 'dhs_cbh')
ref_source_list <- add_ref_source(input_data, 'TZA', c('ais_sbh', 'census_sbh', 'dhs_sbh', 'mis_sbh', 'other_survey_sbh'))
ref_source_list <- add_ref_source(input_data, 'MMR', c('census_sbh', 'census_tab_sbh', 'mics_sbh_tab', 'rhs_sbh'))
ref_source_list <- add_ref_source(input_data, 'TGO', 'mis_sbh')

ref_source_list <- add_ref_source(input_data, 'KOR', c('vr_incomplete', 'vr_incomplete_tb'))
ref_source_list <- add_ref_source(input_data, 'GUM', c('vr_incomplete', 'vr_incomplete_tb'))
ref_source_list <- add_ref_source(input_data, 'VCT', c('vr_incomplete', 'vr_incomplete_tb'))
ref_source_list <- add_ref_source(input_data, 'GRL', c('vr_incomplete', 'vr_incomplete_tb'))
ref_source_list <- add_ref_source(input_data, 'LSO', c('dhs_cbh'))
ref_source_list <- add_ref_source(input_data, 'MWI', c('dhs_cbh', 'mis_cbh'))

for(loc in c('UKR_', 'ITA_')){
ref_source_list <- add_ref_source(input_data, loc, c('vr_complete', 'vr_complete_tb'), subnat = T)
}
ref_source_list <- add_ref_source(input_data, 'CYP', c('vr_complete', 'vr_complete_tb'))
ref_source_list <- add_ref_source(input_data, 'UKR_', c('dhs_cbh', 'dhs_cbh_tab'), subnat = T)
ref_source_list <- add_ref_source(input_data, 'IND_4849', c('dhs_cbh', 'dhs_cbh_tab'))
ref_source_list <- add_ref_source(input_data, 'IND_4863', c('dhs_cbh', 'dhs_cbh_tab'))
ref_source_list <- ref_source_list[!(ihme_loc_id == 'IND_4863' & grepl('vr', source_type))]
ref_source_list <- ref_source_list[!(ihme_loc_id == 'MEX_4655' & !grepl('vr', source_type))]
ref_source_list <- add_ref_source(input_data[outlier==0], "PHL_", c('dhs_cbh', 'dhs_cbhs_tab'), subnat=T)
ref_source_list <- add_ref_source(input_data, 'RUS_44965', 'vr_complete')

missing_locs <- data_locs[!data_locs %in% unique(ref_source_list$ihme_loc_id)]
agnostic_locs <- all_sources[ihme_loc_id %in% missing_locs]
ref_source_list <- rbind(ref_source_list, agnostic_locs)

ref_source_list <- unique(ref_source_list)
ref_source_list[,age := model_age]

if(loop == 2) write.csv(ref_source_list, paste0(out_dir, 'reference_sources_', super_reg1, model_age, '.csv'), row.names = F)

ref_source_list[,age := NULL]

#####################################
## end custom ref source designations
######################################

## get reference loc-source random effects
ref_re <- merge(ref_source_list, unique(input_data[,.(ihme_loc_id, source_type, loc_source, location_id)]), all.x = T, by=c('ihme_loc_id', 'source_type', 'location_id'))
ref_re <- ref_re[!(loc_source == 'UNICEF_MULTIPLE_INDICATOR_CLUSTER_SURVEY')]
ref_re <- merge(ref_re, loc_source_re, by = 'loc_source', all.x=T)
ref_re <- ref_re[!is.na(loc_source_re)]
ref_re <- ref_re[,.(ref_loc_source_re = base::mean(loc_source_re, na.rm =T)), by =c('ihme_loc_id', 'location_id')]

## merging on reference loc-source REs
input_data <- merge(input_data, ref_re, by = c('ihme_loc_id', 'location_id'), all.x = T)

## merging on the loc-source REs and source_type FEs of the actual data point
input_data <- merge(input_data, loc_source_re, by = 'loc_source', all.x=T)

## making the complete vr adjustment 0, to fix/revisit
input_data[grepl('vr_complete', source_type), ref_loc_source_re := 0]
input_data[grepl('vr_complete', source_type), loc_source_re := 0]


## making sure nothing is missing
if(nrow(input_data[is.na(ref_loc_source_re) & outlier == 0]) > 0) {
  test <- input_data[is.na(ref_loc_source_re) & outlier == 0]
  print(head(test))
  stop('missing reference loc source random effects')
}
print(input_data[is.na(loc_source_re) & outlier == 0])
if(nrow(input_data[is.na(loc_source_re) & outlier == 0]) > 0) stop('missing loc source random effects')

## doing the actual adjustment
input_data[outlier == 1, adjusted_logit_asfr_data := logit_asfr_data]
input_data[outlier == 0, adjustment_factor :=  (ref_loc_source_re - loc_source_re)]

## making incomplete_vr 0 if it's being adjusted down
input_data[grepl('vr_incomplete', source_type) & adjustment_factor <0, adjustment_factor := 0]

ref_source_list[,outlier := NULL]
ref_source_list[, ref_source := 1]
ref_source_list <- unique(ref_source_list)
input_data <- merge(input_data, ref_source_list, by= c('ihme_loc_id', 'location_id', 'source_type'), all.x=T)
input_data[ref_source ==1, adjustment_factor := 0]
input_data[is.na(ref_source), ref_source := 0]

## don't adjust data away from reference source
ref_data <- input_data[ref_source == 1 & outlier == 0]
ref_data[, ref_asfr_data := mean(logit_asfr_data), by=c('ihme_loc_id', 'year_id')]
input_data <- merge(input_data, unique(ref_data[,.(ihme_loc_id, year_id, ref_asfr_data)]), by=c('ihme_loc_id', 'year_id'), all.x=T)
input_data[!ref_source & !is.na(ref_asfr_data), adjust := (logit_asfr_data > ref_asfr_data) == (loc_source_re > ref_loc_source_re)]
input_data[adjust == F, adjustment_factor := 0]
input_data[,c('ref_asfr_data', 'adjust') := NULL]

input_data[outlier == 0, adjusted_logit_asfr_data := logit_asfr_data + adjustment_factor]

if(nrow(input_data[is.na(adjusted_logit_asfr_data)]) > 0) stop('missing adjusted data')

#save input data w/ outliers and ref sources
if(loop==2)write.csv(input_data, paste0(data_dir, super_reg1, model_age, '_input_data.csv'), row.names=F)


####################
## Make Pred
####################


### create initial pred template
compare_manual_pred <- F
### merge on standard covariates needed for predictions
pred <- data.table(expand.grid(year_id = year_start:year_end, location_id = model_locs, age = model_age))
input_data[,age := as.numeric(as.character(age))]
pred[,age := as.numeric(as.character(age))]
female_edu[,age := as.numeric(age)]
pred <- merge(pred, loc_map, all.x=T, by='location_id')
pred <- merge(pred, female_edu, by=c('age', 'year_id', 'location_id'), all.x=T)
if(model_age != 20) pred <- merge(pred, asfr20, by=c('location_id', 'year_id'), all.x=T)
pred[, loc_source := 'pred']
pred[, stage1_pred_no_re := predict(std_mod, newdata = pred, re.form = ~0, allow.new.levels = T)] # predict without random effects
setnames(input_data, 'ihme_loc_id', 'loc')
pred[, ref_loc_source_re := input_data[loc == ihme_loc_id, unique(ref_loc_source_re)], by=ihme_loc_id]
pred[, stage1_pred_w_re := stage1_pred_no_re + ref_loc_source_re] # predict with random effects
pred[, c('loc_source', 'ref_loc_source_re') := NULL]
setnames(input_data, 'loc', 'ihme_loc_id')

coefs <- broom::tidy(std_mod)
write.csv(coefs, paste0(diagnostic_dir, super_reg1, model_age, '_coefs.csv'), row.names=F)

pred <- merge(pred, input_data[,.(location_id, year_id, age, nid, asfr_data, logit_asfr_data,id, outlier, source_type, loc_source, adjusted_logit_asfr_data, adjustment_factor)],
              by=c('year_id', 'age', 'location_id'), all = T)

if(nrow(pred[is.na(stage1_pred_no_re)]) >0 ) stop('missing stage 1 predictions')

message('Stage 1 predictions done')

#####################################################
## Set Parameters
## based on data density
## values from age-sex model
####################################################

params <- copy(pred)
params <- params[!is.na(asfr_data) & outlier ==0]
params <- params[,.(ihme_loc_id, age, year_id, nid, asfr_data, outlier, id, source_type, location_id, age_group_id)]

vr <- copy(params)
vr <- vr[grepl('vr', source_type) | source_type == 'sample_registration']

vr <- merge(vr, births, all.x = T, by= c('location_id', 'age_group_id', 'year_id'))
vr[,dd := births /100]
vr[dd >= 1, dd := 1]
vr <- vr[,.(dd = sum(dd)), by =c('ihme_loc_id', 'age', 'source_type')]

vr[,dd_source := source_type]
vr[dd_source == 'sample_registration', dd_source := 'vr_incomplete']
vr[,source_type := NULL]

nonvr <- copy(params)
nonvr <- nonvr[!grepl('vr', source_type) & source_type != 'sample_registration']
nonvr[grepl('cbh', source_type),dd_source := 'cbh']
nonvr[grepl('sbh', source_type),dd_source := 'sbh']

nonvr[is.na(dd_source), dd_source := 'other']
nonvr <- unique(nonvr, by= c('ihme_loc_id', 'age', 'dd_source', 'nid'))
nonvr <- subset(nonvr)[,.(dd = .N), by = c('ihme_loc_id', 'age', 'dd_source')]

dd <- rbind(vr, nonvr)

write.csv(dd, paste0(diagnostic_dir, 'data_density_by_source', model_age, '_', super_reg1, '.csv'), row.names=F)

dd[dd_source == 'vr_incomplete', dd := dd * 0.5]
dd[dd_source == 'cbh', dd := dd * 2]
dd[dd_source == 'sbh', dd := dd * 0.25]

setkey(dd, ihme_loc_id, age)
dd <- dd[,.(dd = sum(dd)), by=key(dd)]

locs <- data.table(get_locations(level='estimate', gbd_year=gbd_year))
locs <- locs[super_region_name %in% super_reg]
locs <- locs[,.(ihme_loc_id)]
dd <- merge(dd, locs, all = T, by=c('ihme_loc_id'))
dd[,age := model_age]
dd[is.na(dd), dd := 0]

params <- copy(dd)

params[dd>=50, lambda := 0.2]
params[dd>=50, zeta := 0.99]
params[dd>=50, scale := 5]

params[dd<50 & dd>=30, lambda := 0.4]
params[dd<50 & dd>=30, zeta := 0.9]
params[dd<50 & dd>=30, scale := 10]

params[dd<30 & dd>=20, lambda := 0.6]
params[dd<30 & dd>=20, zeta := 0.8]
params[dd<30 & dd>=20, scale := 15]

params[dd<20 & dd>=10, lambda := 0.8]
params[dd<20 & dd>=10, zeta := 0.7]
params[dd<20 & dd>=10, scale := 15]

params[dd<=10, lambda := 1]
params[dd<=10, zeta := 0.6]
params[dd<=10, scale := 15]

params[,amp2x := 1]
params[,best:=1]

params <- params[,.(ihme_loc_id, scale, amp2x, lambda, zeta, best, dd)]
params <- unique(params, by=c())

#############################
## Custom parameter choosing
##############################

params[ihme_loc_id %in% c('BMU', 'TKM', 'GEO'), lambda := 0.2]
params[ihme_loc_id %in% c('BMU', 'TKM', 'GEO'), zeta := 0.9]

params[ihme_loc_id == 'CAF', lambda := 0.4]
params[ihme_loc_id == 'COM', lambda := 0.5]

params[ihme_loc_id == 'BEN', lambda := 0.5]
ceb_locs <- c('BDI', 'BWA', 'CAF', 'CIV', 'GAB', 'GMB', 'LBR', 'MOZ', 'NAM', 'SEN', 'STP', 'SWZ', 'ZMB','BFA', 'GIN', 'NER', 'SEN', 'SWZ', 'CMR', 'TZA', 'MRT')
if(model_age == 20) params[ihme_loc_id %in% ceb_locs, lambda := 0.4]
if(model_age == 20) params[ihme_loc_id %in% c('COM'), lambda := 0.2]

params[ihme_loc_id == 'IND', lambda := 0.2]

params[ihme_loc_id == 'TON', zeta := 0.8]
params[ihme_loc_id == 'SGP', scale := 1]
params[ihme_loc_id == 'SGP', zeta := 0.99]
params[ihme_loc_id == 'JPN', scale := 1]
params[ihme_loc_id == 'ETH', lambda := 0.3]
params[ihme_loc_id == 'JAM', lambda := 0.2]
params[ihme_loc_id == 'YEM', lambda := 0.4] # 37

params[ihme_loc_id == 'CHN_44533', lambda := 0.2]
params[ihme_loc_id == 'CHN_44533', zeta := 0.95]
params[ihme_loc_id == 'CHN_44533', scale := 2]

if(model_age >=40) params[ihme_loc_id %in% c('BGR', 'CHE'), lambda := 0.1]

params[ihme_loc_id == 'KEN', scale := 15]
params[ihme_loc_id == 'KEN', lambda := 0.8]

params[ihme_loc_id == 'BTN', lambda := 0.6]

params[grepl('PHL_', ihme_loc_id), scale := 25]
params[grepl('PHL_', ihme_loc_id), lambda := 1] #47

params[ihme_loc_id == 'WSM', lambda := 0.6]
params[ihme_loc_id == 'IRN', lambda := 0.2]
if(model_age == 45) params[ihme_loc_id == 'CZE', lambda := 0.2]

if(model_age == 45) params[ihme_loc_id == 'DMA', lambda := 0.4]
params[grepl('ZAF_', ihme_loc_id), lambda := 0.6]
params[ihme_loc_id == 'BLR', lambda := 0.2]
params[ihme_loc_id == 'SYC', lambda := 0.2]
params[ihme_loc_id == 'USA_531', lambda := 0.2]
if(model_age == 15) params[ihme_loc_id == 'MLT', lambda := 0.2]
params[ihme_loc_id == 'TUR', lambda := 0.2]
params[ihme_loc_id == 'COG', lambda := 0.4]
params[ihme_loc_id == 'GAB', lambda := 0.2]
params[ihme_loc_id == 'NER' & lambda == 1, lambda := 0.4]
params[ihme_loc_id == 'NER' & lambda == 0.4, lambda := 0.2]
params[ihme_loc_id == 'KOR', lambda := 0.2]
params[grepl('ZAF_', ihme_loc_id), lambda := 0.2]
params[ihme_loc_id == 'TUN', lambda := 0.2]
params[ihme_loc_id == 'BLR', scale := 2]
params[grepl('MEX_', ihme_loc_id), lambda := 0.4]
params[ihme_loc_id == 'KHM', lambda := 0.2]
if(model_age == 20) params[ihme_loc_id == 'KGZ', lambda := 0.2]
params[ihme_loc_id == 'POL_53662', lambda := 0.2]
params[ihme_loc_id == 'POL_53665', lambda := 0.2]
params[ihme_loc_id == 'GRD', zeta := 0.99]
params[ihme_loc_id == 'KWT', scale := 2] # 72
params[ihme_loc_id == 'GEO', lambda := 2]

if (model_age == 45) params[grepl('KEN_', ihme_loc_id), zeta := 0.9]
if (model_age == 20) params[ihme_loc_id == 'BGD', lambda :=  0.2]

if (model_age == 35) params[ihme_loc_id == 'ROU', scale := 3]


write.csv(params, paste0(param_dir, 'asfr_params_age_', model_age,'_', super_reg1, '.txt'), row.names=F)
write.csv(params, paste0(j_results_dir, 'asfr_params_age_', model_age, '_', super_reg1, '.txt'), row.names=F)


################################
## Stage 2: Space-Time Smoothing
################################
if(model_age != 20){
  pred[,resid := stage1_pred_no_re - logit_asfr_data]
  pdf(paste0(diagnostic_dir, 'resid_to_asfr20', model_age, '_loop_', loop, '.pdf'))
  p <- ggplot(data = pred, aes(x = asfr20, y=resid)) + geom_point(alpha = 0.3, size = 0.5) +
    labs(title = paste0('Loop ', loop, ' Age ', model_age))
  print(p)
  dev.off()
  pred[,resid := NULL]
}

vr_locs <- input_data[source_type == 'vr_complete', .N, by='ihme_loc_id'][N > 40, ihme_loc_id]

## calculate residual
st_data <- copy(pred)
st_data[,resid := stage1_pred_no_re - adjusted_logit_asfr_data]
st_data[outlier == 1, resid := NA]

regs <- get_spacetime_loc_hierarchy(old_ap = F, gbd_year = gbd_year)
regs <- regs[!grepl('SAU_', ihme_loc_id)]
st_data <- merge(st_data, regs, by=c('location_id', 'ihme_loc_id'), all=T,allow.cartesian=TRUE)

setkey(st_data, ihme_loc_id, year_id, region_name, age, keep)
st_data <- st_data[,.(resid = mean(resid, na.rm=T)), by=key(st_data)]

temp <- unique(reg_map$region_name[reg_map$super_region_name %in% super_reg])
st_regions <- list()
for (rr in temp) st_regions[[paste0(rr)]] <-  unique(regs$region_name[grepl(rr, regs$region_name)])
st_regions <- unlist(st_regions)

message('Run space time')

st_pred <- resid_space_time_beta(data=st_data, min_year=year_start, max_year =year_end,
                             use_super_regs = 'East Asia_PRK', param_path = paste0(param_dir, 'asfr_params_age_', model_age, '_', super_reg1, '.txt'), regression = F, region = st_regions, vr_locs=vr_locs)


## process spacetime output
st_pred[,year:=floor(year)]
st_pred[,'weight':=NULL]
st_pred<- st_pred[keep==1]
st_pred[,'keep':=NULL]
setnames(st_pred, 'year', 'year_id')

st_pred <- merge(st_pred, pred, all=T, by=c('ihme_loc_id', 'year_id'))
st_pred[,stage2_pred:=stage1_pred_no_re-pred.2.resid]

write.csv(st_pred, paste0(out_dir, 'stage2_results_age', model_age, '_reg_', super_reg1, '.csv'), row.names=F)
write.csv(dd, paste0(out_dir, 'data_den_age', model_age, '_reg_', super_reg1, '.csv'), row.names=F)







