####################################################################################################################################################################
# 
# Author: AUTHOR
# Purpose: Crosswalking Network Meta-analysis for CSA alternate definitions, between study matches
#
####################################################################################################################################################################

rm(list=ls())

# set-up environment -----------------------------------------------------------------------------------------------------------------------------------------------

library(data.table)
library(dplyr)
library(openxlsx)
library(ggplot2)
library(stringr)
library(crosswalk, lib.loc = "FILEPATH")
library(locfit)

source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/get_crosswalk_version.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/upload_bundle_data.R")

date <- Sys.Date()
locs <- get_location_metadata(22)

# Read in data and get into correct matched format for within-study pairs ------------------------------------------------------------------------------------------
fdata <- get_bundle_version(35450, fetch='all')
mdata <- get_bundle_version(35453, fetch='all')
data <- rbind(fdata, mdata, fill=T)
bv_data <- copy(data)

#format
data[sex=='Female', sex_id:=2]
data[sex=='Male', sex_id:=1]
setnames(data, 'val', 'mean')

#subset to vacs data only
vacs_nids <- fread('FILEPATH')
data <- data[nid %in% vacs_nids$x]

#process data before using mr-brt -------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#save back-up of data before further processing
backup <- copy(data)

#get original age start and end information
data[, age_start:=age_start_orig]
data[, age_end:=age_end_orig]

# re-name SE
setnames(data, 'standard_error', 'se')

#remove unnecessary columns
data <- data[, c('sex_id', 'age_start', 'age_end', 'ihme_loc_id', 'nid', 'year_id', 'survey_name', 'vargroup', 'mean', 'se')]

#save an unmatched version of the data to apply adjustments to 
data_orig <- copy(data) 

# drop subnats
data <- data[!ihme_loc_id %like% '_']

# adjust zero means to lowest 97.5th percentile of non zero data
mean_adj_val <- quantile(data[mean!=0]$mean, probs=0.025)
data[mean==0, mean:=mean_adj_val]

#get subsets of gs and alternate defs
ref_subset <- copy(data) #for network, refvar can be any of the alternative definitions
alts_subset <- data[!vargroup=='contact_only'] #altvar is any of the alternative defs only

#set names accordingly
setnames(ref_subset, c('mean', 'se', 'vargroup'), c('ref_mean', 'ref_se', 'refvar'))
setnames(alts_subset, c('mean', 'se', 'vargroup'), c('alt_mean', 'alt_se', 'altvar'))

#merge back onto each other
matched <- merge(ref_subset, alts_subset, by=c('sex_id', 'age_start', 'age_end', 'ihme_loc_id', 'nid', 'year_id', 'survey_name'), allow.cartesian = T)

#get rid of rows that match the same def
onepoint_data <- matched[altvar==refvar]
matched <- matched[!altvar==refvar]

#remove duplicate indirect comparisons (B:C == C:B)
data <- copy(matched)

alt_defs <- unique(alts_subset$altvar)

for (i in 1:length(alt_defs)){
  for (j in 1:length(alt_defs)){
    data[refvar==alt_defs[i] & altvar==alt_defs[j], comparison_pair:=paste0(alt_defs[i], ' to ', alt_defs[j])]
    data[refvar==alt_defs[j] & altvar==alt_defs[i], comparison_pair:=paste0(alt_defs[i], ' to ', alt_defs[j])]
  }
}

data[!is.na(comparison_pair), duplicate_pair:=duplicated(comparison_pair), by=c('nid', 'age_start', 'age_end')]
data <- data[is.na(duplicate_pair) | duplicate_pair==FALSE]

data[, comparison_pair:=NULL]
data[, duplicate_pair:=NULL]

matched <- copy(data)

#get logit calcs using the delta transform package
logit_alt_means <- as.data.table(delta_transform(mean=matched$alt_mean, sd=matched$alt_se, transformation='linear_to_logit'))
setnames(logit_alt_means, c('mean_logit', 'sd_logit'), c('logit_alt_mean', 'logit_alt_se'))
logit_ref_means <- as.data.table(delta_transform(mean=matched$ref_mean, sd=matched$ref_se, transformation='linear_to_logit'))
setnames(logit_ref_means, c('mean_logit', 'sd_logit'), c('logit_ref_mean', 'logit_ref_se'))

#bind back onto main data table
matched <- cbind(matched, logit_alt_means)
matched <- cbind(matched, logit_ref_means)

matched[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
  df = matched, 
  alt_mean = "logit_alt_mean", alt_sd = "logit_alt_se",
  ref_mean = "logit_ref_mean", ref_sd = "logit_alt_se" )

#create sex covariate
matched[, sex_cov:=ifelse(sex_id==1, 1, 0)]

#drop non-contact & sex_acts_only definitions, bc no data with these alternate definitions are selected for modeling
matched <- matched[!refvar %like% 'non' & !altvar %like% 'non']
matched <- matched[!refvar %like% 'acts' & !altvar %like% 'acts']

# create separate male/female dts
matched_male <- matched[sex_id==1]
matched_female <- matched[sex_id==2]

# take out female only module for joint data set
matched <- matched[!nid==126418]

# Specify the data for model fitting -------------------------------------------------------------------------------------------------------------------------------------------------------

df <- CWData(
  df = matched,             # dataset for metaregression
  obs = "logit_diff",       # column name for the observation mean
  obs_se = "logit_diff_se", # column name for the observation standard error
  alt_dorms = "altvar",     # column name of the variable indicating the alternative method
  ref_dorms = "refvar",     # column name of the variable indicating the reference method
  dorm_separator = '.',
  covs = list(),
  study_id = 'nid',          # name of the column indicating group membership, usually the matching groups
  add_intercept = TRUE
)

# Fit the model ----------------------------------------------------------------------------------------------------------------------------------------------------------------------

fit_csa_network <- CWModel(
  cwdata = df,            # object returned by `CWData()`
  obs_type = "diff_logit", # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
  cov_models = list(       # specifying predictors in the model; see help(CovModel)
    CovModel("intercept")),
  gold_dorm = "contact_only",  # the level of `ref_dorms` that indicates it's the gold standard
  order_prior = list(c('contact_only', 'any_csa'),
                     c('intercourse_only', 'contact_only')),
  max_iter=100L, #default is 100
  inlier_pct=0.9 #set trimming to 10%
)

# Save results ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
# set 'version'
version <- 'vacs_data_sex_neutral_less_vars'

#write betas and beta sd to csv
dt <- data.table(beta=c(fit_csa_network$beta), beta_sd=c(fit_csa_network$beta_sd))
dt[, exp_beta:=exp(beta)]
write.csv(dt, paste0('FILEPATH', version, '_betas', date, '.csv'))

#save model object
py_save_object(object = fit_csa_network, filename = paste0('FILEPATH', version, '_modelobj_', date, '.pkl'), pickle = "dill")
