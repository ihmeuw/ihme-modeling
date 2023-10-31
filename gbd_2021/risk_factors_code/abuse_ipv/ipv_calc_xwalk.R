####################################################################################################################################################################
# 
# Author: AUTHOR
# Purpose: Crosswalking Network Meta-analysis for IPV alternate definitions
#
####################################################################################################################################################################

rm(list=ls())

# set-up environment -----------------------------------------------------------------------------------------------------------------------------------------------

library(data.table)
library(dplyr)
library(openxlsx)
library(ggplot2)
library(crosswalk, lib.loc = "FILEPATH")

source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/upload_bundle_data.R")

## Function to fill out mean/cases/sample sizes
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}

## Function to calculate standard error
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure %in% c("prevalence","proportion"), standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

version <- 'network_meanadj97.5min_natonly'

# Read in data and get into correct matched format for within-study pairs ------------------------------------------------------------------------------------------

#Read in collapsed 2020 microdata and tabulated data
data_2020 <- fread('FILEPATH')
ipv_tabdata <- as.data.table(read.xlsx('FILEPATH', sheet = 'extraction'))
ipv_tabdata <- ipv_tabdata[2:nrow(ipv_tabdata),]
ipv_tabdata <- ipv_tabdata[!is.na(nid)]

# fill in missing measures from tab data
ipv_tabdata <- get_cases_sample_size(ipv_tabdata)
ipv_tabdata <- get_se(ipv_tabdata)

#bind together data
data <- rbind(data_2020, ipv_tabdata, fill=T)
data_raw <- copy(data)

#list of alt defs we will look for this network (the unique best available definitions in the data set)
xwalk_defs <- c('any_sexpv_lifetime', 'any_physpv_lifetime', 'any_sexpv_pastyr', 'any_physpv_pastyr', 'anysex_anyphys_ipv_lifetime', 'anysex_anyphys_ipv_pastyr',
                'severe_physpv_lifetime', 'severe_physpv_pastyr')

data <- data[var %in% xwalk_defs]

#map and filter sex_id
data[sex=='Female', sex_id:=2]
data <- data[sex_id==2]
data[, age_midpt:=(as.numeric(age_start)+as.numeric(age_end))/2]

# Create year_id variable
data[, year_id:=floor((year_start+year_end)/2)]

# Set zero mean adjustment value 
mean_adj_val <- quantile(data[mean!=0]$mean, probs=0.025)
setnames(data, 'standard_error', 'se')

#remove unnecessary columns
data <- data[, c('age_start', 'age_end', 'age_midpt', 'ihme_loc_id', 'nid', 'year_id', 'survey_name', 'var', 'mean', 'se')]
data_orig <- copy(data) #save an unmatched version of the data to apply adjustments to 

# Drop subnat locations
data <- data[!ihme_loc_id %like% '_']

#get subsets of gs and alternate defs
ref_subset <- copy(data) #for network, refvar can be any of the alternative definitions
alts_subset <- data[!var=='anysex_anyphys_ipv_lifetime'] #altvar is any of the alternative defs only

#set names accordingly
setnames(ref_subset, c('mean', 'se', 'var'), c('ref_mean', 'ref_se', 'refvar'))
setnames(alts_subset, c('mean', 'se', 'var'), c('alt_mean', 'alt_se', 'altvar'))

#merge back onto each other
matched <- merge(ref_subset, alts_subset, by=c('age_start', 'age_end', 'ihme_loc_id', 'nid', 'year_id', 'survey_name', 'age_midpt'), allow.cartesian = T)

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

# Change zeros to very small numbers bc 0 will not mathematically logit transform
matched <- matched[ref_mean<=mean_adj_val, ref_mean:=mean_adj_val]
matched <- matched[alt_mean<=mean_adj_val, alt_mean:=mean_adj_val]

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

# Specify the data for model fitting -------------------------------------------------------------------------------------------------------------------------------------------------------

df <- CWData(
  df = matched,             # dataset for metaregression
  obs = "logit_diff",       # column name for the observation mean
  obs_se = "logit_diff_se", # column name for the observation standard error
  alt_dorms = "altvar",     # column name of the variable indicating the alternative method
  ref_dorms = "refvar",     # column name of the variable indicating the reference method
  covs = list('age_midpt'),
  study_id = 'nid',          # name of the column indicating group membership, usually the matching groups
  add_intercept = TRUE
)

# Fit the model ----------------------------------------------------------------------------------------------------------------------------------------------------------------------

fit_ipv_network <- CWModel(
  cwdata = df,            # object returned by `CWData()`
  obs_type = "diff_logit", # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
  cov_models = list(       # specifying predictors in the model; see help(CovModel)
    CovModel("intercept"),
    CovModel(
      cov_name = "age_midpt",
      spline = XSpline(
        knots = c(0, 15, 20, 25, 35, 110), # outer knots must encompass the data 
        degree = 3L, # polynomial order (1=linear, 2=quadratic, 3=cubic)
        l_linear = TRUE, # linearity in the left tail
        r_linear = TRUE ))), # linearity in the right tail
  gold_dorm = "anysex_anyphys_ipv_lifetime",  # the level of `ref_dorms` that indicates it's the gold standard
  order_prior = list(c("any_sexpv_pastyr", "any_sexpv_lifetime"),
                     c('any_physpv_pastyr', 'any_physpv_lifetime'),
                     c('anysex_anyphys_ipv_pastyr', 'anysex_anyphys_ipv_lifetime'), 
                     c('any_physpv_pastyr', 'anysex_anyphys_ipv_pastyr'),
                     c('any_physpv_lifetime', 'anysex_anyphys_ipv_lifetime')),
  max_iter=100L, #default is 100
  inlier_pct=0.9 #set trimming to 10%
)

# Save results ----------------------------------------------------------------------------------------------------------------------------------------------------------------------

version <- '10232020_final'
py_save_object(object = fit_ipv_network, filename = paste0('FILEPATH', version, '.pkl'), pickle = "dill")
