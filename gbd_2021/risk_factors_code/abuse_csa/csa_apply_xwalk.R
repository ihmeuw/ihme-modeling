####################################################################################################################################################################
# 
# Author: AUTHOR
# Purpose: Apply CSA adjustment factors
#
####################################################################################################################################################################

rm(list=ls())

# Set up environment -----------------------------------------------------------------------------------------------------------------------------------------------
library(data.table)
library(dplyr)
library(openxlsx)
library(ggplot2)
library(stringr)
library(crosswalk, lib.loc = "FILEPATH")

female <- T #set model option

if (female==T){
  model_sex <- 'fem_csa'
  stgpr_bv <- 35450
} else { 
  model_sex <- 'male_csa'
  stgpr_bv <- 35453
}

date <- '2020-11-18'

bv <- stgpr_bv
model_folder <- 'stgpr'

# Source functions ---------------------------------------------------------------------------------------------------------------------------------------------------

source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/upload_bundle_data.R")
source("FILEPATH/get_location_metadata.R")

# GET METADATA --------------------------------------------------------------------------------------------------------------------------------------------------------

loc_dt <- get_location_metadata(location_set_id = 22)
age_dt <- get_age_metadata(19)
age_dt[, age_group_years_end := age_group_years_end - 1]
age_dt[age_group_id == 235, age_group_years_end := 99]

# Load and format data to apply xwalk adjustments  ---------------------------------------------------------------------------------------------------------------------

# Load data
bv_data <- get_bundle_version(bv, fetch='all')

#fix data point not marked with group review
bv_data[nid==23289, group_review:=1]

#rename estimate column
setnames(bv_data, 'val', 'mean')

# adjust zeros
mean_adj_val <- quantile(bv_data[mean>0.001]$mean, probs=0.025)
bv_data[mean<=mean_adj_val, mean:=mean_adj_val]

data <- copy(bv_data)

#split into data used for modeling that needs adjusting vs. not
data_to_adjust <- data[(group_review==1 | is.na(group_review)) & !vargroup=='contact_only']
data_unadj <- data[(group_review==1 | is.na(group_review)) & vargroup=='contact_only']

# read in xwalk model and apply adjustments -----------------------------------------------------------------------------------------------------------------------------------------------------
csa_network <- py_load_object(filename = paste0('FILEPATH_', date, '.pkl'), pickle = "dill")

#create data id for merge:
data_to_adjust[, data_id:=1:.N]

preds <- adjust_orig_vals(
  fit_object = csa_network, # object returned by `CWModel()`
  df = data_to_adjust,
  orig_dorms = "vargroup",
  orig_vals_mean = "mean",
  orig_vals_se = "standard_error"
)

# now we add the adjusted values back to the original dataset
data_to_adjust[, c("meanvar_adjusted", "sdvar_adjusted", "adjustment_logit", "adjustment_se_logit", "data_id")] <- preds

# add note to adjusted points
data_to_adjust[, note_modeler:=paste0(note_modeler, ' | applied network xwalk adjustment for alternate definition ', Sys.Date())]

# Format and save data ------------------------------------------------------------------------------------------------------------------------------------------------------------

# Set crosswalk parent seq and seq, this is the first step in adjustment, so all data should have a seq and none should have a crosswalk_parent_seq
data_to_adjust[, crosswalk_parent_seq:=NA]
data_to_adjust[!vargroup=='contact_only', crosswalk_parent_seq:=seq]
data_to_adjust[!is.na(crosswalk_parent_seq), seq:='']

#set adjusted means to mean, etc.
setnames(data_to_adjust, c('mean', 'standard_error', 'meanvar_adjusted', 'sdvar_adjusted'), c('mean_unadj', 'standard_error_unadj', 'mean', 'standard_error'))

#clear out unadjusted se information
data_to_adjust[, c('upper', 'lower', 'variance', 'cases', 'sample_size') := '']

#re-calculate variance
data_to_adjust[, variance:=as.numeric(standard_error)^2]

# bind back to data with unadj data
data_to_save <- rbind(data_to_adjust, data_unadj, fill=T)

#save file
write.xlsx(data_to_save, paste0('FILEPATH', model_folder, '/', model_sex, '_bv', bv, '_2020xwalkadj_sexneutraladj_norperp_noagelimit.xlsx'), sheetName='extraction')
