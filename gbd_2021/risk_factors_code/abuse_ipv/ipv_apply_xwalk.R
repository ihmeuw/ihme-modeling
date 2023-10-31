####################################################################################################################################################################
# 
# Author: AUTHOR
# Purpose: Apply adjustments to alternate definition IPV data
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

#set values
bv <- 34838
model_folder <- 'stgpr'

  
# Read in data and create 'var' for all observations for use with adjust orig vals ------------------------------------------------------------------------------------------
bv <- get_bundle_version(bv, fetch='all')
data <- copy(bv)
setnames(data, 'val', 'mean')
mean_adj_val <- quantile(data[mean>0.001]$mean, probs=0.025)
data[mean<=mean_adj_val, mean:=mean_adj_val] #adjust zeros

#only adjusting data that is used for modeling (best available definitions)
data_to_adjust <- data[is.na(group_review) | group_review==1]
data_to_adjust[, age_midpt:=((age_start_orig+age_end_orig)/2)]

# #split into data that needs adjusting vs. not
data_unadj <- data_to_adjust[var %in% c('anysex_anyphys_ipv_lifetime', 'sex_phys_psych_pv_lifetime')]
data_to_adjust <- data_to_adjust[!var %in% c('anysex_anyphys_ipv_lifetime', 'sex_phys_psych_pv_lifetime')]

#create data_id
data_to_adjust[, data_id:=1:.N]

#load ipv network object -------------------------------------------------------------------------------------------------------------------------------------------
ipv_network <- py_load_object(filename = "FILEPATH", pickle = "dill")

npreds <- adjust_orig_vals(
  fit_object = ipv_network, # object returned by `CWModel()`
  df = data_to_adjust,
  orig_dorms = "var",
  orig_vals_mean = "mean",
  orig_vals_se = "standard_error"
)

# now we add the adjusted values back to the original dataset
data_to_adjust[, c("meanvar_adjusted", "sdvar_adjusted", "adjustment_logit", "adjustment_se_logit", "data_id")] <- preds

# add note to adjusted points
data_to_adjust[, note_modeler:=paste0(note_modeler, ' | applied network xwalk adjustment ', Sys.Date())]

# Format and save data ------------------------------------------------------------------------------------------------------------------------------------------------------------

# Set crosswalk parent seq and seq: 
data_to_adjust[, crosswalk_parent_seq:=seq]
data_to_adjust[, seq:='']

#set adjusted means to mean, etc.
setnames(data_to_adjust, c('mean', 'standard_error', 'meanvar_adjusted', 'sdvar_adjusted'), c('mean_unadj', 'standard_error_unadj', 'mean', 'standard_error'))

#clear out unadjusted se information
data_to_adjust[, c('upper', 'lower', 'variance', 'cases', 'sample_size') := '']

# bind back to data with vars not included in the network
data_to_save <- rbind(data_to_adjust, data_unadj, fill=T)

#save file
write.xlsx(data_to_save, paste0('FILEPATH', model_folder, '/', 'ipv_bv', stgpr_bv, '_2020xwalkadj.xlsx'), sheetName='extraction')








