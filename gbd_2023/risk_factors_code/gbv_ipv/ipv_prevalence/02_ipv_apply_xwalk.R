####################################################################################################################################################################
# 
# Purpose: Apply adjustments to alternate definition IPV data
#
####################################################################################################################################################################
rm(list=ls())

#set bundle version
bvid <- 'BV ID'

#crosswalk date
date <- 'DATE'

#root
root <- 'FILEPATH'


##### 0. SET UP ####################################################################################################################################################
source("FILEPATH/utility.r")

#libraries + central functions
pacman::p_load(data.table, dplyr, openxlsx, ggplot2, stringr, reticulate)
invisible(sapply(list.files("FILEPATH", full.names = T), source))

#load crosswalk package + model object
reticulate::use_python("FILEPATH")
cw <- import("crosswalk")
library(crosswalk002)
ipv_network <- py_load_object(filename = paste0(root, 'FILEPATH', date, '.pkl'), pickle = "dill")



##### 1. PULL + FORMAT DATA FOR CROSSWALKING #######################################################################################################################

# read in data and create 'var' for all observations to adjust orig vals -------------------------------------------------------------------------------------------

#get bundle data
bv <- get_bundle_version(bvid)
data <- copy(bv)

#rename + adjust zero to 2.5th percentile
setnames(data, 'val', 'mean')
mean_adj_val <- quantile(data[mean > 0.001]$mean, probs = 0.025)
data[mean <= mean_adj_val, mean := mean_adj_val]


# aggregate detailed vars to create more general bins
# why: when extracting data on gender-based violence, we extract much more information than we can use
#      as a result, we have to remove some of the 'extra' information that we do not yet use (eg, force types)

#copy original var for comparison
data[, og_var := var]

#remove attempt tags
data[var %like% '_any_force_incl_attempts', var := str_replace(var, '_any_force_incl_attempts', '')]
data[var %like% '_physical_force_incl_attempts', var := str_replace(var, '_physical_force_incl_attempts', '')]
data[var %like% '_attempts_only', var := str_replace(var, '_attempts_only', '')]

#remove force tags
data[var %like% '_coerce_force', var := str_replace(var, '_coerce_force', '')]
data[var %like% '_physical_force', var := str_replace(var, '_physical_force', '')]
data[var %like% '_any_force', var := str_replace(var, '_any_force', '')]

#remove perp ids for partner violence
data[var %like% '_anypart', var := str_replace(var, '_anypart', '')]
data[var %like% '_currpart', var := str_replace(var, '_currpart', '')]
data[var %like% '_prevpart', var := str_replace(var, '_prevpart', '')]

#remove psych and econ
data[var %like% '_psych', var := str_replace(var, '_psych', '')]
data[var %like% '_econ', var := str_replace(var, '_econ', '')]

#put recall period at the end
data[var %like% '_1yr', var := str_replace(var, '_1yr', '_pastyr')]

#change specific sv to general sv (only sexual violence - we do not differentiate types of SV at this point in time)
data[var %like% 'lifetime' & !var %like% 'phys' & (var %like% 'int_sexpv_' | var %like% 'acts_sexpv_' | var %like% 'nonintsex_' | var %like% 'noncontactsex_' | var %like% 'intsex_' | var %like% 'anysex_'), var := 'any_sexpv_lifetime']
data[var %like% 'pastyr' & !var %like% 'phys' & (var %like% 'int_sexpv_' | var %like% 'acts_sexpv_' | var %like% 'nonintsex_' | var %like% 'noncontactsex_' | var %like% 'intsex_' | var %like% 'anysex_'), var := 'any_sexpv_pastyr']
data[var %like% 'intsex', var := str_replace(var, 'intsex', 'anysex')]
data[var %like% 'actsex', var := str_replace(var, 'actsex', 'anysex')]

#change specific pv to general phys (only physical violence - we do not differentiate types of PV at this point in time)
data[var %like% 'lifetime' & !var %like% 'sex' & (var %like% 'nonseverephys_' | var %like% 'anyphys_' | var %like% 'nonsevere_physpv' | var %like% 'all_pv_'), var := 'any_physpv_lifetime']
data[var %like% 'pastyr' & !var %like% 'sex' & (var %like% 'nonseverephys_' | var %like% 'anyphys_' | var %like% 'nonsevere_physpv' | var %like% 'all_pv_'), var := 'any_physpv_pastyr']
data[var == "anysex_nonseverephys_ipv_lifetime", var := "anysex_anyphys_ipv_lifetime"]
data[var == "severephys_lifetime", var := "any_physpv_lifetime"]
data[var == "anysex_severephys_ipv_pastyr", var := "anysex_anyphys_ipv_pastyr"]
                
#final fixes
data[og_var == 'any_phys_anypart_lifetime', var := 'any_physpv_lifetime']
data[og_var == 'any_phys_anypart_pastyr', var := 'any_physpv_pastyr']
data[og_var == 'any_sex_anypart_lifetime', var := 'any_sexpv_lifetime']
data[og_var == 'any_sex_anypart_pastyr', var := 'any_sexpv_pastyr']
data[var == "anyphys_anysex_pastyr" | var == "anyphys_anysex_pastyr", var := "anysex_anyphys_ipv_pastyr"]
data[var == "anyphys_anysex_lifetime" | var == "anyphys_anysex_lifetime", var := "anysex_anyphys_ipv_lifetime"]
data[var == "ipv_phys", var := "any_physpv_lifetime"]

#subset to data that is used for modeling (best available definitions)
data_to_adjust <- data[is.na(group_review) | group_review == 1]
data_to_adjust[, age_midpt := ifelse(age_start != 999, ((age_start + age_end) / 2), ((age_start_orig + age_end_orig) / 2))]

#split into data that needs adjusting vs. not
data_unadj <- data_to_adjust[var %in% c('anysex_anyphys_ipv_lifetime', 'sex_phys_psych_pv_lifetime')]
data_to_adjust <- data_to_adjust[!var %in% c('anysex_anyphys_ipv_lifetime', 'sex_phys_psych_pv_lifetime')]
data_to_adjust[var == "contanysex_lifetime", var := "any_sexpv_lifetime"]

#create data_id
data_to_adjust[, data_id:=1:.N]



##### 2. CREATE PREDICTIONS FROM CROSSWALK #########################################################################################################################

#predict out
preds <- adjust_orig_vals(
  fit_object = ipv_network, # object returned by `CWModel()`
  df = data_to_adjust,
  orig_dorms = "var",
  orig_vals_mean = "mean",
  orig_vals_se = "standard_error"
)

# add the adjusted values to the original dataset
data_to_adjust[, c("meanvar_adjusted", "sdvar_adjusted", "adjustment_logit", "adjustment_se_logit", "data_id")] <- preds

#add note to adjusted points
data_to_adjust[!is.na(note_modeler), note_modeler := paste0(note_modeler, ' | applied xwalk adj from ', date)]
data_to_adjust[is.na(note_modeler), note_modeler := paste0('applied xwalk adj from ', date)]



##### 3. FORMAT BEFORE SAVING ######################################################################################################################################

# Set crosswalk parent seq and seq: 
data_to_adjust[, crosswalk_parent_seq:=seq]
data_to_adjust[, seq:='']

#set adjusted means to mean, etc.
setnames(data_to_adjust, c('mean', 'standard_error', 'meanvar_adjusted', 'sdvar_adjusted'), c('mean_unadj', 'standard_error_unadj', 'mean', 'standard_error'))

#clear out unadjusted se information
data_to_adjust[, c('upper', 'lower', 'variance') := '']

# bind back to data with vars not included in the network
data_to_save <- rbind(data_to_adjust, data_unadj, fill=T)

#remove extra tags we don't need
drop <- c('sex_violence_type', 'perpetrator_identity', 'data_id', 'phys_violence_type', 'og_var')
data_to_save[, (drop) := NULL]

#save file
output_dir <- paste0(root, 'FILEPATH', date,'/')
dir.create(output_dir, recursive=T)
write.xlsx(data_to_save, paste0(output_dir, 'ipv_bv', bvid, '_2023xwalkadj_update.xlsx'), sheetName='extraction') 





