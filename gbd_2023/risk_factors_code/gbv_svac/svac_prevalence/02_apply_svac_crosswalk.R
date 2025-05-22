####################################################################################################################################################################
# 
# Purpose: Apply Crosswalk Network Meta-analysis for SVAC alternate definitions
#
####################################################################################################################################################################

rm(list=ls())

#xwalk date and version
xw_date <- "DATE"
xw_version <- "updated_vacs_data_sex_agnostic_all_defs"

#central parameterss
root <- 'FILEPATH'
release_id_num <- 16


# Set up environment -----------------------------------------------------------------------------------------------------------------------------------------------

pacman::p_load(data.table, dplyr, openxlsx, ggplot2, stringr)
library(reticulate)
reticulate::use_python("FILEPATH")
cw <- import("crosswalk")
library(crosswalk002)
library(logitnorm, lib.loc = paste0('FILEPATH', Sys.getenv(x='USER')))

model_sex <- 'both_sex_csa'
bv <- 'BV ID'


# Functions and metadata -------------------------------------------------------------------------------------------------------------------------------------------

#central functions
invisible(sapply(list.files("FILEPATH", full.names = T), source))

#check missing nids custom function
check <- function(df) {
  #check to make sure nothing is missing
  og[!nid %in% df$nid]$nid %>% unique()
}

#location and age metadata
loc_dt <- get_location_metadata(location_set_id = 22, release_id = release_id_num)
age_dt <- get_age_metadata(24, release_id = release_id_num)
age_dt[, age_group_years_end := age_group_years_end - 1]
age_dt[age_group_id == 235, age_group_years_end := 99]



# Load and format data to apply xwalk adjustments  -----------------------------------------------------------------------------------------------------------------

#load data
bv_data <- get_bundle_version(bv, fetch='all')

#rename and adjust zero vals
setnames(bv_data, 'val', 'mean')
mean_adj_val_fem <- quantile(bv_data[mean>0.001 & sex == 'Female']$mean, probs=0.025)
mean_adj_val_male <- quantile(bv_data[mean>0.001 & sex == 'Male']$mean, probs=0.025)
bv_data[mean<=mean_adj_val_fem & sex == 'Female', mean:=mean_adj_val_fem]
bv_data[mean<=mean_adj_val_male & sex == 'Male', mean:=mean_adj_val_male]


# Rename vars that don't match -------------------------------------------------------------------------------
# when extracting new data for vac, we extract more information than we can actually use
# the new cleaning script also makes some vars that are not compatible with historical data
# we therefore need to clean things up so everything matchs

#preserve original var name
bv_data[, og_var := var]

#change age threshold tag
bv_data[grepl('_under', var), var := str_replace(var, '_under', '.under')]
bv_data[grepl('_before', var), var := str_replace(var, '_before', '.under')]

#drop age threshold tag if age not given
bv_data[grepl('.under$', var), var := str_replace(var, '.under', '')]

#remove below 15 because that is GS definition
bv_data[grepl('.under15', var), var := str_replace(var, '.under15', '')]

#remove force types + recall period
bv_data[grepl('_any_force', var), var := str_replace(var, '_any_force', '')]
bv_data[grepl('_incl_attempts', var), var := str_replace(var, '_incl_attempts', '')]
bv_data[grepl('_attempts_only', var), var := str_replace(var, '_attempts_only', '')]
bv_data[grepl('_coerce_force', var), var := str_replace(var, '_coerce_force', '')]
bv_data[grepl('_lifetime', var), var := str_replace(var, '_lifetime', '')]

#reset any adult or any perp to non-restricted csa
bv_data[grepl('_anyperp', var), var := str_replace(var, '_anyperp', '')]
bv_data[grepl('_anyadult', var), var := str_replace(var, '_anyadult', '')]

#set rperp
bv_data[grepl('_nonpart', var), var := str_replace(var, '_nonpart', '.rperp')]
bv_data[grepl('_relative', var), var := str_replace(var, '_relative', '.rperp')]
bv_data[grepl('_anypart', var), var := str_replace(var, '_anypart', '.rperp')]
bv_data[grepl('_stranger', var), var := str_replace(var, '_stranger', '.rperp')]
bv_data[grepl('_knownperp', var), var := str_replace(var, '_knownperp', '.rperp')]
bv_data[grepl('_caregiver', var), var := str_replace(var, '_caregiver', '.rperp')]
bv_data[grepl('_familyperp', var), var := str_replace(var, '_familyperp', '.rperp')]
bv_data[grepl('_partnerperp', var), var := str_replace(var, '_partnerperp', '.rperp')]
bv_data[grepl('_rperp', var), var := str_replace(var, '_rperp', '.rperp')]

#rename abuse types
bv_data[grepl('intsex', var), var := str_replace(var, 'intsex', 'intercourse_only')]
bv_data[grepl('contactsex', var), var := str_replace(var, 'contactsex', 'contact_only')]
bv_data[grepl('anysex', var), var := str_replace(var, 'anysex', 'any_csa')]
bv_data[grepl('any_sex_viol', var), var := str_replace(var, 'any_sex_viol', 'any_csa')]
bv_data[grepl('any_sex', var), var := str_replace(var, 'any_sex', 'any_csa')]
bv_data[grepl('any_SV', var), var := str_replace(var, 'any_SV', 'any_csa')]
bv_data[grepl('SVage1stexp_firstsexCSA', var), var := str_replace(var, 'SVage1stexp_firstsexCSA', 'any_csa')]
bv_data[var == "csa", var := "any_csa"]

#drop random resp
bv_data[grepl("_resp", var), var := str_replace(var, '_resp', '')]

#fix contact but non intercourse
bv_data[grepl('nonintercourse_only', var), var := str_replace(var, 'nonintercourse_only', 'contact_only')] 



# Rename VACS vars that don't match other extractions --------------------------------------------------------

#any csa
bv_data[grepl("any_csa_age1stexp", var), var := str_replace_all(var, "any_csa_age1stexp", "any_csa")]

#non contact
bv_data[grepl("noncontact_only_age1stexp", var), var := str_replace_all(var, "noncontact_csa_age1stexp", "noncontact_only")]
bv_data[grepl("non_contact_only_age1stexp", var), var := str_replace_all(var, "non_contact_csa_age1stexp", "noncontact_only")]
bv_data[grepl("noncontact_csa_age1stexp", var), var := str_replace_all(var, "noncontact_csa_age1stexp", "noncontact_only")]
bv_data[grepl("non_contact_csa_age1stexp", var), var := str_replace_all(var, "non_contact_csa_age1stexp", "noncontact_only")]

#intercourse
bv_data[grepl("intercourse_csa_age1stexp", var), var := str_replace_all(var, "intercourse_csa_age1stexp", "intercourse_only")]
bv_data[grepl("intercourse_only_age1stexp", var), var := str_replace_all(var, "intercourse_only_age1stexp", "intercourse_only")]

#contact or sex acts
bv_data[grepl("sex_acts_only_age1stexp", var), var := str_replace_all(var, "sex_acts_only_age1stexp", "contact_only")]
bv_data[grepl("contact_only_age1stexp", var), var := str_replace_all(var, "contact_only_age1stexp", "contact_only")]
bv_data[grepl("contact_csa_age1stexp", var), var := str_replace_all(var, "contact_csa_age1stexp", "contact_only")]

#first sex csa
bv_data[grepl("sexual_debut", var), var := str_replace_all(var, "sexual_debut", "sexual_debut")]
bv_data[grepl("first_sex_csa", var), var := str_replace_all(var, "first_sex_csa", "sexual_debut")]

#create vargroup (aggregate used to xwalk) and restore var (extraction info)
bv_data[, vargroup := var]
bv_data[, var := og_var]
bv_data$og_var <- NULL

#copy to edit
data <- copy(bv_data)


# create vargroup --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#make sure nothing got through without being assigned a var group
if (length(data[(group_review==1 | is.na(group_review)) & vargroup %in% c('', NA)]$nid)>0) {
  stop('Blank vargroup!')
}

#trim rperp (historically over-adjusted)
data[grepl('rperp', vargroup), vargroup := str_sub(vargroup, 1,-7)]

#make sure everything has a vargroup
if (NA %in% unique(data[group_review==1 | is.na(group_review)]$new_vargroup)) {
  stop('Blank vargroup!')
}

#vargroup refers to the violence type only - it does not include age information
#edit var to (1) add the .under15 tag and (2) remove the .under18 tag
data[!grepl('under', vargroup) & !grepl('rperp', vargroup), vargroup := paste0(vargroup, '.under15')]
data[!grepl('under', vargroup) & grepl('rperp', vargroup), vargroup := str_replace_all(vargroup, 'rperp', 'under15.rperp')]
data[grepl('under18', vargroup), vargroup := str_replace_all(vargroup, '.under18', '')]

#read in xwalk model [it is the same for both sexes]
csa_network <- py_load_object(filename = paste0(root, 'FILEPATH', xw_date, '/', xw_version, '_modelobj', '.pkl'), pickle = "dill")


### ADJUST ALTNERATE DEFINITIONS #############################################################################################################################

#split into data used for modeling that needs alternate definition adjustments (ie, not contact-only)
data_to_adjust <- data[(group_review==1 | is.na(group_review)) & !vargroup=='contact_only']
data_unadj <- data[(group_review==1 | is.na(group_review)) & vargroup=='contact_only']

#create data id for merge:
data_to_adjust[, data_id:=1:.N]

#crosswalk
preds <- adjust_orig_vals(
  fit_object = csa_network,
  df = data_to_adjust,
  orig_dorms = "vargroup",
  orig_vals_mean = "mean",
  orig_vals_se = "standard_error", 
)

#add the adjusted values back to the original dataset
data_to_adjust[, c("meanvar_adjusted", "sdvar_adjusted", "adjustment_logit", "adjustment_se_logit", "data_id")] <- preds

#add note to adjusted points
data_to_adjust[!is.na(note_modeler), note_modeler:=paste0(note_modeler, ' | applied xwalk for alternate definition')]
data_to_adjust[is.na(note_modeler), note_modeler:=paste0('applied xwalk for alternate definition')]

#set crosswalk parent seq and seq
data_to_adjust[!vargroup=='contact_only', crosswalk_parent_seq:=seq]

#set adjusted means to mean
setnames(data_to_adjust, c('mean', 'standard_error', 'meanvar_adjusted', 'sdvar_adjusted'), c('mean_unadj', 'standard_error_unadj', 'mean', 'standard_error'))

#clear out unadjusted se information
data_to_adjust[, c('upper', 'lower', 'variance') := '']

#re-calculate variance
data_to_adjust[, variance:=as.numeric(standard_error)^2]

# bind back to data with unadj data
data_to_save <- rbind(data_to_adjust, data_unadj, fill=T)

# sex info
data_to_save[sex == "Male", sex_id := 1]
data_to_save[sex == "Female", sex_id := 2]


### SAVE #############################################################################################################################3

#create dir + save file
dir.create(paste0(root, 'FILEPATH', xw_date, '/'), recursive = T)
write.xlsx(data_to_save, paste0(root, 'FILEPATH', xw_date, '/', model_sex, '_bv', bv, '_xwalkadj_', xw_version, '.xlsx'), sheetName='extraction')








