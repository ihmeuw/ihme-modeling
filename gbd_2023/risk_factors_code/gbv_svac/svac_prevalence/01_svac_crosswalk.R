####################################################################################################################################################################
# 
# Purpose: Create Crosswalk Network Meta-analysis for SVAC alternate definitions
#
####################################################################################################################################################################

rm(list=ls())

#date of data extraction 
date <- "DATE"


# set-up environment -----------------------------------------------------------------------------------------------------------------------------------------------

#libraries + central functions
pacman::p_load(data.table, openxlsx, dplyr, ggplot2, stringr, locfit)
invisible(sapply(list.files("FILEPATH", full.names = T), source))
reticulate::use_python("FILEPATH")
library(crosswalk002)
cw <- import("crosswalk")
library(logitnorm, lib.loc='FILEPATH')

#directories
xwalk_output_fp <- paste0('FILEPATH/', date, '/')
dir.create(xwalk_output_fp, recursive = T)

#location metadata 
locs <- get_location_metadata(22, release_id = 16)


# read in data and get into correct matched format for within-study pairs ------------------------------------------------------------------------------------------

#list files
files <- list.files('FILEPATH', recursive = T, full.names = T)

#extract data
og <- c()
for (f in files) {
  temp <- fread(f)
  og <- rbind(og, temp, fill = T)
}

#copy to edit
data <- copy(og)


# set-up environment -----------------------------------------------------------------------------------------------------------------------------------------------

#format
data[sex_id == 2, sex:='Female']
data[sex_id==1, sex:='Male']

#subset to vacs only
vacs_nids <- fread('FILEPATH') 
data <- data[nid %in% vacs_nids$x | survey_name=='CDC_VACS']

#drop before 18 (<= 18), rename 17 (<=17) to 18
data <- data[!grepl('18', var)]
data <- data[, var := str_replace_all(var, '17', '18')]

#remove under 15
data <- data[, var := str_replace_all(var, '_under15', '')]

#fix under
data <- data[, var := str_replace_all(var, '_under', '.under')]

#copy var
data[, vargroup := var]

#fill in vargroup for tabulated data
data[, `:=` (og_var = var, og_vargroup = vargroup)]

#keep all data; save a copy to edit
data_var <- copy(data)

#rename: any csa
data_var[grepl("any_csa_age1stexp", vargroup), vargroup := str_replace_all(vargroup, "any_csa_age1stexp", "any_csa")]

#rename: non contact only
data_var[grepl("noncontact_only_age1stexp", vargroup), vargroup := str_replace_all(vargroup, "noncontact_csa_age1stexp", "noncontact_only")]
data_var[grepl("non_contact_only_age1stexp", vargroup), vargroup := str_replace_all(vargroup, "non_contact_csa_age1stexp", "noncontact_only")]
data_var[grepl("noncontact_csa_age1stexp", vargroup), vargroup := str_replace_all(vargroup, "noncontact_csa_age1stexp", "noncontact_only")]
data_var[grepl("non_contact_csa_age1stexp", vargroup), vargroup := str_replace_all(vargroup, "non_contact_csa_age1stexp", "noncontact_only")]
data_var[grepl("noncontact_age1stexp", vargroup), vargroup := str_replace_all(vargroup, "noncontact_age1stexp", "noncontact_only")]
data_var[grepl("non_contact_age1stexp", vargroup), vargroup := str_replace_all(vargroup, "non_contact_age1stexp", "noncontact_only")]
data_var[grepl("noncontact_only", vargroup), vargroup := str_replace_all(vargroup, "noncontact_csa", "noncontact_only")]
data_var[grepl("non_contact_only", vargroup), vargroup := str_replace_all(vargroup, "non_contact_csa", "noncontact_only")]
data_var[grepl("noncontact_csa", vargroup), vargroup := str_replace_all(vargroup, "noncontact_csa", "noncontact_only")]
data_var[grepl("non_contact_csa", vargroup), vargroup := str_replace_all(vargroup, "non_contact_csa", "noncontact_only")]

#rename: intercourse only group
data_var[grepl("intercourse_csa_age1stexp", vargroup), vargroup := str_replace_all(vargroup, "intercourse_csa_age1stexp", "intercourse_only")]
data_var[grepl("intercourse_only_age1stexp", vargroup), vargroup := str_replace_all(vargroup, "intercourse_only_age1stexp", "intercourse_only")]

#rename: contact only group
data_var[grepl("contact_only_age1stexp", vargroup), vargroup := str_replace_all(vargroup, "contact_only_age1stexp", "contact_only")]
data_var[grepl("sex_acts_only_age1stexp", vargroup), vargroup := str_replace_all(vargroup, "sex_acts_only_age1stexp", "contact_only")]
data_var[grepl("contact_csa_age1stexp", vargroup), vargroup := str_replace_all(vargroup, "contact_csa_age1stexp", "contact_only")]

#rename: first sex csa group
data_var[grepl("sexual_debut", vargroup), vargroup := str_replace_all(vargroup, "sexual_debut", "sexual_debut")]
data_var[grepl("first_sex_csa", vargroup), vargroup := str_replace_all(vargroup, "first_sex_csa", "sexual_debut")]

#rename: fix family perp
data_var[vargroup %like% '_familyperp', vargroup := str_replace_all(vargroup, '_familyperp', '.rperp')]
data_var[vargroup %like% '_partnerperp', vargroup := str_replace_all(vargroup, '_partnerperp', '.rperp')]

#create backup just in case
backup <- copy(data_var)

#rename and format year
setnames(data, 'standard_error', 'se')
data[, year_id := ceiling((year_start + year_end)/2)]

#remove extra columns
data <- data[, c('sex_id', 'age_start', 'age_end', 'ihme_loc_id', 'nid', 'year_id', 'survey_name', 'vargroup', 'mean', 'se')]

#drop subnats
data <- data[!ihme_loc_id %like% '_']

#change zeros to a really small number, because otherwise, they will not logit transform
mean_adj_val <- quantile(data[mean!=0]$mean, probs=0.025)
data[mean==0, mean:=mean_adj_val]

#add under15 to the definitions that do not have that tag (these are the former gbd case definitions)
data[!grepl('under', vargroup) & !grepl('perp', vargroup), vargroup := paste0(vargroup, '.under15')]
data[!grepl('under', vargroup) & grepl('perp', vargroup), vargroup := str_replace_all(vargroup, 'rperp', 'under15.rperp')]

#remove the under18 tag from definitions (these are the new gbd case definition)
data[grepl('under18', vargroup), vargroup := str_replace_all(vargroup, '.under18', '')]

#get subsets of gs and alternate defs
ref_subset <- copy(data)
alts_subset <- data[!vargroup=='contact_only']

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

#create a matched copy
matched <- copy(data)

#get logit calcs using the delta transform package
logit_alt_means <- as.data.table(delta_transform(mean=matched$alt_mean, sd=matched$alt_se, transformation='linear_to_logit'))
setnames(logit_alt_means, c('mean_logit', 'sd_logit'), c('logit_alt_mean', 'logit_alt_se'))
logit_ref_means <- as.data.table(delta_transform(mean=matched$ref_mean, sd=matched$ref_se, transformation='linear_to_logit'))
setnames(logit_ref_means, c('mean_logit', 'sd_logit'), c('logit_ref_mean', 'logit_ref_se'))

#bind back onto main data table
matched <- cbind(matched, logit_alt_means)
matched <- cbind(matched, logit_ref_means)

#logit diff
matched[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
  df = matched, 
  alt_mean = "logit_alt_mean", alt_sd = "logit_alt_se",
  ref_mean = "logit_ref_mean", ref_sd = "logit_alt_se" )

#sex covariate
matched[, sex_cov:=ifelse(sex_id==1, 1, 0)]

#drop female only module
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
  study_id = 'nid',         # name of the column indicating group membership, usually the matching groups
  add_intercept = TRUE
)


# Fit the model ----------------------------------------------------------------------------------------------------------------------------------------------------------------------

fit_csa_network <- CWModel(
  cwdata = df,             # object returned by `CWData()`
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

#name 'version'
version <- 'updated_vacs_data_sex_agnostic_all_defs'

#save model object
py_save_object(object = fit_csa_network, filename = paste0(xwalk_output_fp, version, '_modelobj', '.pkl'), pickle = "dill")




