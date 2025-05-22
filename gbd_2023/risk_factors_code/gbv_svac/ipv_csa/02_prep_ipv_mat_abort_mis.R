####################################################################################################################################################################
# 
# Author: USERNAME
# Purpose: Clean & prepare IPV-Maternal Abortion & Miscarriage RR data for modeling
#
####################################################################################################################################################################

rm(list=ls())

# set-up environment -----------------------------------------------------------------------------------------------------------------------------------------------

library(data.table)
library(dplyr)
library(openxlsx)
library(ggplot2)
library(stringr)
library(ggsci)
library(forestplot)
library(tidyr)
library(crosswalk002, lib.loc = "FILEPATH")

invisible(sapply(list.files("FILEPATH", full.names = T), source))

aesth <- theme_bw() + theme(axis.text.x = element_text(size=14,face="plain",angle=0,hjust=1),
                            axis.text.y = element_text(size=14,face="plain"),
                            strip.text = element_text(size=14,face="bold"),
                            plot.background = element_blank(),
                            plot.title =element_text(size=14,face='bold'),
                            legend.text=element_text(size=14),
                            legend.title=element_text(size=14, face='bold'))

ro_pair <- 'ipv_abortion'

main_dir <- 'FILEPATH'
plot_dir <- 'FILEPATH'
study_info_dir <- 'FILEPATH'
mrbrt_out <- 'FILEPATH'
table_out <- 'FILEPATH'
cov_dir <- 'FILEPATH'

plot_dir <- paste0(plot_dir, ro_pair, '/')
dir.create(plot_dir, showWarnings = F)
study_info_dir <- paste0(study_info_dir, ro_pair, '/')
dir.create(study_info_dir, showWarnings = F)
table_out <- paste0(table_out, ro_pair, '/')
dir.create(table_out, showWarnings = F)
cov_dir <- paste0(cov_dir, ro_pair, '/')
dir.create(cov_dir, showWarnings = F)

# load in data -------------------------------------------------------------------------------------------------------------------------------------------------------
data <- fread(paste0(main_dir, 'data_cleaned_2023_04_21.csv'))

#Abortion/miscarriage data
mat <- data[(cause %in% c('maternal abortion and miscarriage', 'abortion', 'miscarriage'))|
              cause %like% 'abortion' | cause %like% 'miscarriage']

mat[, .(length(unique(Study_ID))), by=.(cause)]

mat[!is.na(most_adjusted_mean), `:=` (mean=most_adjusted_mean, lower=most_adjusted_lower, upper=most_adjusted_upper)]
mat[is.na(most_adjusted_mean), `:=` (mean=raw_mean, lower=raw_lower, upper=raw_upper)]

#create ipv subset
ipv_mat <- mat[(perpetrator_type == 'partner' | perpetrator_type=='partner; former partner') | Study_ID=='leung 2002']
length(unique(ipv_mat$Study_ID))

# 1. Prepare IPV input data set ------------------------------------------------------------------------------------------------------------------------------------------
ipv_mat[Study_ID=='abdollahi 2015', cause:='Miscarriage']

model_input <- ipv_mat[sex=='Female']
model_input <- model_input[!(Study_ID %in% unique(ipv_mat[violence_type=='physical; sexual' & violence_type_combination=='and/or']$Study_ID) & violence_type!='physical; sexual')]
model_input <- model_input[!(Study_ID %in% intersect(unique(model_input[violence_type == 'physical']$Study_ID), 
                                                     unique(model_input[violence_type == 'sexual']$Study_ID)) 
                             & !violence_type %in% c('physical', 'sexual'))]
model_input <- model_input[violence_type!='psychological']
model_input <- model_input[!(Study_ID=='ibrahim 2015' & violence_type=='physical; sexual; psychological')]

unique(model_input[, .(Study_ID, violence_type, cause)])

study_info <- fread(paste0(study_info_dir, ro_pair, '_annotated.csv'))
model_input <- merge(model_input, study_info[, .(`Covidence_#`, subpopulation, cv_subpop, cv_abortion)], all.y=T)

#final covariates 
model_input[, cv_aggregate_expdef:=0]
model_input[violence_type %in% c('violent relationship', 'physical; sexual; psychological'), cv_aggregate_expdef:=1]
model_input[, cv_component_expdef:=0]
model_input[Study_ID %in% c('cokkinides 1999', 'catak 2016', 'nelson 2003', 'ibrahim 2015'), cv_component_expdef:=1]

#pick best points for taft pubs
model_input <- model_input[!(Study_ID=='taft 2019' & subgroup_analysis=='yes')] #drop age-stratified analyses
model_input <- model_input[Study_ID!='taft 2007'] #drop duplicative underlying cohort

#exclude cokkinides for unclear design/measurement
model_input <- model_input[Study_ID!='cokkinides 1999']

#pick best points for nelson 2003
model_input <- model_input[!(Study_ID=='nelson 2003' & subgroup_analysis=='yes')]
model_input <- model_input[!(Study_ID=='nelson 2003' & exposure_recall_type=='during pregnancy')]

model_input[, bc_non_lifetime_recall:=ifelse(exposure_recall_type %in% c('lifetime', 'ipv prior to 12-month follow-up', 'lifetime, including past 4 years', 'more than 1 year ago', 'undefined'), 0, 1)]
model_input[Study_ID=='taft 2019', bc_non_lifetime_recall:=0]

#mr-brt required inputs
model_input[!is.na(most_adjusted_mean), `:=` (effect_size=most_adjusted_mean, effect_size_type='adjusted', ln_rr=log(most_adjusted_mean))]
model_input[is.na(most_adjusted_mean), `:=` (effect_size=raw_mean, effect_size_type='raw', ln_rr=log(raw_mean))]

model_input[effect_size_type=='adjusted' & Uncertainty_type=="confidence interval" & Confidence_interval_level == "95", 
            ln_rr_se:=(log(most_adjusted_upper)-log(most_adjusted_lower))/3.92]
model_input[effect_size_type=='adjusted' & Uncertainty_type=="confidence interval" & Confidence_interval_level == "90", 
            ln_rr_se:=(log(most_adjusted_upper)-log(most_adjusted_lower))/3.29]
model_input[effect_size_type=='adjusted' & Uncertainty_type=="confidence interval" & Confidence_interval_level == "99", 
            ln_rr_se:=(log(most_adjusted_upper)-log(most_adjusted_lower))/5.15]

model_input[effect_size_type=='raw' & Uncertainty_type=="confidence interval" & Confidence_interval_level == "95", 
            ln_rr_se:=(log(raw_upper)-log(raw_lower))/3.92]
model_input[effect_size_type=='raw' & Uncertainty_type=="confidence interval" & Confidence_interval_level == "90", 
            ln_rr_se:=(log(raw_upper)-log(raw_lower))/3.29]
model_input[effect_size_type=='raw' & Uncertainty_type=="confidence interval" & Confidence_interval_level == "99", 
            ln_rr_se:=(log(raw_upper)-log(raw_lower))/5.15]

model_input$seq <- 0:(nrow(model_input) - 1)
model_input[,rei_id:=167]
model_input[,cause_id:=995]
setnames(model_input, 'Covidence_#', 'study_id')
model_input[, bundle_id:=2621]
model_input[, bundle_version_id:=0]
model_input[, risk_type:='dichotomous']
model_input[, risk_unit:='exposed']
model_input[, `:=` (ref_risk_lower=0, ref_risk_upper=0, alt_risk_lower=0, alt_risk_upper=0)]
names(model_input) <- gsub('cv_', 'bc_', names(model_input))

#scale SEs for studies with two, non-mutually exclusive defs
scalar <- 2
model_input[Study_ID %in% c('taft 2019', 'romito 2009'),
            ln_rr_se:=ln_rr_se*sqrt(scalar)]

#sensitivity 1: exclude abdollahi (which has cell count with fewer than 10 obs)
model_input <- model_input[Study_ID!='abdollahi 2015']

#set up cascading dummy for level of control (no age control, controlled for age and 1-2 others, controlled for age+3 or more other vars)
model_input[, bc_L1:=ifelse(bc_age_uncontrolled==0 & num_confounders>3, 0, 1)]

#get list of covs meeting minimum value criteria
model_input[, bc_abortion:=NULL]
candidate_covs_pot <- names(model_input)[names(model_input) %like% 'bc_']

candidate_covs_one <- c()
candidate_covs <- c()
for (c in candidate_covs_pot) {
  if (length(unique(model_input[get(c)==1]$Study_ID))>1 & length(unique(model_input[get(c)==0]$Study_ID))>1) {
    candidate_covs_one <- c(candidate_covs_one, c)
  }
}

covs_to_remove <- setdiff(candidate_covs_pot, candidate_covs_one)
model_input[, c(covs_to_remove):=NULL]

#check for identical covariates
sub2 <- copy(as.data.frame(model_input[, .SD, .SDcols=candidate_covs_one]))
duplicated_columns <- duplicated(t(sub2))

# Show the Names of the Duplicated Columns
dup_covs <- colnames(sub2[duplicated_columns])

# Remove the Duplicated Columns
if(length(dup_covs)>0){model_input[, c(dup_covs):=NULL]}


write.csv(model_input, paste0(mrbrt_out, 'abuse_ipv_exp-maternal_abort_mi.csv'), row.names=F)

#save out table summarizing study info
model_input[!is.na(raw_mean), raw_effect:=paste0(raw_mean, ' (', raw_lower, '-', raw_upper, ')')]
model_input[!is.na(most_adjusted_mean), adj_effect:=paste0(most_adjusted_mean, ' (', most_adjusted_lower, '-', most_adjusted_upper, ')')]
model_input[!is.na(age_mean) & !is.na(age_sd), age_summary:=paste0(age_mean, ' (', age_sd, ')')]
model_input[is.na(age_mean) & !is.na(age_lower) & !is.na(age_upper), age_summary:=paste0(age_lower, '-', age_upper)]
model_input[is.na(age_summary) & !is.na(age_mean), age_summary:=age_mean]

table_output <- model_input[, .(study_id, Study_ID, Study_name, Study_selection_criteria, ihme_loc_id, 
                                Study_design, sex, age_summary, 
                                violence_type, violence_type_combination, Exposure_assessment_instrument,
                                cause, Outcome_assessment_instrument,
                                ss,
                                fup_years, attrition, confounders,
                                raw_effect, adj_effect)]
write.csv(table_output, paste0(table_out, ro_pair, '_',gsub('-', '_', Sys.Date()), '.csv'), row.names=F)

bc_output <- unique(model_input[, .SD, .SDcols=c('Study_ID', setdiff(candidate_covs_one, dup_covs))])
bc_output[, `Author and year`:=str_to_title(Study_ID)]
bc_output[, Study_ID:=NULL]
setcolorder(bc_output, neworder=c('Author and year'))
write.csv(bc_output, paste0(table_out, ro_pair, '_biascovs_',gsub('-', '_', Sys.Date()), '.csv'), row.names=F)
