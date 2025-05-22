####################################################################################################################################################################
# 
# Author: USERNAME
# Purpose: Clean & prepare IPV - HIV/AIDS RR data for modeling
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

ro_pair <- 'ipv_hiv'

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

#hiv disorder data
hiv <- data[cause %in% c('hiv/aids') | cause %like% 'hiv/aids']

hiv[!is.na(most_adjusted_mean), `:=` (mean=most_adjusted_mean, lower=most_adjusted_lower, upper=most_adjusted_upper)]
hiv[is.na(most_adjusted_mean), `:=` (mean=raw_mean, lower=raw_lower, upper=raw_upper)]

#start to create subsets by exposure type
gbv_hiv <- hiv[exposure_timing=='Adulthood violence exposure']
length(unique(gbv_hiv$Study_ID))
ipv_hiv <- gbv_hiv[perpetrator_type == 'partner' | perpetrator_type=='partner; former partner']
length(unique(ipv_hiv$Study_ID))

ipv_hiv <- ipv_hiv[!Study_ID=='thirumalai 2018'] #drop this study because XC dissertation

# 1. Prepare IPV input data set ------------------------------------------------------------------------------------------------------------------------------------------

study_info <- fread(paste0(study_info_dir, ro_pair, '_annotated.csv'))
ipv_hiv <- merge(ipv_hiv, study_info[, .(`Covidence_#`, subpopulation, cv_subpop)], all.x=T)

model_input <- ipv_hiv
model_input <- model_input[!other_information %like% 'sensitivity' & exposure_recall_type=='lifetime' & exposed_level2=='any exposure']
model_input <- model_input[(Study_ID %in% c('delong 2019', 'jewkes 2010') & violence_type=='physical; sexual') | 
                             (Study_ID %in% c('deyessa 2018', 'kouyoumdjian 2013') & violence_type %in% c('physical', 'sexual')) | 
                             (Study_ID %in% c('fonck 2005', 'maman 2002'))]
model_input[, bc_non_lifetime_recall:=ifelse(exposure_recall_type %in% c('lifetime', 'ipv prior to 12-month follow-up', 'lifetime, including past 4 years', 'more than one year ago'), 0, 1)]

unique(model_input[, .(Study_ID, violence_type, violence_type_combination)])

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

#scale SEs for studies with two, non-mutually exclusive defs
scalar <- 2
model_input[Study_ID %in% c('deyessa 2018', 'kouyoumdjian 2013', 'maman 2002'),
            ln_rr_se:=ln_rr_se*sqrt(scalar)]

model_input$seq <- 0:(nrow(model_input) - 1)
model_input[,rei_id:=167]
model_input[,cause_id:=298]
setnames(model_input, 'Covidence_#', 'study_id')
model_input[, bundle_id:=2621]
model_input[, bundle_version_id:=0]
model_input[, risk_type:='dichotomous']
model_input[, risk_unit:='exposed']
model_input[, `:=` (ref_risk_lower=0, ref_risk_upper=0, alt_risk_lower=0, alt_risk_upper=0)]
names(model_input) <- gsub('cv_', 'bc_', names(model_input))

#sensitivity analysis: remove non-cohort studies
model_input <- model_input[!Study_ID %in% c("deyessa 2018", "fonck 2005", "maman 2002")]

#set up cascading dummy for level of control (no age control, controlled for age and 1-2 others, controlled for age+3 or more other vars)
model_input[, bc_L1:=ifelse(bc_age_uncontrolled==0 & num_confounders>3, 0, 1)]

#get list of covs meeting minimum value criteria
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

write.csv(model_input, paste0(mrbrt_out, 'abuse_ipv_exp-hi.csv'), row.names=F)

#save out table summarizing study info
model_input[!is.na(raw_mean), raw_effect:=paste0(raw_mean, ' (', raw_lower, '-', raw_upper, ')')]
model_input[!is.na(most_adjusted_mean), adj_effect:=paste0(most_adjusted_mean, ' (', most_adjusted_lower, '-', most_adjusted_upper, ')')]
model_input[!is.na(age_mean) & !is.na(age_sd), age_summary:=paste0(age_mean, ' (', age_sd, ')')]
model_input[is.na(age_mean) & !is.na(age_lower) & !is.na(age_upper), age_summary:=paste0(age_lower, '-', age_upper)]
model_input[is.na(age_summary) & !is.na(age_mean), age_summary:=age_mean]

table_output <- model_input[, .(study_id, Study_ID, ihme_loc_id, Study_name, Study_design, 
                                ss,
                                fup_years, attrition,
                                sex, age_summary,
                                Study_selection_criteria,
                                violence_type, violence_type_combination, Exposure_assessment_instrument,
                                cause, Outcome_assessment_instrument,
                                confounders,
                                Effect_Size_Measure,
                                raw_effect, adj_effect)]
write.csv(table_output, paste0(table_out, 'ipv_hiv_',gsub('-', '_', Sys.Date()), '.csv'), row.names=F)

bc_output <- unique(model_input[, .SD, .SDcols=c('Study_ID', setdiff(candidate_covs_one, dup_covs))])
bc_output[, `Author and year`:=str_to_title(Study_ID)]
bc_output[, Study_ID:=NULL]
setcolorder(bc_output, neworder=c('Author and year'))
write.csv(bc_output, paste0(table_out, 'ipv_hiv_biascovs_',gsub('-', '_', Sys.Date()), '.csv'), row.names=F)

