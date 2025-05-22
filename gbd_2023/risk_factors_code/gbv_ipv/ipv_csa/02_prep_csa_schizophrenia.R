####################################################################################################################################################################
# 
# Author: USERNAME
# Purpose: Clean & prepare CSA - Schizophrenia RR data for modeling
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

ro_pair <- 'csa_schizophrenia'

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

#schizophrenia data
schizophrenia <- data[cause %like% 'schizo' | cause %like% 'psychos']

schizophrenia[!is.na(most_adjusted_mean), `:=` (mean=most_adjusted_mean, lower=most_adjusted_lower, upper=most_adjusted_upper)]
schizophrenia[is.na(most_adjusted_mean), `:=` (mean=raw_mean, lower=raw_lower, upper=raw_upper)]

#start to create subsets by exposure type
cm_schizophrenia <- schizophrenia[exposure_timing=='Child maltreatment']
length(unique(cm_schizophrenia$Study_ID))

#get csa
cm_schizophrenia[, .(length(unique(Study_ID))), by=.(violence_type)]
csa_schizophrenia <- cm_schizophrenia[violence_type == 'sexual']
length(unique(csa_schizophrenia$Study_ID))

study_info <- fread(paste0(study_info_dir, ro_pair, '_annotated.csv'))
csa_schizophrenia <- merge(csa_schizophrenia, study_info[, .(`Covidence_#`,
                                           bc_subpop, bc_psychotic_disorder_agg)], all.x=T)
csa_schizophrenia <- csa_schizophrenia[!(Study_ID=='cutajar 2010' & sex!='Combined Male and Female')]
csa_schizophrenia <- csa_schizophrenia[!(Study_ID=='cutajar 2010' & exposure_definition!='experienced childhood sexual abuse (all csa subjects) prior to age 16')]
csa_schizophrenia <- csa_schizophrenia[!(Study_ID=='mall 2020' & exposed_level!='any exposure')]
csa_schizophrenia <- csa_schizophrenia[!(Study_ID=='mansueto 2022' & sex=='Male')] # does not report an ES for males

model_input <- copy(csa_schizophrenia)
model_input[, bc_exp_before_ageabove15:=ifelse(exposure_age_upper > 15, 1, 0)]
model_input[, bc_exp_before_agebelow15:=ifelse(exposure_age_upper < 15, 1, 0)]
model_input[, bc_restricted_perp:=ifelse(!perpetrator_type %in% c('anyone or not-specified', 'anyone or not specified', 'any adult or person older than yourself'), 1, 0)]

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
model_input[,rei_id:=134]
model_input[,cause_id:=559]
setnames(model_input, 'Covidence_#', 'study_id')
model_input[, bundle_id:=2618]
model_input[, bundle_version_id:=0]
model_input[, risk_type:='dichotomous']
model_input[, risk_unit:='exposed']
model_input[, `:=` (ref_risk_lower=0, ref_risk_upper=0, alt_risk_lower=0, alt_risk_upper=0)]
names(model_input) <- gsub('cv_', 'bc_', names(model_input))

#set up cascading dummy for controlling for at least one thing beyond age and/or sex
model_input[, confounders:=gsub('age; ', '', confounders)]
model_input[, confounders:=gsub('sex; ', '', confounders)]
model_input[, confounders:=gsub('sex', '', confounders)]
model_input[, confounders:=gsub('age', '', confounders)]

model_input[, num_confounders:=str_count(confounders, ';')+str_count(confounders,',')+1]
model_input[confounders=='', num_confounders:=0]
model_input[bc_twin_study==1, `:=` (bc_age_uncontrolled=0, bc_L1=0, bc_confounding_uncontrolled=0)]
model_input[, bc_L1:=ifelse(bc_age_uncontrolled==0 & bc_sex_uncontrolled==0 & num_confounders>2, 0, 1)]

#remove irrelevant covariates
model_input[, c('bc_twin_study'):=NULL]
model_input[bc_males_only==1 | bc_females_only==1, bc_subpop:=1]

#find covariates with minimum studies represented
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

write.csv(model_input, paste0(mrbrt_out, 'abuse_csa-mental_schizo.csv'), row.names=F)

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
write.csv(table_output, paste0(table_out, 'csa_schizo_',gsub('-', '_', Sys.Date()), '.csv'), row.names=F)

bc_output <- unique(model_input[, .SD, .SDcols=c('Study_ID', setdiff(candidate_covs_one, dup_covs))])
bc_output[, `Author and year`:=str_to_title(Study_ID)]
bc_output[, Study_ID:=NULL]
setcolorder(bc_output, neworder=c('Author and year'))
write.csv(bc_output, paste0(table_out, 'csa_schizo_biascovs_',gsub('-', '_', Sys.Date()), '.csv'), row.names=F)

