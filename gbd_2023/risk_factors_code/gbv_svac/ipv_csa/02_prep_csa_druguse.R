####################################################################################################################################################################
# 
# Author: USERNAME
# Purpose: Clean & prepare CSA - Drug Use Disorder RR data for modeling
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

ro_pair <- 'csa_druguse'

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

#drug use data
drug_use <- data[cause %like% 'use' & !cause %like% 'alcohol' & cause!='substance use disorders']
drug_use[Study_ID=='sweet 2013', violence_type:='sexual']

drug_use[!is.na(most_adjusted_mean), `:=` (mean=most_adjusted_mean, lower=most_adjusted_lower, upper=most_adjusted_upper)]
drug_use[is.na(most_adjusted_mean), `:=` (mean=raw_mean, lower=raw_lower, upper=raw_upper)]

#start to create subsets by exposure type
cm_drug_use <- drug_use[exposure_timing=='Child maltreatment']
length(unique(cm_drug_use$Study_ID))

#get csa
cm_drug_use[, .(length(unique(Study_ID))), by=.(violence_type)]
csa_drug_use <- cm_drug_use[violence_type == 'sexual']
csa_drug_use <- csa_drug_use[!(Study_ID=='nelson 2006' & outcome_definition %like% 'nicotine')]
length(unique(csa_drug_use$Study_ID))

csa_drug_use[, use_model:=1]

csa_drug_use[, bc_twin_study:=0]
csa_drug_use[Study_ID %in% c('kendler 2000', 'nelson 2006', 'duncan 2008'), bc_twin_study:=1]

csa_drug_use[, bc_subpop:=0]
csa_drug_use[Study_ID %in% c('kendler 2000', 'nelson 2006', 'duncan 2008', 'conroy 2009', 'kalichman 2001'), bc_subpop:=1]

csa_drug_use[, `:=` (bc_use=1, bc_specific_substance=0, bc_cannabis=0)]
csa_drug_use[!outcome_name %like% 'drug', bc_specific_substance:=1]
csa_drug_use[outcome_name %like% 'cannabis' | outcome_name %like% 'marijuana', bc_cannabis:=1]
csa_drug_use[outcome_name %like% 'depend' | outcome_name %like% 'abuse' | outcome_name %like% 'disorder', bc_use:=0]

csa_drug_use <- csa_drug_use[!Study_ID %in% c('mills 2017')] #cannabis disorder from MUSP; already have from MUSP (Hayatbakhsh 2009)
csa_drug_use <- csa_drug_use[`Covidence_#`!=63052] #injecting drug use using MUSP; already having drug use disorders from MUSP (Najman 2022)
csa_drug_use <- csa_drug_use[!Study_ID %in% c('harrington 2011')] #incident illicit substance use from NESARC; already have DUDs from NESARC (Sweet 2013)

#for overall analysis remove studies which have an overall and also different pubs reporting on specific
sp_study_excl <- c('scheidell 2015', 'austin 2020') #and abajobir 2017: 63506
sp_study_excl <- c(sp_study_excl, 'hayatbakhsh 2009')
csa_drug_use <- csa_drug_use[!Study_ID %in% sp_study_excl & `Covidence_#`!=63506]

#fix extractions
csa_drug_use[is.na(sex), sex:='Combined Male and Female']
csa_drug_use[Study_ID=='borges 2021', `:=` (most_adjusted_lower=0.01, lower=0.01)] #correct extraction but transforms into infinite

#filter
csa_drug_use[, use_model:=1]
csa_drug_use[Study_ID=='conroy 2009' & exposed_level!='any exposure', use_model:=0]
csa_drug_use[Study_ID=='kendler 2000' & exposed_level!='any exposure', use_model:=0]
csa_drug_use[Study_ID=='nelson 2006' & cause!='Drug use', use_model:=0]
csa_drug_use[Study_ID=='najman 2022' & cause!='drug use disorders', use_model:=0]
csa_drug_use[Study_ID=='tonmyr 2017' & outcome_name!='illicit drug use', use_model:=0]
csa_drug_use[Study_ID=='huang 2011' & !outcome_definition %like% 'year', use_model:=0]
model_input <- copy(csa_drug_use[use_model==1])

#fix missing values in Kisely 2022
model_input[Study_ID=='kisely 2022', `:=` (Uncertainty_type="confidence interval", Confidence_interval_level = "95")]
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

#scale SEs for studies with non-mutually exclusive drug use outcomes
scalar <- 5
model_input[Study_ID %in% c('nelson 2006'),
            ln_rr_se:=ln_rr_se*sqrt(scalar)]

model_input$seq <- 0:(nrow(model_input) - 1)
model_input[,rei_id:=134]
model_input[,cause_id:=561]
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

# subset to outcome definitions explicitly using disorder criteria
# model_input <- model_input[bc_use==0]

# exclude studies measuring specific disorders only
# model_input <- model_input[!Study_ID %in% c('duncan 2008', 'conroy 2009')]

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
if (length(candidate_covs_one>0)){
  sub2 <- copy(as.data.frame(model_input[, .SD, .SDcols=candidate_covs_one]))
  duplicated_columns <- duplicated(t(sub2))
  
  # Show the Names of the Duplicated Columns
  dup_covs <- colnames(sub2[duplicated_columns])
} else {dup_covs <- c()}


# Remove the Duplicated Columns
if(length(dup_covs)>0){model_input[, c(dup_covs):=NULL]}

write.csv(model_input, paste0(mrbrt_out, 'abuse_csa-mental_drug.csv'), row.names=F)

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
write.csv(table_output, paste0(table_out, 'csa_mental_drug_',gsub('-', '_', Sys.Date()), '.csv'), row.names=F)

bc_output <- unique(model_input[, .SD, .SDcols=c('Study_ID', setdiff(candidate_covs_one, dup_covs))])
bc_output[, `Author and year`:=str_to_title(Study_ID)]
bc_output[, Study_ID:=NULL]
setcolorder(bc_output, neworder=c('Author and year'))
write.csv(bc_output, paste0(table_out, 'csa_mental_drug_',gsub('-', '_', Sys.Date()), '.csv'), row.names=F)
