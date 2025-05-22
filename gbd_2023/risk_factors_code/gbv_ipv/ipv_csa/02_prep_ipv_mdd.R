####################################################################################################################################################################
# 
# Author: USERNAME
# Purpose: Clean & prepare IPV - Major Depressive Disorder RR data for modeling
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

ro_pair <- 'ipv_mdd'

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

#Depressive disorder data
dep <- data[(cause %in% c('depressive disorders', 'dysthymia', 'major depressive disorder') | cause %like% 'depress') & (!cause %like% 'natal' & !cause %like% 'partum')]

dep <- dep[!Study_ID=='ackard 2007' & !Study_ID=='cations 2020'] #exclude based on UQ team advice
dep <- dep[Study_ID!='vaeth 2010'] #exclude cx analysis
dep <- dep[Study_ID!='logie 2022'] #exclude study among refugee youth
dep <- dep[Study_ID!='daundasekara 2022'] #exclude study duplicative with Suglia 2011
dep <- dep[Study_ID!='vos 2006'] #exclude study duplicative with taft 2008 and XC design
dep <- dep[Study_ID!='dichter 2017'] #duplicative with Makaroun 2020
dep <- dep[Study_ID!='chandan 2020'] #duplicative with cprd data analysis

dep[, gbd_2019_ipv:=ifelse(`Covidence_#` %in% c(ipv_dep_ids),1,0)]

#remap chronic dep
dep[cause %in% c('recurrent major depressive disorder', 'chronic depressive disorder', 'chronic major depressive disorder'), cause:='chronic depressive disorder']

#start to create subsets by exposure type
gbv_dep <- dep[exposure_timing=='Adulthood violence exposure']
length(unique(gbv_dep$Study_ID))
ipv_dep <- gbv_dep[perpetrator_type == 'partner' | perpetrator_type=='partner; former partner']
length(unique(ipv_dep$Study_ID))

dep_study_info <- fread(paste0(study_info_dir, 'ipv_dep_info_annotated.csv'))
ipv_dep <- merge(ipv_dep, dep_study_info[, .(Study_ID, `Covidence_#`, exposure_timing, cv_symptom_scale, subpopulation, cv_subpop,
                                     `control for outcome at baseline`, cv_baseline_uncontrolled, cv_clinical_sample)], 
             by=c('Study_ID', 'Covidence_#'),
             all.x=T, all.y=F)

# 1. Prepare IPV input data set ------------------------------------------------------------------------------------------------------------------------------------------

ipv_dep[, use_model:=1]

#make some fixes
ipv_dep[Study_ID=='brown 2020' & exposure_recall_type=='past year', exposure_recall_type:='past year at 10-year follow-up']
ipv_dep[Study_ID=='ouellet-morin 2015' & outcome_definition %like% 't3', `:=` (fup_years=7, exposure_recall_type='exposed at t1 and/or t2, outcome at t3')]
ipv_dep[Study_ID=='ouellet-morin 2015' & !outcome_definition %like% 't3', `:=` (fup_years=5, exposure_recall_type='exposed at t1 and/or t2, outcome at t2')]
ipv_dep[Study_ID=='suglia 2011', exposure_recall_type:=exposed_level]
ipv_dep[Study_ID=='suglia 2011', exposed_level:='any exposure']
ipv_dep[Study_ID=='ahmadabadi 2020' & violence_type=='emotional', violence_type:='psychological']
ipv_dep <- ipv_dep[cause!='moderate to severe depressive disorder']
ipv_dep[Study_ID=='taft 2008' & exposure_recall_type=='lifetime', exposure_recall_type:='lifetime, including past 4 years']
ipv_dep[Study_ID=='taft 2008' & exposure_recall_type=='past 4 years', exposure_recall_type:='past 4 years only']

#select best data points
ipv_dep[, use_model:=1]
ipv_dep[Study_ID=='chowdhary 2008' & exposure_recall_type!='lifetime', use_model:=0]
ipv_dep[Study_ID=='brown 2020' & exposure_recall_type!='past year at 10-year follow-up', use_model:=0]
ipv_dep[Study_ID=='taft 2008' & exposure_recall_type=='past 4 years only', use_model:=0]
ipv_dep[Study_ID=='suglia 2011' & exposure_recall_type!='ipv prior to 12-month follow-up', use_model:=0]
ipv_dep[Study_ID=='suglia 2011' & outcome_type=='prevalence', use_model:=0]
ipv_dep[Study_ID=='ouellet-morin 2015' & exposed_level!='any exposure', use_model:=0]
ipv_dep[Study_ID=='ouellet-morin 2015' & !outcome_definition %like% 't3', use_model:=0]
ipv_dep[Study_ID=='chandan 2020' & subgroup_analysis=='no', use_model:=0]

model_input <- ipv_dep[use_model==1 & sex=='Female']
model_input <- model_input[!(Study_ID %in% unique(ipv_dep[violence_type=='physical; sexual' & violence_type_combination=='and/or']$Study_ID) & violence_type!='physical; sexual')]
model_input <- model_input[!(Study_ID %in% intersect(unique(model_input[violence_type == 'physical']$Study_ID), 
                                                     unique(model_input[violence_type == 'sexual']$Study_ID)) 
                             & !violence_type %in% c('physical', 'sexual'))]
model_input <- model_input[violence_type!='psychological' & violence_type!='harassment']
unique(model_input[, .(Study_ID, violence_type, violence_type_combination)])

#final covariates 
model_input[, cv_aggregate_outcomedef:=ifelse(cause %in% c('depressive disorders, anxiety disorders', 'depressive disorders'), 1, 0)]
model_input[, cv_aggregate_expdef:=0]

model_input[violence_type %in% c('violent relationship', 'physical; sexual; psychological', 'domestic abuse', 'physical; psychological'), cv_aggregate_expdef:=1]

model_input[, cv_component_expdef:=0]
model_input[violence_type=='physical' & !Study_ID %in% unique(model_input[violence_type=='sexual']$Study_ID), cv_component_expdef:=1]
model_input[violence_type=='sexual' & !Study_ID %in% unique(model_input[violence_type=='physical']$Study_ID), cv_component_expdef:=1]

model_input[Study_ID =='ali 2009', `:=` (cv_aggregate_expdef=1, cv_component_expdef=0)]
model_input[Study_ID =='ahmadabadi 2020', `:=` (cv_aggregate_expdef=0, cv_component_expdef=0)]

model_input <- model_input[!(Study_ID=='han 2019' & other_information!='extracted as is')]
model_input <- model_input[!(Study_ID=='han 2019' & violence_type!='physical')]

model_input <- model_input[!(Study_ID=="llosamartínez 2019" & outcome_definition!="depression (bdi-ii>-17)")]

model_input[, bc_non_lifetime_recall:=ifelse(exposure_recall_type %in% c('lifetime', 'ipv prior to 12-month follow-up', 'lifetime, including past 4 years'), 0, 1)]

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
model_input[Study_ID %in% c('ali 2009', 'chowdhary 2008', 'ahmadabadi 2020'),
            ln_rr_se:=ln_rr_se*sqrt(scalar)]

model_input$seq <- 0:(nrow(model_input) - 1)
model_input[,rei_id:=167]
model_input[,cause_id:=568]
setnames(model_input, 'Covidence_#', 'study_id')
model_input[, bundle_id:=2621]
model_input[, bundle_version_id:=0]
model_input[, risk_type:='dichotomous']
model_input[, risk_unit:='exposed']
model_input[, `:=` (ref_risk_lower=0, ref_risk_upper=0, alt_risk_lower=0, alt_risk_upper=0)]
names(model_input) <- gsub('cv_', 'bc_', names(model_input))

# #Sensitivity 1: exclude case-controls
# model_input <- model_input[study_design!='case-control']

# #Sensitivity 2: remove aggregate exposure defs
# model_input <- model_input[bc_aggregate_expdef==0]

#set up cascading dummy for level of control (no age control, controlled for age and 1-2 others, controlled for age+3 or more other vars)
model_input[, bc_L1:=ifelse(bc_age_uncontrolled==0 & num_confounders>3, 0, 1)]

#remove some covs without minimum value of observations
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

write.csv(model_input, paste0(mrbrt_out, 'abuse_ipv_exp-mental_unipolar_mdd.csv'), row.names=F)

#save out table summarizing study info
model_input[!is.na(raw_mean), raw_effect:=paste0(raw_mean, ' (', raw_lower, '-', raw_upper, ')')]
model_input[!is.na(most_adjusted_mean), adj_effect:=paste0(most_adjusted_mean, ' (', most_adjusted_lower, '-', most_adjusted_upper, ')')]
model_input[!is.na(age_mean) & !is.na(age_sd), age_summary:=paste0(age_mean, ' (', age_sd, ')')]
model_input[is.na(age_mean) & !is.na(age_lower) & !is.na(age_upper), age_summary:=paste0(age_lower, '-', age_upper)]
model_input[is.na(age_summary) & !is.na(age_mean), age_summary:=age_mean]

table_output <- model_input[, .(study_id, Study_ID, ihme_loc_id, Study_name, Study_design, 
                                subpopulation, ss,
                                fup_years, attrition,
                                sex, age_summary,
                                violence_type, violence_type_combination, Exposure_assessment_instrument,
                                cause, Outcome_assessment_instrument,
                                `control for outcome at baseline`, confounders,
                                raw_effect, adj_effect)]
write.csv(table_output, paste0(table_out, 'ipv_dep_',gsub('-', '_', Sys.Date()), '.csv'), row.names=F)

bc_output <- unique(model_input[, .SD, .SDcols=c('Study_ID', candidate_covs_one)])
bc_output[, `Author and year`:=str_to_title(Study_ID)]
bc_output[, Study_ID:=NULL]
setcolorder(bc_output, neworder=c('Author and year'))
write.csv(bc_output, paste0(table_out, ro_pair, '_biascovs_',gsub('-', '_', Sys.Date()), '.csv'), row.names=F)
