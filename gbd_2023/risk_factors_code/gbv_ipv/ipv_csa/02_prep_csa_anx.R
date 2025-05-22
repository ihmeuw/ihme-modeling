####################################################################################################################################################################
# 
# Author: USERNAME
# Purpose: Clean & prepare CSA - Anxiety Disorders RR data for modeling
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

ro_pair <- 'csa_anxiety'

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
data[, bc_admin_exp_method:=ifelse(Exposure_assessment_method=="routinely collected/administrative data",1,0)]

#Anxiety disorder data
anx <- data[(cause %in% c('anxiety disorders', 'post-traumatic stress disorder', 'generalized anxiety disorder', 'social phobia') | cause %like% 'anx') & 
              (!cause %like% 'natal' & !cause %like% 'partum' & !cause %like% 'preg')]


anx[, .(length(unique(Study_ID))), by=.(cause)]

anx[!is.na(most_adjusted_mean), `:=` (mean=most_adjusted_mean, lower=most_adjusted_lower, upper=most_adjusted_upper)]
anx[is.na(most_adjusted_mean), `:=` (mean=raw_mean, lower=raw_lower, upper=raw_upper)]

#start to create subsets by exposure type
cm_anx <- anx[exposure_timing=='Child maltreatment']
length(unique(cm_anx$Study_ID))

#get csa
cm_anx[, .(length(unique(Study_ID))), by=.(violence_type)]
csa_anx <- cm_anx[violence_type == 'sexual']
length(unique(csa_anx$Study_ID))

#merge on additional study info/study-level covariates
study_info <- fread(paste0(study_info_dir, ro_pair, '_annotated.csv'))
csa_anx <- merge(csa_anx, study_info[, .(`Covidence_#`, bc_subpop,
                                         bc_symptom_scale, bc_uq_excluded, bc_selfreport_dep_diagnosis,
                                         bc_twin_study, bc_lifetime_diagnosis)], all.x=T)

# manual exclusions
csa_anx <- csa_anx[!Study_ID %in% c('cutajar 2010', 'austin 2020')]

# 1. Prepare CSA input data set ------------------------------------------------------------------------------------------------------------------------------------------

#fix exposure mapping
csa_anx[exposed_level=='csa involving intercourse', exposed_level:='intercourse csa']

#revise
csa_anx[Study_ID=='widom 1999', `:=` (bc_age_uncontrolled=0, bc_sex_uncontrolled=0)]

#prep to plot
csa_anx[, plot:=1]
csa_anx[is.na(sex), `:=` (sex='Combined Male and Female', cv_percentfemale=0.5)]

#pick subgroup for gonzalez
csa_anx[subgroup_analysis_free_text=="individuals with chronic pain conditions outside of back pain/headache were excluded", plot:=0]
csa_anx[Study_ID %in% c('hovens 2010', 'hovens 2015'), plot:=1]

#mark self-reported (retrospective) exposure versus agency-reported (prospective)
csa_anx[Study_ID=='scott 2012' & exposure_definition=='any prospectively ascertained maltreatment', `:=` (bc_admin_exp_method=1, admin_vs_selfreport='agency records (prospective)')]
csa_anx[Study_ID=='scott 2012' & exposure_definition!='any prospectively ascertained maltreatment', `:=` (bc_admin_exp_method=1, admin_vs_selfreport='self-reported (retrospective)')]
csa_anx[Study_ID=='kisely 2021' & exposure_definition %like% 'queensland', `:=` (bc_admin_exp_method=1, admin_vs_selfreport='agency records (prospective) - notified')]
csa_anx[Study_ID=='kisely 2021' & !exposure_definition %like% 'queensland', `:=` (bc_admin_exp_method=0, plot=1, admin_vs_selfreport='self-reported (retrospective)')]
csa_anx[Study_ID=='kisely 2020' & exposure_definition %like% 'notification', `:=` (bc_admin_exp_method=1, admin_vs_selfreport='agency records (prospective) - notified')]
csa_anx[Study_ID=='kisely 2020' & exposure_definition %like% 'substantiated', `:=` (bc_admin_exp_method=0, plot=1, admin_vs_selfreport='agency records (prospective) - substantiated')]
csa_anx[Study_ID=='cohen 2001', `:=` (bc_admin_exp_method=0, admin_vs_selfreport='self-reported (retrospective)')]

#exclude Cohen because duplicative with Brown 1999
csa_anx[Study_ID=='cohen 2001', plot:=1]

#now, remove studies with shorter follow-up
csa_anx[Study_ID=='kisely 2020', plot:=0]
csa_anx[Study_ID=='kisely 2021' & admin_vs_selfreport=='agency records (prospective) - notified', plot:=0]
csa_anx[Study_ID=='kisely 2021' & cause=='post-traumatic stress disorder', plot:=0]
csa_anx[Study_ID=='kisely 2018', plot:=0]
csa_anx[Study_ID=='hovens 2010', plot:=0]
csa_anx[Study_ID=='fergusson 1996', plot:=0]
csa_anx[Study_ID=='najman 2022', plot:=0]

#drop studies with non-acceptable exposure defs
csa_anx[!cause %in% c('social phobia', 'post-traumatic stress disorder', 'generalized anxiety disorder', 'anxiety disorders',
                      'anxiety disorders excluding ptsd'), plot:=0]

#choose exposure level
csa_anx[Study_ID %in% c('kendler 2000') & exposed_level!='any exposure', plot:=0]

csa_anx3 <- copy(csa_anx[plot==1])

#set up some more covs
csa_anx3[, cv_component_outcomedef:=ifelse(cause %in% c('social phobia', 'post-traumatic stress disorder', 'generalized anxiety disorder'), 1, 0)]
csa_anx3[Study_ID=='cutajar 2010', cv_component_outcomedef:=0]
csa_anx3[Study_ID=='dinwiddie 2000', `:=` (bc_females_only=1)]

model_input <- copy(csa_anx3)
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
model_input[,cause_id:=571]
setnames(model_input, 'Covidence_#', 'study_id')
model_input[, bundle_id:=2618]
model_input[, bundle_version_id:=0]
model_input[, risk_type:='dichotomous']
model_input[, risk_unit:='exposed']
model_input[, `:=` (ref_risk_lower=0, ref_risk_upper=0, alt_risk_lower=0, alt_risk_upper=0)]
names(model_input) <- gsub('cv_', 'bc_', names(model_input))

#revise widom 1999 confounders
model_input[Study_ID=='widom 1999', `:=` (confounders='age; race; sex; family social class', num_confounders=4,
                                          bc_age_uncontrolled=0, bc_sex_uncontrolled=0, bc_confounding_uncontrolled=0, bc_females_only=0)]
model_input[Study_ID=='zinzow 2012', bc_lifetime_diagnosis:=0]
#fix study controlled for gender
model_input[Study_ID=='fergusson 2008', bc_sex_uncontrolled:=0]

model_input[, confounders:=gsub('age; ', '', confounders)]
model_input[, confounders:=gsub('sex; ', '', confounders)]
model_input[, confounders:=gsub('sex', '', confounders)]
model_input[, confounders:=gsub('age', '', confounders)]

model_input[, num_confounders:=str_count(confounders, ';')+str_count(confounders,',')+1]
model_input[confounders=='', num_confounders:=0]
model_input[bc_twin_study==1, `:=` (bc_age_uncontrolled=0, bc_L1=0, bc_confounding_uncontrolled=0)]
model_input[, bc_L1:=ifelse(bc_age_uncontrolled==0 & bc_sex_uncontrolled==0 & num_confounders>2, 0, 1)]

#remove irrelevant covariates
model_input[, c('bc_uq_excluded', 'bc_twin_study'):=NULL]
model_input[bc_males_only==1 | bc_females_only==1, bc_subpop:=1]
# 
# #sensitivity analysis 1: exclude cc studies
# model_input <- model_input[Study_design!='case-control']

# #sensitivity analysis 2: exclude studies with adminsitrative exposure ascertainment
# model_input <- model_input[Exposure_assessment_method!='routinely collected/administrative data']

# #sensitivity 3: exclude studies with component outcome def (selected as significant in main model)
# model_input <- model_input[bc_component_outcomedef==0]

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

write.csv(model_input, paste0(mrbrt_out, 'abuse_csa-mental_anxiety.csv'), row.names=F)

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
write.csv(table_output, paste0(table_out, 'csa_anxiety_',gsub('-', '_', Sys.Date()), '.csv'), row.names=F)

bc_output <- unique(model_input[, .SD, .SDcols=c('Study_ID', setdiff(candidate_covs_one, dup_covs))])
bc_output[, `Author and year`:=str_to_title(Study_ID)]
bc_output[, Study_ID:=NULL]
setcolorder(bc_output, neworder=c('Author and year'))
write.csv(bc_output, paste0(table_out, 'csa_anxiety_biascovs_',gsub('-', '_', Sys.Date()), '.csv'), row.names=F)
