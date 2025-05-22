####################################################################################################################################################################
# 
# Author: USERNAME
# Purpose: Clean & prepare downloaded Covidence extractions for MR-BRT modeling and pipeline
#
####################################################################################################################################################################

rm(list=ls())

# set-up environment -----------------------------------------------------------------------------------------------------------------------------------------------

library(data.table)
library(dplyr)
library(openxlsx)
library(ggplot2)
library(stringr)

invisible(sapply(list.files("FILEPATH", full.names = T), source))

aesth <- theme_bw() + theme(axis.text.x = element_text(size=14,face="plain",angle=0,hjust=1),
                            axis.text.y = element_text(size=14,face="plain"),
                            strip.text = element_text(size=14,face="bold"),
                            plot.background = element_blank(),
                            plot.title =element_text(size=14,face='bold'),
                            legend.text=element_text(size=14),
                            legend.title=element_text(size=14, face='bold'))

ext_dir <- 'FILEPATH'
outcome_map_dir <- 'FILEPATH'

# load in data -------------------------------------------------------------------------------------------------------------------------------------------------------

#get mapped outcomes
outcome_map <- as.data.table(read.xlsx(paste0(outcome_map_dir, 'outcome_mapping_03_29_2023.xlsx'), sheet='data'))
outcome_map <- outcome_map[!is.na(`Covidence_#`)]

#change header names to corrected version using hand-made changes to an earlier file
ext <- fread(paste0(ext_dir, 'data_download_Apr20.csv'))
hdrs <- fread(paste0(ext_dir, 'data_download_Jan19.csv'))
names(ext) <- names(hdrs)

#read in continued extractions (those continued outside of Covidence system for having more than 24 unique data points to extract)
musp <- fread(paste0(ext_dir, 'Continued_Extractions_Mar27_2023_Completed_All_Reviewers.csv'), encoding = "UTF-8")
names(musp)[1:1131] <- names(hdrs)
musp_ids <- unique(musp$`Covidence #`)

#read in last extraction
kas2022 <- fread(paste0(ext_dir, 'CovidenceID_185188_Extraction.csv'), encoding = "UTF-8")
names(kas2022)[1:1131] <- names(hdrs)
names(kas2022) <- gsub('er  UI', 'er (UI)', names(kas2022))
kas2022_id <- unique(kas2022$`Covidence #`)

#take extended musp extractions and consensus extractions
consensus_ids <- ext[`Reviewer Name`=='Consensus']$`Covidence #`
consensus <- ext[`Reviewer Name`=='Consensus' & !`Covidence #` %in% c(kas2022_id, musp_ids)]
non_consensus <- ext[!`Covidence #` %in% c(consensus_ids, musp_ids, kas2022_id)]
data <- rbind(consensus, non_consensus, musp, kas2022, fill=T)

#list of manual exclusions identified in consensus checking
excl <- c(57878,58926,149506,82353,66892,70567,65969,65029)

#list of intergenerational or linear effect sizes that were extracted - not to be used for these analyses
ip <- c(62692,63001,51345,49346,50003,134181,49892,140808,49146,58338) 

#list of studies using duplicated underlying cohorts/datasets
underlying_data_dup <- c(50631,118591)

#list of dissertations
dissertations <- c(121154,122029,123421,131520,131549,132897,134121,134181,136310,136323,137393)

#exclude relevant article types
data <- data[!`Covidence #` %in% c(excl, ip, underlying_data_dup, dissertations)]

#melt and format
data_long <- melt.data.table(data, id.vars=names(data)[!names(data) %like% 'Model'])
entry_names <- trimws(str_sub(unique(data_long[variable %like% 'Model 1 ']$variable), 9, -1))
hundreds <- as.data.table(expand.grid('Model ', 100:210, ' ', entry_names))
hundreds[, names:=paste0(Var1, Var2, Var3, Var4)]
hundreds2 <- hundreds$names
data_long[, model_label:=ifelse(variable %in% hundreds2, trimws(str_sub(variable, 1, 9)), trimws(str_sub(variable, 1, 8)))]
data_long[model_label %in% paste0('Model ', 1:9), variable_short:=trimws(tolower(str_sub(variable, 9,-1)))]
data_long[model_label %in% paste0('Model ', 10:99), variable_short:=trimws(tolower(str_sub(variable, 10,-1)))]
data_long[model_label %in% paste0('Model ', 100:210), variable_short:=trimws(tolower(str_sub(variable, 11,-1)))]
data_long[variable_short=='ages mean', variable_short:='age mean']
data_long[variable_short=='ages lower', variable_short:='age lower']
data_long[variable_short=='ages upper', variable_short:='age upper']
data_long[variable_short=='ages sd', variable_short:='age sd']
data_long[variable_short=='perpetrator', variable_short:='perpetrator type']

#remove empty models
empty_models <- data_long[variable_short=='exposure definition' & (value=='' | is.na(value)), .(Title, `Study ID`, model_label, `Covidence #`)]
empty_models[, model_empty:='Y']
data_long <- merge(data_long, empty_models, by=c('Covidence #', 'Study ID', 'Title', 'model_label'), allow.cartesian = T, all.x=T)
data_long <- data_long[is.na(model_empty)]

#cast back wide
dups <- data_long[duplicated(data_long[, .(Title, `Study ID`, model_label, `Covidence #`, variable_short)])]
data_wide <- dcast.data.table(data_long[!duplicated(data_long[, .(Title, `Study ID`, model_label, `Covidence #`, variable_short)])], 
                              Title+`Study ID`+model_label+`Covidence #`~variable_short)

#bind back on other data
meta_data <- data[, .SD, .SDcols=names(data)[!names(data) %like% 'Model']]
all_data <- merge(data_wide, meta_data, by=c('Covidence #', 'Study ID', 'Title'), all.x=T, allow.cartesian = T)
all_data[, `ages:`:=NULL]
all_data[, `effect size:`:=NULL]
all_data[, `sample size:`:=NULL]
all_data[, `Study locations:`:=NULL]
all_data[, `temporality of exposure:`:=NULL]

#take out 'other:' leaders 
var_cols <- names(all_data)
all_data[, (var_cols):=lapply(.SD, function(x) gsub('Other: ', '', x)), .SDcols=var_cols]
all_data[, (var_cols):=lapply(.SD, tolower), .SDcols=var_cols]

#replace spaces from column names
names(all_data) <- gsub(' ', '_', names(all_data))

#transform effect sizes to numerical
adjusted_effect_sizes <- c('most_adjusted_lower_(ui)', 'most_adjusted_upper_(ui)','most_adjusted_mean')
raw_effect_sizes <- c('raw_lower_(ui)', 'raw_upper_(ui)','raw_mean')
effect_sizes <- c(adjusted_effect_sizes, raw_effect_sizes)
all_data[, (effect_sizes):=lapply(.SD, as.numeric), .SDcols=effect_sizes]

new_es_names <- c('most_adjusted_lower', 'most_adjusted_upper', 'most_adjusted_mean', 'raw_lower', 'raw_upper', 'raw_mean')
setnames(all_data, effect_sizes, new_es_names)

#more formatting
all_data[, `percent_female`:=as.numeric(`percent_female`)]
all_data[, sex:=ifelse(`percent_female`==1, 'Female', ifelse(`percent_female`==0, 'Male', 'Combined Male and Female'))]
all_data[, violence_type_short := tolower(ifelse(`violence_type` %like% ';', 'combination', `violence_type`))]
all_data[, outcome_definition:=trimws(outcome_definition)]

combo_cols <- c('violence_type', 'perpetrator_type')
all_data[, (combo_cols):=lapply(.SD, function(x) gsub(' ; ', '; ', x)), .SDcols=combo_cols]

#mark if multiple locations, otherwise keep sole location and remove extra loc fields
loc_id_cols <- names(all_data)[names(all_data) %like% 'Location_ID']
loc_name_cols <- names(all_data)[names(all_data) %like% 'Location_Name']
all_data[, (loc_id_cols):=lapply(.SD, toupper), .SDcols=loc_id_cols]
all_data[, (loc_name_cols):=lapply(.SD, str_to_title), .SDcols=loc_name_cols]

#save out data with all locs for mapping
#clean up to readable format
mapdata <- all_data[, .SD, .SDcols=c('Covidence_#', 'Study_ID', loc_id_cols, loc_name_cols)]
write.csv(mapdata, 'FILEPATH/cleaned_data_locs.csv', row.names = F)

all_data[`Location_2_Location_Name_(location_ascii_name)`!='', `:=` (`Location_1_Location_Name_(location_ascii_name)`='Multiple', `Location_1_Location_ID_(ihme_loc_id)`='MULT')]
setnames(all_data, c('Location_1_Location_Name_(location_ascii_name)','Location_1_Location_ID_(ihme_loc_id)'), c('location_name', 'ihme_loc_id'))
loc_cols_remove <- c(loc_id_cols, loc_name_cols)
loc_cols_remove <- loc_cols_remove[!loc_cols_remove %like% '_1_']
all_data[, (loc_cols_remove):=NULL]

#merge on outcomes
og <- copy(all_data)

outcome_map[cause_mapping=='component definition ', cause:=component_cause_name]
outcome_map[cause_mapping=='aggregate definition', cause:=paste0(cause, ', ', aggregate_cause1, ', ', aggregate_cause2, ', ', aggregate_cause3, ', ', aggregate_cause4, ', ', aggregate_cause_nonGBD)]
outcome_map[, cause:=gsub(', NA', '', cause)]
# outcome_map[, c('rei_id', 'risk_mapping', 'risk_notes'):=NULL]
outcome_map <- outcome_map[!duplicated(outcome_map[, .(`Covidence_#`, outcome_name, outcome_type)])]
outcome_map[is.na(outcome_type), outcome_type:='']
outcome_map[, outcome_name:=gsub('â€“', '–', outcome_name)]
outcome_map[, outcome_name:=gsub('\r\n', ' \n', outcome_name)] 

all_data <- merge(all_data, 
                  outcome_map[, .(`Covidence_#`, outcome_name, outcome_type, cause, cause_id, risk, rei_id, cause_or_risk)], 
                  by=c('Covidence_#', 'outcome_name', 'outcome_type'), 
                  all.x=T, all.y=F)

#remove unusable outcomes
all_data[, cause:=tolower(cause)]
# risks <- all_data[cause_or_risk=='risk']
all_data[cause_or_risk=='risk', `:=` (cause=risk, cause_id=rei_id)]

all_data[, c('risk', 'rei_id'):=NULL]
all_data[Study_ID=='seid 2022' & outcome_name %like% 'obese', `:=` (cause_mapping='exact definition', cause='High body-mass index', cause_id=108, cause_or_risk='cause')]
all_data[Study_ID=='houtepen 2020' & outcome_name=='', `:=` (cause_mapping='exact definition', cause='Major depressive disorder', cause_id=567, cause_or_risk='cause')]
unmatched <- all_data[is.na(cause_or_risk)]
unique(unmatched$Study_ID)

all_data <- all_data[cause!='not mappable']
all_data <- all_data[!is.na(cause)]

#calculate SS
ss <- c('number_of_participants_total', 'number_of_participants_exposed_group', 'number_of_participants_unexposed_group',
        'number_of_cases_total', 'number_of_cases_exposed_group', 'number_of_cases_unexposed_group')
all_data[, (ss):=lapply(.SD, as.numeric), .SDcols=ss]
all_data[is.na(number_of_participants_total) & !is.na(number_of_participants_exposed_group) & !is.na(number_of_participants_unexposed_group),
         number_of_participants_total:=number_of_participants_exposed_group+number_of_participants_unexposed_group]
all_data[, ss:=number_of_participants_total]

#standardize attrition indicators
all_data$`Cohort_Study:_Drop-out_rate` <- as.numeric(all_data$`Cohort_Study:_Drop-out_rate`)
all_data$`Case-control_Study:_Percent_of_participants_for_whom_data_ascertained` <- as.numeric(all_data$`Case-control_Study:_Percent_of_participants_for_whom_data_ascertained`)

all_data[, attrition:=ifelse(Study_design %like% 'cohort', `Cohort_Study:_Drop-out_rate`, ifelse(Study_design=='case-control', 1-`Case-control_Study:_Percent_of_participants_for_whom_data_ascertained`, NA))]
all_data[is.na(attrition) & Study_design %in% c('case-control', 'prospective cohort', 'retrospective cohort'), attrition:='not reported']

#standardize fup years
all_data[`Cohort_Study:_Duration_of_follow-up_(units)`=='years', fup_years:=`Cohort_Study:_Duration_of_follow-up_(value)`]
all_data[`Cohort_Study:_Duration_of_follow-up_(units)`=='months', fup_years:=as.numeric(`Cohort_Study:_Duration_of_follow-up_(value)`)/12]
all_data[`Cohort_Study:_Duration_of_follow-up_(units)`=='weeks', fup_years:=as.numeric(`Cohort_Study:_Duration_of_follow-up_(value)`)/52]

#calculate ORs for IDs where only raw cell counts were extracted
all_data[`Covidence_#`%in%c(72128,50205,49669), number_of_noncases_unexposed_group:=number_of_participants_unexposed_group-number_of_cases_unexposed_group]
all_data[`Covidence_#`%in%c(72128,50205,49669), number_of_noncases_exposed_group:=number_of_participants_exposed_group-number_of_cases_exposed_group]
all_data[`Covidence_#`%in%c(72128,50205,49669), raw_mean:=(number_of_cases_exposed_group*number_of_noncases_unexposed_group)/(number_of_cases_unexposed_group*number_of_noncases_exposed_group)]
all_data[`Covidence_#`%in%c(72128,50205,49669), raw_upper:=exp(log(raw_mean)+(1.96*sqrt((1/number_of_cases_exposed_group)+(1/number_of_cases_unexposed_group)+1/(number_of_noncases_exposed_group)+(1/number_of_noncases_unexposed_group))))]
all_data[`Covidence_#`%in%c(72128,50205,49669), raw_lower:=exp(log(raw_mean)-(1.96*sqrt((1/number_of_cases_exposed_group)+(1/number_of_cases_unexposed_group)+1/(number_of_noncases_exposed_group)+(1/number_of_noncases_unexposed_group))))]
all_data[`Covidence_#`%in%c(72128,50205,49669), `:=` (Effect_Size_Measure='odds ratio (or)', Uncertainty_type="confidence interval", Confidence_interval_level = "95")]


#clean up to readable format
data2 <- all_data[, .(`Covidence_#`, Study_ID, Study_name, Title, location_name, ihme_loc_id, Year_start, Year_end,
                  Study_design, Location_representive, Study_selection_criteria,
                  fup_years, attrition, `Case-control_Study:_Controls_selected_from_community`,
                  exposure_definition, exposed_level, 
                  unexposed_definition, unexposed_level, violence_type, violence_type_combination, perpetrator_type, 
                  exposure_age_lower, exposure_age_upper, exposure_recall_type, 
                  outcome_name, outcome_type, outcome_definition, cause, cause_id, 
                  Effect_Size_Measure, Uncertainty_type, Confidence_interval_level,
                  most_adjusted_mean, most_adjusted_lower, most_adjusted_upper, most_adjusted_other_uncertainty_value,
                  raw_mean, raw_lower, raw_upper, raw_other_uncertainty_value, 
                  other_information,
                  sex, percent_female, ss,
                  number_of_cases_exposed_group, number_of_cases_unexposed_group,
                  age_lower, age_upper, age_mean, age_sd, 
                  confounders,
                  subgroup_analysis, subgroup_analysis_free_text,
                  Exposure_assessment_method, Exposure_assessment_instrument, 
                  Outcome_assessment_method, Outcome_assessment_instrument,
                  Extractor_notes, Reviewer_Name)]

#exponentiate log odds from Fergusson 2008
data2[`Covidence_#`==48956, `:=` (most_adjusted_mean=exp(most_adjusted_mean), 
                                  most_adjusted_lower=exp(most_adjusted_mean-1.96*as.numeric(most_adjusted_other_uncertainty_value)), 
                                  most_adjusted_upper=exp(most_adjusted_mean+1.96*as.numeric(most_adjusted_other_uncertainty_value)),
                                  Confidence_interval_level='95',
                                  Uncertainty_type="confidence interval")]

#manually fix entries from Chowdhary 2008
data2[Study_ID=='chowdhary 2008', confounders:="age; income; literacy (whether able to read and write)"]
data2[Study_ID=='chowdhary 2008' & cause=='depressive disorders, anxiety disorders', cause:='depressive disorders']

#set up study level covs
data2[, bc_odds_ratio:=ifelse(Effect_Size_Measure=='odds ratio (or)', 1, 0)]

#find sex-stratified studies (ie report for both sexes, but separately) and identify single-sex studies with corresponding bc_ variables
sex_strat_studies <- intersect(unique(data2[sex=='Male']$Study_ID), unique(data2[sex=='Female']$Study_ID))
data2[, bc_males_only:=ifelse(sex=='Male' & !Study_ID %in% sex_strat_studies, 1, 0)]
data2[, bc_females_only:=ifelse(sex=='Female' & !Study_ID %in% sex_strat_studies, 1, 0)]

#continue with standard covs                                                                  
data2[, bc_representativeness:=ifelse(Location_representive=='yes', 0, 1)]
data2[, bc_reverse_causation:=ifelse(Study_design=='case-control', 1, 0)]
data2[, bc_selection_bias_ascertained:=0]
data2[, bc_selection_bias:=0]
data2[attrition=='not reported', bc_selection_bias_ascertained:=1]
data2[attrition>0.2, bc_selection_bias:=1]
data2[, bc_admin_exp_method:=ifelse(Exposure_assessment_method=='routinely collected/administrative data', 1, 0)]

#create covariates capturing level of control in study
data2[Study_ID=='tenhave 2019', confounders:='sex; age; study-ID']
data2[Study_ID=='fergusson 2008', confounders:=paste0('sex; ', confounders)]
data2[, bc_confounding_uncontrolled:=ifelse(confounders=='', 1, 0)]
data2[, bc_sex_uncontrolled:=ifelse(!confounders %like% 'sex' & sex=='Combined Male and Female', 1, 0)]

# find studies which aren't controlled for age nor age-stratified analyses
age_cols <- c('age_mean', 'age_sd', 'age_lower', 'age_upper')
data2[, (age_cols):=lapply(.SD, as.numeric), .SDcols=age_cols]
age_strat <- unique(data2[, .(Study_ID, `Covidence_#`, age_lower, age_upper, age_mean, age_sd, sex)])
age_strat2 <- age_strat[, .(age_group_count=.N), by=.(Study_ID, `Covidence_#`, sex)]
single_age_ids <- unique(data2[((age_upper-age_lower)<5) | (!is.na(age_sd) & age_sd<1)]$`Covidence_#`) #if study conducted among single age group spanning less than 5 years, consider controlled for age
data2[, bc_age_uncontrolled:=ifelse(!confounders %like% 'age' & !`Covidence_#` %in% c(age_strat2[age_group_count>1], single_age_ids), 1, 0)]

#rough count of number of confounders
data2[, num_confounders:=str_count(confounders, ';')+str_count(confounders,',')+1]

#set up exposure type indicators
data2[exposure_recall_type %in% c('childhood'), exposure_age_upper:=18]
data2[, exposure_timing:=ifelse(exposure_age_upper<19, 'Child maltreatment', 'Adulthood violence exposure')]

data2[violence_type %in% c("witnessing parent or adult violence", "witnessed ipv",
                            "witnessed interparental violence","family violence", 
                            "domestic violence/witnessed violence"), 
       violence_type:='witnessed domestic violence']
data2[, violence_type:=gsub('witnessing parental violence', 'witnessed domestic violence', violence_type)]
data2[, violence_type:=gsub('any neglect', 'neglect', violence_type)]

#create easy indicators of violence type
data2[, gbd_csa:=ifelse(violence_type=='sexual' & exposure_age_upper<19 & exposure_recall_type=='lifetime', 1, 0)]
data2[, ch_physical:=ifelse(violence_type=='physical' & exposure_age_upper<19 & exposure_recall_type=='lifetime', 1, 0)]
data2[, ch_psychological:=ifelse((violence_type=='psychological' | violence_type=='emotional') & exposure_age_upper<19 & exposure_recall_type=='lifetime', 1, 0)]
data2[, ch_neglect:=ifelse(violence_type=='neglect' & exposure_age_upper<19 & exposure_recall_type=='lifetime', 1, 0)]
data2[, gbd_ipv:=ifelse(violence_type %in% c('sexual', 'physical', 'physical; sexual', 'physical; sexual; psychological') & perpetrator_type %in% c('partner', 'partner; former partner'), 1, 0)]
data2[, psych_ipv:=ifelse(violence_type=='psychological' & perpetrator_type %like% 'partner', 1, 0)]
data2[, phys_ipv:=ifelse(violence_type=='physical' & perpetrator_type %like% 'partner', 1, 0)]
data2[, sex_ipv:=ifelse(violence_type=='sexual' & perpetrator_type %like% 'partner', 1, 0)]
data2[, nonparter_sexual_viol:=ifelse(violence_type=='sexual' & perpetrator_type=='non-partner', 1, 0)]

#minor manual fix
data2[Study_ID=='cohen 2001', bc_females_only:=0]

write.csv(data2, paste0('FILEPATH/data_cleaned_', gsub('-', '_', Sys.Date()), '.csv'), row.names=F)



