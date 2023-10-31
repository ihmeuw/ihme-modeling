  #--------------------------------------------------------------
  # Name: 
  # Date: 27 Feb 2021
  # Project: GBD nonfatal COVID
  # Purpose: estimate long COVID duration among mild/moderate cases and among hospital cases
  #--------------------------------------------------------------
  
  # setup -------------------------------------------------------
  
  # clear workspace
#  rm(list=ls())
  setwd("FILEPATH")
  
  
  # load packages
  pacman::p_load(data.table, ggplot2, DBI, openxlsx, gtools)

  
  folder <- "FILEPATH"
  outputfolder <- "FILEPATH"
  
  
  # datadate <- '051321'
  # datadate <- '081221'
  # datadate <- '091221'
  datadate <- '012822'
  data <- paste0("FILEPATH/adjusted_long_covid_data_", datadate, ".csv")
  

  dataset_raw <- read.csv(data)
  dataset_raw$follow_up_index <- as.character(dataset_raw$follow_up_index)
  dataset <- data.table(dataset_raw)
  table(dataset$study_id)
  

  ################################################################
  # DATA PREP
  ################################################################
  
  table(dataset$symptom_cluster, useNA = 'always')
  dataset <- dataset[!is.na(symptom_cluster)]
  
  ########################################################
  # SYMPTOM CLUSTERS
  
  table(dataset$symptom_cluster)
  dataset$outcome[dataset$symptom_cluster=="any symptom cluster" | dataset$symptom_cluster=="Persistent symtpoms related to illness (new/worsened)"] <- "any"
  dataset$outcome[dataset$symptom_cluster=="post-acute fatigue syndrome" | dataset$symptom_cluster=="Fatigue"] <- "fat"
  dataset$outcome[dataset$symptom_cluster=="cognitive"] <- "cog"
  dataset$outcome[dataset$symptom_cluster=="respiratory"] <- "rsp"
  dataset$outcome[dataset$symptom_cluster=="post-acute fatigue and cognitive" | dataset$symptom_cluster=="Fatigue + cognitive"] <- "fat_cog"
  dataset$outcome[dataset$symptom_cluster=="post-acute fatigue and respiratory" | dataset$symptom_cluster=="Fatigue + respiratory"] <- "fat_rsp"
  dataset$outcome[dataset$symptom_cluster=="respiratory and cognitive" | dataset$symptom_cluster=="Cognitive + respiratory"] <- "cog_rsp"
  dataset$outcome[dataset$symptom_cluster=="post-acute fatigue and respiratory and cognitive" | dataset$symptom_cluster=="Fatigue + cognitive + respiratory"] <- "fat_cog_rsp"
  dataset$outcome[dataset$symptom_cluster=="mild respiratory among respiratory" | dataset$symptom_cluster=="Mild respiratory among Respiratory" | 
                    dataset$symptom_cluster=="mild respiratory among respiratory" | dataset$symptom_cluster=="Mild respiratory among respiratory"] <- "mild_rsp"
  dataset$outcome[dataset$symptom_cluster=="moderate respiratory among respiratory" | dataset$symptom_cluster=="Moderate respiratory among Respiratory" | 
                    dataset$symptom_cluster=="moderate respiratory among respiratory" | dataset$symptom_cluster=="Moderate respiratory among respiratory"] <- "mod_rsp"
  dataset$outcome[dataset$symptom_cluster=="severe respiratory among respiratory" | dataset$symptom_cluster=="Severe respiratory among Respiratory" | 
                    dataset$symptom_cluster=="severe respiratory among respiratory" | dataset$symptom_cluster=="Severe respiratory among respiratory"] <- "sev_rsp"
  dataset$outcome[dataset$symptom_cluster=="mild cognitive among cognitive" | dataset$symptom_cluster=="Mild cognitive among Cognitive" | 
                    dataset$symptom_cluster=="mild cognitive among cognitive" | dataset$symptom_cluster=="Mild cognitive among cognitive"] <- "mild_cog"
  dataset$outcome[dataset$symptom_cluster=="moderate cognitive among cognitive" | dataset$symptom_cluster=="Moderate cognitive among Cognitive" | 
                    dataset$symptom_cluster=="moderate cognitive among cognitive" | dataset$symptom_cluster=="Moderate cognitive among cognitive"] <- "mod_cog"
  dataset$outcome[dataset$symptom_cluster=="GBS"] <- "gbs"
  table(dataset$outcome, useNA = 'always')
  table(dataset$symptom_cluster[is.na(dataset$outcome)], useNA = 'always')
  
  
  
  ########################################################
  # ADJUST DATA WITHOUT PRE-COVID CRITERIA (USING ADJUSTMENT DONE BY --)
  # this also replaces mean with mean_adjusted for overlap categories where mean_adjusted = (mean among all COVID) / (mean any long COVID among all COVID)
  
  dataset$mean_orig <- dataset$mean
  dataset[(study_id=="CO-FLOW" | study_id=="Sweden PronMed" | study_id=="Helbok et al" | study_id=="pa-COVID" | study_id=="HAARVI") 
          & !is.na(dataset$mean_adjusted), mean := mean_adjusted]
  dataset$standard_error_orig <- dataset$standard_error
  dataset[(study_id=="CO-FLOW" | study_id=="Sweden PronMed" | study_id=="Helbok et al" | study_id=="pa-COVID" | study_id=="HAARVI") 
          & !is.na(dataset$mean_adjusted), standard_error := se_adjusted]
  

  dataset$prev <- NULL
  dataset$prev_se <- NULL
  dataset$Group <- NULL
  dataset$se_adjusted <- NULL
  dataset$diff <- NULL
  dataset$diff_se <- NULL
  dataset$data_id <- NULL
  dataset$adjusted_ratio <- NULL

  
  dim(dataset)
  table(dataset$study_id)
  table(dataset$study_id, dataset$outcome)
  
  dataset$is_outlier <- 0
#  dataset$exclude <- 0
  
  ########################################################
  # COVARIATES
  
  table(dataset$outcome_name, useNA = 'always')
  dataset$memory_problems <- 0
  dataset[outcome_name=="memory problems" | outcome_name=="Memory difficulties" | outcome_name=="Mental slowness" | outcome_name=="Concentration problems" | 
            outcome_name=="confusion/lack of concentration" | outcome_name=="Amnesic complaints" |outcome_name=="Memory loss" | outcome_name=="Memory problems" |
            outcome_name=="New or worsened concentration problem" | outcome_name=="New or worsened short-term memory problem" | outcome_name=="confusion" | 
            outcome_name=="Cognitive fuzziness/brain fog/difficulty concentrating" | outcome_name=="cognitive problems" | outcome_name=="concentration memory probs" | 
            outcome_name=="confusion, disorientation, or drowsiness" | outcome_name=="memory disorders" | outcome_name=="memory/concentration impairment" |
            outcome_name=="concentration difficulties" | outcome_name=="concentration issues" | outcome_name=="memory impairment" | outcome_name=="cognitive failure", 
          memory_problems := 1]
  dataset$fatigue <- 0
  dataset[outcome_name=="Fatigue" | outcome_name=="fatigue" | outcome_name=="Fatigue or muscle weakness" | outcome_name=="physical decline/ fatigue" | 
            outcome_name=="fatigue " | outcome_name=="tiredness" | outcome_name=="fatigue and worse than pre-COVID", 
          fatigue := 1]
  dataset$cough <- 0
  dataset$cough[dataset$outcome_name=="Cough" | dataset$outcome_name=="cough" | dataset$outcome_name=="Cough (new/worsened)" | 
                  dataset$outcome_name=="persistent cough"] <- 1
  dataset$shortness_of_breath <- 0
  dataset[outcome_name=="shortness of breath" | outcome_name=="Shortness of breath" | outcome_name=="Dyspnea" | outcome_name=="Breathlessness" |
            outcome_name=="difficulty breathing/chest tightness" | outcome_name=="Dyspnoea" |outcome_name=="Shortness of breath/chest tightness/ wheezing (new/worsened)" |
            outcome_name=="breathing difficulties" | outcome_name=="dyspnea" | outcome_name=="postactivity polypnoea" | outcome_name=="dyspnoea" |
            outcome_name=="exertional dyspnoe" | outcome_name=="exertional dyspnoea" | outcome_name=="respiratory disorders" | outcome_name=="respiratory symptoms", 
          shortness_of_breath := 1]
  dataset$administrative <- 0
  dataset$administrative[dataset$sample_characteristics=="administrative data" | dataset$study_id=="Veterans Affairs" | dataset$study_id=="PRA"] <- 1
  
  dataset[outcome_name=="mMRC >= 2" | outcome_name=="dyspnea at rest (our severe resp category)", exclude := 1]
  
  table(dataset$outcome_name[dataset$memory_problems==0 & dataset$fatigue==0 & dataset$cough==0 & dataset$shortness_of_breath==0])
  
  
  ########################################################
  # OTHER SYMPTOM LIST FOR "ANY LONG COVID"
  
  dataset$other_list <- 0
  dataset[outcome_name=="from their symptom list" | outcome_name=="Persistent symtpoms related to illness (new/worsened)" | 
            outcome_name=="any_symptom" | outcome_name=="new or persistent symptoms" | outcome_name=="Persistent symtpoms related to illness (new/worsened)" |
            outcome_name=="Post-COVID syndrome, defined as the persistence of at least one clinically relevant symptom, spirometry disturbances or significant radiological alterations" |
            outcome_name=="symptomatic" | outcome_name=="any persistent symptoms" | outcome_name=="overall",
          other_list := 1]
  table(dataset$outcome_name, dataset$outcome)
  table(dataset$outcome_name[dataset$memory_problems==0 & dataset$fatigue==0 & dataset$cough==0 & dataset$shortness_of_breath==0], dataset$other_list[dataset$memory_problems==0 & dataset$fatigue==0 & dataset$cough==0 & dataset$shortness_of_breath==0])
  
  
  
  ########################################################
  # MEAN, STANDARD ERROR
  
  dataset$mean <- as.numeric(dataset$mean)
  dataset$cases <- as.numeric(dataset$cases)
  dataset$sample_size <- as.numeric(dataset$sample_size)
  dataset$sample_size_envelope <- as.numeric(dataset$sample_size_envelope)
  dataset$lower <- as.numeric(dataset$lower)
  dataset$upper <- as.numeric(dataset$upper)
  dataset$follow_up_value <- as.numeric(dataset$follow_up_value)
  # fill in blank means as cases / sample size for main symptom clusters
  dataset[is.na(mean) & (outcome=='any' | outcome=='cog' | outcome=='fat' | outcome=='rsp'), mean := cases/sample_size]
  # fill in blank means as cases / sample size for overlaps where mean_adjusted is blank
  dataset[!is.na(sample_size_envelope) & !is.na(mean_adjusted) & (study_id=='CO-FLOW' | study_id=='HAARVI' | study_id=='Faroe' | study_id=='Iran' | study_id=='Italy ISARIC' | 
                                                                   study_id=='Sechenov StopCOVID' | study_id=='Sweden PronMed' | study_id=='Zurich CC') & 
            (outcome=='cog_rsp' | outcome=='fat_cog' | outcome=='fat_cog_rsp' | outcome=='fat_rsp' | 
               outcome=='mild_cog' | outcome=='mild_rsp' | outcome=='mod_cog' | outcome=='mod_rsp' | outcome=='sev_rsp'), mean := mean_adjusted]
  dataset[!is.na(sample_size_envelope) & is.na(mean_adjusted) & (study_id=='CO-FLOW' | study_id=='HAARVI' | study_id=='Faroe' | study_id=='Iran' | study_id=='Italy ISARIC' | 
                                                                   study_id=='Sechenov StopCOVID' | study_id=='Sweden PronMed' | study_id=='Zurich CC') & 
            (outcome=='cog_rsp' | outcome=='fat_cog' | outcome=='fat_cog_rsp' | outcome=='fat_rsp' | 
               outcome=='mild_cog' | outcome=='mild_rsp' | outcome=='mod_cog' | outcome=='mod_rsp' | outcome=='sev_rsp'), mean := cases/sample_size_envelope]

  # standard error for cases>=5:
  dataset[cases>=5 & !is.na(cases) & is.na(standard_error) & (outcome=='any' | outcome=='cog' | outcome=='fat' | outcome=='rsp'), 
          standard_error := sqrt((mean * (1-mean)) / sample_size)]
  dataset[cases>=5 & !is.na(cases) & is.na(standard_error) & (outcome=='cog_rsp' | outcome=='fat_cog' | outcome=='fat_cog_rsp' | outcome=='fat_rsp' | 
                                     outcome=='mild_cog' | outcome=='mild_rsp' | outcome=='mod_cog' | outcome=='mod_rsp' | outcome=='sev_rsp'), 
          standard_error := sqrt((mean * (1-mean)) / sample_size_envelope)]
  dataset[cases>=5 & !is.na(cases) & study_id=="PRA" & (outcome=='cog_rsp' | outcome=='fat_cog' | outcome=='fat_cog_rsp' | outcome=='fat_rsp' | outcome=='gbs'), 
          standard_error := sqrt((mean * (1-mean)) / sample_size)]
  
  # standard error for cases<5, use Wilson's formula:
  dataset[cases<5 & !is.na(cases) & is.na(standard_error) & (outcome=='any' | outcome=='cog' | outcome=='fat' | outcome=='rsp'), 
          tag := 1]
  dataset[cases<5 & !is.na(cases) & is.na(standard_error) & (outcome=='any' | outcome=='cog' | outcome=='fat' | outcome=='rsp'), 
          standard_error := sqrt(((mean * (1-mean)) / sample_size) + ((1.96^2) / (4 * sample_size^2))) / (1 + (1.96^2 / sample_size))]
  dataset$tag <- 0
  dataset[cases<5 & !is.na(cases) & is.na(standard_error) & (outcome=='cog_rsp' | outcome=='fat_cog' | outcome=='fat_cog_rsp' | outcome=='fat_rsp' | 
                                                               outcome=='mild_cog' | outcome=='mild_rsp' | outcome=='mod_cog' | outcome=='mod_rsp' | outcome=='sev_rsp'), 
          tag := 1]
  dataset[cases<5 & !is.na(cases) & is.na(standard_error) & (outcome=='cog_rsp' | outcome=='fat_cog' | outcome=='fat_cog_rsp' | outcome=='fat_rsp' | 
                                                               outcome=='mild_cog' | outcome=='mild_rsp' | outcome=='mod_cog' | outcome=='mod_rsp' | outcome=='sev_rsp'), 
          standard_error := sqrt(((mean * (1-mean)) / sample_size_envelope) + ((1.96^2) / (4 * sample_size_envelope^2))) / (1 + (1.96^2 / sample_size_envelope))]
  dataset[cases<5 & !is.na(cases) & study_id=="PRA" & (outcome=='cog_rsp' | outcome=='fat_cog' | outcome=='fat_cog_rsp' | outcome=='fat_rsp' | outcome=='gbs'), 
          standard_error := sqrt(((mean * (1-mean)) / sample_size) + ((1.96^2) / (4 * sample_size^2))) / (1 + (1.96^2 / sample_size))]
  dataset$tag <- NULL
  
  #  dataset[is.na(standard_error), standard_error := sqrt((mean * (1-mean)) / sample_size)]
  # the only data missing both cases and standard error are VA data, which have lower and upper
  dataset[is.na(standard_error), standard_error := (upper - lower) / 3.96]
  
  ########################################################
  # COMMUNITY VS HOSPITAL VS ICU
  
  dataset$hospital <- -1
  dataset$icu <- -1
  unique(dataset$sample_population)
  table(dataset$sample_population)
  dataset[sample_population=="general population" | sample_population=="outpatient" | sample_population=="community" | sample_population=="community + hospital + ICU"
          | sample_population=="Non-hospitalized" | sample_population=="Community" | sample_population=="community + hospital + icu" | sample_population=="comm" | 
            sample_population=="largely comm (10.4% vs 9.3% hospitalised for symptoms in those with pos and neg PCR tests, respectively; see T3)", hospital := 0]
  dataset[sample_population=="hospitalized" | sample_population=="hospitalized and outpatient" | sample_population=="hospitalized + ICU" | sample_population=="hospitalized + icu"
          | sample_population=="hospital" | sample_population=="hospital + icu" | sample_population=="hospital; receiving oxygen alone" | sample_population=="hosptial + icu" 
          | sample_population=="hospital + ICU" | sample_population=="hospitalized and icu" | sample_population=="hospitalized, icu, and outpatient"
          | sample_population=="Hospitalized" | sample_population=="hosp" | sample_population=="hosp 48, ICU 10" | sample_population=="hospital (inpatients and outpatients)"
          | sample_population=="hospitalized (non-icu)", 
          hospital := 1]
  dataset[sample_population=="general population" | sample_population=="outpatient" | sample_population=="community" | sample_population=="community + hospital + ICU" | hospital==1
              | sample_population=="Non-hospitalized" | sample_population=="Community" | sample_population=="community + hospital + icu" | sample_population=="comm" | 
            sample_population=="largely comm (10.4% vs 9.3% hospitalised for symptoms in those with pos and neg PCR tests, respectively; see T3)", icu := 0]
  dataset[sample_population=="ICU patients" | sample_population=="ICU" | sample_population=="icu", icu := 1]
  dataset[icu==1, hospital := 0]
  table(dataset$sample_population, dataset$icu)
  table(dataset$sample_population, dataset$hospital)
  dataset$hospital_or_icu <- 0
  dataset[hospital==1 | icu==1, hospital_or_icu := 1]
  dataset$hospital_and_icu <- 0
  dataset$hospital_and_icu[dataset$hospital==1 & grepl('icu', dataset$sample_population, ignore.case = TRUE)] <- 1
  table(dataset$sample_population[dataset$hospital==-1 & dataset$icu==-1])
  table(dataset$sample_population[dataset$icu==0])
  table(dataset$sample_population, dataset$hospital_or_icu)
  table(dataset$sample_population, dataset$hospital_and_icu)
  
  dataset$hospital <- as.numeric(dataset$hospital)
  dataset$icu <- as.numeric(dataset$icu)
  dataset$hospital_or_icu <- as.numeric(dataset$hospital_or_icu)
  dataset$hospital_and_icu <- as.numeric(dataset$hospital_and_icu)
  
  dataset <- dataset[study_id=="Garrigues et al" & sample_population=="hospital + icu", exclude := 1]
  
  
  
  ########################################################
  # FOLLOW-UP TIME
  table(dataset$follow_up_value, useNA = 'always')
  table(dataset$follow_up_index, useNA = 'always')
  table(dataset$follow_up_units, useNA = 'always')
  dataset$follow_up_value <- as.numeric(dataset$follow_up_value)
  #dataset$follow_up_units[is.na(dataset$follow_up_value)] <- 0
  dataset[follow_up_units == 'days', follow_up_days := follow_up_value]
  dataset[follow_up_units == 'weeks', follow_up_days := follow_up_value * 7]
  dataset[follow_up_units %in% c('month', 'months'), follow_up_days := follow_up_value * 30]
  dataset[follow_up_units == 'year', follow_up_days := follow_up_value * 365]
  
  table(dataset$follow_up_days, useNA='always')
  unique(dataset$follow_up_days)
  table(dataset$follow_up_index[dataset$hospital==0 & dataset$icu==0])
  table(dataset$follow_up_index[dataset$hospital==1 & dataset$icu==0])
  table(dataset$follow_up_index[dataset$hospital==0 & dataset$icu==1])
  
  unique(dataset$follow_up_days[dataset$follow_up_index=="COVID diagnosis"])
  table(dataset$follow_up_index)
  unique(dataset$follow_up_days[dataset$hospital==0 & dataset$icu==0])
  # community cases benchmarked from COVID diagnosis = 7 days after infection, 14 - 7 = 7 left of acute phase
  dataset[hospital == 0 & icu == 0 & follow_up_index == 'COVID diagnosis', follow_up_days := follow_up_days - 7]
  # community cases benchmarked from infection = 14 days left of acute phase
  dataset[hospital==0 & icu==0 & !is.na(follow_up_days) & follow_up_index=="infection", follow_up_days := follow_up_days - 14]
  # community cases benchmarked from symptom onset = 5 days after infection, 14 - 5 = 9 days left of acute phase
  dataset[hospital==0 & icu==0 & !is.na(follow_up_days) & follow_up_index=="symptom onset", follow_up_days := follow_up_days - 9]
  # community cases benchmarked from 30 days after COVID diagnosis = (30-7)= 22 days after infection, 8 days extra follow-up time
  dataset[hospital==0 & icu==0 & !is.na(follow_up_days) & follow_up_index=="30 days after COVID diagnosis", follow_up_days := follow_up_days + 8]
  # community cases benchmarked from end of acute phase = follow_up_days, no change!
  
  # hospital cases benchmarked from hospital discharge = 26 days after infection, 35 - 26 = 9 days left of acute phase
  dataset[hospital == 1 & icu == 0 & (follow_up_index == 'hospital discharge' | follow_up_index == 'Hospital or ICU discharge'), follow_up_days := follow_up_days - 9]
  # hospital cases benchmarked from COVID diagnosis = 8 days after infection, 35 - 8 = 27 days left of acute phase
  dataset[hospital==1 & !is.na(follow_up_days) & follow_up_index=="COVID diagnosis", follow_up_days := follow_up_days - 27]
  # hospital cases benchmarked from 30 days after COVID diagnosis = 27 days after infection, 30+8 > 35 by 3 days after acute phase
  dataset[hospital==1 & !is.na(follow_up_days) & follow_up_index=="30 days after COVID diagnosis", follow_up_days := follow_up_days + 3]
  # hospital cases benchmarked from hospital admission = 12 days after infection, 35 - 12 = 23 days left of acute phase
  dataset[hospital==1 & !is.na(follow_up_days) & follow_up_index=="hospital admission", follow_up_days := follow_up_days - 23]
  # hospital cases benchmarked from symptom onset = 5 days after infection, 35 - 5 = 30 days left of acute phase
  dataset[hospital==1 & !is.na(follow_up_days) & (follow_up_index=="symptom onset" | follow_up_index=="disease onset"), follow_up_days := follow_up_days - 30]
  # hospital cases benchmarked from end of acute phase = follow_up_days, no change!
  
  # ICU cases benchmarked from COVID diagnosis = 8 days after infection (let's say COVID diagnosis is 3 days after symptom onset), 42 - 8 = 34 days left of acute phase
  dataset[icu==1 & !is.na(follow_up_days) & follow_up_index=="COVID diagnosis", follow_up_days := follow_up_days - 34]
  # ICU cases benchmarked from ICU discharge = 28 days after infection, 42 - 28 = 14 days left of acute phase
  dataset[icu==1 & !is.na(follow_up_days) & follow_up_index=="ICU discharge", follow_up_days := follow_up_days - 14]
  # ICU cases benchmarked from 30 days after COVID diagnosis = 8 days after infection, 42 - 38 = 4 days left of acute phase
  dataset[icu==1 & !is.na(follow_up_days) & follow_up_index=="30 days after COVID diagnosis", follow_up_days := follow_up_days - 4]
  # ICU cases benchmarked from hospital admission = 12 days after infection, 42 - 12 = 30 days left of acute phase
  dataset[icu==1 & !is.na(follow_up_days) & follow_up_index=="hospital admission", follow_up_days := follow_up_days - 30]
  # ICU cases benchmarked from hospital discharge = 32 days after infection, 42 - 32 = 10 days left of acute phase
  dataset[icu==1 & !is.na(follow_up_days) & follow_up_index=="hospital discharge", follow_up_days := follow_up_days - 10]
  # ICU cases benchmarked from ICU admission = 14 days after infection, 42 - 14 = 28 days left of acute phase
  dataset[icu==1 & !is.na(follow_up_days) & follow_up_index=="ICU admission", follow_up_days := follow_up_days - 28]
  # ICU cases benchmarked from symptom onset = 5 days after infection, 42 - 5 = 37 days left of acute phase
  dataset[icu==1 & !is.na(follow_up_days) & (follow_up_index=="symptom onset"), follow_up_days := follow_up_days - 37]
  
  unique(dataset$follow_up_days)
  table(dataset$follow_up_index)
  table(dataset$study_id, dataset$follow_up_value)
  table(dataset$study_id, dataset$follow_up_days)
  unique(dataset$follow_up_days)
  
  ########################################################
  # SEX
  
  dataset$female <- 0
  dataset$male <- 0
  table(dataset$sex)
  dataset[sex=='58% male', sex := "Both"]
  dataset$female[dataset$sex=="Female" | dataset$sex=="female" | dataset$sex=='2' | dataset$sex=="F"] <- 1
  dataset$male[dataset$sex=="Male" | dataset$sex=="male" | dataset$sex=='1' | dataset$sex=="M"] <- 1
  
  

  ########################################################
  # CHILDREN
  
  dataset$children <- 0
  dataset[age_end<20, children := 1]
  table(dataset$study_id, dataset$children)
  
  ########################################################
  # AGE
  
  dataset$age_start <- as.numeric(dataset$age_start)
  dataset$age_end <- as.numeric(dataset$age_end)
  dataset$age_range <- dataset$age_end - dataset$age_start
  dataset$age_specific <- 0
  dataset$age_specific[dataset$age_range<40] <- 1
  
  dataset <- dataset[age_range >= 40 | (study_id == "Italy ISARIC" & age_start == 0 & age_end == 19) | study_id=="Sechenov StopCOVID peds" |
                       study_id=="García‑Abellán J et al" | study_id=="CLoCk" | study_id=="Asadi-Pooya A et al", age_specific := 0]
  dataset <- dataset[age_range >= 12 & children == 1, age_specific := 0]
  dataset$age_range <- NULL
  table(dataset$study_id, dataset$age_specific)
  
  
  ########################################################
  # SET MIN CV
  
  dataset[(standard_error / mean) < 0.1, standard_error := (0.1 * mean)]
  
  
  ########################################################
  # CLEAN UP
  
  
  dataset[mean<0, is_outlier := 1]
  dataset[mean==0 & outcome %in% c("cog_rsp", "fat_cog", "fat_rsp", "fat_cog_rsp", "mild_cog", "mod_cog", "mild_rsp", "mod_rsp", "sev_rsp"), is_outlier := 1]
  dataset[follow_up_days<0, is_outlier := 1]
  #dataset[study_id=="RUS ISARIC peds" & outcome_name=="persistent cough", is_outlier := 1]
  #dataset[study_id=="RUS ISARIC peds" & follow_up_value>1, is_outlier := 1]
  dataset <- dataset[(study_id=="PRA"), is_outlier := 0]
  table(dataset$study_id, dataset$female)
  table(dataset$follow_up_index)
  table(dataset$follow_up_days)

  table(dataset$study_id, dataset$is_outlier)
  dataset <- dataset[!is.na(mean)]
  
  table(dataset$study_id, dataset$outcome)
  table(dataset$study_id[dataset$is_outlier==0], dataset$outcome[dataset$is_outlier==0])
  
  table(dataset$exclude)
  dataset[is.na(exclude), exclude := 0]
  table(dataset$exclude)
  dataset$zero <- 0
  dataset[mean==0, zero := 1]
  table(dataset$exclude, dataset$zero)
  dataset[study_id=="Iran" & outcome %in% c('mild_rsp', 'mod_rsp', 'sev_rsp'), exclude := 0]
  dataset[study_id=="Italy ISARIC" & outcome %in% c("cog_rsp", "fat_cog", "fat_rsp", "fat_cog_rsp") & age_start==20, exclude := 1]
  dataset <- dataset[exclude!=1]
  dataset[mean>=1 | mean<0, is_outlier := 1]
  
  length(unique(dataset$study_id[dataset$is_outlier!=1]))
  
  write.csv(dataset, paste0(outputfolder, "prepped_data_", datadate, ".csv"))
  
  
  dim(dataset)
  
