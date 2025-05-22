################################################################################################################
## Title: IHD SEV to MI incidence approach
## Purpose: Preparing diagnosed MI: prehospital MI scalar 
################################################################################################################
## Source central functions: 

central <- "FILEPATH"
for (func in paste0(central, list.files(central))) source(paste0(func))

## 
'%ni%' <- Negate('%in%')

## Source libraries
library(openxlsx)
library(data.table)
library(ggplot2)
library(plyr)
library(dplyr)
library(stringr)
library(tidyr)


#########################################################
## Function to make scalar data
## mi_model_id: current best diagnosed MI incidence model
## mi_xwalk_filename: MI incidence crosswalk object with prehospital included
#########################################################

make_mi_adj_data <- function(mi_model_id, path_to_mi_xwalk, make_plots=F){
  
  ## Get metadata for age and location
  message('Getting metadata...')
  age_meta <- get_age_metadata(age_group_set_id=19)
  age_meta[,age_mid := (age_group_years_start+age_group_years_end)/2]
  
  loc_meta <- get_location_metadata(location_set_id = 35, release_id = 16)
  bzl <- loc_meta[parent_id==135,]
  
  date<-gsub("-", "_", Sys.Date())
  
  message('PULLING SEV...')
  ihd_sev_df <- get_covariate_estimates(covariate_id = 580, age_group_id = c(8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32,235),
                                        sex_id = c(1,2),location_id = unique(loc_meta$location_id),
                                        year_id = c(seq(1990,2022,5),2022), release_id = 16)
  setnames(ihd_sev_df, c('mean_value'),c('ihd_sev'))
  
  message('PULLING DIAGNOSED AMI RESULTS...')
  
  #mi_model_id <- mi_model
  mi_dismod <- get_model_results(gbd_team = 'epi',gbd_id = 28683, model_version_id = mi_model_id, 
                                 age_group_id = c(8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32,235), sex_id = c(1,2),location_id = unique(loc_meta$location_id),
                                 year_id = c(seq(1990,2022,5),2022), release_id = 16, measure_id = 6)
  setnames(mi_dismod, c('mean'),c('mi_inc'))
  
  message('PULLING HAQI...')
  haqi <- get_covariate_estimates(covariate_id = 1099,location_id = unique(loc_meta$location_id),
                                  year_id = seq(1990,2023,1), release_id = 16)
  haqi[,mean_value := mean_value/100] ## make 0-1 scale
  setnames(haqi, c('mean_value'),c('haqi_mean'))
  
  message('PULLING RAW DATA...')
  
  load(path_to_mi_xwalk) #- Testing sex split only
  
  
  use_xw <- T
  if(use_xw){
    mi_xwalk <- crosswalk_holder$data}
  
  else{
    mi_xwalk <- crosswalk_holder$sex_specific_data
  }
  
  message('FORMATING RAW DATA...')
  
  ## post xwalk from sex-split & troponin split
  gs_inc_xwalk <- mi_xwalk[cv_nonfatal==0,] ## post sex-split & xwalk from fes/trpn
  gs_inc_xwalk[,`:=` (group_review = NA, group=NA, specificity = NA)]

  ##clean-up the data
  gs_inc_xwalk <- gs_inc_xwalk[measure == 'incidence',]
  gs_inc_xwalk[,is_outlier:=0]
  gs_loc_ids <- unique(gs_inc_xwalk$location_id)
  gs_inc_xwalk[,crosswalk_parent_seq := 0]
  gs_inc_xwalk <- gs_inc_xwalk[-which(location_id==93 & cv_ihdvr==1)]


  message('AGE-SPLITTING GOLD-STANDARD...')
  ## age-split gold-standard data - 

  source('FILEPATH/age_split.R')

  if('crosswalk_parent_seq' %ni% names(gs_inc_xwalk)){
    gs_inc_xwalk[,crosswalk_parent_seq := seq]
  }
  
  as_inc <- age_split(gs_inc_xwalk[sex!='Both'],
                      split_size = 5,
                      model_id = 24694, 
                      model_version_id = 798675 ,  
                      release_id = 16,
                      measure = 'incidence',
                      global_age_pattern = T,
                      drop_age_groups = c(1,6,7),
                      drop_under_1_case = F)
  
  
  as_inc$age_group_id <- NULL
  setnames(as_inc,c('age_start','age_end'), c('age_group_years_start','age_group_years_end'))
  as_inc <- data.table(merge(as_inc, age_meta[,c('age_group_years_start','age_group_id')], by='age_group_years_start'))
  as_inc[,`:=` (sex_id = ifelse(sex=='Male',1,2), midyear = (year_start+year_end)/2)]
  as_inc[,`:=` (year_id = ceiling(midyear/5)*5)]

  
  #########################################################
  ## Step 2
  #########################################################
  
  message('MERGING ON DAMI/IHD SEV/HAQI to input data')
  # match mi incidence to IHD SEV
  m_cols <- c('age_group_id','sex_id','year_id','location_id')
  ihd_sev_mi_dismod <- data.table(merge(ihd_sev_df[,c(m_cols,'ihd_sev'),with=F],
                                        mi_dismod[,c(m_cols,'mi_inc'),with=F],
                                        by=m_cols))
  ihd_sev_mi_dismod <- data.table(merge(ihd_sev_mi_dismod,loc_meta[,c('location_id','super_region_name','region_name','ihme_loc_id'), with=F], by='location_id'))
  ihd_sev_mi_dismod <- data.table(merge(ihd_sev_mi_dismod,age_meta[,c('age_group_id','age_group_name'), with=F], by='age_group_id'))
  ihd_sev_mi_dismod <- data.table(merge(ihd_sev_mi_dismod,haqi[,c('year_id','location_id','haqi_mean'), with=F], by=c('year_id','location_id')))
  
  ihd_sev_mi_dismod[,gold_standard_available := ifelse(location_id %in% gs_loc_ids, 'yes','no')]
  ihd_sev_mi_dismod[,gold_standard_available := as.factor(gold_standard_available)]
  
  bzl <- loc_meta[parent_id==135, ]
  ihd_sev_mi_dismod <- ihd_sev_mi_dismod[-which(location_id %in% bzl$location_id & year_id==1990),]
  
  ## remove troublesome data
  ihd_sev_mi_dismod_input <- data.table(merge(ihd_sev_mi_dismod, as_inc, by=c('age_group_id','sex_id','location_id','year_id')))

  haqi_q <- quantile(haqi$haqi_mean, c(0,.20,.40,.60,.80,1.0)) ## define quantiles of HAQi, 1980-2022 
  
  ## haqi scaled 0-1
  ihd_sev_mi_dismod_input[haqi_mean>= 0 & haqi_mean < haqi_q[2] , haqi_group := paste0('(0',' <= haqi < ',round(100*haqi_q[2],1),') low haqi')]
  ihd_sev_mi_dismod_input[haqi_mean>= haqi_q[2] & haqi_mean < haqi_q[3], haqi_group := paste0('(',round(100*haqi_q[2],1),' <= haqi < ',round(100*haqi_q[3],1),') low-mid haqi')]
  ihd_sev_mi_dismod_input[haqi_mean>= haqi_q[3] & haqi_mean < haqi_q[4], haqi_group := paste0('(',round(100*haqi_q[3],1),' <= haqi < ',round(100*haqi_q[4],1),') mid haqi')]
  ihd_sev_mi_dismod_input[haqi_mean>= haqi_q[4] & haqi_mean < haqi_q[5], haqi_group := paste0('(',round(100*haqi_q[4],1),' <= haqi < ',round(100*haqi_q[5],1),') high-mid haqi')]
  ihd_sev_mi_dismod_input[haqi_mean>= haqi_q[5], haqi_group := paste0('(',round(100*haqi_q[5],1),' <= haqi',') high haqi')]
  
  ihd_sev_mi_dismod_input[,age_mid := (age_group_years_start+age_group_years_end)/2]
  
  #########################################################
  ## Calculate proportion of gold-standard MI incidence not accounted for by diagnosed MI
  #########################################################
  
  message('CALCULATING PROPORTION OF GOLD-STANDARD MI NOT EXPLAINED BY DIAGNOSED MI')
  ihd_sev_mi_dismod_input[is.na(age_mid), age_mid := (age_group_years_start+age_group_years_end)/2]
  ihd_sev_mi_dismod_input[,remain_ihd := mean - mi_inc]
  ihd_sev_mi_dismod_input[,remain_ihd_pct := remain_ihd/mean]
  ihd_sev_mi_dismod_input[,diag_ihd_pct := mi_inc/mean]
  ihd_sev_mi_dismod_input[, age_cutoff := ifelse(age_mid <= 65, 'Less than 65','Over 65')]
  ihd_sev_mi_dismod_input[, sex_name := ifelse(sex==0, 'Male','Female')]
  
  haqi_groups <- sort(unique(ihd_sev_mi_dismod_input$haqi_group))
  ihd_sev_mi_dismod_input$haqi_group <- factor(ihd_sev_mi_dismod_input$haqi_group, levels = haqi_groups)
  
  ihd_sev_remain <- ihd_sev_mi_dismod_input[,c('age_group_name','sex','location_name','midyear','remain_ihd')]
  ihd_sev_remain[,source := 'Remaining IHD']
  setnames(ihd_sev_remain, c('remain_ihd'),c('mi'))
  
  ihd_sev_remain_dami <- ihd_sev_mi_dismod_input[,c('age_group_name','sex','location_name','midyear','mi_inc')]
  ihd_sev_remain_dami[,source := 'Diagnosed MI']
  setnames(ihd_sev_remain_dami, c('mi_inc'),c('mi'))
  
  ihd_sev_remain_all <- ihd_sev_mi_dismod_input[,c('age_group_name','sex','location_name','midyear','mean')]
  ihd_sev_remain_all[,source := 'Gold-Standard']
  setnames(ihd_sev_remain_all, c('mean'),c('mi'))
  
  ihd_sev_components <- unique(data.table(rbind(ihd_sev_remain, ihd_sev_remain_dami, ihd_sev_remain_all)))
  

  
  
  message('WRITING RESULTS...')
  openxlsx::write.xlsx(ihd_sev_mi_dismod_input,file=paste0('FILEPATH/ihd_sev_mi_dismod_input_alt_',date,'.xlsx'))
  path_str <- paste0('FILEPATH/ihd_sev_mi_dismod_input_alt_',date,'.xlsx')
  return(path_str)
}


