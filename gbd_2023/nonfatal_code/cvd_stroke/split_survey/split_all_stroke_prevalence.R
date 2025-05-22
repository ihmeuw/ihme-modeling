############################### 
## All stroke prevalence split 
## Author: USERNAME
## Purpose: Get subtype-specific proportions from stage1/stage2 survivors and apply to all stroke prevalence to get subtype specific prevalence estimates. 
###############################

## Get libraries/functions
library(raster)
library(plyr)
library(openxlsx)
library(ggplot2)
library(dplyr)
library(R.utils)
date<-gsub("-", "_", Sys.Date())
central <- "FILEPATH"
for (func in paste0(central, list.files(central))) source(paste0(func))
'%ni%' <- Negate('%in%')

# testing variables
#prop_date <- '2024_08_30'
#stage <- 'stage2'
#subtype <- 'ich'

split_all_stroke_prev <- function(prop_date, stage, subtype){
  
  ## prop_date: Date that survivors was run on and proportions produced - format 'yyyy_mm_dd'
  ## stage: Stage of stroke process to split (w or w/o CSMR models) - "stage1" or "stage2"
  ## subtype: stroke subtype to split: "is", "ich", "sah" 
  
  # Get age-meta data: 
  age_map <- get_age_metadata(age_group_set_id = 19, release_id = 16)
  age_map[,':=' (age_group_name = NULL,age_group_weight_value = NULL , most_detailed = NULL)]
  new_age_maps <- data.table(c(4,5,28,27),c(0.07671233,1,0,0),c(1,4,1,99))
  names(new_age_maps) <- names(age_map)
  age_map <- data.table(rbind(age_map, new_age_maps))
  age_map <- data.table(merge(age_map,age_ids, by='age_group_id'))
  setnames(age_map, c('age_group_years_start','age_group_years_end'), c('age_start','age_end'))
  
  all_stroke <- get_bundle_data(bundle_id = 456)
  all_stroke_prev <- all_stroke[measure=='prevalence']
  
  ## Identify data that needs splitting
  
  data_to_split <- all_stroke_prev  #data that needs spliting. 
  
  ## NID removals. 
  data_to_split <- data_to_split[nid!= 125159] 
  data_to_split <- data_to_split[-which(case_name=='doc_stroke' & nid==238481)] 
  data_to_split <- data_to_split[nid %ni% c(91559, 126610, 204725,265153,174154, 13265,118580)] 
  
  ## Get most recent files that contain proportion split. 
  files <- list.files(path = paste0("FILEPATH/props/",prop_date,"_",stage), pattern = "\\.rds$", full.names = TRUE)
  
  prop_files <- files[grep(paste0(subtype,'_prop'),files)] ## get subtype specific proportions. 
  a_r  <- lapply(prop_files, readRDS)
  props  <- bind_rows(a_r, .id = "column_label")
  props <- merge(props, age_map, by=c('age_group_id'))  
  props$year_start <- props$year_id
  props$year_end  <- props$year_id
  props <- data.table(props)                               
  props[sex_id==1 , sex:='Male']
  props[sex_id==2, sex:='Female']
  props[sex_id==3, sex:='Both']
  
  ## These parameters are set up to help with merging. 
  props[,midage := (age_end + age_start)/2]
  props[,midyear:= year_id]
  props[,location_id_merge:= location_id]
  data_to_split[,original_midage := (age_end+age_start)/2] 
  data_to_split[,midyear:= round((year_end+year_start)/2)]
  data_to_split[midyear < 1990, midyear := 1990]
  
  ## If cases are not calculated, calculate them/ initialize proportion to 0. 
  data_to_split[is.na(cases) & !is.na(sample_size), cases:= sample_size*mean]
  data_to_split[,prop:=0]
  
  
  years <- c(1990,1995,2000,2005,2010,2015,2019,2020,2021,2022,2023)
  midages <- sort(unique(props$midage))
  
  ## Merge onto closest year/age. 
  message('Merging onto closest year/age...')
  
  ## Map each year to closest estimation year. 
  data_to_split[,year_id:= 
                  ifelse(midyear - years[findInterval(midyear,years)]<= 2,years[findInterval(midyear,years)],years[findInterval(midyear,years)]+5) ]
  
  ## Map each age to closest midage. 
  data_to_split[,midage:= midages[findInterval(original_midage,midages)]]
  
  ## Merge on new proportions to split
  cols <- c('midage','year_id','location_id_merge','sex','mean_ich')
  merge_cols <- c('midage','year_id','location_id_merge','sex')
  
  with_props <- data.table(merge(data_to_split,props[,cols,with=F], by=merge_cols))
  
  ## Calculate new cases. 
  if(subtype=='is'){
    with_props[,cases_new:= cases*mean_is]    ## multiply by proportion
  }
  if(subtype=='ich'){
    with_props[,cases_new:= cases*mean_ich]    ## multiply by proportion
  }
  if(subtype=='sah'){
    with_props[,cases_new:= cases*mean_sah]    ## multiply by proportion
  }
  
  ## Compute new mean based on new cases. 
  with_props[,mean_new := cases_new/sample_size]  ## compute new mean. 
  
  with_props[,`:=` (mean=mean_new,cases=cases_new)] ## set mean and cases to be recalculated values. 
  with_props[,`:=` (mean_new=NULL, cases_new=NULL, original_midage=NULL, midyear=NULL, prop=NULL,mean_ich=NULL,location_id_merge=NULL)] #remove un-needed columns.
  
  split_prev <- unique(with_props, by=c('nid','year_start','year_end','age_start','age_end','sex','mean','location_name')) #remove duplicates. 
  split_prev[,note_modeler:= paste0(note_modeler, paste0(' split using bested models on ',date))] #make a note of the split. 
  
  return(split_prev) ## return split prevalence
  
}


