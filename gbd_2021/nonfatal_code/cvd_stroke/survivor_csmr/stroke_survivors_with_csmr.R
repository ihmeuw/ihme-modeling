#===================================================================================================
# Title: Stroke survivors with CSMR
# Author: USERNAME
# Date: 12-20-2020
# Description: This code calculates the surviving incidenct cases from the stage 1 and stage 2
#              acute stroke models by taking FES incidence and applying transformed EMR -> CFR
#              using relationship EMR = -ln(1-cfr)/(1/12). Also calculated subtype proportions of 
#              stroke survivors in order to split all stroke prevalence data. 
#===================================================================================================


## Functions

source('FILEPATH/get_ids.R')
source('FILEPATH/get_outputs.R')
source('FILEPATH/get_model_results.R')
source('FILEPATH/get_draws.R')
source('FILEPATH/get_location_metadata.R')
source('FILEPATH/get_age_metadata.R')
'%ni%' <- Negate('%in%')

## Paths
if (Sys.info()[1] == "Linux") jpath <- "FILEPATH"

## Libraries
library(openxlsx)
library(ggplot2)
library(dplyr)
library(raster)
library(plyr)

############ step 0: Initialize Parameters for processing ############
#Arguments for parallel process 
args <- commandArgs(trailingOnly = T)
parameters_filepath <- args[1]  #locations. 
stage <- args[4]
date<-gsub("-", "_", Sys.Date())

print(stage)
print(parameters_filepath)


## Retrieving array task_id
task_id <- ifelse(is.na(as.integer(Sys.getenv("SGE_TASK_ID"))), 1, as.integer(Sys.getenv("SGE_TASK_ID")))
print(task_id)

parameters <- fread(parameters_filepath) #for reading table data. 
location <- parameters[task_id, locs] 

print(paste0("Calculating Survivors for location ", location))
print(stage)

########### step 1: Get incidence/excess mortality rate draws from stage1/stage 2 FES models #############
############ isch , ich , sah

## Determine which MEs to pull. 
if(stage=='stage1'){
  me_list <- c(3952,3953,18730)
}
if(stage=='stage2'){
  me_list <- c(24714,24706,24710)

}

## get draws of incidence & EMR
draws <- get_draws("modelable_entity_id",me_list,sex_id=c(1,2,3), "epi", location_id=location, measure_id=c(6,9), decomp_step="iterative", gbd_round_id=7,
                   age_group_id = c(2,3,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,27,30,31,32,235,34,238,388,389))

draws <- data.frame(draws)

## format incidence & EMR data
incidence <- draws[draws$measure_id==6,]#data.frame(draws[measure_id==6])
incidence$measure_id <- NULL
names(incidence) <- gsub("draw_", "inc_", names(incidence))
emr <- draws[draws$measure_id==9,]
incidence$measure_id <- NULL
names(emr) <- gsub("draw_", "emr_", names(emr))

## Merge emr and incidence 
df <- merge(incidence, emr, by=c("location_id", "sex_id", "year_id", "age_group_id", "metric_id", "model_version_id", "modelable_entity_id"))

########### step 2: Convert excess mortality rate into case fatality rate #############
## transform back to cfr. 
df[,c(grep("emr_", names(df)))] <- df[,c(grep("emr_", names(df)))]/ (12 + df[,c(grep("emr_", names(df)))])


########### step 3: Multiply FES incidence by (1 - case fatality rate) to yield survivors #############
## Calculate survivor incidence
df[,paste0('new_inc_',seq(0,999,1))] <- df[,c(grep("inc_", names(df)))] * (1 - df[,c(grep("emr_", names(df)))]) ###### 

########### step 4: Calculated proportion of all survivors by {subtype} to use in splitting all-stroke prevalence (different script) #############
## Compute proportions: 
df_prop <- df[,c(paste0('new_inc_',seq(0,999,1)),'location_id','sex_id','year_id','age_group_id','modelable_entity_id')]
df_prop <- data.table(df_prop)

## assign names to new df
if(stage=='stage1'){
  df_prop_is <- df_prop[modelable_entity_id == 3952,]
  names(df_prop_is) <- gsub("new_inc_", "new_isch", names(df_prop_is))
  df_prop_ich<- df_prop[modelable_entity_id == 3953,]
  names(df_prop_ich) <- gsub("new_inc_", "new_ich", names(df_prop_ich))
  df_prop_sah<- df_prop[modelable_entity_id == 18730,]
  names(df_prop_sah) <- gsub("new_inc_", "new_sah", names(df_prop_sah))
}

if(stage=='stage2'){
  df_prop_is <- df_prop[modelable_entity_id == 24714,]
  names(df_prop_is) <- gsub("new_inc_", "new_isch", names(df_prop_is))
  df_prop_ich<- df_prop[modelable_entity_id == 24706,]
  names(df_prop_ich) <- gsub("new_inc_", "new_ich", names(df_prop_ich))
  df_prop_sah<- df_prop[modelable_entity_id == 24710,]
  names(df_prop_sah) <- gsub("new_inc_", "new_sah", names(df_prop_sah))
}

## Merge together survivor data for proportions
df_prop_all <- merge(df_prop_is,df_prop_ich, by=c('location_id','sex_id','year_id','age_group_id'))
df_prop_all <- merge(df_prop_all,df_prop_sah, by=c('location_id','sex_id','year_id','age_group_id'))

## Compute {subtype} proportions
df_prop_all[,paste0('prop_isch',seq(0,999,1))] <- df_prop_all[,paste0('new_isch',seq(0,999,1))]/(df_prop_all[,paste0('new_isch',seq(0,999,1))]+df_prop_all[,paste0('new_ich',seq(0,999,1))]+df_prop_all[,paste0('new_sah',seq(0,999,1))]) ###### 
df_prop_all[,paste0('prop_ich',seq(0,999,1))] <- df_prop_all[,paste0('new_ich',seq(0,999,1))]/(df_prop_all[,paste0('new_isch',seq(0,999,1))]+df_prop_all[,paste0('new_ich',seq(0,999,1))]+df_prop_all[,paste0('new_sah',seq(0,999,1))]) ###### 
df_prop_all[,paste0('prop_sah',seq(0,999,1))] <- df_prop_all[,paste0('new_sah',seq(0,999,1))]/(df_prop_all[,paste0('new_isch',seq(0,999,1))]+df_prop_all[,paste0('new_ich',seq(0,999,1))]+df_prop_all[,paste0('new_sah',seq(0,999,1))]) ###### 

## re-seperate into subtype specific propportions
df_prop_isch <- df_prop_all[,c(paste0('prop_isch',seq(0,999,1)),
                               'location_id','sex_id','year_id','age_group_id')]
df_prop_ich <- df_prop_all[,c(paste0('prop_ich',seq(0,999,1)),
                              'location_id','sex_id','year_id','age_group_id')]
df_prop_sah <- df_prop_all[,c(paste0('prop_sah',seq(0,999,1)),
                              'location_id','sex_id','year_id','age_group_id')]

## take means of proportion estimates
df_prop_isch$mean <- rowMeans(df_prop_isch[,paste0('prop_isch',seq(0,999,1))])
df_prop_ich$mean <- rowMeans(df_prop_ich[,paste0('prop_ich',seq(0,999,1))])
df_prop_sah$mean <- rowMeans(df_prop_sah[,paste0('prop_sah',seq(0,999,1))])

## compute upper/lower bounds of proportions, necessary to compute SE of post-split prevalence
df_isch_bounds<- apply(df_prop_isch[,paste0('prop_isch',seq(0,999,1))], 1, function(x) quantile(x, probs=c(.025,.975),na.rm=T))
df_isch_bounds<-data.frame(t(df_isch_bounds))
df_prop_isch$lower <- df_isch_bounds$X2.5.
df_prop_isch$upper <- df_isch_bounds$X97.5.

df_prop_isch<- df_prop_isch[,c('location_id','sex_id',"year_id","age_group_id","mean","lower","upper")] 
setnames(df_prop_isch, 'mean','mean_isch')
df_prop_isch[,standard_error_isch := (upper-mean_isch)/1.96]

df_ich_bounds<- apply(df_prop_ich[,paste0('prop_ich',seq(0,999,1))], 1, function(x) quantile(x, probs=c(.025,.975),na.rm=T))
df_ich_bounds<-data.frame(t(df_ich_bounds))
df_prop_ich$lower <- df_ich_bounds$X2.5.
df_prop_ich$upper <- df_ich_bounds$X97.5.

df_prop_ich<- df_prop_ich[,c('location_id','sex_id',"year_id","age_group_id","mean","lower","upper")] 
setnames(df_prop_ich, 'mean','mean_ich')
df_prop_ich[,standard_error_ich := (upper-mean_ich)/1.96]

df_sah_bounds<- apply(df_prop_sah[,paste0('prop_sah',seq(0,999,1))], 1, function(x) quantile(x, probs=c(.025,.975),na.rm=T))
df_sah_bounds<-data.frame(t(df_sah_bounds))
df_prop_sah$lower <- df_sah_bounds$X2.5.
df_prop_sah$upper <- df_sah_bounds$X97.5.

df_prop_sah<- df_prop_sah[,c('location_id','sex_id',"year_id","age_group_id","mean","lower","upper")] 
setnames(df_prop_sah, 'mean', 'mean_sah')
df_prop_sah[,standard_error_sah := (upper-mean_sah)/1.96]

#save prop files: 
direct_path <- paste0("FILEPATH/",date,'_',stage)
dir.create(direct_path,showWarnings = FALSE)
saveRDS(df_prop_isch[,c('location_id','sex_id','year_id','age_group_id','mean_isch','standard_error_isch')], file= paste0(direct_path,"/is_prop_",location,".rds"))
saveRDS(df_prop_ich[,c('location_id','sex_id','year_id','age_group_id','mean_ich','standard_error_ich')], file=paste0(direct_path,"/ich_prop_",location,".rds"))
saveRDS(df_prop_sah[,c('location_id','sex_id','year_id','age_group_id','mean_sah','standard_error_sah')], file=paste0(direct_path,"/sah_prop_",location,".rds"))

##PROPORTION WORK OVER - RESUME NORMAL SURVIVORS. 
df <- df[which(df$sex_id != 3),]
df <- data.frame(df)

## compute means and bounds
df$mean <- rowMeans(df[,c(grep("new_inc_", names(df)))])
df_bounds<- apply(df[,grep('new_inc_',names(df))], 1, function(x) quantile(x, probs=c(.025,.975),na.rm=T))
df_bounds<-data.frame(t(df_bounds))
df$lower <- df_bounds$X2.5.
df$upper <- df_bounds$X97.5.

########### step 5: Save survivors and subtype proportions to central folders referenced in upload scripts #############

## save relevant columns.
df<- df[,c('location_id','sex_id',"year_id","age_group_id","mean" ,"lower" ,"upper" ,"modelable_entity_id")] 

## subset into MEs for upload
if(stage=='stage1'){
  final_survivor_is  <- subset(df, modelable_entity_id== 3952) 
  final_survivor_ich <- subset(df, modelable_entity_id== 3953) 
  final_survivor_sah <- subset(df, modelable_entity_id== 18730) 
}

if(stage=='stage2'){
  final_survivor_is  <- subset(df, modelable_entity_id== 24714) 
  final_survivor_ich <- subset(df, modelable_entity_id== 24706) 
  final_survivor_sah <- subset(df, modelable_entity_id== 24710) 
}

## set up folder for storage
folder <- paste0("FILEPATH/",date,"_",stage,"/")
dir.create(folder, showWarnings = FALSE)
saveRDS(final_survivor_is[,c('location_id','sex_id','year_id','age_group_id','mean','lower','upper')], file=paste0(folder, "is_chronic_surv_", location, ".rds"))
saveRDS(final_survivor_ich[,c('location_id','sex_id','year_id','age_group_id','mean','lower','upper')], file=paste0(folder, "ich_chronic_surv_", location, ".rds"))
saveRDS(final_survivor_sah[,c('location_id','sex_id','year_id','age_group_id','mean','lower','upper')], file=paste0(folder, "sah_chronic_surv_", location, ".rds"))

print('Survivors complete")
