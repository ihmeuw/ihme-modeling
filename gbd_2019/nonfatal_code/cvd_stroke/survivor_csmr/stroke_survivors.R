######################
# Title: Chronic stroke
# Author: USERNAME
# Date: 10-1-2019
# Description: This code is intended populate then step 4 bundle for chronic isch,ich, and sah stroke with EMR created from recent draws.
# This function needs to be wrapped for years (this is the child script)
######################
library(raster)
library(plyr)
library(dplyr)
date<-gsub("-", "_", Sys.Date())
source('FILEPATH/get_ids.R')
source('FILEPATH/get_outputs.R')
source('FILEPATH/get_model_results.R')
source('FILEPATH/get_draws.R')
source('FILEPATH/get_bundle_data.R')
source('FILEPATH/upload_bundle_data.R')
source('FILEPATH/get_bundle_version.R')
source('FILEPATH/save_bundle_version.R')
source('FILEPATH/get_crosswalk_version.R')
source('FILEPATH/save_crosswalk_version.R')
source('FILEPATH/get_location_metadata.R')
source('FILEPATH/get_age_metadata.R')
source('FILEPATH/save_bulk_outlier.R')

if (Sys.info()[1] == "Linux") jpath <- "FILEPATH"
if (Sys.info()[1] == "Windows") jpath <- "FILEPATH"
library(openxlsx)
library(ggplot2)
library(dplyr)
'%ni%' <- Negate('%in%')
jpath <- "FILEPATH"
############ step 0: Initialize Parameters for processing ############
#Arguments for parallel process
args <- commandArgs(trailingOnly = T)
parameters_filepath <- args[1]  #locations.
print(parameters_filepath)

## Retrieving array task_id
task_id <- ifelse(is.na(as.integer(Sys.getenv("SGE_TASK_ID"))), 1, as.integer(Sys.getenv("SGE_TASK_ID")))
print(task_id)


parameters <- fread(parameters_filepath) #for reading table data.

location <- parameters[task_id, locs]

print(paste0("Calculating Survivors for location ", location))

########### step 1: Calculate survivors for stroke subtypes #############
############ isch , ich , saj
me_list <- c(24714,24706,24710)


draws <- get_draws("modelable_entity_id",me_list, "epi", location_id=location, measure_id=c(6,9), decomp_step="step4")
print('debug flag 1')
draws <- data.frame(draws)


incidence <- draws[draws$measure_id==6,]
print('debug flag 2')
incidence$measure_id <- NULL
print('debug flag 3')
names(incidence) <- gsub("draw_", "inc_", names(incidence))
print('debug flag 4')
emr <- draws[draws$measure_id==9,]
print('debug flag 5')
incidence$measure_id <- NULL
print('debug flag 6')
names(emr) <- gsub("draw_", "emr_", names(emr))
print('debug flag 7')
print(paste0('incidence: ' ,names(incidence)))
print(paste0('emr: ', names(emr)))

df <- merge(incidence, emr, by=c("location_id", "sex_id", "year_id", "age_group_id", "metric_id", "model_version_id", "modelable_entity_id"))
print('debug flag 8')
df[,c(grep("emr_", names(df)))] <- df[,c(grep("emr_", names(df)))]/ (12 + df[,c(grep("emr_", names(df)))])
print('debug flag 9')
df[,c(grep("emr_", names(df)))] <- df[,c(grep("inc_", names(df)))] * (1 - df[,c(grep("emr_", names(df)))]) ######
print('debug flag 10')
df$mean <- rowMeans(df[,c(grep("emr_", names(df)))])
print('debug flag 11')


df_bounds<- apply(df[,grep('emr_',names(df))], 1, function(x) quantile(x, probs=c(.025,.975),na.rm=T))
print('debug flag 12')
df_bounds<-data.frame(t(df_bounds))
df$lower <- df_bounds$X2.5.
df$upper <- df_bounds$X97.5.

df<- df[,c('location_id','sex_id',"year_id","age_group_id","mean" ,"lower" ,"upper" ,"modelable_entity_id")]

print('debug flag 13')
#subset into MEs for upload

final_survivor_is  <- subset(df, modelable_entity_id== 24714)
final_survivor_ich <- subset(df, modelable_entity_id== 24706)
final_survivor_sah <- subset(df, modelable_entity_id== 24710)

folder <- "FILEPATH" #STORAGE FOLDER

outdir <- paste0(folder, 'FILEPATH', "/")

print('debug flag 5')
saveRDS(final_survivor_is[,c('location_id','sex_id','year_id','age_group_id','mean','lower','upper')], file=paste0(outdir, "is_chronic_surv_", location, ".rds"))
saveRDS(final_survivor_ich[,c('location_id','sex_id','year_id','age_group_id','mean','lower','upper')], file=paste0(outdir, "ich_chronic_surv_", location, ".rds"))
saveRDS(final_survivor_sah[,c('location_id','sex_id','year_id','age_group_id','mean','lower','upper')], file=paste0(outdir, "sah_chronic_surv_", location, ".rds"))

# save RDS for each location
# mean, lower, upper, sex, age, location, year


