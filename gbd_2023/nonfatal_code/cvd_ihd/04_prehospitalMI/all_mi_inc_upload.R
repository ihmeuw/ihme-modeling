################################################################################################################
## Title: Save results EPI for total MI results
## Purpose: Add diagnosed MI draws to prehospital MI draws
################################################################################################################
rm(list=ls())

## Source central functions: 
central <- "FILEPATH"
for (func in paste0(central, list.files(central))) source(paste0(func))

'%ni%' <- Negate('%in%')

## Source libraries
library(openxlsx)
library(data.table)
library(ggplot2)
library(plyr)
library(dplyr)
library(stringr)
library(tidyr)

## Get metadata for age and location
message('Getting metadata...')
age_meta <- get_age_metadata(age_group_set_id=19)
loc_meta <- get_location_metadata(location_set_id = 35, release_id=16)

date<-gsub("-", "_", Sys.Date())

args <- commandArgs(trailingOnly = T)
parameters_filepath <- args[1]  #locations. 
print(parameters_filepath)

## Retrieving array task_id
task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
print(task_id)

parameters <- fread(parameters_filepath) #for reading table data. 

loc <- parameters[task_id, locs] 
print(paste0("Creating MI draws for location ", loc))

#########################################################
## Input
#########################################################

ages  <- c(8,9,10,11,12,13,14,15,16,17,18,19,20,22,30,31,32,235)
years <- c(1990, 1995, 2000, 2005, 2010,2015,2020,2021,2022, 2023, 2024)


dami_model    <- 849467 
ph_model      <- 867246 
undet_model   <-  806370 


dami_ph_only <- T

dami <- data.table(get_draws(gbd_id_type='modelable_entity_id', source = 'epi', gbd_id = 28683, version_id = dami_model, age_group_id=ages, sex_id=c(1,2),
                             year_id=years,location_id=c(loc), 
                             gbd_round_id=8, release_id = 16, measure_id = c(5,6)))
dami_long<- melt(dami, measure.vars = names(dami)[grepl("draw", names(dami))], variable.name = "draw", value.name = "dami_draw")

phmi <- data.table(get_draws(gbd_id_type='modelable_entity_id', source = 'epi', gbd_id = 28681, version_id = ph_model, age_group_id=ages, sex_id=c(1,2),
                             year_id=years,location_id=c(loc), 
                             gbd_round_id=8, release_id = 16, measure_id = c(5,6)))

phmi_long<- melt(phmi, measure.vars = names(phmi)[grepl("draw", names(phmi))], variable.name = "draw", value.name = "phmi_draw")



all_mi_long <- merge(dami_long, phmi_long, by=c('age_group_id','year_id','sex_id','location_id','draw','measure_id','metric_id'))

all_mi_long <- data.table(all_mi_long)

## calculate all MI. 

all_mi_long[,all_mi := dami_draw + phmi_draw]
all_mi_long[all_mi > 1, all_mi := 1]

#########################################################
## Save Necessary columns for upload
#########################################################
all_mi_wide <- dcast(all_mi_long[,c('location_id','age_group_id','sex_id','year_id','measure_id','draw','all_mi')], location_id + age_group_id + sex_id + year_id + measure_id~ draw, 
                     value.var = "all_mi", fun.aggregate = sum)
all_mi_wide <- data.table(all_mi_wide)


## write results 
write.csv(all_mi_wide, file = paste0('FILEPATH',loc,'.csv'), row.names = FALSE)
print('complete')


