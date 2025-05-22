################################################################################################################
## Title: HAMI:IHD proportion 
## Purpose: Take the modeled proportion of IHD deaths that died within 30-days of AMI hospitalization, apply to IHD CSMR for Dismod - run parallel by location.  
##          1.) Load in the saved proportions modeled in MR-BRT & Load in IHD CoDCorrect CSMR (produced in mrbrt_30_day)
##          2.) Load CodCorrected IHD CSMR
##          3.) Split into acute diagnosed MI CSMR.
################################################################################################################

## Source central functions: 
central <- "FILEPATH/"
for (func in paste0(central, list.files(central))) source(paste0(func))

## NEGATE
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
loc_meta <- get_location_metadata(location_set_id = 35, release_id = 16)


args <- commandArgs(trailingOnly = T)
parameters_filepath <- args[1]  #locations. 
print(parameters_filepath)

## Retrieving array task_id
task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
print(task_id)

parameters <- fread(parameters_filepath) #for reading table data. 

loc <- parameters[task_id, locs] 
print(paste0("Creating MI draws for location ", loc))

date <- gsub("-", "_", Sys.Date())

model_id <- 161 ## best model_id for CSMR proportion model (mrbrt_30_day.R settings file)
modeled_prop_dir <- paste0('FILEPATH/') ## proportion modeled in mrbrt_30_day.R

plot_dir         <- paste0('FILEPATH/')
output_dir       <- paste0('FILEPATH/')


#########################################################
## Step 1
#########################################################

years <- c(seq(1990,2020,5),2021,2022,2023) 
ages <- c(8:20, 30, 31, 32, 235)
sex_ids <- c(1,2)

## get all IHD CSMR draws
ihd_csmr <- data.table(get_draws(gbd_id_type='cause_id', source = 'codcorrect', gbd_id = 493, age_group_id=ages, sex_id=sex_ids,
                             year_id=years,location_id=c(loc), release_id = 16, measure_id = 1))

## get population
pop <- get_population(age_group_id= ages, year_id = years, sex_id = sex_ids, location_id=loc, release_id = 16)

## convert to rate space
ihd_csmr <- merge(ihd_csmr, pop, by=c('age_group_id','location_id','sex_id','year_id'))
ihd_csmr[,c(grep("draw_", names(ihd_csmr)))] <- lapply(ihd_csmr[,c(grep("draw_", names(ihd_csmr))),with=F], function(x, y) x/y, y=ihd_csmr$population)

ihd_csmr_long <- melt(ihd_csmr, measure.vars = names(ihd_csmr)[grepl("draw", names(ihd_csmr))], variable.name = "draw", value.name = "ihd_draw")

## Load proportion of HAMI:IHD
mrbrt_prop <- data.table(readRDS(paste0(modeled_prop_dir,'proportion_split_draws_model_',model_id,'.RDS')))
mrbrt_prop[,age_group_id := as.integer(age_group_id)]
mrbrt_prop <- mrbrt_prop[location_id==loc,]
mrbrt_prop_long <- melt(mrbrt_prop, measure.vars = names(mrbrt_prop)[grepl("draw", names(mrbrt_prop))], variable.name = "draw", value.name = "prop_draw")

hami_csmr_new <- data.table(merge(ihd_csmr_long,
                                  mrbrt_prop_long,
                                  by=c('location_id','age_group_id','sex_id','year_id','draw')))

hami_csmr_new[,hami_csmr := ihd_draw*prop_draw]

hami_csmr_new_wide <- dcast(hami_csmr_new[,c('location_id','age_group_id','sex_id','year_id','draw','hami_csmr')], location_id + age_group_id + sex_id + year_id ~ draw, 
                            value.var = "hami_csmr", fun.aggregate = sum)
hami_csmr_new_wide <- data.table(hami_csmr_new_wide)

hami_csmr_new_wide$lower <- apply(hami_csmr_new_wide[,names(hami_csmr_new_wide)[grep('draw',names(hami_csmr_new_wide))],with=F], 1, function(x) quantile(x, 0.025))
hami_csmr_new_wide$upper <- apply(hami_csmr_new_wide[,names(hami_csmr_new_wide)[grep('draw',names(hami_csmr_new_wide))],with=F], 1, function(x) quantile(x, 0.975))
hami_csmr_new_wide$mean <- apply(hami_csmr_new_wide[,names(hami_csmr_new_wide)[grep('draw',names(hami_csmr_new_wide))],with=F], 1, function(x) mean(x))

#########################################################
## write draws
#########################################################
openxlsx::write.xlsx(hami_csmr_new_wide[,c('location_id','year_id','sex_id','age_group_id','mean','lower','upper')],paste0(output_dir, loc,'.xlsx'))

