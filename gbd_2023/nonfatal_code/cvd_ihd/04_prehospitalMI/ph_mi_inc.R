################################################################################################################
## Title: Save results EPI for total MI results
## Purpose:
##          1.) load in resulting modeled proportion of [diagnosed AMI]/[diagnosed + prehosp AMI] & make draws
##          2.) load in diagnosed MI draws
##          3.) Apply inverse scalar to diagnosed MI at the draw level
##          4.) save necessary columns for upload
##          5.) save_results_epi.R

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
loc_meta <- get_location_metadata(location_set_id = 35, release_id = 16)

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


model_id <- 972
dami_model <- 849467 
args <- commandArgs(trailingOnly = TRUE)
loc <- parameters[task_id, locs] 
print(paste0("Creating MI draws for location ", loc))

#########################################################
##  proportion
#########################################################

message('LOADING SCALAR ADJUSTMENTS...')
gs_scalar <- data.table(openxlsx::read.xlsx(paste0('FILEPATH/sev_approach_model_results_','model_id_',model_id,'.xlsx' )))
gs_scalar[,standard_error := (pred_upper - pred_lower)/(2*1.96)] 
gs_scalar <- merge(gs_scalar, age_meta[,c('age_group_id','age_group_name')])
gs_scalar[,sex_id := ifelse(sex_name=='Male',1,2)]
gs_scalar[,year_id := year]

gs_scalar <- gs_scalar[location_id == loc, ]

## Melt draw
gs_scalar_long <- melt(gs_scalar, measure.vars = names(gs_scalar)[grepl("draw", names(gs_scalar))], variable.name = "draw", value.name = "prop_draw")


#########################################################
## diagnosed MI
#########################################################
message('GETTING DIAGNOSED MI RESULTS')


ages  <- c(8,9,10,11,12,13,14,15,16,17,18,19,20,22,30,31,32,235)
years <- c(1990, 1995, 2000, 2005, 2010,2015,2020,2021,2022,2023,2024)

## Get model estimates
dami <- data.table(get_draws(gbd_id_type='modelable_entity_id', source = 'epi', gbd_id = 28683, version_id = dami_model, age_group_id=ages, sex_id=c(1,2),
                             year_id=years,location_id=c(proc_loc), #loc_meta$location_id
                             gbd_round_id=8, release_id = 16, measure_id = c(5,6)))

dami_long<- melt(dami, measure.vars = names(dami)[grepl("draw", names(dami))], variable.name = "draw", value.name = "dami_draw")


#########################################################
## Apply inverse scalar
#########################################################
dami_scalar_long <- merge(dami_long, gs_scalar_long, by=c('age_group_id','sex_id','year_id','location_id','draw'))
dami_scalar_long <- data.table(dami_scalar_long)
dami_scalar_long[,all_mi := dami_draw*prop_draw]
dami_scalar_long[,ph_mi := all_mi - dami_draw]

## if any draws greater than 1 - set to 1. 
dami_scalar_long[ph_mi > 1, ph_mi := 0.9999]

dami_scalar_sum <- dami_scalar_long[,.(mean_ph = mean(ph_mi)), by=c('location_id','year_id','sex_id','age_group_id','measure_id')]

#########################################################
## Check if larger than IHD deaths, if so set upper limit.
#########################################################

cod.draws <- data.table(get_draws("cause_id", gbd_id=493, location_id=proc_loc, year_id=years, source="codcorrect",
                                  age_group_id=ages , measure_id=1, release_id = 16)) #all deaths from CodCorrect

pop <- data.frame(get_population(age_group_id=ages, location_id=proc_loc, year_id=years, sex_id=c(1,2), release_id = 16))

cod.draws.long <- melt(cod.draws, measure.vars = names(cod.draws)[grepl("draw", names(cod.draws))], variable.name = "draw", value.name = "cod_draw")
cod.draws.long <- data.table(merge(cod.draws.long, pop, by=c('location_id','year_id','sex_id','age_group_id')))
cod.draws.long[,cod_draw_rate := cod_draw/population]

## summarize mean 
cod.draws.sum <- cod.draws.long[,.(mean_cod = mean(cod_draw_rate)), by=c('location_id','year_id','sex_id','age_group_id')]

## identify where ph > cod
ph.cod.draws.sum <- merge(dami_scalar_sum, cod.draws.sum, by=c('location_id','year_id','sex_id','age_group_id'))
ph.cod.draws.sum[,mean_diff := mean_cod - mean_ph]
ph.cod.draws.sum[,mismatch := ifelse(mean_diff < 0, 1,0)]

## merge back on mismatches 
dami_scalar_long <- merge(dami_scalar_long, cod.draws.long[,-c('measure_id')], by=c('location_id','year_id','sex_id','age_group_id','draw'))
dami_scalar_long <- merge(dami_scalar_long, ph.cod.draws.sum, by=c('location_id','year_id','sex_id','age_group_id','measure_id'))

## for any location where mismatch == 1, replace ph_draw with cod_rate_draw
dami_scalar_long[mismatch==1, ph_mi := cod_draw_rate]

#####################################################
## Save Necessary columns for upload
#########################################################
dami_scalar_wide <- dcast(dami_scalar_long[,c('location_id','age_group_id','sex_id','year_id','measure_id','draw','ph_mi')], location_id + age_group_id + sex_id + year_id + measure_id~ draw, 
                          value.var = "ph_mi", fun.aggregate = sum)
dami_scalar_wide <- data.table(dami_scalar_wide)
dami_scalar_wide[,location_id := loc]

## write results 
write.csv(dami_scalar_wide, file = paste0('FILEPATH/',loc,'.csv'), row.names = FALSE)


