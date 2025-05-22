################################################################################################################
## Title: cad prevalence split code
## Purpose: Split cad model (ME 1817) into severities (mild - 1818, mod - 1819, sev - 1820, asymp - 3102)
################################################################################################################

## Source central functions: 
central <- "FILEPATH"
for (func in paste0(central, list.files(central))) source(paste0(func))

## Source my helper functions
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

date<-gsub("-", "_", Sys.Date())

args <- commandArgs(trailingOnly = T)
parameters_filepath <- args[1]  #locations. 
print(parameters_filepath)
parameters <- fread(parameters_filepath) #for reading table data

## testing parameters
task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
loc<- parameters[task_id, locs] 
print(paste0("Spliting CAD for location ", loc))


rel_id <- 16

#########################################################
## Read in data 
#########################################################

## Read in CAD 
cad_prev <- data.table(get_draws(gbd_id_type='modelable_entity_id', source = 'epi', gbd_id = 27177, sex_id=c(1,2),
                                 location_id=c(loc), release_id = rel_id, measure_id = c(5,6)))


cad_prev_long <- data.table(melt(cad_prev, measure.vars = names(cad_prev)[grepl("draw", names(cad_prev))], variable.name = "draw", value.name = "cad_draw"))
cad_prev_long[age_group_id==7, cad_draw := 0] 
cad_prev_long[measure_id==6, cad_draw :=0] ## incidence set to 0

## Read in HF draws and remove 
hf_prev <- data.table(get_draws(gbd_id_type='modelable_entity_id', source = 'epi', gbd_id = 9567, sex_id=c(1,2),
                                location_id=c(loc), release_id = rel_id, measure_id = c(5,6)))

hf_prev_long <- melt(hf_prev, measure.vars = names(hf_prev)[grepl('draw',names(hf_prev))], variable.name = 'draw', value.name = 'hf_draw')
hf_prev_long[measure_id==6, hf_draw :=0] ## incidence should be 0

## merge on hf and remove
cad_prev_long_minus_hf <- data.table(merge(cad_prev_long, hf_prev_long, 
                                           by=c('age_group_id','sex_id','year_id','location_id','measure_id','draw')))

cad_prev_long_minus_hf[,cad_wo_hf_draw := cad_draw-hf_draw]
cad_prev_long_minus_hf[cad_wo_hf_draw < 0, cad_wo_hf_draw := 0]

## read in severity split draws
sev_split_path <-'FILEPATH'
files <- list.files(sev_split_path)
sex_files <- c('493_1.csv','493_2.csv')

prop_m <- data.table(read.csv(paste0(sev_split_path,sex_files[1])))
prop_f <- data.table(read.csv(paste0(sev_split_path,sex_files[2])))
prop_all <- data.table(rbind(prop_m,prop_f))

## fill 2023 and 2024 for severity split - same value regardless of year. 
prop_all_2023 <- prop_all[year_id == 2022, ]
prop_all_2023[, year_id := 2023]

prop_all_2024 <- prop_all[year_id == 2022, ]
prop_all_2024[, year_id := 2024]

prop_all <- rbind(prop_all, prop_all_2023, prop_all_2024)
prop_all <- prop_all[year_id %in% unique(cad_prev$year_id)]

## 3102 - asymptomatic
## 1818 - mild
## 1819 - moderate
## 1820 - severe

sev_split_long <- melt(prop_all, measure.vars = names(prop_all)[grepl("draw", names(prop_all))], variable.name = "draw", value.name = "cad_draw")
sev_split_long <- data.table(sev_split_long)
sev_split_long_me <- dcast(sev_split_long[,c('measure_id','sex_id','year_id','age_group_id','child_meid','draw','cad_draw')],
                           measure_id + sex_id + year_id + age_group_id + draw ~ child_meid, 
                           value.var = "cad_draw", fun.aggregate = sum)
sev_split_long_me <- data.table(sev_split_long_me)


## merge severity split onto cad prevalence
cad_prev_and_sev <- merge(cad_prev_long_minus_hf,sev_split_long_me, by=c('measure_id','sex_id','year_id','age_group_id','draw'))
cad_prev_and_sev <- data.table(cad_prev_and_sev)
cad_prev_and_sev[,`:=` (custom_3102 = cad_wo_hf_draw*`3102`,
                        custom_1818 = cad_wo_hf_draw*`1818`,
                        custom_1819 = cad_wo_hf_draw*`1819`,
                        custom_1820 = cad_wo_hf_draw*`1820`)]

## asymptomatic
draws_3102 <- cad_prev_and_sev[,c('location_id','sex_id','year_id','age_group_id','measure_id','draw','custom_3102')]
draws_3102_save <- dcast(draws_3102,
                         location_id  + sex_id + year_id  + age_group_id+ measure_id ~ draw, 
                         value.var = "custom_3102", fun.aggregate = sum)
write.csv(draws_3102_save, file = paste0('FILEPATH',loc,'.csv'))

## mild
draws_1818 <- cad_prev_and_sev[,c('location_id','sex_id','year_id','age_group_id','measure_id','draw','custom_1818')]
draws_1818_save <- dcast(draws_1818,
                         location_id  + sex_id + year_id  + age_group_id+ measure_id  ~ draw, 
                         value.var = "custom_1818", fun.aggregate = sum)
write.csv(draws_1818_save, file = paste0('FILEPATH',loc,'.csv'))

## moderate
draws_1819 <- cad_prev_and_sev[,c('location_id','sex_id','year_id','age_group_id','measure_id','draw','custom_1819')]
draws_1819_save <- dcast(draws_1819,
                         location_id  + sex_id + year_id  + age_group_id+ measure_id ~ draw, 
                         value.var = "custom_1819", fun.aggregate = sum)
write.csv(draws_1819_save, file = paste0('FILEPATH',loc,'.csv'))

## severe
draws_1820 <- cad_prev_and_sev[,c('location_id','sex_id','year_id','age_group_id','measure_id','draw','custom_1820')]
draws_1820_save <- dcast(draws_1820,
                         location_id  + sex_id + year_id  + age_group_id+ measure_id  ~ draw, 
                         value.var = "custom_1820", fun.aggregate = sum)
write.csv(draws_1820_save, file = paste0('FILEPATH',loc,'.csv'))

