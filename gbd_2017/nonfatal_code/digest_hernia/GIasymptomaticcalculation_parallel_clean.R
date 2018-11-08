###################################################################################################################
## Author: 
## Based on asymptomatic_parallel.R 
## Description: parallelize getting asymptomatic draws, remove negative 
## Output:  asymptomatic draws
## Notes: hernia 
###################################################################################################################


rm(list=ls())

##working environment

os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- "FILEPATH"
  j<- "J:/"
  h<-"H:/"
} else {
  lib_path <- "FILEPATH"
  j<- "FILEPATH"
  h<-"FILEPATH"
}

##load in arguments from parent script (must start index at 3 because of shell)

cause<-commandArgs()[3]
location<-commandArgs()[4]
draw_files<-commandArgs()[5]
measure<-commandArgs()[6]

age_groups<-c(2:20, 30:32, 235)
##GBD2017 age-group ids for EN, LN, PN, 1-4yo and 5-year groups through 94 and then 95+

##ME_ids
hernia_symp <-  1934
hernia_total <-  9794
hernia_asymp <-  9542
##can add additional causes

##create list of draws

draws <- paste0("draw_", 0:999)

##get total and asymptomatic draws

source(paste0(j, "FILEPATH"))

#get draws from DisMod model for total prevalence
total_draws <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=get(paste0(cause,"_total")), source="epi", location_id=location, measure_id=measure, age_group_id=age_groups)
total_draws[,grep("mod", colnames(total_draws)) := NULL]

symp_draws <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=get(paste0(cause,"_symp")), source="epi", location_id=location, measure_id=measure, age_group_id=age_groups)
symp_draws[,grep("mod", colnames(symp_draws)) := NULL]

##calculate asymptomatic from total - symptomatic, replace negative draws with 0, and save draws in a new folder 

asymp_draws <- copy(total_draws)

all(
  symp_draws[,c("measure_id","location_id","year_id","age_group_id","sex_id"),with=F]==
    asymp_draws[,c("measure_id","location_id","year_id","age_group_id","sex_id"),with=F]
)

asymp_draws <- asymp_draws[,draws,with=FALSE]-symp_draws[,draws,with=FALSE]
asymp_draws[asymp_draws<0] <- 0
asymp_draws <- cbind(
  symp_draws[,c("measure_id","location_id","year_id","age_group_id","sex_id"),with=F],
  asymp_draws)

##make a directory and write these asymptomatic draws to an output file
dir.create(file.path(draw_files), showWarnings = F)
write.csv(asymp_draws, file.path(draw_files, paste0(measure,"_", location, ".csv")), row.names = F)

