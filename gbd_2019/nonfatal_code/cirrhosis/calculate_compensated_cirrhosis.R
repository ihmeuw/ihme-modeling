################################################################################################
## Description: Calculates compensated cirrhosis (=total cirrhosis - decompensated cirrhosis), 
## for a given location, age, sex, year.
## Output:  Compensated cirrhosis draws
################################################################################################

rm(list=ls())

## Load arguments passed with qsub ----
args <- commandArgs(trailingOnly = TRUE)
location<-args[1]
cause_draws<-args[2]

## Set IDs ----
age_group_ids<-c(2:20, 30:32, 235)
#prevalence and incidence
measures <- c(5, 6)

## Use shared functions ----
source("FILEPATH/get_draws.R")
total_draws <- get_draws(gbd_id_type="modelable_entity_id", 
                         gbd_id=24544, 
                         source="epi", 
                         location_id=location, 
                         measure_id=measures, 
                         gbd_round_id = 6, 
                         decomp_step= "step4")
symp_draws <- get_draws(gbd_id_type="modelable_entity_id", 
                        gbd_id=24545, 
                        source="epi", 
                        location_id=location, 
                        measure_id=measures, 
                        gbd_round_id = 6, 
                        decomp_step= "step4")

## Subset ages of interest ----
total_draws <- total_draws[age_group_id %in% age_group_ids,]
symp_draws <- symp_draws[age_group_id %in% age_group_ids,]

## Drop columns that aren't needed ----
total_draws[,grep("mod", colnames(total_draws)) := NULL]
symp_draws[,grep("mod", colnames(symp_draws)) := NULL]
           
## Creat list of draws ----
draws <- paste0("draw_", 0:999)

## Calculate asymptomatic = total - symptomatic ----
asymp_draws <- copy(total_draws)

for (draw in draws) {
  asymp_draws[,draw := get(draw) - symp_draws[,get(draw)],with=F]
  asymp_draws[[draw]][asymp_draws[[draw]]<0]=0 # sets any negative draws to zero
}

if(!dir.exists(cause_draws)) dir.create(cause_draws)
write.csv(asymp_draws, FILEPATH, row.names=FALSE)
