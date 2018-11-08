###################################################################################################################
## Author:
## Description: Calculates compensated cirrhosis (=total cirrhosis - decompensated cirrhosis), for a given location, age, sex, year.
## Output:  Compensated cirrhosis draws
## Notes: 
###################################################################################################################

rm(list=ls())

## Working environment ----
os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- "FILEPATH"
  j<- "FILEPATH"
  h<-"FILEPATH"
} else {
  lib_path <- "FILEPATH"
  j<- "FILEPATH"
  h<-"FILEPATH"
  shell <-"FILEPATH"
  scratch <- "FILEPATH"
}

## Load arguments passed with qsub ----
cause<-commandArgs()[3]
location<-commandArgs()[4]
cause_draws<-commandArgs()[5]

## Set IDs ----
age_group_ids<-c(2:20, 30:32, 235)
#prevalence and incidence
measures <- c(5, 6)
# ME_ids
cirrhosis_total <- 9999
cirrhosis_symp <- 9999 # technically "decompensated", but keep "symp" for consistent naming convention with other causes that use similar code


## Use shared functions ----
source(paste0(j, "FILEPATH"))
total_draws <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=get(paste0(cause,"_total")), source="epi", location_id=location, measure_id=measures)
symp_draws <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=get(paste0(cause,"_symp")), source="epi", location_id=location, measure_id=measures)

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

write.csv(asymp_draws, file.path(cause_draws, "asymptomatic", paste0(location, ".csv")),row.names=FALSE)

## the end
