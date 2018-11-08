###################################################################################################################
## Author:
## Based on: ic_scalar_apply_parallel.R 
## Description: parallel child script for indibd_scalar_apply.R
## Output:  scaled draws ready for custom model save_results
## Notes: original stata script written by 
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
  shell <- "FILEPATH"
}

##load in arguments from parent script (must start index at 3 because of shell)

cause<-commandArgs()[3]
location<-commandArgs()[4]
cause_draws<-commandArgs()[5]

age_group_ids<-c(2:20, 30:32, 235)

##ME_ids
ulcerative_colitis_adj <- 3103
crohns_disease_adj <- 3104
ulcerative_colitis_unadj <- 1935
crohns_disease_unadj <- 1937

################################################################################################################
##code starts here; generate 1000 draws of scalar and create a vector; get draws then multiply scalar by draws
################################################################################################################

##values 1+mean/1+lower/1+upper results from "FILEPATH"
mean <- 1.0624
lower <- 1.0549
upper <- 1.0699
sd <- (upper-lower)/(2*1.96)

##generate vector of 1000 draws of distribution centered around scalar w/ standard errors 

ic_scalars<-rnorm(1000,mean, sd)

##get draws for each cause location

source(paste0(j, "FILEPATH"))

unadj_draws <- get_draws(gbd_id_type="modelable_entity_id", gbd_id=get(paste0(cause,"_unadj")), source="epi", location_id=location, measure_id=c(5,6), age_group_id=age_group_ids)
unadj_draws[,grep("mod", colnames(unadj_draws)) := NULL]

##apply scalars to draws

adj_draws <- copy(unadj_draws)

for(i in 0:999) {
  draw <- paste0("draw_", i)
    adj_draws[, draw := adj_draws[, draw,with=F]*ic_scalars[i+1], with=F]
  }

##save draws 

dir.create(file.path(cause_draws, "adjusted_draws"), showWarnings = F)
write.csv(adj_draws, file.path(cause_draws, "adjusted_draws", paste0(location, ".csv")), row.names = F)







