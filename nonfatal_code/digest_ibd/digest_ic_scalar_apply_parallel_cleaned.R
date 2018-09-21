###################################################################################################################
## Author: USERNAME
## Description: parallel child script for ic_scalar_apply.R
## Output:  scaled draws ready for custom model save_results
###################################################################################################################


rm(list=ls())

##working environment

os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- FILEPATH
  j<- FILEPATH
  h<- FILEPATH
} else {
  lib_path <- FILEPATH
  j<- FILEPATH
  h<- FILEPATH
  shell <- FILEPATH
}

##load in arguments from parent script (must start index at 3 because of shell)

cause<-commandArgs()[3]
location<-commandArgs()[4]
cause_draws<-commandArgs()[5]

##update if age_group_ids change, or if changing measure of draws 

age_group_ids<-c(2:20, 30:32, 235)

##ME_ids
ulcerative_colitis_adj <- 1935
crohns_disease_adj <- 1937

################################################################################################################
##code starts here; generate 1000 draws of scalar and create a vector; get draws then multiply scalar by draws
################################################################################################################

##fill in values from digest_ic_scalar_cleaned.R
mean <- 1.0590
lower <- 1.0472
upper <- 1.0708
sd <- (upper-lower)/(2*1.96)

##generate vector of 1000 draws of distribution centered around scalar w/ standard errors 

ic_scalars<-rnorm(1000,mean, sd)

##get draws for each cause location

source(paste0(j, FILEPATH))

unadj_draws <- get_draws(gbd_id_field="modelable_entity_id", gbd_id=get(paste0(cause,"_adj")), source="epi", location_id=location, measure_ids=c(5,6))
unadj_draws <- unadj_draws[age_group_id %in% age_group_ids,]
unadj_draws[,grep("mod", colnames(unadj_draws)) := NULL]

##apply scalars to draws

adj_draws <- copy(unadj_draws)

for(i in 0:999) {
  draw <- paste0("draw_", i)
    adj_draws[, draw := adj_draws[, draw,with=F]*ic_scalars[i+1], with=F]
  }

##save draws 

dir.create(file.path(FILEPATH))
write.csv(adj_draws, FILEPATH)







