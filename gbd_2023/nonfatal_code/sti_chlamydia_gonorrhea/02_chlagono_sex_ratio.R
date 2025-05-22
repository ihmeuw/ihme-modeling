###################################################################################################################
## SEX RATIO MODELING
## Purpose: apply sex ratio from data processing to calculate male prevalence from female DisMod prevalance
###################################################################################################################

# rm(list=ls())

# GET ARGS ---------------------------------------------------------------------------
args<-commandArgs(trailingOnly = TRUE)
location<-args[1]
out_dir<-args[2]
crosswalk_version<-args[3]
scalars_loc<-args[4]
age_group_ids <- c(2:3,388, 389, 238, 34,6:20, 30:32, 235) #all ages


################################################################################################################
## generate 1000 draws of scalar and create a vector; get draws then multiply scalar by draws
################################################################################################################

##bring in model draws from MR-BRT
scalars <- read.csv(scalars_loc)

##get draws for each cause location
#data frame objects, getting draws from dismod model
unadj_draws <- get_draws(gbd_id_type="modelable_entity_id", 
                         gbd_id=OBJECT,
                         source="epi",
                         location_id=location, 
                         measure_id=5,
                         sex_id=2,
                         age_group_id=age_group_ids, 
                         release_id = release)
unadj_draws[,grep("mod", colnames(unadj_draws)) := NULL]

#make a copy
adj_draws <- copy(unadj_draws) #make a copy

#change sex id to male 
adj_draws$sex_id<- 1

# logit of the adj draws
vars <- c(paste0("draw_", seq(0,999, by=1)))
adj_draws<-adj_draws[, (vars) := lapply(.SD, function(x) log(x/(1-x))), .SDcols=vars]

##apply scalars to draws
##scalars coming from MR-BRT outputs should alreaady be logit-transformed
for(i in 0:999) { #loop
  draw <- paste0("draw_", i)
  scalar_col <- paste0("V", (i+1))
  adj_draws[, (draw) := adj_draws[, draw,with=F]-scalars[, scalar_col]]
}

# inverse logit
vars <- c(paste0("draw_", seq(0,999, by=1)))
adj_draws<- adj_draws[, (vars) := lapply(.SD, function(x) exp(x)/(1+exp(x))), .SDcols=vars]

#append male and female to the same folder
both_sexes<- rbind(adj_draws,  unadj_draws)

both_sexes[,"modelable_entity_id"] <- OBJECT
both_sexes[,"bundle_id"] <- OBJECT
both_sexes[,"crosswalk_version_id"] <- crosswalk_version

##save draws; products of two sets of draws and save them in csv. 
write.csv(both_sexes, file.path(out_dir,"/", paste0(location, ".csv")), row.names = F)

