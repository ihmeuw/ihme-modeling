####################
##Author: USERNAME
##Date: 12/30/2017
##Purpose: Child script for parallel NRVD symptomatic
##
########################

rm(list=ls())
os <- .Platform$OS.type


library(data.table)

date<-gsub("-", "_", Sys.Date())


################### ARGS AND PATHS #########################################
######################################################

args <- commandArgs(trailingOnly = TRUE)
prev_me<-as.integer(args[1])
year<-as.integer(args[2])
sex<-as.integer(args[3])
asympt_by_age<-as.logical(as.integer(args[4]))

message("Asympt by age:", asympt_by_age)
if(F){
  year<-2017
  prev_me<-16602
  sex<-1
}
me<-ifelse(prev_me==16601, "aort", ifelse(prev_me== 16602,"mitral", "other"))
sympt_me<-ifelse(me=="aort", 19671, ifelse(me=="mitral", 19672, 19673))
asympt_me<-ifelse(me=="aort", 19386, ifelse(me=="mitral", 19387, 19587))
tx_me<-ifelse(me=="aort", 19578, ifelse(me=="mitral", 19582, 19586))
tx_prop_me<-ifelse(me=="aort", 18811, ifelse(me=="mitral", 18812, 18813))


if(asympt_by_age==T){
  asympt_folder<-paste0("FILEPATH")
}else{
  asympt_folder<-paste0("FILEPATH")
}

output_folder<-paste0("FILEPATH/", me, "/")

central<-paste0("FILEPATH")


################### SCRIPTS #########################################
######################################################

source(paste0(central, "get_location_metadata.R"))
source(paste0(central, "get_draws.R"))
source(paste0("FILEPATH/utility/get_recent.R"))

################### GET DATA  #########################################
######################################################

message("Reading in tx draws..")
tx<-get_draws(gbd_id_type = "modelable_entity_id", gbd_id=tx_prop_me, source="epi", measure=18, year_id=year, sex_id=sex)
setnames(tx, paste0("draw_", 0:999), paste0("tx_draw_", 0:999))
tx[, c("modelable_entity_id", "model_version_id", "metric_id", "measure_id"):=NULL]
message("Done getting draws")


message("Reading in prev draws..")
prev<-get_draws(gbd_id_type="modelable_entity_id", gbd_id=prev_me, source="epi", measure=5, year_id=year, sex_id=sex, age_group_id = unique(tx$age_group_id))
prev[, c("modelable_entity_id", "model_version_id", "measure_id"):=NULL]
prev<-prev[age_group_id %in% unique(tx$age_group_id)]
message("Done getting draws")

message("Reading in incidence draws..")
inc<-get_draws(gbd_id_type="modelable_entity_id", gbd_id=prev_me, source="epi", measure=6, year_id=year, sex_id=sex, age_group_id = unique(tx$age_group_id))
inc[, c("modelable_entity_id", "model_version_id", "measure_id"):=NULL]
inc<-prev[age_group_id %in% unique(tx$age_group_id)]
message("Done getting draws")


asympt<-get_recent(asympt_folder)
setnames(asympt, paste0("draw", 0:999), paste0("asympt_draw_", 0:999))



################### SAVE ASYMPTOMATIC IN PREV SPACE   #########################################
######################################################


##USERNAME: calculate prevalence
if(asympt_by_age==T){
  asympt_full<-merge(prev, asympt, by="age_group_id", all.x=T, all.y=F)
}else{
  asympt_full<-cbind(prev, asympt)
}
asympt_full[, (paste0("asympt_prev_draw_", 0:999)):=lapply(0:999, function(x){
  (get(paste0("asympt_draw_", x)))*get(paste0("draw_", x))
})]

##USERNAME: this saves asymptomatic draws (for sequelea, in prevalence space)
output<-paste0(output_folder, "asympt_draws/", year, "_", sex, "_5.csv")
asympt_full[, c(grep("^asympt_draw|^draw_", names(asympt_full), value=T), "sex_id", "year_id"):=NULL]
setnames(asympt_full, paste0("asympt_prev_draw_", 0:999), paste0("draw_", 0:999 ))
asympt_full[, modelable_entity_id:=asympt_me]
write.csv(asympt_full, file=output, row.names=F)

##USERNAME: write incidence draws to this folder as well
output<-paste0(output_folder, "asympt_draws/", year, "_", sex, "_6.csv")
inc[, modelable_entity_id:=asympt_me]
write.csv(inc, file=output, row.names=F)


################### SAVE TX IN PREV SPACE   #########################################
######################################################

##USERNAME: calculate prevalence
tx_full<-merge(prev, tx, by=c("age_group_id", "year_id", "sex_id", "location_id"))

if(asympt_by_age==T){
  tx_full<-merge(tx_full, asympt, by="age_group_id", all.x=T, all.y=F)
}else{
  tx_full<-cbind(tx_full, asympt)
}

tx_full[, (paste0("sympt_draw_", 0:999)):=lapply(0:999, function(x){
  ((1-get(paste0("asympt_draw_", x))))*get(paste0("draw_", x))
})]

tx_full[, (paste0("tx_prev_draw_", 0:999)):=lapply(0:999, function(x){
  (get(paste0("tx_draw_", x)))*get(paste0("sympt_draw_", x))
})]

##sy: save
output<-paste0(output_folder, "tx_draws/", year, "_", sex, "_5.csv")
tx_full[, c(grep("^tx_draw|^draw_|^sympt_draw", names(tx_full), value=T), "sex_id", "year_id"):=NULL]
setnames(tx_full, paste0("tx_prev_draw_", 0:999), paste0("draw_", 0:999 ))
tx_full[, modelable_entity_id:=tx_me]
write.csv(tx_full, file=output, row.names=F)


################### CALCULATE AND SAVE SYMPTOMATIC HF   #########################################
###########################################################

##USERNAME: first subtract off asymptomatic
if(asympt_by_age==T){
  sympt_full<-merge(prev, asympt, by="age_group_id", all.x=T, all.y=F)
}else{
  sympt_full<-cbind(prev, asympt)
}

sympt_full[, (paste0("sympt_draw_", 0:999)):=lapply(0:999, function(x){
  ((1-get(paste0("asympt_draw_", x))))*get(paste0("draw_", x))
})]
##USERNAME: redo a calculation in order to save tx draws
full<-merge(sympt_full, tx, by=c("age_group_id", "year_id", "sex_id", "location_id"))

full[, (paste0("hf_draw_", 0:999)):=lapply(0:999, function(x){
  (1-(get(paste0("tx_draw_", x))))*get(paste0("sympt_draw_", x))
})]


##USERNAME: this saves symptomatic draws
output<-paste0(output_folder, "sympt_draws/", year, "_", sex, "_5.csv")
full[, c(grep("tx_draw|^asympt_draw|^draw_", names(full), value=T), "sex_id", "year_id"):=NULL]
setnames(full, paste0("hf_draw_", 0:999), paste0("draw_", 0:999 ))
full[, modelable_entity_id:=sympt_me]
write.csv(full, file=output, row.names=F)

