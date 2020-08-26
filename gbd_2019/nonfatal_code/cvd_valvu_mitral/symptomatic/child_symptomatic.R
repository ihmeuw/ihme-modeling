
library(data.table)

date<-gsub("-", "_", Sys.Date())


################### ARGS AND PATHS #########################################
######################################################

args <- commandArgs(trailingOnly = TRUE)
prev_me<-as.integer(args[1])
year<-as.integer(args[2])
sex<-as.integer(args[3])
asympt_by_age<-as.logical(args[4])
decomp_step<-as.character(args[5])
message("Asympt by age:", asympt_by_age)

me<-"VALUE"
sympt_me<-"VALUE"
asympt_me<-"VALUE"
tx_me<-"VALUE"
tx_prop_me<-"VALUE"

asympt_folder <- "FILEPATH"

output_folder<-"FILEPATH"

central<-"FILEPATH"


################### SCRIPTS #########################################
######################################################

source(paste0(central, "get_location_metadata.R"))
source(paste0(central, "get_draws.R"))
source("get_recent.R")

################### GET DATA  #########################################
######################################################

message("Reading in tx draws..")
tx<-get_draws(gbd_id_type = "modelable_entity_id", gbd_id=tx_prop_me, source="epi", measure=18, year_id=year, sex_id=sex)
setnames(tx, paste0("draw_", 0:999), paste0("tx_draw_", 0:999))
tx[, c("modelable_entity_id", "model_version_id", "metric_id", "measure_id"):=NULL]
message("Done getting draws")

message("Reading in prev draws..")
prev<-get_draws(gbd_id_type="modelable_entity_id", gbd_id=prev_me, source="epi", measure=5, year_id=year, sex_id=sex, age_group_id = unique(tx$age_group_id),
                decomp_step=paste0("step", decomp_step))
prev[, c("modelable_entity_id", "model_version_id", "measure_id"):=NULL]
prev<-prev[age_group_id %in% unique(tx$age_group_id)]
message("Done getting draws")

message("Reading in incidence draws..")
inc<-get_draws(gbd_id_type="modelable_entity_id", gbd_id=prev_me, source="epi", measure=6, year_id=year, sex_id=sex, age_group_id = unique(tx$age_group_id),
               decomp_step=paste0("step", decomp_step))
inc[, c("modelable_entity_id", "model_version_id", "measure_id"):=NULL]
inc<-inc[age_group_id %in% unique(tx$age_group_id)]
message("Done getting draws")


asympt<-get_recent(asympt_folder, pattern=paste0("decomp", decomp_step))
setnames(asympt, paste0("draw", 0:999), paste0("asympt_draw_", 0:999))


################### SAVE ASYMPTOMATIC IN PREV SPACE   #########################################
######################################################

if(asympt_by_age==T){
  asympt_full<-merge(prev, asympt, by="age_group_id", all.x=T, all.y=F)
}else{
  asympt_full<-cbind(prev, asympt)
}
asympt_full[, (paste0("asympt_prev_draw_", 0:999)):=lapply(0:999, function(x){
  (get(paste0("asympt_draw_", x)))*get(paste0("draw_", x))
})]

output<-paste0(output_folder, "asympt_draws_cvd/", year, "_", sex, "_5.csv")
asympt_full[, c(grep("^asympt_draw|^draw_", names(asympt_full), value=T), "sex_id", "year_id"):=NULL]
setnames(asympt_full, paste0("asympt_prev_draw_", 0:999), paste0("draw_", 0:999 ))
asympt_full[, modelable_entity_id:=asympt_me]
write.csv(asympt_full, file=output, row.names=F)

output<-paste0(output_folder, "asympt_draws_cvd/", year, "_", sex, "_6.csv")
inc[, modelable_entity_id:=asympt_me]
write.csv(inc, file=output, row.names=F)


################### SAVE TX IN PREV SPACE   #########################################
######################################################

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

output<-paste0(output_folder, "tx_draws_cvd/", year, "_", sex, "_5.csv")
tx_full[, c(grep("^tx_draw|^draw_|^sympt_draw", names(tx_full), value=T), "sex_id", "year_id"):=NULL]
setnames(tx_full, paste0("tx_prev_draw_", 0:999), paste0("draw_", 0:999 ))
tx_full[, modelable_entity_id:=tx_me]
write.csv(tx_full, file=output, row.names=F)


################### CALCULATE AND SAVE SYMPTOMATIC HF   #########################################
###########################################################

if(asympt_by_age==T){
  sympt_full<-merge(prev, asympt, by="age_group_id", all.x=T, all.y=F)
}else{
  sympt_full<-cbind(prev, asympt)
}

sympt_full[, (paste0("sympt_draw_", 0:999)):=lapply(0:999, function(x){
  ((1-get(paste0("asympt_draw_", x))))*get(paste0("draw_", x))
})]
full<-merge(sympt_full, tx, by=c("age_group_id", "year_id", "sex_id", "location_id"))

full[, (paste0("hf_draw_", 0:999)):=lapply(0:999, function(x){
  (1-(get(paste0("tx_draw_", x))))*get(paste0("sympt_draw_", x))
})]


output<-paste0(output_folder, "sympt_draws_cvd/", year, "_", sex, "_5.csv")
full[, c(grep("tx_draw|^asympt_draw|^draw_", names(full), value=T), "sex_id", "year_id"):=NULL]
setnames(full, paste0("hf_draw_", 0:999), paste0("draw_", 0:999 ))
full[, modelable_entity_id:=sympt_me]
write.csv(full, file=output, row.names=F)

