##########################################################################
### Purpose: Use proportion model #9428 to split intellectual disability, adj (#18777) into proportion profound and severe (9935 & 9936) by country. 
#rm(list = ls())

# System info & drives
os <- Sys.info()[1]
user <- Sys.info()[7]
j <- if (os == "Linux") "/home/j/" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("/homes/", user, "/") else if (os == "Windows") "H:/"

code_dir <- if (os == "Linux") paste0("/ihme/code/dbd/", user, "/") else if (os == "Windows") ""
source(paste0(code_dir, 'shared/utils/primer.R'))


source("FILEPATH/get_model_results.R")


#bring in arguments from wrapper call
args<-commandArgs(trailingOnly = TRUE)
map_path <-args[1]
save_dir <-args[2]
dstep <- args[3]
params <- fread(map_path)
task_id <- Sys.getenv("SGE_TASK_ID")
location_id <- params[task_num == task_id, location] 

print(save_dir)

if(!dir.exists(paste0(save_dir, "/split_18777"))){
  dir.create(paste0(save_dir, "/split_18777"))
  dir.create(paste0(save_dir, "/split_18777/9935"))
  dir.create(paste0(save_dir, "/split_18777/9936"))
  }

prop_model <- get_draws(gbd_id_type="modelable_entity_id",gbd_id=9428, gbd_round_id = 6, decomp_step = dstep, source="epi", location_id=location_id)
id_parent_model <- get_draws(gbd_id_type="modelable_entity_id",gbd_id=18777, gbd_round_id = 6, decomp_step = dstep, source="epi", location_id=location_id)

prop_model[, c("measure_id", "measure", "model_version_id", "metric_id", "modelable_entity_id"):=NULL]
id_parent_model[, c("model_version_id", "metric_id", "modelable_entity_id"):=NULL]

#setnames
colnames(prop_model) <- gsub("draw_", "prop_", colnames(prop_model))
colnames(id_parent_model) <- gsub("draw_", "total_", colnames(id_parent_model))
totals <- sapply(0:999, function(x) paste0("total_",x))
props <- sapply(0:999, function(x) paste0("prop_",x))

# 9935 =  18777*(1-9428)
# 9936 =  18777*(9428)

adjust_model9936 <- merge(id_parent_model, prop_model, by=c("location_id","year_id","age_group_id","sex_id"))
adjust_model9935 <- merge(id_parent_model, prop_model, by=c("location_id","year_id","age_group_id","sex_id"))

for (i in 0:999) {
  if(i %in% c(250,500,750,900)){message(i)}
  adjust_model9936[,paste0('draw_',i):= get(paste0("total_", i))*get(paste0("prop_",i)) ] 
  adjust_model9935[,paste0('draw_',i):= get(paste0("total_", i))*(1-get(paste0("prop_",i))) ] 
}

adjust_model9935[, c(totals,props):=NULL]
adjust_model9936[, c(totals,props):=NULL]

write.csv(adjust_model9935, paste0(save_dir, "/split_18777/9935/",location_id,".csv"), row.names = FALSE)
write.csv(adjust_model9936, paste0(save_dir, "/split_18777/9936/",location_id,".csv"), row.names = FALSE)

message("csvs have been saved to!")
