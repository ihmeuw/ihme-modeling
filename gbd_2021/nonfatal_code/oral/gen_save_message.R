### this script generates updated save_results descriptions for each model that is run
### must run on cluster
  ### login to cluster, get a qlogin
  ### type in 'R' in the command line
  ### source this function by executing 'source("/prog_dir/gen_save_messages.R")

  ### be sure to update filepath in '01_saveresults.R' to reflect the filepath of your repo
source(paste0(j, "FILEPATH"))

## until then, change these variables manually to run
me_list <- c(2337, 2336, 2335, 2334)
prog_dir <- "ADDRESS"

### generate current best_model versions
best <- get_best_model_versions(entity="modelable_entity", ids = me_list, gbd_round_id = 6, decomp_step = 'step2')
save_map <- read.csv(paste0(prog_dir, "/save_descriptions.csv"))
keeps <- c("model_version_id", "acause", "modelable_entity_id")
best <- best[,colnames(best) %in% keeps, with=FALSE]
names(best)[names(best) == "modelable_entity_id"] <- "parent_id"
save_map <- merge(save_map, best, by = 'parent_id', all.x = TRUE)


constant_str <- "Decomp 2 upload " ## change this string to change the message you want before writing the model versions
for(i in c(1:nrow(save_map))){
  if(is.na(save_map$split_id[i])){
    save_map$message[i] <- paste0(constant_str, " -- parent_id = ", save_map$parent_id[i], " (", save_map$acause[i], "), best_model_version_used = ", save_map$model_version_id[i])
  }
  else{
    save_map$message[i] <- paste0(constant_str, " -- parent_id = ", save_map$parent_id[i], " (", save_map$acause[i], "), best_model_version_used = ", save_map$model_version_id[i], "; split by ", best$acause[best$parent_id == save_map$split_id[i]], " = ", save_map$split_id[i], ", best_model_version used = ", best$model_version_id[best$parent_id == save_map$split_id[i]])
    
  }
}

write.csv(save_map, paste0(prog_dir, "/FILEPATH"), row.names = FALSE)