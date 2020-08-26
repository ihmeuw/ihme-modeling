### this script generates updated save_results descriptions for each model that is run
### must run on cluster
  ### login to cluster, get a qlogin
  ### type in 'R' in the command line

rm(list=ls())
if (Sys.info()[1] == "Linux"){
  j <- "FILEPATH"
  h <- paste0("USERNAME")
} else if (Sys.info()[1] == "Windows"){
  j <- "J:/"
  h <- "H:/"
}

source(paste0("FILEPATH"))

# ### read in args
# arg <- commandArgs(trailingOnly = TRUE)
# me_list <- arg[1]
# prog_dir <- arg[2]

me_list <- c(2337, 2336, 2335, 2334)
prog_dir <- "FILEPATH"

### generate current best_model versions
best <- get_best_model_versions(entity="modelable_entity", ids = me_list, gbd_round_id = 6, decomp_step = 'step2')
save_map <- read.csv(paste0("FILEPATH"))
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

write.csv(save_map, paste0(prog_dir, "FILEPATH"), row.names = FALSE)
