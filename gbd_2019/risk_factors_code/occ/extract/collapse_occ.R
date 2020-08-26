###########################################################
### Purpose: Collapse extraction output
###########################################################

###################
### Setting up ####
###################
rm(list=ls())
os <- .Platform$OS.type

## Load Packages
pacman::p_load(DBI, data.table, haven, dplyr, survey, parallel)

## in/out
dir <- "FILEPATH"

## Load Functions
ubcov_central <- "FILEPATH"
setwd(ubcov_central)
source("FUNCTION")

######################################################################################################################

## collapse differently depending on whether occupation codes and/or industry codes are present in dataset
total_types <- c("census","regular") ## c("census","regular")
total_topics <- apply(expand.grid(c("hrh"), total_types), 1, paste, collapse="_") 

version <- "1" #change to prevent overwriting previous best versions
cores.provided <- 4
config.path <- file.path(dir,"occ_collapse_config.csv") ## Path to config.csv
parallel <- T ## Run in parallel?
slots <- 4 ## How many slots per job (used in mclapply) | Set to 1 if running on desktop
logs <- "FILEPATH" ## Path to logs
cluster_project <- "proj_custom_models"

collapse.topic <- function(this.topic) {
  print(this.topic)

  ## launch collapse and load output
  new <- collapse.launch(topic=this.topic, config.path=config.path, parallel=parallel, slots=slots, logs=logs, cluster_project=cluster_project)
  if (parallel) new <- fread(gsub("DONE! Saved output to ","",new))

  ## check whether output has all the expected inputs (that a job didn't fail)
  config <- fread(config.path)
  files_in <- list.files(config[topic == this.topic,input.root])
  if (length(files_in) != length(unique(new$nid))) {
    warning(paste0(length(files_in) - length(unique(new$nid))," JOBS SEEM TO HAVE FAILED ON ",this.topic, " !!!!!"))
    cat(paste0(length(files_in) - length(unique(new$nid))," JOBS SEEM TO HAVE FAILED ON ",this.topic, " !!!!!"))
    print(paste0(length(files_in) - length(unique(new$nid))," JOBS SEEM TO HAVE FAILED ON ",this.topic, " !!!!!"))
  }

  ## append this collapse to the existing version (if one exists), identifying
  ## duplicates by NID and removing from the older version
  final_file <- file.path(dir,paste0(this.topic,"_",version,".csv"))
  if (file.exists(final_file)){
    existing <- fread(final_file)
    df <- rbind(unique(new),existing[!nid %in% unique(new$nid)],fill=T)
  } else {
    df <- unique(new)
  }
  write.csv(df,final_file,row.names = F)
}

## log all files in folder at time of job submissions
folder <- fread(config.path)[topic %in% total_topics,unique(input.root)]
allfiles <- list.files(folder)

## collapse all surveys for every topic
mclapply(total_topics,collapse.topic,mc.cores = cores.provided)
Sys.sleep(10)

## move files from new folder to the non-new folder, overwriting older versions where applicable
for (file in allfiles){
  file.copy(file.path(folder,file),file.path(gsub("/new","",folder),file),overwrite = T)
  file.remove(file.path(folder,file))
}
