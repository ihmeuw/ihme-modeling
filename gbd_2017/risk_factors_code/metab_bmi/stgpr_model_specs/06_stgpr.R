# Load packages
rm(list=ls())
model_root <- 'FILEPATH'
library("RMySQL")
library("dplyr")
library("haven")
library("lme4")
library("data.table")
setwd(model_root)
source('init.r')
source('register_data.r')

args <- commandArgs(trailingOnly = TRUE)
data_notes <- args[1]
mark_best <- args[2]
data_path <- args[3]
num_draws <- args[4]
me <- args[5]
date <- args[6]
if (me == "obese") {
    name <- "obesity"
    f_name <- "ob"
} else {
    name <- "overweight"
    f_name <- "ow"
}

# Data Upload
me_name <- paste0("metab_", me)
username <- "USERNAME"

#data path - data must be in csv or dta format
id <- register_data(me_name = me_name, path = data_path, user_id=username, notes=data_notes, is_best=mark_best, bypass=TRUE)

#Set data_id, model_id, and path to config file
data_ids <- id
path <- paste0("FILEPATH", name, "_config_2017.csv")
model_id <- max(read.csv(path)$my_model_id)
my_model_ids <- model_id

## Load your config file to get model_ids and run_ids assigned (don't change anything)
run_ids <- lapply(1:length(data_ids), function(x) {
my_model_id <- my_model_ids[x]
data_id <- data_ids[x]
id <- register.config(path = path,
                      my.model.id = my_model_id,
                      data.id = data_id)
return(id$run_id)
})

write(unlist(run_ids), file=paste0("FILEPATH", date, "/", f_name, "_run_id.txt"), sep="")

## Settings
holdouts        <- 0                                         # 0-10 (0 indicating no cross-validation)
cluster_project <- 'proj_ensemble'
draws           <- num_draws                                 # Leave as 1000 for now
nparallel       <- 150                                       # Please put some positive integer
logs            <- "FILEPATH"
slots           <- 8                                         # Number of slots for components
master_slots    <- 50                                        # Number of slots for the "master" job (should be larger than "slots")
ko_pattern      <- "country"                                 #Specify either "random" or "country"

## Run entire pipeline for each new run_id
mapply(submit.master, run_ids, holdouts, draws, cluster_project, nparallel, slots, model_root, logs, master_slots, ko_pattern)
