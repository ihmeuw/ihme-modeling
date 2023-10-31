##################################
# DESCRIPTION: Location job submission of zinc prevalence calc
# 
# INPUTS: Need to have best run_id and save_folder
# OUTPUTS: Stores them in save_dir/run_id
## AUTHOR: Written by: 
## 
##################################

rm(list = ls())
os <- Sys.info()[1]
user <- Sys.info()[7]

j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "FILEPATH"


code_dir <- paste0("FILEPATH", user, "/")

#source this so we can use the qsub function
source(paste0(code_dir, 'FILEPATH/primer.R'))

library(ini)
library(csvy)
library(fst)
library(readODS)
library(rmatio)
library(rio)
library(dplyr)
library(data.table)


################################################################################
######################  Inputs to fill out  ###################################
################################################################################

save_dir <- "FILEPATH"
run_id <- 149714

################################################################################
################################################################################

source("FILEPATH/get_location_metadata.R")

loc_dt <- get_location_metadata(location_set_id = 35, gbd_round_id = 7)
loc_dt <- loc_dt[is_estimate == 1 & most_detailed == 1]


params <- data.table(location = loc_dt[, location_id])
params[, task_num := 1:.N]
map_path <- paste0(save_dir, "task_map.csv")
write.csv(params, map_path, row.names = F)

code_path <- "FILEPATH/zinc_calc_prev_parallel.R"

#takes approx 
array_qsub(jobname = "zinc_def_calc", 
           shell = "FILEPATH/r_shell_singularity_3501.sh",
           code = "FILEPATH/zinc_calc_prev_parallel.R",
           pass = list(map_path, save_dir, run_id),
           proj = "proj_diet",
           num_tasks = nrow(params),
           mem = 2, cores=1, log = T, submit = T)




