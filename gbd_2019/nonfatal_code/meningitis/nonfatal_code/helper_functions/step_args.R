steps = "02a"
step <- steps
date <- date <- gsub("-", "_", Sys.Date())
# date <- "2019_09_25"

library(data.table)

# source shared functions
source("get_demographics.R")
source("get_location_metadata.R")

step_args <- fread("step_args.csv")

step_name <- step_args[step_num == step, step_name]
step_num <- step

code_dir <- paste0("filepath")
in_dir <- # filepath
men_dir <- paste0("filepath", date)
root_tmp_dir = paste0(men_dir,"/draws")
root_j_dir <- paste0(men_dir,"/logs")

out_dir <- paste0(root_j_dir, "/", step_num, "_", step_name)
tmp_dir <- paste0(root_tmp_dir, "/",step_num, "_", step_name)

locations <- 44651
location <- locations

ds <- 'step4'