################
# 4.3. Merge RR & DALYs
################

#Grab packages
library(tidyverse)
library(data.table)
library(plyr)
library(dplyr)
library(parallel)
library(ggplot2)

source('FILEPATH') # 04_00_tmrel_functions.R
source('FILEPATH') # 04_00_tmrel_config.R

param_map <- fread('FILEPATH')
task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))

s <- param_map[task_id, sex]
l <- param_map[task_id, location]
a <- param_map[task_id, age]

daly_path <- 'FILEPATH'
directory <- 'FILEPATH'

message("starting execution")

setwd(directory)
files <- list.files(directory)
files <- files[grep(".csv", files)]

message("binding rr")
rr <- rbindlist(lapply(files, grab_and_format))
rr <- rr[exposure <= 100]

daly_files <- list.files(daly_path)
setwd(daly_path)

message("binding dalys")
daly_draws <- rbindlist(lapply(daly_files, fread))

rr_sub <- copy(rr)
rr_sub$cause_id <- as.numeric(rr_sub$cause_id)

daly_draws_sub <- daly_draws[location_id == l & age_group_id == a & sex_id == s]


total <- data.table(join(rr_sub, daly_draws_sub, by=c("cause_id", "draw", "sex_id"))) 

total[, weighted_rr := rr*weight_factor]
total[, all_cause_rr := sum(.SD$weighted_rr), by=c("draw", "exposure", specific)]
total[,c("cause_id", "weight_factor", "weighted_rr", "rr")] <- NULL
total <- unique(total)

save_path <- 'FILEPATH'
dir.create(save_path)

fwrite(total, 'FILEPATH', row.names = F)

