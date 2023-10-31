################
# 4.2. Calculate DALY weights
################

##### Load packages & functions ---------------------------------------------------------------------------------------
library(dplyr)
library(tidyverse)
library(data.table)
library(plyr)
library(dplyr)
library(parallel)
library(ggplot2)

source('FILEPATH')
source('FILEPATH')
source('FILEPATH') # 04_00_tmrel_config.R
source('FILEPATH') # 04_00_tmrel_functions.R

param_map <- fread('FILEPATH')
task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))

s <- param_map[task_id, sex]
l <- param_map[task_id, location]
a <- param_map[task_id, age]


##### Load RRs ---------------------------------------------------------------------------------------

if ("year_id" %in% specific){
  path <- 'FILEPATH'
} else {
  path <-'FILEPATH'
}

list_burden <- list.files(path)
list_burden <- list_burden[grep(paste0("_",s,".csv"), list_burden)]

##### Load DALYs ---------------------------------------------------------------------------------------

message("pulling burden")

daly_draws <- rbindlist(mclapply(list_burden, agg_burden, mc.cores = 5, for_viz = F))
daly_draws <- daly_draws[location_id == l & age_group_id == a]

daly_draws$model <- "new"

##### Clean DALYs ---------------------------------------------------------------------------------------

message("cleaning up burden")

if(loc_type == "super_region"){
  daly_draws <- merge(daly_draws, super_regions, by.x = "location_id", by.y = "region_id", all.x = T)
  daly_draws[,c("location_id")] <- NULL
  daly_draws <- setnames(daly_draws, old = "super_region_id", new = "location_id")
}

if (length(same) > 0){
  daly_draws <- copy(daly_draws) %>%
    .[, dalys := sum(.SD$dalys), by=c("draw", "cause_id", "model", specific)] %>%
    .[, (same) := NULL] %>% unique()
}

daly_draws <- daly_draws[cause_id %in% causes] 

##### Calculate Weights ---------------------------------------------------------------------------------------

daly_draws[, total_dalys := sum(.SD$dalys), by=c("draw","model", specific)]
daly_draws[, weight_factor := dalys/total_dalys]

daly_draws <- merge(daly_draws, cause_map[,c("cause_id_old", "cause_id_new")], by.x = "cause_id", by.y = "cause_id_new", all.x = T)
daly_draws[cause_id %in% c(522, 523, 524, 525, 971), cause_id_old := 524]
daly_draws[cause_id %in% c(418, 419, 420, 421, 996), cause_id_old := 420]
daly_draws[is.na(cause_id_old), cause_id_old := cause_id]

daly_draws[, weight_factor_tot := sum(.SD$weight_factor), by=c("draw", specific, "cause_id_old")]

daly_draws[,c("weight_factor", "cause_id")] <- NULL
daly_draws <- unique(daly_draws)

setnames(daly_draws, old = c("cause_id_old", "weight_factor_tot"), new = c("cause_id", "weight_factor"))


### Clean dataframe to match relative risks ------------------------------------------------
daly_draws[,c("dalys", "total_dalys")] <- NULL
daly_draws[, draw:=as.numeric(gsub("draw_", "", draw))]
daly_draws <- unique(daly_draws)


#Example: pretend as if IHD was 50% of deaths
#daly_draws[cause_id == 493, weight_factor := 100]
#daly_draws[, weight_adj := sum(.SD$weight_factor), by="draw"]
#daly_draws[, weight_factor := weight_factor/weight_adj, by="draw"]
#daly_draws <- daly_draws[, .(draw, cause_id, weight_factor)]

fwrite(daly_draws, 'FILEPATH', row.names = F)
