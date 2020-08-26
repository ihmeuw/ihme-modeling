rm(list=ls())

Sys.umask(mode = 002)

library(magrittr)
library(ggplot2)
library(parallel)
library(data.table)

source("FILEPATH/find_non_draw_cols.R")

args <- commandArgs(trailingOnly = TRUE)
param_map_filepath <- args[1]
task_id <- ifelse(is.na(as.integer(Sys.getenv("SGE_TASK_ID"))), 1, as.integer(Sys.getenv("SGE_TASK_ID")))
param_map <- fread(param_map_filepath)
loc_id <- param_map[task_id, loc_id]
v <- param_map[task_id, v]
aggregation <- param_map[task_id, aggregation]
bins <- param_map[task_id, bins]
airpol_shifted <- param_map[task_id, airpol_shifted]

print(loc_id)

out_dir <- "FILEPATH"

apply_ga_lbw_splits <- function(data, split){
  
  if(split == "0_28_32_37_99"){
    data[ga >= 37, modelable_entity_id := 0]
    data[ga < 37 & ga >= 32, modelable_entity_id := 1559]
    data[ga < 32 & ga >= 28, modelable_entity_id := 1558]
    data[ga < 28, modelable_entity_id := 1557]
  } else if (split == "0_37_99"){
    data[ga >= 37, modelable_entity_id := 0]
    data[ga < 37, modelable_entity_id := 15801]
  } else if (split == "0_28_99"){
    data[ga >= 28, modelable_entity_id := 0]
    data[ga < 28, modelable_entity_id := 15798]
  } else if (split == "0_2500_9999"){
    data[bw > 2500, modelable_entity_id := 0]
    data[bw <= 2500, modelable_entity_id := 16282]
  } else if (split == "bins"){
    me_map <- fread("FILEPATH")
    data[!(modelable_entity_id %in% me_map[gbd_2019_me == 1, modelable_entity_id]), modelable_entity_id := 0 ]
  } else if (split == "airpol_shifted"){
    me_map <- fread("FILEPATH")
    airpol_map <- fread("FILEPATH")

    setnames(airpol_map, "categorical_parameter", "parameter")
    data <- data[, -c("modelable_entity_id", "modelable_entity_name")]
    data <- merge(data, airpol_map[, -c("modelable_entity_description")], by = "parameter",all.x = T)
    data[!(modelable_entity_id %in% airpol_map[, modelable_entity_id]), modelable_entity_id := 0 ]    
    
  }else{
    message("Incorrect splits specified")
  }
  
  return(data)
  
  
}


aggregate_into_mes <- function(data){
  
  dt_summary <- data[, .(baseline_exposure = sum(baseline_exposure), period_prevalence = sum(period_prevalence), final_exposure = sum(final_exposure)), by = list(location_id, year_id, sex_id, age_group_id, modelable_entity_id, draw)]

  return(dt_summary)
  
}



convert_to_prev_per_age <- function(data){
  
  data_0_0 <- data[age_group_id == 2, list(location_id, year_id, sex_id, age_group_id, modelable_entity_id, draw, prev = baseline_exposure)]
  data_0_0[, age_group_id := 164]
  
  data_0_6 <- data[age_group_id == 2, list(location_id, year_id, sex_id, age_group_id, modelable_entity_id, draw, prev = period_prevalence)]  
  data_0_6[, age_group_id := 2]
  
  data_7_27 <- data[age_group_id == 3, list(location_id, year_id, sex_id, age_group_id, modelable_entity_id, draw, prev = period_prevalence)]
  data_7_27[, age_group_id := 3]
  
  data_28_28 <- data[age_group_id == 3, list(location_id, year_id, sex_id, age_group_id, modelable_entity_id, draw, prev = final_exposure)]
  data_28_28[, age_group_id := 9999]
  
  data <- rbindlist(list(data_0_0, data_0_6, data_7_27, data_28_28), use.names = T, fill = T)
  
  if(data[prev < 0, .N] > 0){
    
    write.csv(data[prev<0, ], "FILEPATH")
    data[prev < 0 , prev := 0]
    
  }
  
  if(data[is.na(prev), .N] > 0){
    
    write.csv(data[is.na(prev), ], "FILEPATH")
    
    data[is.na(prev) , prev := 0]
    
  }
  
  return(data)
  
}



convert_wide_1000_draws <- function(data){
  
  data <- data[ , .(prev_mean = mean(prev), prev_var = var(prev)), by = .(location_id, age_group_id, sex_id, year_id, modelable_entity_id)]
  
  data[, shape1 := ((1 - prev_mean) / prev_var - 1 / prev_mean) * prev_mean ^ 2]
  data[, shape2 := shape1 * (1 / prev_mean - 1)]
  
  num_draws = 999
  newvars = paste0("draw_", 0:num_draws)
  data[!(prev_mean == 0 & prev_var == 0), (newvars) := as.list(rbeta(num_draws,shape1, shape2)),  by = .(location_id, age_group_id, sex_id, year_id, modelable_entity_id)]
  
  data[(prev_mean == 0 & prev_var == 0), (newvars) := 0,  by = .(location_id, age_group_id, sex_id, year_id, modelable_entity_id)]

  data[, metric_id := 3]
  data[, measure_id := 5]
  
  data <- data[, -c("prev_mean", "prev_var", "shape1", "shape2")]
  
  setkeyv(data, c("modelable_entity_id", "location_id", "year_id", "sex_id", "age_group_id", "metric_id", "measure_id"))
  return(data)
  
}


save_by_me_loc_sex_year_age <- function(data){
  
  
  me.list <- unique(data[modelable_entity_id != 0, modelable_entity_id])
  
  lapply(me.list, function(me) {
    
    
    dir.create(file.path(out_dir, "aggregation", me), showWarnings = F)
    dir.create(file.path(out_dir, "aggregation", me, "for_upload"), showWarnings = F)
    dir.create(file.path(out_dir, "aggregation", me, "birth"), showWarnings = F)
    dir.create(file.path(out_dir, "aggregation", me, "0-6"), showWarnings = F)
    dir.create(file.path(out_dir, "aggregation", me, "7-27"), showWarnings = F)
     
    me_data <- data[modelable_entity_id == me, ]
    
    for_upload <- me_data[age_group_id != 9999]
    for_upload <- for_upload[, -c("modelable_entity_id")]
    
    write.csv(for_upload, "FILEPATH", row.names = F, na = "")
    
    
    if(me %in% c(1557, 1558, 1559)){
      
      birth <- me_data[age_group_id == 164 ]
      write.csv(birth, "FILEPATH", row.names = F, na = "")
      
      enn <- me_data[age_group_id == 2 ]
      write.csv(enn, "FILEPATH", row.names = F, na = "")
      
      lnn <- me_data[age_group_id == 3 ]
      write.csv(lnn, "FILEPATH", row.names = F, na = "")
      
      prev_28 <- me_data[age_group_id == 9999 ]
      write.csv(prev_28, "FILEPATH", row.names = F, na = "")
      
    }
    
  }) %>% try
  
  
}


dt <- readRDS("FILEPATH")
  
dt <- dt[!is.na(bw) & !is.na(ga), ]

splits.list <- c()

if(aggregation == 1){  splits.list <- c(splits.list, c("0_28_99", "0_37_99", "0_28_32_37_99", "0_2500_9999")) }

if(bins == 1){  splits.list <- c(splits.list, c("bins")) }

if(airpol_shifted == 1){  splits.list <- c(splits.list, c("airpol_shifted")) }


for(s in splits.list){
    
    dt_split <- copy(dt)
    dt_split <- apply_ga_lbw_splits(dt_split, split = s)
    dt_split <- aggregate_into_mes(dt_split)
    dt_split <- convert_to_prev_per_age(dt_split)
    dt_split <- convert_wide_1000_draws(dt_split)
    
    print(dt_split[, .N, by = eval(find_non_draw_cols(dt_split))])
    print(dt_split[, sum(draw_0), by = .(year_id, sex_id, age_group_id)])
    
    save_by_me_loc_sex_year_age(dt_split)
    
}