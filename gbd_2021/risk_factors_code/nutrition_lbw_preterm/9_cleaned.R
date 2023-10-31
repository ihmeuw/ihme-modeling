

library(magrittr)
library(ggplot2)
library(parallel)
library(data.table)
library(stringr)
library(argparse)


source("FILEPATH/find_non_draw_cols.R")
 

run_interactively = 0

if(run_interactively == 1){
  
  loc_id = 195
  out_dir = "FILEPATH"
  version = "VERSION"
  aggregation = 1
  bins = 0
  airpol_shifted = 0
  

} else{
  
  args <- commandArgs(trailingOnly = TRUE)
  param_map_filepath <- args[1]
  
  ## Retrieving array task_id
  task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
  param_map <- fread(param_map_filepath)
  
  loc_id <- param_map[task_id, loc_id]
  out_dir <- param_map[task_id, out_dir]
  version <- param_map[task_id, version]
  aggregation <- param_map[task_id, aggregation]
  bins <- param_map[task_id, bins]
  airpol_shifted <- param_map[task_id, airpol_shifted]
  
}

if(airpol_shifted == 1){
  joint_distr_type = "joint_distr_airpol_shifted"
} else {
  joint_distr_type = "joint_distr_raked"
}

# ----- APPLY MODELABLE_ENTITY_SPLITS
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
    me_map <- fread("FILEPATH/me_map.csv")
    data[!(modelable_entity_id %in% me_map[gbd_2020_me == 1, modelable_entity_id]), modelable_entity_id := 0 ]
  } else if (split == "airpol_shifted"){
    me_map <- fread("FILEPATH")
    airpol_map <- fread("FILEPATH")

    setnames(airpol_map, "categorical_parameter", "parameter")
    data <- data[, -c("modelable_entity_id", "modelable_entity_name")]
    data <- merge(data, airpol_map[, -c("modelable_entity_description")], by = "parameter",all.x = T)
    data[!(modelable_entity_id %in% airpol_map[, modelable_entity_id]), modelable_entity_id := 0 ]    
    
  }else{
    message("incorrect me splits specified")
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
    
    message("prevalences less than zero, check diagnostics. changing", data[prev<0, .N], "negatives to 0.")
    
    dir.create(file.path(out_dir, "aggregation", "diagnostics"), recursive = T)
    
    write.csv(data[prev<0, ], paste0(out_dir, "/aggregation/", version, "/diagnostics/", loc_id, ".csv"))
    
    data[prev < 0 , prev := 0]
    
  }
  
  if(data[is.na(prev), .N] > 0){
    
    message("prevalences are equal to NA, check diagnostics. changing", data[is.na(prev), .N], "null values to 0.")
    write.csv(data[is.na(prev), ], paste0(out_dir, "FILEPATH", loc_id, ".csv"))
    
    data[is.na(prev) , prev := 0]
    
  }
  
  return(data)
  
}



convert_wide_1000_draws <- function(data){
  
  source("FILEPATH/find_non_draw_cols.R")
  
  data <- data[ , .(prev_mean = mean(prev), prev_var = var(prev)/.N), by = .(location_id, age_group_id, sex_id, year_id, modelable_entity_id)]
  
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
    
    
    dir.create(file.path(out_dir, "FILEPATH", me), showWarnings = F, recursive = T)
    dir.create(file.path(out_dir, "FILEPATH"), showWarnings = F, recursive = T)
    dir.create(file.path(out_dir, "FILEPATH"), showWarnings = F, recursive = T)
    dir.create(file.path(out_dir, "FILEPATH"), showWarnings = F, recursive = T)
    dir.create(file.path(out_dir, "FILEPATH"), showWarnings = F, recursive = T)
    
    
    me_data <- data[modelable_entity_id == me, ]
    
    # ----- WRITE FOR UPLOAD DATA SET, DOESNT INCLUDE 28 DAYS
    for_upload <- me_data[age_group_id != 9999]
    for_upload <- for_upload[, -c("modelable_entity_id")]
    
    write.csv(for_upload, paste0(out_dir, "FILEPATH"), row.names = F, na = "")
    
    # ----- WRITE AGE SPECIFIC DATA SETS FOR MES 1557, 1558, 1559
    
    if(me %in% c(1557, 1558, 1559)){
      
      birth <- me_data[age_group_id == 164 ]
      write.csv(birth, paste0(out_dir, "/aggregation/", version, "/", me, "/birth/5_", loc_id, ".csv"), row.names = F, na = "")
      
      enn <- me_data[age_group_id == 2 ]
      write.csv(enn, paste0(out_dir, "/aggregation/", version, "/", me, "/0-6/5_", loc_id, ".csv"), row.names = F, na = "")
      
      lnn <- me_data[age_group_id == 3 ]
      write.csv(lnn, paste0(out_dir, "/aggregation/", version, "/", me, "/7-27/5_", loc_id, ".csv"), row.names = F, na = "")
      
      prev_28 <- me_data[age_group_id == 9999 ]
      write.csv(prev_28, paste0(out_dir, "/aggregation/", version, "/", me, "/5_", loc_id,".csv"), row.names = F, na = "")
      
    }
    
  }) %>% try
  
  
}


bind_all_location_data <- function(agedcohorts_dir, version, joint_distr_type, loc_id){
  
  files <- list.files(file.path(out_dir, "age_birth_cohort", version, joint_distr_type, "cohorts"), full.names = T)
  files <- files[grepl(x = basename(files), pattern = paste0("^", loc_id, "_"), fixed = F)]
  
  data <-lapply(files, function(fp){
    
    print(fp)
    data <- readRDS(fp)
  }) %>% rbindlist(use.names = T, fill = T)
  
  return(data)
  
}


dt <- bind_all_location_data(agedcohorts_dir, version, joint_distr_type, loc_id)
  
dt <- dt[!is.na(bw) & !is.na(ga), ] # this should be 0 anyway 

splits.list <- c()

if(aggregation){  splits.list <- c(splits.list, c("0_28_99", "0_37_99", "0_28_32_37_99", "0_2500_9999")) }

if(bins){  splits.list <- c(splits.list, c("bins")) }

if(airpol_shifted){  splits.list <- c(splits.list, c("airpol_shifted")) }


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

















  
