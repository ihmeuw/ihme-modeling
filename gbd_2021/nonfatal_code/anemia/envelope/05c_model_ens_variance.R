#' Purpose: Find optimal variance that produces an ensemble distribution characterized by mean/variance that minimizes weighted squared error at thresholds
# --------------

rm(list=ls())

Sys.umask(mode = 002)

library(data.table)
library(magrittr)
library(ggplot2)

source("FILEPATH/get_demographics.R"  )
source(paste0("FILEPATH/ens_variance_functions.R"))

date <- "2020-09-22"

run_interactively = 0

if(run_interactively == 1){
  
  decomp_step = "iterative"
  gbd_round_id = 7
  var = "anemia"
  version = "gbd2020_prelim"
  mean_meid = 10487
  prev_meids = "25321,25322,25318"
  thresholds = "80,110,130"
  threshold_weights = "0.149,0.052,0.004"
  outdir = "FILEPATH"
  num_draws = 1000
  location_ids = 191
  year_ids = 2019
  sex_ids = c(1)
  age_group_ids = 10
  
  microdata_filepath = "FILEPATH/microdata.csv"
  
  dir.create(file.path(outdir, var, version), recursive = T)
  
  if(microdata_filepath %like% ".rds"){
    microdata <- readRDS(microdata_filepath)  
  } else if(microdata_filepath %like% "csv") {
    microdata <- fread(microdata_filepath)
  } else{
    stop("Microdata needs to be saved in .csv or .rds format")
  }
  
  XMIN = quantile(microdata[, .(min = min(data)), by = .(nid, location_id, year_id)]$min, 0.01)
  XMAX = quantile(microdata[, .(max = max(data)), by = .(nid, location_id, year_id)]$max, 0.99)
  
  min_allowed_sd = 0.50 * min(microdata[, .(sd = sd(data)), by = .(nid, location_id, year_id)]$sd)
  max_allowed_sd = 1.50 * max(microdata[, .(sd = sd(data)), by = .(nid, location_id, year_id)]$sd)
  
  write.csv(microdata, file.path(outdir, var, version, "microdata.csv"), row.names = F, na = "")
  
  rm(microdata)
  
  
} else {
  
  args <- commandArgs(trailingOnly = TRUE)
  param_map_filepath <- args[1]
  
  ## Retrieving array task_id
  task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
  param_map <- fread(param_map_filepath)
  
  decomp_step = param_map[task_id, decomp_step]
  gbd_round_id = param_map[task_id, gbd_round_id]
  var = param_map[task_id, var]
  version = param_map[task_id, version]
  
  mean_meid = param_map[task_id, mean_meid]
  prev_meids = param_map[task_id, prev_meids]
  min_allowed_sd = param_map[task_id, min_allowed_sd]
  max_allowed_sd = param_map[task_id, max_allowed_sd]
  XMIN = param_map[task_id, XMIN]
  XMAX = param_map[task_id, XMAX]
  
  thresholds = param_map[task_id, thresholds]
  threshold_weights = param_map[task_id, threshold_weights]
  outdir = param_map[task_id, outdir]
  num_draws = param_map[task_id, num_draws]
  
  location_ids = param_map[task_id, location_ids]
  year_ids = param_map[task_id, year_ids]
  sex_ids = param_map[task_id, sex_ids]
  age_group_ids = param_map[task_id, age_group_ids]
  
}

print(file.path(outdir, var, version, "weights.csv"))
weights <- fread(file.path(outdir, var, version, "weights.csv"))

if(thresholds %like% ","){thresholds <- as.numeric(unlist(strsplit(thresholds, split=",")))}
if(threshold_weights %like% ","){threshold_weights <- as.numeric(unlist(strsplit(threshold_weights, split=",")))}
if(prev_meids %like% ","){prev_meids <- as.numeric(unlist(strsplit(prev_meids, split=",")))}
meids <- c(mean_meid, prev_meids)

# Demographics
if(is.na(location_ids)){
  location_ids<-get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id)$location_id
}
if(is.na(sex_ids)){
  sex_ids<-get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id)$sex_id
}
if(is.na(age_group_ids)){
  age_group_ids<-get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id)$age_group_id
}
if(is.na(year_ids)){
  year_ids<-get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id)$year_id
}


# ----- 

print(Sys.time())
message("Creating Mean & Prev Param List")
meanprev_list <- createMeanPrevList(meids = meids, 
                                    location_ids = location_ids, sex_ids = sex_ids, year_ids = year_ids, age_group_ids = age_group_ids,
                                    num_draws = num_draws, decomp_step = decomp_step, gbd_round_id = gbd_round_id,
                                    date=date)
# -----

print(Sys.time())
message("Optimizing Standard Deviations")

meanprev_list <- getOptimalSDs(meanprev_list = meanprev_list,
                               mean_meid = mean_meid,
                               prev_meids = prev_meids,
                               min_allowed_sd = min_allowed_sd, 
                               max_allowed_sd = max_allowed_sd, 
                               weights = weights, 
                               thresholds = thresholds,
                               threshold_weights = threshold_weights, 
                               XMIN = XMIN, 
                               XMAX = XMAX)

meanprev_list <- rbindlist(meanprev_list)
meanprev_list[, var := var]
meanprev_list[, mean := get(paste0("meid_", mean_meid))]

sd_list <- meanprev_list[,.(location_id,sex_id,year_id,age_group_id,draw,sd)]
sd_list <- dcast(sd_list,location_id+sex_id+year_id+age_group_id ~ draw, value.var="sd")

print(Sys.time())
message("Saving")

label = "meanSD"

if(length(location_ids)==1){label <- paste0(label, "_", location_ids)}
if(length(age_group_ids)==1){label <- paste0(label, "_", age_group_ids)}
if(length(sex_ids)==1){label <- paste0(label, "_", sex_ids)}
if(length(year_ids)==1){label <- paste0(label, "_", year_ids)}

dir.create(file.path(outdir, var, version, "meanSD"))
saveRDS(sd_list, file = file.path(outdir, var, version, "meanSD", paste0(label, ".rds")))
message("File Saved")
