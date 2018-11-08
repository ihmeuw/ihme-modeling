###########################################################
### Author: 
### Date: 12/23/17
### Project: Upload GBS Hospital Data 2017 with outliers
### Purpose: GBD 2017 Nonfatal Analysis
###########################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "/home/j/" 
  h_root <- "/homes/USERNAME/"
} else { 
  j_root <- "J:/"
  h_root <- "H:/"
}

pacman::p_load(data.table, ggplot2, readr, RMySQL, boot)
library("openxlsx", lib.loc = paste0(j_root, FILEPATH))
library("readxl", lib.loc = paste0(j_root, FILEPATH))
library("dbplyr", lib.loc = paste0(j_root, FILEPATH))
library("meta", lib.loc = paste0(j_root, FILEPATH))

## SET OBJECTS
temp_dir <- paste0(j_root, FILEPATH)
functions_dir <- paste0(j_root, FILEPATH)
date <- gsub("-", "_", Sys.Date())
byvars <- c("location_id", "sex", "year_start", "year_end", "nid")

## SOURCE FUNCTIONS
source(paste0(functions_dir, "upload_epi_data.R"))
source(paste0(functions_dir, "get_epi_data.R"))

## USER FUNCTIONS
col_order <- function(data.table){
  dt <- copy(data.table)
  epi_order <- fread(paste0(temp_dir, FILEPATH), header = F)
  epi_order <- tolower(as.character(epi_order[, V1]))
  names_dt <- tolower(names(dt))
  setnames(dt, names(dt), names_dt)
  for (name in epi_order){
    if (name %in% names(dt) == F){
      dt[, c(name) := ""]
    }
  }
  extra_cols <- setdiff(names(dt), tolower(epi_order))
  new_epiorder <- c(epi_order, extra_cols)
  setcolorder(dt, new_epiorder)
  return(dt)
}

adjustment <- function(data){
  dt <- copy(data)
  dt[, var_mean := standard_error^2]
  dt[, var_sr := sr_pooled_se^2]
  dt[, var := var_mean * var_sr + var_sr * mean^2 + var_mean * sr_pooled^2]
  dt[, standard_error := sqrt(var)]
  dt[, mean := mean * sr_pooled]
  dt[, c("var_mean", "var_sr", "var") := NULL]
  dt[, uncertainty_type := "Standard error"]
  dt[, note_modeler := paste0(note_modeler, " | SR adjustment by ", round(sr_pooled,3))]
  return(dt)
}

correction_cols <- function(){
  corrects <- c("inpt_primarydx_admit_rate", "inpt_primarydx_ind_rate", "inpt_anydx_ind_rate", 
                "encounter_anydx_ind_rate", "encounter_anydx_ind_rate_mod")
  lowers <- paste0("lower_", corrects)
  uppers <- paste0("upper_", corrects)
  factors <- c(paste0("correction_factor_", 1:3), "correction_factor_3_mod")
  delete_cols <- c(corrects, lowers, uppers, factors)
  return(delete_cols)
}

mark_outliers <- function(dt){
  outlier_dt <- as.data.table(fread(paste0(j_root, FILEPATH)))
  outlier_dt[, flag := 1]
  dt <- merge(dt, outlier_dt, by = c("nid", "location_id", "sex", "year_start", "year_end"), all.x = T)
  dt[flag == 1, is_outlier := 1]
  dt[flag == 1, note_modeler := paste0(note_modeler, " | ", note)]
  dt[, c("flag", "note") := NULL]
  return(dt)
}

upload_inp_hospital <- function(bundle, acause, version, correction, drop_imputed){
  print(paste0("uploading cause ", acause))
  dt <- as.data.table(read_excel(paste0(j_root, FILEPATH)))
  if (drop_imputed == 0) {
    other_means <- c("inpt_primarydx_ind_rate", "inpt_anydx_ind_rate", "encounter_anydx_ind_rate")
    dt[inpt_primarydx_admit_rate == 0, (other_means) := list(0, 0, 0)]
  }
  dt[, mean := get(paste0(correction))/ haqi_cf]
  dt[, lower := get(paste0("lower_", correction))/ haqi_cf]
  dt[, upper := get(paste0("upper_", correction))/ haqi_cf]
  delete_cols <- correction_cols()
  dt[, c(delete_cols) := NULL]
  dt[, cv_hospital := 1]
  dt <- dt[!is.na(mean)]
  dt[, note_modeler := paste0("version ", version, " correction ", correction, " with haqi_cf")]
  dt <- extremes(dt, c(2:20, 30:32, 235))
  dt <- adjustment(dt)
  dt <- mark_outliers(dt)
  dt <- col_order(dt)
  dt[, haqi_cf := NULL]
  write.xlsx(dt, paste0(j_root, FILEPATH), sheetName = "extraction")
  upload_epi_data(bundle_id = bundle, filepath = paste0(j_root, FILEPATH))
}

extremes <- function(dt, age_using){
  datasheet <- copy(dt)
  datasheet[age_end == 124, age_end := 99]
  datasheet <- datasheet[extractor == ""]
  datasheet <- datasheet[!is.na(mean)]
  
  ##merge age table map and merge on to dataset
  datasheet <- merge(datasheet, ages, by = c("age_start", "age_end"), all.x = T)
  
  #calculate age-standardized prevalence/incidence
  
  ##Format gbd standard age-weights
  weights <- copy(age_weights)
  weights <- weights[age_group_id %in% age_using,] ##switch to ages that apply to that cause
  
  ##create age_group_id 1
  age_group1 <- copy(weights)
  age_group1 <- age_group1[age_group_id %in% c(2:4)]
  age_group1 <- age_group1[ ,age_group_weight_value := sum(age_group_weight_value)]
  age_group1 <- unique(age_group1, by = "age_group_weight_value")
  age_group1[, age_group_id := 999]
  weights <- weights[!age_group_id %in% c(2:4)]
  weights <- rbind(weights, age_group1)
  
  datasheet <- merge(datasheet, weights, by = "age_group_id")
  
  ##create new age weights for each data source
  ##create weights for alternative age sets
  datasheet <- datasheet[, sum := sum(age_group_weight_value), by = byvars]
  datasheet <- datasheet[, new_weight := age_group_weight_value/sum, by = byvars]
  
  ##multiply prev/inc by age weight and sum over age
  ##add a column titled "age_std_mean" with the age-standardized mean for the location
  datasheet[, as_mean := mean * new_weight]
  datasheet[, as_mean := sum(as_mean), by = byvars]
  
  ## TRY LOG TRANSFORMATION
  datasheet[!as_mean == 0, as_mean := log(as_mean)]
  datasheet[as_mean == 0, is_outlier := 1]
  datasheet[as_mean == 0, note_modeler := paste0(note_modeler, " | outliered because age standardized mean is 0")]
  
  # calculate median aboluste deviation
  datasheet[as_mean == 0, as_mean := NA] ## don't count zeros in median calculations
  datasheet[,mad:=mad(as_mean,na.rm = T),by=c("sex")]
  datasheet[,median:=median(as_mean,na.rm = T),by=c("sex")]
  
  datasheet[as_mean>((1.5*mad)+median), is_outlier := 1]
  datasheet[as_mean>((1.5*mad)+median), note_modeler := paste0(note_modeler, " | outliered because is higher than 1.5 MAD above median")]
  datasheet[as_mean<(median-(1.5*mad)), is_outlier := 1]
  datasheet[as_mean<(median-(1.5*mad)), note_modeler := paste0(note_modeler, " | outliered because is higher than 1.5 MAD below median")]
  datasheet[, c("sum", "new_weight", "as_mean", "median", "mad", "age_group_weight_value", "age_group_id") := NULL]
  
  return(datasheet)
}

## GET AGE WEIGHTS AND AGE TABLE
con <- dbConnect(MySQL(), user=USER, password= PASSWORD, 
                 host = HOST, dbname= NAME)

source(paste0(j_root, FILEPATH))
source(paste0(j_root, FILEPATH))
age_row <- data.table(age_group_id = 999, age_start = 0, age_end = 0.999)
ages <- rbind(ages, age_row)
age_weights <- as.data.table(get_age_weights())
age_weights <- age_weights[gbd_round_id==5 & age_group_weight_description=="IHME standard age weight",]
age_weights <- age_weights[, .(age_group_id, age_group_weight_value)]

## GET CFR RATE 
cfr <- get_epi_data(bundle_id = 1379)
cfr[, survived := cases - deaths]
cfr[, `:=` (survived = round(survived, 0),
            cases = round(cases, 0))]
meta <- metaprop(data = cfr, event = survived, n = cases,
                 studylab = location_name, comb.random = T, level = .95)
random <- summary(meta)$random
sr_pooled <- inv.logit(random$TE)
sr_pooled_se <- sr_pooled * (1-sr_pooled) * random$seTE 

## GET SETTINGS
settings <- fread(paste0(temp_dir, FILEPATH))

## RUN HOSPITAL
run_hospital <- settings[run == 1 & hospital == 1]
mapply(upload_inp_hospital, bundle = run_hospital$bundle, acause = run_hospital$acause, version = run_hospital$version,
       correction = run_hospital$correction, drop_imputed = run_hospital$drop_imputed)
