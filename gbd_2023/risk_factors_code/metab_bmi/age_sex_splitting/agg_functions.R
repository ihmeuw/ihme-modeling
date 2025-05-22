################################################################################
## DESCRIPTION: Aggregating functions for ST-GPR model results across global, super-region, and region
## INPUTS: run_id of STGPR models ##
## OUTPUTS: CSVs of aggregated age-pattern results ##
## AUTHOR: 
## DATE: 
################################################################################

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "FILEPATH"

# Base filepaths
share_dir <- 'FILEPATH' # only accessible from the cluster
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""
work_dir <- paste0(code_dir, "FILEPATH")
save_dir <- "FILEPATH"

## LOAD DEPENDENCIES
source(paste0("FILEPATH"))
source(paste0("FILEPATH"))
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_population.R")
source("FILEPATH")
library(rhdf5)

# Load helper datasets
locs <- get_location_metadata(22, release_id = 16)
pops <- get_population(age_group_id = c(34, 6:20, 30:32, 235), year_id = c(1980:2022), sex_id = c(1,2), location_id = unique(locs[level == 3, location_id]),
                       release_id = 16)[,run_id := NULL]
reg_pops <- get_population(age_group_id = c(34, 6:20, 30:32, 235), year_id = c(1980:2022), sex_id = c(1,2), location_id = unique(locs[level == 2, location_id]),
                           release_id = 16)[,run_id := NULL]
setnames(reg_pops, c("location_id", "population"), c("region_id", "reg_population"))
sr_pops <- get_population(age_group_id = c(34, 6:20, 30:32, 235), year_id = c(1980:2022), sex_id = c(1,2), location_id = unique(locs[level == 1, location_id]),
                          release_id = 16)[,run_id := NULL]
setnames(sr_pops, c("location_id", "population"), c("super_region_id", "sr_population"))
glb_pops <- get_population(age_group_id = c(34, 6:20, 30:32, 235), year_id = c(1980:2022), sex_id = c(1,2), location_id = 1,
                           release_id = 16)[,run_id := NULL]
setnames(glb_pops, c("location_id", "population"), c("glb_id", "glb_population"))


# Aggregating functions
reg_agg <- function(df, year_bins=NULL){
  
  # Some merging
  dt <- copy(df)
  dt <- dt[location_id %in% unique(locs[level == 3, location_id])]
  dt <- merge(dt, pops, by = c("location_id", "age_group_id", "year_id", "sex_id"))
  dt <- merge(dt, locs[,.(location_id, region_id)], by = "location_id")
  dt <- merge(dt, reg_pops, by = c("region_id", "age_group_id", "year_id", "sex_id"))
  
  # weighted averages
  dt[,reg_mean := lapply(.SD, function(x) sum(x*population/reg_population)), 
     by = c("region_id", "age_group_id", "year_id", "sex_id"), .SDcols = "val"]
  dt <- unique(dt[,.(region_id, age_group_id, year_id, sex_id, reg_mean)])
  
  # After calculating weighted regional averages, calculate average regional mean across year bins
  if(!is.null(year_bins)){
    if(year_bins == 10){
      dt[year_id %in% c(1980:1989), year_bin := "1980-1989"]
      dt[year_id %in% c(1990:1999), year_bin := "1990-1999"]
      dt[year_id %in% c(2000:2009), year_bin := "2000-2009"]
      dt[year_id %in% c(2010:2022), year_bin := "2010-2022"]
      
      dt[, reg_mean_binned := mean(reg_mean), by = c("region_id", "age_group_id", "year_bin", "sex_id")]
      dt <- unique(dt[,.(region_id, age_group_id, year_bin, sex_id, reg_mean_binned)])
    }
    
    if(year_bins == 20){
      dt[year_id %in% c(1980:1999), year_bin := "1980-1999"]
      dt[year_id %in% c(2000:2022), year_bin := "2000-2022"]
      
      dt[, reg_mean_binned := mean(reg_mean), by = c("region_id", "age_group_id", "year_bin", "sex_id")]
      dt <- unique(dt[,.(region_id, age_group_id, year_bin, sex_id, reg_mean_binned)])
    }
    
    if(!year_bins %in% c(10, 20)){
      warning(message("Current code doesn't support year bins specified. No aggregation is being done."))
    }
  }
  return(dt)
}

sr_agg <- function(df, year_bins=NULL){
  
  # Some merging
  dt <- copy(df)
  dt <- dt[location_id %in% unique(locs[level == 3, location_id])]
  dt <- merge(dt, pops, by = c("location_id", "age_group_id", "year_id", "sex_id"))
  dt <- merge(dt, locs[,.(location_id, super_region_id)], by = "location_id")
  dt <- merge(dt, sr_pops, by = c("super_region_id", "age_group_id", "year_id", "sex_id"))
  
  # weighted averages
  dt[,sr_mean := lapply(.SD, function(x) sum(x*population/sr_population)), 
     by = c("super_region_id", "age_group_id", "year_id", "sex_id"), .SDcols = "val"]
  dt <- unique(dt[,.(super_region_id, age_group_id, year_id, sex_id, sr_mean)])
  
  # After calculating weighted regional averages, calculate average regional mean across year bins
  if(!is.null(year_bins)){
    if(year_bins == 10){
      dt[year_id %in% c(1980:1989), year_bin := "1980-1989"]
      dt[year_id %in% c(1990:1999), year_bin := "1990-1999"]
      dt[year_id %in% c(2000:2009), year_bin := "2000-2009"]
      dt[year_id %in% c(2010:2022), year_bin := "2010-2022"]
      
      dt[, sr_mean_binned := mean(sr_mean), by = c("super_region_id", "age_group_id", "year_bin", "sex_id")]
      dt <- unique(dt[,.(super_region_id, age_group_id, year_bin, sex_id, sr_mean_binned)])
    }
    
    if(year_bins == 20){
      dt[year_id %in% c(1980:1999), year_bin := "1980-1999"]
      dt[year_id %in% c(2000:2022), year_bin := "2000-2022"]
      
      dt[, sr_mean_binned := mean(sr_mean), by = c("super_region_id", "age_group_id", "year_bin", "sex_id")]
      dt <- unique(dt[,.(super_region_id, age_group_id, year_bin, sex_id, sr_mean_binned)])
    }
    
    if(!year_bins %in% c(10, 20)){
      warning(message("Current code doesn't support year bins specified. No aggregation is being done."))
    }
    
  }
  return(dt)
}

glb_agg <- function(df, year_bins=NULL){
  
  # Some merging
  dt <- copy(df)
  dt <- dt[location_id %in% unique(locs[level == 3, location_id])]
  dt <- merge(dt, pops, by = c("location_id", "age_group_id", "year_id", "sex_id"))
  dt[,glb_id := 1]
  dt <- merge(dt, glb_pops, by = c("glb_id", "age_group_id", "year_id", "sex_id"))
  
  # weighted averages
  dt[,glb_mean := lapply(.SD, function(x) sum(x*population/glb_population)), 
     by = c("glb_id", "age_group_id", "year_id", "sex_id"), .SDcols = "val"]
  dt <- unique(dt[,.(glb_id, age_group_id, year_id, sex_id, glb_mean)])
  
  # After calculating weighted regional averages, calculate average regional mean across year bins
  if(!is.null(year_bins)){
    if(year_bins == 10){
      dt[year_id %in% c(1980:1989), year_bin := "1980-1989"]
      dt[year_id %in% c(1990:1999), year_bin := "1990-1999"]
      dt[year_id %in% c(2000:2009), year_bin := "2000-2009"]
      dt[year_id %in% c(2010:2022), year_bin := "2010-2022"]
      
      dt[, glb_mean_binned := mean(glb_mean), by = c("glb_id", "age_group_id", "year_bin", "sex_id")]
      dt <- unique(dt[,.(glb_id, age_group_id, year_bin, sex_id, glb_mean_binned)])
    }
    
    if(year_bins == 20){
      dt[year_id %in% c(1980:1999), year_bin := "1980-1999"]
      dt[year_id %in% c(2000:2022), year_bin := "2000-2022"]
      
      dt[, glb_mean_binned := mean(glb_mean), by = c("glb_id", "age_group_id", "year_bin", "sex_id")]
      dt <- unique(dt[,.(glb_id, age_group_id, year_bin, sex_id, glb_mean_binned)])
    }
    
    if(!year_bins %in% c(10, 20)){
      warning(message("Current code doesn't support year bins specified. No aggregation is being done."))
    }
    
  }
  return(dt)
}

cat_agg <- function(df, year_bins=NULL, super_regions = F){
  
  dt_age_std <- copy(df)
  dt_age_std <-dt_age_std[location_id %in% unique(locs[level == 3, location_id])]
  
  #####
  # Age-standardization
  source("FILEPATH/get_age_weights.R")
  age_weights <- get_age_weights(release_id=16)
  age_weights <- age_weights[age_group_id %in% unique(df$age_group_id)]
  age_weights[,age_weight := age_group_weight_value/(sum(age_weights$age_group_weight_value))][,age_group_weight_value := NULL]
  if(sum(age_weights$age_weight) != 1){
    stop(message("Something went wrong with rescaling the age weights"))
  }
  #####
  
  dt_age_std <- merge(dt_age_std, age_weights, by = "age_group_id")
  if(!is.null(year_bins)){
    dt_age_std[year_id %in% c(1980:1999), year_bin := "1980-1999"]
    dt_age_std[year_id %in% c(2000:2022), year_bin := "2000-2022"]
    dt_age_std[,age_std_val := lapply(.SD, function(x) sum(x*age_weight)), by = c("location_id", "sex_id", "year_id"), .SDcols = "val"]
    dt_age_std[,age_std_val_year_binned := mean(age_std_val), by = c("location_id", "sex_id", "year_bin")]
    dt_age_std <- unique(dt_age_std[,.(location_id, year_bin, sex_id, age_std_val_year_binned)])
    
    tert_1_1980 <- quantile(dt_age_std[year_bin == "1980-1999"]$age_std_val_year_binned, probs = (0.33))
    print(paste0("The 33rd percentile of the age-standardized value from 1980-1999 is ", tert_1_1980))
    
    tert_2_1980 <- quantile(dt_age_std[year_bin == "1980-1999"]$age_std_val_year_binned, probs = (0.66))
    print(paste0("The 66th percentile of the age-standardized value from 1980-1999 is ", tert_2_1980))
    
    tert_1_2000 <- quantile(dt_age_std[year_bin == "2000-2022"]$age_std_val_year_binned, probs = (0.33))
    print(paste0("The 33rd percentile of the age-standardized value from 2000-2022 is ", tert_1_2000))
    
    tert_2_2000 <-quantile(dt_age_std[year_bin == "2000-2022"]$age_std_val_year_binned, probs = (0.66))
    print(paste0("The 66th percentile of the age-standardized value from 2000-2022 is ", tert_2_2000))
    
    
    dt_age_std[age_std_val_year_binned <= tert_1_1980 & year_bin == "1980-1999", weight_tert := 1]
    dt_age_std[age_std_val_year_binned > tert_1_1980 & age_std_val_year_binned <= tert_2_1980 & year_bin == "1980-1999", weight_tert := 2]
    dt_age_std[age_std_val_year_binned > tert_2_1980 & year_bin == "1980-1999", weight_tert := 3]
    
    dt_age_std[age_std_val_year_binned <= tert_1_2000 & year_bin == "2000-2022", weight_tert := 1]
    dt_age_std[age_std_val_year_binned > tert_1_2000 & age_std_val_year_binned <= tert_2_2000 & year_bin == "2000-2022", weight_tert := 2]
    dt_age_std[age_std_val_year_binned > tert_2_2000 & year_bin == "2000-2022", weight_tert := 3]
    
    loc_sex_year_tert <- unique(dt_age_std[,.(location_id, sex_id, year_bin, weight_tert)]) # only keep the location, sex, year bin, and weight tertile
    
  }
  
  if(is.null(year_bins)) {
    warning(message("Calculating average across all years."))
    dt_age_std[,age_std_val := lapply(.SD, function(x) sum(x*age_weight)), by = c("location_id", "sex_id", "year_id"), .SDcols = "val"]
    dt_age_std[,age_std_val_year_binned := mean(age_std_val), by = c("location_id", "sex_id")]
    dt_age_std <- unique(dt_age_std[,.(location_id, sex_id, age_std_val_year_binned)])
    
    tert_1 <- quantile(dt_age_std$age_std_val_year_binned, probs = (0.33))
    print(paste0("The 33rd percentile of the age-standardized value is ", tert_1))
    
    tert_2 <- quantile(dt_age_std$age_std_val_year_binned, probs = (0.66))
    print(paste0("The 66th percentile of the age-standardized value is ", tert_2))
    
    dt_age_std[age_std_val_year_binned <= tert_1 , weight_tert := 1]
    dt_age_std[age_std_val_year_binned > tert_1 & age_std_val_year_binned <= tert_2, weight_tert := 2]
    dt_age_std[age_std_val_year_binned > tert_2, weight_tert := 3]
    
    loc_sex_tert <- unique(dt_age_std[,.(location_id, sex_id, weight_tert)]) # only keep the location, sex and weight tertile
    
  }

  #########
  # Age-pattern by weight tertile
  #########
  
  dt_ap <- copy(df)
  dt_ap <- dt_ap[location_id %in% unique(locs[level == 3]$location_id)]
  dt_ap <- merge(dt_ap, locs[,.(location_id, super_region_id)], by = "location_id")
  if(!is.null(year_bins)){
    dt_ap[year_id %in% c(1980:1999), year_bin := "1980-1999"]
    dt_ap[year_id %in% c(2000:2022), year_bin := "2000-2022"]
    
    dt_ap <- merge(dt_ap, loc_sex_year_tert, by = c("location_id", "sex_id", "year_bin"))
    dt_ap <- merge(dt_ap, pops, by = c("location_id", "year_id", "sex_id", "age_group_id"))
    dt_ap[,group_pop := sum(population), by = c("year_id", "sex_id", "age_group_id", "weight_tert")]
    # Calculate the weighted average of value (age, sex, year, and tertile specific)
    dt_ap[, weight_tert_val := lapply(.SD, function(x) sum(x*population/group_pop)), 
          by = c("year_id", "sex_id", "age_group_id", "weight_tert"), .SDcols = "val"] 
    # Calculate the average of value across year bin
    dt_ap[, weight_tert_val_binned := mean(weight_tert_val), by = c("year_bin", "sex_id", "age_group_id", "weight_tert")]
    
    dt_ap <- unique(dt_ap[,.(weight_tert, age_group_id, sex_id, year_bin, weight_tert_val_binned)])
    out <- list(dt_ap, loc_sex_year_tert)
  } 
  
  if(is.null(year_bins)) {
    warning(message("Calculating average across all years."))
    dt_ap <- merge(dt_ap, loc_sex_tert, by = c("location_id", "sex_id"))
    dt_ap <- merge(dt_ap, pops, by = c("location_id", "year_id", "sex_id", "age_group_id"))
    dt_ap[, group_pop := sum(population), by = c("year_id", "sex_id", "age_group_id", "weight_tert")]
    dt_ap[, sr_population := sum(population), by = c("year_id", "sex_id", "age_group_id", "weight_tert", "super_region_id")]
    
    if(super_regions == F){
      # Calculate the weighted average of value (age, sex, year, and tertile specific)
      dt_ap[, weight_tert_val := lapply(.SD, function(x) sum(x*population/group_pop)), 
            by = c("year_id", "sex_id", "age_group_id", "weight_tert"), .SDcols = "val"] 
      
      # Calculate the average of value across all years
      dt_ap[, weight_tert_val_binned := mean(weight_tert_val), by = c("sex_id", "age_group_id", "weight_tert")]
      dt_ap <- unique(dt_ap[,.(weight_tert, age_group_id, sex_id, weight_tert_val_binned)])
      out <- list(dt_ap, loc_sex_tert)
      
    }

    if(super_regions == T){
      # Calculate the weighted average of value (age, sex, year, and tertile specific)
      dt_ap[, weight_tert_val := lapply(.SD, function(x) sum(x*population/group_pop)), 
            by = c("year_id", "sex_id", "age_group_id", "weight_tert"), .SDcols = "val"] 
      
      # Calculate the average of value across all years
      dt_ap[, weight_tert_val_binned := mean(weight_tert_val), by = c("sex_id", "age_group_id", "weight_tert")]
      
      # Calculate the weighted average of value across super-region
      dt_ap[, sr_weight_tert := lapply(.SD, function(x) sum(x*population/sr_population)),
            by = c("super_region_id", "sex_id", "age_group_id", "year_id", "weight_tert"), .SDcols = "val"]
      
      # Calculate the weighted average of value across super-region, all years
      dt_ap[, sr_weight_tert_binned := mean(sr_weight_tert), by = c("sex_id", "age_group_id", "weight_tert", "super_region_id")]
      
      dt_ap_sr <- unique(dt_ap[,.(weight_tert, super_region_id, age_group_id, sex_id, weight_tert_val_binned, sr_weight_tert_binned)])
      dt_ap <- unique(dt_ap[,.(weight_tert, age_group_id, sex_id, weight_tert_val_binned)])
      out <- list(dt_ap, dt_ap_sr, loc_sex_tert)
    }
    
  }
  
  return(out)

}

