##################################################################################
# Name of Script: 02_cw_inputs.R
# Date: 4/19/2024

# Can start at this step if CoD data is from the most recent refresh
#
# Output: Two prepped data files for input to crosswalk: 
#             1) overlap_one.csv
#             2) overlap_two.csv
#
# Contributors: INDIVIDUAL_NAME
###################################################################################

# Load required libraries
pacman::p_load(data.table, dplyr, tidyr, tidyverse, haven, readxl, reshape2,
               crayon)

library(ggplot2)
library(Cairo)
# define drive paths
if (Sys.info()["sysname"] == 'Linux'){
  j <- "FILEPATH"
  h <- paste0("FILEPATH",Sys.info()[7],"/")
} else if (Sys.info()["sysname"] == 'Darwin'){
  j <- "FILEPATH"
  h <- paste0("FILEPATH",Sys.info()[7],"/")
} else {
  j <- "J:/"
  h <- "H:/"
}

# Source required shared functions
source("FILEPATH/get_location_metadata.R")

location_set = 35
release <- 16

#get locations
locs <- get_location_metadata(location_set_id=location_set, release_id = release)

# Read in CoD data that has been saved after most recent refresh
gbd_2023_part1 <- fread("FILEPATH")

gbd_2023_part2 <- fread("FILEPATH")

gbd_2023_rec <- rbind(gbd_2023_part1, gbd_2023_part2)

#pull in outliers
cod_outliers <- fread("FILEPATH")

# merge in is_outlier column
gbd_2023_rec <- merge(gbd_2023_rec, 
                      cod_outliers[, .(nid, source_id, location_id, year_id, age_group_id, sex_id, site_id, cause_id, is_outlier)], 
                      by = c("nid", "source_id" , "location_id", "year_id", "age_group_id", "sex_id", "site_id", "cause_id"), 
                      all.x = TRUE)


gbd_2023_rec$data_type_name <- "VR"

# OR Read in CoD data that has been saved from step 01
#gbd_2023_rec <- fread("FILEPATH")

# merge in is_estimate column
gbd_2023_rec <- merge(gbd_2023_rec, locs[,.(location_id, is_estimate)], by = "location_id", all.x=T)


gbd_2023_rec <- gbd_2023_rec[is_estimate == 1]


cr_mort_rec <- fread("FILEPATH")

#Subset to CoD columns needed
cod_dt23 <- gbd_2023_rec[,.(location_id, year_id, age_group_id, sex_id, cause_id, cf_raw, cf_final=cf, sample_size,
                           data_type_name, is_outlier)]

#Subset to CR columns needed
cr_dt23 <- cr_mort_rec[,.(location_id, year_id, age_group_id, sex_id, cause_id, cf_raw, cf_final, sample_size)]

#Separate out VR data
  cod_vr <- cod_dt23[ which(cod_dt23$data_type_name=='VR'), ]
#fill in missing is_outlier values with 0:
  cod_vr[is.na(is_outlier), is_outlier := 0]
#Remove outliers from VR data because these should not be used as gold standard
  cod_vr <- cod_vr[ which(cod_vr$is_outlier== 0), ]
  
  # Rename CF columns before merging
  setnames(cr_dt23, old = "cf_final", new = "cf_cr")
  setnames(cod_vr, old = "cf_final", new = "cf_vr")
  
  #rename sample_size for CR and VR sample size
  setnames(cr_dt23, old = "sample_size", new = "sample_cr")
  setnames(cod_vr, old = "sample_size", new = "sample_vr")
  
  #subset for direct comparison
  cr_merge <- setDT(subset(cr_dt23, select = c("age_group_id", "location_id", "sex_id", 
                                        "year_id", "cause_id", "cf_cr", "sample_cr")))
  vr_merge <- setDT(subset(cod_vr, select = c("age_group_id", "location_id", "sex_id", 
                                            "year_id", "cause_id", "cf_vr", "sample_vr")))
  #merge VR and CR data
  vr_cr_merge <- merge(x = vr_merge, y = cr_merge,
                       by = c("age_group_id", "location_id", "sex_id", "year_id", "cause_id"),
                       all = TRUE)


  vr_cr_merge[, `:=`(has_cr = any(!is.na(cf_cr)), has_vr = any(!is.na(cf_vr))), 
             by = .(cause_id, sex_id, age_group_id, location_id)]


  cod_data <- vr_cr_merge[has_cr & has_vr]
  
#Create variable for where the is a CR and VR match  
  cod_data <- cod_data[is.na(cf_cr)|is.na(cf_vr), has_match := 0]
  cod_data <- cod_data[!is.na(cf_cr)&!is.na(cf_vr), has_match := 1]


  years_overlap <- cod_data[, years := sum(has_match, na.rm = TRUE), by = .(cause_id, sex_id, age_group_id, location_id)]


  years_overlap <- years_overlap %>%
  mutate(overlap_flag = ifelse(years >= 2, 1, 0))
  

  dt_subset1 <- years_overlap[overlap_flag == 1]

  fwrite(dt_subset1, "FILEPATH/overlap_two.csv")
  

  dt_subset2 <- years_overlap[overlap_flag == 0]


  fwrite(cod_data, "FILEPATH/overlap_one.csv")
