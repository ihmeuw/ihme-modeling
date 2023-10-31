################
# 4.0. TMREL config
################

#Grab packages
library(dplyr)
library(tidyverse)
library(data.table)
library(plyr)
library(dplyr)
library(parallel)
library(ggplot2)

source('FILEPATH')
source('FILEPATH')
  
locs <- get_location_metadata(22, gbd_round_id = 7)
super_regions <- unique(locs[level == 2, c("region_id", "super_region_id")])
sex_name <- get_ids("sex")
age_name <- get_ids("age_group")
cause_names <- get_ids("cause")

#Set options for TMREL calculation
regions <- unique(locs[level == 3]$region_id)
sex  <- c(1,2)
ages <- c(8:20, 30:32, 235)

rr_version <- "fisher_information_boost"
loc_type <- c("region", "super_region")[1]

specific <- c("age_group_id", "location_id", "sex_id", "year_id")
same <- setdiff(c("location_id", "age_group_id", "sex_id", "year_id"), specific)

directory <- 'FILEPATH'
setwd(directory)

files <- list.files(directory)
causes <- as.numeric(unique(regmatches(files, regexpr("\\d{3}", files))))
cause_map <- fread('FILEPATH')
causes <- c(causes, unique(cause_map[cause_id_old == 696]$cause_id_new))
causes <- causes[!causes %in% c(696)] #parent cause
causes <- c(causes, c(522, 523, 524, 525, 971, 418, 419, 420, 1021, 996)) #cirrhosis and liver cancer redistribution
causes <- unique(causes)


lab <- ""
if ("age_group_id" %in% specific){
  lab <- paste0(lab, "age_")
}
if ("sex_id" %in% specific){
  lab <- paste0(lab, "sex_")
} 
if ("location_id" %in% specific){
  if (loc_type == "region"){ 
    lab <- paste0(lab, "region_")
  } else {
    lab <- paste0(lab, "superregion_")
  }
}
if ("year_id" %in% specific){
  lab <- paste0(lab, "year_")
}
