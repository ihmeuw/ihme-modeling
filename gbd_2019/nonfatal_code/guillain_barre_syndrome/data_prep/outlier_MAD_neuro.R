###########################################################
### Author: USER
### Description: Accepts data as Excel spreadsheet as output from epi_dataversionreview.do, identifies data-series (defined by sex, location, year and NID) for which all observed values are 0 and those for which the age-adjusted mean is more than xMADs above or below median, marks them as outliers, exports file that can be uploaded to Epi DB.  Can specify which sources to include in automated process, but cannot include literature (which is usually less abundant and should be visually outliered)
### Based on: GBS hospital data prep code from USER (12/2017)
### Modified so that a series is outliered by super region (lumping high-income and Eastern Europe, Central Europe & Asia together)
###########################################################
#***Marks user input needed; best to use Find for all ***


rm(list=ls())

##working environment 

os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- "FILEPATH"
  j<- "FILEPATH"
  h<-"FILEPATH"
  scratch <-"FILEPATH"
} else {
  lib_path <- "FILEPATH"
  j<- "FILEPATH"
  h<-"FILEPATH"
  scratch <- "FILEPATH"
}

#set object

functions_dir <- "FILEPATH"
temp_dir <- paste0(j, "FILEPATH")
date <- Sys.Date()
date <- gsub("-", "_", date)


## Source central functions
source(paste0(lib_path, "get_age_metadata.R"))

library("data.table")
library("ggplot2")
library("readr")
library("RMySQL")
library("openxlsx")
library("readxl")
library("stringr")
library("tidyr")
library("plyr")
library("dplyr")


## SET OBJECTS
#age_using <- c(2:20, 30:32, 235) #this can be modified
age_using <- c(2:3,388, 389, 238, 34,6:20, 30:32, 235 ) # new in GBD 2020

byvars <- c("location_id", "sex", "year_start", "year_end", "nid") ##same as USER

## INPUT DATA
dt <- as.data.table(read.xlsx("FILEPATH"))

output_filepath <- ("FILEPATH")


## GET AGE WEIGHTS
all_fine_ages <- as.data.table(get_age_metadata(age_group_set_id=19, gbd_round_id=7))
all_fine_ages[, age_start := age_group_years_start]


## OUTLIERING
  ##make a set to be run through outlier script
  dt_inp <- copy(dt)
  
  ##merge age table map and merge on to dataset
  dt_inp <- merge(dt_inp, all_fine_ages, by = c("age_start"), all.x = T)
  
  #calculate age-standardized prevalence/incidence
  
  ##create new age-weights for each data source
  dt_inp <- dt_inp[, sum := sum(age_group_weight_value), by = byvars] #sum of standard age-weights for all the ages we are using, by location-age-sex-nid, sum will be same for all age-groups and possibly less than one 
  dt_inp <- dt_inp[, new_weight := age_group_weight_value/sum, by = byvars] #divide each age-group's standard weight by the sum of all the weights in their location-age-sex-nid group 
  
  ##age standardizing per location-year by sex
  #add a column titled "age_std_mean" with the age-standardized mean for the location-year-sex-nid
  dt_inp[, as_mean := mean * new_weight] #initially just the weighted mean for that AGE-location-year-sex-nid
  dt_inp[, as_mean := sum(as_mean), by = byvars] #sum across ages within the location-year-sex-nid group, you now have age-standardized mean for that series
  
  #add super-region to data table and combine high-income and C/E Europe and C Asia due to small number of studies
  locs <- read.csv("FILEPATH")
  dt_inp <- join(dt_inp, locs[,c("location_id","super_region_name")], by="location_id")
  dt_inp[super_region_name=="Central Europe, Eastern Europe, and Central Asia", super_region_name:= "High-income"]
  
  ##mark as outlier if age standardized mean is 0 (either biased data or rare disease in small ppln)
  dt_inp[as_mean == 0, is_outlier := 1] 
  dt_inp[as_mean == 0, note_modeler := paste0(note_modeler, " | outliered this location-year-sex-NID age-series because age standardized mean is 0")]
  
  ## log-transform to pick up low outliers
  dt_inp[as_mean != 0, as_mean := log(as_mean)]
  
  # calculate median absolute deviation
  dt_inp[as_mean == 0, as_mean := NA] ## don't count zeros in median calculations
  dt_inp[,mad:=mad(as_mean,na.rm = T),by=c("sex", "super_region_name")]
  dt_inp[,median:=median(as_mean,na.rm = T),by=c("sex", "super_region_name")]
  
  
  #***can change number of MAD to mark here
  dt_inp[as_mean>((2.0*mad)+median), is_outlier := 1]
  dt_inp[as_mean>((2.0*mad)+median), note_modeler := paste0(note_modeler, " | outliered because age-standardized mean for location-year-sex-NID is higher than 2 MAD above median")]
  dt_inp[as_mean<(median-(2.0*mad)), is_outlier := 1]
  dt_inp[as_mean<(median-(2.0*mad)), note_modeler := paste0(note_modeler, " | outliered because log age-standardized mean for location-year-sex-NID is lower than 2 MAD below median")]
  dt_inp[, c("sum", "new_weight", "as_mean", "median", "mad", "age_group_weight_value", "age_group_id", "super_region_name") := NULL]
  


## SEND TO REVIEW FOLDER
write.xlsx(dt_inp, output_filepath)

