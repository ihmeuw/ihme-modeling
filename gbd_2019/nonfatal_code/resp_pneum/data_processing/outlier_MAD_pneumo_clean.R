################################################################################################################################################################
### Author: USERNAME, modified by USERNAME
### Date: 6/13/2018
### Description: Accepts data as Excel spreadsheet as output from epi_dataversionreview.do, identifies data-series (defined by sex, location, year and NID) 
###              for which all observed values are 0 and those for which the age-adjusted mean is more than xMADs above or below median, marks them as outliers, 
###              exports file that can be uploaded to Epi DB.  Can specify which sources to include in automated process, but cannot include literature (which is 
###              usually less abundant and should be visually outliered)
### Based on: GBS hospital data prep code from USERNAME (12/2017)
##################################################################################################################################################################
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

functions_dir <- paste0("FILEPATH")
temp_dir <- paste0("FILEPATH")
date <- Sys.Date()
date <- gsub("-", "_", date)

## Source central functions
source(paste0(j, lib_path, "get_age_metadata.R"))

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


dt <- as.data.table(read.csv("FILEPATH"))
dt$note_modeler <- ""

byvars <- c("location_id", "sex", "year_start", "year_end", "nid")

output_filepath <- ("FILEPATH")


## GET AGE WEIGHTS
all_fine_ages <- as.data.table(get_age_metadata(age_group_set_id=12))
not_babies <- all_fine_ages[!age_group_id %in% c(2:4)]
not_babies[, age_end := age_group_years_end-1]
group_babies <- all_fine_ages[ c(1:3)]
group_babies[, age_end := 0.999]
all_ages <- rbind(not_babies, group_babies)  
all_ages[, age_start := age_group_years_start]
all_ages[, c("age_group_years_start", "age_group_years_end") := NULL]


## OUTLIERING
  ##take inp data to be reviewed out of the main dataset
  dt_final <- copy(dt)
  dt_final <- dt_final[dt_final$clinical_data_type!="inpatient", ]
  
  ##make a set with only the inpatient data
  dt_inp <- copy(dt)
  dt_inp <- dt_inp[dt_inp$clinical_data_type=="inpatient",]
  
  ##merge age table map and merge on to dataset
  dt_inp <- merge(dt_inp, all_ages, by = c("age_start", "age_end"), all.x = T)
  
  #calculate age-standardized prevalence/incidence
  
  ##create new age-weights for each data source
  dt_inp <- dt_inp[, sum := sum(age_group_weight_value), by = byvars] #sum of standard age-weights for all the ages we are using, by location-age-sex-nid, sum will be same for all age-groups and possibly less than one 
  dt_inp <- dt_inp[, new_weight := age_group_weight_value/sum, by = byvars] #divide each age-group's standard weight by the sum of all the weights in their locaiton-age-sex-nid group 
  
  ##age standardizing per location-year by sex
  #add a column titled "age_std_mean" with the age-standardized mean for the location-year-sex-nid
  dt_inp[, as_mean := mean * new_weight] #initially just the weighted mean for that AGE-location-year-sex-nid
  dt_inp[, as_mean := sum(as_mean), by = byvars] #sum across ages within the location-year-sex-nid group, you now have age-standardized mean for that series
  
  ##mark as outlier if age standardized mean is 0 (either biased data or rare disease in small ppln)
  dt_inp[as_mean == 0, is_outlier := 1] 
  dt_inp$note_modeler <- ""
  dt_inp[as_mean == 0, note_modeler := paste0(note_modeler, " | outliered this location-year-sex-NID age-series because age standardized mean is 0")]
  
  ## log-transform to pick up low outliers
  dt_inp[as_mean != 0, as_mean := log(as_mean)]
  
  # calculate median absolute deviation
  dt_inp[as_mean == 0, as_mean := NA] ## don't count zeros in median calculations
  dt_inp[,mad:=mad(as_mean,na.rm = T),by=c("sex")]
  dt_inp[,median:=median(as_mean,na.rm = T),by=c("sex")]
  
  #***can change number of MAD to mark here
  dt_inp[as_mean>((2.0*mad)+median), is_outlier := 1]
  dt_inp[as_mean>((2.0*mad)+median), note_modeler := paste0(note_modeler, " | outliered because age-standardized mean for location-year-sex-NID is higher than 2 MAD above median")]
  dt_inp[as_mean<(median-(2.0*mad)), is_outlier := 1]
  dt_inp[as_mean<(median-(2.0*mad)), note_modeler := paste0(note_modeler, " | outliered because log age-standardized mean for location-year-sex-NID is lower than 2 MAD below median")]
  dt_inp[, c("sum", "new_weight", "as_mean", "median", "mad", "age_group_weight_value", "age_group_id") := NULL]
  
  dt_final <- rbind(dt_final, dt_inp, fill = FALSE)

## SEND TO REVIEW FOLDER
write.csv(dt_final, "FILEPATH", row.names = F)

