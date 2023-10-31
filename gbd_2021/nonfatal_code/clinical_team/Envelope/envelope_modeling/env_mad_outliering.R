####################################################################################################
## ENV_MAD_OUTLIERING.R
## Use MAD outliering technique for inpatient utilization envelope data prep 
## Description: Identifies data-series (defined by sex, location, year, NID) for which all observed values are 0 and those 
## for which the age-adjusted mean is more than xMADs above or below median, and marks them as outliers.
####################################################################################################

rm(list=ls())

library(data.table)
library(ggplot2)
library(readr)
library(RMySQL)
library(openxlsx)
library(readxl)
library(stringr)
library(tidyr)
library(plyr)
library(dplyr)

## -----------------------------------------------------------------------------
## SET UP & SOURCE FUNCTIONS
## Prep
functions_dir <- "FILEPATH"
temp_dir <- "FILEPATH"
date <- Sys.Date()
date <- gsub("-", "_", date)

## Source central functions
functions <- c("get_age_metadata","get_ids")
lapply(paste0(functions_dir, functions, ".R"), source)

## Set objects
age_using <- c(2:3,388,389,238,34,6:20,30:32,235) # most detailed GBD 2020 ages
byvars <- c("location_id", "sex", "year_start", "year_end", "nid") # variables used to identify data-series

## Read in data (post sex-splitting, crosswalking, age-splitting)
dt <- as.data.table(read.csv("FILEPATH"))

## Set filepath for output data - note this states x MAD in name
output_filepath <- paste0("FILEPATH", format(Sys.time(), "%Y_%m_%d_%H_%M"), ".csv")

## Assorted cleanup 
dt$X <- NULL
dt$age_group_id <- NULL

## Get age weights 
all_fine_ages <- as.data.table(get_age_metadata(age_group_set_id=19)) # age_group_set_id = 19 is most detailed for GBD 2020
all_fine_ages[, age_start := age_group_years_start]

## Delete rows with empty means
dt <- dt[!is.na(mean)]

## Some cleanup
dt$age_group_name <- NULL
dt$start_age_group_id <- NULL
dt[age_end == 100, age_end := 125] 

## Merge age table map and merge on to dataset
dt <- merge(dt, all_fine_ages, by = c("age_start"))
dt[age_end == 125, age_end := 100] 
dt$mid_age <- (dt$age_start + dt$age_end)/2

## -----------------------------------------------------------------------------
## DO MAD OUTLIERING
## Create new age-weights for each data source
dt <- dt[, sum1 := sum(age_group_weight_value), by = byvars] # sum of standard age-weights for all the ages we are using, by location-age-sex-nid, sum will be same for all age-groups and possibly less than one 
dt <- dt[, new_weight1 := age_group_weight_value/sum1, by = byvars] # divide each age-group's standard weight by the sum of all the weights in their location-age-sex-nid group 

## Age standardizing per location-year by sex
## Add a column titled "as_mean" with the age-standardized mean for the location-year-sex-nid
dt[, as_mean := mean * new_weight1] # initially just the weighted mean for that AGE-location-year-sex-nid
dt[, as_mean := sum(as_mean), by = byvars] # sum across ages within the location-year-sex-nid group, you now have age-standardized mean for that series

dt[as_mean == 0, is_outlier := 1] 
dt$note_modeler <- as.character(dt$note_modeler)
dt[as_mean == 0, note_modeler := paste0(note_modeler, " | outliered this location-year-sex-NID age-series because age standardized mean is 0")]

## log-transform to pick up low outliers 
dt[as_mean != 0, as_mean := log(as_mean)]

## Calculate median absolute deviation
dt[as_mean == 0, as_mean := NA] # don't count zeros in median calculations
dt[, mad := mad(as_mean, na.rm = T), by=c("sex")]
dt[, median := median(as_mean, na.rm = T), by=c("sex")]

## Set number of MAD and mark here
dt[as_mean>((3*mad)+median), is_outlier := 1]
dt[as_mean>((3*mad)+median), note_modeler := paste0(note_modeler, " | outliered because log age-standardized mean for location-year-sex-NID is higher than 3 MAD above median")]
dt[as_mean<(median-(3*mad)), is_outlier := 1]
dt[as_mean<(median-(3*mad)), note_modeler := paste0(note_modeler, " | outliered because log age-standardized mean for location-year-sex-NID is lower than 3 MAD below median")]
dt[, c("sum1", "new_weight1", "as_mean", "median", "mad", "age_group_weight_value") := NULL]


## -----------------------------------------------------------------------------
## FINAL CLEANUP
## Automatically outlier any high outliers
dt[mean > 1.15, is_outlier := 1]

## Fix missing sample size, if still a problem 
dt[is.na(cases) & is.na(sample_size) & measure == "continuous", sample_size := (mean*(1-mean)/standard_error^2)]

## Bound standard errors that are greater than 1
dt$standard_error[dt$standard_error > 1] <- 1

## Save file
write.csv(dt, output_filepath)

