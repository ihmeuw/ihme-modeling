
##################################
## STEP 4 MAD OUTLIERING PROCESS
##################################

# SOURCE AND OBJECTS ----------------------------------------------------------
rm(list=ls())
##working environment
#set object
temp_dir <- "FILEPATH"
date <- Sys.Date()
date <- gsub("-", "_", date)
bv_id <-  # new bundle version id
  step4_cv_id <- # crosswalked data that you need to do MAD outliering on
  b_id <- # bundle_id
  cause <- # cause name

## Source central functions
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
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
n <- 1 # this will be the number of MAD you will outlier
## GET DATA
#
original_data4 <- get_crosswalk_version(step4_cv_id)
dt_emr <- original_data4[measure == "mtexcess", ]
dt_other_measures <- original_data4[measure != "prevalence" & measure != "mtexcess"]
dt_outliers <- original_data4[measure == "prevalence" & nid == 365281]
dt <- original_data4[measure == "prevalence" & nid != 365281, ] # prevalence plus modeled EMR data NID
cv_drop <- c("cv_survey", "cv_hospital", "cv_literature", "cv_marketscan_all_2000", "cv_marketscan_inp_2000",
             "cv_marketscan_all_2010", "cv_marketscan_inp_2010", "cv_marketscan_all_2012", "cv_marketscan_inp_2012", "cv_inpatient")
byvars <- c("location_id", "sex", "year_start", "year_end", "nid")

## GET AGE WEIGHTS
all_fine_ages <- as.data.table(get_age_metadata(age_group_set_id=12))
not_babies <- all_fine_ages[!age_group_id %in% c(2:4)]
not_babies[, age_end := age_group_years_end-1]
group_babies <- all_fine_ages[ c(1:3)]
group_babies[, age_end := 0.999]
all_ages <- rbind(not_babies, group_babies)
all_ages[, age_start := age_group_years_start]
all_ages[, c("age_group_years_start", "age_group_years_end") := NULL]
all_ages <- rbind(all_ages, all_ages[20,])
all_ages <- all_ages[1:21,]

## SET UP DATATABLE
dt<- dt[!is.na(mean)]
dt_merged <- merge(dt, all_ages, by = c("age_start", "age_end"), all.x = T) # merge age table map and merge on to dataset

for (i in 1:nrow(dt_merged)) {
  if (is.na(dt_merged[i, age_group_weight_value])) {
    dt_merged[, effective_age_start := floor(age_start/5)*5]
    dt_merged[i, age_group_weight_value := sum(all_ages[c(which(all_ages$age_start == dt_merged[i, effective_age_start]), which(all_ages$age_start > dt_merged[i, effective_age_start] & all_ages$age_start <= (dt_merged[i, age_end]-4))), age_group_weight_value])]
    dt_merged$effective_age_start <- NULL
  }
}

#calculate age-standardized prevalence/incidence

##create new age-weights for each data source
dt_merged <- dt_merged[, sum := sum(age_group_weight_value), by = byvars] 
dt_merged <- dt_merged[, new_weight := age_group_weight_value/sum, by = byvars] 
##age standardizing per location-year by sex
dt_merged[, as_mean := mean * new_weight] 
dt_merged[, as_mean := sum(as_mean), by = byvars] 
dt_merged[as_mean == 0, is_outlier_new := 1]
dt_merged[as_mean == 0, note_modeler := paste0(note_modeler, " | outliered this location-year-sex-NID age-series because age standardized mean is 0 ", date)]

## log-transform to pick up low outliers
dt_merged[as_mean != 0, as_mean := log(as_mean)]
# calculate median absolute deviation
dt_merged[as_mean == 0, as_mean := NA] ## don't count zeros in median calculations
dt_merged[,mad:=mad(as_mean,na.rm = T),by=c("sex")]
dt_merged[,median:=median(as_mean,na.rm = T),by=c("sex")]

# MAD outlier
dt_merged[as_mean>((n*mad)+median), is_outlier_new := 1]
dt_merged[as_mean>((n*mad)+median), note_modeler := paste0(note_modeler, " | outliered because log age-standardized mean for location-year-sex-NID is higher than ", n," MAD above median ", date)]
dt_merged[, c("sum", "new_weight", "as_mean", "median", "mad", "age_group_weight_value", "age_group_id") := NULL]

print(paste(nrow(dt_merged[is_outlier_new == 1]), "points were outliered with", n, "MAD"))
percent_outliered <- round((nrow(dt_merged[is_outlier_new == 1]) / nrow(dt_merged))*100, digits = 1)
print(paste("outliered", percent_outliered, "% of data"))
dropped_locs <- setdiff(unique(original_data4$country), unique(dt_merged[is.na(is_outlier_new)]$country))
print(paste("Dropped ", length(dropped_locs), " countries from model:", paste(dropped_locs, collapse = " ")))

## SEND TO REVIEW FOLDER
outlier_dt <- copy(dt_merged)
# Situations where the outlier status would change
# UN-OUTLIER
outlier_dt[is_outlier == 1 & is.na(is_outlier_new) & grepl("MAD", note_modeler),
           `:=` (is_outlier = 0, note_modeler = paste("unoutlier based on new MAD calculation", date))]
# NEWLY OUTLIER
outlier_dt[is_outlier == 0 & is_outlier_new == 1, is_outlier := 1] 
# append the prevalence data and the emr data together
all_data <- rbind(outlier_dt, dt_emr, fill = TRUE)
all_data <- rbind(all_data, dt_other_measures, fill = T)
all_data <- rbind(all_data, dt_outliers, fill = T)

upload <- all_data[, c("seq", "is_outlier")]

source("FILEPATH")

output_filepath <- "FILEPATH"
write.xlsx(upload, output_filepath, sheetName = "extraction", col.names=TRUE, showNA = FALSE, row.names= FALSE)
description <-  paste("outliered data ", n, " MAD only above median (", percent_outliered, "%),", paste("dropped", length(dropped_locs), "countries from model:", paste(dropped_locs, collapse = " ")))

result <- save_bulk_outlier(crosswalk_version_id = step4_cv_id, decomp_step = "step4", filepath = output_filepath, description = description) # neck pain: 9803; low back pain: 10838; lbp at 1.5 above/below: 11756; lbp at 1.5 above only: 11804; lbp at 1 MAD above only: 11813



