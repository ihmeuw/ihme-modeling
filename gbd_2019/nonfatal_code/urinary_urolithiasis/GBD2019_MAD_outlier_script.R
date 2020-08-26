######################################################################################################################
### This script accepts data as Excel spreadsheet as output, identifies data-series (defined by sex, location, year and NID) 
### for which all observed values are 0 and those for which the age-adjusted mean is more than xMADs above or below
### Only reference case definition was systematically outliered
######################################################################################################################
#set object

functions_dir <- "FILEPATH"
temp_dir <- "FILEPATH"
date <- Sys.Date()
date <- gsub("-", "_", date)


## Source central functions
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
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/get_crosswalk_version.R")
source("FILEPATH/get_age_metadata.R")

## SET OBJECTS
date <- gsub("-", "_", Sys.Date())


##GET_CROSSWALK_VERSION
bundle_version_id <- 	 OBJECT
crosswalk_version_id <- 		OBJECT
dt <- get_crosswalk_version(crosswalk_version_id, export=FALSE)
dt <- as.data.table(dt)


dt$cv_ms2000 <-ifelse(dt$nid==244369,1,0)
dt = within(dt, {cv_marketscan = ifelse((nid==336847 |nid==408680 |nid== 244370|nid==336850
                                           |nid== 244371|nid== 336849|nid==336848 ), 1, 0)})
dt <- within(dt, {cv_hospital = ifelse(cv_marketscan==0 & cv_ms2000==0 & measure=="prevalence", 1, 0)})

output_filepath <- FILEPATH

## Meta data
age_using <- c(2:20, 30:32, 235)
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

## SET UP DATATABLE
dt<- dt[!is.na(mean)]
setnames(dt, "is_outlier", "is_outlier_step2")

## MAD OUTLIERING BEGINS HERE:

  ##take inp data to be reviewed out of the main dataset
  dt_final <- copy(dt)
  dt_final <- dt_final[cv_hospital!=1,]
  
  ##make a set with only the inpatient data
  dt_inp <- copy(dt)
  dt_inp <- dt_inp[cv_hospital==1,]
  
  ##merge age table map and merge on to dataset
  dt_inp <- merge(dt_inp, all_ages, by = c("age_start", "age_end"), all.x = T)
  
  #calculate age-standardized prevalence/incidence
  
  ##create new age-weights for each data source
  dt_inp <- dt_inp[, sum1 := sum(age_group_weight_value), by = byvars] #sum of standard age-weights for all the ages we are using, by location-age-sex-nid, sum will be same for all age-groups and possibly less than one 
  dt_inp <- dt_inp[, new_weight1 := age_group_weight_value/sum1, by = byvars] #divide each age-group's standard weight by the sum of all the weights in their locaiton-age-sex-nid group 
  
  ##age standardizing per location-year by sex
  #add a column titled "age_std_mean" with the age-standardized mean for the location-year-sex-nid
  dt_inp[, as_mean := mean * new_weight1] #initially just the weighted mean for that AGE-location-year-sex-nid
  dt_inp[, as_mean := sum(as_mean), by = byvars] #sum across ages within the location-year-sex-nid group, you now have age-standardized mean for that series
  
  ##mark as outlier if age standardized mean is 0 (either biased data or rare disease in small ppln)
  dt_inp[as_mean == 0, is_outlier := 1] 
  dt_inp$note_modeler <- as.character(dt_inp$note_modeler)
  dt_inp$note_modeler[dt_inp$as_mean == 0] <- paste0( "outliered this location-year-sex-NID age-series because age standardized mean is 0")
  
  
  ## log-transform to pick up low outliers
  dt_inp[as_mean != 0, as_mean := log(as_mean)]
  
  # calculate median absolute deviation
  dt_inp[as_mean == 0, as_mean := NA] ## don't count zeros in median calculations
  dt_inp[,mad:=mad(as_mean,na.rm = T),by=c("sex")]
  dt_inp[,median:=median(as_mean,na.rm = T),by=c("sex")]
  
  
  #***can change number of MAD to mark here
  dt_inp[as_mean>((1.5*mad)+median), is_outlier := 1]
  dt_inp[as_mean>((1.5*mad)+median), note_modeler := paste0(note_modeler, " | outliered because log age-standardized mean for location-year-sex-NID is higher than 1.5 MAD above median")]
  dt_inp[as_mean<(median-(1.5*mad)), is_outlier := 1]
  dt_inp[as_mean<(median-(1.5*mad)), note_modeler := paste0(note_modeler, " | outliered because log age-standardized mean for location-year-sex-NID is lower than 1.5 MAD below median")]
  dt_inp[, c("sum", "new_weight", "as_mean", "median", "mad", "age_group_weight_value", "age_group_id") := NULL]
  
  
  dt_final <- rbind(dt_final, dt_inp, fill=TRUE)
  
  dt_final$seq[dt_final$measure=="mtexcess"] <- NA
  dt_final$crosswalk_parent_seq[dt_final$measure=="mtexcess"] <- NA
  dt_final$is_outlier[is.na(dt_final$is_outlier)] <-0
  
  ## to get ONLY STEP 4 DATA
  dt_final1 <- subset(dt_final, !(nid== 336847 | nid== 354896 | nid== 397812| nid== 397813 | nid== 397814 |
                        nid== 404395 | nid== 407536 | nid== 408336 | nid== 408680 | nid== 411100 | nid== 411786 |
                        nid== 411787 |  nid== 408789 |  nid==409153 |  nid==409154 |  nid==409155 | nid==409156 |
                        nid==409157 | nid==409158 | nid==409159    ))
  dt_final1 <- subset(dt_final, nid== 336847 | nid== 354896 | nid== 397812| nid== 397813 | nid== 397814 | nid== 404395 | nid== 407536 | nid== 408336 | nid== 408680 | nid== 411100 | nid== 411786 | nid== 411787)
  
  
  write.xlsx(dt_final1, output_filepath, sheetName = "extraction", col.names=TRUE)
  
  
##SAVE_CROSSWALK_VERSION
  path_to_data <-output_filepath
  description <- "step4 new data, updated MAD"
  result <- save_crosswalk_version( bundle_version_id, path_to_data, description=description)

  
  
##TO SAVE BULK OUTLIER Based on new mad
source("FILEPATH/save_bulk_outlier.R")
  
  crosswalk_version_id <- OBJECT
  decomp_step <- OBJECT
  filepath <- FILEPATH
  description <- 'Crosswalk Version with updated MAD status'
  result <- save_bulk_outlier(
    crosswalk_version_id=crosswalk_version_id,
    decomp_step=decomp_step,
    filepath=filepath,
    description=description
  )
  
  print(sprintf('Request ID: %s', result$request_id))
  print(sprintf('New crosswalk Version ID with outliers: %s', result$crosswalk_version_id))