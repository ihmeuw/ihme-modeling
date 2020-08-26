## ---------------------------------------------------------------------------------------------------- ##
## Format input data for dementia severity mrbrt analysis
##	Populate covariate vals
##	Merge in ids
##	Calculate mean/cases
##	Calculate mean/sd ages
##	Identify & save data for which we need to calculate mean age
##	Save all data
## 
##
## Author: USERNAME
## 06/05/2019
## ---------------------------------------------------------------------------------------------------- ##


## SET UP ENVIRONMENT --------------------------------------------------------------------------------- ##



rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_drive <- "FILEPATH"
  h_drive <- "FILEPATH"
} else {
  j_drive <- "FILEPATH"
  h_drive <- "FILEPATH"
}

pacman::p_load(data.table, openxlsx, ggplot2)
date <- gsub("-", "_", Sys.Date())


## SET OBJECTS ---------------------------------------------------------------------------------------- ##

step <- "iterative"
round <- 6
prev <- 5
dem_cause <- ID
years <- 1990:2017
sexes <- 1:3
dem_ages <- c(13:22, 30:32, 235)

dem_dir <- paste0("FILEPATH")
severity_dir <- paste0("FILEPATH")
fn_dir <- "FILEPATH"

shell_path <- "ADDRESS"
error_path <- "FILEPATH"

## SOURCE FNS ----------------------------------------------------------------------------------------- ##

source(paste0(fn_dir,"get_ids.R"))
source(paste0(fn_dir,"get_location_metadata.R"))
source(paste0(fn_dir,"get_age_metadata.R"))
source(paste0(fn_dir,"get_model_results.R"))
source(paste0(fn_dir,"get_outputs.R"))
mround <- function(x,base){
  base*round(x/base)
}


## READ IN DATA  -------------------------------------------------------------------------------------- ##


locs <- get_location_metadata(location_set_id = 22)
locs <- locs[,c("location_id","location_ascii_name")]
sex_names <- get_ids(table = "sex")
age_dt <- get_age_metadata(12)


extraction <- read.xlsx(file = paste0("FILEPATH"), sheetIndex = 1)
extraction <- data.table(extraction)
extraction <- extraction[,c("NID",
                            "location_name",
                            "location_id",
                            "sex",
                            "year_start",
                            "year_end",
                            "age_start",
                            "age_end",
                            "age_issue",
                            "mean_age",
                            "sd_age",
                            "measure",
                            "age_demographer",
                            "mean",
                            "lower",
                            "upper",
                            "standard_error",
                            "cases",
                            "sample_size",
                            "scale",
                            "severity_rating",
                            "representative_name",
                            "note_sr",
                            "group",
                            "specificity",
                            "urbanicity_type",
                            "is_outlier",
                            grep(pattern = "cv_",names(extraction),value = T)), with = F]
data <- extraction


## FORMATTING ---------------------------------------------------------------------------------------- ##

#convert all covariate columns to numeric type
setnames(data, "cv_0.5_label", "label_0.5")
covs <- grep(pattern = "cv_",names(data),value = T)
dt_covs <- data[,lapply(.SD, as.numeric), .SDcols=covs]
dt_covs <- as.data.table(dt_covs)

#convert all non-1s to 0s
for(cov in covs){
  a <- sum(dt_covs[,cov, with = F], na.rm = T)
  print(a)
  dt_covs[is.na(get(cov)),(cov):=0]
  b <- sum(dt_covs[,cov, with = F], na.rm = T)
  print(b)
  
  #check that there are the same amount of 1s as before conversion
  if(a!=b){
    print("something went wrong")
    break
  }
}

#merge covariate cols back into main dt
data <- cbind(data[,!covs, with = F], dt_covs)

## merge in sex_ids ---------------------------------------------------------------------------------- ##
data <- merge(data, sex_names, by="sex", all.x =T)

## add loc_ids --------------------------------------------------------------------------------------- ##
setnames(locs, "location_id","id")
data <- merge(data, locs, by.x = "location_name", by.y = "location_ascii_name", all.x = T)
data <- data[id!=35,] 

extraction_locs <- unique(data$location_name)
contained <- extraction_locs %in% locs$location_ascii_name
missing <- extraction_locs[!contained]

if(is.null(missing)|length(missing)==0){
  print("no unknown location_names")
} else {
  print(paste("need to check location_names: ",missing))
}

data[location_id!=id & !is.na(location_id),c("location_name","location_id","id")]

data$location_id <- as.integer(as.character(data$location_id))
data[is.na(location_id),location_id:=id]
data[,id:=NULL]


## fill in mean / cases ------------------------------------------------------------------------------ ##

issues <- nrow(data[is.na(mean) & is.na(cases),])
if(issues>0){
  print(paste0(length(issues)," rows missing both cases & mean"))
  print(data[is.na(mean) & is.na(cases),c("NID","location_name","year_start","year_end","sex","mean_age","sd_age","cases","mean","sample_size")])
}

data[is.na(cases),cases:=mean*sample_size]
data[is.na(mean),mean:=cases/sample_size]

if(nrow(data[is.na(mean)|is.na(cases),])){
  print("oops; still missing either a mean or cases for at least one row.")
}

## IDENTIFY WHICH DATA NEEDS MEAN AGE ---------------------------------------------------------------- ##

age_issues <- nrow(data[is.na(mean_age) & (is.na(age_start) | is.na(age_end)),])

if(age_issues>0){
  print("missing age info")
}

## SAVE DATA WHICH NEEDS MEAN AGE -------------------------------------------------------------------- ##

## select rows which need to calculate mean_age

calc_age <- copy(data[is.na(mean_age),.(location_name, location_id, sex_id, year_start, year_end, age_start, age_end)])
calc_age <- unique(calc_age)
calc_age[,year:=round((year_start + year_end)/2, 0)]

## save csv
filename <- paste0("FILEPATH")
write.csv(calc_age, filename)

## SAVE DATA ALL DATA ---------------------------------------------------------------------------------------- ##

write.csv(data, paste0("FILEPATH"))


