#----HEADER-------------------------------------------------------------------------------------------------------------
# Author: "USERNAME"
# Date:    02/07/2017, revised June 2018 for GBD 2017
# Purpose: Correct bias in administrative measles SIA coverage using admin bias in DTP3 coverage estimates
# Path:    ""FILEPATH"adjust_sia_data.R"
#***********************************************************************************************************************


#----OPEN FUNCTION------------------------------------------------------------------------------------------------------
adjust_sia_data <- function(...) {
#***********************************************************************************************************************
  

#----CONFIG-------------------------------------------------------------------------------------------------------------
### runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH" 
  h_root <- "/homes/"USERNAME""
} else { 
  j_root <- ""FILEPATH""
  h_root <- ""FILEPATH""
}

### load packages
pacman::p_load(magrittr, foreign, stats, MASS, data.table, dplyr, plyr, lme4, reshape2, parallel)

### load shared functions
source(""FILEPATH"get_covariate_estimates.R") 
source(""FILEPATH"get_location_metadata.R") 
source(""FILEPATH"get_population.R") 

### load custom functions
""FILEPATH"read_excel.R" %>% source

### set objects
acause <- "measles"
age_start <- 2             ## age "early neonatal"
age_end <- 16              ## age 55-59 years
a <- 3                     ## birth cohort years before 1980

### directories
home <- file.path(j_root, ""FILEPATH"", "00_documentation")
#***********************************************************************************************************************


#----GET DATA-----------------------------------------------------------------------------------------------------------
### get location data,  drop US subnational Georgia to avoid confusion
locations2 <- locations[location_id != 533, ]

### prep WHO supplementary immunization activity data, 1987 through 1999
sia_1987_1999 <- fread(file.path(home, FILEPATH))
setnames(sia_1987_1999, c("yr", "iso", "reached_pct", "country"), c("year_id", "ihme_loc_id", "prop_reached", "location_name"))
sia_1987_1999[is.na(reached) & !is.na(prop_reached) & !is.na(target), reached := prop_reached * target]
sia_1987_1999 <- sia_1987_1999[!(is.na(reached) | reached==0) &
                               year_id < 2000, ]
# fix location names
sia_1987_1999[location_name=="CAR", location_name := "Central African Republic"]
sia_1987_1999[location_name=="DRCongo", location_name := "Democratic Republic of the Congo"]
sia_1987_1999[location_name=="Bahamas", location_name := "The Bahamas"]
sia_1987_1999[location_name=="Hong Kong", location_name := "Hong Kong Special Administrative Region of China"]
sia_1987_1999[location_name=="Saint Kitts & Nevis", location_name := "Saint Kitts and Nevis"]
sia_1987_1999[location_name=="Sao Tome & Principe", location_name := "Sao Tome and Principe"]
sia_1987_1999[location_name=="UAE", location_name := "United Arab Emirates"]
sia_1987_1999[location_name=="VietNam", location_name := "Vietnam"]
sia_1987_1999[location_name=="DPRKorea", location_name := "North Korea"]
sia_1987_1999[location_name=="UK", location_name := "United Kingdom"]
# merge ihme location ids
sia_1987_1999 <- merge(sia_1987_1999, locations2[, .(location_id, location_name)], by="location_name", all.x=TRUE)
if (length(sia_1987_1999[is.na(location_id), location_name] %>% unique) > 0) print(paste0("dropping locations with missing location_id: ", paste(sia_1987_1999[is.na(location_id), location_name] %>% unique, collapse=", ")))
sia_1987_1999 <- sia_1987_1999[!is.na(location_id), ]
# prep only necessary columns
sia_1987_1999 <- sia_1987_1999[, c("location_id", "year_id", "target", "reached", "prop_reached"), with=FALSE]

### post-2000 SIA data downloaded from  "ADDRESS"
sia_2 <- read_excel(file.path(home, FILEPATH), skip=1, sheet="SIAs_Jan2000_Dec2019") %>% as.data.table
setnames(sia_2, c("Country", "ISO", "Year", "Target", "Reached", "% Reached", "Age", "Survey results"), c("location_name", "ihme_loc_id", "year_id", "target", "reached", "prop_reached", "ages", "survey_coverage"))  
# drop where number of administered vaccines is missing
sia_2[is.na(reached) & !is.na(prop_reached) & !is.na(target), reached := prop_reached * target]
sia_2 <- sia_2[Intervention %in% c("Measles", "MMR", "MR", "measles") &
               !(is.na(reached) | reached==0), ]
# keep survey data
sia_2_survey <- sia_2[!is.na(survey_coverage), .(ihme_loc_id, year_id, survey_coverage, prop_reached)] %>% unique
sia_2_survey[, survey_coverage := survey_coverage / 100]
# collapse by country-year
sia_2 <- sia_2[!duplicated(sia_2), ]
sia_2 <- sia_2[, .(target=sum(target), reached=sum(reached)), by=c("ihme_loc_id", "year_id")] %>% .[, .(ihme_loc_id, year_id, reached, target)] %>% unique
# merge ihme location ids
sia_2 <- merge(sia_2, locations2[, .(ihme_loc_id, location_id)], by="ihme_loc_id", all.x=TRUE)
if (length(sia_2[is.na(location_id), ihme_loc_id] %>% unique) > 0) print(paste0("dropping locations with missing location_id: ", paste(sia_2[is.na(location_id), ihme_loc_id] %>% unique, collapse=", ")))
sia_2 <- sia_2[!is.na(location_id), ]
# add on survey coverage
sia_2 <- merge(sia_2, sia_2_survey, by=c("ihme_loc_id", "year_id"), all.x=TRUE)
#***********************************************************************************************************************


#----BIAS ADJUSTMENT----------------------------------------------------------------------------------------------------
### SIA bias correction
# bind together all years of SIA data
who_sia <- rbind(sia_1987_1999, sia_2, fill=TRUE)

# prep SIA as a continuous measure representing the proportion of the population under 15 reached
sum_pop_all_sia <- population[age_group_id <= 8, .(target_pop=sum(population)), by=c("location_id", "year_id")] %>% unique
who_sia <- merge(who_sia, sum_pop_all_sia, by=c("location_id", "year_id"), all.x=TRUE)

# if reported target population is missing or zero, replace with target_pop from under 15 GBD population
who_sia[is.na(target) | target==0, target := target_pop]

# calculate administrative coverage from reported doses and target population
who_sia[, administrative_coverage := reached / target] # target_pop

# get DTP3 coverage modeled administrative bias, used as a proxy for bias ratio of post-campaign survey coverage to administrative reported coverage
RUNS <- fread(file.path(j_root, ""FILEPATH"/run_log.csv"))[is_best==1 & me_name=="vacc_dpt3", run_id] 
model_root <- ""FILEPATH"code"
setwd(model_root)
source("init.r")
source(""FILEPATH"utility.r")
dtp3_bias <- rbindlist(lapply(RUNS, function(x) model_load(x, obj="raked") %>% data.table %>% .[, run_id := x]))[, .(location_id, year_id, gpr_mean)]
setnames(dtp3_bias, "gpr_mean", "cv_admin_bias_ratio")

# use DTP3 administrative bias to predict bias in measles SIAs in location-years without post-campaign surveys
who_sia <- merge(who_sia, dtp3_bias, by=c("location_id", "year_id"), all.x=TRUE)
who_sia[is.na(survey_coverage), survey_coverage := administrative_coverage * cv_admin_bias_ratio]

# calculate bias ratio of post-campaign survey coverage to administrative reported coverage
who_sia[, post_campaign_survey_bias_ratio := survey_coverage / administrative_coverage]

# coverage variable of interest: corrected number of doses administered divided by the population under age 15 (target pop)
who_sia[, corrected_target_coverage := (reached * post_campaign_survey_bias_ratio) / target_pop]

### drop unnecessary columns
who_sia <- who_sia[year_id < year_end, c("location_id", "year_id", "corrected_target_coverage"), with=FALSE]
#***********************************************************************************************************************


#----END FUNCTION-------------------------------------------------------------------------------------------------------
return(who_sia)

}
#***********************************************************************************************************************