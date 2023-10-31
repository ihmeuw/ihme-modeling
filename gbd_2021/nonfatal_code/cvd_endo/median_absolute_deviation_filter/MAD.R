###################################
# MAD FILTER: ENDOCARDITIS
# AUTHOR: "USERNAME"
# PURPOSE: TO LABEL EXTREME DATA POINTS ACCORDING TO THE MEDIAN ABSOLUTE DEVIATION METHOD
#          MEDIAN ABSOLUTE DEVIATION = MAD = MEDIAN(|X_i -MEDIAN(X)|), we then filter those above 2 MADS or below .5 MADS. 
###################################


source('FILEPATH/get_ids.R')
source('FILEPATH/get_outputs.R')
source('FILEPATH/get_model_results.R')
source('FILEPATH/get_draws.R')
source('FILEPATH/get_bundle_data.R')
source('FILEPATH/upload_bundle_data.R')
source('FILEPATH/get_bundle_version.R')
source('FILEPATH/save_bundle_version.R')
source('FILEPATH/get_crosswalk_version.R')
source('FILEPATH/save_crosswalk_version.R')
source('FILEPATH/get_location_metadata.R')
source('FILEPATH/get_age_metadata.R')
source('FILEPATH/get_demographics.R')
source('FILEPATH/get_population.R')

if (Sys.info()[1] == "Linux") jpath <- "FILEPATH"
if (Sys.info()[1] == "Windows") jpath <- "FILEPATH"

library(openxlsx)
library(ggplot2)
library(plyr)
library(doBy)
library(readxl)
suppressMessages(library(R.utils))

'%ni%' <- Negate('%in%')

stats <- function(x) {
  c(
    mean = mean(x,na.rm=TRUE),
    SD = sd(x,na.rm=TRUE),
    min = min(x,na.rm=TRUE),
    max = max(x,na.rm=TRUE)
  )
}

mad.stats <- function(x) {
  c(
    mad = mad(x),
    median = median(x)
  )
}

locs <- get_location_metadata(location_set_id = 35, gbd_round_id = 7)

## Import hospital data
hosp <- get_bundle_version(bundle_version_id=22028, fetch='all') ## enter your bundle version here here. 
hosp <- subset(hosp, clinical_data_type!="")                    ## subset to clinical
hosp[, c("parent_id", "cv_marketscan", "cv_marketscan_all_2000", "cv_marketscan_all_2010", "cv_marketscan_all_2012", "cv_marketscan_inp_2000",
         "cv_marketscan_inp_2010", "cv_marketscan_inp_2012", "cv_outpatient", "cv_hospital") := NULL] ## remove unnecessary columns.

## Generate upper/lower thresholds using data from High-Income North America, Western Europe, Central Europe, Australia by sex, age group
gbd2020 <- merge(hosp, locs[,c("location_id", "region_id", "region_name", "parent_id")], by="location_id", all.x=T)  
gbd2020$flag <- with(gbd2020, ifelse(region_id %in% c(100, 73, 42), 1, 0))  #High-income North America, Western Europe, Central Europe

df.mad <- summaryBy(data=subset(gbd2020, flag==1 & mean!=0), mean ~ age_start + sex, FUN=mad.stats) ## subset to above locations (high quality Clinical data)
df.mad$mad.upper <- with(df.mad, mean.median + 2*mean.mad)
df.mad$mad.lower <- with(df.mad, mean.median - 0.5*mean.mad)
df.mad <- data.table(df.mad)


## Bounds for new age groups
new_age_mad <- data.table()
new_age_mad[,`:=` (age_start = c(2,2),
                   sex=c('Female','Male'),
                   mean.mad=c(	5.790838e-06,6.540170e-06),
                   mean.median=c(1.038403e-05,1.325753e-05),
                   mad.upper=c(2.196571e-05,2.633787e-05),
                   mad.lower=c(7.488615e-06,9.987443e-06))]
new_age_mad <- data.table(new_age_mad)
df.mad <- rbind(df.mad,new_age_mad)

# merge df.mad with new clinical (used for filtering)
df <- data.table(merge(hosp, df.mad, by=c("age_start", "sex")))

df$drop <- with(df, ifelse(mean>mad.upper | mean<mad.lower, 1, 0)) 
#Mark as outliers any data point greater than mad.upper limit or less than the min of the age/sex grouping for data that we "trust"
df$is_outlier <- with(df, ifelse(drop==1, 1, is_outlier))
