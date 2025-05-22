###################################
# MAD FILTER: MYOCARDITIS
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
library(readxl)
library(doBy)
library(ggplot2)
library(doBy)
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

locs <- data.table(get_location_metadata(location_set_id=35, release_id = 16))

## Import hospital data
hosp <- get_bundle_version(bundle_version_id=47530, fetch='all')

gbd2023 <- merge(hosp, locs[,c("location_id", "region_id", "region_name", "parent_id", "location_type", "ihme_loc_id")], by="location_id", all.x=T)  

## Outlier problematic data
df <- copy(gbd2023)

outlier_bra <- locs$location_id[which(locs$parent_id==130 | locs$parent_id==135 )] 
outlier_pol <- locs$location_id[which(locs$parent_id==51)] 
df$is_outlier[which(df$location_id %in% outlier_bra)] <- 1 # Brazil & Mexico 
df$is_outlier[which(df$location_id %in% outlier_pol)] <- 1     # Poland subnatinals
df[location_id==51, is_outlier:=1]                             # Poland national
df[location_id==193, is_outlier:=1]                            # Botswana
df[location_id==43887, is_outlier:=1]                          # India - Karnataka, Urban 
df[location_id==4856, is_outlier:=1]                           # Karnataka 
df[location_id > 53659 & location_id <53676, is_outlier:=1]    # Poland 
df[location_id==164, is_outlier:=1]                            # Nepal
df[location_id==122, is_outlier:=1]                            # Ecuador
df[location_id==144, is_outlier:=1]                            # Jordan 
df[nid==469234| nid== 469235, is_outlier := 1]                       # Mongolia
df[nid==428286 | nid== 464884, is_outlier := 1]                      # Russia 
df[nid==514241| nid== 514242 | nid== 514243, is_outlier := 1]        # Korea
df[ parent_id == 90 , is_outlier:=1]         # Norway and subnationals 
df[nid ==  244369, is_outlier:=1]  #       # US Market data 
 

## Generate upper/lower thresholds using data from High-Income North America, Western Europe, Central Europe, Australia by sex, age group

# Identify locations with subnationals
df_nat <- copy(df[location_type %in% c('admin0'),]) 

df_subnat <- copy(df[location_type %in% c('admin1','admin2', 'ethnicity'),]) 
df_subnat <- df_subnat[,.(cases_sum = sum(cases), sample_size_sum = sum(sample_size)), by = c('parent_id', 'age_start','age_end','sex','nid','year_start','year_end')]
df_subnat$mean <- df_subnat$cases_sum / df_subnat$sample_size_sum
  
#merge region
loc_region <- unique(locs[, c('parent_id', 'region_id')])
df_subnat <- merge(df_subnat, loc_region, by='parent_id')

df2 <- rbind.fill(df_nat, df_subnat)

df2$flag <- with(df2, ifelse(region_id %in% c(100, 73, 42,65), 1, 0))  #High-income North America, Western Europe, Central Europe, HI_AP

# Built MAD filter at the national level
df.mad <- summaryBy(data=subset(df2, flag==1 & mean!=0), mean ~ age_start + sex, FUN=mad.stats)
df.mad$mad.upper <- with(df.mad, mean.median + 2*mean.mad)
df.mad$mad.lower <- with(df.mad, mean.median - .5*mean.mad)
df.mad <- data.table(df.mad)

df <- data.table(merge(df, df.mad, by=c("age_start", "sex")))  
df$drop <- with(df, ifelse(mean>mad.upper | mean<mad.lower, 1, 0)) 

# Mark as outliers any data point greater than mad.upper limit or less than the min of the age/sex grouping for data that we "trust"
df$is_outlier <- with(df, ifelse(drop==1, 1, is_outlier))

# Add columns necessary for upload. 
df$bundle_id <- 118 
df$bundle_name <-'Acute myocarditis'
df$age_demographer <-1
df$response_rate <- NA
df$extractor <- NA 

df <- df[,c("age_start", "sex", "location_id", "seq", "input_type", "underlying_nid", "nid", "source_type", "bundle_id", "bundle_name", "location_name",
            "year_start", "year_end", "age_end", "age_demographer", "measure", "standard_error", "cases", "effective_sample_size", "sample_size", 
            "unit_type", "unit_value_as_published", "uncertainty_type", "uncertainty_type_value", "representative_name", "urbanicity_type",
            "recall_type", "recall_type_value", "sampling_type", "response_rate", "design_effect", "extractor", "is_outlier", "mean", "lower", "upper")]

# Generate hospital study-level covariate
df$cv_hospital <- 1
df$crosswalk_parent_seq <- NA
df$unit_value_as_published <- 1
df$recall_type <- "Not Set"
df$unit_type   <- "Person"
df$urbanicity_type <- "Unknown"
