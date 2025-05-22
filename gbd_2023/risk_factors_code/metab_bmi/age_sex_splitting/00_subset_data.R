################################################################################
## DESCRIPTION: Subsets rows of data for age-pattern model in STGPR
## INPUTS: XLSX of cleaned data ##
## OUTPUTS: CSVs of gold-standard, GBD age-binned data ##
## AUTHOR:
## DATE:
################################################################################

rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "FILEPATH"


# Base filepaths
share_dir <- 'FILEPATH' # only accessible from the cluster
data_dir <- "FILEPATH"
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""

## LOAD DEPENDENCIES
source(paste0(code_dir, 'FILEPATH'))
source(paste0(code_dir, 'FILEPATH'))
library(openxlsx, lib = paste0(h, "FILEPATH"))

# Version of age-split data
version <- 1

# Read in data
overweight_data <- read.xlsx(paste0(data_dir, "FILEPATH")) %>% as.data.table(.)
obese_data <- read.xlsx(paste0(data_dir, "FILEPATH")) %>% as.data.table(.)
underweight_data <- read.xlsx(paste0(data_dir, "FILEPATH")) %>% as.data.table(.)
severe_underweight_data <- read.xlsx(paste0(data_dir, "FILEPATH")) %>% as.data.table(.)

# Select measured rows only
overweight_age_pattern <- overweight_data[diagnostic == "measured"]
obese_age_pattern <- obese_data[diagnostic == "measured"]
underweight_age_pattern <- underweight_data[diagnostic == "measured"]
severe_underweight_age_pattern <- severe_underweight_data[diagnostic == "measured"]

# Select only rows that have 5 year age bins with age_start as multiple of 5 OR is for 2-4 year olDs
overweight_age_pattern[,age_end := age_end +1]
obese_age_pattern[,age_end := age_end +1]
underweight_age_pattern[,age_end := age_end +1]
severe_underweight_age_pattern[,age_end := age_end +1]
overweight_age_pattern <- overweight_age_pattern[(age_start == age_end - 5 & age_start %% 5 == 0 & age_start != 0)| (age_start == 2 & age_end == 5)| (age_start == 95 & age_end == 125)]
obese_age_pattern <- obese_age_pattern[(age_start == age_end - 5 & age_start %% 5 == 0 & age_start != 0)| (age_start == 2 & age_end == 5)| (age_start == 95 & age_end == 125)]
underweight_age_pattern <- underweight_age_pattern[(age_start == age_end - 5 & age_start %% 5 == 0 & age_start != 0)| (age_start == 2 & age_end == 5)| (age_start == 95 & age_end == 125)]
severe_underweight_age_pattern <- severe_underweight_age_pattern[(age_start == age_end - 5 & age_start %% 5 == 0 & age_start != 0)| (age_start == 2 & age_end == 5)| (age_start == 95 & age_end == 125)]

# Select rows with sample size greater than or equal to 20 for prevalence, and 10 for proportion
# Arbitrary but the 20th percentile of sample size is 20, SUW has smaller so use 10 for proportions
overweight_age_pattern$sample_size <- round(overweight_age_pattern$sample_size) # first round sample size 
obese_age_pattern$sample_size <- round(obese_age_pattern$sample_size) # first round sample size
underweight_age_pattern$sample_size <- round(underweight_age_pattern$sample_size) # first round sample size 
severe_underweight_age_pattern$sample_size <- round(severe_underweight_age_pattern$sample_size) # first round sample size

## Only keep data with enough observations. Prevalence =20, Proportion=10
overweight_age_pattern <- overweight_age_pattern[sample_size >= 20]
obese_age_pattern <- obese_age_pattern[sample_size >= 10]
underweight_age_pattern <- underweight_age_pattern[sample_size >= 20]
severe_underweight_age_pattern <- severe_underweight_age_pattern[sample_size >= 10]

# Only keep male and female data and create sex_id
overweight_age_pattern <- overweight_age_pattern[sex %in% c("Male", "Female")][,sex_id := ifelse(sex == "Male", 1, 2)]
obese_age_pattern <- obese_age_pattern[sex %in% c("Male", "Female")][,sex_id := ifelse(sex == "Male", 1, 2)]
underweight_age_pattern <- underweight_age_pattern[sex %in% c("Male", "Female")][,sex_id := ifelse(sex == "Male", 1, 2)]
severe_underweight_age_pattern <- severe_underweight_age_pattern[sex %in% c("Male", "Female")][,sex_id := ifelse(sex == "Male", 1, 2)]

# Use rule of 3 for sample proportion = 0 to get the upper confidence interval and then back calculate the variance
overweight_age_pattern[val == 0, `:=` (upper = -log(0.05)/sample_size, variance = (upper/qt(0.975, df = sample_size - 1))^2)]
obese_age_pattern[val == 0, `:=` (upper = -log(0.05)/sample_size, variance = (upper/qt(0.975, df = sample_size - 1))^2)]
underweight_age_pattern[val == 0, `:=` (upper = -log(0.05)/sample_size, variance = (upper/qt(0.975, df = sample_size - 1))^2)]
severe_underweight_age_pattern[val == 0, `:=` (upper = -log(0.05)/sample_size, variance = (upper/qt(0.975, df = sample_size - 1))^2)]

# Use rule of 3 for sample proportion = 1 to get the lower confidence interval and then back calculate the variance
obese_age_pattern[val == 1, `:=` (lower = 1+log(0.05)/sample_size, variance = ((lower-1)/qt(0.025, df = sample_size - 1))^2)]
severe_underweight_age_pattern[val == 1, `:=` (lower = 1+log(0.05)/sample_size, variance = ((lower-1)/qt(0.025, df = sample_size - 1))^2)]

## keep upper and lower bounds between 0 and 1
overweight_age_pattern[upper > 1, upper := 1][lower < 0, lower:= 0]
obese_age_pattern[upper > 1, upper := 1][lower < 0, lower:= 0]
underweight_age_pattern[upper > 1, upper := 1][lower < 0, lower:= 0]
severe_underweight_age_pattern[upper > 1, upper := 1][lower < 0, lower:= 0]

# Check that all lower and upper bounds are logical
if(nrow(overweight_age_pattern[lower > val | upper < val | lower > upper | lower<0 | upper>1]) != 0) stop(message("Some lower and upper bounds are illogical."))
if(nrow(obese_age_pattern[lower > val | upper < val | lower > upper | lower<0 | upper>1]) != 0) stop(message("Some lower and upper bounds are illogical."))
if(nrow(underweight_age_pattern[lower > val | upper < val | lower > upper | lower<0 | upper>1]) != 0) stop(message("Some lower and upper bounds are illogical."))
if(nrow(severe_underweight_age_pattern[lower > val | upper < val | lower > upper | lower<0 | upper>1]) != 0) stop(message("Some lower and upper bounds are illogical."))

# Set up filepaths for saving
if(!dir.exists(paste0(data_dir, "../age_pattern/data/v", version))){
  dir.create(paste0(data_dir, "../age_pattern/data/v", version), mode = 000) # Should grant read and write access
  save_dir <- paste0(data_dir, "../age_pattern/data/v", version, "/")
} else {
  save_dir <- paste0(data_dir, "../age_pattern/data/v", version, "/")
}

## get outliers from last round
overweight_outliers <- fread("FILEPATH")
obese_outliers <- fread("FILEPATH")
overweight_outliers <- distinct(overweight_outliers[,c("nid", "age_group_id","sex_id","location_id","year_id","is_outlier")])
obese_outliers <- distinct(obese_outliers[,c("nid", "age_group_id","sex_id","location_id","year_id","is_outlier")])

##apply outliers to new data
overweight_age_pattern <- merge(overweight_age_pattern[,-c("is_outlier")], overweight_outliers, all.x=T, by=c("nid", "age_group_id","sex_id","location_id","year_id"))
obese_age_pattern <- merge(obese_age_pattern[,-c("is_outlier")], obese_outliers, all.x=T, by=c("nid", "age_group_id","sex_id","location_id","year_id"))
underweight_age_pattern <- merge(underweight_age_pattern[,-c("is_outlier")], overweight_outliers, all.x=T, by=c("nid", "age_group_id","sex_id","location_id","year_id"))
severe_underweight_age_pattern <- merge(severe_underweight_age_pattern[,-c("is_outlier")], obese_outliers, all.x=T, by=c("nid", "age_group_id","sex_id","location_id","year_id"))
overweight_age_pattern[is.na(is_outlier), is_outlier:=0]
obese_age_pattern[is.na(is_outlier), is_outlier:=0]
underweight_age_pattern[is.na(is_outlier), is_outlier:=0]
severe_underweight_age_pattern[is.na(is_outlier), is_outlier:=0]

##get new outliers
for(measure in c("overweight","obese","underweight","severe_underweight")){
  new_outliers <- fread("FILEPATH")
  df <- get(paste0(measure,"_age_pattern")) %>% as.data.table()
  df <- merge(df, new_outlier, by=c("nid","age_group_id","sex_id","location_id","year_id"))
  assign(paste0(measure,"_age_pattern"),df)
}

## Add on additional necessary variables
overweight_age_pattern[, seq := .I][, underlying_field_citation_value:= NA]
obese_age_pattern[, seq := .I][, underlying_field_citation_value:= NA]
underweight_age_pattern[, seq := .I][, underlying_field_citation_value:= NA]
severe_underweight_age_pattern[, seq := .I][, underlying_field_citation_value:= NA]

## Add on measure_id variable
overweight_age_pattern[, measure_id :=18]
obese_age_pattern[, measure_id :=18]
underweight_age_pattern[, measure_id :=18]
severe_underweight_age_pattern[, measure_id :=18]

fwrite(overweight_age_pattern, paste0(save_dir, "overweight_age_pattern.csv"))
fwrite(obese_age_pattern, paste0(save_dir, "obese_age_pattern.csv"))
fwrite(underweight_age_pattern, paste0(save_dir, "underweight_age_pattern.csv"))
fwrite(severe_underweight_age_pattern, paste0(save_dir, "severe_underweight_age_pattern.csv"))

