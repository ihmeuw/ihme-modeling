################################################################################
# Purpose: Chagas disease sex crosswalk

# Details: Sex splitting, run mr-brt fit model to sex specific data, and apply the model to both sexes data.
# Using BIRDS team sex split function
################################################################################
### ----------------------- Set-Up ------------------------------

rm(list=ls())

source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_population.R")
source('FILEPATH/save_bundle_version.R')
source('FILEPATH/save_crosswalk_version.R')
source('FILEPATH/get_crosswalk_version.R')
source("FILEPATH/sex_split_function_birds.R")

library(crosswalk, lib.loc = "FILEPATH")
library(dplyr)
library(data.table)
library(openxlsx)
library(stringr)
library(tidyr)
library(boot)
library(ggplot2)

################################################################################
## Set-up run directory

crosswalks_dir    <- paste0("FILEPATH")

### ----------------------- Consolidate Data ------------------------------
################################################################################
# 1. Get data data and split train data and data to split
################################################################################

# using cleaned up data
all_data <- read.csv("FILEPATH/chagas_data_corrected_agg_5124.csv")

data <- as.data.table(all_data)

#' [Direct Matches]
# sex specific rows
data <- data[sex != "Both"]

# subset out 0 cases here so pair does not end up in
data <- data[cases != 0]
data<- subset(data, sample_size!=cases)

dat_original <- copy(data)
dat_original[, match := str_c( age_start, age_end, location_name, year_start, year_end)]

dm      <- dat_original[, .N, by = c( "age_start", "age_end", "location_name", "year_start", "year_end")][N > 1]
dm      <- dm[N == 2]
matches <- dm[, str_c(age_start, age_end, location_name, year_start, year_end)]
matches <- matches[!(is.na(matches))]

# subset to the matches 
data_ss_subset <- dat_original[match %in% matches]

# sort
setorder(data_ss_subset, year_start, year_end, age_start, age_end, location_name, site_memo, case_diagnostics)

# correct names
data_ss_m <- data_ss_subset[sex == "Male", ]
data_ss_f <- data_ss_subset[sex == "Female",]

# validate -- if all true then ready to clean and prep for mrbrt
all(data_ss_m[, .(nid, year_start, year_end, age_start, age_end, location_name )] == data_ss_f[, .(nid, year_start, year_end, age_start, age_end, location_name)])

# merge together male and female data from same population-
data_merged<-merge(data_ss_m,data_ss_f,by=c("nid","location_name", "location_id","year_start","age_start","age_end"))
data_merged <- data_merged[cases.x != 0]
data_merged <- data_merged[cases.y != 0]

## keep mean, se, location_id, year_start, nid, site_memo
# note X and mean_1=male
data_merged<-data_merged %>% 
  select(nid, location_id, year_start, age_start, age_end, cases.x, cases.y, effective_sample_size.x, effective_sample_size.y, sex.x, sex.y,  mean.x, mean.y, standard_error.x, standard_error.y) %>%
  rename(mean_1 = mean.x,mean_2 = mean.y, se_1=standard_error.x, se_2=standard_error.y, cases_1=cases.x, cases_2=cases.y, sample_size_1=effective_sample_size.x, sample_size_2=effective_sample_size.y, altvar=sex.x, refvar=sex.y)

# create an index of matches
setDT(data_merged)
data_merged[, id := .I]

# compare males:females by year
ggplot(data_merged, aes(x=mean_1, y=mean_2, color=age_start)) +
  geom_point() +
  ggtitle("Chagas disease- Prevalence Male vs Females") +
  xlab("Males") + ylab("Females") +
  geom_abline(slope=1, intercept = 0) +
  geom_smooth(method = "lm")

# write training data
data_ss_subset[, splitting := 0] #marking this data as not sex_split

# outliering data with sample size <5
data_ss_subset <- subset(data_ss_subset, sample_size >5)
write.csv(data_ss_subset, paste0(crosswalks_dir, "FILEPATH"))

################################################################################
# 2. CHECK IF COLUMN SPLITTING IS TAGGED CORRECTLY
################################################################################
# check unique values in splitting column
# using cleaned up data
all_data <- fread("FILEPATH")

df <- copy(all_data)

df[ sex == "Both", splitting := 2]
df[ sex != "Both", splitting := 0]

table(df$splitting)

# excluding data with sample_size <=5
df_sex_tosplit <- subset(df_sex_tosplit, sample_size >5)
write.csv(df_sex_tosplit, paste0(crosswalks_dir,"FILEPATH"))

################################################################################
# 3. SEX SPLIT
################################################################################
##
### ----------------------- Model Fit + Params ------------------------------
sex_split(topic_name ="ntd_chagas", 
          output_dir = paste0(crosswalks_dir,"FILEPATH"), 
          bundle_version_id = NULL, 
          data_all_csv = NULL, 
          data_to_split_csv = paste0(crosswalks_dir,"FILEPATH"), 
          data_raw_sex_specific_csv = paste0(crosswalks_dir, "FILEPATH"), 
          nids_to_drop = c(), 
          cv_drop = c(), 
          mrbrt_model = NULL, 
          mrbrt_model_age_cv = F,  
          release_id = ADDRESS, 
          measure = "ADDRESS",
          vetting_plots = TRUE )

################
# Post adjustment  estimate cases and sample size based on function from mea se, and correction for 0 cases - 0 se. 
#### 
post_split_folder <- "FILEPATH"

calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt$cases <- as.numeric(dt$cases)
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := as.numeric( (mean*(1-mean)/standard_error^2))]
  dt[is.na(cases), cases := as.numeric(mean * sample_size)]
  return(dt)
}

sex_split_data <- fread(paste0(post_split_folder, "FILEPATH"))

sex_split_data2 <- calculate_cases_fromse(sex_split_data)

sex_split_data2[mean ==0 & is.na(cases), cases :=0]

### estimate lower and upper

quantile = 0.975
z = qnorm(quantile)
sex_split_data2[mean==0, standard_error := sqrt(mean * (1 - mean) / sample_size + z ** 2 / (4 * sample_size ** 2))]

# Upper from standard error
confidence = 0.95 
quantile = 1 - (1 - confidence) / 2   #0.975

# if type = “proportion”, upper has a ceiling of 1
sex_split_data2[, upper := mean + qnorm(quantile) * standard_error]
sex_split_data2[upper >1,upper :=1 ]

# Lower from standard error
confidence = 0.95 
quantile = (1 - confidence) / 2 

sex_split_data2[, lower := mean + qnorm(quantile) * standard_error]
sex_split_data2[lower <0 ,lower := 0 ]
sex_split_data2[, effective_sample_size := sample_size]

### combine with data that was not splitted
non_splitdata <- copy(all_data)
non_splitdata <- subset(non_splitdata, sex != "Both")

final_datacomb <- rbind(non_splitdata, sex_split_data2 , fill=T)

write.csv(final_datacomb,paste0(crosswalks_dir,"FILEPATH"))
