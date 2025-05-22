rm(list = ls())

# Libraries
library(data.table)
library(tidyverse)
library(dplyr)
library(haven) # for reading in dta files
library(readxl)
library(locfit)

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "H:/"

# Arguments/Variables (interactive is mainly for running the script individually)
if(interactive()) {
  gbd_round <- 'gbd2022'
  version <- '2022'
  id <- 26
} else {
  args <- commandArgs(trailingOnly = TRUE)
  gbd_round <- args[1]
  version <- args[2]
  task_id <- Sys.getenv("SLURM_ARRAY_TASK_ID")
  print(args)
  print(task_id)
}

data_path <- paste0("FILEPATH") # this is generally where data will be output throughout the pipeline
input_f <- ("FILEPATH")
output_f <- paste0( "FILEPATH/")

# for testing
og_output <- read_dta("FILEPATH")
og_output_afg <- og_output %>% filter(location_name == 'Afghanistan')

message(paste0("THE INPUT FOLDER IS ", input_f))
message(paste0("THE OUTPUT FOLDER IS ", output_f))

#### Figure out get_location_metadata code for R ####
source("FILEPATH/get_location_metadata.R")
df <- get_location_metadata(location_set_id = 22, release_id=6)

# Subsetting the data
df <- df[df$is_estimate == 1,]
df <- df[, c('location_id', 'region_name', 'ihme_loc_id')]

# Assuming 'input_f' is the directory where your files are located
file_list <- list.files(path = input_f, pattern = "*variance_unadj.csv", full.names = TRUE)

# Appending all files in the directory
df_combined <- rbindlist(lapply(file_list, fread))

# We can replace this from get_location_metadata output

df_combined <- dplyr::select(df_combined, -ihme_loc_id)

# Merging datasets
df_final <- merge(df, df_combined, by = "location_id") 

# Creating new variables and replacing some

df_final <- df_final %>%
  group_by(region_name, gbd_cause) %>%
  mutate(region_var_max = max(variance))

df_final <- df_final %>%
  group_by(location_id, gbd_cause, grams_daily_unadj) %>%
  mutate(duplicate = ifelse(n()==1,0,n()),
         counter = dplyr::n())

df_final <- df_final %>%
  mutate(flag = ifelse(duplicate == counter, 1, 0))

df_final$variance[df_final$flag == 1] <- df_final$region_var_max[df_final$flag == 1]

# clean up dataframe
df_final <- dplyr::select(df_final, c("ihme_loc_id", "year", "grams_daily_unadj",
                                      "variance", "cv", "sd_resid", "nid", "gbd_cause",
                                      "location_name", "location_id", "ihme_risk",
                                      "total_calories", "duplicate"))

# Save the data
write_csv(df_final, paste(output_f, "FAO_sales_raw_data_variance_unadj_", version, ".csv", sep = ""))
