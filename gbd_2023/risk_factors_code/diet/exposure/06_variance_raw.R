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
j <- if (os == "Linux") "FILEPATTH/" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "H:/"

# Arguments/Variables (interactive is mainly for running the script individually)
if(interactive()) {
  gbd_round <- 'gbd2022'
  version <- '2022'
  id <- 130
 } else {
  args <- commandArgs(trailingOnly = TRUE)
  gbd_round <- args[1]
  version <- args[2]
  map_path <- args[3]
  task_id <- Sys.getenv("SLURM_ARRAY_TASK_ID")
  params <- fread(map_path)
  print(args)
  print(task_id)
  id <- params[job_ids == task_id, loc_ids]
}


data_path <- paste0("FILEPATH") # this is generally where data will be output throughout the pipeline

input_f<- paste0("FILEPATH")
output_f <- paste0("FILEPATH")

dir.create(paste0(output_f))

message(paste0("THE LOCATION ID IS ", id))
message(paste0("THE INPUT FOLDER IS ", input_f))
message(paste0("THE OUTPUT FOLDER IS ", output_f))

# Load data
in_data <- fread(paste0(input_f, "/FAO_all_", version, ".csv")) %>%
  filter(location_id == id, grams_daily_unadj != 0, !is.na(grams_daily_unadj))

# Copy loaded in data to avoid having to load it in every time for testing
data <- in_data

# Filter your data
data <- data[grams_daily_unadj != 0 & !is.na(grams_daily_unadj)]

# Initialize new columns
data$variance <- NA
data$cv <- NA
data$sd_resid <- NA

# Get unique foods
foods <- unique(data$gbd_cause)

# Loop over foods
for(food in foods){
  
  # Filter data for the current food
  food_data <- data[data$gbd_cause == food,]
  
 
  # Generate SD of residuals using a lowess regression
  pred <- try({fitted(locfit(food_data$grams_daily_unadj ~ lp(food_data$year, nn=0.5)))})

  # If an error occurred, skip to the next iteration
  if(class(pred) == "try-error") {
    print(paste("An error occurred with input:", food, ". Skipping to next iteration."))
    pred <- mean(food_data$grams_daily_unadj)
    # next
  }
  
  diff <- food_data$grams_daily_unadj - pred
  
  # Get min and max years so that they aren't hardcoded in the loop
  year_min <- min(food_data$year)
  year_max <- max(food_data$year)
  

  # Loop over years
  for(i in (year_min-1):year_max){ 
    # get sd based on diff values -5 and +5 the current year
    temp_sd <- sd(diff[food_data$year %in% (i-5):(i+5)])
    
    # assign temp_sd to sd_resid for the current year
    data$sd_resid[data$year == i & data$gbd_cause == food] <- temp_sd
  }

  for(j in (year_min-1):(year_min+4)){
    temp_sd <- sd(diff[food_data$year %in% (year_min-1):(j+10-(j-(year_min-1)))])
    data$sd_resid[data$year == j & data$gbd_cause == food] <- temp_sd
  }

  for(k in (year_max-6):year_max){
    temp_sd <- sd(diff[food_data$year %in% (k-10+(year_max-k)):year_max])
    data$sd_resid[data$year == k & data$gbd_cause == food] <- temp_sd
  }

  data$variance[data$gbd_cause == food] <- data$sd_resid[data$gbd_cause == food] ^ 2
  data$cv[data$gbd_cause == food] <- sqrt(data$variance[data$gbd_cause == food]) / data$grams_daily_unadj[data$gbd_cause == food]
  
  rm(pred, diff)
}

# Save the data
write_csv(data, paste(output_f, id, "_variance_unadj.csv", sep = ""))
