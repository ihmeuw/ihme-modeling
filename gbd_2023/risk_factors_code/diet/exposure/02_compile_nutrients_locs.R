rm(list = ls())

# Libraries
library(data.table)
library(tidyverse)
library(dplyr)
library(haven) # for reading in dta files

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "H:/"

# Arguments/Variables (interactive is for running the script individually)

if(interactive()) {
  gbd_round <- 'gbd2022'
  version <- '2022'
  id <- 132
} else {
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  gbd_round <- args[1]
  release_id <- args[2]
  version <- args[3]
}

# Path to files
data_path <- paste0("FILEPATH")
input_path <- paste0("FILEPATH")
output_path <- paste0("FILEPATH")
dir.create(output_path)
sua_data <- read_csv(paste0(data_path, "simple_sua.csv"))



# get countries
loc_ids <- unique(sua_data$location_id)

counter <- 0

for (id in loc_ids) { # loop over countries
# browser()
  file_use <- read_csv(paste0(input_path, "nutrients_est_", id, "_by_item_", version, ".csv"), show_col_types = FALSE)

  df <- dplyr::select(file_use, c(countries, year, location_id, grep("_sum", names(file_use))))
  
  # keep one row per country + year 
  df <- df %>% distinct(countries, year, .keep_all = TRUE)
  
  if (counter == 0) {
    df_final <- df
  } else {
    df_final <- rbind(df_final, df)
  }

  message(paste0("location_id ", id, " has been compiled. ", nrow(df), " records added."))
  counter <- counter + 1
  
}
write_csv(df_final, paste0(output_path, "compiled_nutrients_", version, "_nadim.csv"))
