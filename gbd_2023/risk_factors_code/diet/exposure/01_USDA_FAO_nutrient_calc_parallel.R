rm(list = ls())

# Libraries
library(data.table)
library(tidyverse)
library(dplyr)
library(haven) # for reading in dta files
library(readxl)

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FIELPATH/" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("FIELPATH", user, "/") else if (os == "Windows") "H:/"

# Arguments/Variables (interactive is mainly for running the script individually)
if(interactive()) {
  print("Running interactively")
  gbd_round <- 'gbd2022'
  version <- '2022'
  task_id <-1
  print(paste0("Task ID is: ", task_id))
  params <- fread(map_path)
  # print(args)
  print(task_id)
  id <- params[job_ids == task_id, loc_ids]
  print(id)
} else {
  print("Launched in parallel")
  args <- commandArgs(trailingOnly = TRUE)
  print(paste0("Here are the args: ", args))
  gbd_round <- args[1]
  print(paste0('arg1 is: ', gbd_round))
  release_id <- args[2]
  print(paste0('arg2 is: ', release_id))
  version <- args[3]
  print(paste0('arg3 is: ', version))
  map_path <- args[4]
  print(paste0('map_path is: ', map_path))
  task_id <- Sys.getenv("SLURM_ARRAY_TASK_ID")
  print(paste0("Task ID is: ", task_id))
  params <- fread(map_path)
  print(args)
  print(task_id)
  id <- params[job_ids == task_id, loc_ids]
  print(id)
}

#Paths

data_path <- paste0("FILEPATH") # this is generally where data will be output throughout the pipeline
output_path <- paste0("FILEPATH")
dir.create(output_path)

# SUA dataset from 00 script
sua_data <- read_csv(paste0(data_path, "simple_sua.csv"))
relevant_codes <- read_xlsx(paste0(data_path, "nutrient_codes.xlsx"), sheet = 'nutrient_codes') %>% setDT()

# Import USDA Nutrient Data
usda_nutrients_data <- read_csv("FILEPATH")

usda_nutrients_data[3:152] <- lapply(usda_nutrients_data[3:152], as.numeric)

sua_data$ndb_no[sua_data$ndb_no == 5163] <- 5165 # turkey meat
sua_data$ndb_no[sua_data$ndb_no == 1147] <- 1044 # processed cheese to next best option (Swiss)
sua_data$ndb_no[sua_data$ndb_no == 4071] <- 4655 # margarine + shortening
sua_data$ndb_no[sua_data$ndb_no == 4107] <- 4073 # liquid margarine
sua_data$ndb_no[sua_data$ndb_no == 9187] <- 43216 # pure fructose
sua_data$ndb_no[sua_data$ndb_no == 18099] <- 18017 # mixes + dough
sua_data$ndb_no[sua_data$ndb_no == 14342] <- 43479 # rice fermented beverages
sua_data$ndb_no[sua_data$ndb_no == 20099] <- 20105 # macaroni
sua_data$ndb_no[sua_data$ndb_no == 9188] <- 9288 # canned prunes
sua_data$ndb_no[sua_data$ndb_no == 20581] <- 20481 #enriched flour to unenriched flour
# Fish recodes
sua_data$ndb_no[sua_data$products == "Freshwater Fish"] <- 15024
sua_data$ndb_no[sua_data$products == "Demersal Fish"] <- 15015
sua_data$ndb_no[sua_data$products == "Pelagic Fish"] <- 15121
sua_data$ndb_no[sua_data$products == "Marine Fish, Other"] <- 15027
sua_data$ndb_no[sua_data$products == "Cephalopods"] <- 15166
sua_data$ndb_no[sua_data$products == "Crustaceans"] <- 15149
sua_data$ndb_no[sua_data$products == "Molluscs, Other"] <- 15164
sua_data$ndb_no[sua_data$products == "Aquatic Animals, Other"] <- 15229

# These are the years in which data is available
yrs <- sort(unique(sua_data$year))
loc_ids <- unique(sua_data$location_id)
ctrys <- unique(sua_data$countries)
rlvnt <- relevant_codes$code

country_counter <- 1
sua_data_country <- filter(sua_data, location_id == id) # only keep data for current country
message(paste0("Processing ", " (location id: ", id, ")"))
year_counter <- 1
df<- data.frame()
for (yr in yrs) { # loop over years
# browser()
  sua_data_country_year <- filter(sua_data_country, year == yr) # filter by year in loop
  merged_data <- merge(sua_data_country_year, usda_nutrients_data, by.x = "ndb_no", by.y = "NDB_No") # merge sua data with nutrients data

  for (nutr_code in rlvnt) { # loop over each nutrient code
    nutr_name_and_unit <- subset(relevant_codes, nutr_code == code, select = c(nutrient, units)) # get nutrient name
    est_name <- paste0('est_', nutr_name_and_unit[[1]], '_', nutr_name_and_unit[[2]]) # create column name for est calculation
    sum_name <- paste0(nutr_name_and_unit[[1]], '_', nutr_name_and_unit[[2]], '_sum') # create column name for sum calculation
    
    merged_data <- merged_data %>% 
      mutate(new_col = data * merged_data[[paste0(nutr_code)]]) %>% # create a new column containing the recalculation
      mutate(sum_col = sum(new_col, na.rm = TRUE)) %>% # create a new column containing the sum of the previous column
      rename_with(~c(est_name, sum_name), c(new_col, sum_col)) # assign previously defined names to columns
  }
  
  if (year_counter == 1) { # save initial dataframe
    df <- merged_data
  } else { # combine subsequent dataframes with initial
    df <- rbind(df, merged_data)
  }
  
  year_counter <- year_counter + 1 # add to counter after each location_id is complete
  
}



write_csv(df, paste0(output_path, "nutrients_est_",id,"_by_item_",version,".csv"))
message(paste0("Processed ", "location id: ", id, " Saved at: ", paste0(output_path, "nutrients_est_",id,"_by_item_",version,".csv")))
