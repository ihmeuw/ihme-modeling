rm(list = ls())

# Libraries
library(data.table)
library(tidyverse)
library(dplyr)
library(haven) 
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
  id <- 160
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
output_f <- paste0( "FILEPATH")

# for testing
og_output <- fread("FILEPATH")
og_output_afg <- og_output %>% filter(location_name == 'Afghanistan')

message(paste0("THE INPUT FOLDER IS ", input_f))
message(paste0("THE OUTPUT FOLDER IS ", output_f))

# A "skeleton" file is used for creating a "complete square dataset"
skeleton <- fread("FILEPATH/ready_to_model/calcium_g_unadj.csv") %>%
  dplyr::select(c(location_id, year_id))
skeleton <- distinct(skeleton)

# Reading in 07 output

df <- fread(paste0(input_f,"FAO_sales_raw_data_variance_unadj_", version, ".csv"))
df <- dplyr::rename(df, risk = ihme_risk, year_id = year) 

# Get unique foods
causes <- unique(df$gbd_cause)

# Loop over foods
for(cause in causes){
  df_subset <- df %>%
    filter(gbd_cause == cause) %>%
    dplyr::select(-risk, -total_calories) %>%
    mutate(sex_id = 3,
           age_group_id = 22,
           sample_size = NA,
           is_outlier = 0,
           measure = "continuous",
           me_name = paste0(gbd_cause, "_g_unadj"))
  
  setnames(df_subset, old = 'grams_daily_unadj', new = 'data')
  
  df_subset_with_locid <- merge(df_subset, skeleton, by = c("location_id", "year_id"), all.x = TRUE) 
  
  df_subset_with_locid$gbd_cause <- cause 
  
  df_subset_with_locid <- df_subset_with_locid %>%
    mutate(gbd_cause = case_when(
      gbd_cause == "energy_kcal_unadj" ~ "energy_sua",
      gbd_cause %in% c("cholesterol", "salt", "iron", "magnesium", "phosphorus", "potassium") ~ paste0(cause, "_mg"),
      gbd_cause %in% c("energy", "energy_sua") ~ paste0(cause, "_kcal"),
      gbd_cause %in% c("transfat", "saturated_fats", "pufa", "mufa", "carbohydrates", "protein", "starch", "fats", "hvo_sales") ~ paste0(cause, "_%"), 
      gbd_cause %in% c("vit_a_rae", "vit_a_retinol", "selenium", "folates", "folic_acid") ~ paste0(cause, "_ug"),
      TRUE ~ gbd_cause
    )) %>% 
    mutate(me_name = case_when(
      gbd_cause == "energy_sua" ~ paste0(gbd_cause, "_g_unadj"),
      gbd_cause %in% c(paste0(cause, "_mg"), paste0(cause, "_kcal"), paste0(cause, "_%"), paste0(cause, "_ug"), "vit_a_iu") ~ paste0(gbd_cause, "_unadj"),
      TRUE ~ me_name
    ))
  
  df_subset_with_locid <- df_subset_with_locid %>%
    mutate(data = ifelse(me_name == "energy_kcal_unadj" & data < 100, NA, data)) %>%
    mutate(data = ifelse(ihme_loc_id == "GRD" & year_id == 2004 & gbd_cause == "cholesterol_mg", NA, data))
  
  # if data or variance is empty, it gets dropped 
  df_subset_with_locid <- df_subset_with_locid %>%
    filter(!is.na(variance) | !is.na(data))
  
  
  if(unique(df_subset_with_locid$me_name) == "energy_kcal_unadj") {
    if(cause == "energy") {
      write_csv(df_subset_with_locid, paste0(output_f, cause, "_kcal_unadj.csv"))
    }
    if(cause == "energy_kcal_unadj") {
      write_csv(df_subset_with_locid, paste0(output_f, "energy_sua_kcal_unadj.csv"))
    }
  } else {
    write_csv(df_subset_with_locid, paste0(output_f, unique(df_subset_with_locid$me_name), ".csv"))
  }
    

}
