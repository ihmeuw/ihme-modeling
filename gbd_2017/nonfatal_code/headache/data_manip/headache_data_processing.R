###########################################################
### Author: 
### Date: 12/12/17
### Project: Data Processing Pipeline
### Purpose: GBD 2017 Nonfatal Estimation
###########################################################

## SET-UP
rm(list=ls())

if (Sys.info()[1] == "Linux"){
  j_root <- "/home/j/"
  h_root <- "~/"
} else if (Sys.info()[1] == "Windows"){
  j_root <- "J:/"
  h_root <- "H:/"
}

pacman::p_load(data.table, ggplot2, boot)
library(openxlsx, lib.loc = paste0(j_root, FILEPATH))

## SET OBJECTS
repo_dir <- paste0(h_root, FILEPATH)
functions_dir <- paste0(j_root, FILEPATH)
date <- gsub("-", "_", Sys.Date())

## SOURCE FUNCTION
source(paste0(functions_dir, "get_epi_data.R"))
source(paste0(functions_dir, "upload_epi_data.R"))
source(paste0(repo_dir, "data_processing_functions.R"))

## GET MAP
map <- fread(paste0(repo_dir, FILEPATH))

## READ DATA
all_data <- lapply(map[adjust == "unadjusted", bundle_id], read_data)
for (i in 1:length(all_data)){
  name <- map[adjust == "unadjusted", bundle_name][i]
  print(paste0("assigning name for ", name))
  assign(name, as.data.table(all_data[i]))
}

## SPLIT ETHIOPIA STUDY (STEP 1)
source(paste0(repo_dir, FILEPATH))
step1 <- lapply(map[adjust == "unadjusted", bundle_name], return_ethiopia)
for (i in 1:length(step1)){
  name <- paste0(map[adjust == "unadjusted", bundle_name][i], "_1")
  print(paste0("assigning name for ", name))
  assign(name, as.data.table(step1[i]))
}

## AGE SEX SPLIT (STEP 2)
step2 <- lapply(map[adjust == "unadjusted", bundle_name], age_sex_split)
for (i in 1:length(step2)){
  name <- paste0(map[adjust == "unadjusted", bundle_name][i], "_2")
  print(paste0("assigning name for ", name))
  assign(name, as.data.table(step2[i]))
}

## APPLY SEX PATTERN IF NECESSARY (STEP 3)
step3 <- lapply(map[adjust == "unadjusted", bundle_name], sex_split)
for (i in 1:length(step3)){
  name <- paste0(map[adjust == "unadjusted", bundle_name][i], "_3")
  print(paste0("assigning name for ", name))
  assign(name, as.data.table(step3[i]))
}

## APPLY AGE PATTERN IF NECESSARY (STEP 4)
step4 <- lapply(map[adjust == "unadjusted", bundle_name], age_split_fromsource)
for (i in 1:length(step4)){
  name <- paste0(map[adjust == "unadjusted", bundle_name][i], "_4")
  print(paste0("assigning name for ", name))
  assign(name, as.data.table(step4[i]))
}

## APPLY AGE-SEX PATTERN IF NECESSARY (STEP 5)
step5 <- lapply(map[adjust == "unadjusted", bundle_name], age_sex_split_type)
for (i in 1:length(step5)){
  name <- paste0(map[adjust == "unadjusted", bundle_name][i], "_5")
  print(paste0("assigning name for ", name))
  assign(name, as.data.table(step5[i]))
}

## ADD/SUBTRACT TO CREATE THIRD CATEGORY
step6 <- lapply(map[adjust == "unadjusted", bundle_name], add_sub)
for (i in 1:length(step6)){
  name <- paste0(map[adjust == "unadjusted", bundle_name][i], "_6")
  print(paste0("assigning name for ", name))
  assign(name, as.data.table(step6[i]))
}

## OUTLIER DATA
step7 <- lapply(map[adjust == "unadjusted", bundle_name], add_outliers)
for (i in 1:length(step7)){
  name <- paste0(map[adjust == "unadjusted", bundle_name][i], "_7")
  print(paste0("assigning name for ", name))
  assign(name, as.data.table(step7[i]))
}

## CLEAR OUT BUNDLES
lapply(map[adjust == "adjusted", bundle_name], delete_previous_data)

## UPLOAD NEW DATA
lapply(map[adjust == "unadjusted", bundle_name], function(x) upload_data(name = x, file_name = "collapse_turkey"))

## AFTERWARDS, NEED TO AGE SPLIT USING HEADACHE AGE SPLIT CODE- CORRECT MODELS NEED TO BE MARKED BEST
