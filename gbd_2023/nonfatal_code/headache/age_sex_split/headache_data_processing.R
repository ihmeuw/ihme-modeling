###########################################################
### Project: Data Processing Pipeline
### Purpose: GBD 2019 Nonfatal Estimation
###########################################################

## SET-UP
rm(list=ls())

pacman::p_load(data.table, ggplot2, boot)
library(openxlsx, lib.loc = paste0("FILEPATH"))

date <- gsub("-", "_", Sys.Date())

## SOURCE FUNCTION
source(paste0("FILEPATH"))
source(paste0("FILEPATH"))
source(paste0("FILEPATH"))

## GET MAP
map <- fread(paste0("FILEPATH", "bundle_map.csv"))

## READ DATA
all_data <- lapply(map[adjust == "unadjusted", bundle_id], read_data) 
  name <- map[adjust == "unadjusted", bundle_name][i]
  print(paste0("assigning name for ", name))
  assign(name, as.data.table(all_data[i])) 
}

## AGE SEX SPLIT 
step2 <- lapply(map[adjust == "unadjusted", bundle_name], age_sex_split) 
for (i in 1:length(step2)){
  name <- paste0(map[adjust == "unadjusted", bundle_name][i], "_2")
  print(paste0("assigning name for ", name))
  assign(name, as.data.table(step2[i]))
}

## APPLY SEX PATTERN IF NECESSARY 
step3 <- lapply(map[adjust == "unadjusted", bundle_name], sex_split) 
for (i in 1:length(step3)){
  name <- paste0(map[adjust == "unadjusted", bundle_name][i], "_3")
  print(paste0("assigning name for ", name))
  assign(name, as.data.table(step3[i]))
}

## APPLY AGE PATTERN IF NECESSARY 
step4 <- lapply(map[adjust == "unadjusted", bundle_name], age_split_fromsource) 
for (i in 1:length(step4)){
  name <- paste0(map[adjust == "unadjusted", bundle_name][i], "_4")
  print(paste0("assigning name for ", name))
  assign(name, as.data.table(step4[i]))
}

## APPLY AGE-SEX PATTERN IF NECESSARY 
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

# ## CLEAR OUT BUNDLES
lapply(map[adjust == "adjusted", bundle_name], delete_previous_data)

# ## UPLOAD NEW DATA
lapply(map[adjust == "unadjusted", bundle_name], function(x) upload_data(name = x, file_name = "FILEPATH"))

