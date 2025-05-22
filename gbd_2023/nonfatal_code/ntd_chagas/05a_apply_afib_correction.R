### apply new  split to afib draws
rm(list=ls())

library(data.table)
library(dplyr)
library(openxlsx)
library(stringr)
library(readxl)

source("FILEPATH/get_bundle_data.R")
source("FILEPATH/upload_bundle_data.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_crosswalk_version.R")
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/get_population.R")
source("FILEPATH/sex_split_function_birds.R")
date <- Sys.Date()

#read in
files <- list.files(paste0("FILEPATH"), pattern = ".csv", full.names = TRUE)
temp <- lapply(files, fread, sep=",")
afib <- rbindlist(temp, use.names=TRUE, fill = TRUE)
rm(temp)

afib$id <- NULL

vars <- paste0("draw_", 0:999)
cols <- colnames(afib)
cols <- cols[!(cols %in% vars)]

afib <- afib[, paste0("new_draw_", 0:999) := lapply(0:999, function(x) (get(paste0("draw_", x))) * 0.4), by = cols]

# get rid of ss
afib[, paste0("draw_", 0:999) := NULL] # drop draw_ cols
setnames(afib, paste0("new_draw_", 0:999), paste0("draw_", 0:999))
#save out
i  <- 1
for (loc in unique(afib$location_id)){
  message(paste0("saving: ", loc,"; ",i,  "/", length(unique(afib$location_id))))
  sub <- afib[afib$location_id == loc,]
  fwrite(sub, paste0("FILEPATH"))
  i  <- i +  1
}

