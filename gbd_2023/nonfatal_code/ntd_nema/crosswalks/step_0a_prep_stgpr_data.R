###==============================================###
##
## Purpose:Prep ST-GPR data
##
###==============================================###
rm(list = ls())

# Shared functions
j <- "FILEPATH"
h <- "FILEPATH"

data_root <- '/FILEPATH'
params_dir <- paste0(data_root, "/FILEPATH")
date <- Sys.Date()

library(data.table)
library(ggplot2)
library(dplyr)
library(viridis)
library(tidyr)
library(scales)
library(openxlsx)
library(readxl)
library(tidyverse)
library(viridisLite)
library(gridExtra)
library(stringr)
library(fuzzyjoin)
library(stringdist)
source("/FILEPATH/get_ids.R")
source("/FILEPATH/get_age_metadata.R")
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/get_demographics.R")
source("/FILEPATH/get_population.R")
source("/FILEPATH/get_bundle_data.R")
source("/FILEPATH/get_bundle_version.R")
source("/FILEPATH/get_crosswalk_version.R")
source("/FILEPATH/save_bundle_version.R")
source("/FILEPATH/save_crosswalk_version.R")
source("/FILEPATH/processing.R")

release_id <- ADDRESS

bdl_stgpr <- ADDRESS
cw_vsn <- ADDRESS
worm <- 'ascariasis'
worm_name_short <- 'ascar'

bdl_stgpr <- ADDRESS
cw_vsn <- ADDRESS
worm <- 'hookworm'
worm_name_short <- 'hook'

bdl_stgpr <- ADDRESS
cw_vsn <- ADDRESS
worm <- 'trichuriasis'
worm_name_short <- 'trichur'

#####============================#####
##==       TRANSFORM DATA         ==##
#####============================#####
transform_dismod_to_stgpr <- function(in_dismod_data, worm) {
  
  stgpr_data <- copy(in_dismod_data)
  if (!(worm %in% c('ascariasis', 'trichuriasis', 'hookworm'))){ stop('only for worm in ascariasis, trichuriasis, hookworm')}
  
  if (worm == "ascariasis"){
    my_start  <- 0
    my_end    <- 16
  } else if (worm == "hookworm"){
    my_start  <- 5
    my_end    <- 20
  } else if (worm == "trichuriasis"){
    my_start  <- 5
    my_end    <- 20
  }
  
  stgpr_data <- stgpr_data[, ages := paste0(age_start, "_", age_end)]
  stgpr_data <- stgpr_data[is_outlier == 0]
  stgpr_data <- stgpr_data[, c("sex", "underlying_nid", "ihme_loc_id", "location_id", "age_start", "age_end", "ages", "year_start", "nid", "mean", "cases", "sample_size",
                               'rowid_orig', 'source_espen', 'rowid_espen', 'source', 'rowid_gbd', 'dx_num_duplicates', 'dx_num_samples', 'case_diagnostics','site_memo')]
  stgpr_data <- stgpr_data[age_start >= my_start & age_end <= my_end]
  
  ## Prep for ST-GPR
  stgpr_data[, age_group_id := 22]
  stgpr_data[, year_id := year_start]
  stgpr_data[, sex_id  := 3]
  
  stgpr_data[, val := mean]
  stgpr_data[, variance := (val * (1 - val) / sample_size)]
  stgpr_data <- stgpr_data[variance != 0]
  stgpr_data[, me_name := ifelse(worm == 'ascariasis', 'Ascariasis', ifelse(worm == 'trichuriasis', 'Trichuriasis', ifelse(worm == 'hookworm', 'Hookworm', stop('only for worm in ascariasis, trichuriasis, hookworm'))))]
  stgpr_data <- stgpr_data[val != 0]
  out_stgpr_data <- stgpr_data[, c("sex", "underlying_nid", "ihme_loc_id", "location_id", "nid", "year_id", "sex_id", "age_group_id", "me_name", "val", "variance", "sample_size",
                                   'rowid_orig', 'source_espen', 'rowid_espen', 'source', 'rowid_gbd', 'dx_num_duplicates', 'dx_num_samples', 'case_diagnostics','site_memo')]
  out_stgpr_data[, seq := NA]
  out_stgpr_data[, is_outlier := 0]
  out_stgpr_data[, measure := 'proportion']
  if (worm == "ascariasis") stgpr_data <- stgpr_data[location_id != ADDRESS]
  
  return(out_stgpr_data)
}

transform_dismod_to_stgpr_who <- function(in_dismod_data, worm) {
  
  stgpr_data <- copy(in_dismod_data)
  if (!(worm %in% c('ascariasis', 'trichuriasis', 'hookworm'))){ stop('only for worm in ascariasis, trichuriasis, hookworm')}
  
  stgpr_data <- stgpr_data[, ages := paste0(age_start, "_", age_end)]
  stgpr_data <- stgpr_data[is_outlier == 0]
  stgpr_data <- stgpr_data[, c("sex", "underlying_nid", "ihme_loc_id", "location_id", "age_start", "age_end", "ages", "year_start", "nid", "mean", "cases", "sample_size",
                               'rowid_orig', 'source_espen', 'rowid_espen', 'source', 'rowid_gbd', 'dx_num_duplicates', 'dx_num_samples', 'case_diagnostics','site_memo')]

  ## Prep for ST-GPR
  stgpr_data[, age_group_id := 22]
  stgpr_data[, year_id := year_start]
  stgpr_data[, sex_id  := 3]
  
  stgpr_data[, val := mean]
  stgpr_data[, variance := (val * (1 - val) / sample_size)]
  stgpr_data <- stgpr_data[variance != 0]
  stgpr_data[, me_name := ifelse(worm == 'ascariasis', 'Ascariasis', ifelse(worm == 'trichuriasis', 'Trichuriasis', ifelse(worm == 'hookworm', 'Hookworm', stop('only for worm in ascariasis, trichuriasis, hookworm'))))]
  stgpr_data <- stgpr_data[val != 0]
  out_stgpr_data <- stgpr_data[, c("sex", "underlying_nid", "ihme_loc_id", "location_id", "nid", "year_id", "sex_id", "age_group_id", "me_name", "val", "variance", "sample_size",
                                   'rowid_orig', 'source_espen', 'rowid_espen', 'source', 'rowid_gbd', 'dx_num_duplicates', 'dx_num_samples', 'case_diagnostics','site_memo')]
  out_stgpr_data[, seq := NA]
  out_stgpr_data[, is_outlier := 0]
  out_stgpr_data[, measure := 'proportion']
  if (worm == "ascariasis") stgpr_data <- stgpr_data[location_id != 97]
  
  return(out_stgpr_data)
}

###------- dismod to stgpr -------###

data_cw <- get_crosswalk_version(cw_vsn)

if (worm != 'ascariasis'){
  
  espen <- subset(data_cw, source == 'espen' & age_start == 1 & age_end == 14)
  rows_to_bind <- setdiff(data_cw,espen)

  espen$age_start <- 5
  espen$age_end <- 20

  data_cw <- rbind(rows_to_bind,espen)
  
}

data_who <- subset(data_cw, source == 'who')
data_transform <- setdiff(data_cw,data_who)

data_stgpr <- transform_dismod_to_stgpr(in_dismod_data = data_transform, worm = worm)
data_who <- transform_dismod_to_stgpr_who(in_dismod_data = data_who, worm = worm)

data_stgpr2 <- rbind(data_who,data_stgpr)

description <- 'COMMENT'
write.csv(data_stgpr2, paste0(j, 'FILEPATH.csv'),row.names = FALSE)


source("/FILEPATH/get_version_quota.R")
df <- get_version_quota(bundle_id = bdl_stgpr)  
data <- get_bundle_version(bundle_version_id = ADDRESS, fetch = 'all') 


cols <- colnames(data)
cols <- setdiff(cols, 'seq')
for(i in cols){
  data[,i] <- ""
}

write.xlsx(data, paste0('FILEPATH.xlsx'), sheetName = 'extraction')

###########################

bdl_vsn <- save_bundle_version(bundle_id = bdl_stgpr)
print(bdl_vsn$bundle_version_id)  

cw_data <- get_bundle_version(bundle_version_id = ADDRESS, fetch = 'all')  
cw_who <- subset(cw_data, source == 'who')
cw_outlier <- setdiff(cw_data,cw_who)

cw_outlier$is_outlier <- ifelse(grepl("Kato", cw_outlier$case_diagnostics), 0, 1)
cw_outlier$is_outlier <- ifelse(grepl("kk", cw_outlier$case_diagnostics), 0, cw_outlier$is_outlier)

cw_data <- rbind(cw_who,cw_outlier)

nids_to_outlier <- c(ADDRESS)  
cw_data$is_outlier <- ifelse(cw_data$nid %in% nids_to_outlier, 1, cw_data$is_outlier)

seq_to_outlier <- c(ADDRESS)  
cw_data$is_outlier <- ifelse(cw_data$seq %in% seq_to_outlier, 1, cw_data$is_outlier)
cw_data$is_outlier <- ifelse(cw_data$location_id == ADDRESS & cw_data$year_start < 1990, 1, cw_data$is_outlier)  
cw_data$is_outlier <- ifelse(cw_data$location_id == ADDRESS & cw_data$year_start < 1990, 1, cw_data$is_outlier) 

nids_to_outlier <- c(ADDRESS)
cw_data$is_outlier <- ifelse(cw_data$underlying_nid %in% nids_to_outlier, 1, cw_data$is_outlier)

cw_data$is_outlier <- ifelse(cw_data$location_id == ADDRESS & cw_data$year_start == 2001, 1, cw_data$is_outlier) 
cw_data$is_outlier <- ifelse(cw_data$location_id == ADDRESS & cw_data$year_start %in% c(1984,1991), 1, cw_data$is_outlier) 


cw_description <- 'COMMENT'
cw_data[, crosswalk_parent_seq := seq]
cw_data[, seq := NA]
cw_data <- cw_data[sex == 'Both']
cw_data <- cw_data[is_outlier == 0]
cw_data[, unit_value_as_published := 1]
write.xlsx(cw_data, paste0(params_dir, '/FILEPATH.xlsx'), sheetName = 'extraction')
save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = paste0(params_dir, '/FILEPATH.xlsx'), description = cw_description)
