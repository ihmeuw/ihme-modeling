# date: 08/07/2020
# author: 
# purpose: use for SEX SPLIT crosswalk versions of etiologies for GBD 2020

rm(list=ls())

date <- gsub("-", "_", Sys.Date())
ds <- 'iterative'
gbd_round_id = 7
# sync = T

library(openxlsx); library(data.table); library(parallel); library(magrittr); library(plyr)
# Source all GBD shared functions at once
shared.dir <- "/filepath/"
files.sources <- list.files(shared.dir)
files.sources <- paste0(shared.dir, files.sources) # writes the filepath to each function
invisible(sapply(files.sources, source)) # "invisible suppresses the sapply output


bundle_map <- as.data.table(read.xlsx(paste0('filepath','bundle_map.xlsx')))

dir_2020 <- 'filepath'
cv_tracker <- as.data.table(read.xlsx(paste0(dir_2020, 'crosswalk_version_tracking.xlsx')))

data_dir <- 'filepath'
bundles <- c(29, 33, 37, 41, 7958)
bundles <- c(29,33)
bundle_map <- bundle_map[bundle_id %in% bundles]

for (i in 1:nrow(bundle_map)) {
  
  id <- bundle_map$bundle_id[i] 
  meta <- get_elmo_ids(bundle_id = id, gbd_round_id = gbd_round_id, decomp_step = ds)
  cv_id <- unique(meta[is_best==1 & !modelable_entity_name%like%"squeeze"]$crosswalk_version_id)
  bv_id <- unique(meta[is_best==1 & !modelable_entity_name%like%"squeeze"]$bundle_version_id)
  cv_df <- get_crosswalk_version(cv_id)
  
  out_dir <- paste0(h, "/02_xwalk/gbd_2020/", ds, "/", id, "/")

  # read in the outlier sheet
  outlier_sheet <- fread(paste0("/filepath/etiology_outlier_sheet.csv"))
  cols_keep <- c("nid", as.character(id))
  outlier_sheet <- outlier_sheet[, c(cols_keep), with = FALSE]
  
  id_col <- copy(outlier_sheet[which(outlier_sheet[[2]]==1)])
  
  cv_df[nid %in% id_col$nid, is_outlier := 1]
  
  write.xlsx(cv_df[,.(seq, is_outlier)], paste0(out_dir, date, "_outliers_for_upload.xlsx"), sheetName = "extraction")
    
  # save a crosswalk version
  description <- paste('bulk outlier xwalk version', cv_id, 'on NIDs', paste(unique(id_col$nid), collapse = ", "))
  result <- save_bulk_outlier(crosswalk_version_id = cv_id,
                              filepath = paste0(out_dir, date, "_outliers_for_upload.xlsx"),
                              description = description,
                              gbd_round_id = 7,
                              decomp_step = "iterative")
  
  print(sprintf('Request status: %s', result$request_status))
  print(sprintf('Request ID: %s', result$request_id))
  print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))
  
  if (result$request_status == "Successful") {
    df_tmp <- data.table(bundle_id = id,
                         bundle_version_id = bv_id,
                         crosswalk_version = result$crosswalk_version_id, 
                         parent_crosswalk_version = cv_id,
                         is_bulk_outlier = 1,
                         filepath = paste0(out_dir, date, "_outliers_for_upload.xlsx"),
                         current_best = 1,
                         date = date,
                         description = description)

    cv_tracker <- as.data.table(read.xlsx(paste0(dir_2020, 'crosswalk_version_tracking.xlsx')))
    cv_tracker[bundle_id == id, current_best := 0]
    cv_tracker <- rbind(cv_tracker, df_tmp, fill = T)
    write.xlsx(cv_tracker, paste0(dir_2020, 'crosswalk_version_tracking.xlsx'))
  }
}
