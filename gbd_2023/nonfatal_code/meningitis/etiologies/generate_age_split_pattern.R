pacman::p_load(openxlsx, parallel, pbapply, boot, data.table)

# SOURCE FUNCTIONS ------------------------------------------------
# Source all GBD shared functions at once
shared.dir <- "/filepath/"
files.sources <- list.files(shared.dir)
files.sources <- paste0(shared.dir, files.sources) # writes the filepath to each function
invisible(sapply(files.sources, source)) # "invisible suppresses the sapply output

# SET OBJECTS -------------------------------------------------------------
# bundle <- 29 # bundle_id
ds <- 'iterative'   # decomp step
gbd_round_id <- 7
date <- gsub("-", "_", Sys.Date())

# SET DIRECTORIES ----------------------------------------------------
bundle_map <- as.data.table(read.xlsx(paste0('filepath/bundle_map.xlsx')))
dir_2020 <- 'filepath'
cv_tracker <- as.data.table(read.xlsx(paste0(dir_2020, 'crosswalk_version_tracking.xlsx')))

data_dir <- 'filepath'
bundles <- c(29, 33, 37, 41, 7958)
bundle_map <- bundle_map[bundle_id %in% bundles]

for (i in 1:nrow(bundle_map)) {
  id <- bundle_map$bundle_id[i] 
  # need to make sure to pull the NON-age-split data. pull this manually from tracker sheet
  # cv_id <- cv_tracker[bundle_id == id & current_best == 1]$crosswalk_version
  bv_id <- cv_tracker[crosswalk_version == cv_id]$bundle_version
  cv_df <- get_crosswalk_version(crosswalk_version_id = cv_id)
  out_dir <- paste0(h, "/02_xwalk/gbd_2020/", ds, "/" , id, "/age_split_model/")
  dir.create(out_dir, showWarnings = F)
 
  dismod_age_data <- copy(cv_df)
  dismod_age_data[, age_bin := age_end - age_start]
  # Drop all the 0 to 99 rows 
  dismod_age_data <- dismod_age_data[(age_end < 20) | (age_end > 20 & age_start > 5)] # this row only is Goldilocks
  if (id == 7958 | id == 41){
    # specific focus on neonatal for GBS and other
    dismod_age_data <- dismod_age_data[(age_start < 28/365 & age_bin < 1) | 
                                         (age_start > 28/365 & age_end < 20) |
                                         (age_end > 20 & age_start > 5)]
    desc <- "stricter neonatal"
  } else {
    # keep "goldilocks" for pneumococcal and Hib and meningococcal
    # this should show the "double peak"
    dismod_age_data <- dismod_age_data[(age_end < 20 & age_bin < 5) |
                                         (age_end > 20 & age_bin < 20)]
    desc <- "original goldilocks, showing double peak for Nm"
  }
  
  # set all seqs to NA...
  dismod_age_data[, seq := NA]
  
  # save as .xlsx file which will point to in 'save' call
  write.xlsx(dismod_age_data, 
             file=paste0(out_dir, date, "_age_specific.xlsx"), 
             sheetName="extraction", 
             row.names=FALSE, col.names=TRUE)
  
  # save a crosswalk version
  description <- paste(desc, 'age-specific data only from crosswalk version', cv_id)
  result <- save_crosswalk_version(bundle_version_id = bv_id,
                                   paste0(out_dir, date, "_age_specific.xlsx"), 
                                   description = description
  )
  
  print(sprintf('Request status: %s', result$request_status))
  print(sprintf('Request ID: %s', result$request_id))
  print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))
  
  if (result$request_status == "Successful") {
    df_tmp <- data.table(bundle_id = id,
                         bundle_version_id = bv_id,
                         crosswalk_version = result$crosswalk_version_id, 
                         parent_crosswalk_version = NA,
                         is_bulk_outlier = 0,
                         filepath = paste0(out_dir, date, "_age_specific.xlsx"),
                         current_best = 0,
                         date = date,
                         description = description)
    
    cv_tracker <- as.data.table(read.xlsx(paste0(dir_2020, 'crosswalk_version_tracking.xlsx')))
    #cv_tracker[bundle_id == id, current_best := 0]
    cv_tracker <- rbind(cv_tracker, df_tmp, fill = T)
    write.xlsx(cv_tracker, paste0(dir_2020, 'crosswalk_version_tracking.xlsx'))
  }
}

