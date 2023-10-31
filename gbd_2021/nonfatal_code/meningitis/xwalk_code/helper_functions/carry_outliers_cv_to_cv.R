## SCRIPT TO APPLY OUTLIERS FROM ONE CV (old) TO ANOTHER CV (new)

#rm(list=ls())

# SET OBJECTS -------------------------------------------------------------
acause <- "meningitis"
ds <- 'iterative'   # decomp step
gbd_round_id <- 7
date <- gsub("-", "_", Sys.Date())
# cv_old <- 34370
cv_new <- 35849

carry_outliers_cv_to_cv <- function(cv_old = NULL, 
                                    cv_new = NULL, 
                                    acause,
                                    gbd_round_id = 7,
                                    ds = "iterative",
                                    date){
  if (Sys.info()["sysname"] == "Linux") {
    j <- "/home/j/" 
    h <- paste0("/ihme/homes/", Sys.info()["user"], "/")
    l <- "/ihme/limited_use/"
    k <- "/ihme/cc_resources/libraries/"
  } else { 
    j <- "J:/"
    h <- "H:/"
    l <- "L:/"
    k <- "K:/libraries/"
  }
  
  pacman::p_load(openxlsx, parallel, pbapply, boot, data.table)
  
  # SOURCE FUNCTIONS ------------------------------------------------
  # Source all GBD shared functions at once
  shared.dir <- "/filepath/"
  files.sources <- list.files(shared.dir)
  files.sources <- paste0(shared.dir, files.sources) # writes the filepath to each function
  invisible(sapply(files.sources, source)) # "invisible suppresses the sapply output
  
  # SET DIRECTORIES ----------------------------------------------------
  out_path <-  "/filepath/"
  dir_2020 <- "/filepath/"
  
  # RUN SCRIPT ----------------------------------------------------------
  # get crosswalk version id from bv tracking sheet

  # get metadata on the new crosswalk version
  meta <- get_elmo_ids(crosswalk_version_id = cv_new, gbd_round_id = gbd_round_id, decomp_step = ds)
  bundle <- meta$bundle_id
  bv_id <- meta$bundle_version_id
  
  if (is.null(cv_old)) {
    meta <- get_elmo_ids(bundle_id = bundle, gbd_round_id = gbd_round_id, decomp_step = ds)
    cv_old <- unique(meta[is_best==1 & !modelable_entity_name%like%"squeeze"]$crosswalk_version_id)
  }
  previous_cv <- get_crosswalk_version(cv_old)
  print(paste("carry outliers for bundle id", bundle, "from version", cv_old, "to version", cv_new))
  
  # find outliers
  old_outliers <- previous_cv[is_outlier == 1]

  # remove those which were outliered because clinical data == 0 
  if ("clinical_data_type" %in% names(old_outliers)){
    old_outliers[, nid_location := paste(nid, location, sep ="_")]
    bad_clin_nid_locs <- unique(old_outliers[is_outlier == 1 & mean != 0]$nid_location)
    remove <- old_outliers[clinical_data_type %in% c("inpatient", "claims", "claims, inpatient only") 
                           & mean == 0
                           & !nid_location %in% bad_clin_nid_locs] 
    old_outliers <- old_outliers[! seq %in% remove$seq]
  }
  
  # pull crosswalk version from 2020
  current_cv <- get_crosswalk_version(cv_new)
  
  # get bundle version corresponding to new crosswalk version from tracking sheet
  table_2020 <- as.data.table(read.xlsx(paste0(dir_2020, "crosswalk_version_tracking.xlsx")))

  # create merged identifier of /location/sex/year/source for old outliers
  # NOT age - because of new age groups
  old_outliers[, merged_identifier_start := paste0(age_start, location_id, sex, year_start, year_end, nid)]
  old_outliers[, merged_identifier_end := paste0(age_end, location_id, sex, year_start, year_end, nid)]
  bad_rows_start <- old_outliers$merged_identifier_start
  bad_rows_end <- old_outliers$merged_identifier_end
  
  # find merged identifiers of old outliers in new cv
  current_cv[, merged_identifier_start := paste0(age_start, location_id, sex, year_start, year_end, nid)]
  current_cv[, merged_identifier_end := paste0(age_end, location_id, sex, year_start, year_end, nid)]
  
  current_cv[merged_identifier_start %in% bad_rows_start,`:=` (is_outlier = 1)]
  current_cv[merged_identifier_end %in% bad_rows_end,`:=` (is_outlier = 1)]
  
  ## THESE BELOW checks are not working quite right :( 
  
  # look at rows in old outliers that got missed
  missed_start <- !old_outliers$merged_identifier_start %in% current_cv[is_outlier == 1]$merged_identifier_start
  if (sum(missed_start) == 0) {
    print ("no outliers missed according to age start!")
    missed_start_dt <- data.table()
  } else {
    missed_start_dt <- old_outliers[which(missed_start)]
  }
  missed_end <- !old_outliers$merged_identifier_end %in% current_cv[is_outlier == 1]$merged_identifier_end
  if (sum(missed_end) == 0) {
    print ("no outliers missed according to age end!")
    missed_end_dt <- data.table()
  } else {
    missed_end_dt <- old_outliers[which(missed_start)]
  }
  
  missed <- rbind(missed_start_dt, missed_end_dt)
  missed <- unique(missed)
  
  # see if any of those nids have rows that were not outliered
  outlier_nids <- unique(missed$nid)
  # find nids that have *only* outliered rows
  indices <- which(!outlier_nids %in% previous_cv[is_outlier == 0]$nid)
  nids_to_outlier <- outlier_nids[indices]
  current_cv[nid %in% nids_to_outlier,`:=` (is_outlier = 1)]
  
  # see if that did the trick
  # if there are no 0's in the "missed" data table, then we are good to go
  if (!0 %in% unique(missed$is_outlier)) {
    print ("all outliers are accounted for!")
  }
  
  # upload a new crosswalk version
  file_out_path <- paste0(out_path, date, "_carry_outliers_from_cv_", cv_old, ".xlsx")
  # only include seq and is_outlier column
  bulk_outlier <- current_cv[, c("seq", "is_outlier")]
  write.xlsx(bulk_outlier, file_out_path, sheetName = "extraction")
  desc <- paste("apply outliers from", cv_old, "to", cv_new)
  
  result <- save_bulk_outlier(crosswalk_version_id = cv_new, 
                              decomp_step = ds,
                              filepath = file_out_path, 
                              gbd_round_id = gbd_round_id,
                              description = desc)
  
  print(sprintf('Request status: %s', result$request_status))
  print(sprintf('Request ID: %s', result$request_id))
  print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))
  
  if (result$request_status == "Successful") {
    df_tmp <- data.table(bundle_id = bundle,
                         bundle_version_id = bv_id,
                         crosswalk_version = result$crosswalk_version_id, 
                         parent_crosswalk_version = cv_new,
                         is_bulk_outlier = 1,
                         date = date,
                         description = desc,
                         filepath = file_out_path,
                         current_best = 0
                         )
    
    cv_tracker <- read.xlsx(paste0(dir_2020, 'crosswalk_version_tracking.xlsx'))
    cv_tracker <- data.table(cv_tracker)
    # cv_tracker[bundle_id == bundle,`:=` (current_best = 0)]
    cv_tracker <- rbind(cv_tracker, df_tmp, fill = T)
    write.xlsx(cv_tracker, paste0(dir_2020, 'crosswalk_version_tracking.xlsx'))
  }
  
}

