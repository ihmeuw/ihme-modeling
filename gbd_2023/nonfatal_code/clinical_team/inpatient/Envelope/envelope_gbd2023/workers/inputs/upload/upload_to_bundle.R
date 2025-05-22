# Upload processed data to the bundle

# Wipe the bundle
wipe_existing_bundle <- function(bundle_id) {
  
  # get existing bundle data
  bundle_dt <- get_bundle_data(bundle_id)
  
  # download seq table to remove the current rows
  # Create temporary file that is seq column from bundle_dt saved to xlsx saved to data_dir
  seq_to_remove_dt <- bundle_dt %>% select(seq)
  sheet_name <- "extraction"
  wipe_bundle_file_path <- paste0(run_dir, "utils/", "seq_to_remove_", 
                                format(Sys.time(), "%Y_%m_%d_%H_%M_%S"), 
                                ".xlsx")
  openxlsx::write.xlsx(seq_to_remove_dt, 
                     file = wipe_bundle_file_path, 
                     sheetName = sheet_name)
  
  # upload the seq column to wipe the bundle
  wipe_bundle <- upload_bundle_data(bundle_id, filepath = wipe_bundle_file_path)  
  # check how it looks, should be empty
  wiped_bundle <- get_bundle_data(bundle_id)
  # check that wiped bundle has 0 rows
  if (nrow(wiped_bundle) == 0) {
    cat(green("Bundle is empty, proceeding with upload...\n"))
  } else {
    cat(red("Bundle was not wiped correctly, review the function execution."))
  }
  
  return(wiped_bundle)
  
}

# Upload updated table
update_bundle_get_version <- function(new_bundle_data, bundle_id, description) {
  
  # Indicate bundle data path for upload
  bundle_data_path <- paste0(run_dir, "data/bundle/", "run_", run_id, "_for_bundle_upload_", 
                             format(Sys.time(), "%Y_%m_%d_%H_%M_%S"), ".xlsx")
  # Save xlsx and name the sheet "extraction" for epi uploader to work
  openxlsx::write.xlsx(new_bundle_data, 
                       bundle_data_path, 
                       sheetName = "extraction")
  # upload new data
  call_upload_bundle_data <- upload_bundle_data(bundle_id, 
                                                filepath = bundle_data_path)  
  
  # check how it looks
  updated_bundle <- get_bundle_data(bundle_id)
  
  # if alright, save bundle version
  call_save_bundle_version <- save_bundle_version(bundle_id = bundle_id,
                                                  description = description)
  
  # record bundle version ID!
  print(sprintf('Request status: %s', call_save_bundle_version$request_status))
  print(sprintf('Request ID: %s', call_save_bundle_version$request_id))
  print(sprintf('Bundle version ID: %s', call_save_bundle_version$bundle_version_id))
  
  # save flat file for backup
  bundle_version_id <- call_save_bundle_version$bundle_version_id
  path <- paste0(run_dir, "FILEPATH", bundle_version_id, ".csv")
  fwrite(updated_bundle, path)
  
  return(updated_bundle)
  
}
