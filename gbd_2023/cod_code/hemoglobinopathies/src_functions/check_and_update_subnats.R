# Define function to check and update files based on given location IDs

check_and_update_subnats <- function(old_location_id, new_location_id, out_dir, data_dir, cause_id = NULL, sex_id = NULL) {
  
  # set file names based on given data directory
  file_prefix <- sub("_.*$", "", data_dir)
  base_file_name <- file.path(
    out_dir, 
    data_dir, 
    ifelse(!is.null(cause_id), cause_id, ""), 
    ifelse(!is.null(sex_id), sex_id, "")
  )
  old_file_name <- file.path(base_file_name, glue::glue("{file_prefix}_hemog_{old_location_id}.qs"))
  new_file_name <- file.path(base_file_name, glue::glue("{file_prefix}_hemog_{new_location_id}.qs"))
  
  # if a file for the old loc ID exists, create a file for the new loc ID
  if (fs::file_exists(old_file_name)) {
    changefile <- qs::qread(old_file_name)
    changefile$location_id <- new_location_id
    qs::qsave(changefile, new_file_name)
    
    # confirm the newly created file exists
    if (fs::file_exists(new_file_name)) {
      message(glue::glue("New file based on {file_prefix}_hemog_{old_location_id} created for {new_location_id}"))
    }
  } else {
    message(glue::glue("File {file_prefix}_hemog_{old_location_id} does not exist - no new file created"))
  }
  
  return(old_file_name)
}