# Convert QS files to CSV format if needed --------------------------------

convert_qs_to_csv <- function(source_dir, target_dir = NULL) {
  # check that source directory exists and has files
  if (!dir.exists(source_dir)) {
    stop("Source directory does not exist:", source_dir)
  }
  qs_files <- list.files(source_dir, pattern = "\\.qs$", full.names = TRUE)
  if (length(qs_files) == 0) {
    message("No QS files found in source directory ", source_dir)
  }
  # if needed, assign or create target directory
  if (length(qs_files) > 0) {
    if (is.null(target_dir)) {
      target_dir <- source_dir
    } else if (!dir.exists(target_dir)) {
      dir.create(target_dir, recursive = TRUE)
      Sys.chmod(target_dir, "775", use_umask = FALSE)
      message("Target directory created: ", target_dir)
    }
    # convert each QS file in source directory to CSV in target directory
    failed_files <- c() # initialize vector to track failed conversions
    for (qs_file in qs_files) {
      tryCatch({
        dat <- qs::qread(qs_file)
        target_file_name <- sub("\\.qs$", ".csv", basename(qs_file))
        target_file_path <- file.path(target_dir, target_file_name)
        data.table::fwrite(dat, target_file_path, row.names = FALSE)
      }, error = function(e) {
        failed_files <<- c(failed_files, qs_file) # add failed file to vector
      })
    }
    # throw an error if there are any failed conversions
    if (length(failed_files) > 0) {
      error_message <- paste("Error converting the following files:", paste(basename(failed_files), collapse=", "), sep=" ")
      stop(error_message) 
    } 
    message(length(qs_files), " QS files in ", source_dir, " converted to CSV files in ", target_dir)
  }
}
