
# Remove extra columns to facilitate merging ------------------------------

keep_needed_cols_only <- function(data_dir, cause_id, sex_id, write_as_csv) {
  message("Starting to check for and remove unneeded columns in '", data_dir, 
          "' for cause(s) ", paste(cause_id, collapse = ", "), 
          " and sex(es) ", paste(sex_id, collapse = ", "))
  
  # create a vector of full file paths using each cause/sex-specific directory
  paths <- expand.grid(cause_id = cause_id, sex_id = sex_id) |>
    dplyr::rowwise() |>
    dplyr::mutate(
      file_paths = list(
        list.files(file.path(data_dir, cause_id, sex_id), full.names = TRUE)
      )
    ) |>
    dplyr::pull(file_paths) |>
    unlist()
  
  # apply function that checks for extraneous cols and, if needed, removes them
  error_paths <- c() # to store paths with errors
  lapply(paths, function(x) {
    # process each file and accumulate paths that produced errors, if any
    result <- tryCatch({
      keep_cols(x, write_as_csv)
      return(NULL)
    }, error = function(e) {
      message(paste0("Error processing ", x, ": ", e$message))
      return(x)  # if there is an error, return the path that produced it
    })
    if (!is.null(result)) {
      error_paths <- c(error_paths, result)
    }
  })
  
  # display an informative error if any of the files are processed unsuccessfully
  if (length(error_paths) > 0) {
    error_message <- paste("Error checking the following files:", 
                           paste(error_paths, collapse=", "), sep=" ")
    stop(error_message)
  }
  message("Finished checking for and removing unneeded columns")
}

keep_cols <- function(path, write_as_csv) {
  dat <- qs::qread(path)
  cols_to_keep <- c("age_group_id",
                    "sex_id",
                    "year_id",
                    "location_id",
                    "cause_id",
                    stringr::str_subset(names(dat), "^draw_\\d+$"))
    dat_keep <- dat |> 
      dplyr::select(dplyr::all_of(cols_to_keep))
    if (ncol(dat) != ncol(dat_keep) && ncol(dat_keep) == 1005) {
      if (write_as_csv == TRUE) {
        csv_path <- sub("\\.[^.]+$", ".csv", path)
        data.table::fwrite(dat_keep, csv_path)
      } else {
        qs::qsave(dat_keep, path)
      }
      message(paste("Removed unneeded column(s) from", path))
    }
  }
