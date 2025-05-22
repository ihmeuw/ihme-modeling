#' @title Ensure that a file is recently saved
#'
#' @param file_path path to file
#' @param minutes_threshold numeric value to specify a number of minutes that
#' the file is expected within. Defaults to 10 minutes.
#'
#' @return Error message in the case that the time since last file modification
#' is beyond threshold

recent_check <- function(file_path, minutes_threshold = 10) {
  
  # Time that file was last edited
  mtime <- file.info(file_path)$mtime
  
  # Difference in time in minutes
  time_difference <- difftime(Sys.time(), mtime, units = "mins")
  
  if (time_difference > minutes_threshold) {
    stop(paste0("File was not saved within the last ", minutes_threshold, "minutes.",
    "Ensure that file saved properly or adjust minutes_threshold argument.",
    "File was last saved at ", mtime))
  }
  
}