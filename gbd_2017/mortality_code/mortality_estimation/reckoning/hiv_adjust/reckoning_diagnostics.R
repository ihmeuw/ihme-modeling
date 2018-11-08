library(readr);
library(slackr)
slackrSetup(config_file = "/FILEPATH/.slackr")

dump_diagnostic_file <- function(data, reckoning_version, folder, filename, dump_if_no_rows=FALSE) {
  if ((!is.null(data) && nrow(data) > 0) || dump_if_no_rows) {
    
    diagnostics_dir <- file.path("/FILEPATH/hiv_adjust", reckoning_version, "diagnostics")
    dir.create(diagnostics_dir, showWarnings = FALSE)
    
    script_dir <- file.path(diagnostics_dir, folder)
    dir.create(script_dir, showWarnings = FALSE)
    
    fpath <- file.path(script_dir, filename)
    print(paste0("Dumping file with ", nrow(data), " to ", fpath))
    write_csv(data, fpath)
  }
}

check_diagnostics <- function(reckoning_version, send_slack_usr=NA) {
  diagnostics_dir <- file.path("/FILEPATH/hiv_adjust", reckoning_version, "diagnostics")
  existing_diagnostics_files <- list.files(diagnostics_dir, recursive = TRUE)
  
  if (length(existing_diagnostics_files) > 0 & !is.na(send_slack_usr)) {
    text_slackr(channel=paste0("@", send_slack_usr), paste0(length(existing_diagnostics_files), " diagnostics files generated."))
  }
  
  return(existing_diagnostics_files)
}

send_slack <- function(usr, msg) {
  text_slackr(channel=paste0("@", usr), msg)
}
