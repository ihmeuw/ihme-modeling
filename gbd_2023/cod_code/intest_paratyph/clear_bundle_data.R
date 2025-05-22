library(xlsx)
library(data.table)

SHARED_FUN_DIR <- 'FILEPATH'
source(file.path(SHARED_FUN_DIR, 'get_bundle_data.R'))
source(file.path(SHARED_FUN_DIR, 'upload_bundle_data.R'))

clear_bundle_data <- function(bundle_id, path, export = TRUE) {
  to_clear <- get_bundle_data(bundle_id, export = export)[, .(seq)]

  date_stamp <- gsub('-', '', Sys.Date())
  upload_fname <- file.path(path, paste0('clearing_bundle_',bundle_id, '_', date_stamp, '.xlsx'))
  write.xlsx(to_clear, file = upload_fname, sheetName = 'extraction', rowNames = FALSE)
  
  result <- upload_bundle_data(bundle_id = bundle_id, filepath = upload_fname)  
  return(result)
}

