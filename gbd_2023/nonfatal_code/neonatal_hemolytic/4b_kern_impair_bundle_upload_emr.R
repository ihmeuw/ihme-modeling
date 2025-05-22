remove <- TRUE
upload <- TRUE
bundle_id <- 458

if (remove) {
  remove_rows <- function(bundle_id) {
    bundle_data <- ihme::get_bundle_data(bundle_id = bundle_id)
    seqs_to_delete <- bundle_data[measure == "mtexcess", list(seq)]
    path <- withr::local_tempfile(fileext = ".xlsx")
    openxlsx::write.xlsx(seqs_to_delete, file = path, sheetName = "extraction")
    ihme::upload_bundle_data(bundle_id = bundle_id, filepath = path)
  }
  remove_rows(bundle_id = bundle_id)
}

error <- withr::local_tempdir()
result <- ihme::validate_input_sheet(
  bundle_id = bundle_id,
  filepath = "FILEPATH",
  error_log_path = error
)
checkmate::assert_string(
  readr::read_file(file.path(error, "detailed_log_999.txt")),
  fixed = ""
)

if (upload) {
  ihme::upload_bundle_data(
    bundle_id = bundle_id,
    filepath = "FILEPATH"
  )
}
