################################################################################
## Purpose:   Upload nonfirstborn data to bundle 7667
## Input:     Extraction sheets saved in post_collapse_processing.R and
##            dyb_extract_and_process.R
## Output:    Bundle version
################################################################################

# remove bundle contents -------------------------------------------------------
remove <- TRUE
bundle_id <- 7667

if (remove) {
  remove_rows <- function(bundle_id) {
    bundle_data <- ihme::get_bundle_data(bundle_id = bundle_id)
    seqs_to_delete <- bundle_data[measure == "proportion", list(seq)]
    path <- withr::local_tempfile(fileext = ".xlsx")
    openxlsx::write.xlsx(seqs_to_delete, file = path, sheetName = "extraction")
    ihme::upload_bundle_data(bundle_id = bundle_id, filepath = path)
  }
  remove_rows(bundle_id = bundle_id)
}

# Upload collapsed data --------------------------------------------------------
path_to_data <- 'FILEPATH'
result <- ihme::upload_bundle_data(bundle_id, filepath=path_to_data)

# Upload UNDYB data ------------------------------------------------------------
path_to_data <- '/FILEPATH'
result <- ihme::upload_bundle_data(bundle_id, filepath=path_to_data)

# Save bundle version ----------------------------------------------------------
result <- ihme::save_bundle_version(bundle_id, automatic_crosswalk = TRUE)