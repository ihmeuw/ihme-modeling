clear_bundle_data <- function(bundle_id){
  bundle_data <- ihme::get_bundle_data(bundle_id)
  if(nrow(bundle_data) > 0){
    seqs <- bundle_data[, "seq"]
    # path to the extraction sheet to be created:
    path <- fs::path_expand(
      paste0(file.path(getwd(), "vmnis/src/", bundle_id, "_seq.xlsx"))
    )
    openxlsx::write.xlsx(seqs, path, sheetName = "extraction")
    ihme::upload_bundle_data(bundle_id = bundle_id, filepath = path)
    # remove the extraction sheet used to clear the bundle data
    file.remove(path)
  }
}