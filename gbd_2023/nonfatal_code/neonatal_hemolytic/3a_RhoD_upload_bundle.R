################################################################################
## Purpose:   Save updated data on RhoD sales from Marketing Research Bureau
##            NID: 539703 to Bundle ID 10513 (Neonatal hemolytic RhoD sales - dismod)
## Input:     rhod_sales_master.csv created from RhoD_sales_data_processing.R
## Output:    Bundle ID 10513, Bundle version ID
################################################################################

# Set up -----------------------------------------------------------------------
params_global <- readr::read_rds("params_global.rds")
library(dplyr)

# Bundle validations -----------------------------------------------------------
rhod <-
  data.table::fread(file.path("FILEPATH"))

# add bundle validations
rhod <- rhod %>%
  mutate(nid = 539703) %>% #add nid
  mutate(is_outlier = 0) %>%
  mutate(year_start = year_id) %>%
  mutate(year_end = year_id) %>%
  mutate(sex = "Both") %>%
  mutate(cases = sum_vials) %>%
  mutate(seq = NA) %>%
  mutate(measure = "proportion") %>%
  mutate(underlying_nid = NA)

# Save bundle data to snfs drive -----------------------------------------------
upload_filepath <-
  'FILEPATH'
openxlsx::write.xlsx(rhod, file = upload_filepath, sheetName = 'extraction')

# remove rows from bundle
remove <- TRUE
bundle_id <- 10513

if (remove) {
  remove_rows <- function(bundle_id) {
    bundle_data <- ihme::get_bundle_data(bundle_id = bundle_id)
    seqs_to_delete <- bundle_data[measure == "prevalence", list(seq)]
    path <- withr::local_tempfile(fileext = ".xlsx")
    openxlsx::write.xlsx(seqs_to_delete, file = path, sheetName = "extraction")
    ihme::upload_bundle_data(bundle_id = bundle_id, filepath = path)
  }
  remove_rows(bundle_id = bundle_id)
}


# deleting data in bundle 4715 so it's ready to be uploaded with new rhod data that's been processed.
# confirmed we have a bundle version for old data.
bundle <- ihme::get_bundle_data(4715)
seq <- bundle[,.(seq)]
path_to_clear_data <- 'FILEPATH'
openxlsx::write.xlsx(sheetName = "extraction", x = seq, file = path_to_data)
bundle_id <- 4715
result <- ihme::upload_bundle_data(bundle_id, filepath=path_to_clear_data)

# upload bundle data
bundle_id <- 4715
result <- ihme::upload_bundle_data(bundle_id, filepath=upload_filepath)
