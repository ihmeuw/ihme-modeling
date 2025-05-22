################################################################################
## Purpose: Save cerebral palsy SMR values to Bundle 10590
## Input:   CP SMR flat file
## Output:  Bundle 10590
################################################################################

library(data.table)
library(dplyr)

# pull standard mortality ratio ------------------------------------------------
smr <- data.table::fread("FILEPATH")

# process for custom bundle validations ----------------------------------------
# the year start and year end value is an approximation of when the meta-analysis occurred, and these
# values are not year specific.
smr <- smr %>%
  mutate(
    location_id = 1,
    measure = 'mtstandard',
    sex = "Both",
    nid = 256559,
    year_start = 2013,
    year_end = 2013,
    field_citation_value = "Institute for Health Metrics and Evaluation (IHME). IHME GBD DisMod Neonatal Hemolytic Disease Excess Mortality Estimates.",
    seq = NA,
    underlying_nid = NA
  )

# save extraction sheet --------------------------------------------------------
openxlsx::write.xlsx(
  smr,
  file = "FILEPATH",
  sheetName = "extraction"
)

# upload to bundle and save bundle version -------------------------------------
bundle_id <- 10590
ihme::upload_bundle_data(
  bundle_id = bundle_id,
  filepath = "FILEPATH"
)
ihme::save_bundle_version(bundle_id = bundle_id)
