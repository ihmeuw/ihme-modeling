
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/get_bundle_version.R")

bundle_id <- ADDRESS
decomp_step <- ADDRESS


afrtryp_bv<- save_bundle_version(bundle_id, decomp_step)

afrtryp_bundle_version_df <- get_bundle_version(ADDRESS)
afrtryp_bundle_version_df[, crosswalk_parent_seq := seq]


afrtryp_bundle_version_df[, unit_value_as_published := as.double(unit_value_as_published)]

afrtryp_bundle_version_df[,unit_value_as_published := 1]
openxlsx::write.xlsx(afrtryp_bundle_version_df, sheetName = "extraction", file = "FILEPATH")


description<- 'ntd_afrtryp_all_age_cases'
afrtryp_cwv <- save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = "FILEPATH", description = description)
