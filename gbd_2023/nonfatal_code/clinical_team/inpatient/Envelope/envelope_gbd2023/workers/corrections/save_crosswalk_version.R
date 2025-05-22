# Title: saving crosswalk version
# Purpose: data snapshot for utilization modeling post split and outliering
# Author: USERNAME

# get quota information
source("FILEPATH")
version_quota <- get_version_quota(bundle_id = bundle_id)
n_active_versions <- n_distinct(version_quota[crosswalk_version_status %in% c("active",
                                                                              "associated with active model",
                                                                              "associated with best model"), 
                                              .(crosswalk_version_id)])
if (n_active_versions >= 15) {
  cat("Quota of 15 active versions reached. Prune before uploading new data...\n")
  print(unique(version_quota[crosswalk_version_status != "delete", .(bundle_version_id, crosswalk_version_id, crosswalk_version_status, bundle_version_status, model_version_id)]))
  stop("Please consider deleting active versions that are not associated with an active model.\n")
} else {
  cat("Number of active versions: ", n_active_versions, "\n")
  cat("Proceeding with crosswalk upload...\n")
}

# Indicate crosswalk data path for upload
if (!dir.exists(paste0(run_dir, "data/crosswalk/"))) {
  dir.create(paste0(run_dir, "data/crosswalk/"))
}
crosswalk_data_path <- paste0(run_dir, "data/crosswalk/for_xwalk_upload_", "run_", run_id, "_BVID_", bundle_version_id, ".xlsx")
# Save xlsx and name the sheet "extraction" for epi uploader to work
openxlsx::write.xlsx(data_with_covariates, crosswalk_data_path, sheetName = "extraction")

# upload new data
source("FILEPATH")
description <- "Crosswalk version for GBD 2023"
result <- save_crosswalk_version(
  bundle_version_id = bundle_version_id,
  data_filepath = crosswalk_data_path,
  description = description)

# record crosswalk version ID!
print(sprintf('Request status: %s', result$request_status))
print(sprintf('Request ID: %s', result$request_id))

crosswalk_version_id <- result$crosswalk_version_id
print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))

# save flat file
crosswalk_version_table <- get_crosswalk_version(crosswalk_version_id)
data_sharing_path <- paste0(home_dir, "FILEPATH", crosswalk_version_id, ".csv")
fwrite(crosswalk_version_table, data_sharing_path)
