# Purpose: save rabies bundle version & cw version to save estimates
# Description: saves cw version for use in subsequent scripts
#
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

library(data.table)
library(openxlsx)

run_file <- fread(paste0(data_root, "FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]
interms_dir    <- paste0(run_dir, "FILEPATH")

bundle_version_metadata <- save_bundle_version(bundle_id = ADDRESS, decomp_step = 'ADDRESS', gbd_round_id = ADDRESS)
bundle_version_data <- get_bundle_version(bundle_version_id = ADDRESS, fetch = 'all')
bundle_version_data[, crosswalk_parent_seq := seq]
bundle_version_data[, seq := NA]
write.xlsx(bundle_version_data, paste0(interms_dir, '/crosswalk_data.xlsx'), sheetName = 'extraction')

save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = paste0(interms_dir, '/crosswalk_data.xlsx'), description = 'rabies cfr data')
