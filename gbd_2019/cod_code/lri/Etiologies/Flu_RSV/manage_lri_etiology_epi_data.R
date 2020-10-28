source("/ihme/cc_resources/libraries/current/r/save_crosswalk_version.R")
source("/ihme/cc_resources/libraries/current/r/save_bundle_version.R")
source("/ihme/cc_resources/libraries/current/r/get_bundle_version.R")
source("/ihme/cc_resources/libraries/current/r/get_bundle_data.R")
source("/ihme/cc_resources/libraries/current/r/upload_bundle_data.R")
source("/home/j/temp/ctroeger/Code/GBD_2019/mr_brt/sex_split_mrbrt_weights.R")
source("/ihme/cc_resources/libraries/current/r/get_population.R")

source("/ihme/cc_resources/libraries/current/r/get_bundle_data.R")
rsv <- get_bundle_data(bundle_id=25, gbd_round_id = 6, decomp_step="step4")

# GBD 2020 s2: 7835
# GBD 2019 s2: 8103
# GBD 2019 s4: 478

## ----------------- Workflow for LRI etiologies -------------------------##
## RSV ##

  # upload_bundle_data(bundle_id=23, filepath = "/home/j/WORK/12_bundle/lri_rsv/23/03_review/02_upload/bundle_reassign_non_age_specific.xlsx", decomp_step="step2")
  #
  # # 1616
  # #save_bundle_version(bundle_id=23, decomp_step="step2")
  #
  # rsv <- get_bundle_version(bundle_version_id = 1616)
  #   rsv$crosswalk_parent_seq <- ""
  #   rsv$seq_parent <- ""
  #   rsv <- duplicate_sex_rows(rsv)
  #   rsv$group_review[is.na(rsv$group_review)] <- 1
  #   rsv <- subset(rsv, group_review!=0 & !(location_id %in% bad_ids))
  #   rsv$group_review <- ifelse(rsv$specificity=="","",rsv$group_review)
  # write.xlsx(rsv, "/home/j/WORK/12_bundle/lri_rsv/23/03_review/02_upload/rsv_gbd2019_decomp2_raw.xlsx", sheetName="extraction")
  #
  # save_crosswalk_version(bundle_version_id = 1616, data_filepath = "/home/j/WORK/12_bundle/lri_rsv/23/03_review/02_upload/rsv_gbd2019_decomp2_raw.xlsx", description = "Raw data (only sex split)")
  # save_crosswalk_version(bundle_version_id = 1616, data_filepath = "/home/j/WORK/12_bundle/lri_rsv/23/03_review/02_upload/rsv_gbd2019_decomp2_age_split.xlsx", description = "Age split RSV data")
  save_crosswalk_version(bundle_version_id = 1616, data_filepath = "/home/j/WORK/12_bundle/lri_rsv/23/03_review/02_upload/rsv_gbd2019_decomp2_crosswalk.xlsx", description = "Fixed sex split, consistent age curve")

## Influenza ##
  # upload_bundle_data(bundle_id=20, filepath = "/home/j/WORK/12_bundle/lri_flu/20/03_review/02_upload/bundle_reassign_non_age_specific.xlsx", decomp_step="step2")
  #
  # #1619
  # #save_bundle_version(bundle_id=20, decomp_step="step2")
  #
  # flu <- get_bundle_version(bundle_version_id = 1619)
  #   flu$crosswalk_parent_seq <- ""
  #   flu$seq_parent <- ""
  #   flu <- duplicate_sex_rows(flu)
  #   flu$group_review[is.na(flu$group_review)] <- 1
  #   flu <- subset(flu, group_review!=0 & !(location_id %in% bad_ids))
  #   flu$group_review <- ifelse(flu$specificity=="","",flu$group_review)
  # write.xlsx(flu, "/home/j/WORK/12_bundle/lri_flu/20/03_review/02_upload/influenza_gbd2019_decomp2_raw.xlsx", sheetName="extraction")
  #
  # save_crosswalk_version(bundle_version_id = 1619, data_filepath = "/home/j/WORK/12_bundle/lri_flu/20/03_review/02_upload/influenza_gbd2019_decomp2_raw.xlsx", description = "Raw data (only sex split)")
  # save_crosswalk_version(bundle_version_id = 1619, data_filepath = "/home/j/WORK/12_bundle/lri_flu/20/03_review/02_upload/influenza_gbd2019_decomp2_age_split.xlsx", description = "Age split Influenza data")
  save_crosswalk_version(bundle_version_id = 1619, data_filepath = "/home/j/WORK/12_bundle/lri_flu/20/03_review/02_upload/influenza_gbd2019_decomp2_crosswalk.xlsx", description = "Fixed sex split, consistent age curve")
