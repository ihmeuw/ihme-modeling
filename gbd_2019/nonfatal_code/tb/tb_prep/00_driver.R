## EMPTY THE ENVIRONMENT
rm(list = ls())

## ESTABLISH FOCAL DRIVES
os <- .Platform$OS.type
if (os=="windows") {
  j <-"J:/"
  h <-"H:/"
  k <-"K:/"
} else {
  j <-"/home/j/"
  h <-paste0("homes/", Sys.info()[7], "/")
  k <-"/ihme/cc_resources/"
}

## LOAD FUNCTIONS
library(readxl)
library(readstata13)
library(writexl, lib.loc = paste0(j, "temp/TB/joyma/libraries/"))

source(paste0(k, "libraries/current/r/get_location_metadata.R"))
source(paste0(k, "libraries/current/r/get_bundle_data.R"))
source(paste0(k, "libraries/current/r/save_bundle_version.R"))
source(paste0(k, "libraries/current/r/get_bundle_version.R"))
source(paste0(k, "libraries/current/r/save_crosswalk_version.R"))
source(paste0(k, "libraries/current/r/upload_bundle_data.R"))

#############################################################################################
###                                   DATA PREPARATION                                    ###
#############################################################################################

## HELPER FUNCTIONS
decomp_step         <- "iterative"
date                <- "2020_05_26"
main_dir            <- paste0(j, "WORK/12_bundle/tb/712/01_input_data/")
upload_metadata_dir <- paste0(j, "WORK/12_bundle/tb/712/03_review/02_upload/upload_metadata/")
bundle_dir          <- paste0(j, "WORK/12_bundle/tb/712/03_review/01_download/")

## GET BUNDLE DATA
dt <- get_bundle_data(bundle_id=712, decomp_step=decomp_step, gbd_round_id=7, sync=F)

## SAVE BUNDLE VERSION
bundle_metadata <- save_bundle_version(bundle_id=712, decomp_step=decomp_step, gbd_round_id=7)
fwrite(bundle_metadata, paste0(upload_metadata_dir, decomp_step, "_bundle_ver_raw_data_", date, ".csv"), row.names = F)

## GET BUNDLE VERSION AND SAVE
dt <- get_bundle_version(bundle_version_id=bundle_metadata$bundle_version_id, fetch = "all")
write.csv(dt, paste0(bundle_dir, decomp_step, "_", date, "_download.csv"), row.names = F, na="")

## STOP CODE TO INFLATE FOR EXTRAPULMONARY TB AND AGE SPLIT
stop("stop code to inflate for extrapulmonary TB and age split")

#############################################################################################
###                     REMOVE ALTERNATIVE ROWS WHEN REFERENCE IS PRESENT                 ###
#############################################################################################

## GET DATA
dt  <- as.data.table(read_excel(paste0(main_dir, "03_ep_adjusted/", date, "/", decomp_step, "_ep_adj_age_split_", date, ".xlsx")))
dup <- unique(dt[, .(nid, location_id, year_start, year_end, age_start, age_end, sex, cv_diag_smear)])
dup <- dup[order(nid, location_id, year_start, cv_diag_smear, sex, age_start)]

## CREATE REF AND ALT TABLES
dup_smear   <- dup[cv_diag_smear == 1]
dup_nosmear <- dup[cv_diag_smear == 0]
setnames(dup_nosmear, "cv_diag_smear", "no_smear")

## IDENTIFY WHERE REF AND ALT ARE PRESENT
dup <- merge(dup_smear, dup_nosmear)
dup[, duplicate := 1]
dup[, cv_diag_smear := NULL][, no_smear := NULL]

## CREATE DUPLICATE INDICATOR VARIABLE
dt <- merge(dt, dup, all.x = T, by = names(dup)[names(dup) != "duplicate"])
dt[(duplicate == 1) & (cv_diag_smear == 1), row_rem := 1]
dt[is.na(row_rem), row_rem := 0]

## REMOVE DUPLICATE AND CLEAN
dt <- dt[row_rem == 0]
dt <- dt[!(ihme_loc_id=="KHM" & year_start == 2002 & sex == "Both")] # Remove duplicate KHM data point
dt[, `:=` (year_id = NULL, age_group_id = NULL, duplicate = NULL, row_rem = NULL)]
write.csv(dt, file = paste0(main_dir, "03_ep_adjusted/", date, "/", decomp_step, "_ep_adj_age_split_", date, ".csv"), row.names = F, na = "")

## STOP CODE TO CROSS-WALK
stop("stop code to crosswalk")

#############################################################################################
###                           GATHER INPUTS FOR DIVISION OF LTBI                          ###
#############################################################################################

## GET DATA
dt   <- fread(paste0(bundle_dir, decomp_step, "_", date, "_download.csv"))
dt   <- dt[measure %in% c("incidence", "mtspecific")]
prev <- fread(paste0(main_dir, "04_crosswalk/", date,"/genexpert_adj_data.csv"))

## PREP TO APPEND
setnames(prev, old = c("authoryear", "pubyear"), new = c("author year", "pub year"))
prev[, `:=` (sex_id = NULL, orig_mean = NULL)]
dt[, `:=` (year_id = NULL, age_group_id = NULL)]

## APPEND
dt <- rbind(dt, prev, fill = T)
dt <- dt[order(seq)]

## APPEND US SUBNATS
us <- fread(paste0(main_dir, "04_crosswalk/", date,"/us_subnat_split.csv"))
dt <- dt[!((measure == "incidence") & (location_id == 102) & (year_start == 2018))]
dt <- rbind(dt, us, fill=T)
dt <- dt[order(seq)]

## SAVE
writexl::write_xlsx(list(extraction = dt), path = paste0(main_dir, "04_crosswalk/", date, "/", decomp_step, "_prev_screening_adj_inc_csmr_pre_ltbi.xlsx"))
ifelse(!dir.exists(paste0(main_dir, "05_ltbi_divided/", date, "/")), dir.create(paste0(main_dir, "05_ltbi_divided/", date, "/")), FALSE)

#############################################################################################
###                        REMOVE INCIDENCE IF LOWER THAN NOTIFICATION                    ###
#############################################################################################

## GET NOTIFICATIONS
notifications <- as.data.table(read.dta13(paste0(j, "WORK/12_bundle/tb/GBD2016/04_intermediate_files/SUP_xbs_TB_CNs_squeezed.dta")))
notifications <- notifications[, .(iso3, year, sex_id, age, CN_bact_xb)]
notifications <- notifications[year == 2010]
setnames(notifications, old = c("iso3", "CN_bact_xb"), new = c("ihme_loc_id", "inc"))

## AGGREGATE NOTIFICATION CASES
notifications <- notifications[, .(inc_notif = sum(inc)), by = .(ihme_loc_id, year)]

## GET MI RATIO BASED INCIDENCE
dt <- fread(paste0(bundle_dir, decomp_step, "_", date, "_download.csv"))
dt <- dt[(nid == 305682) & (year_start == 2010)]
dt <- dt[, .(location_name, location_id, ihme_loc_id, year_start, sex, age_start, age_end, cases)]
setnames(dt, old = "year_start", new = "year")

## AGGREGATE GBD INCIDENCE
dt <- dt[, .(inc_gbd = sum(cases)), by = .(location_name, location_id, ihme_loc_id, year)]

## MERGE NOTIFICATIONS
dt <- merge(dt, notifications, all.x = T)

## CHECK WHERE WE ARE LOWER
dt[, ratio := inc_gbd / inc_notif]

## SUBNAT
subnat <- dt[ihme_loc_id %like% "_"]
subnat[, parent_loc := tstrsplit(ihme_loc_id, "_")[[1]]]

## AGGREGATE SUBNATIONS
subnat <- subnat[, .(ihme_loc_id = parent_loc, inc_gbd = sum(inc_gbd)), by = .(parent_loc, year)]
subnat <- merge(subnat, notifications)
subnat[, ratio := inc_gbd / inc_notif]

## APPEND SUBNATIONS
dt <- dt[!(ihme_loc_id %in% subnat$ihme_loc_id)]
dt <- rbind(dt, subnat, fill=T)

## SAVE WHERE WE ARE LOWER
too_low <- unique(dt[ratio < 0.99, ihme_loc_id])

## EXCEPTIONS
too_low <- too_low[too_low != "CHN"]
too_low <- too_low[!(too_low %in% c("HTI", "PNG", "BTN", "MHL", "PRK", "FSM", "SDN", "DZA", "BIH"))]
too_low <- sort(append(too_low, c("ARE", "SAU")))

#############################################################################################
###                           APPEND EMR AFTER DIVISION BY LTBI                           ###
#############################################################################################

## GET DATA
emr <- fread(paste0(bundle_dir, decomp_step, "_", date, "_download.csv"))
emr <- emr[measure == "mtexcess" | measure == "remission"]
all <- as.data.table(read_excel(paste0(main_dir, "05_ltbi_divided/", date, "/", decomp_step, "_prev_screening_adj_inc_csmr_prev_501269_20200527.xlsx")))
all <- all[order(crosswalk_parent_seq)]

## ADD AGE-SPLIT REMISSION
#emr <- emr[measure == "mtexcess"]
#rem <- fread(paste0(main_dir, "04_crosswalk/", date, "/remission_age_split.csv"))
#emr <- rbind(emr, rem, fill=T)

## FORMAT
all[, `:=` (lower = mean-1.96*standard_error, upper = mean+1.96*standard_error, uncertainty_type = "Standard error", uncertainty_type_value = 95)]
all[lower < 0, lower := 0]
all[upper > 1, upper := 1]
all[mean > 1, mean := 1]
all[standard_error > 1, standard_error := 0.99]

## PREP TO APPEND
all[, `:=` (year_id = NULL, sex_id = NULL, age_group_id = NULL)]
emr[, `:=` (year_id = NULL, age_group_id = NULL)]

## APPEND
all <- rbind(all, emr, fill = T)

## PREP TO UPLOAD CROSSWALK VERSION
all <- all[(is.na(group_review)) | (group_review == 1)]
all <- all[sex != "Both"]
all <- all[mean != 0]

## KEEP ONLY DISMOD LOCATIONS
locs <- get_location_metadata(location_set_id = 9, gbd_round_id = 6)
all  <- all[location_id %in% unique(locs$location_id)]


## SAVE A TABLE WITHOUT NOTIFICATION DATA THAT IS TOO LOW
fix <- copy(all)
fix[, parent_loc := tstrsplit(ihme_loc_id, "_")[[1]]]

fix <- fix[!((nid == 305682) & (parent_loc %in% too_low))]
fix[, parent_loc := NULL]
## FORMAT
all[, unit_value_as_published := 1]
all[, table_num := NA]
all[, page_num := NA]
all[(specificity == "") & (!is.na(group)), `:=` (group = NA, group_review = NA, specificity = "")]

## OUTLIER BMU
#all <- all[!(ihme_loc_id == "BMU" & measure == "incidence" & year_start %in% c(2010, 2016))]
#all <- all[!(ihme_loc_id %like% "CHN" & measure %in% c("incidence", "mtspecific") & year_start == 2017)]

## SAVE
ifelse(!dir.exists(paste0(main_dir, "06_crosswalk_upload/", date, "/")), dir.create(paste0(main_dir, "06_crosswalk_upload/", date, "/")), FALSE)
#writexl::write_xlsx(list(extraction = all), path = paste0(main_dir, "06_crosswalk_upload/", date, "/", decomp_step, "_prev_screening_all_ltbi_divided.xlsx"))

## ENSURE GROUP IS RESOLVED
#all <- as.data.table(read_excel(paste0(main_dir, "06_crosswalk_upload/", date, "/", decomp_step, "_prev_screening_all_ltbi_divided.xlsx")))
all[, `:=` (group = NA, group_review = NA, specificity = NA)]
all[(note_modeler %like% "\""), note_modeler := gsub("\"", "", note_modeler)]
writexl::write_xlsx(list(extraction = all), path = paste0(main_dir, "06_crosswalk_upload/", date, "/", decomp_step, "_prev_screening_all_ltbi_divided.xlsx"))
writexl::write_xlsx(list(extraction = fix), path = paste0(main_dir, "06_crosswalk_upload/", date, "/", decomp_step, "_prev_screening_all_ltbi_divided_no_low_inc.xlsx"))

##
#all <- all[!((ihme_loc_id == "MAR") & (measure %in% c("incidence", "mtspecific")) & (sex == "Male"))]
#fix <- fix[!((ihme_loc_id == "MAR") & (measure %in% c("incidence", "mtspecific")) & (sex == "Male"))]

#############################################################################################
###                               UPLOAD CROSSWALK VERSION                                ###
#############################################################################################

## SAVE CROSSWALK VERSION
bundle_metadata        <- fread(paste0(upload_metadata_dir, decomp_step, "_bundle_ver_raw_data_", date, ".csv"))
cross_walk_description <- paste0(date, " GBD2019 bundle with remission; EMR (age dummy,haq,sr), EP adjusted (stgpr - 56726); ",
                                 "sex split (age midpoint); smear adjusted (age midpoint; sex dummy); screening adjusted,",
                                 "ltbi(501269)")
save_xwalk_result      <- save_crosswalk_version(bundle_version_id = bundle_metadata$bundle_version_id,
                                                 data_filepath     = paste0(main_dir, "06_crosswalk_upload/", date, "/", decomp_step, "_prev_screening_all_ltbi_divided.xlsx"),
                                                 description       = cross_walk_description)

## SAVE CROSSWALK RESULT METADATA
save_xwalk_result[, description := cross_walk_description]
save_xwalk_result[, uploaded_file := paste0(main_dir, "06_crosswalk_upload/", date, "/", decomp_step, "_prev_screening_all_ltbi_divided.xlsx")]
fwrite(save_xwalk_result, file = paste0(upload_metadata_dir, decomp_step, "_xwalk_screening_metadata_", date, ".csv"), row.names = F)

## SAVE CROSSWALK VERSION
cross_walk_description <- paste0(date, " - REMOVE LOW INC; EMR (age dummy,haq,sr), EP adjusted (stgpr - 56726); age-split; ",
                                 "sex split (age midpoint w/ splines); smear adjusted (age midpoint; sex dummy); screening adjusted",
                                 "ltbi (399674)")
save_xwalk_result      <- save_crosswalk_version(bundle_version_id = bundle_metadata$bundle_version_id,
                                                 data_filepath     = paste0(main_dir, "06_crosswalk_upload/", date, "/", decomp_step, "_prev_screening_all_ltbi_divided_no_low_inc.xlsx"),
                                                 description       = cross_walk_description)


#############################################################################################
###                                       DONE                                            ###
#############################################################################################




