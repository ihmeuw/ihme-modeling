################################################################################
# TETANUS IMPAIRMENT BUNDLE UPLOAD
# AUTHOR: REDACTED
# DATE: January 13, 2024
# PURPOSE: UPLOAD IMPAIRMENT ESTIMATES FROM TETANUS NON FATAL MODEL TO BUNDLE
# PART A: Setup
# PART B: Mild impairments data bundle upload function
# PART C: Moderate to severe impairments data bundle upload function
# This script is sourced by the tetanus 01_inverse_model.R script
################################################################################

#*******************************************************************************
# PART A: SETUP ####
#*******************************************************************************

# Source functions
source("/FILEPATH/get_location_metadata.R") 
source("/FILEPATH/get_ids.R")
source("/FILEPATH/get_bundle_data.R") 
source("/FILEPATH/upload_bundle_data.R") 
source('/FILEPATH/save_bundle_version.R')
source('/FILEPATH/get_bundle_version.R')
source('/FILEPATH/save_crosswalk_version.R')
source('/FILEPATH/get_crosswalk_version.R')
source("/FILEPATH/validate_input_sheet.R")
source("/FILEPATH/get_model_results.R")

"/FILEPATH/collapse_point.R" %>% source

#*******************************************************************************
# PART B: Mild impairments data bundle upload ####
#*******************************************************************************

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Section 1: Clear existing bundle data
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# Get tetanus mild impairments bundle, keep only seq column
print("Getting existing mild bundle data to clear")
mild_epi_data <- get_bundle_data(bundle_id = mild_bundle_id)[, .(seq)]

# Check if bundle is empty; if not clear bundle by uploading dataframe with seq column only
print("Clearing mild bundle for new upload")
if(nrow(mild_epi_data)>=1) {
  clear_old_path <- file.path(j_root, "FILEPATH", ".xlsx")
  openxlsx::write.xlsx(mild_epi_data, clear_old_path, rowNames=FALSE, colNames=TRUE, sheetName="extraction")
  upload_bundle_data(bundle_id=mild_bundle_id, filepath=clear_old_path)
}

# Get bundle data again to confirm bundle is empty
print("Loading empty bundle")
mild_epi_data <- get_bundle_data(bundle_id=mild_bundle_id)[, .(seq)]
if (length(mild_epi_data$seq) >= 1) {
  stop(paste0("STOP | Bundle ", mild_bundle_id, " not empty for new upload"))
} else {
  print("Bundle has been successfully cleared")
}

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Section 2. Format mild impairments data for upload to bundle
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

#Format results for epi uploader template -- to model in DisMod
mild_impairment <- mild_impairment %>% collapse_point #calculate draw means [?]

# format year columns
setnames(mild_impairment, "year_id", "year_start")
mild_impairment[, year_end := year_start]

# Make new columns required by epiuploader
mild_impairment[, `:=` (age_start= 0, 
                        age_end= 0,
                        bundle_id = mild_bundle_id,
                        nid = 254292,
                        modelable_entity_id = 1427,
                        modelable_entity_name = "Mild impairment due to neonatal tetanus",
                        measure = "incidence",
                        input_type = "adjusted", 
                        source_type = "Mixed or estimation", 
                        representative_name = "Nationally representative only", 
                        urbanicity_type = "Unknown",
                        unit_type = "Person",
                        unit_value_as_published = 1,
                        uncertainty_type_value = 95,
                        recall_type = "Point",
                        extractor = username)]

# Add sex column
mild_impairment[, sex := ifelse(sex_id == 2, "Female", "Male")]

# Create additional columns
mild_impairment[, c("underlying_nid", "parent_id", "underlying_field_citation_value", "field_citation_value", "file_path",
                    "page_num", "table_num", "site_memo", "cases", "sample_size", "effective_sample_size", "design_effect",
                    "measure_adjustment", "uncertainty_type", "standard_error", "recall_type_value", "sampling_type",
                    "response_rate", "case_name", "case_definition", "case_diagnostics", "group", "specificity",
                    "group_review", "note_modeler", "note_sr", "seq", "seq_parent") := ""] 
mild_impairment[, c("sex_issue", "year_issue", "age_issue", "age_demographer", "measure_issue", "is_outlier",
                    "smaller_site_unit") := 0]
mild_impairment[, c("age_group_id", "sex_id") := NULL]

# Assign correct value to representative name column               
mild_impairment[location_id %in% locations[level >= 4, location_id], representative_name := "Representative for subnational location only"]

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Section 3. Save, upload, version and crosswalk data in bundle
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# Save new data
new_path <- file.path(j_root, FILEPATH, ".xlsx")
mild_impairment[, data_sheet_file_path := new_path]
openxlsx::write.xlsx(mild_impairment, new_path, rowNames=FALSE, colNames=TRUE, sheetName="extraction")

# Validate input sheet containing new data
result <- validate_input_sheet(bundle_id=mild_bundle_id, new_path, "/FILEPATH/")

if(result$status != "passed") {stop()}

# Upload new data to bundle
print("Uploading mild bundle with new data")
active_bundle_upload <- upload_bundle_data(bundle_id=mild_bundle_id, filepath=new_path)

# Save bundle metadata to version log
versions_path   <- file.path(j_root, "FILEPATH/combined_impairments_version_log.csv")
version_df <- data.frame("date"         = custom_version,
                         "impairment"   = "mild",
                         "version"      = "active bundle",
                         "version_id"   = active_bundle_upload$request_id[1],
                         "data_path"    = new_path,
                         "description"  = "Mild impairment")

versions_file   <- fread(versions_path) %>% rbind(., version_df, fill=TRUE)

# Save active data in bundle as bundle version

# Get bundle data again
print("Getting the mild bundle with new, active data")
active_bundle   <- get_bundle_data(bundle_id=mild_bundle_id)

# Save bundle version
print("Saving a mild bundle version")
bundle_metadata <- save_bundle_version(bundle_id=mild_bundle_id)

# Append bundle_metadata to file versions log
version_df      <- data.frame("date"         = custom_version,
                              "impairment"   = "mild",
                              "version"      = "bundle",
                              "version_id"   = bundle_metadata$bundle_version_id[1],
                              "data_path"    = new_path,
                              "description"  = "")
versions_file   <- fread(versions_path) %>% rbind(., version_df, fill=TRUE)
fwrite(versions_file, versions_path, row.names=FALSE)

# Load newly saved bundle version
print("Getting  mild bundle version")
bv <- get_bundle_version(bundle_version_id=bundle_metadata$bundle_version_id[1], export=TRUE, fetch = "all")

# Make additional column edits 
bv <- bv[, crosswalk_parent_seq := seq]
bv <- bv[, c("group", "specificity", "group_review", "seq") := NA]

# Subset to locations in location_set_id 9: DisMod locations
bv <- bv[location_id %in% dismod_locs]

# Save new file
bv_path <- file.path(j_root, FILEPATH, ".xlsx")
openxlsx::write.xlsx(bv, bv_path, rowNames=FALSE, colNames=TRUE, sheetName="extraction")

# Save file as a crosswalk version to be used in DisMod model
print("Saving mild bundle crosswalk version for Dismod")
crosswalk_description <- paste0("For DisMod mild model, made ", custom_version)
xw_metadata           <- save_crosswalk_version(bundle_version_id=bundle_metadata$bundle_version_id[1], description=crosswalk_description,
                                                data_filepath=bv_path)

# Append xw_metadata to file versions log
version_df            <- data.frame("date"         = custom_version,
                                    "impairment"   = "mild",
                                    "version"      = "crosswalk",
                                    "version_id"   = xw_metadata$crosswalk_version_id[1],
                                    "data_path"    = bv_path,
                                    "description"  = crosswalk_description)
versions_file <- fread(versions_path) %>% rbind(., version_df, fill=TRUE)
fwrite(versions_file, versions_path, row.names=FALSE)

#*******************************************************************************
# PART C: Moderate to severe data upload to bundle ####
#*******************************************************************************

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Section 1: Clear existing bundle data
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# Get existing moderate to severe bundle data, keeping seq column only 
modsev_epi_data <- get_bundle_data(bundle_id=modsev_bundle_id)[, .(seq)]

# Clear bundle if not yet empty
if(nrow(modsev_epi_data) >= 1){
  clear_old_path_modsev <- file.path(j_root, "FILEPATH", ".xlsx")
  openxlsx::write.xlsx(modsev_epi_data, clear_old_path_modsev, rowNames=FALSE, colNames=TRUE, sheetName="extraction")
  print("Clearing mod/sev bundle for new upload")
  upload_bundle_data(bundle_id=modsev_bundle_id, filepath=clear_old_path_modsev)
}

# Reload bundle data to make sure it is empty
modsev_epi_data <- get_bundle_data(bundle_id=modsev_bundle_id)[, .(seq)]
if (length(modsev_epi_data$seq) >= 1) {stop(paste0("STOP | Bundle ", modsev_bundle_id, " not empty for new upload"))
} else {(print("The moderate to severe bundle was successfully cleared"))
}
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Section 2: Format new data for upload to bundle
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

#format results for epi uploader template 
mod_sev_impairment <- mod_sev_impairment %>% collapse_point #calculate row means

# Align sex information
mod_sev_impairment[, sex := ifelse(sex_id == 2, "Female", "Male")]

# Format year data 
setnames(mod_sev_impairment, "year_id", "year_start")
mod_sev_impairment[, year_end := year_start]

# Remove columns
mod_sev_impairment[, c("age_group_id", "sex_id") := NULL]

# Create new columns 
mod_sev_impairment[, `:=` (age_start= 0, 
                           age_end = 0,
                           bundle_id = modsev_bundle_id, 
                           nid = 254293, 
                           modelable_entity_id = 1428, 
                           modelable_entity_name = "Moderate to severe impairment due to neonatal tetanus", 
                           measure = "incidence", 
                           input_type = "adjusted", 
                           source_type = "Mixed or estimation", 
                           representative_name = "Nationally representative only", 
                           urbanicity_type = "Unknown",
                           unit_type = "Person",
                           unit_value_as_published = 1,
                           uncertainty_type_value = 95,
                           recall_type = "Point",
                           extractor = username)]

# Format additional columns
mod_sev_impairment[, c("underlying_nid", "parent_id", "underlying_field_citation_value", "field_citation_value", "file_path",
                       "page_num", "table_num", "site_memo", "cases", "sample_size", "effective_sample_size", "design_effect",
                       "measure_adjustment", "uncertainty_type", "standard_error", "recall_type_value", "sampling_type",
                       "response_rate", "case_name", "case_definition", "case_diagnostics", "group", "specificity",
                       "group_review", "note_modeler", "note_sr", "seq", "seq_parent") := ""] 

mod_sev_impairment[, c("sex_issue", "year_issue", "age_issue", "age_demographer", "measure_issue", "is_outlier",
                       "smaller_site_unit") := 0]

# Fix representative name column
mod_sev_impairment[location_id %in% locations[level >= 4, location_id], representative_name := "Representative for subnational location only"]

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Section 3. Load neonatal encephalitis data and combine with tetanus impairments
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# Per GBD2021 notes, excess mortality data from neonatal encephalopathy to use in moderate-severe tetanus mode
# find crosswalk version used in current bested neonatal encephalopathy model by pulling best model from one location, sex
ne_emr_xw <- get_model_results(gbd_team = "epi", 
                               gbd_id = 8653, 
                               release_id = release_id,
                               status = "best", 
                               location_id = 6, 
                               sex_id = 1)

# Get neonatal encephalopathy crosswalk version and keep mt excess measure
emr_xw <- unique(ne_emr_xw$crosswalk_version_id[ne_emr_xw$measure == "mtexcess"])
ne_emr <- get_crosswalk_version(crosswalk_version_id = emr_xw)

# Keep only excess mortality data
ne_emr <- ne_emr[measure == "mtexcess", ]

# Assign age start and age end to be the midpoint of the age_range. 
# Then drop every other age group over 5, with top age group being 97.5 
my_emr <- copy(ne_emr)
my_emr <- my_emr[age_end <= 100]
my_emr[, age := (age_start + age_end)/2]
my_emr[, c("age_start", "age_end"):= age]
my_emr <- my_emr[!(age %in% c(12.5, 22.5, 32.5, 42.5, 52.5, 62.5, 72.5, 82.5, 92.5)),] # max age for which we estimate tetanus NF is age grp 235 (99 + )

# Clean columns to prepare for upload
my_emr[, seq:=NA]

# Remove columns that are added in bundle_version and xw stage 
my_emr[, c("crosswalk_parent_seq","year_id", "age", "origin_id", "origin_seq") :=NULL]

# Remove variance column as already have standard error
my_emr[, variance := NULL]

# Add modeler note
my_emr[, note_modeler := paste0("EMR data from NE model meid 8653, model_version: ", 
                                unique(ne_emr_xw$model_version_id), ", crosswalk version: ", emr_xw)]

# Fix missing column data
my_emr[, bundle_id := modsev_bundle_id]
my_emr[, modelable_entity_id := 1428]
my_emr[, modelable_entity_name := "Moderate to severe impairment due to neonatal tetanus"]

cat(paste0("Mod/Sev EMR input data from NE model input data crosswalk version: ", emr_xw),
    file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)

# Bind moderate to severe impairment data frame to the neonatal EMR dataframe
mod_sev_impairment <- rbind(mod_sev_impairment, my_emr, fill = TRUE)

# temp fix for upper q  < mean
mod_sev_impairment[, upper := ifelse(upper < mean, mean, upper)]

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Section 4. Save, upload, version and crosswalk data in bundle
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# Save and upload model results to bundle
new_path_modsev <- file.path(j_root, "FILEPATH", ".xlsx")
mod_sev_impairment[, data_sheet_file_path := new_path_modsev]
openxlsx::write.xlsx(mod_sev_impairment, new_path_modsev, rowNames=FALSE, colNames=TRUE, sheetName="extraction")
active_bundle_upload <- upload_bundle_data(bundle_id = modsev_bundle_id, 
                                           filepath = new_path_modsev)

# Get active data again
active_bundle   <- get_bundle_data(bundle_id = modsev_bundle_id)

# Save active data as a bundle version
bundle_metadata <- save_bundle_version(bundle_id = modsev_bundle_id)

# Load new bundle version and format for crosswalk [or make automatic crosswalk above when saving bundle version]
bv <- get_bundle_version(bundle_version_id=bundle_metadata$bundle_version_id[1], export=TRUE, fetch = "all")

# Format columns
bv <- bv[, crosswalk_parent_seq := seq]
bv <- bv[, c("group", "specificity", "group_review", "seq") := NA]

# Subset to locations in location_set_id 9: DisMod model locations
bv <- bv[location_id %in% dismod_locs]

# Save file
bv_path <- file.path(j_root, "FILEPATH", ".xlsx")
openxlsx::write.xlsx(bv, bv_path, rowNames=FALSE, colNames=TRUE, sheetName="extraction")

# Upload data as a crosswalk version
crosswalk_description <- paste0("For DisMod modsev model, made ", custom_version)
xw_metadata           <- save_crosswalk_version(bundle_version_id=bundle_metadata$bundle_version_id[1], description=crosswalk_description,
                                                data_filepath=bv_path)
print(paste0("The moderate to severe bundle crosswalk id is ", xw_metadata$crosswalk_version_id))

##END SCRIPT####################################################################