#### EUROCAT Processing
rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

library('openxlsx')
library('tidyr')
library('dplyr')
library('data.table')

source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source(paste0("FILEPATH", "process_eurocat_functions.R"))

data_dir <- "FILEPATH"
map_dir <- "FILEPATH"

data_years <- c(2011, 2012, 2014, 2015, 2016, 2017)
registries <- c("associate/", "full/")


#### Assemble registry data ####
#returns all unique rows of eurocat data
eurocat_original <- create_raw_eurocat(data_dir, data_years, registries)

#### Map to GBD locations and drop non GBD locations ####
# drops non GBD locations
location_drops <- as.data.table(read.xlsx(paste0(j, "FILEPATH")))
loc_keeps <- unique(eurocat_original$registry_name)
loc_keeps <- loc_keeps[!loc_keeps %in% location_drops$location_name]

eurocat_original <- eurocat_original[registry_name %in% loc_keeps]

### format locs - maps to eurocat NIDs and underlying NIDS
format_locs <- format_locations(eurocat_original)

#### Split Italy, Poland, United Kingdom data into more granular GBD locations ####
italy <- create_italy_subnationals(format_locs, gbd_round, step)
poland <- split_poland(format_locs, map_dir, gbd_round, step)
uk <- split_uk(format_locs, gbd_round, step)

format_locs <- format_locs[!location_name %in% c("Italy", "Poland", "United Kingdom")]
format_locs[, note_SR := paste0("Data directly from ", location_name, " registry entry")]
format_locs[, smaller_site_unit := 1] # data  collected in a location more granular than the assigned location_id

#confirmed representative_name
format_locs[location_name %in% c("Malta", "Hungary", "Czech Republic", "Finland"), representative_name := "Nationally representative only"]
format_locs[is.na(representative_name), representative_name := "Representative for subnational location only"]
format_locs <- rbind(format_locs, italy, poland, uk)

case_map <- as.data.table(read.xlsx(paste0(j, "FILEPATH")))

#### map to Level 1 and Level 2 bundles ####
level1 <- merge_level1(format_locs) # merges level1 bundles by case_name
level2 <- merge_level2(format_locs) # merges level2 bundles by case_name

final_list <- list(level1, level2)
final <- rbindlist(final_list, use.names = TRUE, fill = TRUE, idcol = FALSE)


#################################################
############### epi formatting ##################
#################################################
add_cols <- c("seq", "seq_parent", "input_type", "underlying_field_citation_value")
final[, c(add_cols):= '']
final[, file_path := "FILEPATH"][, c("page_number", "table_num") := ''][, source_type := "Registry - congenital"]
loc_md <- get_location_metadata(location_set_id = 35, gbd_round_id = gbd_round)
loc_md <- loc_md[, .(location_id, ihme_loc_id, location_name)]

#registry named "Czech republic" renames to GBD name "Czechia
final[location_name %like% "Czec", location_name := "Czechia"]

final <- merge(final, loc_md, by = 'location_name', all.x = TRUE)
final[, location_name := paste0(location_name, "|", ihme_loc_id)]
final[, sex_issue := 0][, year_end := year_start] 

#### apply cause_specific sex restrictions ####
final <- sex_specific_denom(final, gbd_round, step)

final[, c("year_issue", "age_start", "age_end", "age_issue", "age_demographer") := 0]
final[, measure := "prevalence"][, c("mean", "lower", "upper", "standard_error", "effective_sample_size") := '']
final[, design_effect := ''][, unit_type := "Person"][, unit_value_as_published := 1]
final[, c("measure_issue", "measure_adjustment") := 0] 
final[, c("uncertainty_type", "uncertainty_type_value") := ''][, urbanicity_type := "Mixed/both"]
final[, c("recall_type_value", "sampling_type", "response_rate") := '']
final[, c("case_definition", "case_diagnostics","note_modeler") := '']
final[, extractor := "chikeda"]
final[, is_outlier := 0][, data_sheet_filepath := '']
final[, c("cv_inpatient", "cv_aftersurgery") := 0] 

final[, short_registry_name := site_memo]
final[, cases := cases_lb]
final[, mean := cases/sample_size]
final <- final[!is.na(mean)]

write.xlsx(final, paste0("FILEPATH"),
           row.names = FALSE, sheetName = "extraction")
