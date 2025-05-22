# Title: Format Eurostat
# Purpose: Format data for further processing and collation with other envelope sources
# Input: Eurostat flat file
# Output: `formatted_eurostat` in the `output_data_dir` and global environment
# Author: USERNAME

# Hospital discharges by diagnosis, in-patients, total number. All causes of diseases (A00-Z99) excluding V00-Y98 and Z38
# Z38*: Liveborn infants according to place of birth and type of delivery.2024 ICD-10-CM Codes Z38*: Liveborn infants according to place of birth and type of delivery.
# Confirmed that this excludes live births and day cases. Add to metadata.

# initial formatting
format_columns <- function(dt) {
  dt <- as.data.table(dt)
  cat("\nFormatting Eurostat...\n")
  cat("\nRaw data contains columns: ", paste0(names(dt), collapse = ", "), "\n")
  
  cols_to_remove <- c('DATAFLOW', 'LAST UPDATE', 'freq', 'indic_he', 'unit', 'icd10', 'OBS_FLAG')
  cat(yellow("Removing columns: "), paste0(cols_to_remove, collapse = ", "), "\n")
  dt <- dt %>% select(-(all_of(cols_to_remove)))
  
  # not renaming geo deliberately. merging location_metadata on geo later, then deselecting the col
  setnames(dt, old = c("age", "sex", "TIME_PERIOD", "OBS_VALUE"),
           new = c("age", "sex", "year_id", "cases")) # map_id is the ISO in location_metadata
  cat("New columns: ", paste0(names(dt), collapse = ", "), "; Rows: ", nrow(dt))
  
  return(dt)
}
# assign sex IDs
get_sex_ids <- function(dt) {
  
  cat(yellow("\nFormatting sex_id"))
  sex_ids <- get_ids("sex")
  cat(yellow("\nUnique sex values in dt: ", paste0(unique(dt$sex), collapse = ", ")))
  
  cat(yellow("\nFiltering out both-sex and unknown sex case counts and getting sex IDs"))
  dt <- dt[!(sex %in% c("T", "UNK")), ]
  dt[sex == "F", sex := "Female"][sex == "M", sex := "Male"]
  
  dt <- merge(dt, sex_ids, by = "sex", all.x = T)
  
  # warn if there are missing sex_id values in dt now
  if (any(is.na(dt$sex_id))) {
    cat(red("\nMissing sex_id values for ", paste0(unique(dt[is.na(sex_id) == TRUE, .(sex_id)]))))
  } else {
    cat(green("\nAll sex_id values are filled!"))
  }
  return(dt)
}
# assign age start and end values
get_age_start_end <- function(dt) {
  setDT(dt) # Convert df to a data.table if it's not already
  
  cat("Starting age group processing...\n")
  
  # Filter out the rows with age values of "TOTAL", "UNK"
  dt_filtered <- dt[!age %in% c("TOTAL", "UNK")]
  cat("Filtered out 'TOTAL' and 'UNK' ...\n")
  
  cat(yellow("Y_GE90, Y_GE95, Y_LT1 are handled manually - those are 90 plus, 95 plus, and less than 1 year, respectively.\n"))
  
  # Handle special cases and general cases
  dt_filtered[, `:=` (age_start = fifelse(age == "Y_GE90", 90,
                                          fifelse(age == "Y_GE95", 95,
                                                  fifelse(age == "Y_LT1", 0, NA_integer_))),
                      age_end = fifelse(age %in% c("Y_GE90", "Y_GE95"), 125,
                                        fifelse(age == "Y_LT1", 1, NA_integer_)))]
  
  # age=="Y_LT14" age_start = 0, age_end = 15
  dt_filtered[age == "Y_LT14", `:=` (age_start = 0, age_end = 15)]
  dt_filtered[age == "Y_GE65", `:=` (age_start = 65, age_end = 125)]
  
  # Process the general case where it's not one of the special cases
  dt_filtered[is.na(age_start), age_start := as.numeric(sub("Y", "", sub("-.*", "", age)))]
  dt_filtered[is.na(age_end), age_end := as.numeric(sub(".*-", "", sub("Y", "", age)))]
  
  # Add 1 to age end where age_end %% 5 == 4
  dt_filtered[age_end %% 5 == 4, age_end := age_end + 1]
  
  # Remove NA rows that were placeholders for the special cases
  dt_filtered[!is.na(age_start) & !is.na(age_end)]
  
  cat("Processed age groups into 'age_start' and 'age_end'. Special cases for 'Y_GE90', 'Y_GE95', and 'Y_LT1' handled.\n")
  
  return(dt_filtered)
}
# assign location IDs
get_location_ids <- function(dt) {
  
  cat(yellow("\nFetching location_id from location_metadata through map_id (3-character ISO code)..."))
  cat(yellow("\nFiltering dt to rows where geo is not empty (nchar>0) and not NA..."))
  
  # write country names of these BE, CH, CY, CZ, DE, DK, ES, FI, FR, HR, HU, IE, IS, IT, LT, MT, NL, NO, PL, PT, RO, RS, SE, SI, SK, TR, UK please
  locations_dict <- data.table(
    location_name = c("Austria", "Belgium", "Switzerland", "Cyprus", "Czechia", "Germany", "Denmark", "Estonia", "Spain", "Finland", "France", "Croatia", "Hungary", "Ireland", "Iceland", "Italy", "Lithuania", "Luxembourg", "Latvia", "North Macedonia", "Malta", "Netherlands", "Norway", "Poland", "Portugal", "Romania", "Serbia", "Sweden", "Slovenia", "Slovakia", "Türkiye", "United Kingdom"),
    geo = c("AT", "BE", "CH", "CY", "CZ", "DE", "DK", "EE", "ES", "FI", "FR", "HR", "HU", "IE", "IS", "IT", "LT", "LU", "LV", "MK", "MT", "NL", "NO", "PL", "PT", "RO", "RS", "SE", "SI", "SK", "TR", "UK")
  )
  
  # merge on dt by geo
  dt <- merge(dt, locations_dict, by = "geo", all.x = T)
  
  # check if any location_name in dt are missing
  if (any(is.na(dt$location_name))) {
    cat(red("\nMissing location_name values for ", paste0(unique(dt[is.na(location_name) == TRUE, .(geo)]))))
  } else {
    cat(green("\nAll location_name values are filled!"))
  }
  
  # merge on location_metadata to get location_id and then remove geo and location_name columns
  dt <- merge(dt, location_metadata[level == 3, .(location_id, location_name)], by = "location_name", all.x = T)
  dt <- dt[, c("location_name") := NULL]
  
  # check if any location_name in dt are missing
  if (any(is.na(dt$location_id))) {
    cat(red("\nMissing location_id values for ", paste0(unique(dt[is.na(location_id) == TRUE, .(geo)]))))
  } else {
    dt <- dt[, geo := NULL]
    cat(green("\nAll location_id values are filled!"))
  }
  
  return(dt)
}
# pull sample size from populations
get_sample_size <- function(dt) {
  cat(yellow("\nFetching sample_size from populations..."))
  
  cat(yellow("Need to get populations for regular age groups and then sum for nonstandard ones.\n",
             "For example, for 1 to 4 years, I would sum populations per loc-year-sex for 12 to 23 months and 2 to 4 years\n",
             "For <1 year, I would sum populations for all the 4 disaggregated neonates\n",
             "For 90 plus, I would sum populations for 90 to 94 and 95 plus\n"))
  
  cat(yellow("\nImporting population metadata...\n"))
  
  populations <- get_population(release_id = release_id, location_id = unique(dt$location_id),
                                year_id = unique(dt$year_id), sex_id = unique(dt$sex_id),
                                age_group_id = unique(age_metadata$age_group_id))
  
  setDT(populations) # Ensure populations is a data.table
  setDT(age_metadata) # Ensure age_metadata is a data.table
  
  asly_vars <- c("location_id", "year_id", "age_group_id", "sex_id") # demographics for merging
  
  populations <- populations %>% select(all_of(asly_vars), population)
  
  # merge on age metadata to get age_start and age_end
  populations <- merge(populations, age_metadata[, .(age_group_id, age_start, age_end)], by = "age_group_id", all.x = T)
  
  # handle special populations
  # sum populations where age_end - age_start <1 by location_id year_id sex_id for neonates
  spec_dems <- dt %>% 
    select(age_start, age_end, sex_id, location_id, year_id) %>% 
    unique() %>% 
    left_join(age_metadata, by = c("age_start" = "age_start", "age_end" = "age_end")) %>% 
    filter(is.na(age_group_id)) %>% 
    select(-age_group_id, -age_group_name)
  
  result_list <- list()
  
  for (i in 1:nrow(spec_dems)) {
    row <- spec_dems[i]
    sum_pop <- populations[sex_id == row$sex_id &
                             location_id == row$location_id &
                             year_id == row$year_id &
                             age_start >= row$age_start &
                             age_end <= row$age_end, sum(population)]
    result_list[[i]] <- sum_pop
  }
  
  spec_dems$population <- unlist(result_list)
  
  populations <- rbind(populations, spec_dems, fill = TRUE) %>% 
    select(-age_group_id)
  
  # merge on dt
  dt1 <- merge(dt, populations, by = c("location_id", "year_id", "sex_id", "age_start", "age_end"), all.x = TRUE)
  
  # rename to sample_size
  setnames(dt1, "population", "sample_size")
  
  # check if there are rows with duplicate location_id year_id sex_id age_start age_end cobminations are warn if so
  if (any(duplicated(dt1, by = c("location_id", "year_id", "sex_id", "age_start", "age_end")))) {
    cat(red("\nDuplicate rows found in dt1!\n"))
    # print those rows
    print(dt1[duplicated(dt1, by = c("location_id", "year_id", "sex_id", "age_start", "age_end"))])
  } else {
    cat(green("\nNo duplicate rows found in dt1!"))
  }
  
  # happy message if no missing in populations in dt1 otherwise warn
  if (any(is.na(dt1$population))) {
    cat(red("\nMissing population values for ", paste0(unique(dt1[is.na(population) == TRUE, .(location_id, year_id)]))))
  } else {
    cat(green("\nAll population values are filled!"))
  }
  
  return(dt1)
}
# pull NID, data types and field citation value 
get_ghdx_metadata <- function(dt) {
  cat(yellow("\nRetrieving NID, data_type_name, and title from ghdx_metadata..."))
  if (!exists("ghdx_metadata")) {
    stop("\nghdx_metadata does not exist in the environment. Please load it first.")
  } else {
    cat(yellow("\nSearching GHDx DB by known title 'Eurostat Data Browser - Hospital Discharges by Diagnosis, In-Patients, Total Number' to get NID, title, and data_type_name..."))
    ghdx_row <- unique(ghdx_metadata[grepl("Eurostat Data Browser - Hospital Discharges by Diagnosis, In-Patients, Total Number", field_citation_value),])
  }
  # bind that row with all existing rows in dt
  if (nrow(ghdx_row) == 0) {
    stop("\nNo NID found for the given title. Please double-check the title.")
  } else {
    # cbind dt and ghdx_row
    dt <- cbind(dt, ghdx_row) # this will fill all the rows automatically
  }
  
  if (any(is.na(dt$nid))) {
    stop("\nNID is missing in the dt. Please double-check the input.")
  } else {
    cat(green("\nNID, data_type_name, and title are filled!\n"))
  }
  return(dt)
}

format_eurostat <- function(path) {
  
  dt <- fread(path)
  
  dt <- dt %>% 
    format_columns() %>% 
    get_sex_ids() %>% 
    get_age_start_end() %>% 
    dplyr::select(-age) %>% 
    get_location_ids() %>%
    get_sample_size() %>% 
    get_ghdx_metadata()
  
  # function to manually fill out is_clinical, recall_type, recall_type_value ----
  cat("\nFilling in underlying_nid = NA, is_clinical = 0, recall_type = 'Period: months', recall_type_value = 12 (annual recall), uses_env = 0, measure = 'continuous as those are standard values")
  dt[, `:=`(is_clinical = 0, recall_type = "Period: months", recall_type_value = 12, measure = "continuous", uses_env = 0)]
  
  # setdiff with minimum_vars
  missing_vars <- setdiff(minimum_vars, colnames(dt)) %>% print()
  
  # if missing vars is empty print happy message that formatting is complete
  if (length(missing_vars) == 0) {
    cat(green("\nNo missing variables! Checking for missing values. Given that I indicated underlying_nid, should not have any..."))
  } else {
    cat(red("\nMissing variables: ", missing_vars))
  }
  
  # final check for missing values in every column except underlying_nid, shouldn't have any
  missing_values <- dt[, lapply(.SD, function(x) sum(is.na(x))), 
                       .SDcols = vars_no_missing_values] %>% print()
  if (nrow(missing_values) > 1) {
    cat(red("\nThere are missing values in the final dt in column(s) other than `underlying_nid`. Please double-check the input."))
  } else {
    formatted_eurostat <- copy(dt)
    file_path <- paste0(run_dir, "FILEPATH")
    cat(paste0("Saved `formatted_eurostat.csv run into ", file_path, " for further processing with the other sources.\n"))
    fwrite(formatted_eurostat, paste0(file_path))
  }
  
  return(formatted_eurostat)
  
}
