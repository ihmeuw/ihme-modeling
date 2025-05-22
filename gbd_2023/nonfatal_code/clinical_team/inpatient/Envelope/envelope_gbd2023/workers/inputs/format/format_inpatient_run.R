# Title: Format aggregated clinical master data
# Purpose: Format data for further processing and collation with other envelope sources
# Input: aggregated master data from a specified inpatient run
# Output: master data in line with the bundle shape saved as a flat .CSV file
# Author: USERNAME

# Wrap the script into a function

# Define a custom function to read each file and add a 'clinical_run_id' column
read_in_files_with_id <- function(dir) {
  # Extract run ID from the file name using a regular expression
  # This assumes the run ID is always right before ".csv" and following the last underscore
  run_id <- sub(".*_([0-9]+)\\.csv$", "\\1", basename(dir))

  # Read the file
  dt <- fread(dir)

  # Add the 'clinical_run_id' column
  dt[, clinical_run_id := as.integer(run_id)]

  return(dt)
}

# Step 1: extract location_name from ghdx_subset
get_location_years <- function(df) {
  known_multi_word_countries <- c("Czech Republic", "United Kingdom")
  
  df <- df %>%
    mutate(
      location_name = if_else(str_detect(underlying_field_citation_value, "United States - "),
                              str_remove(underlying_field_citation_value, "United States - "),
                              underlying_field_citation_value
      ),
      location_name = str_extract(location_name, "^[^-]+")
    ) %>%
    mutate(location_name = case_when(
      str_detect(location_name, " State") ~ str_remove(location_name, " State.*"),
      str_detect(field_citation_value, "European Hospital Morbidity Database") &
        str_detect(location_name, paste(known_multi_word_countries, collapse = "|")) ~ str_extract(location_name, paste(known_multi_word_countries, collapse = "|")),
      str_detect(field_citation_value, "European Hospital Morbidity Database") ~ str_extract(location_name, "^\\w+"),
      TRUE ~ location_name
    )) %>%
    mutate(year_id = as.numeric(str_extract(underlying_field_citation_value, "\\d{4}"))) %>%
    mutate(location_name = if_else(location_name == "Czech Republic", "Czechia", location_name))
  
  # check for missing and print ifelse statement
  if (any(is.na(df$location_name))) {
    stop("Location name is missing in the df. Please double-check the input.")
  } else {
    cat(green("\nLocation name and year_id are filled!\n"))
  }
  
  return(df)
}

# Format the data table
format_inpatient_run <- function() {
  
  # Use the modified path and pattern, assuming `run_dir` is already defined
  dir <- paste0(run_dir, "data/clinical_inpatient/")
  
  # Read in all .csv files, apply the custom function, and rbind the results
  # cat()
  dt <- lapply(
    list.files(dir, pattern = "\\.csv$", full.names = TRUE),
    read_in_files_with_id
  ) %>%
    rbindlist(use.names = TRUE, fill = TRUE)
  
  # Omit rows with missing demographics (ASLY vars)
  dt <- dt[sex_id %in% c(1,2), ]
  dt <- dt[is.na(location_id) == F, ]
  dt <- na.omit(dt, "year_id")
  dt <- na.omit(dt, "age_group_id")
  
  # Remove all age estimates - in the clinical data, those with age_start 0 age_end 125 are "Unknown" and should not be included
  dt <- dt[!(age_start == 0 & age_end == 125), ]
  
  # remove mean
  dt[, mean := NULL]
  
  # Validate column names
  missing_cols <- setdiff(minimum_vars, names(dt))
  
  # if missing_columns are present, print a statement that these columns will be filled, otherwise say proceeding
  if (length(missing_cols) > 0) {
    cat("\nChecking for missing columns after initial import...")
    cat(yellow(paste0("\nThe following columns are missing from the master data and will be filled: ", paste0(missing_cols, collapse = ", "))))
  } else {
    cat("\nAll columns are present in the master data. Proceeding...")
  }
  
  # process NIDs
  # 2024-07-29 manually edit NIDs for Argentina
  dt[nid == 433042, `:=`(nid = 174085)]
  dt[nid == 433045, `:=`(nid = 114624)]
  
  # subset ghdx_metadata to nid that are in dt$nid
  nid_vector <- ghdx_metadata[nid %in% dt$nid & is.na(underlying_nid) == FALSE, unique(nid)]
  
  # ghdx subset for nids that have underlying nids
  ghdx_unid <- ghdx_metadata[nid %in% nid_vector, ]
  
  # ghdx subset for nids that do not have unlderying nids
  ghdx_no_unid <- ghdx_metadata[!(nid %in% nid_vector), ]
  
  # should sum up to the total number of rows in ghdx_metadata
  nrow(ghdx_unid) + nrow(ghdx_no_unid) == nrow(ghdx_metadata)
  
  # Apply the function to your dataframe
  ghdx_unid <- get_location_years(ghdx_unid)
  
  # Step 2: get location_id by location_name
  ghdx_unid <- merge(ghdx_unid, location_metadata[, .(location_name, location_id)], by = "location_name", all.x = TRUE)
  ghdx_unid[, location_name := NULL]
  
  # get NID vector
  # nid_vector <- ghdx_metadata[nid %in% dt$nid & is.na(underlying_nid) == FALSE, unique(nid)]
  dt1 <- dt %>% filter(nid %in% nid_vector)
  dt1 <- merge(dt1, ghdx_unid, by = c("nid", "location_id", "year_id"), all.x = TRUE)
  
  dt2 <- dt %>% filter(!(nid %in% nid_vector))
  
  # Add aggregated columns directly in the original data table
  ghdx_no_unid[, `:=`(
    aggregated_series_nid = paste(unique(series_nid), collapse = ";"),
    aggregated_series_field_citation_value = paste(unique(series_field_citation_value), collapse = ";")
  ), by = .(nid)]
  
  # Then, remove duplicates based on 'nid' while keeping the first occurrence
  ghdx_no_unid <- unique(ghdx_no_unid, by = "nid")
  
  # remove and rename
  ghdx_no_unid <- ghdx_no_unid[, c("series_nid", "series_field_citation_value") := NULL]
  # rename to original
  setnames(ghdx_no_unid, c("aggregated_series_nid", "aggregated_series_field_citation_value"), c("series_nid", "series_field_citation_value"))
  
  dt2 <- merge(dt2, ghdx_no_unid, by = "nid", all.x = TRUE)
  
  dt <- rbind(dt1, dt2)
  
  # Manually fill out underlying_nid, is_clinical, recall_type, recall_type_value
  cat("\nFilling in is_clinical = 1, recall_type = 'Period: months', recall_type_value = 12 (annual recall) as those are standard values\n")
  dt[, `:=`(is_clinical = 1, recall_type = "Period: months", recall_type_value = 12)]
  
  # Get uses_env values from a flat file from Ray
  # uses_env_dt_path <- paste0(input_data_dir, "master_data/run_", run_id, "/uses_env/clinical_sources_in_envelope_v2_RM(V2).csv")
  uses_env_dt_path <- "FILEPATH"
  uses_env_dt <- fread(uses_env_dt_path)
  uses_env_dt <- uses_env_dt %>%
    as.data.table() %>%
    select(nid, year_id, uses_env)
  
  # if uses_env is missing, merge on uses_env_dt and print the result, then check is uses_env values are missing
  if (!all(c("uses_env" %in% names(dt)))) {
    dt <- merge(dt, uses_env_dt, by = c("nid", "year_id"), all.x = TRUE)
    cat(paste0("\nMerged clinical inpatient run data on uses_env_dt to get uses_env. Dt imported from ", uses_env_dt_path, " on ", Sys.Date()))
    
    dt[location_id == 81, uses_env := 0] # Germany
  }
  
  dt[location_id == 97, uses_env := 1] # Argentina
  
  # print if there are uses_env values missing in dt
  if (sum(is.na(dt$uses_env)) > 0) {
    cat(yellow("\nuses_env values are missing in the master data"))
  } else {
    cat(green("\nNo missing `uses_env` values in the processed master data. Proceeding..."))
  }
  
  # setdiff with minimum_vars
  missing_vars <- setdiff(minimum_vars, colnames(dt)) %>% print()
  
  # if missing vars is empty print happy message that formatting is complete
  if (length(missing_vars) == 0) {
    cat(green("\nNo missing variables! Checking for missing values. Given that I indicated underlying_nid, should not have any..."))
  } else {
    cat(red("\nMissing variables: ", missing_vars))
  }
  
  
  dt[nid == 3822, `:=`(field_citation_value = "European Hospital Morbidity Database, 2008", data_type_name = "Administrative data")]
  dt[nid == 90319, `:=`(field_citation_value = "United States State Inpatient Databases 2008", data_type_name = "Administrative data")]
  dt[nid == 90322, `:=`(field_citation_value = "United States State Inpatient Databases 2009", data_type_name = "Administrative data")]
  
  dt[nid %in% c(114624, 174085), uses_env := 1]
  
  # final check for missing values in every column except underlying_nid, shouldn't have any
  missing_values <- dt[, lapply(.SD, function(x) sum(is.na(x))),
                       .SDcols = vars_no_missing_values
  ] %>% print()
  
  # Save the object and output flat file ----
  cat(green("\nSuccess! Assigning the formatted inpatient run to `processed_inpatient_run` data.table object. Proceed with data formatting."))
  formatted_inpatient_run <- copy(dt)
  
  # saving csv for further processing with the other sources
  file_path <- paste0(run_dir, "FILEPATH")
  fwrite(formatted_inpatient_run, paste0(file_path))
  
  # explain
  cat(paste0("Saved `formatted_inpatient run into ", file_path, " for further processing with the other sources.\n"))
  
  return(formatted_inpatient_run)
  
}
