# Title: Format MNG_H_INFO claims formatting
# Purpose: Format data for further processing and collation with other envelope sources
# Input: Mongolia claims data
# Output: `formatted_MNG_H_INFO` in the `output_data_dir` and global environment
# Author: USERNAME


# Get aggregated master data in a single object
read_in_files <- function(file, key) {
  fread(file) %>% as.data.table(file)
}
# filter_select function
filter_select <- function(dt) {
  dt <- dt %>%
    filter(sex_id %in% c(1, 2)) %>%
    filter(!is.na(location_id)) %>%
    filter(!is.na(year_id)) %>%
    filter(!is.na(age_group_id)) %>%
    return(dt)
}

# complete function
format_mng_h_info <- function(path) {
  # Import data
  claims_files <- list.files(path, pattern = "*.csv", full.names = T)
  claims_data <- lapply(claims_files, read_in_files) %>% rbindlist(use.names = T, fill = T)
  dt <- copy(claims_data) %>% as.data.table()

  dt <- dt %>%
    filter_select()

  # Format cols ----

  # get the minimum set of variables that should be present in the master data
  # keep columns in names(template_dt) and add missing columns to dt
  missing_cols <- setdiff(minimum_vars, names(dt))

  # remove columns that are present in the dt but not among minimum_vars
  dt <- dt[, !setdiff(names(dt), minimum_vars), with = F]

  # if missing_columns are present, print a statement that these columns will be filled, otherwise say proceeding
  if (length(missing_cols) > 0) {
    cat("\nChecking for missing columns after initial import...")
    cat(yellow(paste0("\nThe following columns are missing from the master data and will be filled: ", paste0(missing_cols, collapse = ", "))))
  } else {
    cat("\nAll columns are present in the master data. Proceeding...")
  }

  # if data_type_name and title are missing, merge on ghdx_db_output and print the respective statement
  if (!all(c("data_type_name" %in% names(dt), "title" %in% names(dt)))) {
    dt <- merge(dt, ghdx_metadata, by = "nid", all.x = TRUE)
    cat("\nMerged clinical inpatient run data on ghdx_db_output to get data_type_name and title")
  }

  # if no missing values present in the data, print that, otherwise throw a message that there are missing values and in which columns
  if (sum(is.na(dt)) == 0) {
    cat(green("\nNo missing values present in the master data after merge on GHDx output"))
  } else {
    cat(yellow(paste0("\nMissing values present in the following columns: ", names(dt)[colSums(is.na(dt)) > 0])))
  }

  # manually fill out underlying_nid, is_clinical, recall_type, recall_type_value
  cat("\nFilling in is_clinical = 1, recall_type = 'Period: months', recall_type_value = 12 (annual recall) as those are standard values")
  dt[, `:=`(is_clinical = 1, recall_type = "Period: months", recall_type_value = 12)]

  # Get uses_env values from a flate file from Ray
  uses_env_dt_path <- "FILEPATH"

  uses_env_dt <- fread(uses_env_dt_path)
  uses_env_dt <- uses_env_dt %>%
    as.data.table() %>%
    select(nid, year_id, uses_env)

  # if uses_env is missing, merge on uses_env_dt and print the result, then check is uses_env values are missing
  if (!all(c("uses_env" %in% names(dt)))) {
    dt <- merge(dt, uses_env_dt, by = c("nid", "year_id"))
    cat(paste0("\nMerged MNG_H_INGO on uses_env_dt to get uses_env. Dt imported from ", uses_env_dt_path, " on ", Sys.Date()))
  }

  # print if there are uses_env values missing in dt
  if (sum(is.na(dt$uses_env)) > 0) {
    cat(yellow("\nuses_env values are missing in MNG_H_INGO"))
  } else {
    cat(green("\nNo missing `uses_env` values in the MNG_H_INGO. Proceeding..."))
  }

  # setdiff with minimum_vars
  missing_vars <- setdiff(minimum_vars, colnames(dt)) %>% print()

  # if missing vars is empty print happy message that formatting is complete
  if (length(missing_vars) == 0) {
    cat(green("\nNo missing variables! Checking for missing values. Given that I indicated underlying_nid, should not have any..."))
  } else {
    cat(red("\nMissing variables: ", missing_vars))
  }

  missing_values <- dt[, lapply(.SD, function(x) sum(is.na(x))),
    .SDcols = vars_no_missing_values
  ] %>% print()

  if (nrow(missing_values) > 1) {
    cat(red("\nThere are missing values in the final dt in column(s) other than `underlying_nid`. Please double-check the input."))
  } else {
    formatted_mng_h_info <- copy(dt)
    file_path <- paste0(run_dir, "FILEPATH")
    cat(paste0("Saved `formatted_pol_nhf.csv into ", file_path, " for further processing with the other sources.\n"))
    fwrite(formatted_mng_h_info, paste0(file_path))
  }

  return(formatted_mng_h_info)
}
