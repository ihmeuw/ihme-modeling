# Title: Format POL_NHF data aggregated by loc-yr-age-sex-los (length of stay) from a CSV (made by USERNAME)
# Purpose: Format data for further processing and collation with other envelope sources.
# Input: aggregated POL_NHF data from a CSV (made by USERNAME)
# Output: POL claims
# Author: USERNAME

# initial formatting
format_data <- function(dt){
  dt <- dt[location_id != 51, ]  # Exclude national level data because if it's 51 it was missing and then filled (per Taylor's Slack msg 1/25/24)
  dt <- dt %>% select(nid, location_id, year_id, age_group_id, age_start, age_end, sex_id, cases) # Select cols for the bundle
  dt <- distinct(dt) # Remove duplicate rows
  dt[, cases := sum(cases), by = c("location_id", "year_id", "age_group_id", "sex_id")]
  dt <- distinct(dt)
  return(dt)
}

# pull sample sizes from populations
pull_sample_size <- function(df) {
  populations <- get_population(release_id = release_id, location_id = unique(df$location_id), year_id = unique(df$year_id), 
                                age_group_id = unique(df$age_group_id), sex_id = unique(df$sex_id))
  populations <- populations %>% select(all_of(asly_vars), population)
  df <- merge(df, populations, by = asly_vars, all.x = TRUE)
  setnames(df, "population", "sample_size")
  return(df)
}

# pull NIDs, data types names and field citation values
pull_ghdx_metadata <- function(dt) {
  cat(yellow("\nRetrieving NID, data_type_name, and title from ghdx_metadata..."))
  if (!exists("ghdx_metadata")) {
    stop("ghdx_metadata does not exist in the environment. Please load it first.")
  } else {
    cat(yellow("Searching GHDx DB by known title 'European Health for All Database - Number of all Hospital Discharges' to get NID, title, and data_type_name..."))
    dt <- merge(dt, ghdx_metadata, by = "nid", all.x = TRUE)
  }
  
  return(dt)
}


# The population of the <1 disaggregated age groups is not the total population of neonates in a given year
# Therefore, if you simply recalculate val from there, it will be overestimated, because the population denominator is small
# Therefore, need to rescale the cases down by multiplying them by the age span (age_end - age_start)
# pass through the scaling function
recalculate_cases <- function(dt) {
  dt_scaled <- copy(dt)
  dt_scaled[, age_span := age_end - age_start] # calculate the age span
  dt_scaled[age_span < 1, cases := cases * age_span] # only for cases where the age span is less than 1
  dt_scaled[, age_span := NULL] # remove the temporary column
  return(dt_scaled)
}


# complete function
format_pol_nhf <- function(path){
  
  dt <- fread(path)
  
  dt <- dt %>% 
    format_data() %>% 
    pull_sample_size()
  
  missing_cols <- setdiff(minimum_vars, names(dt))
  
  # remove columns that are present in the dt but not among minimum_vars
  dt <- dt[, !setdiff(names(dt), minimum_vars), with = F]
  
  # if missing_columns are present, print a statement that these columns will be filled, otherwise say proceeding
  if (length(missing_cols) > 0) {
    cat("\nChecking for missing columns after initial import...")
    cat(yellow(paste0("\nThe following columns are missing from POL_NHF and will be filled: ", paste0(missing_cols, collapse = ", "))))
  } else {
    cat("\nAll columns are present in POL_NHF. Proceeding...")
  }
  
  dt <- dt %>% 
    pull_ghdx_metadata()
  
  # if no missing values present in the data, print that, otherwise throw a message that there are missing values and in which columns
  if (sum(is.na(dt)) == 0) {
    cat(green("\nNo missing values present in POL_NHF after merge on GHDx output"))
  } else {
    cat(yellow(paste0("\nMissing values present in the following columns: ", names(dt)[colSums(is.na(dt)) > 0])))
  }
  
  
  # manually fill out underlying_nid, is_clinical, recall_type, recall_type_value
  cat("\nFilling in underlying_nid = NA, is_clinical = 1, recall_type = 'Period: months', recall_type_value = 12 (annual recall) as those are standard values")
  dt[, `:=`(underlying_nid = NA, is_clinical = 1, recall_type = "Period: months", recall_type_value = 12)]
  
  # manually fill uses_env = 0 for POL_NHF
  cat("\nFilling in uses_env = 0 for POL_NHF")
  dt[, uses_env := 0]
  
  # manually fill measure = "continuous" for POL_NHF
  cat("\nFilling in measure = 'continuous' for POL_NHF")
  dt[, measure := "continuous"]
  
  # check if there are still any missing columns that are in minimum_vars
  missing_cols <- setdiff(minimum_vars, names(dt))
  if (length(missing_cols) > 0) {
    cat(yellow(paste0("\nThere are still missing columns in POL_NHF: ", paste0(missing_cols, collapse = ", "))))
  } else {
    cat(green("\nAll columns are present in POL_NHF"))
  }
  
  dt <- dt %>% 
    recalculate_cases()
  
  # if there are rows with age_start < 1 and age_end <=1, apply recalculate_cases function
  if (any(dt$age_start < 1 & dt$age_end <= 1)) {
    cat(yellow("\nAttention! Cases for <1 year disaggregated age groups were scaled down by the age span, because the population denominator represents the average number of babies in a given age span (a week for early neonatal group for instance). The step is also implemented further in the inpatient pipeline to calculate bundle-specific prevalence and incidence estimates."))
    dt <- recalculate_cases(dt)
  } else {
    cat(green())
  }
  
  # print the count of missing values by column in dt and print how many are missing
  cat(yellow(paste0("\nMissing values present in the following columns: ", names(dt)[colSums(is.na(dt)) > 0])))
  cat(yellow(paste0("\nTotal missing values: ", sum(is.na(dt)))))
  
  # if missing values are found in underlying_nid column, find out if all rows have missing and then it's okay
  if(sum(is.na(dt$underlying_nid)) == nrow(dt)) {
    cat(green("\nAll values in underlying_nid are missing. These were not pulled from the GHDx database, likely needs reprocessing, take note. Proceeding for now...\n"))
  } else {
    cat(red("\nSome, but not all values in underlying_nid are missing. Please check the data.\n"))
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
                       .SDcols = vars_no_missing_values] %>% print()
  if (nrow(missing_values) > 1) {
    cat(red("\nThere are missing values in the final dt in column(s) other than `underlying_nid`. Please double-check the input."))
  } else {
    formatted_pol_nhf <- copy(dt)
    file_path <- paste0(run_dir, "FILEPATH")
    cat(paste0("Saved `formatted_pol_nhf.csv into ", file_path, " for further processing with the other sources.\n"))
    fwrite(formatted_pol_nhf, paste0(file_path))
  }
  
  return(formatted_pol_nhf)
  
}
