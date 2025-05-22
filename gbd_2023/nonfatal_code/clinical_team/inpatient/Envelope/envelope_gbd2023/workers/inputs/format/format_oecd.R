# Title: Format OECD
# Purpose: Prepare OECD aggregations for deduplication and bundle upload
# Input: FILEPATH
# Output: formatted OECD
# Author: USERNAME

# initial formatting
select_filter <- function(dt) {
  # filter to inpatient
  dt <- dt %>% filter(Variable == "Inpatient care discharges (all hospitals)")
  # select required columns
  dt <- dt %>% select(Measure, Country, Year, Value)
  setnames(dt, c("Country", "Year", "Value"), c("location_name", "year_id", "cases"))
  # filter to counts (not per 100 000 population)
  dt <- dt %>% filter(Measure == "Number")
  dt[, Measure := NULL]
  
  cat(green("Initial formatting done!"))
  
  return(dt)
  
}
# pull location IDs
pull_location_ids <- function(dt) {
  # Unique country names from both data tables
  unique_countries_dt <- unique(dt$location_name)
  unique_countries_metadata <- unique(location_metadata$location_name)
  # Finding mismatches
  mismatched_countries <- setdiff(unique_countries_dt, unique_countries_metadata)
  # Displaying mismatched country names
  cat(yellow("\nAs of 2024-07-16, the following country names are mismatched in OECD and location metadata:", mismatched_countries, " - assigning new locaiton_name to those per GBD nomenclature.\n"))
  # Example manual mapping
  renaming_map <- data.table(
    wrong_name = c("China (People's Republic of)", "Czech Republic", "Korea", "Russia", "Slovak Republic"),
    correct_name = c("China", "Czechia", "Republic of Korea", "Russian Federation", "Slovakia")
  )
  # Rename based on the mapping
  for(i in 1:nrow(renaming_map)) {
    dt[location_name == renaming_map$wrong_name[i], location_name := renaming_map$correct_name[i]]
  }
  # Now, merge dt with location_metadata on location_name to get location_id
  dt <- merge(dt, location_metadata[level == 3, .(location_id, location_name)], by = "location_name", all.x = TRUE)
  
  # check if location_id has missing and cat statement in red if yes and happy statement in green if no
  if (any(is.na(dt$location_id))) {
    cat(red("Location ID is missing in the dt. Please double-check the input."))
  } else {
    cat(green("Location ID is filled!"))
  }
  
  return(dt)
}
# format sex and age groups
format_sex_ages <- function(dt) {
  dt[,`:=`(sex_id = 3, age_group_id = 22, age_start = 0, age_end = 125)]
  cat(green("\nAssigned all-age-both-sex to all rows."))
  
  return(dt)
}
# pull sample size from populations
pull_sample_size <- function(dt) {
  
  cat(yellow("\nRetrieving sample_size from population..."))
  cat(yellow("\nImporting population metadata..."))
  
  # Assuming get_population is a function defined elsewhere that fetches population data based on the provided parameters
  populations <- get_population(release_id = release_id, location_id = unique(dt$location_id), year_id = unique(dt$year_id), 
                                age_group_id = unique(dt$age_group_id), sex_id = unique(dt$sex_id))
  populations <- populations %>% select(all_of(asly_vars), population)
  dt <- merge(dt, populations, by = asly_vars, all.x = TRUE)
  setnames(dt, "population", "sample_size")
  
  # remove age_group_id - needed it to get populations
  dt[,age_group_id := NULL]
  
  # statement
  if (any(is.na(dt$sample_size))) {
    cat(red("\nMissing sample_size values for ", paste0(unique(dt[is.na(sample_size) == TRUE, .(location_id, year_id, age_group_id, sex_id)]), collapse = ", "), "\n"))
  } else {
    cat(green("\nAll sample_size values are filled!\n"))
  }
  return(dt)
}
# pull nid, data_type_name, and title from ghdx_db_output that is already in the environment
pull_ghdx_metadata <- function(dt) {
  cat(yellow("\nRetrieving NID, data_type_name, and title from ghdx_metadata..."))
  if (!exists("ghdx_metadata")) {
    stop("ghdx_metadata does not exist in the environment. Please load it first.")
  } else {
    cat(yellow("Searching GHDx DB by known title 'OECD.Stat Healthcare Utilization' to get NID, title, and data_type_name..."))
    ghdx_row <- unique(ghdx_metadata[grepl("OECD.Stat Healthcare Utilization", field_citation_value), ])
  }
  # bind that row with all existing rows in dt
  if (nrow(ghdx_row) == 0) {
    stop("No NID found for the given title. Please double-check the title.")
  } else {
    # cbind dt and ghdx_row
    dt <- cbind(dt, ghdx_row) # this will fill all the rows automatically
  }
  
  if (any(is.na(dt$nid))) {
    stop("NID is missing in the dt. Please double-check the input.")
  } else {
    cat(green("\nNID, data_type_name, and title are filled!\n"))
  }
  return(dt)
}
# complete function
format_oecd <- function(path) {
  
  dt <- fread(path)
  
  dt <- dt %>% 
    select_filter() %>% 
    pull_location_ids() %>% 
    format_sex_ages() %>% 
    pull_sample_size() %>% 
    pull_ghdx_metadata()
  
  # function to manually fill out is_clinical, recall_type, recall_type_value ----
  cat("\nFilling in is_clinical = 0, recall_type = 'Period: months', recall_type_value = 12 (annual recall), uses_env = 0, measure = 'continuous as those are standard values")
  dt[, `:=`(is_clinical = 0, recall_type = "Period: months", recall_type_value = 12, measure = "continuous", uses_env = 0)]
  
  # fill in underlying_nid 548992 manually
  # cat(red("\nNote: I cannot pull underlying_nid from the GHDx database yet as of 2024-07-16. I will impute 536419 manually, because the specific file I am using has that value listed in the GHDx. Ask honj / follow up on the Helpdesk ticket to update the GHDx DB ouput to avoid this step. Refer to <https://internal-ghdx.healthdata.org/record/european-health-all-database-number-all-hospital-discharges> Underlying NIDs are important for publications."))
  # dt[, `:=`(underlying_nid = 536419)]
  dt[, `:=`(underlying_nid = NA)]
  
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
  
  # final check for missing values in every column except underlying_nid, shouldn't have any
  missing_values <- dt[, lapply(.SD, function(x) sum(is.na(x))), 
                       .SDcols = vars_no_missing_values] %>% print()
  if (nrow(missing_values) > 1) {
    cat(red("\nThere are missing values in the final dt in column(s) other than `underlying_nid`. Please double-check the input."))
  } else {
    formatted_oecd <- copy(dt)
    file_path <- paste0(run_dir, "FILEPATH")
    cat(paste0("Saved `formatted_oecd.csv run into ", file_path, " for further processing with the other sources.\n"))
    fwrite(formatted_oecd, paste0(file_path))
  }
  
  return(formatted_oecd)
    
}
