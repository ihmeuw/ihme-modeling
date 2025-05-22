# Title: Format Singapore aggregated inpatient data
# Purpose: Format data for further processing and collation with other envelope sources
# Input: aggregated Singapore aggregated inpatient data
# Output: `formatted+SGP_claims.csv` in the `output_data_dir` and global environment
# Author: USERNAME

# initial formatting
format_dt <- function(dt) {
  
  dt <- as.data.table(dt)
  # sex_id
  dt[, sex_id := as.integer(ifelse(gender == "M", 1, 2))]
  # ensure no missing values in sex_id, print respective statement
  if (any(is.na(dt$sex_id))) {
    cat(red("Missing sex_id values\n"))
  } else {
    cat(green("sex_id values are filled!\n"))
    dt[, gender := NULL]
  }
  
  # renaming
  setnames(dt, old = c("discharge_year", "countnum"), new = c("year_id", "cases"))
  
}
# get location ID
pull_location_ids <- function(dt) {
  
  loc_id <- location_metadata[location_name == "Singapore", .(location_id)] %>% pull()
  
  # append loc_id to all rows as location_id
  dt[, location_id := loc_id]
  cat(green("Assigned location ID", loc_id, "to all rows.\n"))
  
  return(dt)
}
# function to get age_start and age_end from age column
pull_age_start_end <- function(dt) {
  
  cat(yellow("Getting age_start, age_end, and age_group_id from $agegrp...\n"))
  
  # write regex to split "45-49" to 45 and 49
  dt[, c("age_start", "age_end") := tstrsplit(agegrp, "-", fixed = TRUE)]
  
  # ">=100" special case
  dt[agegrp == ">=100", `:=` (age_start = 100, age_end = 125)]
  
  # make age_start and age_end numeric
  dt[, c("age_start", "age_end") := lapply(.SD, as.numeric), .SDcols = c("age_start", "age_end")]
  
  # add 1 to every age end %% 5 == 4
  dt[age_end %% 5 == 4, age_end := as.numeric(age_end) + 1]
  
  # sum up cases for 95 plus (95-99, >=100) age_start >=95
  dt[age_start >= 95, `:=`(age_start = 95, age_end = 125)]
  dt[age_start == 95, cases := sum(cases), by = c("location_id", "year_id", "sex_id")]
  
  # remove the original column
  dt[, agegrp := NULL]
  dt <- distinct(dt)
  
  cat("Processed age groups into 'age_start' and 'age_end'. Special case of 0 to 4 handled.\n")
  
  return(dt)
}
# function to get sample_size from populations
pull_sample_size <- function(dt) {
  cat(yellow("\nFetching sample_size from populations..."))
  
  cat(yellow("Need to get populations for regular age groups and then sum for nonstandard ones.\n",
             "For example, for 0 to 4 years, I would sum populations per loc-year-sex for <1 year, 12 to 23 months and 2 to 4 years\n",
             "For <1 year, I would sum populations for all the 4 disaggregated neonates\n"))
  
  cat(yellow("\nImporting population metadata...\n"))
  
  populations <- get_population(release_id = release_id, location_id = unique(dt$location_id),
                                year_id = unique(dt$year_id), sex_id = unique(dt$sex_id),
                                age_group_id = c(unique(age_metadata$age_group_id)))
  
  setDT(populations) # Ensure populations is a data.table
  
  asly_vars <- c("location_id", "year_id", "age_group_id", "sex_id") # demographics for merging
  
  populations <- populations %>% select(all_of(asly_vars), population)
  
  # merge on age metadata to get age_start and age_end
  populations <- merge(populations, age_metadata[, .(age_group_id, age_start, age_end)], by = "age_group_id", all.x = T, allow.cartesian = T)
  
  # handle special populations
  
  # sum up 0-1, 1-2, 2-5 for 0 to 5
  populations[age_group_id %in% c(2, 3, 388, 389, 238, 34), 
              `:=`(population = sum(population), age_start = 0, age_end = 5), by = .(location_id, year_id, sex_id)]
  populations[, age_group_id := NULL]
  populations <- distinct(populations)
  
  # merge on dt
  dt[, `:=`(age_start = as.integer(age_start), age_end = as.numeric(age_end))]
  dt <- merge(dt, populations, by = c("location_id", "year_id", "sex_id", "age_start", "age_end"), all.x = TRUE, allow.cartesian = TRUE)
  
  # rename to sample_size
  setnames(dt, "population", "sample_size")
  
  # happy message if no missing in populations in dt1 otherwise warn
  if (any(is.na(dt))) {
    cat(red("\nMissing population values for ", paste0(unique(dt[is.na(population) == TRUE, .(location_id, year_id)]))))
  } else {
    cat(green("\nAll population values are filled!"))
  }
  
  return(dt)
}
# function to get year_id from title and convert to long format
pull_ghdx_metadata <- function(dt) {
  # Ensure 'dt' is a data.table
  setDT(dt)
  
  cat(yellow("\nRetrieving NID, data_type_name, and title from ghdx_metadata...\n"))
  if (!exists("ghdx_metadata")) {
    stop("\nghdx_metadata does not exist in the environment. Please load it first.\n")
  } else {
    cat(yellow("\nSearching GHDx DB by known title 'Singapore MediClaims Database - Resident Population Inpatient Hospitalization' to get NID, title, and data_type_name...\n"))
    ghdx_subset <- unique(ghdx_metadata[grepl("Singapore MediClaims Database - Resident Population Inpatient Hospitalization", field_citation_value), ])
    # bind that row with all existing rows in dt
    if (nrow(ghdx_subset) == 0) {
      stop("\nNo NID found for the given title. Please double-check the title.")
    }
  } 
  
  # Extract years and create a list column where each entry is a vector of the years that need to be expanded
  cat(yellow("Extracting years from titles...\n"))
  cat(yellow("This is necessary because 468873 spans 1999-2019, so to merge properly, we are transforming the GHDx output to copy the row for each year.\n"))
  ghdx_subset[, year_id := str_extract_all(field_citation_value, "\\d{4}")][
    , year_id := lapply(year_id, function(yrs) {
      if (length(yrs) == 2) {
        seq(from = as.numeric(yrs[1]), to = as.numeric(yrs[2]))
      } else {
        as.numeric(yrs)
      }
    })]
  
  # convert from list to nuimeric
  ghdx_subset$year_id <- as.numeric(ghdx_subset$year_id)
  
  # Check that year_id is not NA
  cat(yellow("\nChecking for missing year_id..."))
  if (any(is.na(dt$year_id))) {
    stop("Some year_id values are missing. Please check the 'expanded_years' column.")
  } else {
    cat(green("\nAll year_id values are present!\n"))
  }
  
  # Merge dt_long with dt in the argument of the function by year_id
  cat(yellow("\nMerging the GHDx metadata with the original dataset...\n"))
  dt <- merge(dt, ghdx_subset, by = "year_id", all.x = TRUE, allow.cartesian = TRUE)
  
  # Check resulting dt for missing
  cat(yellow("\nChecking for missing values in the merged dataset...\n"))
  if (any(is.na(dt$nid))) {
    stop("Some NID values are missing. Please check the 'nid' column.")
  } else {
    cat(green("\nAll `nid`, `data_type_name`, and `title` values are present!\n"))
  }
  
  return(dt)
}
# manually add the rest of the columns
add_remaining_columns <- function(dt) {
  missing_cols <- setdiff(minimum_vars, names(dt))
  cat(yellow("\nAdding the remaining columns to the dataset:", paste0(missing_cols, collapse = ", "), "...\n"))
  dt <- dt[, `:=`(uses_env = 0,
                  is_clinical = 0,
                  measure = "continuous",
                  recall_type = "Period: months",
                  recall_type_value = 12)]
  missing_cols <- setdiff(minimum_vars, names(dt))
  # Now if missing columns is empty say that no missing columns are present
  if (length(missing_cols) == 0) {
    cat(green("\nAll remaining columns added!\n"))
  } else {
    cat(red("\nSome columns are still missing. Please check the following columns: ", paste0(missing_cols, collapse = ", "), "\n"))
  }
  
  return(dt)
}

# complete function
format_sgp_claims <- function(path, password, sheet){
  
  dt <- xlsx::read.xlsx(file = path, password = password, sheetName = sheet)
 
  dt <- dt %>% 
    format_dt() %>% 
    pull_location_ids() %>% 
    pull_age_start_end() %>% 
    pull_sample_size() %>% 
    pull_ghdx_metadata() %>% 
    add_remaining_columns()
  
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
    formatted_sgp_claims <- copy(dt)
    file_path <- paste0(run_dir, "FILEPATH")
    cat(paste0("Saved `formatted_sgp_claims.csv into ", file_path, " for further processing with the other sources.\n"))
    fwrite(formatted_sgp_claims, paste0(file_path))
  }
  
  return(formatted_sgp_claims)
    
}
