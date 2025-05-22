# Title: Format reports from the extraction spreadsheet
# Purpose: Prepare the extracted government reports for deduplication and bundle upload
# Input: 03_admin_data_extraction_template.xlsx sheet = "extraction_upd_long"
# Downloaded manually to my home directory and renamed to Sys.Date()
# Output: formatted reports from the extraction spreadsheet, ready for bundle pre-processing
# Author: USERNAME


# initial formatting
filter_select <- function(dt, cols_to_keep) {
  int_cols <- c("nid", "year_id", "age_group_id", "sex_id", "cases", "level")

  # Ensure dt is a data.table
  dt <- as.data.table(dt)

  # Assign integer type to integer columns if NA introduced by coercion is OK
  for (col in int_cols) {
    if (col %in% cols_to_keep) {
      dt[, (col) := fifelse(is.na(get(col)), 0, as.integer(get(col))), with = FALSE]
      cat(yellow(paste0("Coercing ", col, " to integer type\n")))
    }
  }

  # Use .SDcols to specify columns to keep, then drop unneeded columns
  dt <- dt[, .SD, .SDcols = cols_to_keep]

  # if cases = NA, remove these rows
  if (nrow(dt[is.na(cases), ]) > 0) {
    cat(yellow("Removing rows with missing `cases` values: ", nrow(dt[is.na(cases), ]), "\n"))
    dt <- dt[!is.na(cases), ]
  }

  # Filter out admission_type != "inpatient"
  if (nrow(dt[admission_type != "inpatient", ]) > 0) {
    cat(yellow("Filtering out rows with `admission_type` != 'inpatient': ", nrow(dt[admission_type != "inpatient", ]), "\n"))
    dt <- dt[admission_type == "inpatient", ]
  }

  # If measure is missing, assume "continuous"
  if (nrow(dt[is.na(measure), ]) > 0) {
    cat(yellow("Assuming `measure` == 'continuous' for missing values: ", nrow(dt[is.na(measure), ]), "\n"))
    dt[is.na(measure), measure := "continuous"]
  }

  # Check for any missing data otherwise
  if (nrow(dt[rowSums(is.na(dt)) > 0, ]) > 0) {
    cat(red("There are still rows with missing data: ", nrow(dt[rowSums(is.na(dt)) > 0, ]), "\n"))
  } else {
    cat(green("All data is present and accounted for!\n"))
  }

  return(dt)
}
# validate that sex_id is either 1 2 or 3 function
validate_sex_id <- function(dt) {
  dt$sex_id <- as.integer(dt$sex_id) # Ensure data type
  sex_id_valid <- dt$sex_id %in% c(1, 2, 3) # Create a temporary validity check specific to sex_id
  # dt$valid <- dt$valid & sex_id_valid # Combine with existing validity (if you want to preserve previous checks)
  if (any(!sex_id_valid)) {
    stop("Invalid `sex_id` values found. Please check the `sex_id` column.")
  } # Throw a warning if valid is FALSE is any rows and stop

  return(dt)
}
# process age group name to get age_start and age_end ----
process_age_groups <- function(dt, age_group_column_name) {
  cat(yellow("Parsing `age_group_name` column to retrieve `age_start` and `age_end` numeric values. \n"))
  cat(yellow("The function `process_age_groups()` includes parsing of the strings and validations of `age_start` and `age_end` valu\n"))
  cat(red("Please! Make sure that this is done right by looking at the resulting values. The age group names are not consistent or standard, so there might be some errors, although I tried to make the function as robust as possible.\n"))

  # Paste the entire parse_age_group_name function here
  parse_age_group_name <- function(age_group_name) {
    # Normalize the string
    normalized_age_group_name <- tolower(age_group_name)
    normalized_age_group_name <- gsub("years", "", normalized_age_group_name)
    normalized_age_group_name <- gsub("\\s+", " ", normalized_age_group_name) # Ensure single spaces
    normalized_age_group_name <- gsub("\\+", " plus", normalized_age_group_name) # Ensure "plus" is consistently captured

    # Explicitly normalize "70+ years", "70+", and "15+ years" for consistent handling
    normalized_age_group_name <- gsub("^15\\+\\s?years?$", "15 plus", normalized_age_group_name)
    normalized_age_group_name <- gsub("^70\\+\\s?years?$", "70 plus", normalized_age_group_name)

    # Handle explicit and special cases

    # Handling "plus" patterns explicitly for "70+ years", "70+", and "15+ years"
    # NB! It is important to put this chunk for these three age groups BEFORE the rest
    # Otherwise the condition is checked after a more general condition that catches any string with "plus" in it.
    # This means the specific checks for "70+ years" and "15+ years" are never reached for these cases, as the earlier, more general "plus" condition matches first and returns a result.
    # Also note I am using age_group_name, not normalized_age_group_name, as the input to the function.
    if (normalized_age_group_name == "70+ years" | normalized_age_group_name == "70+") {
      return(c(70, 125)) # "70+ years", "70+"
    } else if (normalized_age_group_name == "15+ years") {
      return(c(15, 125)) # "15+ years"
    }

    if (normalized_age_group_name == "all ages") {
      return(c(0, 125)) # "All Ages"
    } else if (normalized_age_group_name %in% c("age not specified", "unknown")) {
      return(NA) # "Unknown"
    } else if (grepl("neonatal", normalized_age_group_name)) {
      if (grepl("early", normalized_age_group_name)) {
        return(c(0, 6 / 365)) # Early Neonatal
      } else if (grepl("late", normalized_age_group_name)) {
        return(c(7 / 365, 27 / 365)) # Late Neonatal
      } else {
        return(c(0, 28 / 365)) # "Neonatal"
      }
    } else if (normalized_age_group_name == "post neonatal") {
      return(c(28 / 365, 1)) # "Post Neonatal"
    } else if (normalized_age_group_name == "less than 5") {
      return(c(0, 5)) # "Under 5"
    } else if (normalized_age_group_name %in% c("<=28days", "<= 28 days")) {
      return(c(0, 27 / 365)) # "<= 28 days"
    } else if (normalized_age_group_name %in% c("29 days - 1 year", "29 days-1 year")) {
      return(c(29 / 365, 1)) # "29 days - 1 year" aka Post Neonatal
    } else if (grepl("plus", normalized_age_group_name) | grepl(">=\\s?60", normalized_age_group_name)) {
      age_start <- as.integer(str_extract(normalized_age_group_name, "\\d+"))
      return(c(age_start, 125)) # "60+" - mind this! says 60 specifically, not very robust
    } else if (grepl("under", normalized_age_group_name) | grepl("<", normalized_age_group_name)) {
      age_end <- as.integer(str_extract(normalized_age_group_name, "\\d+"))
      return(c(0, age_end)) # "Under X"
    } else if (grepl("-", normalized_age_group_name) | grepl("to", normalized_age_group_name)) {
      ages <- as.integer(unlist(str_extract_all(normalized_age_group_name, "\\d+")))
      return(c(ages[1], ages[length(ages)])) # "X - Y"
    } else if (grepl("^\\d+$", normalized_age_group_name)) { # Match whole string as a digit
      age <- as.integer(normalized_age_group_name) # Single age
      return(c(age, age))
    }

    return(NA) # If no match found
  }

  # Apply parsing function to each age group name
  dt$age_range <- lapply(dt[[age_group_column_name]], parse_age_group_name)

  # Convert the list to two separate columns
  age_start_end <- do.call(rbind, dt$age_range)
  dt$age_start <- age_start_end[, 1]
  dt$age_end <- age_start_end[, 2]

  # Cleanup: Remove the age_range list column
  dt$age_range <- NULL

  # Validation
  dt$valid <- ifelse(is.na(dt$age_start) & !(dt[[age_group_column_name]] %in% c("unknown", "age not specified")), FALSE, TRUE)
  dt$valid <- dt$valid & (dt$age_start <= dt$age_end)
  dt$valid <- dt$valid & (dt$age_start >= 0 & dt$age_end <= 125)

  dt$valid <- NULL

  return(dt)
}
# Function to adjust age groups ----
adjust_age_groups <- function(dt) {
  # Sum neonatal and single-year age groups (0 to <1 and 1 to 4) into 0 to 1 and 1 to 5 age groups
  neonatal <- dt[age_start >= 0 & age_start != 1 & age_end <= 1, ]
  neonatal[, `:=`(age_group_name = "<1 year", age_start = 0, age_end = 1)]
  neonatal[, cases := sum(cases), by = .(nid, year_id, location_name, sex_id, age_start, age_end)]
  neonatal <- unique(neonatal)

  one_to_four <- dt[age_start >= 1 & age_end <= 5, ] # including single-year ones
  one_to_four[, `:=`(age_group_name = "1 to 4", age_start = 1, age_end = 5)] # increment by 5 (see the rest of the function)
  one_to_four[, cases := sum(cases), by = .(nid, year_id, location_name, sex_id, age_start, age_end)]
  one_to_four <- unique(one_to_four)

  # Maldives special case (90-107 and 108 plus)
  MDV_over_90 <- dt[location_name == "Maldives" & age_start >= 90, ]
  MDV_over_90[, `:=`(age_group_name = "90 plus", age_start = 90, age_end = 125)]
  MDV_over_90[, cases := sum(cases), by = .(nid, year_id, location_name, sex_id, age_start, age_end)]
  MDV_over_90 <- unique(MDV_over_90)

  # setorder(MDV_over_90, location_name, year_id, sex_id)
  # View(MDV_over_90)

  # combine the new dts
  dt_aggregated <- rbind(neonatal, one_to_four, MDV_over_90) %>% unique()
  setorder(dt_aggregated, location_name, year_id)

  # Remove the original neonatal and 1 to 4 rows and 90 plus Maldives rows based on the same conditions
  dt <- dt[!(age_start >= 0 & age_start != 1 & age_end <= 1) & !(age_start >= 1 & age_end <= 5) & !(location_name == "Maldives" & age_start >= 90)]

  # Add the aggregated neonatal and 1 to 4 rows back
  dt <- rbind(dt, neonatal, one_to_four, MDV_over_90, fill = T)

  # Directly handle the "1 to 19" case
  dt[age_start == 1 & age_end == 19, ":="(age_start = 1, age_end = 20)]

  # Directly handle the "1 to 14" case
  dt[age_start == 1 & age_end == 14, ":="(age_start = 1, age_end = 15)]

  # Directly handle the "1 to 19" case
  dt[age_start == 0 & age_end == 12, ":="(age_start = 0, age_end = 10)]

  # apply specific adjustment for "0 but not to be rounded" when age_end > 12
  dt[age_start == 0 & age_end > 12, ":="(age_start = 0, age_end = as.numeric(ceiling(age_end / 5) * 5))]

  # apply general rounding logic to age groups >= 5 and not covered by the specific cases above
  dt[age_start >= 5, ":="(
    age_start = as.numeric(floor(age_start / 5) * 5),
    age_end = as.numeric(ceiling(age_end / 5) * 5)
  )]

  # ensure age_end does not exceed 125
  dt[age_end > 125, age_end := 125]

  # ensure there are no overlaps for .(nid, year_id, location_name, sex_id)

  return(dt)
}
# Function to validate and find overlapping age groups
validate_overlaps <- function(dt) {
  # Adjust the calculation of overlap detection to exclude consecutive ranges
  dt[, `:=`(overlap_detected = shift(age_start, type = "lead", fill = Inf) < age_end),
    by = .(nid, year_id, location_name)
  ]

  # Filter to show only rows where an overlap is detected
  overlaps_dt <- dt[overlap_detected == TRUE, .(nid, year_id, location_name, sex_id, age_start, age_end)]

  # Check if any overlaps are detected
  if (nrow(overlaps_dt) == 0) {
    message("No overlaps detected.")
  } else {
    # If overlaps are detected, return the data.table containing those overlaps
    return(overlaps_dt)
  }
}
# Process Argentina - reports need to be summed up
process_ARG <- function(dt) {
  cat(yellow("\nProcessing Argentina data"))

  # sum up Argentina cases by nid, location_name, year_id, sex_id
  ARG_all <- dt %>% filter(location_name == "Argentina")
  ARG_all[, cases := sum(cases), by = .(nid, location_name, year_id)]
  ARG_all[, `:=`(age_group_name = "all ages", age_start = 0, age_end = 125, sex_id = 3)]

  ARG_all <- unique(ARG_all)
  ARG_all <- rbind(ARG_all, dt[location_name == "Buenos Aires" & age_start == 0 & age_end == 125, ], fill = TRUE)

  # now simply sum them up by year
  ARG_all[, `:=`(cases = sum(cases)), by = .(year_id)]
  ARG_all <- unique(ARG_all)

  # use NID values from the Argentina reports and take note that the Buenos Aires reports were used but they were summed with the naitonal ones to get full population coverage
  nid_year_dict <- ARG_all[location_name == "Argentina"]
  nid_year_dict <- nid_year_dict[, .(nid, year_id)] %>% unique()

  # merge the dict on arg_all
  ARG_all[, nid := NULL]
  ARG_all <- merge(ARG_all, nid_year_dict, by = "year_id", all.x = TRUE)

  # set location_name to "Argentina" and deduplicate
  ARG_all[, `:=`(location_name = "Argentina", level = 3)]
  ARG_all <- unique(ARG_all)

  # remove existing Argentina reports from the dt
  dt <- dt[!(location_name %in% c("Argentina", "Buenos Aires"))]

  # rbind
  dt <- rbind(dt, ARG_all, fill = TRUE)

  return(dt)
}
# get location_id
get_location_id <- function(dt) {
  cat(yellow("\nFetching location_id from location_metadata through `location_name` and `level`"))

  # merge on dt by location_name and level
  dt <- merge(dt, location_metadata[, .(level, location_name, location_id)], by = c("location_name", "level"), all.x = T)

  # check if any location_name in dt are missing
  if (any(is.na(dt$location_id))) {
    cat(red("\nMissing location_id values for ", paste0(unique(dt[is.na(location_id) == TRUE, .(location_name)]))))
  } else {
    cat(green("\nAll location_id values are filled!"))
  }

  # remove the location_id column
  dt <- dt[, c("location_name") := NULL]

  return(dt)
}
# append sample sizes
get_sample_size <- function(dt, release_id, age_metadata) {
  cat(yellow("\nRetrieving sample_size from population..."))

  cat(yellow("\nImporting population metadata for GBD age groups..."))

  # Assuming get_population is a function defined elsewhere that fetches population data based on the provided parameters
  populations <- get_population(
    release_id = release_id,
    location_id = unique(dt$location_id), year_id = unique(dt$year_id),
    age_group_id = unique(age_metadata$age_group_id), sex_id = unique(dt$sex_id)
  )

  populations <- populations %>% select(all_of(asly_vars), population)

  # get age_start and age_end from age_metadata
  populations <- merge(populations, age_metadata[, .(age_group_id, age_start, age_end)], by = "age_group_id", all.x = TRUE)

  # get a frame of asly combos to be filled
  asly_combos <- dt[, .(location_id, year_id, sex_id, age_start, age_end)] %>% unique()


  # Define the function to compute aggregated population
  compute_aggregated_population <- function(asly_combos, populations) {
    # Convert age_end in populations to be inclusive by adding a small epsilon where needed
    # populations[, age_end := ifelse(age_end == floor(age_end), age_end, floor(age_end) + 1)]

    # Merge the two tables based on location_id, year_id, and sex_id
    merged_data <- merge(asly_combos, populations, by = c("location_id", "year_id", "sex_id"))

    # Filter rows where the age groups overlap
    overlap <- merged_data[age_start.x <= age_end.y & age_end.x >= age_start.y, ]

    # Aggregate the population for the overlapping age groups
    result <- overlap[, .(total_population = sum(population)), by = .(location_id, year_id, sex_id, age_start = age_start.x, age_end = age_end.x)]

    # Return the result
    return(result)
  }

  # Example usage
  aggregated_population <- compute_aggregated_population(asly_combos, populations)

  # merge aggregated_population on dt to get the tmp_age_id
  dt_pop <- merge(dt, aggregated_population, by = c("location_id", "year_id", "age_start", "age_end", "sex_id"), all.x = TRUE)
  setnames(dt_pop, "total_population", "sample_size")

  if (any(is.na(dt_pop$sample_size))) {
    cat(red("\nMissing sample_size values for ", paste0(unique(dt_pop[is.na(sample_size) == TRUE, .(location_id, year_id, age_group_id, sex_id)]), collapse = ", "), "\n"))
  } else {
    cat(green("\nAll sample_size values are filled!\n"))
  }
  return(dt_pop)
}
# function get nid, data_type_name, and title from ghdx_db_output that is already in the environment
get_ghdx_metadata <- function(dt) {
  cat(yellow("\nRetrieving NID, data_type_name, and title from ghdx_db_output..."))
  if (!exists("ghdx_metadata")) {
    stop("ghdx_db_output does not exist in the environment. Please load it first.")
  } else {
    cat(yellow("Merging dt on GHDx DB output to get NID, title, and data_type_name..."))
    dt_ghdx <- merge(dt, ghdx_metadata, by = "nid", all.x = TRUE)
  }
  # bind that row with all existing rows in dt

  if (any(is.na(dt_ghdx[, .(data_type_name, field_citation_value)]))) {
    stop("data_type_name and/or title var(s) have missing values in the merged dt. Please double-check the input.")
  } else {
    cat(green("\ndata_type_name, and title are filled!\n"))
  }
  return(dt_ghdx)
}


col_names <- c("nid", "location_name", "level", "year_id", "age_group_name", "sex_id", "cases", "admission_type", "measure")
path <- "FILEPATH"
asly_vars <- c("location_id", "year_id", "age_group_id", "sex_id") # demographics for merging

format_extraction_spreadsheet <- function(path, col_names, release_id) {
  # Read in the extraction spreadsheet
  dt <- openxlsx::read.xlsx(path) %>% as.data.table()

  # Function to filter and select columns
  dt <- filter_select(dt, col_names)
  dt <- validate_sex_id(dt)
  dt <- process_age_groups(dt, "age_group_name")
  dt <- adjust_age_groups(dt)

  # Apply the validation function
  overlaps_found <- validate_overlaps(dt)

  # If overlaps are found, they will be contained in 'overlaps_found'
  # Display the results or handle accordingly
  if (!is.null(overlaps_found)) {
    print(overlaps_found)
  } else {
    print("No overlaps detected.")
  }

  # special case: sum up Argentina all-age-both-sex and Buenos Aires all-age-both-sex ----
  dt[, overlap_detected := NULL]

  dt <- process_ARG(dt)

  dt <- get_location_id(dt)

  dt <- get_sample_size(dt, release_id, age_metadata)

  dt <- get_ghdx_metadata(dt)

  # One-time fix for location inconsistency (Punjab in Pakistan)
  dt <- dt[!(nid == 537890 & location_id == 4867), ]

  # function to manually fill out is_clinical, recall_type, recall_type_value
  cat("\nFilling in is_clinical = 0, recall_type = 'Period: months', recall_type_value = 12 (annual recall), uses_env = 0")
  dt[, `:=`(is_clinical = 0, recall_type = "Period: months", recall_type_value = 12, uses_env = 0)]

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
    .SDcols = vars_no_missing_values
  ] %>% print()

  if (nrow(missing_values) > 1) {
    cat(red("\nThere are missing values in the final dt in column(s) other than `underlying_nid`. Please double-check the input."))
  } else {
    formatted_extraction_sheet <- copy(dt)
    file_path <- paste0(run_dir, "FILEPATH")
    cat(paste0("Saved `formatted_extraction_sheet run into ", file_path, " for further processing with the other sources.\n"))
    fwrite(formatted_extraction_sheet, paste0(file_path))
  }

  return(formatted_extraction_sheet)
}
