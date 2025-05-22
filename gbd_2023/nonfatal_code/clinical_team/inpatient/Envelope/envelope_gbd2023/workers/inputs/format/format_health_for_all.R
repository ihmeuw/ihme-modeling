# Title: Format WHO Europe Health for All database
# Purpose: Format data for further processing and collation with other envelope sources
# Input: HfA long format
# Output: `formatted_HfA` in the `output_data_dir` and global environment
# Author: USERNAME

# initial formatting
format_columns <- function(dt) {
  dt <- as.data.table(dt)
  cat("\nFormatting Health for All data...\n")
  cat("\nRaw data contains columns: ", paste0(names(dt), collapse = ", "), "\n")
  dt[, "Measure code" := NULL]
  setnames(dt, old = c("COUNTRY_REGION", "SEX", "YEAR", "VALUE"),
           new = c("ihme_loc_id", "sex", "year_id", "cases")) # ihme_loc_id is the ISO in location_metadata
  cat("New columns: ", paste0(names(dt), collapse = ", "), "; Rows: ", nrow(dt))
  return(dt)
}
# pull location IDs
pull_location_ids <- function(dt) {
  library(crayon) # Ensure you have the crayon library loaded
  
  cat(yellow("\nFetching location_id from location_metadata through ihme_loc_id (3-character ISO code)..."))
  cat(yellow("\nFiltering dt to rows where ihme_loc_id is not empty (nchar>0) and not NA..."))
  dt <- dt %>% filter(nchar(ihme_loc_id) > 0 & !is.na(ihme_loc_id))
  
  cat(yellow("\nUnique countries in the dt (check if they look right, should be universal encoding): ", paste0(unique(dt$ihme_loc_id), collapse = ", "), "\n"))
  cat(green("\nIf you see one of these CARINFONET, CIS, EU_AFTER_MAY2004, EU_BEFORE_FEB2020, EU_BEFORE_MAY2004, EU_MEMBERS, NORDIC, SEEHN, SMALL, SMR, WESTERN_BALKANS, etc, then just proceed! \n"))
  
  dt <- merge(dt, location_metadata[level == 3, .(ihme_loc_id, location_id)], # level == 3 means national, this is important! will fill all the subnationals as well otherwise...
              by = "ihme_loc_id", all.x = TRUE, nomatch = NULL, allow.cartesian = TRUE)
  
  if (any(is.na(dt$location_id))) {
    missing_ihme_loc_ids <- unique(dt[is.na(location_id) == TRUE, .(ihme_loc_id)]$ihme_loc_id)
    cat(red("\nThere are missing location_ids for these ihme_loc_ids in dt:", paste0(missing_ihme_loc_ids, collapse = ", ")))
    if (all(missing_ihme_loc_ids %in% c("CARINFONET", "CIS", "EU_AFTER_MAY2004", "EU_BEFORE_FEB2020", "EU_BEFORE_MAY2004", "EU_MEMBERS", "NORDIC", "SEEHN", "SMALL", "SMR", "WESTERN_BALKANS"))) {
      cat(green("\n... but these are San Marino, and different European regions, which are not modeled in the GBD, so removing those and proceeding...\n"))
      # Remove rows where ihme_loc_id is MCO or SMR
      dt <- dt[!ihme_loc_id %in% c("CARINFONET", "CIS", "EU_AFTER_MAY2004", "EU_BEFORE_FEB2020", "EU_BEFORE_MAY2004", "EU_MEMBERS", "NORDIC", "SEEHN", "SMALL", "SMR", "WESTERN_BALKANS")]
      cat(green("\nAll location_id values are filled!\n"))
    } else {
      stop("\nThere are missing location_id values that are not MCO or SMR, and they are missing in location_metadata. Double-check the input.")
    }
  }
  
  return(dt)
  
}
# pull age/sex metadata
pull_age_sex_metadata <- function(dt) {
  # get sex_id
  cat(yellow("\nFormatting sex and age metadata to get sex_id, age_start, age_end"))
  sex_ids <- get_ids("sex")
  cat(yellow("\nUnique sex values in dt: ", paste0(unique(dt$sex), collapse = ", ")))
  dt[sex == "ALL", sex := "Both"]
  dt <- merge(dt, sex_ids, by = "sex", all.x = T)
  # warn if there are missing sex_id values in dt now
  if (any(is.na(dt$sex_id))) {
    cat(red("\nMissing sex_id values for ", paste0(unique(dt[is.na(sex_id) == TRUE, .(sex_id)]))))
  } else {
    cat(green("\nAll sex_id values are filled!"))
  }
  # get age+start and age_end
  cat(yellow("\nGetting age_start and age_end; as of", date(), ", the Health for All database reports all-age-both sex counts only and there is no age column, so hard coding age_start := 0 and age_end := 125 and age_group_id = 22 (need to get sample_size)."))
  dt[, `:=`(age_group_id = 22, age_start = 0, age_end = 125)]
  cat(green("\nAll age_start and age_end values are filled!"))
  return(dt)
}
# pull sample sizes from populations
pull_sample_size <- function(dt) {
  
  cat(yellow("\nRetrieving sample_size from population..."))
  cat(yellow("\nImporting population metadata..."))
  
  dt <- as.data.frame(dt)
  # Assuming get_population is a function defined elsewhere that fetches population data based on the provided parameters
  populations <- get_population(release_id = release_id, location_id = unique(dt$location_id), year_id = unique(dt$year_id), 
                                age_group_id = unique(dt$age_group_id), sex_id = unique(dt$sex_id))
  populations <- populations %>% select(all_of(asly_vars), population)
  dt <- merge(dt, populations, by = asly_vars, all.x = TRUE)
  setnames(dt, "population", "sample_size")
  
  if (any(is.na(dt$sample_size))) {
    cat(red("\nMissing sample_size values for ",
            paste0(dt[is.na(sample_size) == TRUE, unique(location_id)], collapse = ", "), "\n"))
  } else {
    cat(green("\nAll sample_size values are filled!\n"))
  }
  
  # Identifying rows with missing sample_size
  missing_sample_size <- dt[is.na(sample_size), .(location_id)]
  unique_missing_locations <- unique(missing_sample_size$location_id)

  if (length(unique_missing_locations) > 0) {
    cat(red("\nMissing sample_size values for ",
            paste(unique_missing_locations, collapse = ", "), "\n"))
  } else {
    cat(green("\nAll sample_size values are filled!\n"))
  }
  
  return(dt)
}
# pull NID, data type names and field citation value
pull_ghdx_metadata <- function(dt) {
  cat(yellow("\nRetrieving NID, data_type_name, and title from ghdx_metadata..."))
  if (!exists("ghdx_metadata")) {
    stop("ghdx_metadata does not exist in the environment. Please load it first.")
  } else {
    cat(yellow("Searching GHDx DB by known title 'European Health for All Database - Number of all Hospital Discharges' to get NID, title, and data_type_name..."))
    ghdx_row <- unique(ghdx_metadata[grepl("European Health for All Database - Number of all Hospital Discharges", field_citation_value),])
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
    cat(green("\nNID, data_type_name, field_citation_value, underlying_nid, underlying_field_citation_value, series_nid, series_field_citation_value are filled!\n"))
  }
  return(dt)
}


# complete function
format_health_for_all <- function(path, sheet) {
  
  dt <- read_excel(path, sheet)
  
  dt <- format_columns(dt)
  dt <- pull_location_ids(dt)
  dt <- pull_age_sex_metadata(dt)
  dt <- pull_sample_size(dt)
  dt <- pull_ghdx_metadata(dt)
  
  # function to manually fill out is_clinical, recall_type, recall_type_value
  cat("\nFilling in is_clinical = 0, recall_type = 'Period: months', recall_type_value = 12 (annual recall), uses_env = 0, measure = 'continuous as those are standard values")
  dt <- dt %>% 
    mutate(
      is_clinical = 0,
      recall_type = "Period: months",
      recall_type_value = 12,
      measure = "continuous",
      uses_env = 0
    )
  
  # setdiff with minimum_vars
  missing_vars <- setdiff(minimum_vars, colnames(dt)) %>% print()
  
  # if missing vars is empty print happy message that formatting is complete
  if (length(missing_vars) == 0) {
    cat(green("\nNo missing variables! Checking for missing values. Given that I indicated underlying_nid, should not have any..."))
  } else {
    cat(red("\nMissing variables: ", missing_vars))
  }
  
  dt <- as.data.table(dt)
  missing_values <- dt[, lapply(.SD, function(x) sum(is.na(x))), 
                       .SDcols = vars_no_missing_values] %>% print()
  if (nrow(missing_values) > 1) {
    cat(red("\nThere are missing values in the final dt in column(s) other than `underlying_nid`. Please double-check the input."))
  } else {
    formatted_health_for_all <- copy(dt)
    file_path <- paste0(run_dir, "FILEPATH")
    cat(paste0("Saved `formatted_health_for_all.csv run into ", file_path, " for further processing with the other sources.\n"))
    fwrite(formatted_health_for_all, paste0(file_path))
  }
   
  return(formatted_health_for_all)
    
}
