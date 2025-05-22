# Title: Format surveys extracted for the GBD 2021 envelope
# Purpose: Filter our the excluded surveys and format
# Input: old flate file from mobergm with 12-month recall admission-years surveys (12mo_avg) from June 3rd, 2020
# Refer to correspondence with USERNAME
# Output: formatted 12-month recall admission-year surveys
# Author: USERNAME

# Import the surveys
dt <- fread(path)

# Function to filter and select columns -------
format_surveys <- function(dt) {
  
  # select columns
  cols_to_keep <- c("nid", "location_name", "sex", "age_start", 'age_end', "year_start", "year_end", 
                    "cases", "sample_size", "measure", "true_recall", "recall_type_value", "location_id")
  dt <- dt %>% select(all_of(cols_to_keep))
  
  # show rows with missing values
  if (nrow(dt[rowSums(is.na(dt)) > 0, ]) > 0) {
    cat(red("There are rows with missing data: ", nrow(dt[rowSums(is.na(dt)) > 0, ]), "\n"))
    # print(dt[rowSums(is.na(dt)) > 0, ])
  } else {
    cat(green("All data is present and accounted for!\n"))
  }
  
  # remove cases == 0 or NA
  if (nrow(dt[cases == 0 | is.na(cases),]) > 0) {
    cat(yellow("Removing rows with `cases` == 0 or NA: ", nrow(dt[cases == 0 | is.na(cases),]), "\n"))
    dt <- dt[cases != 0 & !is.na(cases), ]
  } else {
    cat(green("All `cases` values are > 0\n"))
  }
  
  # make sure all measure values are "continuous"; if missing, assume "continuous"
  if (nrow(dt[measure != "continuous",]) > 0) {
    cat(yellow("Changing `measure` values to 'continuous': ", nrow(dt[measure != "continuous",]), "\n"))
    dt[measure != "continuous", measure := "continuous"]
  } else {
    cat(green("All `measure` values are 'continuous'\n"))
  }
  
  # print unique true_recall values and remove rows with true_recall != 52
  cat("Unique `true_recall` values (weeks): ", unique(dt$true_recall), "\n")
  if (nrow(dt[!true_recall %in% c(52),]) > 0) {
    cat(yellow("Removing rows with `true_recall` != 52 (not annual): ", nrow(dt[!true_recall %in% c(52),]), "\n"))
    dt <- dt[true_recall %in% c(52), ]
    cat(yellow("Explicitly changing 52 weeks to 12 months in `recall_type_value` and removing `true_recall`. Setting `recall_type` to 'Period: months'\n"))
    dt[, `:=`(recall_type_value = 12, true_recall = NULL)]
    dt[, recall_type := "Period: months"]
  } else {
    cat(green("All `true_recall` values are 52\n"))
  }
  
  # get year_id, which is the rounded mean of _end and _start, that is identical to _start
  if ("year_id" %in% names(dt)) {
    cat(yellow("The `year_id` column is present. \n"))
    if (any(is.na(dt$year_id))) {
      stop("There are missing values in the `year_id` column. Please check the `year_start` and `year_end` columns.")
    } else {
      cat(green("All `year_id` values are filled!\n"))
    }
    } else {
      if ("year_start" %in% names(dt) & "year_end" %in% names(dt)) {
        cat(yellow("The `year_id` column is missing but `year_start` and `year_end` are present.
                  Computing `year_id` by taking a rounded mean, which will be equivalent to `year_start`\n"))
        dt[, `:=`(year_id = round((year_end + year_start) / 2))]
        # if no missing year_id values, green message
        if (any(is.na(dt$year_id))) {
          stop("There are missing values in the `year_id` column. Please check the `year_start` and `year_end` columns.")
        } else {
          cat(green("All `year_id` values are filled!\n"))
        }
      } else {
        stop("The `year_start` and `year_end` columns are missing. Please check the column names.")
      }
    }
  
  dt[, `:=`(year_start = NULL, year_end = NULL)]
  
  # add indicator saying these are old GBD 2021 surveys
  dt[, `:=`(is_gbd_2021_survey = 1)]
  
  # if underlying_nid, is_clinical, uses_env columns are missing, fill with NA, 0, 0
  if (!all(c("underlying_nid", "is_clinical", "uses_env") %in% names(dt))) {
    cat(yellow("The `underlying_nid`, `is_clinical`, and `uses_env` columns are missing. Filling with NA, 0, 0\n"))
    dt[, `:=`(underlying_nid = NA, is_clinical = 0, uses_env = 0)]
  } else {
    cat(green("All `underlying_nid`, `is_clinical`, and `uses_env` columns are present!\n"))
  }

  return(dt)
  
}

# validate that sex_id is either 1 2 or 3 function ----
get_sex_id <- function(dt) {
  # check if "sex" column exists
  if ("sex" %in% names(dt)) {
    cat(green("The `sex` column is present. Computing `sex_id`\n"))
    dt[, sex_id := ifelse(sex == "Male", 1, 
                          ifelse(sex == "Female", 2,
                                 ifelse(sex == "Both", 3, NA)))]
    dt$sex_id <- as.integer(dt$sex_id) # Ensure data type
    dt[, sex := NULL]
  } else {
    stop("The `sex` column is missing. Please check the column names.")
  }
  
  # check sex_id for NA
  if (any(is.na(dt$sex_id))) {
    stop("There are missing values in the `sex_id` column. Please check the `sex` column.")
  } else {
    cat(green("All `sex_id` values are present\n"))
  }
  
  return(dt)
}

# validate location_id
validate_location_ids <- function(dt) {
  # check if "location_id" values are
  if ("location_id" %in% names(dt)) {
    cat(yellow("The `location_id` column is present. Checking for missing.\n"))
    if (nrow(dt[is.na(location_id),]) > 0) {
      stop("There are missing values in the `location_id` column. Please check the `location` column.")
    } else {
      cat(green("All `location_id` values are present. Validating values using GBD 2023 location hierarchy (release ID 16)...\n"))
      if (any(!dt$location_id %in% unique(location_metadata$location_id))) {
        # print location_id values are not present in location_metadata
        cat(red("Some `location_id` values are not valid. Please check the `location_id` column.\n"))
      } else {
        cat(green("All `location_id` values are valid!\n"))
      }
    }
  } else {
    stop("The `location` column is missing. Please check the column names.")
  }
  
  return(dt)
}

# nikitn 9/30/2024 fix location inconsistencies - see email "Location inconsistencies - GBD and GHDx for GBD 2023" from 9/22/2024 from DQI
fix_loc_inconsistencies <- function(dt){
  dt[nid == 210529 & location_name == "Australia", `:=` (location_name = "Austria", location_id = 75)]
  dt[nid == 160683 & location_name == "Northern Ireland", `:=` (location_name = "Ireland", location_id = 84)]
  dt[nid == 160684 & location_name == "Scotland", `:=` (location_name = "Ireland", location_id = 84)]
  
  return(dt)
}

# validate age spans and overlaps ----
validate_ages <- function(dt) {
  # if age_end > 95, set to 125
  if (any(dt$age_end > 95)) {
    cat(yellow("Some `age_end` values are greater than 95. Setting them to 125.\n"))
    dt[age_end > 95, age_end := 125]
  } else {
    cat(green("All `age_end` values are less than or equal to 95.\n"))
  }
  
  # if age_end >= 5, set age_end to the nearest increment of 5
  if (any(dt$age_start >= 5 & dt$age_end %% 5 != 0)) {
    cat(yellow("Some `age_end` values are not multiples of 5. Rounding them to the nearest increment of 5.\n"))
    # apply general rounding logic to age groups >= 5 and not covered by the specific cases above
    dt[age_start >= 5, ':=' (
      age_start = as.numeric(floor(age_start / 5) * 5),
      age_end = as.numeric(ceiling(age_end / 5) * 5)
    )]
    # specifically for 0 to 4
    dt[age_end == 4, age_end := 5]
  } else {
    cat(green("All `age_end` values for rows with age_start >= 5 are multiples of 5.\n"))
  }
  
  # ensure no age_start == age_end
  if (any(dt$age_start == dt$age_end)) {
    stop("Some `age_start` values are equal to `age_end`. Please check the `age_start` and `age_end` columns.")
  } else {
    cat(green("All `age_start` values are not equal to `age_end`.\n"))
  }
  
  return(dt)
}

# function get nid, data_type_name, and title from ghdx_metadata that is already in the environment ----
get_ghdx_metadata <- function(dt) {
  cat(yellow("\nRetrieving NID, data_type_name, and title from ghdx_metadata..."))
  if (!exists("ghdx_metadata")) {
    stop("ghdx_metadata does not exist in the environment. Please load it first.")
  } else {
    cat(yellow("Merging dt on GHDx DB output to get NID, title, and data_type_name..."))
    dt[, underlying_nid := NULL]
    dt_ghdx <- merge(dt, ghdx_metadata, by = "nid", all.x = TRUE)
  }
  # bind that row with all existing rows in dt
  
  if (any(is.na(dt_ghdx[, .(data_type_name, title)]))) {
    stop("data_type_name and/or title var(s) have missing values in the merged dt. Please double-check the input.")
  } else {
    cat(green("\ndata_type_name, and title are filled!\n"))
  }
  return(dt_ghdx)
}


# apply the function
dt <- format_surveys(dt)
dt <- get_sex_id(dt)
dt <- validate_location_ids(dt)
dt <- fix_loc_inconsistencies(dt)
dt <- validate_ages(dt)
dt <- get_ghdx_metadata(dt)


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
  formatted_surveys <- copy(dt)
  file_path <- paste0(run_dir, "FILEPATH")
  cat(paste0("Saved formatted_surveys.csv into ", file_path, " for further processing with the other sources.\n"))
  fwrite(formatted_surveys, paste0(file_path))
}
