## Script Information
# Author: 
=# Revision Date: 07/19/2023
# Purpose: Cleaning the newly obtained VMNIS data for zinc and uploading it to the bundle.
# Dependencies: Requires the 'openxlsx', 'tools', 'data.table', 'tidyverse' packages, and specific IHME resources.

# Clear the environment
rm(list = ls())

# Install required packages (uncomment and specify packages as needed)
# install.packages(c("openxlsx", "tools", "data.table", "tidyverse"))

# Load libraries
library(openxlsx)
library(tools)
library(data.table)
library(tidyverse)
source("FILEPATH/get_age_metadata.R")  # Load age metadata functions
source("FILEPATH/get_location_metadata.R")  # Load location metadata functions

# Set nutrient and directories
nutrient <- "zinc"
data_dir <- "FILEPATH"

# Define the file path for the data to be cleaned
file_path <- file.path(data_dir, paste0(nutrient, "Filepath.xlsx"))

# Reading the data file
zinc_data <- openxlsx::read.xlsx(file_path)

# Selecting columns 2 through 46 which are the necessary variables for the zinc exposure modeling.
zinc_data <- data.table(zinc_data[, 2:46])

# Filtering the dataset
# The 'to_be_included' column was generated during the manual data cleaning phase 
# of the WHO VMNIS zinc dataset in Excel. This column designates whether a row is 
# eligible for inclusion (1 for eligible). Here, we filter the dataset to include only 
# those rows marked as eligible.

zinc_data <- zinc_data[to_be_included == 1]


# Remove "Underlying.NID.2021" column, which is not needed anymore
zinc_data[, `Underlying.NID.2021` := NULL]

# Rename "Underlying.NID.2023" to "underlying_nid"
setnames(zinc_data, "Underlying.NID.2023", "underlying_nid")

# Subset the data where to_be_included equals 1
zinc_data = zinc_data[to_be_included == 1, ]

# Drop unnecessary variables
zinc_data[, c("WHO.region", "ISOCODE", "Survey.(Id)", "Representativeness.(Id)", "Start.month", "End.month", "Severity.Alias", "General.data.comments", "Status") := NULL]


names1 = c("indicator", "verified_to_be_included", "location_name",  "underlying_nid", "survey_title", "representativeness", "urbanicity_type", "year_id",
           "year_start", "year_end", "population", "sex", "age_unit", "age_start", "age_end", "sample_size",  "indicator_unit", "mean_biomarker", "geometric_mean_biomarker",
           "median_biomarker", "SD_biomarker","SE_biomarker", "L95CI_biomarker", "U95CI_biomarker", "Value.type", "Cut.off.value", "mean", "Sample.collection.method", "biomarker_sample.type",
           "Method.of.analysis", "data_adjusted", "inidicator_comments", "Survey.Methodology", "Survey.type", "reference")


# Rename columns to standardize names 
setnames(zinc_data, old = names(zinc_data), new = names1)

# Store renamed data table for further processing
input_data = zinc_data

# Prepare data for epi data uploader
input_data$seq <- NA
input_data$input_type <- NA
input_data$page_num <- NA
input_data$table_num <- NA
input_data$source_type <- "Survey - other/unknown"
input_data$note_SR <- NA
input_data$bundle_id <- bundle_id
input_data$measure <- "proportion"

# Convert 'mean' column from percentage to proportion for consistency
input_data$mean <- as.numeric(input_data$mean)/100

# Exclude rows with NA values in 'mean' 
input_data <- input_data[!is.na(mean),]

# Retrieve location metadata based on specified location set and release identifiers
location_data <- get_location_metadata(location_set_id = 35, release_id=16)

# Compare location names from the input data(WHO_VMNIS) against those from the location metadata(IHME location names)
# This helps in identifying names present in input_data but missing in location_data
print(setdiff(input_data$location_name, location_data$location_name))

# Perform explicit renaming in the input data to ensure consistent location names
# This step is crucial for accurately merging data based on location names because location names is the only merging variable that exists in both WHO VMNIS dataset and 
## IHME location meta-data

# Correct the name for the United Kingdom reported in WHO VMNIS to match with IHME location naming
input_data$location_name[input_data$location_name == "United Kingdom of Great Britain and Northern "] <- "United Kingdom"

# Adjust the name for West Bank and Gaza Strip to match with IHME location naming

input_data$location_name[input_data$location_name == "West Bank and Gaza Strip" ] <- "Palestine"

# Merge input_data with location_data based on 'location_name'
input_data = merge(input_data, location_data, by = "location_name")

# Identify countries without zinc deficiency data
# These countries should be the priorities for future data extraction on zinc deficiency
no_countries_with_nozn_deficiencydata = paste0(
  setdiff(
    location_data[location_type == "admin0"]$location_name, 
    input_data$location_name
  ), 
  collapse = ","
)
##----------Formatting the columns needed for data upload to the database and adding flagging variables -------

input_data$smaller_site_unit <- NA  # Placeholder for future subnational data segmentation, currently not in use
input_data$site_memo <- NA  # Placeholder for notes related to site specifics, currently unused
input_data$input_type_id <- NA  

# Standardizing gender terminology in the dataset
# The purpose here is to unify the representation of gender data by converting variations to standard terms
input_data$sex[input_data$sex == "Females"] <- "Female"
input_data$sex[input_data$sex == "Males"] <- "Male"
input_data$sex[input_data$sex == "All"] <- "Both"  # 'Both' is used to denote data that encompasses all genders
input_data$sex_issue <- 0  # Initializing a flag for potential issues with sex data, set to 0 indicating no issues by default

# Initializing flags for data quality issues related to 'year' and 'age'
# These flags can be used in subsequent analyses to quickly identify records with potential data issues
input_data$year_issue <- 0  # Flag for tracking year-related issues, 0 indicates no issues

# Handling age data issues and initializing a flag for age-related data quality
input_data$age_issue <- 0  # Initially, we do not see any issues with age data
# Marking records with missing 'age_start' or 'age_end' as having an age issue
input_data$age_issue[input_data$age_start == "" | input_data$age_end == ""] <- 1
input_data$age_issue[input_data$age_start == NA | input_data$age_end == NA] <- 1  # NA values also indicate issues

# Adding notes for records with assumed age start or end values
# This is crucial for transparency and understanding the assumptions made during data processing
input_data$note_SR[input_data$age_start == "" | input_data$age_end == ""] <- "assumed age start or end"
input_data$note_SR[input_data$age_start == NA | input_data$age_end == NA] <- "assumed age start or end"

input_data$age_demographer <- 0  

##--------- Assign default age start and end values for different population groups in the dataset------------------

# Assign 15 as age start for women in reproductive age if age_start is NA
input_data$age_start[input_data$population %in% c("Pregnant women", "Lactating women (LW)", "Women of reproductive age", "Non-pregnant women (NPW)") & is.na(input_data$age_start) & input_data$age_unit == "Year"] <- 15

# Assign 49 as age end for women in reproductive age if age_end is NA
input_data$age_end[input_data$population %in% c("Pregnant women", "Lactating women (LW)", "Women of reproductive age") & is.na(input_data$age_end) & input_data$age_unit == "Year"] <- 49

# For nid 8618, set age_end to 59: This is based on a report from the survey

input_data$age_end[input_data$population %in% c("Adults", "Men") & is.na(input_data$age_end) & input_data$age_unit == "Year" & input_data$underlying_nid == 8618] <-  59

# For the Elderly population, assign 99 as age_end if NA
input_data$age_end[input_data$population %in% c("Elderly") & is.na(input_data$age_end) & input_data$age_unit == "Year"] <- 99

# For a catch-all population group "All", also assign 99 as age end if NA
input_data$age_end[input_data$population %in% c("All") & is.na(input_data$age_end) & input_data$age_unit == "Year"] <- 99

# If age_end is 0 and age_start is 65, set age_end to 99: This is based  on the report
input_data$age_end[input_data$age_end == 0 & input_data$age_start == 65] <- 99

# Set default values for various indicators and remove blanks in the indicator of interest
input_data$measure <- "proportion"
input_data$standard_error <- NA # place holder 
input_data$effective_sample_size <- NA # place holder 
input_data$cases <- NA # place holder 

# Ensure all entries have a sample size before proceeding with further analysis. 
input_data <- subset(input_data, !is.na(input_data$sample_size)) 

input_data$unit_type <- "Person"
input_data$unit_value_as_published <- 1
input_data$measure_issue <- 0
input_data$representative_name <- "Unknown"
input_data$uncertainty_type <- NA
input_data$uncertainty_type_value <- NA
input_data$recall_type <- "Point" # Setting recall type to 'Point'.
input_data$sampling_type  <- NA # NA implies no sampling type specified.
input_data$cases <- NA 
input_data$case_definition  <- NA 
input_data$case_diagnostics  <- NA 
input_data$note_modeler  <- NA 
input_data$extractor  <- "Copied_WHO_VMNIS" 

# Initialize outlier and subnational variables.
input_data$is_outlier  <- 0 # Mark as not an outlier.
input_data$cv_subnational <- NA

# Initialize recall type value and design effect.
input_data$recall_type_value <- NA # No recall type value specified.
input_data$design_effect <- NA # No design effect specified.

# For records with nid equal to 409630, set a specific citation.
input_data$field_citation_value[input_data$nid ==  409630] <- "WHO Global Micronutrients Database"
# For all other records, use the survey title as the citation.
input_data$field_citation_value[input_data$nid !=  409630] <- input_data$survey_title
input_data$response_rate <- NA
input_data$cv_pregnant <- NA
input_data$group_review <- NA
input_data$specificity <- NA

# Mark 'cv_pregnant' as 1 for pregnant and lactating women
input_data$cv_pregnant[input_data$population %in% c("Pregnant women", "Lactating women (LW)")] <- 1

input_data$group <- NA 
input_data$group[input_data$population %in% c("Non-pregnant women (NPW)", "Non-pregnant, non-lactating women (NPNLW)")] <- 1

# Mark 'group_review' as 1 for NPW and NPNLW
input_data$group_review[input_data$population %in% c("Non-pregnant women (NPW)", "Non-pregnant, non-lactating women (NPNLW)")] <- 1

# Ensure 'input_data' is a data frame
input_data <- as.data.frame(input_data)


# Convert 'input_data' to a data.table for more efficient data manipulation
input_data <- as.data.table(input_data)

# Rename the 'Value.type' column to 'value_type' for consistency in naming conventions
setnames(input_data, "Value.type", "value_type")

# Subset the data to include only rows where 'value_type' indicates zinc deficiency ('94 Deficient')
input_data1 = input_data[value_type %in% c("94 Deficient"),]

# Convert 'input_data_zinc_deficient' back to a data frame, if necessary
input_data1 <- as.data.frame(input_data1)
# Convert 'input_data_zinc_deficient' to datatable, if necessary
input_data1 <- as.data.table(input_data1)

# Calculate the standard error for prevalence rates
# The formula for standard error of a proportion is sqrt(p*(1-p)/n)
# where 'p' is the prevalence (mean) and 'n' is the sample size
input_data1 <- input_data1[, standard_error := sqrt((mean * (1 - mean) / sample_size))]

# Calculate the variance based on the standard error
input_data1$variance = input_data1$standard_error^2 
# --------------------------------------------------------------------------------
# Formatting to prepare the data for ST-GPR
# --------------------------------------------------------------------------------

# Remove rows with missing 'sample_size'
input_data1 <- input_data1[!is.na(sample_size),]

# Calculate the midpoint year for 'year_id'
input_data1[, year_id := floor((year_start + year_end) / 2)]

# Rename 'mean' to 'val' to comply with ST-GPR naming conventions
setnames(input_data1, "mean", "val")


# Rename 'mean' column to 'val' as 'mean' is not allowed in STGPR
setnames(input_data1, "mean", "val")


input_data1[age_start >= 0 & age_end <= 0.01917808, age_group_id := 2]
input_data1[age_start >= 0.01917808 & age_end <= 0.07671233, age_group_id := 3]
input_data1[age_start >= 5 & age_end <= 10, age_group_id := 6]
input_data1[age_start >= 10 & age_end <= 15, age_group_id := 7]
input_data1[age_start >= 0 & age_end <= 0.01917808, age_group_id := 2]
input_data1[age_start > 0.01917808 & age_end <= 0.07671233, age_group_id := 3]
input_data1[age_start >= 25 & age_end <= 30, age_group_id := 10]
input_data1[age_start >= 30 & age_end <= 35, age_group_id := 11]
input_data1[age_start >= 35 & age_end <= 40, age_group_id := 12]
input_data1[age_start >= 40 & age_end <= 45, age_group_id := 13]
input_data1[age_start >= 45 & age_end <= 50, age_group_id := 14]
input_data1[age_start >= 50 & age_end <= 55, age_group_id := 15]
input_data1[age_start >= 55 & age_end <= 60, age_group_id := 16]
input_data1[age_start >= 60 & age_end <= 65, age_group_id := 17]
input_data1[age_start >= 65 & age_end <= 70, age_group_id := 18]
input_data1[age_start >= 70 & age_end <= 75, age_group_id := 19]
input_data1[age_start >= 75 & age_end <= 80, age_group_id := 20]
input_data1[age_start >= 80 & age_end <= 85, age_group_id := 30]
input_data1[age_start >= 85 & age_end <= 90, age_group_id := 31]
input_data1[age_start >= 90 & age_end <= 95, age_group_id := 32]
input_data1[age_start >= 2 & age_end <= 5, age_group_id := 34]
input_data1[age_start >= 95 & age_end <= 125, age_group_id := 235]
input_data1[age_start >= 1 & age_end <= 2, age_group_id := 238]
input_data1[age_start >= 0.07671233 & age_end <= 0.5, age_group_id := 388]
input_data1[age_start >= 0.5 & age_end <= 1, age_group_id := 389]

# Assign a default age group ID for missing values in 'age_group_id'
input_data1[is.na(age_group_id), age_group_id := 22]

# Preserve the original age and year information in new columns
input_data1[, original_age_end := age_end]
input_data1[, original_age_start := age_start]
input_data1[, original_year_end := year_end]
input_data1[, original_year_start := year_start]
# Mark records that need age group splitting
input_data1[is.na(age_group_id), age_group_id := 22]

# Create columns to store original age and year ranges
input_data1[, `:=` (original_age_end = age_end, original_age_start = age_start, original_year_end = year_end, original_year_start = year_start)]

#

write.xlsx(input_data1, "FILEPATH", sheetName = "extraction")



