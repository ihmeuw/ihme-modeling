## #############################################################################
## 
## PURPOSE: PREPARE DATA FOR HEPATITIS C TREATMENT ANALYSIS
## 
## #############################################################################

rm(list = ls(all.names = TRUE))
user <- Sys.info()['user']


## Setup ---------------------------------------------------------------------->
source(FILEPATH)

# Determine the input data directory
data_dir <- file.path(CONF$get_path('model_inputs'))

# Load and format GBD data 
loc_meta <- get_location_metadata(
  location_set_id=CONF$get_value('location_set_id'),
  gbd_round_id=CONF$get_value('gbd_round_id')
)

# Get age groups for analysis
age_group_meta <- get_age_meta_info()

## Pull GBD data
gbd_location_ids <- loc_meta$location_id

# Load in the data
full_dataset <- load_data(data_dir=data_dir)
full_dataset <- full_dataset[ihme_loc_id != ""]

for (ihme_locid in unique(full_dataset$ihme_loc_id)) {
  message(paste0("Working on: ", ihme_locid))
  #Create directory and subset to location of interest
  output_dir <- file.path(FILEPATH)
  safe.dir.create(output_dir)
  orig_data <- full_dataset[ihme_loc_id == ihme_locid]
  loc_id <- unique(orig_data$location_id)
  if(length(loc_id) > 1) stop("Multiple locations. Check extraction. ")
  
  # Convert rows into single years 
  full_data <- create_single_years(orig_data)
  full_data <- adjustment_incomplete_year_data(full_data)

  # Add in sex ids 
  full_data <- add_sex_ids(in_data=copy(full_data))
  
  # Get cases before processing the data 
  cases <- create_cases(full_data, loc_id, measure = 5)
  
  # Sex split the rows that have both sex information based on chronic hepatis
  full_data_sex <- sex_split_data(full_data, age_group_meta, cases)
  
  # Age split so all rows are in gbd age groups 
  full_data_age_sex <- age_split_data(data = full_data_sex, 
                                      age_group_meta = age_group_meta, 
                                      cases = cases, 
                                      drop_age_groups = FALSE)
  
  # Clean age group ids 
  full_data_age_sex1 <- clean_age_group_ids(full_data_age_sex)
  
  # Checking the sum of treated cases from original data set and processed data set before tx efficiacy applied 
  pct_check <- check_treated_cases(original_data = orig_data, processed_data = full_data_age_sex1) 
  
  
  # Apply treatment efficacy and then 
  # Sum all the cases treated by age, location, year, sex so treatment_total reflects all treatment regimens
  
  processed_data <- apply_treatment_efficacy(full_data_age_sex1)
  
  cleaned_data <- clean_final_data(processed_data)


  # Write final output where total treated is the column of treated of interest
  outpath <- FILEPATH
  write.csv(cleaned_data, outpath, row.names = FALSE)
}

