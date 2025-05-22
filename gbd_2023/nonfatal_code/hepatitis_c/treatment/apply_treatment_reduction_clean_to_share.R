## #############################################################################
## 
## PREPARE DATA FOR HEPATITIS C TREATMENT ANALYSIS
## 
## #############################################################################

# TO DO: 
# Apply half year reduction 
# Previous years of treatment intervention when you were the same age 
# Previous years of treatment intervention when you were an earlier age 


rm(list = ls(all.names = TRUE))
user <- Sys.info()['user']

## Use default arguments if running from an interactive session
## Otherwise, pull from command line arguments
if(interactive()){
  
  ## When running interactively, change only input arguments here
  repo_dir <- file.path('FILEPATH')
  config_fp <- file.path(repo_dir, 'config.yaml')
  date_underscore <- gsub("-", "_", Sys.Date())
} 

## Setup ---------------------------------------------------------------------->

# Load all repo libraries and functions
source(file.path(repo_dir,'functions/setup_functions.R'))
cmem_setup(repo_dir, fail_if_not_lbd=FALSE)
pacman::p_load(tidyverse, dplyr, plyr)
# Load config for all default values
CONF <- HepTreatmentConfig$new(config_path=config_fp)
# Load all GBD get_* functions
load_gbd_shared_functions(
  cc_library_filepath=CONF$get_path('cc_library')
)
draws <- paste0("draw_", 0:999)
n_draws <- paste0("n_draw_", 0:999)
dt.shift <- data.table::shift

#Set up directories 
data_prep_path <- CONF$get_path('data_prep_dir')
reduction_path <- CONF$get_path('reduction_dir')

data_prep_dir <- file.path(data_prep_path, date_underscore)
reduction_dir <- file.path(reduction_path, date_underscore)

viz_dir <- paste0("FILEPATH")
viz_input_dir <- paste0("FILEPATH")


# Create the output directory
safe.dir.create(data_prep_dir)
safe.dir.create(reduction_dir)
safe.dir.create(viz_dir)
safe.dir.create(viz_input_dir)

# Load and format GBD data 
loc_meta <- get_location_metadata(
  location_set_id=CONF$get_value('location_set_id'),
  release_id=CONF$get_value('release_id')
)

# Get age groups for analysis
age_group_meta <- get_age_meta_info()

sex_meta <- get_ids('sex')

measure_meta <- data.table(measure_id = c(5, 6), 
                           measure_name = c("prevalence", "incidence"))

## Pull GBD data
gbd_location_ids <- loc_meta$location_id

# Create list of ihme_loc_ids in data prep 
ihme_loc_ids <- list.dirs(data_prep_dir, full.names = F)
ihme_loc_ids <- ihme_loc_ids[ihme_loc_ids != ""]


  for (ihme_locid in unique(ihme_loc_ids)){
  message(paste0("Working on: ", ihme_locid))

    #Create filepath 
    input_path <- paste0(data_prep_dir, "/", ihme_locid, "/", ihme_locid, ".csv")
    
    # Load in prepped data 
    dt <- fread(input_path)
    loc_id <- unique(dt$location_id)
    
    
    # Subset to data >= 5 years old (ie age group id 6)
    # We are doing this because it simplifies the passage of age through time 
    # and because treatment is not recommended until age 4 
    dt <- dt[age_group_id >= 6 ]
    dt <- subset(dt, age_group_id !=33)
    
    # Creating year_start so create_prevalent_cases will work, this is round about but easiest option 
    dt1 <- copy(dt)
    dt1[, year_start := year_id]  
    
    # Pull in cases from dismod and use population to crease cases number 
    prev_cases <- create_cases(in_data = dt1, loc_id = loc_id, measure = 5)
    prev_cases <- subset(prev_cases, age_group_id !=33)
    
    # Merge in the cases from dismod cases
    dt <- merge(dt, prev_cases, by = c("location_id", "sex_id", "age_group_id", "year_id"), all.x = TRUE)
    
    year_ranges <- unname(tapply(unique(dt$year_id), cumsum(c(1, diff(unique(dt$year_id))) != 1), range))
    
    if (length(year_ranges) == 1) {
        # Don't need to mess with the apply_reduction function because not into future space yet
       reduction <- apply_reduction(dt)
        
       all_reduction <- predict_treatment(reduction, prev_cases, covid_estimation = T)
       all_reduction <- copy(all_reduction)[, (draws) := NULL] # adding these in because of the discontinous years 
       names(all_reduction) <- gsub("n_d", "d", names(all_reduction))
       all_reduction  <- subset(all_reduction , !is.na(total_treated))
       
       
    }  else {
      message("Year ranges are not continuous")
      # NOTE: This is coded so there can only be 2 discontinuous ranges, if there are more it will not factor that in 
      discont_min_year <- min(unique(year_ranges[[2]]))
      dt_subset1 <- dt[year_id %in% c(min(year_ranges[[1]]):max(year_ranges[[1]])), ]
      dt_subset2 <- dt[year_id %in% c(min(year_ranges[[2]]):max(year_ranges[[2]])), ]
      
      # First round of reduction and prediction to fill in missing year 
      reduction1 <- apply_reduction(dt_subset1)
      all_reduction1 <- predict_treatment(reduction1, prev_cases, covid_estimation = T)
      all_reduction1 <- all_reduction1[year_id < discont_min_year, ] # only keep up to (but not including) where we have the next min in treatment data
      all_reduction1[, (n_draws) := NULL] # get rid of n_draws to replicate dt structure 
      
      # Second round of reduction and prediction to fill up to max estimation year 
      dt_nosubset <- rbind(all_reduction1, dt_subset2)
      reduction2 <- apply_reduction(dt_nosubset)
      all_reduction2 <- predict_treatment(reduction2, prev_cases, covid_estimation = T)
      all_reduction2 <- copy(all_reduction2)[, (draws) := NULL] # adding these in because of the discontinous years 
      names(all_reduction2) <- gsub("n_d", "d", names(all_reduction2))
      all_reduction <- as.data.table(copy(all_reduction2))
      all_reduction  <- subset(all_reduction , !is.na(total_treated))
      
    }                    

    all_reduction <- adjust_subnationals(dt1, loc_id, all_reduction, prev_cases)
    prev_reduction <- all_reduction[[1]]
    prev_cases <- all_reduction[[2]]
    locations <- all_reduction[[3]]

    proportion <- create_proportions(prev_cases, prev_reduction)

    inc_cases <- create_cases(in_data = dt1, loc_id = locations, measure = 6)
    inc_reduction <- adjust_incidence(inc_cases, proportion)

    total_reduction <- rbind(prev_reduction, inc_reduction, fill = TRUE)
    diagnostics <- create_diagnostics(total_reduction, prev_cases, inc_cases, ihme_locid)

    prev_rate_draws <- counts_to_rate(prev_reduction)
    prev_rate_draws$measure_id <- 5
    inc_rate_draws <- counts_to_rate(inc_reduction)
    inc_rate_draws$measure_id <- 6
    rate_draws <- rbind(prev_rate_draws, inc_rate_draws, fill = TRUE)

    clean_data <- clean_adjusted_data(data = rate_draws)

    for (loc in unique(clean_data$location_id)) {
      for(measure in unique(clean_data$measure_id)) {
      clean_data_subset <- clean_data[location_id == loc & measure_id == measure]
      ihme_loc_id1 <- loc_meta[location_id == loc, unique(ihme_loc_id)]
      write.csv(clean_data_subset, paste0(reduction_dir, "/", measure, "_", ihme_loc_id1, ".csv"), row.names = FALSE)
      }
    }
}

append_pdf(viz_dir, "diagnostic")

