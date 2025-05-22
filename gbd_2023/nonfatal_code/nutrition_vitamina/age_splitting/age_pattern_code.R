# Age Splitting Models for Micronutrients Analysis
# Author: NAME
# Initial Date: 
# Last Updated: 

# Clear the existing workspace
rm(list = ls())

# Configuration ################################################################

# Load necessary libraries
library(reticulate)  # For Python integration
library(boot)        # For inverse logit functions
library(scam)        # Shape Constrained Additive Models
library(data.table)  # Data manipulation
library(tidyverse)   # Data manipulation and visualization
library(mrbrt003, lib.loc = "FILEPATH/dev_packages/")  # Specific to MRBRT models

# Set Python environment for reticulate
Sys.setenv("RETICULATE_PYTHON" = "FILEPATH/mrtool_0.0.1/bin/python")
use_python("FILEPATH/mrtool_0.0.1/bin/python")

# Import Python module for MRTool
mr <- reticulate::import("mrtool")

# Load shared functions from central resource library
source("FILEPATH/get_outputs.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/get_population.R") 
source("FILEPATH/get_age_metadata.R") # FILEPATH

# Utility function to check for non-inclusion in a vector
'%ni%' <- Negate("%in%")


# Define Project Variables #############

makeDraws <- T
year<-2023
loc_set<-35
release<-16
draw_num<-1000L


# Import Data Section ###################################

# Define data directory and micronutrient of interest
data_dir <- "FILEPATH"  # Directory containing the data files
micronutrient <- "nutrient"  # Specific micronutrient to focus on

# Load sex-splitted data for nutrient exposure from the GBD 2023
input_file_path <- paste0("FILEPATH")
inputdata <- fread(input_file_path)  # Read in the CSV data file
# Create a copy of the data for manipulation
inputdata1 <- copy(inputdata)


# rename orignal_age_start and original_age_end to original_age_start and original_age_end
setnames(inputdata1, old = c("orignal_age_start", "orignal_age_end"), new = c("original_age_start", "original_age_end"))


# Select only the necessary variables for analysis
inputdata1 <- inputdata1[, .(nid, underlying_nid, year_start, year_end, original_age_start, original_age_end, mean, standard_error, sex, location_id, year_id)]

# Filter out rows with missing values in 'mean' or 'standard_error'
inputdata1 <- inputdata1[!is.na(mean) & !is.na(standard_error), ]

setDT(inputdata1)


# Below, we perform the logit transformation on both the mean prevalence and its SE.

# Transforming the standard error using the delta method

inputdata1$se_logit <- mapply(FUN = function(mu, sigma) {             
  msm::deltamethod(g = ~log(x1 / (1 - x1)), mean = mu, cov = sigma^2)
}, mu = inputdata1$mean, sigma = inputdata1$standard_error)

# Applying the logit transformation to the mean prevalence
inputdata1$logit_mean <- mapply(FUN = function(x) {             
  log(x / (1 - x))
}, inputdata1$mean)

# Retrieving location meta-data for the specified location set and release
locMeta <- get_location_metadata(location_set_id = loc_set, release_id = release)[, .(location_type, location_id, ihme_loc_id, super_region_id, super_region_name, region_id, region_name, level, is_estimate)]

# Filtering the data to include only 'admin0' location types
locMeta <- locMeta[location_type == "admin0", ]


# merge location meta data with the input data

inputdata1 =   merge(inputdata1, locMeta, by = "location_id")


#```````````````````````````````` Input data table````````````````````````````````````````````````````````````````

# Filter inputdata1 to exclude rows where logit_mean is NA
inputdata1 <- copy(inputdata1)[!is.na(logit_mean), ]

# Retrieve sex IDs
sex <- get_ids("sex")

# Merge inputdata1 with the sex identifiers
inputdata1 <- merge(inputdata1, sex, by = "sex")

## creating an age variable 

inputdata1[, age_midpoint := (original_age_start + 1 + original_age_end)/2, ]

# Calculate age midpoint for more accurate age representation
inputdata1[, age_midpoint := (original_age_start + original_age_end + 1) / 2]

# Assign age_group_id based on age range conditions
inputdata1[original_age_start >= 0 & original_age_end <= 0.01917808, age_group_id := 2]
inputdata1[original_age_start >= 0.01917808 & original_age_end <= 0.07671233, age_group_id := 3]
inputdata1[original_age_start >= 5 & original_age_end <= 10, age_group_id := 6]
inputdata1[original_age_start >= 10 & original_age_end <= 15, age_group_id := 7]
inputdata1[original_age_start >= 15 & original_age_end <= 20, age_group_id := 8]
inputdata1[original_age_start >= 20 & original_age_end <= 25, age_group_id := 9]
inputdata1[original_age_start >= 25 & original_age_end <= 30, age_group_id := 10]
inputdata1[original_age_start >= 30 & original_age_end <= 35, age_group_id := 11]
inputdata1[original_age_start >= 35 & original_age_end <= 40, age_group_id := 12]
inputdata1[original_age_start >= 40 & original_age_end <= 45, age_group_id := 13]
inputdata1[original_age_start >= 45 & original_age_end <= 50, age_group_id := 14]
inputdata1[original_age_start >= 50 & original_age_end <= 55, age_group_id := 15]
inputdata1[original_age_start >= 55 & original_age_end <= 60, age_group_id := 16]
inputdata1[original_age_start >= 60 & original_age_end <= 65, age_group_id := 17]
inputdata1[original_age_start >= 65 & original_age_end <= 70, age_group_id := 18]
inputdata1[original_age_start >= 70 & original_age_end <= 75, age_group_id := 19]
inputdata1[original_age_start >= 75 & original_age_end <= 80, age_group_id := 20]
inputdata1[original_age_start >= 80 & original_age_end <= 85, age_group_id := 30]
inputdata1[original_age_start >= 85 & original_age_end <= 90, age_group_id := 31]
inputdata1[original_age_start >= 90 & original_age_end <= 95, age_group_id := 32]
inputdata1[original_age_start >= 2 & original_age_end <= 5, age_group_id := 34]
inputdata1[original_age_start >= 95 & original_age_end <= 125, age_group_id := 235]
inputdata1[original_age_start >= 1 & original_age_end <= 2, age_group_id := 238]
inputdata1[original_age_start >= 0.07671233 & original_age_end <= 0.5, age_group_id := 388]
inputdata1[original_age_start >= 0.5 & original_age_end <= 1, age_group_id := 389]
# Assign a default age_group_id (22) to records with missing age_group_id
inputdata1[is.na(age_group_id), age_group_id := 22]

# Convert age_midpoint and sex_id to numeric types for further analysis
inputdata1[, age_midpoint := as.numeric(age_midpoint)]
inputdata1[, sex_id := as.numeric(sex_id)]

# Remove duplicate rows to ensure data uniqueness
inputdata1 <- unique(inputdata1)

# Adjust sex_id values by reducing them by 1 to make the values 1 and 0
inputdata1[, sex_id := sex_id - 1]

# Use ihme_loc_id as country_id for clearer naming
inputdata1[, country_id := ihme_loc_id]

# Convert country_id, region_id, and super_region_id into factor variables
# for better categorization and analysis in statistical models
inputdata1[, countryFactor := as.factor(country_id)]
inputdata1[, regionFactor := as.factor(region_id)]
inputdata1[, superregionFactor := as.factor(super_region_id)]

# Store the final cleaned and transformed dataset into 'full'
full <- inputdata1



#```````````````````````````` MRBRT```````````````````````````````````````````````````````````#
# Define the directory path for MRBRT output
# This path points to a shared mount location and includes specifics about the dataset and analysis purpose (GBD 2023, age splitting)

mrbrtOutDir <- "FILEPATH"
unlink(mrbrtOutDir, recursive = T)
dir.create(mrbrtOutDir)

# Load the data frame

# Initialize the MRData object from the 'mr' library or environment
data <- mr$MRData()

# Load dataset into the MRData object
data$load_df(
  inputdata1, # The dataset to be loaded
  col_obs = "logit_mean", # Column name for the observed values; should be in logit scale
  col_obs_se = "se_logit", # Column name for the standard error of the observed values; note that it should be in logit scale to avoid errors
  col_covs = list("age_midpoint", "sex_id"), # Covariates to include in the analysis, here exemplified by age and sex
  col_study_id = "underlying_nid" # Column to be used as the random effect in the analysis; identifies different studies or units
)
  
  
# specify the model parameters:
  
  logit_model = mr$LinearCovModel(
    alt_cov = c("age_midpoint"), 
    use_re = F, #saying to use random effects
    use_re_mid_point = F, #saying to use random effects
    use_spline = T,
    spline_knots = array(seq(0,1,length.out = 5)),
    spline_degree = 2L, # spline
    spline_knots_type = 'frequency',
    spline_l_linear = T,
    prior_spline_num_constraint_points = 100L
  )
  
  sex_model = mr$LinearCovModel(alt_cov = list("sex_id"), use_re = TRUE)

# fit the model 
  
  model <- mr$MRBRT(
    data = data,
    cov_models = list(mr$LinearCovModel("intercept", use_re=T),
                      logit_model, sex_model),
    inlier_pct = 1) # sets the portion of the data you want to keep
    model$fit_model(inner_print_level = 5L, inner_max_iter = 500L)
  

  
  
# `````````````````````Cascading spline``````````````````````````````````````````````````````````````````````` #


  cascade_model <- run_spline_cascade(stage1_model_object = model,
                                         df = inputdata1,
                                         col_obs = "logit_mean",
                                         col_obs_se = "se_logit",
                                         col_study_id = "underlying_nid",
                                         stage_id_vars = c("super_region_id","country_id"),
                                         thetas = c(10, 10),
                                         output_dir = mrbrtOutDir,
                                         model_label = "cascading_sm_MRBRT_year",
                                         gaussian_prior = T, 
                                         inner_print_level = 5L,
                                         overwrite_previous = T)
  
# ````````````````````````````````````````````````````predict`````````````````````````````````````````````````````````#`
  
  # The 'predict_spline_cascade' function applies the fitted model ('cascade_model') to new data ('full')
  full <- predict_spline_cascade(fit = cascade_model, newdata = full) # The 'full' dataset is being used for predictions here
  
  # Convert 'full' into a data.table object for efficient data manipulation
  
  setDT(full)
  
  # Remove the 'cascade_prediction_id' column from 'full'
  full[, cascade_prediction_id := NULL]
  
  # Rename the column 'pred' to 'pred_logit_yr' for clarity
  setnames(full, "pred", "pred_logit_yr")
  
#  Regress between the predicted value and observed value just for checking the prediction accuracy 
    
  summary(lm(data = full, pred_logit_yr ~ logit_mean))
  ggplot(data = full, aes(x = logit_mean, y = pred_logit_yr, color = super_region_name)) + geom_point()
  
    
  # Add a column to 'full' that indicates the stage_id 
  
  inputdata1$stage_id = inputdata1$super_region_name
  inputdata1$stage_id = as.factor(inputdata1$stage_id)
  
  full$stage_id = as.factor(inputdata1$super_region_name)
  full$stage_id = as.factor(inputdata1$super_region_name)
  
  
 # Plot the pattern of the observed and predicted values
  
  P2 =  ggplot(full, aes(x = age_midpoint, y = logit_mean, color = super_region_name, group = underlying_nid)) +
    geom_point(size = 2) +
    facet_wrap(~super_region_name) + 
    geom_line() + 
    scale_x_continuous("age_midpoint", breaks = c(0, 1, 3, 5, 10, 20, 40, 60, 80, 100))
  
  
  P3 =  ggplot(full, aes(x = age_midpoint, y = pred_logit_yr, color = super_region_name, group = underlying_nid)) +
    geom_point(size = 2) +
    facet_wrap(~super_region_name) + 
    geom_line() + 
    scale_x_continuous("age_midpoint", breaks = c(0, 1, 3, 5, 10, 20, 40, 60, 80, 100))
  
  
##  get population 
    pop <- get_population(age_group_id = c(2,3,4,5,6,7,8,9:20, 30:33, 34, 238, 235, 388, 389) ,
                        year_id=2010, sex_id=c(1,2), with_ui=FALSE, release_id=16, location_id = 'all')
  
## get age meta-data
    ages = get_age_metadata(release_id = 16)
  
  pop_age = merge(pop, ages, by = c("age_group_id"))
  pop_age_loc = merge(pop_age, locMeta, by = c("location_id"))
  
# calculate age_midpoint
  
  pop_age_loc[, age_midpoint := (age_group_years_start + 1 + age_group_years_end)/2, ]
  
##
  pop_age_loc <- pop_age_loc %>%
    mutate(sex_id = sex_id -1)
  
 
    
#```````````````````````````` Draws`````````````````````````````````````` #

  if (makeDraws==T) {
  #Get a list of the models that we have
  pklFolder <- paste0(mrbrtOutDir, "/cascading_sm_MRBRT_year/pickles/")
  model <- list.files(pklFolder, full.names = F)
 
# How many draws we need: 
  
  samples <-draw_num
  
# make the iso3 column in the pop_age_loc
  
  pop_age_loc$iso3 = pop_age_loc$ihme_loc_id
  
# Make country_id & location factor variables
  
  pop_age_loc$country_id = pop_age_loc$ihme_loc_id
  pop_age_loc[, countryFactor     := as.factor(country_id)]
  pop_age_loc[, regionFactor      := as.factor(region_id)]
  pop_age_loc[, superregionFactor := as.factor(super_region_id)]
  
# Add country and super_region file names columns to draws dt
  
  drawTemplate <- copy(pop_age_loc)
  drawTemplate[, country_file := paste0("country_id__", country_id, ".pkl")]
  drawTemplate[, region_file  := paste0("super_region_id__", super_region_id, ".pkl")]
  
  
# Create function to get the draws
  
  
  country_draw = data.table()
  
    for (k in unique(drawTemplate$iso3)) {
      print(k)
        for (i in unique(drawTemplate$age_group_id)) {
        print(i)
        for(j in unique(drawTemplate$sex_id)) {
         print(j)
           
      # Get the country and region file names
          
         cFile <- unique(drawTemplate[country_id == k & age_group_id ==i & sex_id ==j, country_file])
         rFile <- unique(drawTemplate[country_id == k & age_group_id ==i & sex_id ==j, region_file])
       

       # Grab the model that we need for this location
         
          model_file <- ifelse(cFile %in% model, cFile, ifelse(rFile %in% model, rFile, "stage1__stage1.pkl"))
           final_model <- py_load_object(filename = paste0(pklFolder, model_file), pickle = "dill")
    
   
      samples1 <- core$other_sampling$sample_simple_lme_beta(
      sample_size = samples,
      model =   final_model
    )
 # Create a subset of the data for just the country being predicted
    
    drawSubset  <- drawTemplate[country_id == k & age_group_id == i & sex_id ==j, ]
    drawSubset =  unique(drawSubset)
    
    drawSubset$sex_id = as.numeric(drawSubset$sex_id)
    drawSubset$age_midpoint = as.numeric(drawSubset$age_midpoint)
    
    drawPreds <- MRData()
    drawPreds$load_df(
      data = drawSubset,
      col_covs = list("age_midpoint", "sex_id"))
    
    drawCols <- final_model$create_draws(
      data = drawPreds,
      beta_samples = samples1,
      gamma_samples = matrix(rep(0, samples*2), ncol = 2),
      random_study = FALSE )
    
# Prep and bind to prediction data
    colnames(drawCols) <- paste0("draw_",0:999)
    drawSubset <- cbind(drawSubset[iso3 ==k, .(location_id, year_id, ihme_loc_id, sex_id, age_group_id, age_midpoint, population)], as.data.table(drawCols))
    
# Add a column that is the country file and model that was used for reference
    drawSubset[, model_used   := model_file]
    drawSubset[, country_file := cFile]
    
# Append to country_draw rather than overwrite it
    country_draw <- rbind(country_draw, drawSubset)
       }
      }
    }
    
  
# Reverse back the logit draws into a linear space 
  
  
  draws = paste0("draw_", 0:999)
  
# Define function to convert from logit space into linear space 
  
  inverse_logit <- function(x) {
    return(1 / (1 + exp(-x)))
  }
  
# Apply the inverse_logit function to these columns
  
  country_draw[, (draws) := lapply(.SD, inverse_logit), .SDcols = draws]
  
# resetting the sex_id back to 1, and 2
  
  country_draw = country_draw[, sex_id := sex_id +1] 
  
  # save 
  
  write.csv(country_draw,paste0(mrbrtOutDir, "/", "nutrient", ".csv"),row.names = F) # FILEPATH
}



  
  