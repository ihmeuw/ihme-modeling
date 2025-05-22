#########################################################################################
### Date: 03/02/2023
### Project: ST-GPR custom linear stage
### Purpose: Calculate OOS validity and generate weights
#########################################################################################

code_root <- 'FILEPATH'
data_root <- "FILEPATH"

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common /ihme/ IO Paths
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
  library(argparse)
  print(commandArgs())
  parser <- ArgumentParser()
  parser$add_argument("--params_dir", type = "character")
  parser$add_argument("--draws_dir", type = "character")
  parser$add_argument("--interms_dir", type = "character")
  parser$add_argument("--logs_dir", type = "character")
  args <- parser$parse_args()
  print(args)
  list2env(args, environment()); rm(args)
  sessionInfo()
} else {
  params_dir <- paste0(data_root, "FILEPATH")
  draws_dir <- paste0(data_root, "FILEPATH")
  interms_dir <- paste0(data_root, "FILEPATH")
  logs_dir <- paste0(data_root, "FILEPATH")
}

# Load packages/central functions
source("/FILEPATH/get_population.R")
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/get_covariate_estimates.R")
source("/FILEPATH/get_bundle_version.R")
source(paste0(code_root, '/FILEPATH/processing.R'))
library(jtools)
library(lme4)
library(AICcmodavg)
pacman::p_load(data.table, dplyr,tidyr,stringr)
date <- Sys.Date()

# Set run dir
#gen_rundir(data_root = data_root, acause = 'cause', message = 'message')
run_file <- fread(paste0(data_root, "/FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "/crosswalks/")
draws_dir    <- paste0(run_dir, "/draws/")
interms_dir    <- paste0(run_dir, "/interms/")

## Set constants
run_oos <- TRUE
n_folds <- 5
set.seed(42) # For reproducibility
release_id <- release_id
years <- 1980:2024

## Create directories - update when transfer to repos
working_dir <- paste0(crosswalks_dir, 'FILEPATH/') 

#========================================
## CALCULATE IS/OOS RMSE
#========================================
models <- fread(paste0(params_dir, '/FILEPATH'))
models <- subset(models, model_status == 'keep')

# For consistency, reset n_folds to 0 if run_oos is FALSE
if (!run_oos & n_folds != 0) {
  message("Resetting n_folds to 0 since run_oos = FALSE.")
  message("If you meant to run an out-of-sample model, please check your options.")
  n_folds <- 0
}

df_input <- fread(paste0(params_dir, '/FILEPATH'))

# Generate folds (holdouts) by ihme_loc_id
if (run_oos == TRUE) {
  
  # Assign each country to a fold
  
  # Generate a table of the unique ids
  holdout_link_table <- data.table(ihme_loc_id = unique(df_input$ihme_loc_id))
  
  # Generate folds by taking random numbers for each row and dividing into quantiles
  # This ensures that groups are random and as equally sized as possible
  random_draws <- rnorm(nrow(holdout_link_table))
  quantiles <- quantile(random_draws, 0:n_folds/n_folds)
  
  which_folds <- cut(random_draws, quantiles, include.lowest = TRUE)
  levels(which_folds) <- 1:n_folds
  
  # Assign these to the ihme_loc_ids
  holdout_link_table[, fold_id := as.numeric(which_folds)]
  
  # Merge with input data set
  df_input <- merge(df_input, holdout_link_table, by = "ihme_loc_id")
  
}

# Run your model. (Note: here 0 is used as the "fold number" for a full, all-data-included model)
# This code runs the model n_folds + 1 times (the extra time is a full, in-sample model when fold = 0)
# Models and predictions are saved in the folder
# This loop would contain all of your usual regression stuff

model.list <- unique(models$model)

for (i in seq_along(model.list)){
  for (fold in 0:n_folds) {
    
    # Set up your input data set
    if (fold != 0) {
      
      # Subset data to only the data in the fold
      df_input_fold <- df_input[fold_id != fold]
      
    } else {
      
      # Use all of the input data
      df_input_fold <- copy(df_input)
      
    }
    
    # Run negative binomial model
    model <- glmer.nb(model.list[[i]], data = df_input_fold)
    
    
    # Save the model output
    saveRDS(model, file = paste0(working_dir, "FILEPATH.rds"))
    
    
    if (fold == 0) {
      
      # Predict out if the full model 
      
      pred_df <- subset(df_input, select = c("ihme_loc_id", "location_id", "year_id", "sdi", "uhc", "haqi", "pop_dens", "prop_urban", "sanitation", "water_prop", "log_ss", "cases_ur", "location_name", "region_name"))  
      pred_df$predicted_is <- predict(model, pred_df, type = 'response', allow.new.levels = TRUE) #remove type = response
      pred_df$model <- paste0('model_', i)
      
    } else if (fold != 0) {
      
      # For the holdouts, you really only need to make predictions for the held-out rows!
      
      pred_df <- subset(df_input, fold_id == fold) # Just the held-out locations
      pred_df$predicted_oos <- predict(model, pred_df, type = 'response', allow.new.levels = TRUE)
      pred_df <- subset(pred_df, select = c("ihme_loc_id", "location_id", "year_id", "sdi", "uhc", "haqi", "pop_dens", "prop_urban", "sanitation", "water_prop", "log_ss", "fold_id", "cases_ur", "predicted_oos", "location_name", "region_name")) 

      pred_df$model <- paste0('model_', i)
      
    }
    
    # Save the predicted df
    
    saveRDS(pred_df, file = paste0(working_dir, "FILEPATH.rds"))
    
  }
}

# Analyze the in vs out of sample predictions
# This will only work if running the above with more than 0 folds and run_oos == TRUE
# Load your in-sample predictions

in_sample <- lapply(1:length(model.list), function(i) {
  readRDS(paste0(working_dir, "FILEPATH.rds"))
}) %>% rbindlist

# Load your OOS predictions

out_of_sample <- data.table()
for(i in 1:n_folds){
  
  input_path <- (paste0(working_dir, "FILEPATH", i))
  
  for (k in 1:length(model.list)) {
    
    oos_data <- readRDS(paste0(input_path, "FILEPATH.rds"))
    out_of_sample <- rbind(oos_data, out_of_sample)
    
  }
}

# Note that there is one out of sample prediction per row of original input data!
# The out_of_sample data frame contains OOS predictions for each row, since they were
# predicted using a model fi without that whole fold
nrow(df_input) == nrow(out_of_sample)

# Merge the OOS predictions on to the IS predictions for comparison
df_is_oos <- merge(in_sample, 
                   subset(out_of_sample, select = c("location_id", "year_id", "predicted_oos", "fold_id", "model")),
                   by = c("location_id", "year_id", "model"))

# Calculate RMSE (all in log-incidence space here for convenience, not weighted, etc)
rmse = function(m, o){
  sqrt(mean((m - o)^2, na.rm = TRUE))
}

df_is_oos[, is_rmse := lapply(.SD, function(x) rmse(get('cases_ur'), get('predicted_is'))), by = c('model')] 
df_is_oos[, oos_rmse := lapply(.SD, function(x) rmse(get('cases_ur'), get('predicted_oos'))), by = c('model')]

# Generate weights on each of the models and take x% of each model
# For weights, add up all OOS RMSE values for the denominator

df_is_oos[, total_oos_rmse := lapply(.SD, sum), by = c('location_id','year_id'), .SDcols = 'oos_rmse'] #add up unique RMSE values
df_is_oos[, prop_rmse := lapply(.SD, function(x) get('oos_rmse') / get('total_oos_rmse')), by = c('location_id','year_id','model')] #proportion for weight of each model

# Check that proportions add up to 1
sum(unique(df_is_oos$prop_rmse)) 

df_weight <- unique(subset(df_is_oos, select=c('model','oos_rmse')))
df_weight[, weight := oos_rmse/sum(oos_rmse)] #weighted mean by loc/year (ie, weighted mean of models 1 thru XX for each loc/year)

#------------------------------- 
# multiply weight by in-sample predictions and collapse
#-------------------------------
model_ensemble <- left_join(df_is_oos, df_weight, by = c('model', 'oos_rmse'))
model_ensemble[, weight_prediction := lapply(.SD, function(x) get('predicted_is') * get('weight')), by = c('location_id','year_id','model')]
model_ensemble[, val := lapply(.SD, function(x) sum(get('weight_prediction'))), by = c('location_id','year_id')]

model_ensemble <- model_ensemble[, -c('model', 'predicted_is','predicted_oos','fold_id','is_rmse','oos_rmse',
                                      'total_oos_rmse','weight','weight_prediction', 'prop_rmse')]
model_ensemble <- unique(model_ensemble) 
model_ensemble <- model_ensemble[, -c('ihme_loc_id','location_name','region_name')]

#------------------------------- 
# PREPARE FOR UPLOAD TO ST-GPR
#-------------------------------
pop <- get_population(age_group_id = 22, sex_id = 3, location_id = unique(model_ensemble$location_id), year_id = years, release_id = release_id)
loc_meta <- get_location_metadata(release_id = release_id, location_set_id = 22)
loc_meta <- subset(loc_meta, select = c('location_id','super_region_id','region_id','location_name'))

model_final <- left_join(model_ensemble, pop, by = c('location_id','year_id'))
model_final <- left_join(model_final, loc_meta, by = 'location_id')
model_final$stage_1_cases <- model_final$val
model_final$val <- model_final$cases_ur
model_final$cv_custom_stage_1 <- model_final$stage_1_cases/model_final$population

write.csv(model_final, paste0(params_dir, '/FILEPATH'), row.names = FALSE) 

# convert to rates per 100,000 if ST-GPR data converted
model_final$cv_custom_stage_1_orig <- model_final$cv_custom_stage_1
model_final$cv_custom_stage_1 <- model_final$cv_custom_stage_1_orig * 100000

write.csv(model_final, paste0(params_dir, '/FILEPATH'), row.names = FALSE)