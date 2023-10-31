#############################################################
##Purpose: Child script to actually run MR-BRT for CFs
###########################################################

#### Setup -----
# Get arguments from qsub
rm(list = ls())
print(commandArgs())
commands <- commandArgs()[6]
user <- Sys.info()[7]

suppressPackageStartupMessages({
  library(data.table)
  library(tidyverse)
  library(parallel)
  library(magrittr)
  library(gridExtra)
  library(RMySQL)
  library(gtools)
  
})

# bundle: the bundle id you are taking in
# cf: the CF as a string, inputs are 'cf1', 'cf2', and 'cf3'
# vers: correction model version 
# prep_vers: input data version 
# pred_only: whether to run new models or predict off existing ones
# trim: MR-BRT trim level
# knot_vers: method for age spline knot placement - currently accepts 'freq' or 'manual'

commands <- strsplit(commands, ',')
bundle <- commands[[1]][1] %>% as.numeric() %>% print()
cf <- commands[[1]][2] %>% print()
vers <- commands[[1]][3] %>% as.numeric() %>% print()
prep_vers <- commands[[1]][4] %>% as.numeric() %>% print()
pred_only <- commands[[1]][5] %>% print()
trim <- commands[[1]][6] %>% print()
knot_vers <- commands[[1]][7] %>% print()

# data_folder: contains prepped cf estimates, one file per bundle/cf
# write_folder: place for final estimates, stored in separate files for each age/sex combo
# bun_write_folder: place for model outputs, will contain raw model summaries/predictions for troubleshooting
write_folder <- FILEPATH
data_folder <- FILEPATH
bun_write_folder <- FILEPATH

print(write_folder)
if(!file.exists(bun_write_folder)){
  dir.create(bun_write_folder, recursive = TRUE, showWarnings = FALSE)
}

# Run model only if specified to do so -----
error <- NA
if(pred_only){
  # predictions for models that already exist
  
  model_folder <- paste0(write_folder, 'by_cfbundle/mrbrt_', bundle, '_', cf,'/')
  if(!file.exists(paste0(model_folder,'input_metadata.csv'))){
    error = paste0("existing model was not found!")
    stop(error)
  }

  fit <- list(working_dir = model_folder, 
                sh_command = "", 
                input_metadata = fread(paste0(model_folder, 'input_metadata.csv')), 
                model_coefs = fread(paste0(model_folder, 'model_coefs.csv')), 
                fe_covariance_matrix = fread(paste0(model_folder, 'fe_covariance_matrix.csv')),
                train_data = fread(paste0(model_folder, 'train_data.csv')))
  
  if(file.exists(paste0(model_folder, 'spline_specs.csv'))){
    fit$spline_specs <- fread(paste0(model_folder, 'spline_specs.csv'))
  }
  
  # Predictions
  preddf <- createpreddf(bundle, write_folder)
  predictions <- predict_mr_brt(fit, newdata = preddf, write_draws = TRUE)
  
  tries <- 0
  while(tries < 3 & !check_for_preds(fit)){
    print('Rerunning predictions, something happened')
    predictions <- predict_mr_brt(fit, newdata = preddf, write_draws = TRUE)
    tries <- tries + 1
  }
  
  if(tries == 3 & is.na(error)){
    error <- "prediction didn't work!"
    stop(error)
  }
}else{
  # Run new model for bundle/cf
  fit <- mrbrtrun(tr = trim, 
                  knot_vers = knot_vers,
                  bundle = bundle,
                  cf = cf)
  
  if(!check_for_outputs(fit) & is.na(error)){
    error <- "model didn't run for this bundle!"
    stop(error)
  }

  # Predictions
  # preddf created based on 1) age sex restrictions for bundle, 2) clinical age groups being used by the bundle (<1s)
  preddf <- createpreddf(bundle, write_folder)
  print('preddf dimensions: ')
  dim(preddf)
  predictions <- predict_mr_brt(fit, newdata = preddf, write_draws = TRUE)
  
  # Sometimes predictions just don't run. Try again in that case.
  tries <- 0
  while(tries < 3 & !check_for_preds(fit)){
    print('Rerunning predictions, something happened')
    predictions <- predict_mr_brt(fit, newdata = preddf, write_draws = TRUE)
    tries <- tries + 1
  }
  
  if(tries == 3 & is.na(error)){
    error <- "prediction didn't work!"
    stop(error)
  }
  
  # Check if model didn't optimize - mean of cf estimates ~ 2e-9 or 500 million
  if((mean(predictions$model_summaries$Y_mean) < -20) | (mean(predictions$model_summaries$Y_mean) > 20)){
    print("Running model with less trimming")
    trim <- 0.05
    fit <- mrbrtrun(tr = trim, 
                    knot_vers = knot_vers,
                    bundle = bundle,
                    cf = cf)
    predictions <- predict_mr_brt(fit, newdata = preddf, write_draws = TRUE)
  }
  
  if((mean(predictions$model_summaries$Y_mean) < -20) | (mean(predictions$model_summaries$Y_mean) > 20)){
    print("Running model with less trimming")
    trim <- 0.01
    fit <- mrbrtrun(tr = trim, 
                    knot_vers = knot_vers,
                    bundle = bundle,
                    cf = cf)
    predictions <- predict_mr_brt(fit, newdata = preddf, write_draws = TRUE)
  }
  
  # Check if model didn't optimize - mean of cf estimates ~ 2e-9 or 500 million
  if((mean(predictions$model_summaries$Y_mean) < -20) | (mean(predictions$model_summaries$Y_mean) > 20)){
    print("Running model with less trimming")
    trim <- 0.005
    fit <- mrbrtrun(tr = trim, 
                    knot_vers = knot_vers,
                    bundle = bundle,
                    cf = cf)
    predictions <- predict_mr_brt(fit, newdata = preddf, write_draws = TRUE)
  }
  
  # Check if model didn't optimize - mean of cf estimates ~ 2e-9 or 500 million
  if((mean(predictions$model_summaries$Y_mean) < -20) | (mean(predictions$model_summaries$Y_mean) > 20)){
    print("Running model with less trimming")
    input <- fread(paste0(data_folder,bundle,'.csv'))
    if(nrow(input) > 510){
      trim <- 0.002
    }else{
      trim <- 0
    }
    fit <- mrbrtrun(tr = trim, 
                    knot_vers = knot_vers,
                    bundle = bundle,
                    cf = cf)
    predictions <- predict_mr_brt(fit, newdata = preddf, write_draws = TRUE)
  }
  
}

if(is.na(error)){
  print('Draws written')
}

### Extensions -----
# Extend both draws and predictions based on age pattern in input data
# Only want to use model where there's sufficient age data for a pattern;
# If age start/end of model isn't informed w/ enough data, extend model
# fit where there is data to min/max age.
# Also performs squaring where appropriate
predictions_draws <- predictions$model_draws %>% as.data.table()

# Bounds - whether model predicts on age/sex, min & max ages w/ sufficient data
bounds <- get_bounds(fit, preddf)
print(bounds)

# Square draws on sexes if model didn't predict on sex
if(!bounds[[3]] & length(unique(preddf$sex_id)) == 2){
  maled <- predictions_draws %>% mutate(X_sex_id = 1)
  predictions_draws$X_sex_id <- 2
  
  predictions_draws <- rbind(predictions_draws, maled)
  bounds[[3]] <- TRUE
}

# Square draws on age cov if model didn't predict on age
if(!bounds[[4]]){
  predictions_draws$index <- 1
  preddf$index <- 1
  predictions_draws <- merge(unique(predictions_draws), preddf[,.(age_midpoint, index)], allow.cartesian = TRUE)
  
  setnames(predictions_draws,'age_midpoint','X_age_midpoint')
  
  bounds[[4]] <- TRUE
}

# Create linear tails - extend data-rich prediction to data-sparse age min/max. Also copies single <1 prediction to all age groups for models not receiving <1
model_draws <- extender(predictions_draws, preddf, bounds)
model_draws <- model_draws %>% select(-X_intercept, -Z_intercept)

# Final formatting -----
# Check age/sex predictions against age sex restrictions & clinical age group ids
model_draws$X_age_midpoint <- round(model_draws$X_age_midpoint, 6)
preddf$age_midpoint <- round(preddf$age_midpoint,6)
stopifnot(
  nrow(anti_join(select(preddf, age_midpoint, sex_id), 
                 select(model_draws, X_age_midpoint, X_sex_id), by = c("age_midpoint" = "X_age_midpoint", "sex_id" = "X_sex_id"))) == 0
)

# sets ceiling of 100000 for max draw value, merges on age group info
# get_medians - gets point estimate from draws by taking median of draws, to line up w/ what you actually receive in your bundle
model_draws <- ernielite(model_draws, bundle, cf, write_folder)
all_cfs <- get_medians(model_draws)

# Data validation -----
# Series of checks for age and sex. Should be no NAs in draws and every demographic in preddf should be there
vars <- apply(model_draws[,7:length(model_draws)], 1, var)
vars <- vars[!is.na(vars)]
stopifnot(nrow(all_cfs[is.na(age_start)]) == 0,
            nrow(all_cfs[is.na(sex_id)]) == 0,
            nrow(model_draws[is.na(age_start)]) == 0,
            nrow(model_draws[is.na(sex_id)]) == 0,
            nrow(model_draws) == nrow(all_cfs),
            nrow(filter_at(model_draws, vars(starts_with("draw")), any_vars(. > 10000000))) == 0, # No absurdly high draws
            all(vars > 0) # All draws are the same
          )

model_stats <- data.frame("date" = as.character(Sys.time()),
                    "vers" = vers, 
                    "bundle" = bundle, 
                    "CF" = cf, 
                    "trim" = trim,
                    "sex_cov" = bounds[[3]],
                    "age_cov" = bounds[[4]],
                    "min_age_start" = bounds[[1]],
                    "max_age_start" = bounds[[2]],
                    "error" = "")

# Write data ----
# Writes one row to a run log w/ metadata about each model run.
write_csv(model_stats, paste0(write_folder, 'mr_brt_run_log.csv'), append = TRUE)
# Writes point estimate file for use in validation plots, HospViz
write_csv(all_cfs, paste0(write_folder, 'split/cf_estimates/',bundle,'_',cf,'.csv'))
# Writes draws to be compiled by cf_launcher.py or cf_compiler.R
write_csv(model_draws, paste0(write_folder, 'split/by_agesex/',bundle,'_',cf,'.csv'))

# Writes plots
pdf(paste0(write_folder,'validation/',bundle,'_',cf,'_model_plots.pdf'), onefile = TRUE, height = 9, width = 8)
cf_plots(bundle, cf, write_folder,from_file = TRUE)
dev.off()

print('DONE!!')




