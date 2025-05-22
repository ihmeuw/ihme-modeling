#-------------------------------------------------------------------------------
# Project: Nonfatal CKD Estimation - Stage-specific etiology proportions
# Purpose: Home to functions for making stage-specific etiology predictions.
#-------------------------------------------------------------------------------

# ---LOAD LIBRARIES-------------------------------------------------------------

library(assertthat)
library(dplyr)
library(data.table)
library(MASS)
library(magrittr)

# ---FUNCTIONS------------------------------------------------------------------

center_age <- function(age_group_years_start, age_group_years_end) {
  # Fit the age curve for a specified range of age groups..
  #
  # Arguments:
  # age_group_years_start (numeric): Vector of numeric data representing the 
  # value an age range starts at.
  # age_group_years_end (numeric): Vector of numeric data representing the 
  # value an age range ends at.
  #
  # Returns:
  # Vector of numeric values representing centered ages.
  
  assertthat::assert_that(is.numeric(age_group_years_start))
  assertthat::assert_that(is.numeric(age_group_years_end))
  
  age_midpoint <- (
    ((age_group_years_start + age_group_years_end) / 2) - 60) / 10
  message("Centered age values")
  return(age_midpoint)
}


create_stage_etio_predictions <- function(predictors, 
                                          coefficients, 
                                          vcovars) {
  # Creates predictions for each stage and etiology of CKD for specified 
  # datasets of predictors, coefficients and variance-covariances.
  #
  # Arguments:
  # predictors (data.table): Data.table of variables to generate predictions 
  # for.
  # coefficients (data.table): Data.table of coefficients to use in generating 
  # estimates.
  # vcovars (data.table): Data.table of estimates representing
  # variance-covariance values.
  #
  # Returns:
  # Data.table of draws for each, stage, etiology and predictor.
  #
  
  assertthat::assert_that(is.data.table(predictors))
  assertthat::assert_that(is.data.table(coefficients))
  assertthat::assert_that(is.data.table(vcovars))
  
  message("Beginning predictions")

  stages <- unique(coefficients[, stage])
  etios <- unique(coefficients[, etio])
  
  final_predictions <- list()
  
  for (stage_name in stages) {
    for (etio_name in etios) {
      
      # Transform data.table of variables into a matrix because the 
      # mvrnorm function requires a matrix to create a regression coefficient.
      predictor_mat <- as.matrix(
        copy(predictors[stage == stage_name, .(age, age2, sex_id, const)]))
      
      # Transform data.table of coefficients for a given stage and etiology into 
      # a matrix because the mvrnorm function requires a matrix to create a 
      # regression coefficient.
      coefficient_mat <- as.matrix(copy(
        coefficients[stage == stage_name & etio == etio_name, .(rrr)]))
      
      # Subset variance-covariance matrix to the square matrix for your given 
      # stage/etiology.
      keepcols <- names(vcovars)[grep(etio_name, names(vcovars))]
      vcovar_mat <- as.matrix(
        copy(vcovars[stage == stage_name & etio == etio_name, 
                     keepcols, 
                     with = F]))
      
      # Create 1,000 draws of regression coefficient using the betas 
      # (coefficients) + variance-covariance matrix
      message("Create 1,000 draws of regression coefficients from 
              variance-covariance matrix")
      coefficient_draws <- t(MASS::mvrnorm(n = 1000, 
                                           mu = coefficient_mat, 
                                           Sigma = vcovar_mat))
      
      # Predict by multiplying your predictor matrix by the draws of your 
      # regression coefficients
      message(paste("Generate predictions for", etio_name, stage_name))
      etio_predictions <- as.data.table(predictor_mat %*% coefficient_draws)
      
      # Re-label the draw columns
      draw_columns <- paste0("draw_", 0:999)
      setnames(etio_predictions, paste0("V", 1:1000), draw_columns)
      
      # Combine your predictor matrix (initial variables that predictions were 
      # created for) to your predictions to make sure everything is labeled.
      etio_predictions <- cbind(predictors[stage == stage_name, ], 
                                etio_predictions)
      
      # Add on etiologies and stages for labeling purposes.
      etio_predictions[, etio := etio_name]
      etio_predictions[, stage := stage_name]
      
      # Add to existing predictions. 
      final_predictions <- dplyr::bind_rows(list(final_predictions, 
                                                 etio_predictions))
      
    }
  }

  return(final_predictions)
}
